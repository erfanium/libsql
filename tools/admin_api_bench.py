#!/usr/bin/env python3
"""Benchmark the libsql-server admin API query endpoint with
realistic mixed traffic.

Three concurrent actions, each paced at an independent rate:
  N  create:  POST /api/databases {id} + CREATE TABLE IF NOT EXISTS todos
              via POST /api/query  (timing covers both: "time to create a
              ready-to-use namespace with the todos table")
  I  insert:  POST /api/query -> INSERT INTO todos(title) VALUES(?) on a
              random existing namespace
  C  count:   POST /api/query -> SELECT COUNT(*) FROM todos on a random
              existing namespace
  H  health:  GET /health on the sqld HTTP listener (server liveness ping,
              no DB involved); URL via --health-url

Every 10 seconds the tool prints, per action: accumulated total, rate/s over
the interval, and average request latency (ms).

Rates can be changed at any time from stdin:
    n=10 i=100 c=50     change rates (also accepts "n 10 i 100 c 50")
    <empty line>        print current rates
    q / quit            stop and print final summary

No third-party dependencies: stdlib only (threads + urllib).
"""

import argparse
import http.server
import json
import os
import random
import secrets
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from collections import deque

DDL = "CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL)"
INSERT_SQL = "INSERT INTO todos (title) VALUES (?)"
COUNT_SQL = "SELECT COUNT(*) FROM todos"


class Skip(Exception):
    pass


class Pacer:
    """Schedules invocations so the aggregate call rate equals `rate`/s
    regardless of how many worker threads share it.

    Drift-free: the slot schedule is anchored to the first slot, so OS
    sleep overshoot never drags the achieved rate below the target.
    Waits are capped at 1s and re-checked, so setting a rate of 0 (pause)
    or stopping the benchmark is always noticed within a second."""

    def __init__(self, rate=1.0):
        self._lock = threading.Lock()
        self._next = None
        self._interval = 1.0 / rate if rate > 0 else float("inf")
        self._wake = threading.Event()

    def set_rate(self, rate):
        with self._lock:
            now = time.monotonic()
            self._interval = 1.0 / rate if rate > 0 else float("inf")
            if self._next is None or self._next > now + self._interval:
                self._next = now  # schedule paused/abandoned: restart cleanly
        self._wake.set()

    def wake(self):
        self._wake.set()

    def pace(self, stop):
        while True:
            with self._lock:
                now = time.monotonic()
                if self._next is None:
                    self._next = now + self._interval
                slot = self._next
                self._next = slot + self._interval
            while True:
                wait = slot - time.monotonic()
                if wait <= 0:
                    return
                if stop.is_set():
                    return
                self._wake.clear()
                if self._wake.wait(min(wait, 1.0)):
                    break  # rate changed or woken: re-schedule with new interval


class Stats:
    def __init__(self):
        self._lock = threading.Lock()
        self.ok = 0
        self.err = 0
        self.skip = 0
        self.lat_sum = 0.0
        self.lat_min = float("inf")
        self.lat_max = 0.0

    def record_ok(self, dt_ms):
        with self._lock:
            self.ok += 1
            self.lat_sum += dt_ms
            self.lat_min = min(self.lat_min, dt_ms)
            self.lat_max = max(self.lat_max, dt_ms)

    def record_err(self):
        with self._lock:
            self.err += 1

    def record_skip(self):
        with self._lock:
            self.skip += 1

    def snapshot(self):
        with self._lock:
            return (
                self.ok,
                self.err,
                self.skip,
                self.lat_sum,
                self.lat_min,
                self.lat_max,
            )


def post_json(base, path, payload, admin_key, timeout):
    req = urllib.request.Request(
        base + path,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + admin_key,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        resp.read()


def http_get(url, timeout):
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        resp.read()


def make_health_action(url, timeout):
    def run():
        http_get(url, timeout)

    return run


def make_create_action(base, admin_key, timeout, pool, pool_lock):
    def run():
        nid = secrets.token_hex(8)
        post_json(base, "/api/databases", {"id": nid}, admin_key, timeout)
        post_json(base, "/api/query", {"namespace": nid, "sql": DDL}, admin_key, timeout)
        with pool_lock:
            pool.append(nid)

    return run


def make_insert_action(base, admin_key, timeout, pool, pool_lock):
    def run():
        with pool_lock:
            if not pool:
                raise Skip
            ns = random.choice(pool)
        payload = {
            "namespace": ns,
            "statements": [
                {"q": INSERT_SQL, "params": ["todo-" + secrets.token_hex(4)]}
            ],
        }
        post_json(base, "/api/query", payload, admin_key, timeout)

    return run


def make_count_action(base, admin_key, timeout, pool, pool_lock):
    def run():
        with pool_lock:
            if not pool:
                raise Skip
            ns = random.choice(pool)
        post_json(base, "/api/query", {"namespace": ns, "sql": COUNT_SQL}, admin_key, timeout)

    return run


def worker(stop, pacer, stats, fn):
    while not stop.is_set():
        pacer.pace(stop)
        if stop.is_set():
            break
        t0 = time.perf_counter()
        try:
            fn()
            stats.record_ok((time.perf_counter() - t0) * 1000.0)
        except Skip:
            stats.record_skip()
        except Exception:
            stats.record_err()


class Action:
    def __init__(self, name, rate, workers, stop, fn):
        self.name = name
        self.pacer = Pacer(rate)
        self.stats = Stats()
        self.stop = stop
        self.threads = []
        for _ in range(workers):
            t = threading.Thread(
                target=worker, args=(stop, self.pacer, self.stats, fn), daemon=True
            )
            t.start()
            self.threads.append(t)

    def set_rate(self, rate):
        self.pacer.set_rate(rate)


class ProcMonitor:
    """Samples /proc/<pid> for CPU (process-wide), RSS and disk I/O deltas.

    CPU is the sum of user+system ticks across all threads of the process,
    reported as percent of one core. RSS comes from VmRSS. Disk I/O comes
    from the io counters (read_bytes/write_bytes = actual block I/O)."""

    def __init__(self, pid):
        self.pid = pid
        self._lock = threading.Lock()
        self.ticks_per_sec = os.sysconf("SC_CLK_TCK")
        self.prev = self._read()
        self.prev_time = time.monotonic()
        self.cpu_samples = []
        self.io_r_total = 0
        self.io_w_total = 0
        self.sys_r_total = 0
        self.sys_w_total = 0

    def _read(self):
        try:
            with open(f"/proc/{self.pid}/stat") as f:
                data = f.read()
            rest = data.rsplit(")", 1)[1].split()
            utime = int(rest[11])
            stime = int(rest[12])
            rss_kb = None
            try:
                with open(f"/proc/{self.pid}/status") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            rss_kb = int(line.split()[1])
                            break
            except FileNotFoundError:
                pass
            io = {}
            try:
                with open(f"/proc/{self.pid}/io") as f:
                    for line in f:
                        k, _, v = line.partition(":")
                        io[k] = int(v.strip())
            except FileNotFoundError:
                pass
            return {
                "cpu_ticks": utime + stime,
                "rss_kb": rss_kb,
                # physical (actual block I/O)
                "io_r": io.get("read_bytes", 0),
                "io_w": io.get("write_bytes", 0),
                # logical (syscall-level: chars read/written via read/write)
                "sys_r": io.get("rchar", 0),
                "sys_w": io.get("wchar", 0),
            }
        except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError, IndexError):
            return None

    def sample(self):
        with self._lock:
            cur = self._read()
            if cur is None or self.prev is None:
                self.prev = cur
                return None
            now = time.monotonic()
            dt = now - self.prev_time
            cpu_pct = 0.0
            if dt > 0:
                cpu_pct = (
                    (cur["cpu_ticks"] - self.prev["cpu_ticks"])
                    / self.ticks_per_sec
                    / dt
                    * 100.0
                )
            io_r = max(0, cur["io_r"] - self.prev["io_r"])
            io_w = max(0, cur["io_w"] - self.prev["io_w"])
            sys_r = max(0, cur["sys_r"] - self.prev["sys_r"])
            sys_w = max(0, cur["sys_w"] - self.prev["sys_w"])
            self.cpu_samples.append(cpu_pct)
            self.io_r_total += io_r
            self.io_w_total += io_w
            self.sys_r_total += sys_r
            self.sys_w_total += sys_w
            self.prev = cur
            self.prev_time = now
            return {
                "cpu_pct": cpu_pct,
                "rss_kb": cur["rss_kb"],
                "io_r_b": io_r,
                "io_w_b": io_w,
                "sys_r_b": sys_r,
                "sys_w_b": sys_w,
            }


def format_system(sample, interval):
    if sample is None:
        return "sys: <gone>"
    cpu = sample["cpu_pct"]
    rss = sample["rss_kb"] / 1024.0 if sample["rss_kb"] is not None else None
    rss_s = f"{rss:.0f}MB" if rss is not None else "?"
    mbps = lambda b: b / 1024 / 1024 / interval  # noqa: E731
    return (
        f"sys: cpu={cpu:5.1f}% rss={rss_s} "
        f"io_r={mbps(sample['io_r_b']):5.2f}MB/s io_w={mbps(sample['io_w_b']):5.2f}MB/s "
        f"(syscall r={mbps(sample['sys_r_b']):5.2f} w={mbps(sample['sys_w_b']):5.2f}MB/s)"
    )


def join_threads(threads, timeout):
    """Join all threads, bounding the total wait to `timeout` seconds."""
    deadline = time.monotonic() + timeout
    for t in threads:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        t.join(remaining)


def format_action(name, cur, prev, interval):
    ok, err, skip, lat_sum, lat_min, lat_max = cur
    pok, _, pskip, plat_sum, _, _ = prev
    if ok == 0 and err == 0 and skip == 0:
        return f"{name}: total=0"
    per_sec = (ok - pok) / interval
    avg = lat_sum / ok if ok else 0.0
    parts = f"{name}: total={ok} ({per_sec:.1f}/s) avg={avg:.2f}ms"
    if ok and (ok - pok):
        avg_interval = (lat_sum - plat_sum) / (ok - pok) if (ok - pok) else 0.0
        parts += f" [{avg_interval:.2f}ms this window]"
    if ok:
        parts += f" min={lat_min:.2f}ms max={lat_max:.2f}ms"
    if err:
        parts += f" err={err}"
    if skip - pskip:
        parts += f" skip={skip - pskip}"
    return parts


class Live:
    """Per-second ring buffers consumed by the web dashboard."""

    def __init__(self, maxlen=3600):
        self.lock = threading.Lock()
        self.actions = {
            name: {k: deque(maxlen=maxlen) for k in ("rate", "avg", "err_rate")}
            for name in ("create", "insert", "count", "health")
        }
        self.sys = {k: deque(maxlen=maxlen) for k in ("cpu", "rss", "io_r", "io_w", "sys_r", "sys_w")}

    def push_action(self, name, t, rate, avg, err):
        with self.lock:
            self.actions[name]["rate"].append((t, rate))
            self.actions[name]["avg"].append((t, avg))
            self.actions[name]["err_rate"].append((t, err))

    def push_sys(self, t, **vals):
        with self.lock:
            for k, v in vals.items():
                self.sys[k].append((t, v))

    def snapshot(self):
        with self.lock:
            return {
                "actions": {n: {k: list(d) for k, d in self.actions[n].items()} for n in self.actions},
                "sys": {k: list(d) for k, d in self.sys.items()},
            }


def live_sampler(stop, actions, proc, live, t_start):
    """Push one data point per second per action (rate, avg latency, errors)
    and, when a local pid is given, one system point (cpu/rss/io)."""
    prev = {a.name: a.stats.snapshot() for a in actions}
    last = time.monotonic()
    while not stop.wait(1.0):
        now = time.monotonic()
        dt = now - last
        for a in actions:
            ok, err, skip, lat_sum, lat_min, lat_max = a.stats.snapshot()
            pok, perr, pskip, plat_sum, _, _ = prev[a.name]
            d_ok = ok - pok
            rate = d_ok / dt if dt > 0 else 0.0
            avg = (lat_sum - plat_sum) / d_ok if d_ok > 0 else 0.0
            live.push_action(a.name, now - t_start, rate, avg, err - perr)
            prev[a.name] = (ok, err, skip, lat_sum, lat_min, lat_max)
        if proc is not None:
            s = proc.sample()
            if s is not None:
                rss_mb = s["rss_kb"] / 1024.0 if s["rss_kb"] is not None else 0.0
                live.push_sys(
                    now - t_start,
                    cpu=s["cpu_pct"],
                    rss=rss_mb,
                    io_r=s["io_r_b"],
                    io_w=s["io_w_b"],
                    sys_r=s["sys_r_b"],
                    sys_w=s["sys_w_b"],
                )
        last = now


DASHBOARD_HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>admin api bench</title>
<style>
  :root { color-scheme: dark; }
  body { margin:0; font-family: system-ui, sans-serif; background:#0d0f12; color:#d7dce4; }
  header { padding:12px 20px; border-bottom:1px solid #262b33; display:flex; gap:22px; align-items:center; flex-wrap:wrap; }
  h1 { font-size:16px; margin:0; }
  .stat { display:flex; flex-direction:column; }
  .stat .label { font-size:11px; color:#8b93a3; text-transform:uppercase; letter-spacing:.05em; }
  .stat .value { font-size:20px; font-weight:600; font-variant-numeric: tabular-nums; }
  .rates { display:flex; gap:8px; align-items:center; margin-left:auto; }
  .rates input { width:64px; background:#171a20; border:1px solid #2a2f38; color:#d7dce4; border-radius:6px; padding:5px 8px; font:13px monospace; }
  .rates button { background:#2b5cff; border:none; color:white; border-radius:6px; padding:6px 12px; font-size:13px; cursor:pointer; }
  .rates button.stop { background:#7a2020; }
  .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(460px, 1fr)); gap:16px; padding:16px 20px; }
  .card { background:#111318; border:1px solid #262b33; border-radius:10px; padding:10px 12px; }
  .card-title { font-size:13px; font-weight:600; margin-bottom:2px; }
  .card-desc { font-size:11px; color:#8b93a3; margin-bottom:6px; }
  .legend { display:flex; gap:14px; flex-wrap:wrap; font-size:12px; margin-bottom:6px; }
  .legend .item { display:flex; align-items:center; gap:5px; }
  .legend b { font-variant-numeric: tabular-nums; }
  .dot { width:9px; height:9px; border-radius:50%; display:inline-block; }
  canvas { display:block; width:100%; }
</style>
</head>
<body>
<header>
  <h1>admin api bench</h1>
  <div class="stat"><span class="label">elapsed</span><span class="value" id="elapsed">0s</span></div>
  <div class="stat"><span class="label">pool</span><span class="value" id="pool">0</span></div>
  <div class="stat" id="tot-create"><span class="label">created</span><span class="value">0</span></div>
  <div class="stat" id="tot-insert"><span class="label">inserted</span><span class="value">0</span></div>
  <div class="stat" id="tot-count"><span class="label">counted</span><span class="value">0</span></div>
  <div class="stat" id="tot-health"><span class="label">health</span><span class="value">0</span></div>
  <div class="rates">
    n <input id="r-n" type="number" min="0" step="1">
    i <input id="r-i" type="number" min="0" step="1">
    c <input id="r-c" type="number" min="0" step="1">
    h <input id="r-h" type="number" min="0" step="1">
    <button id="apply">apply</button>
    <button id="stop" class="stop">stop</button>
  </div>
</header>
<div class="grid">
  <div class="card">
    <div class="card-title">Requests per second</div>
    <div class="card-desc">Successful create / insert / count / health requests completed in each 1s window.</div>
    <div class="legend" id="lg-rate"></div><canvas id="ch-rate"></canvas>
  </div>
  <div class="card">
    <div class="card-title">Average latency (ms)</div>
    <div class="card-desc">Mean round-trip time of successful requests in each 1s window.</div>
    <div class="legend" id="lg-avg"></div><canvas id="ch-avg"></canvas>
  </div>
  <div class="card">
    <div class="card-title">Errors per second</div>
    <div class="card-desc">Failed requests per 1s window (HTTP errors, timeouts, connection failures).</div>
    <div class="legend" id="lg-err"></div><canvas id="ch-err"></canvas>
  </div>
  <div class="card" id="card-cpu">
    <div class="card-title">CPU usage</div>
    <div class="card-desc">sqld process CPU across all threads; 100% = one core, 800% = eight cores.</div>
    <div class="legend" id="lg-cpu"></div><canvas id="ch-cpu"></canvas>
  </div>
  <div class="card" id="card-rss">
    <div class="card-title">Memory (RSS)</div>
    <div class="card-desc">Resident set size of the sqld process in MB.</div>
    <div class="legend" id="lg-rss"></div><canvas id="ch-rss"></canvas>
  </div>
  <div class="card" id="card-io">
    <div class="card-title">Syscall I/O</div>
    <div class="card-desc">Bytes sqld read/wrote via read()/write() per second, in MB/s.</div>
    <div class="legend" id="lg-io"></div><canvas id="ch-io"></canvas>
  </div>
</div>
<script>
const WINDOW = 120; // seconds of history shown per chart
const ACTION_KEYS = ["create", "insert", "count", "health"];
const COLORS = { create:"#4f8ef7", insert:"#2dd4bf", count:"#fbbf24", health:"#94a3b8", cpu:"#f472b6", rss:"#a78bfa", io_r:"#34d399", io_w:"#f87171" };
let state = null;

function fmtNum(v) {
  if (v >= 100) return v.toFixed(0);
  if (v >= 10) return v.toFixed(1);
  return v.toFixed(2);
}

function buildLegend(containerId, items) {
  const el = document.getElementById(containerId);
  el.innerHTML = "";
  for (const it of items) {
    const d = document.createElement("span");
    d.className = "item";
    d.innerHTML = '<span class="dot" style="background:' + it.color + '"></span>' + it.name + ' <b>' + fmtNum(it.latest) + '</b>';
    el.appendChild(d);
  }
}

function drawChart(canvas, seriesList, yLabel, fmt) {
  const dpr = window.devicePixelRatio || 1;
  const cssW = Math.max(canvas.parentElement.clientWidth - 24, 200);
  const cssH = 200;
  canvas.width = Math.round(cssW * dpr);
  canvas.height = Math.round(cssH * dpr);
  canvas.style.width = cssW + "px";
  canvas.style.height = cssH + "px";
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);
  ctx.fillStyle = "#0d0f12";
  ctx.fillRect(0, 0, cssW, cssH);
  const ml = 48, mr = 10, mt = 10, mb = 20;
  const plotW = cssW - ml - mr, plotH = cssH - mt - mb;
  const t0 = state.elapsed - WINDOW;
  let yMin = 0, yMax = 0, any = false;
  for (const s of seriesList) for (const p of s.data) {
    if (p[0] < t0) continue;
    if (p[1] > yMax) yMax = p[1];
    if (p[1] < yMin) yMin = p[1];
    any = true;
  }
  if (!any) yMax = 1;
  if (yMax === yMin) yMax = yMin + Math.max(1, Math.abs(yMin) * 0.1);
  ctx.font = "10px monospace";
  ctx.strokeStyle = "#232830";
  ctx.fillStyle = "#8b93a3";
  for (let i = 0; i <= 4; i++) {
    const y = mt + plotH - (i / 4) * plotH;
    ctx.beginPath(); ctx.moveTo(ml, y); ctx.lineTo(ml + plotW, y); ctx.stroke();
    ctx.fillText(fmt(yMin + (yMax - yMin) * (i / 4)), 4, y + 3);
  }
  for (let i = 0; i <= 4; i++) {
    const x = ml + (i / 4) * plotW;
    ctx.beginPath(); ctx.moveTo(x, mt); ctx.lineTo(x, mt + plotH); ctx.stroke();
    ctx.fillText((t0 + (i / 4) * WINDOW) + "s", x - 10, cssH - 7);
  }
  ctx.fillText(yLabel, 4, mt + 8);
  for (const s of seriesList) {
    ctx.strokeStyle = s.color;
    ctx.lineWidth = 1.6;
    ctx.beginPath();
    let started = false;
    for (const p of s.data) {
      if (p[0] < t0) continue;
      const x = ml + ((p[0] - t0) / WINDOW) * plotW;
      const y = mt + plotH - ((p[1] - yMin) / (yMax - yMin)) * plotH;
      if (!started) { ctx.moveTo(x, y); started = true; } else { ctx.lineTo(x, y); }
    }
    ctx.stroke();
  }
}

function seriesList(prefix) {
  const list = [];
  for (const k of ACTION_KEYS) {
    const data = (state.actions[k] && state.actions[k][prefix]) || [];
    list.push({ name: k, color: COLORS[k], data: data, latest: data.length ? data[data.length - 1][1] : 0 });
  }
  return list;
}

function render() {
  document.getElementById("elapsed").textContent = state.elapsed.toFixed(0) + "s";
  document.getElementById("pool").textContent = state.pool;
  for (const k of ACTION_KEYS) {
    const a = state.actions[k];
    const el = document.getElementById("tot-" + k);
    el.querySelector(".label").textContent = k + " total";
    el.querySelector(".value").textContent = a.total + (a.err ? " (" + a.err + " err)" : "");
  }
  document.getElementById("r-n").value = state.rates.n;
  document.getElementById("r-i").value = state.rates.i;
  document.getElementById("r-c").value = state.rates.c;
  document.getElementById("r-h").value = state.rates.h;

  const sRate = seriesList("rate");
  buildLegend("lg-rate", sRate);
  drawChart(document.getElementById("ch-rate"), sRate, "req/s", fmtNum);

  const sAvg = seriesList("avg");
  buildLegend("lg-avg", sAvg);
  drawChart(document.getElementById("ch-avg"), sAvg, "avg ms", function (v) { return v.toFixed(1); });

  const sErr = seriesList("err_rate");
  buildLegend("lg-err", sErr);
  drawChart(document.getElementById("ch-err"), sErr, "err/s", fmtNum);

  const hasSys = state.sys && state.sys.cpu && state.sys.cpu.length;
  for (const id of ["card-cpu", "card-rss", "card-io"]) {
    document.getElementById(id).style.display = hasSys ? "" : "none";
  }
  if (hasSys) {
    const mk = function (k, color, name) {
      const data = state.sys[k] || [];
      return { name: name, color: color, data: data, latest: data.length ? data[data.length - 1][1] : 0 };
    };
    const cpu = mk("cpu", COLORS.cpu, "cpu%");
    buildLegend("lg-cpu", [cpu]);
    drawChart(document.getElementById("ch-cpu"), [cpu], "% of 1 core", function (v) { return v.toFixed(0); });

    const rss = mk("rss", COLORS.rss, "rss");
    buildLegend("lg-rss", [rss]);
    drawChart(document.getElementById("ch-rss"), [rss], "MB", function (v) { return v.toFixed(0); });

    const ioR = mk("sys_r", COLORS.io_r, "read");
    const ioW = mk("sys_w", COLORS.io_w, "write");
    buildLegend("lg-io", [ioR, ioW]);
    drawChart(document.getElementById("ch-io"), [ioR, ioW], "MB/s", function (v) { return v.toFixed(2); });
  }
}

document.getElementById("apply").addEventListener("click", function () {
  const body = JSON.stringify({
    n: parseFloat(document.getElementById("r-n").value) || 0,
    i: parseFloat(document.getElementById("r-i").value) || 0,
    c: parseFloat(document.getElementById("r-c").value) || 0,
    h: parseFloat(document.getElementById("r-h").value) || 0
  });
  fetch("/api/rate", { method: "POST", body: body });
});
document.getElementById("stop").addEventListener("click", function () {  fetch("/api/stop", { method: "POST" });
  document.getElementById("stop").textContent = "stopping...";
});

async function tick() {
  try {
    const r = await fetch("/api/stats");
    state = await r.json();
    render();
  } catch (e) { /* server gone: stop redrawing */ }
}
tick();
setInterval(tick, 1000);
</script>
</body>
</html>
"""


def open_browser(url):
    """Open the dashboard in the default browser; never blocks or crashes."""
    try:
        threading.Thread(target=webbrowser.open, args=(url,), daemon=True).start()
    except Exception:
        pass


def start_web_server(port, ctx):
    """Serve the live dashboard. `ctx` carries the runtime hooks; returns the
    server so the caller can shut it down."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/", "/index.html"):
                body = DASHBOARD_HTML.encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif self.path == "/api/stats":
                body = json.dumps(ctx["stats"]()).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            if self.path == "/api/rate":
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    updates = json.loads(self.rfile.read(length) or b"{}")
                    updates = {k: float(updates[k]) for k in ("n", "i", "c", "h") if k in updates}
                except Exception:
                    self.send_response(400)
                    self.end_headers()
                    return
                if updates:
                    ctx["set_rates"](updates)
                self.send_response(200)
                self.end_headers()
            elif self.path == "/api/stop":
                ctx["stop"].set()
                self.send_response(200)
                self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server



def main():
    ap = argparse.ArgumentParser(
        description="Benchmark the admin API (namespace create / insert / count)."
    )
    ap.add_argument("--url", default="http://127.0.0.1:3001", help="admin API base URL")
    ap.add_argument("--admin-key", default="admin-key-change-me")
    ap.add_argument("--n", type=float, default=5.0, help="namespace creates per second")
    ap.add_argument("--i", type=float, default=100.0, help="todo inserts per second")
    ap.add_argument("--c", type=float, default=100.0, help="count(*) queries per second")
    ap.add_argument("--h", type=float, default=10.0, help="health pings per second")
    ap.add_argument(
        "--health-url",
        default="http://127.0.0.1:8080/health",
        help="sqld /health endpoint to ping (server liveness, no DB)",
    )
    ap.add_argument("--workers", type=int, default=64, help="threads per action")
    ap.add_argument("--interval", type=float, default=10.0, help="report interval (s)")
    ap.add_argument("--timeout", type=float, default=30.0, help="per-request timeout (s)")
    ap.add_argument(
        "--pid",
        type=int,
        default=None,
        help="optional PID of a LOCAL sqld process to monitor (CPU/RSS/disk I/O); "
        "omit when benchmarking remote servers",
    )
    ap.add_argument(
        "--web-port",
        type=int,
        default=8081,
        help="port for the live web dashboard (0 disables it; open http://127.0.0.1:<port> in a browser)",
    )
    ap.add_argument(
        "--no-browser",
        action="store_true",
        help="do not auto-open the dashboard in the default browser",
    )
    ap.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="stop automatically after this many seconds (0 = run until stopped)",
    )
    ap.add_argument(
        "--seed",
        action="store_true",
        help="seed the namespace pool from existing databases on startup",
    )
    args = ap.parse_args()

    stop = threading.Event()
    pool = []
    pool_lock = threading.Lock()

    if args.seed:
        try:
            req = urllib.request.Request(
                args.url + "/api/databases",
                headers={"Authorization": "Bearer " + args.admin_key},
            )
            with urllib.request.urlopen(req, timeout=args.timeout) as resp:
                data = json.loads(resp.read())
            for rec in data.get("databases", []):
                pool.append(rec["namespace"])
            print(f"seeded namespace pool with {len(pool)} existing database(s)")
        except Exception as e:
            print(f"warning: could not seed pool from {args.url}: {e}")

    actions = [
        Action("create", args.n, args.workers, stop,
               make_create_action(args.url, args.admin_key, args.timeout, pool, pool_lock)),
        Action("insert", args.i, args.workers, stop,
               make_insert_action(args.url, args.admin_key, args.timeout, pool, pool_lock)),
        Action("count", args.c, args.workers, stop,
               make_count_action(args.url, args.admin_key, args.timeout, pool, pool_lock)),
        Action("health", args.h, args.workers, stop,
               make_health_action(args.health_url, args.timeout)),
    ]
    rates = {"n": args.n, "i": args.i, "c": args.c, "h": args.h}

    def set_rates(updates):
        for name, rate in updates.items():
            rates[name] = rate
            actions[{"n": 0, "i": 1, "c": 2, "h": 3}[name]].set_rate(rate)

    proc = ProcMonitor(args.pid) if args.pid else None
    if proc is not None and proc.prev is None:
        print(f"warning: cannot read /proc/{args.pid} (bad pid or no permission); system stats disabled")
        proc = None

    live = Live()
    t_start = time.monotonic()
    threading.Thread(
        target=live_sampler, args=(stop, actions, proc, live, t_start), daemon=True
    ).start()

    web = None
    if args.web_port:
        try:
            web = start_web_server(args.web_port, {
                "stop": stop,
                "set_rates": set_rates,
                "stats": lambda: {
                    "elapsed": time.monotonic() - t_start,
                    "rates": dict(rates),
                    "pool": len(pool),
                    "actions": {
                        a.name: {
                            "total": a.stats.snapshot()[0],
                            "err": a.stats.snapshot()[1],
                            "skip": a.stats.snapshot()[2],
                            **live.snapshot()["actions"][a.name],
                        }
                        for a in actions
                    },
                    "sys": live.snapshot()["sys"] if proc else {},
                },
            })
            print(f"live dashboard:  http://127.0.0.1:{args.web_port}")
            if not args.no_browser:
                open_browser(f"http://127.0.0.1:{args.web_port}")
        except OSError as e:
            print(f"warning: could not start web dashboard on port {args.web_port}: {e}")

    print(
        f"benchmarking {args.url}  rates: create={args.n}/s insert={args.i}/s count={args.c}/s "
        f"health={args.h}/s  "
        f"(report every {args.interval:g}s; type 'n=10 i=50 c=20 h=10' on stdin to change rates, 'q' to quit)"
    )

    def input_loop():
        for line in sys.stdin:
            line = line.strip().lower()
            if not line:
                print("current rates:", ", ".join(f"{k}={v:g}" for k, v in rates.items()))
                continue
            if line in ("q", "quit", "exit"):
                stop.set()
                return
            tokens = line.replace("=", " ").split()
            updates = {}
            ok = True
            try:
                for k, v in zip(tokens[::2], tokens[1::2]):
                    if k not in ("n", "i", "c", "h"):
                        ok = False
                        break
                    updates[k] = float(v)
            except (ValueError, IndexError):
                ok = False
            if not ok or not updates:
                print("usage: n=<rate> i=<rate> c=<rate> h=<rate>   (0 pauses an action, 'q' quits)")
                continue
            set_rates(updates)
            print("rates set:", ", ".join(f"{k}={v:g}" for k, v in rates.items()))

    threading.Thread(target=input_loop, daemon=True).start()

    prev = {a.name: a.stats.snapshot() for a in actions}
    deadline = t_start + args.duration if args.duration > 0 else None
    while not stop.is_set():
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            stop.wait(min(args.interval, remaining))
        else:
            stop.wait(args.interval)
        if stop.is_set():
            break
        now = time.monotonic()
        parts = [
            format_action(a.name, a.stats.snapshot(), prev[a.name], args.interval)
            for a in actions
        ]
        if proc is not None:
            parts.append(format_system(proc.sample(), args.interval))
        print(f"[t+{now - t_start:6.1f}s] " + "  |  ".join(parts))
        for a in actions:
            prev[a.name] = a.stats.snapshot()

    for a in actions:
        a.pacer.wake()
    for a in actions:
        join_threads(a.threads, timeout=2)
    if web is not None:
        web.shutdown()
    elapsed = time.monotonic() - t_start

    print("\n=== final summary ===")
    print(f"run duration: {elapsed:.1f}s   namespace pool size: {len(pool)}")
    for a in actions:
        ok, err, skip, lat_sum, lat_min, lat_max = a.stats.snapshot()
        avg = lat_sum / ok if ok else 0.0
        rate = ok / elapsed if elapsed else 0.0
        print(
            f"{a.name:6s}: total={ok} ({rate:.1f}/s) avg={avg:.2f}ms "
            f"min={lat_min:.2f}ms max={lat_max:.2f}ms err={err} skip={skip}"
        )
    if proc is not None and proc.prev is not None:
        avg_cpu = sum(proc.cpu_samples) / len(proc.cpu_samples) if proc.cpu_samples else 0.0
        rss_mb = proc.prev["rss_kb"] / 1024.0 if proc.prev["rss_kb"] is not None else None
        rss_s = f"{rss_mb:.0f}MB" if rss_mb is not None else "?"
        mib = 1024 * 1024
        print(
            f"system: avg_cpu={avg_cpu:.1f}% final_rss={rss_s} "
            f"block_io_r={proc.io_r_total / mib:.1f}MB block_io_w={proc.io_w_total / mib:.1f}MB "
            f"syscall_r={proc.sys_r_total / mib:.1f}MB syscall_w={proc.sys_w_total / mib:.1f}MB "
            f"(syscall_w avg {proc.sys_w_total / mib / elapsed:.2f}MB/s)"
        )


if __name__ == "__main__":
    main()
