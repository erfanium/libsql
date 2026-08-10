#!/usr/bin/env python3
"""Benchmark insert throughput and latency against an external sqld admin API.

Creates (idempotently) a set of namespaces on the target server, then hammers
`INSERT` statements into random namespaces with fixed concurrency and reports
achieved inserts/s and insert-latency percentiles (p50/p90/p99/p999).

Each worker holds one persistent keep-alive TCP connection, so the benchmark
measures server throughput rather than TCP connection churn.

Usage:
  python3 tools/sqld_insert_bench.py --url http://<host>:3001 \
      --admin-key <key> --namespaces 256 --concurrency 64 --duration 30

Only the Python standard library is used.
"""

import argparse
import http.client
import json
import math
import random
import secrets
import socket
import threading
import time
import urllib.error
import urllib.parse

DDL = (
    "CREATE TABLE IF NOT EXISTS todos "
    "(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL)"
)
INSERT_SQL = "INSERT INTO todos (title) VALUES (?)"

#: Connection-level failures worth transparently reconnecting for (the server
#: may have closed an idle keep-alive connection, or a proxy timed it out).
RECONNECT_ERRORS = (
    http.client.RemoteDisconnected,
    http.client.BadStatusLine,
    ConnectionResetError,
    ConnectionAbortedError,
    BrokenPipeError,
)


class AdminClient:
    """Admin API client with a single persistent keep-alive connection."""

    def __init__(self, base, admin_key, timeout):
        parsed = urllib.parse.urlsplit(base)
        self.scheme = parsed.scheme
        self.host = parsed.hostname
        self.port = parsed.port or (443 if self.scheme == "https" else 80)
        self.admin_key = admin_key
        self.timeout = timeout
        self.conn = None

    def _connect(self):
        cls = (
            http.client.HTTPSConnection
            if self.scheme == "https"
            else http.client.HTTPConnection
        )
        self.conn = cls(self.host, self.port, timeout=self.timeout)

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _request(self, method, path, payload=None):
        if self.conn is None:
            self._connect()
        body = None
        headers = {"Authorization": "Bearer " + self.admin_key}
        if payload is not None:
            body = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        self.conn.request(method, path, body=body, headers=headers)
        resp = self.conn.getresponse()
        data = resp.read()
        if resp.status >= 400:
            raise urllib.error.HTTPError(path, resp.status, resp.reason, resp.headers, None)
        return resp.status, data

    def post(self, path, payload):
        status, _ = self._request("POST", path, payload)
        return status

    def get_json(self, path):
        _, data = self._request("GET", path)
        return json.loads(data)

    def request(self, method, path, payload=None):
        """Like `post`, but transparently reconnects once on a dead connection."""
        try:
            return self._request(method, path, payload)
        except RECONNECT_ERRORS:
            self.close()
            return self._request(method, path, payload)


def ensure_namespaces(base, admin_key, count, timeout, seed_workers=16):
    """Return `count` namespace ids, creating databases + DDL if missing."""
    probe = AdminClient(base, admin_key, timeout)
    try:
        if count > 0:
            ids = [f"ns{i:04d}" for i in range(count)]
        else:
            data = probe.get_json("/api/databases")
            ids = [d["id"] for d in data.get("databases", [])]
    finally:
        probe.close()
    if not ids:
        raise SystemExit("no namespaces to benchmark (--namespaces 0 found none)")

    def ensure(client, nid):
        try:
            client.post("/api/databases", {"id": nid})
        except urllib.error.HTTPError as e:
            # already exists is fine (we want idempotent seeding)
            if e.code < 400 or e.code >= 500:
                raise
        client.post("/api/query", {"namespace": nid, "sql": DDL})

    pending = list(ids)
    done, errors = 0, []
    lock = threading.Lock()

    def work():
        nonlocal done
        client = AdminClient(base, admin_key, timeout)
        try:
            while True:
                with lock:
                    if not pending:
                        return
                    nid = pending.pop()
                try:
                    ensure(client, nid)
                except Exception as e:
                    errors.append((nid, str(e)))
                with lock:
                    done += 1
        finally:
            client.close()

    threads = [
        threading.Thread(target=work, daemon=True) for _ in range(min(seed_workers, len(ids)))
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        print(f"seed errors: {len(errors)} (e.g. {errors[0]})")
    print(f"namespaces ready: {done}/{len(ids)}")
    return ids


def percentile(sorted_lat, p):
    """Nearest-rank percentile over a sorted list of seconds."""
    if not sorted_lat:
        return 0.0
    idx = min(len(sorted_lat) - 1, max(0, int(math.ceil(p * len(sorted_lat))) - 1))
    return sorted_lat[idx] * 1000.0  # ms


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://127.0.0.1:3001", help="admin API base URL")
    ap.add_argument("--admin-key", default="admin-key-change-me")
    ap.add_argument("--namespaces", type=int, default=256,
                    help="namespaces to create and benchmark (0 = use existing ones)")
    ap.add_argument("--concurrency", type=int, default=64, help="concurrent insert workers")
    ap.add_argument("--duration", type=float, default=30.0, help="measurement duration (s)")
    ap.add_argument("--warmup", type=float, default=2.0, help="discarded warmup time (s)")
    ap.add_argument("--interval", type=float, default=5.0, help="progress report interval (s)")
    ap.add_argument("--timeout", type=float, default=30.0, help="per-request timeout (s)")
    args = ap.parse_args()

    base = args.url.rstrip("/")
    print(f"benchmarking {base}  namespaces={args.namespaces} concurrency={args.concurrency} duration={args.duration}s")

    ids = ensure_namespaces(base, args.admin_key, args.namespaces, args.timeout)

    stop = threading.Event()
    latencies = []  # seconds, appended by workers
    errors = []
    errors_lock = threading.Lock()

    def worker():
        client = AdminClient(base, args.admin_key, args.timeout)
        try:
            while not stop.is_set():
                ns = random.choice(ids)
                payload = {
                    "namespace": ns,
                    "statements": [
                        {"q": INSERT_SQL, "params": ["todo-" + secrets.token_hex(4)]}
                    ],
                }
                t0 = time.perf_counter()
                try:
                    client.request("POST", "/api/query", payload)
                    latencies.append(time.perf_counter() - t0)
                except Exception as e:
                    with errors_lock:
                        errors.append(str(e))
                    if len(errors) > 1000:
                        break
        finally:
            client.close()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(args.concurrency)]

    # warmup (runs load but discards samples)
    for t in threads:
        t.start()
    time.sleep(args.warmup)
    latencies.clear()

    measure_start = time.monotonic()
    deadline = measure_start + args.duration
    next_report = time.monotonic() + args.interval
    while time.monotonic() < deadline:
        if time.monotonic() >= next_report:
            elapsed = time.monotonic() - measure_start
            n = len(latencies)
            print(f"t+{elapsed:6.1f}s  rate={n / elapsed:8.1f}/s  total={n}  err={len(errors)}")
            next_report = time.monotonic() + args.interval
        time.sleep(0.05)
    stop.set()
    for t in threads:
        t.join(timeout=args.timeout + 5)

    elapsed = time.monotonic() - measure_start
    n = len(latencies)
    rate = n / elapsed if elapsed > 0 else 0.0
    sorted_lat = sorted(latencies)

    print("\n=== final summary ===")
    print(f"run duration: {elapsed:.1f}s   namespaces: {len(ids)}   concurrency: {args.concurrency}")
    print(f"inserts: total={n} ({rate:.1f}/s)   errors: {len(errors)}")
    if sorted_lat:
        avg = sum(sorted_lat) / len(sorted_lat) * 1000.0
        print(f"latency: avg={avg:.2f}ms  min={sorted_lat[0] * 1000:.2f}ms  "
              f"p50={percentile(sorted_lat, 0.5):.2f}ms  p90={percentile(sorted_lat, 0.9):.2f}ms  "
              f"p99={percentile(sorted_lat, 0.99):.2f}ms  p999={percentile(sorted_lat, 0.999):.2f}ms  "
              f"max={sorted_lat[-1] * 1000:.2f}ms")
    if errors:
        print(f"error samples: {errors[:5]}")


if __name__ == "__main__":
    main()
