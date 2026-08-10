# Admin API Benchmark Suite

Reproducible benchmark suite for the libsql-server platform admin
API (`POST /api/databases`, `POST /api/query`). Used to compare sqld builds and
deployments over time.

## Workload

Three/four concurrent, independently paced actions (all against the admin API,
auth: `Authorization: Bearer <admin-key>`):

| action | rate flag | request |
|---|---|---|
| create | `--n` | `POST /api/databases {id}` + `CREATE TABLE IF NOT EXISTS todos` (timing covers both: time-to-ready namespace) |
| insert | `--i` | `POST /api/query` → `INSERT INTO todos (title) VALUES (?)` on a random namespace |
| count | `--c` | `POST /api/query` → `SELECT COUNT(*) FROM todos` on a random namespace |
| health | `--h` | `GET <health-url>` (server liveness ping) |

Namespaces are chosen uniformly at random from the pool. The pool is seeded
from `GET /api/databases` with `--seed`; the create action also grows it.

## Tooling

- `tools/admin_api_bench.py` — the load generator (stdlib only: threads +
  urllib, drift-free pacer). Options:
  `--url --admin-key --n --i --c --h --health-url --workers --interval
  --timeout --pid --web-port --no-browser --seed --duration`
- Reports every `--interval` s (default 10): accumulated total, per-second
  rate, avg/min/max latency, error count. `--pid <local-pid>` adds a `sys:`
  section (CPU%, RSS, block + syscall I/O via /proc). Final summary on exit.
- Live web dashboard (`http://127.0.0.1:8081`, default): req/s, avg latency,
  err/s, CPU, RSS, I/O charts + live rate changes and stop button.
- `tools/delete_all_namespaces.py` — wipe all namespaces via the admin API
  (`--dry-run` to preview).

## Running

```bash
# 1. start sqld (dev.sh or podman; admin API on 3001)
# 2. benchmark (server ceiling test):
python3 tools/admin_api_bench.py --url http://127.0.0.1:3001 \
  --n 0 --i 5000 --c 0 --h 0 --workers 256 --seed \
  --pid <sqld-pid> --duration 60   # --pid optional (local only)
```

Change rates live: stdin `n=10 i=50 c=20 h=10`, empty line prints rates,
`q` quits. `--duration` auto-stops.

## Metrics

Per-10s and final:
- achieved rate (req/s), avg/min/max latency, error count per action
- `sys:` CPU% (percent of one core, process-wide), RSS, block I/O,
  syscall read/write MB/s — only with `--pid`

Error taxonomy (observed):
- `404 {"error":"Namespace ... doesn't exist"}` — eviction race, see
  erfanium/libsql issue #1 (first query after cache eviction 404s)
- `500 {"error":"Timed out while opening database connection"}` — connection
  throttle semaphore exhausted, `DB_CREATE_TIMEOUT` = 1s
- `500 {"error":"Too many concurrent requests"}` — throttle waiter cap (128)

## Diagnostic endpoints

- `GET /api/queries` — live per-query registry: state (running/blocked),
  current step, elapsed, per-thread cumulative CPU. Use to distinguish
  lock-wait vs CPU vs connection-create time.
- fio for the backing disk (see table below for reference numbers).

## Reference results

Environment key: **L1** = dev laptop (20 cores, NVMe, fsync ~0.1ms, debug
build); **VM1** = 4 cores, "normal SSD" (fsync 18.6ms, 4k randread 130 IOPS,
fsync'd 4k randwrite 2 IOPS — HDD-class); **VM2** = 4 cores, fast disk
(4k randread 654k IOPS, fsync ~0.01ms); **pod** = production pod.

### 256 namespaces, i=1000, 256 workers (release builds unless noted)

| env | inserts/s achieved | avg latency | server CPU | notes |
|---|---|---|---|---|
| L1 (release) | 1000 (client-capped) | 1.6ms | ~1 core | target held exactly |
| VM1 | ~250 | ~400ms | 1.4 cores | fsync-bound, ~15% errors |
| VM2 | ~800 | ~330ms | 1.8 cores | throttle-semaphore-bound |
| pod | ~430 | ~377ms | 100% (capped) | CPU-capped + slow disk |

### Uncapped (i=5000, 256 workers, local, 30s) — connection-cache A/B

| build | inserts/s (r1/r2) | avg latency | CPU | CPU per insert |
|---|---|---|---|---|
| baseline (no cache) | 2604 / 2414 | 153–164ms | ~1291–1437% | ~0.54% |
| + per-namespace conn cache, best-effort (miss → new conn) | 3322 / 3413 | ~112ms | ~414–508% | ~0.14% |
| + per-namespace conn cache, blocking (wait for cached conn) | **4080 / 4171** | **~84ms** | **~110%** | **~0.027% (20x vs no-cache)** |

Blocking cache = exactly one connection per namespace via the admin query
API: the first request creates it (through the throttle), every other
request waits for it to be released. Eliminates open/close churn and the
malloc/futex contention it caused; the server runs on ~1 core instead of ~13.

Configuration used for the A/B: `SQLD_MAX_ACTIVE_NAMESPACES=512`,
`SQLD_MAX_CONCURRENT_CONNECTIONS=1024`, `ulimit -n 65535` (cached connections
hold open file descriptors — the default 1024 fd limit will exhaust).

### Disk reference (fio, 4k, on the test VMs)

| metric | VM1 | VM2 |
|---|---|---|
| seq read / write | 260 / 114 MB/s | 29.5 GB/s / 6.7 GB/s (direct) |
| 4k randread (depth 1) | 130 IOPS, 7.6ms | 654k IOPS, ~1µs |
| 4k randwrite + fsync=1 | 2 IOPS, ~500ms | 247k IOPS, ~4µs |
| fsync | 18.6ms | ~0.01ms |

fsync latency is the dominant insert cost: every commit syncs the replication
log (`libsql-server/src/replication/primary/logger.rs` `sync_data()`).

## Reproducing the A/B

```bash
# build both binaries
cargo build --release -p libsql-server          # cached (working tree)
git stash -u && cargo build --release -p libsql-server && cp target/release/sqld /tmp/sqld-baseline && git stash pop
cargo build --release -p libsql-server          # restore cached build

# per binary: fresh data dirs, 256 namespaces + DDL (see /tmp/opencode/prep_ns.py),
# then:
python3 tools/admin_api_bench.py --url http://127.0.0.1:3002 \
  --n 0 --i 5000 --c 0 --h 0 --workers 256 --seed --pid <pid> --duration 25
```
