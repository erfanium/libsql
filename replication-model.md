# libSQL Embedded Replica & Replication Model

A deep dive into the whole-replication machinery, written from reading the code
(`libsql-replication/`, `libsql-server/src/replication/`, `libsql/src/replication/`).

---

## 1. The core unit: a "Frame"

Everything in libSQL replication revolves around **frames**. A frame is a single
SQLite page (4096 bytes) plus a 24-byte header (`libsql-replication/src/frame.rs`):

```
FrameHeader (24 bytes):
  frame_no  : u64   # monotonically increasing, global replication index
  checksum  : u64   # rolling CRC-64 of all pages so far (corruption detection)
  page_no   : u32   # which SQLite page number this is
  size_after: u32   # database size in pages AFTER this txn; !=0 marks "commit frame"
page (4096 bytes)
```

Total frame size = 4120 bytes (`LogFile::FRAME_SIZE`, `libsql-server/src/replication/primary/logger.rs:101`).

The **commit boundary** is the important bit: SQLite writes pages to the WAL one
statement at a time, but only the *last* page of a transaction carries
`size_after != 0`. Frames therefore stream as "lots of pages, then one commit
frame". `size_after` doubles as the transaction commit signal for replicas.

---

## 2. Files stored per namespace

### On the PRIMARY server
Everything lives in `<base_path>/dbs/<namespace>/`
(`libsql-server/src/namespace/configurator/primary.rs:131`):

| File | Purpose |
|---|---|
| `data` | The primary's real SQLite database file |
| `data-wal`, `data-shm` | SQLite's own WAL + shared-memory files for `data` |
| `wallog` | **The replication log** — a copy of every page written, in frame format |
| `snapshots/` | Snapshot files: `<log_id>-<start_fno>-<end_fno>.snap` |
| `to_compact/` | Old `wallog` files waiting to be turned into snapshots |
| `tmp/` | Scratch dir for building snapshots |
| `.sentinel` | Crash marker for dirty-recovery |

### On a REPLICA

Two flavors, same idea:

- **Server-side replica** (a `sqld`/libsql-server configured to replicate from
  another primary), at `<base>/dbs/<ns>/`:
  - `data`, `data-wal`, `data-shm` — the local SQLite db + its WAL (the replicated copy)
  - `client_wal_index` — the **meta file** tracking which frame the replica has committed (`libsql-replication/src/meta.rs:70`)

- **Client-side embedded replica** (the `libsql` crate, `Builder::new_remote_replica`), with db path `foo.db`:
  - `foo.db` — the local SQLite database
  - `foo.db-wal`, `foo.db-shm` — its SQLite WAL/SHM
  - `foo.db-client_wal_index` — the meta file, *prefixed* next to the db (`meta.rs:86-92`)

The meta file (`WalIndexMetaData`, 32 bytes: `log_id` u128 + `committed_frame_no`
u64 + padding) is the replica's durable "checkpoint": after every applied
transaction it is rewritten so that on crash the replica can resume from exactly
`committed_frame_no + 1`.

---

## 3. What the "wallog" is

`wallog` is a custom, append-only, page-level WAL owned by the **primary**
(`LogFile`, `logger.rs:63`). Layout:

```
LogFileHeader (magic "SQLDWAL\0", start_checksum, log_id, start_frame_no, frame_count, version=2, page_size, sqld_version)
frame [start_frame_no]       # 4120 bytes
frame [start_frame_no+1]
...
```

- Frames are written sequentially; a rolling CRC-64 over page bytes is kept in each frame header.
- `push_page` writes a frame but does **not** update the header. `commit()` first
  `fsync`s frame data, then bumps `frame_count`, then syncs the header — so a
  crash never leaves the header claiming more frames than exist.
- `rollback()` just resets the uncommitted count and checksum, so
  unwritten/aborted pages can be overwritten.
- Frame numbers are **globally increasing across compactions**: after a
  compaction `start_frame_no` jumps to the last committed frame + 1.

---

## 4. Write path on the primary (how frames get into wallog)

The primary runs SQLite with its WAL wrapped by `ReplicationLoggerWalWrapper`
(`libsql-server/src/replication/primary/replication_logger_wal.rs`) — a `WrapWal`
that sits *in front of* the normal SQLite3Wal:

1. App writes `INSERT ...`. SQLite's pager accumulates dirty pages.
2. On each flush, SQLite calls the WAL method
   `insert_frames(page_headers, size_after, is_commit, ...)`. The wrapper
   intercepts it, converts each page into a `WalPage`, and **buffers** them.
3. At the end of a statement it calls `flush(size_after)`: it stamps the last
   buffered page with `size_after` and calls `logger.write_pages(...)` —
   appending all pages to `wallog` (uncommitted).
4. It then delegates to the real SQLite WAL (`wrapped.insert_frames`), so the
   primary's own `data-wal` is written normally.
5. On commit (`is_commit`): `logger.commit()` bumps `wallog`'s `frame_count` and
   fires `new_frame_notifier` (a `tokio::sync::watch` channel). Replicas
   subscribed to that channel are woken up.
6. If `wallog` is due for compaction, `maybe_compact()` runs right here too.

The net effect: **every committed transaction on the primary produces a
contiguous, append-only run of frames in `wallog`**, mirrored in SQLite's own WAL.

---

## 5. The replication protocol (RPC)

`libsql-replication/proto/replication_log.proto`, served by
`ReplicationLogService` (`libsql-server/src/rpc/replication/replication_log.rs`):

- **`Hello`** → `HelloResponse`: `log_id`, `session_token`,
  `current_replication_index`, `generation_id`, and the db `config`. The session
  token is a per-server-restart secret; replicas must echo it on later requests
  (`NO_HELLO` otherwise).
- **`LogEntries(LogOffset{next_offset})`** → stream of frames from `next_offset`
  onward. Implemented by `FrameStream` (`libsql-server/src/replication/primary/frame_stream.rs`):
  - offset < log's `start_frame_no` → **`SnapshotRequired`** (mapped to the `NEED_SNAPSHOT` gRPC error)
  - offset > last frame → waits on `new_frame_notifier` for more commits (`Ahead` if not waiting)
  - each committed frame carries its commit timestamp (for latency metrics)
- **`BatchLogEntries`** → same, but capped at 1024 frames (used by embedded
  clients for efficient prefetch).
- **`Snapshot(LogOffset)`** → streams a snapshot file (frames in **reverse** frame_no order).

The replica (`Replicator`, `libsql-replication/src/replicator.rs`) is a simple state machine:

```
NeedHandshake ──handshake()──▶ NeedFrames ──frames streamed──▶ (commit)
     ▲                              │  NEED_SNAPSHOT
     └────── retry/rollback ◀───────┘        ▼
                                        NeedSnapshot ──snapshot()──▶ NeedFrames
```

Every error path does `client.rollback()` + `injector.rollback()` and retries.

---

## 6. Replica lifecycle in detail

### Server-side replica
1. **Open meta** (`client_wal_index`). If a db exists but no meta →
   `RequiresCleanDatabase` (prevents silent corruption) (`meta.rs:96`).
2. **Handshake** with primary; validate `log_id` matches the meta. Mismatch
   (`LogIncompatible`) → wipe the namespace dir and re-create from scratch
   (`libsql-server/src/namespace/configurator/replica.rs:91-109`).
3. **Sync loop** (`replicator.run()` in `configurator/replica.rs:121`):
   repeatedly call `LogEntries` from `committed_frame_no + 1`, stream frames into
   the **injector**.
4. When a frame has `size_after != 0` (commit), the injector returns the commit
   frame_no; the replica writes it to `client_wal_index` (`set_commit_frame_no`)
   and updates the `new_frame_receiver` watch channel.
5. Reads on the replica go through the local SQLite db
   (`PassthroughWalWrapper`), but writes are proxied to the primary via
   `WriteProxyConnection`. A read that needs a newer frame than locally applied
   uses `wait_for_frame_no` to block until the sync catches up.
6. If the primary has compacted past our position → `NEED_SNAPSHOT` → fetch the
   snapshot and apply it (reverse order, then commit).

### Client-side embedded replica (the `libsql` crate)
Same `Replicator`/injector machinery, but driven on demand:
- `sync()` / `sync_oneshot()` forces a fresh handshake, then pulls
  `BatchLogEntries` batches and applies them
  (`libsql/src/replication/mod.rs:276`).
- For efficiency it **prefetches** a batch concurrently with the handshake
  (`libsql/src/replication/remote_client.rs`).
- `sync_until(frame_no)` loops until the local index catches up.

---

## 7. The injector: how frames become a local SQLite database

`SqliteInjector` (`libsql-replication/src/injector/sqlite_injector/mod.rs`) is the
trickiest part. It opens a real SQLite connection but with a custom WAL manager
(`InjectorWalManager`/`InjectorWal`). To apply incoming frames it doesn't execute
SQL — it *forges* WAL inserts:

1. Incoming frames are buffered (`FrameBuffer`, up to `capacity`, default 10).
2. To flush: it starts `BEGIN IMMEDIATE`, creates a dummy table
   `libsql_temp_injection` (never persisted — it's rolled back), then runs
   `INSERT INTO libsql_temp_injection VALUES (42)` + `cache_flush()`.
3. That forces SQLite's pager to call `insert_frames` on the injector's WAL.
   `InjectorWal::insert_frames` **replaces** the pager's real pages with the
   buffered replication frames (`injector_wal.rs:145-187`) — it builds fake
   `PgHdr` page headers pointing at the frame data and hands them to the *inner*
   real SQLite3 WAL, which writes them into `data-wal`.
4. If the last frame had `size_after != 0`, the injector commits (`writable_schema`
   is toggled so schema changes like `CREATE TABLE` apply even though the schema
   cache might be stale) and returns the commit frame_no.
5. Otherwise it stays "in txn" (`LIBSQL_INJECT_OK_TXN`), waiting for the rest of
   the transaction's frames.

Result: the replica's `data-wal` ends up byte-for-byte equivalent to the
primary's pages. When the local WAL grows past `auto_checkpoint` (default 1000
pages), SQLite checkpoints it into the `data` file, exactly as in a normal
SQLite database.

---

## 8. Snapshots — what they are

A **snapshot** is a compact, portable point-in-time dump of the database, built
from frames:

```
SnapshotFileHeader (48 bytes): log_id, start_frame_no, end_frame_no, frame_count, size_after
frames in DECREASING frame_no order, one page per page_no (deduplicated)
```

Construction (`SnapshotBuilder.append_frames`, `libsql-server/src/replication/snapshot.rs:470`):
walk the log **backwards** (newest → oldest), and keep only the **first**
occurrence of each `page_no` (that's the newest version). Every frame's
`size_after` is forced to 0; when the server streams a snapshot to a replica it
sets `size_after` on the *last* frame streamed so the replica treats the whole
snapshot as one commit unit (`replication_log.rs:432`).

So a snapshot ≈ "the final state of every page, at log position `end_frame_no`,
as frames". The replica applies them in reverse order and ends up with a full
database at that point.

File name encodes coverage: `<log_id>-<start_frame_no>-<end_frame_no>.snap`.

There's also a newer, separate `SnapshotStore`
(`libsql-server/src/replication/snapshot_store.rs`) with its own metadata SQLite
db (`snapshot-store/snapshots/...`) — currently `#![allow(dead_code)]`, i.e. not
wired into the primary path yet. The active path reads the `snapshots/` dir via
`find_snapshot_file`.

---

## 9. When does compaction happen?

There are **two levels** of compaction.

### Level 1: `wallog` → snapshot
Triggered when `should_compact()` is true (`logger.rs:342`):
- `frame_count > max_log_frame_count` (derived from `--max-log-size`, default
  **200 MB** → ~48.5k frames), **or**
- more than `max_log_duration` elapsed since last compaction, **and**
- no uncommitted frames (only between transactions).

It's checked in two places:
- after every commit in the WAL wrapper (`replication_logger_wal.rs:69-77`), and
- every 1 second by `run_periodic_compactions` (`helpers.rs:275`).

How (`do_compaction`, `logger.rs:361`):
1. Create a brand-new empty log in `to_compact/<uuid>` with
   `start_frame_no = last_frame_no + 1` and `start_checksum = committed checksum`.
2. **Atomically swap** the new log into `wallog` (`renameat2 RENAME_EXCHANGE` on Linux).
3. Send the old log to the background **compactor** task, which turns it into a
   snapshot, registers it with the merger, then deletes the old log file (`snapshot.rs:115`).

### Level 2: snapshot → bigger snapshot
The `SnapshotMerger` (`snapshot.rs:272`) triggers when snapshots consume too much space:
- total snapshot frames ≥ **2×** database page count
  (`SNAPHOT_SPACE_AMPLIFICATION_FACTOR`), **or**
- more than **32** snapshots exist.

It then merges all snapshots into one by reading them newest-first and
deduplicating pages (`merge_snapshots`, `snapshot.rs:356`), deleting the originals.

---

## 10. When are files purged?

- **Old `wallog`** → deleted in the compactor right after its snapshot is
  successfully built (`snapshot.rs:115`). Crash-safety: the old file is swapped
  away first, so it survives as `to_compact/<uuid>` and is re-processed on next
  startup (`pending_snapshots_list`).
- **Merged-away snapshots** → deleted in `merge_snapshots` (`snapshot.rs:393`).
- **Empty/leftover `to_compact` logs** → deleted at startup (`snapshot.rs:140`).
- **`snapshots/` and `to_compact/`** → wiped entirely during dirty-log recovery (`logger.rs:697-702`).
- **Entire namespace** → `cleanup_primary` / replica cleanup removes the whole
  `dbs/<ns>/` dir on namespace deletion.

---

## 11. Recovery (the dirty path)

The `.sentinel` file tracks clean shutdown (`libsql-server/src/namespace/configurator/helpers.rs:61`):
- On startup, if `.sentinel` exists → the log is **dirty** (crash happened) →
  `ReplicationLogger::open` takes the `recover()` path (`logger.rs:608, 685`):
  checkpoint the SQLite `data-wal` into `data`, truncate+reset `wallog` (fresh
  header, new `log_id`), delete `snapshots/` + `to_compact/`, then **rebuild the
  entire log from the `data` file** page-by-page (one frame per page, last page
  as commit).
- `.sentinel` is created on open and removed on graceful shutdown (`libsql-server/src/namespace/mod.rs:94`).

Recovery also triggers if the `wallog` version is incompatible or the wallog is
missing but `data` exists.

---

## 12. Whole-lifecycle summary

**Primary:** app write → SQLite pager → `ReplicationLoggerWalWrapper` → pages
appended to `wallog` + notified → (periodically) `wallog` compacted into
`snapshots/*.snap`, old log purged → (when big) snapshots merged, originals purged.

**Replica:** handshake (validates `log_id`) → stream frames from
`committed_frame_no + 1` → injector forges WAL inserts into local `data-wal` →
commit boundary persists `client_wal_index` → local SQLite checkpoints WAL into
`data` as it grows. If primary compacted past us → download snapshot, apply in
reverse, commit. On crash → resume from `committed_frame_no + 1`; if `log_id`
changed (primary recovered) → `LogIncompatible` → reset from scratch.

**Consistency model** (`docs/CONSISTENCY_MODEL.md`): primary is linearizable;
replica reads are monotonic; a process always sees its own writes; two processes
on a replica may momentarily see different points in time.

---

## 13. Worked example — real numbers end to end

Everything above is abstract; here is one concrete story with actual values, so you
can see how `frame_no`, `page_no`, `size_after`, file offsets and the
`client_wal_index` all line up.

### 13.1 The stage

- Page size = **4096 bytes** (`LIBSQL_PAGE_SIZE`).
- Frame size = 24-byte `FrameHeader` + 4096-byte page = **4120 bytes** (`LogFile::FRAME_SIZE`).
- `LogFileHeader` = **64 bytes** (magic 8 + start_checksum 8 + log_id 16 + start_frame_no 8 + frame_count 8 + version 4 + page_size 4 + sqld_version 8).
- `SnapshotFileHeader` = **48 bytes**.
- Fresh namespace `users`. Primary creates `wallog`:

```
wallog, byte 0..63:
  magic          = "SQLDWAL\0"     # 0x4C415744514C4453 LE
  start_checksum = 0
  log_id         = 2f1c9a…c3d4      # random UUID, also served in Hello()
  start_frame_no = 0
  frame_count    = 0
  version        = 2
  page_size      = 4096
  sqld_version   = 0.10.0
```

An empty WAL database has exactly **1 page** (page 1 = the header page).

### 13.2 T1 — `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`

SQLite allocates page 2 as the table root and updates page 1 (header / schema).
The pager flushes both, then commits. The WAL wrapper assigns frames in order:

| frame_no | page_no | size_after | checksum (rolling over page bytes) | meaning |
|---|---|---|---|---|
| 0 | 1 | 0 | `0xA3F1…` (CRC of page 1 from initial 0) | header page, not commit |
| 1 | 2 | 2 | `0xB72C…` | **commit T1** — db is now 2 pages |

After `commit()`: `wallog.frame_count = 2`, last frame_no = **1**.
Replica applies both frames, then writes `client_wal_index.committed_frame_no = 1`.

### 13.3 T2 — `INSERT INTO users (name) VALUES ('alice'), ('bob')`

Both rows fit on the table root (page 2). One statement = one page write + commit:

| frame_no | page_no | size_after | checksum | meaning |
|---|---|---|---|---|
| 2 | 2 | 0 | `0xC840…` | page 2 now has alice+bob |
| 3 | 2 | 2 | `0xD951…` | **commit T2** |

`committed_frame_no = 3`. Note: `size_after` stayed 2 — the db did not grow.

### 13.4 T3 — `BEGIN; INSERT carol; INSERT dave; COMMIT`

The two inserts overflow page 2, so SQLite **splits** it: page 2 becomes an
interior (directory) node, fresh leaf pages 3 and 4 are allocated. Every pager
flush becomes a frame; only the very last carries `size_after`:

| frame_no | page_no | size_after | checksum | meaning |
|---|---|---|---|---|
| 4 | 2 | 0 | `0xE260…` | carol written to page 2 |
| 5 | 3 | 0 | `0xF370…` | split: new leaf page 3 |
| 6 | 2 | 0 | `0x0480…` | page 2 rewritten as interior node |
| 7 | 4 | 0 | `0x1590…` | dave written to new leaf page 4 |
| 8 | 2 | 4 | `0x26A0…` | **commit T3** — db is now 4 pages |

`committed_frame_no = 8`. Note the invariant: **every page_no ≤ size_after**
(a page number can never exceed the database size).

> Why does `size_after` appear on frame 8 (page 2) and not on the last *new* page?
> Because SQLite stamps the commit onto the *last flushed page of the transaction*,
> whatever it is. The replica doesn't care which page it lands on — it only checks
> `size_after != 0`.

### 13.5 What's on disk after T3

`wallog` bytes:

```
byte 0..63          LogFileHeader (frame_count = 9, start_frame_no = 0)
byte 64..4183       frame 0   (header page)
byte 4184..8303     frame 1   (commit T1)
byte 8304..12423    frame 2
byte 12424..16543   frame 3   (commit T2)
byte 16544..20663   frame 4
byte 20664..24783   frame 5
byte 24784..28903   frame 6
byte 28904..33023   frame 7
byte 33024..37143   frame 8   (commit T3)

  global offset of frame f  =  64 + (f - start_frame_no) * 4120
```

`client_wal_index` on the replica (32 bytes):

```
byte 0..15   log_id          = 2f1c9a…c3d4     # must match Hello().log_id
byte 16..23  committed_frame_no = 8
byte 24..31  padding         = 0
```

An embedded replica's sidecars next to `users.db`: `users.db-wal`, `users.db-shm`,
`users.db-client_wal_index`.

### 13.6 How the replica reconstructs the db from these frames

The replica opens `users.db` with the injector WAL. Applying frames **in order**
makes the local WAL identical to the primary's pages:

```
apply frame 0  → WAL frame 1,  page 1 (header)
apply frame 1  → WAL frame 2,  page 2 (table root)      → commit, size_after = 2
apply frame 2  → WAL frame 3,  page 2 (alice, bob)
apply frame 3  → WAL frame 4,  page 2                   → commit
apply frame 4  → WAL frame 5,  page 2 (carol)
apply frame 5  → WAL frame 6,  page 3 (leaf)
apply frame 6  → WAL frame 7,  page 2 (interior)
apply frame 7  → WAL frame 8,  page 4 (dave)
apply frame 8  → WAL frame 9,  page 2                   → commit, size_after = 4
```

Only on a `size_after != 0` frame does the injector (a) COMMIT the local txn and
(b) return that frame_no so the replicator persists it to `client_wal_index`.
Reads hit `users.db` + `users.db-wal`; when the local WAL crosses `auto_checkpoint`
(1000 pages) SQLite checkpoints it into `users.db`.

### 13.7 Compaction → snapshot (Level 1)

Say `max_log_frame_count` is small, so the commit of frame 8 trips
`should_compact()` (`frame_count = 9 > 8`, and no txn in progress). `do_compaction`:

1. Builds a fresh log in `to_compact/<uuid>` with `start_frame_no = 9`,
   `start_checksum = checksum(frame 8) = 0x26A0…`, `frame_count = 0`.
2. Atomically swaps it into `wallog` (`RENAME_EXCHANGE`).
3. Hands the old log (frames 0..8) to the compactor task, which walks it
   **backwards** and keeps only the newest version of each page:

| frame_no walked | page_no | page already kept? | kept in snapshot? |
|---|---|---|---|
| 8 | 2 | no | **YES** (newest page 2) |
| 7 | 4 | no | **YES** |
| 6 | 2 | yes | no |
| 5 | 3 | no | **YES** |
| 4 | 2 | yes | no |
| 3 | 2 | yes | no |
| 2 | 2 | yes | no |
| 1 | 2 | yes | no |
| 0 | 1 | no | **YES** |

Snapshot file `snapshots/2f1c9a…c3d4-0-8.snap`:

```
header (48 B): log_id=2f1c9a…c3d4, start_frame_no=0, end_frame_no=8,
               frame_count=4, size_after=4
frame 8  (page 2)   # stored with size_after forced to 0
frame 7  (page 4)
frame 5  (page 3)
frame 0  (page 1)   # frame_no strictly decreasing
```

That's **4 pages instead of 9 frames** — the space win. The old `wallog` file is
then deleted (`remove_file(to_compact_path)`). The live `wallog` starts fresh at
frame 9.

### 13.8 A brand-new replica bootstraps from the snapshot

Fresh replica: `client_wal_index.committed_frame_no = None` → next offset **0**.
`LogEntries{next_offset: 0}` → frame 0 is before the new `wallog.start_frame_no (9)`
→ gRPC error `NEED_SNAPSHOT`. Replica calls `Snapshot{next_offset: 0}`.

Server runs `find_snapshot_file(path, 0)` → the `[0,8]` snapshot matches → streams
frames **with `frame_no >= 0`**, setting `size_after = 4` on the **last** one:

```
send frame 8 (page 2, size_after 0)
send frame 7 (page 4, size_after 0)
send frame 5 (page 3, size_after 0)
send frame 0 (page 1, size_after 4)   ← last; server stamps commit
```

The injector applies them (reverse frame order is fine — it just writes pages),
commits at `max frame_no seen = 8`, and persists `committed_frame_no = 8`.

### 13.9 A replica that fell behind resumes after compaction

Replica committed **3** (stopped after T2), then came back. `LogEntries{next_offset: 4}`
→ `4 < 9` → `NEED_SNAPSHOT` again. `Snapshot{next_offset: 4}` streams only frames
with `frame_no >= 4`: **8, 7, 5** (frame 0 is skipped — it's `< 4`, the replica
already has page 1 from T1/T2). Last frame streamed (5) gets `size_after = 4`,
replica commits at **8**. It then resumes incremental sync from `LogEntries{next_offset: 9}`.

```
log_entries(4)   → NEED_SNAPSHOT          (old log is gone)
snapshot(4)      → frames 8,7,5           (pages 2,4,3 → db has all 4 pages)
log_entries(9)   → frame 9, 10, …         (back on the live log)
```

### 13.10 Frames continue forever — even across compactions

`frame_no` is a **global, never-reused log position**, so a replica can keep a
single `committed_frame_no` and never get confused by compaction:

| event | wallog.start_frame_no | frames now available | committed at replica |
|---|---|---|---|
| fresh log | 0 | 0..8 | – |
| compact | 9 | 9.. | 8 |
| T4 `INSERT eve` | 9 | frame 9 (page 2, size_after 4) | 9 |
| T5 `INSERT frank` | 9 | frame 10 (page 2, size_after 4) | 10 |
| compact again | 11 | 11.. | 10 |

The checksum chain also survives: each new log header stores the previous log's
**last** checksum as its `start_checksum`, so frames are still verified as one
continuous sequence.

### 13.11 Snapshot merging (Level 2)

After several compactions the `snapshots/` dir holds `[0,8]`, `[9,10]`, `[11,15]`
(16 frames total). The merger's trigger: total snapshot frames ≥ **2× db pages**
or **> 32 snapshots**. `merge_snapshots` reads them newest-first, dedups pages,
and writes one `[0,15]` snapshot, deleting the three originals.

### 13.12 Checksums in one line

Every frame's `checksum` field is the rolling result of:

```
checksum(f) = CRC64_GO_ISO( checksum(f-1), page_bytes(f) )
```

`start_checksum` in the `wallog` header (and the new header after each compaction)
is the seed for frame `start_frame_no`. A replica reading out-of-order or a
corrupted frame breaks the chain — the server can tell immediately.

---

## Key files

| Area | File |
|---|---|
| Frame format | `libsql-replication/src/frame.rs` |
| Replica state machine | `libsql-replication/src/replicator.rs` |
| Replica WAL injector | `libsql-replication/src/injector/sqlite_injector/mod.rs` + `injector_wal.rs` |
| Replica meta file | `libsql-replication/src/meta.rs` |
| Snapshot file reader | `libsql-replication/src/snapshot.rs` |
| RPC proto | `libsql-replication/proto/replication_log.proto` |
| RPC server | `libsql-server/src/rpc/replication/replication_log.rs` |
| Primary `wallog` | `libsql-server/src/replication/primary/logger.rs` |
| Primary WAL wrapper | `libsql-server/src/replication/primary/replication_logger_wal.rs` |
| Frame streaming | `libsql-server/src/replication/primary/frame_stream.rs` |
| Snapshot building/merging | `libsql-server/src/replication/snapshot.rs` |
| Primary setup | `libsql-server/src/namespace/configurator/helpers.rs` |
| Server-side replica setup | `libsql-server/src/namespace/configurator/replica.rs` |
| Embedded client replicator | `libsql/src/replication/mod.rs`, `remote_client.rs`, `local_client.rs` |
