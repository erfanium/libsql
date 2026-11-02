# libsql-backup

Page-level, incremental backups of sqld namespaces to S3.

It works like an embedded replica. It pulls frames from sqld's `GET /sync` route and needs no changes to sqld. It keeps no state of its own: the bucket holds everything, and the only disk it uses is temporary files while it builds a snapshot.

## S3 layout

```
{prefix}/{ns}/status.json                                     the commit point
{prefix}/{ns}/{log_id}/{end:020}.snap.zst                     full SQLite database file, zstd
{prefix}/{ns}/{log_id}/wal/{start:020}-{end:020}-{date}.zst   raw frames (≤ --max-wal-pages pages), zstd
```

`status.json` lists exactly which objects make up the backup. Each backup does these steps in order:
1. Upload the new objects.
2. Replace `status.json` with a conditional write (`If-Match`), so two runs can't overwrite each other.
3. Delete the objects the new status no longer uses.

Objects left behind by a failed run are swept after an hour.

## A backup

- **No status yet:** clone the whole database through `/sync` and upload it as a snapshot.
- **Nothing new:** this costs one S3 GET and one `/sync` request that returns only its trailer.
- **New frames:** each page is kept once, at its newest version. The pages are merged into the last WAL segment if they fit, or else go into a new segment.
- **Too many segments** (`--max-wal-files`): fold the snapshot, the segments and the new pages into a new snapshot.
- **sqld answers 409** (its log was recreated, e.g. after a crash): take a full snapshot again.

## Usage

```sh
export LIBSQL_BACKUP_S3_BUCKET=backups LIBSQL_BACKUP_S3_ENDPOINT=http://minio:9000 \
       LIBSQL_BACKUP_S3_ACCESS_KEY_ID=... LIBSQL_BACKUP_S3_SECRET_ACCESS_KEY=...

# one namespace, or all of them (listed by sqld's admin API)
libsql-backup backup my-ns --sqld-url http://sqld:8080
libsql-backup backup --all --sqld-url http://sqld:8080 \
    --sqld-admin-url http://sqld:8080 --sqld-admin-key "$ADMIN_KEY"

libsql-backup status my-ns

# HTTP API, optionally with a daily backup of every namespace of --sqld-url
libsql-backup serve --listen 0.0.0.0:8090 --schedule-interval 24h ...
```

### `serve` API

| Route | |
|---|---|
| `POST /backup` | Body `{"namespace": "..", "source": "http://sqld:8080", "key": ".."}`. `key` is sent to sqld as a bearer token and defaults to `SQLD_DEFAULT_KEY`; the request fails with 400 when neither is set. |
| `GET /status/{namespace}` | The namespace's `status.json`. |
| `GET /healthz` | |

When `--auth-key` is set, every route except `/healthz` requires `Authorization: Bearer <key>`.

## Tests

```sh
cargo build -p libsql-server --bin sqld   # or set SQLD_BIN
cargo test -p libsql-backup
```

The end-to-end tests run a real sqld against an in-memory fake S3, and skip themselves when no sqld binary is found.
