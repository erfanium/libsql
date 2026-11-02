//! Backing up one namespace: pulls the frames sqld has past the last backup and stores them as
//! WAL segments on top of a full snapshot, rolling a new snapshot once there are too many
//! segments.
//!
//! ```text
//! {prefix}/{ns}/status.json
//! {prefix}/{ns}/{log_id}/{end:020}.snap.zst                  full database file
//! {prefix}/{ns}/{log_id}/wal/{start:020}-{end:020}-{date}.zst raw frames, by page
//! ```

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Result};
use chrono::Utc;
use serde::Serialize;

use crate::dbfile::{self, DbFile};
use crate::frame::{Frame, Pages};
use crate::status::{self, Current, SnapshotEntry, Status, WalEntry, STATUS_VERSION};
use crate::store::Store;
use crate::sync::{Position, SyncClient, SyncItem, SyncOutcome, SyncStream};

/// Objects not in the status and older than this are leftovers of a run that failed.
const ORPHAN_AGE: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone)]
pub struct BackupOptions {
    /// A new snapshot is made rather than going past this many WAL segments.
    pub max_wal_files: usize,
    /// Pages per WAL segment.
    pub max_wal_pages: usize,
    pub zstd_level: i32,
}

impl Default for BackupOptions {
    fn default() -> Self {
        Self {
            max_wal_files: 8,
            max_wal_pages: 2048,
            zstd_level: 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Outcome {
    /// Nothing was written since the last backup.
    UpToDate { last_frame_no: u64 },
    /// The database has no frames at all: nothing to back up.
    Empty,
    /// A new full snapshot was stored.
    Snapshot {
        last_frame_no: u64,
        uploaded_bytes: u64,
        reason: SnapshotReason,
    },
    /// The new pages were stored in a WAL segment.
    Wal {
        last_frame_no: u64,
        pages: usize,
        uploaded_bytes: u64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotReason {
    FirstBackup,
    /// sqld's log is not the one of the backup anymore (it was recovered after a crash).
    NewLog,
    TooManyWalFiles,
}

#[derive(Clone)]
pub struct Backup {
    store: Store,
    options: BackupOptions,
}

impl Backup {
    pub fn new(store: Store, options: BackupOptions) -> Self {
        Self { store, options }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Backs up `namespace` from the sqld that `sync` talks to.
    pub async fn backup(&self, sync: &SyncClient, namespace: &str) -> Result<Outcome> {
        // the common case is a namespace nobody wrote to: one GET to S3 and one /sync that
        // returns only its trailer
        let Some(current) = status::read(&self.store, namespace).await? else {
            return self
                .full(sync, namespace, None, SnapshotReason::FirstBackup)
                .await;
        };

        let position = current.status.position();
        let stream = match sync.sync(namespace, Some(&position)).await? {
            SyncOutcome::Stream(stream) => stream,
            SyncOutcome::LogMismatch => {
                return self
                    .full(sync, namespace, Some(current), SnapshotReason::NewLog)
                    .await
            }
        };
        let batch = Batch::read(stream).await?;
        ensure!(
            batch.log_id == position.log_id,
            "sqld answered from log {} instead of {}",
            batch.log_id,
            position.log_id
        );
        if batch.pages.is_empty() {
            ensure!(
                batch.next_offset == position.next_offset,
                "sqld moved from frame {} to {} without sending frames",
                position.next_offset,
                batch.next_offset
            );
            return Ok(Outcome::UpToDate {
                last_frame_no: current.status.last_frame_no,
            });
        }

        let outcome = self.append(namespace, current, &position, batch).await?;
        Ok(outcome)
    }

    /// Clones the whole database from sqld into a new snapshot.
    async fn full(
        &self,
        sync: &SyncClient,
        namespace: &str,
        previous: Option<Current>,
        reason: SnapshotReason,
    ) -> Result<Outcome> {
        let mut stream = match sync.sync(namespace, None).await? {
            SyncOutcome::Stream(stream) => stream,
            SyncOutcome::LogMismatch => bail!("sqld refused to clone `{namespace}`"),
        };

        let mut db = DbFile::temp()?;
        let mut last: Option<Frame> = None;
        let trailer = loop {
            match stream.next().await? {
                SyncItem::Frame(frame) => {
                    db.write_frame(&frame).await?;
                    last = Some(frame);
                }
                SyncItem::Trailer(trailer) => break trailer,
            }
        };
        let Some(last) = last else {
            return Ok(Outcome::Empty);
        };
        let size_after = commit_size(&last)?;
        db.truncate(size_after).await?;

        let last_frame_no = trailer.next_offset - 1;
        let key = self.unique_key(
            snapshot_key(&self.store, namespace, &trailer.log_id, last_frame_no),
            previous.as_ref(),
        );
        let uploaded_bytes = db
            .upload_compressed(&self.store, &key, self.options.zstd_level)
            .await?;

        let status = Status {
            version: STATUS_VERSION,
            log_id: trailer.log_id,
            last_frame_no,
            size_after,
            snapshot: SnapshotEntry {
                key,
                end_frame_no: last_frame_no,
                size_after,
            },
            wal: Vec::new(),
            updated_at: Utc::now(),
        };
        self.commit(namespace, previous, status).await?;

        Ok(Outcome::Snapshot {
            last_frame_no,
            uploaded_bytes,
            reason,
        })
    }

    /// Stores new pages in the WAL, or in a new snapshot when the WAL would grow too long.
    async fn append(
        &self,
        namespace: &str,
        current: Current,
        position: &Position,
        batch: Batch,
    ) -> Result<Outcome> {
        let last_frame_no = batch.next_offset - 1;
        let page_count = batch.pages.len();
        let mut status = current.status.clone();
        status.last_frame_no = last_frame_no;
        status.size_after = batch.size_after;
        status.updated_at = Utc::now();

        let mut uploaded_bytes = 0;

        // grow the last segment when the pages fit in it: it keeps the number of objects down
        if let Some(last) = status
            .wal
            .last()
            .filter(|w| w.pages < self.options.max_wal_pages)
            .cloned()
        {
            let mut pages = self.read_segment(&last.key).await?;
            if pages.union_len(&batch.pages) <= self.options.max_wal_pages {
                pages.merge(batch.pages);
                let entry = WalEntry {
                    key: wal_key(
                        &self.store,
                        namespace,
                        &status.log_id,
                        last.start_frame_no,
                        last_frame_no,
                        None,
                    ),
                    start_frame_no: last.start_frame_no,
                    end_frame_no: last_frame_no,
                    pages: pages.len(),
                    size_after: batch.size_after,
                };
                uploaded_bytes += self.write_segment(&entry.key, &pages).await?;
                *status.wal.last_mut().unwrap() = entry;
                self.commit(namespace, Some(current), status).await?;
                return Ok(Outcome::Wal {
                    last_frame_no,
                    pages: page_count,
                    uploaded_bytes,
                });
            }
        }

        let chunks = batch.pages.split(self.options.max_wal_pages);
        if status.wal.len() + chunks.len() > self.options.max_wal_files {
            let mut pages = Pages::default();
            for chunk in chunks {
                pages.merge(chunk);
            }
            return self
                .rollover(namespace, current, status, pages, batch.size_after)
                .await;
        }

        let parts = chunks.len();
        for (i, chunk) in chunks.into_iter().enumerate() {
            let part = (parts > 1).then_some(i);
            let entry = WalEntry {
                key: wal_key(
                    &self.store,
                    namespace,
                    &status.log_id,
                    position.next_offset,
                    last_frame_no,
                    part,
                ),
                start_frame_no: position.next_offset,
                end_frame_no: last_frame_no,
                pages: chunk.len(),
                size_after: batch.size_after,
            };
            uploaded_bytes += self.write_segment(&entry.key, &chunk).await?;
            status.wal.push(entry);
        }
        self.commit(namespace, Some(current), status).await?;
        Ok(Outcome::Wal {
            last_frame_no,
            pages: page_count,
            uploaded_bytes,
        })
    }

    /// Folds the snapshot, the WAL and `pages` into a new snapshot.
    async fn rollover(
        &self,
        namespace: &str,
        current: Current,
        mut status: Status,
        pages: Pages,
        size_after: u32,
    ) -> Result<Outcome> {
        let mut db = DbFile::temp()?;
        db.load_compressed(&self.store, &status.snapshot.key)
            .await?;
        db.truncate(status.snapshot.size_after).await?;
        for entry in &status.wal {
            let segment = self.read_segment(&entry.key).await?;
            db.write_pages(&segment).await?;
            db.truncate(entry.size_after).await?;
        }
        db.write_pages(&pages).await?;
        db.truncate(size_after).await?;

        let last_frame_no = status.last_frame_no;
        let key = self.unique_key(
            snapshot_key(&self.store, namespace, &status.log_id, last_frame_no),
            Some(&current),
        );
        let uploaded_bytes = db
            .upload_compressed(&self.store, &key, self.options.zstd_level)
            .await?;
        status.snapshot = SnapshotEntry {
            key,
            end_frame_no: last_frame_no,
            size_after,
        };
        status.wal.clear();
        self.commit(namespace, Some(current), status).await?;

        Ok(Outcome::Snapshot {
            last_frame_no,
            uploaded_bytes,
            reason: SnapshotReason::TooManyWalFiles,
        })
    }

    /// Writes the new status, then removes what the previous one had and this one does not.
    async fn commit(
        &self,
        namespace: &str,
        previous: Option<Current>,
        status: Status,
    ) -> Result<()> {
        status::write(&self.store, namespace, previous.as_ref(), &status).await?;

        if let Some(previous) = previous {
            let keep: HashSet<&str> = status.keys().collect();
            let stale: Vec<String> = previous
                .status
                .keys()
                .filter(|k| !keep.contains(k))
                .map(str::to_string)
                .collect();
            if let Err(e) = self.store.delete(&stale).await {
                // the backup itself is done: what is left is swept on a later run
                tracing::warn!("`{namespace}`: could not delete replaced objects: {e:#}");
            }
        }
        if let Err(e) = self.sweep(namespace, &status).await {
            tracing::warn!("`{namespace}`: could not sweep leftover objects: {e:#}");
        }
        Ok(())
    }

    /// Deletes objects of the namespace that the status does not use, left by failed runs.
    async fn sweep(&self, namespace: &str, status: &Status) -> Result<()> {
        let status_key = status::status_key(&self.store, namespace);
        let keep: HashSet<&str> = status.keys().collect();
        let deadline = Utc::now() - chrono::Duration::from_std(ORPHAN_AGE)?;
        let orphans: Vec<String> = self
            .store
            .list(&namespace_prefix(&self.store, namespace))
            .await?
            .into_iter()
            .filter(|o| o.key != status_key && !keep.contains(o.key.as_str()))
            .filter(|o| o.last_modified.is_some_and(|t| t < deadline))
            .map(|o| o.key)
            .collect();
        if !orphans.is_empty() {
            tracing::info!("`{namespace}`: deleting {} leftover objects", orphans.len());
            self.store.delete(&orphans).await?;
        }
        Ok(())
    }

    async fn read_segment(&self, key: &str) -> Result<Pages> {
        let (bytes, _) = self
            .store
            .get(key)
            .await?
            .ok_or_else(|| anyhow!("WAL segment {key} is missing"))?;
        Pages::decode(dbfile::decompress(&bytes).await?)
    }

    async fn write_segment(&self, key: &str, pages: &Pages) -> Result<u64> {
        let compressed = dbfile::compress(&pages.encode(), self.options.zstd_level).await?;
        let len = compressed.len() as u64;
        self.store.put(key, compressed).await?;
        Ok(len)
    }

    /// A key that the previous status does not use, so that writing it can't damage the
    /// backup in place.
    fn unique_key(&self, key: String, previous: Option<&Current>) -> String {
        let taken = previous.is_some_and(|p| p.status.keys().any(|k| k == key));
        if taken {
            let stem = key.trim_end_matches(".snap.zst");
            format!("{stem}-{}.snap.zst", Utc::now().timestamp_millis())
        } else {
            key
        }
    }
}

/// The frames of one /sync response, deduplicated by page.
struct Batch {
    pages: Pages,
    size_after: u32,
    log_id: String,
    next_offset: u64,
}

impl Batch {
    async fn read(mut stream: SyncStream) -> Result<Self> {
        let mut pages = Pages::default();
        let mut last: Option<Frame> = None;
        loop {
            match stream.next().await? {
                SyncItem::Frame(frame) => {
                    pages.insert(frame.clone());
                    last = Some(frame);
                }
                SyncItem::Trailer(trailer) => {
                    let size_after = match &last {
                        Some(frame) => commit_size(frame)?,
                        None => 0,
                    };
                    return Ok(Self {
                        pages,
                        size_after,
                        log_id: trailer.log_id,
                        next_offset: trailer.next_offset,
                    });
                }
            }
        }
    }
}

/// The last frame of a /sync response commits the transaction.
fn commit_size(frame: &Frame) -> Result<u32> {
    match frame.size_after() {
        0 => bail!("the last frame sqld sent is not a commit frame: {frame:?}"),
        size => Ok(size),
    }
}

fn namespace_prefix(store: &Store, namespace: &str) -> String {
    store.key(&format!("{namespace}/"))
}

fn snapshot_key(store: &Store, namespace: &str, log_id: &str, end: u64) -> String {
    store.key(&format!("{namespace}/{log_id}/{end:020}.snap.zst"))
}

fn wal_key(
    store: &Store,
    namespace: &str,
    log_id: &str,
    start: u64,
    end: u64,
    part: Option<usize>,
) -> String {
    let date = Utc::now().format("%Y%m%d");
    let part = part.map(|p| format!("-p{p}")).unwrap_or_default();
    store.key(&format!(
        "{namespace}/{log_id}/wal/{start:020}-{end:020}-{date}{part}.zst"
    ))
}
