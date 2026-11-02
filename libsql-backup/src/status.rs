//! `status.json`, the commit point of a namespace's backup: the backup is exactly the objects it
//! lists. It is written after the objects it points to, and the objects it stops pointing to are
//! deleted only after it is written.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::store::{Expect, Store};
use crate::sync::Position;

pub const STATUS_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Status {
    pub version: u32,
    /// The sqld log the frames come from.
    pub log_id: String,
    /// Last frame included in the backup.
    pub last_frame_no: u64,
    /// Size of the database in pages at `last_frame_no`.
    pub size_after: u32,
    pub snapshot: SnapshotEntry,
    /// Segments to apply on top of the snapshot, in order.
    pub wal: Vec<WalEntry>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SnapshotEntry {
    pub key: String,
    pub end_frame_no: u64,
    /// Size of the database in pages in the snapshot.
    pub size_after: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WalEntry {
    pub key: String,
    pub start_frame_no: u64,
    pub end_frame_no: u64,
    /// Number of pages in the segment.
    pub pages: usize,
    /// Size of the database in pages once the segment is applied.
    pub size_after: u32,
}

impl Status {
    pub fn position(&self) -> Position {
        Position {
            log_id: self.log_id.clone(),
            next_offset: self.last_frame_no + 1,
        }
    }

    /// Every object the backup is made of.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.snapshot.key.as_str()).chain(self.wal.iter().map(|w| w.key.as_str()))
    }
}

/// A status as read, with what it takes to replace it.
pub struct Current {
    pub status: Status,
    etag: String,
}

pub fn status_key(store: &Store, namespace: &str) -> String {
    store.key(&format!("{namespace}/status.json"))
}

pub async fn read(store: &Store, namespace: &str) -> Result<Option<Current>> {
    let key = status_key(store, namespace);
    let Some((bytes, etag)) = store.get(&key).await? else {
        return Ok(None);
    };
    let status: Status =
        serde_json::from_slice(&bytes).with_context(|| format!("invalid {key}"))?;
    if status.version != STATUS_VERSION {
        bail!(
            "{key} has version {}, expected {STATUS_VERSION}",
            status.version
        );
    }
    Ok(Some(Current { status, etag }))
}

/// Replaces `previous` (or creates the status when there is none). Fails with a
/// [`crate::store::Conflict`] when another run changed it in the meantime.
pub async fn write(
    store: &Store,
    namespace: &str,
    previous: Option<&Current>,
    status: &Status,
) -> Result<()> {
    let key = status_key(store, namespace);
    let body = serde_json::to_vec_pretty(status)?;
    let expect = match previous {
        Some(current) => Expect::Etag(current.etag.clone()),
        None => Expect::Absent,
    };
    store.put_if(&key, body.into(), expect).await
}
