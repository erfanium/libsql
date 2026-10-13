use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use chrono::{DateTime, Utc};
use metrics::{counter, gauge, histogram, increment_counter};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Duration;
use uuid::Uuid;

use crate::namespace::NamespaceName;
use crate::replication::FrameNo;

#[derive(Debug, Default, Clone)]
pub struct StatsUpdateMessage {
    pub elapsed: Duration,
    pub rows_read: u64,
    pub rows_written: u64,
    pub mem_used: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Stats {
    #[serde(skip)]
    namespace: NamespaceName,
    #[serde(skip)]
    sender: Option<mpsc::Sender<StatsUpdateMessage>>,
    /// where the stats are persisted
    #[serde(skip)]
    path: Option<PathBuf>,
    /// whether the namespace was used since the stats were last persisted
    #[serde(skip)]
    dirty: AtomicBool,

    #[serde(default)]
    id: Option<Uuid>,
    #[serde(default)]
    rows_written: AtomicU64,
    #[serde(default)]
    rows_read: AtomicU64,
    #[serde(default)]
    storage_bytes_used: AtomicU64,
    // number of write requests delegated from a replica to primary
    #[serde(default)]
    write_requests_delegated: AtomicU64,
    #[serde(default)]
    current_frame_no: AtomicU64,
    #[serde(default)]
    embedded_replica_frames_replicated: AtomicU64,
    #[serde(default)]
    query_count: AtomicU64,
    #[serde(default)]
    query_latency: AtomicU64,
    /// When the namespace was last used (queried, synced, compacted...): seconds since the unix
    /// epoch, 0 if it never was. Persisted as an ISO 8601 date, or null.
    #[serde(default, with = "iso_date")]
    last_used: AtomicU64,
}

impl Stats {
    pub async fn new(
        namespace: NamespaceName,
        db_path: &Path,
        join_set: &mut JoinSet<anyhow::Result<()>>,
    ) -> anyhow::Result<Arc<Self>> {
        let stats_path = db_path.join("stats.json");
        let mut this = if stats_path.try_exists()? {
            let data = tokio::fs::read_to_string(&stats_path).await?;
            serde_json::from_str(&data)?
        } else {
            Stats::default()
        };

        if this.id.is_none() {
            this.id = Some(Uuid::new_v4());
        }

        let (update_sender, update_receiver) = mpsc::channel(256);

        this.namespace = namespace;
        this.sender = Some(update_sender);
        this.path = Some(stats_path);

        let this = Arc::new(this);

        join_set.spawn(spawn_stats_persist_thread(Arc::downgrade(&this)));

        join_set.spawn(spawn_stats_thread(Arc::downgrade(&this), update_receiver));

        Ok(this)
    }

    pub fn send(&self, msg: StatsUpdateMessage) {
        if let Some(sender) = &self.sender {
            let _ = sender.blocking_send(msg);
        }
    }

    pub fn update(&self, msg: StatsUpdateMessage) {
        let rows_read = msg.rows_read;
        let rows_written = msg.rows_written;
        let mem_used = msg.mem_used;
        let elapsed = msg.elapsed;
        let elapsed_ms = elapsed.as_millis() as u64;
        let rows_read = if rows_read == 0 && rows_written == 0 {
            1
        } else {
            rows_read
        };
        histogram!("libsql_server_statement_execution_time", elapsed);
        histogram!("libsql_server_statement_mem_used_bytes", mem_used as f64);

        self.touch();
        self.inc_rows_read(rows_read);
        self.inc_rows_written(rows_written);
        self.inc_query(elapsed);
        self.update_query_metrics(rows_read, rows_written, mem_used, elapsed_ms)
    }

    /// increments the number of written rows by n
    fn inc_rows_written(&self, n: u64) {
        counter!("libsql_server_rows_written", n, "namespace" => self.namespace.to_string());
        self.rows_written.fetch_add(n, Ordering::Relaxed);
    }

    fn inc_query(&self, d: Duration) {
        let micros = d.as_micros() as u64;
        counter!("libsql_server_query_count", 1, "namespace" => self.namespace.to_string());
        counter!("libsql_server_query_latency", micros / 1000, "namespace" => self.namespace.to_string());
        self.query_count.fetch_add(1, Ordering::Relaxed);
        self.query_latency.fetch_add(micros, Ordering::Relaxed);
    }

    /// increments the number of read rows by n
    fn inc_rows_read(&self, n: u64) {
        counter!("libsql_server_rows_read", n, "namespace" => self.namespace.to_string());
        self.rows_read.fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_storage_bytes_used(&self, n: u64) {
        gauge!("libsql_server_storage", n as f64, "namespace" => self.namespace.to_string());
        self.storage_bytes_used.store(n, Ordering::Relaxed);
    }

    /// returns the total number of rows read since this database was created
    pub(crate) fn rows_read(&self) -> u64 {
        self.rows_read.load(Ordering::Relaxed)
    }

    /// returns the total number of rows written since this database was created
    pub(crate) fn rows_written(&self) -> u64 {
        self.rows_written.load(Ordering::Relaxed)
    }

    /// returns the total number of bytes used by the database (excluding uncheckpointed WAL entries)
    pub(crate) fn storage_bytes_used(&self) -> u64 {
        self.storage_bytes_used.load(Ordering::Relaxed)
    }

    /// increments the number of the write requests which were delegated from a replica to primary
    pub fn inc_write_requests_delegated(&self) {
        increment_counter!("libsql_server_write_requests_delegated", "namespace" => self.namespace.to_string());
        self.write_requests_delegated
            .fetch_add(1, Ordering::Relaxed);
    }

    /// increments the number of the write requests which were delegated from a replica to primary
    pub fn inc_embedded_replica_frames_replicated(&self) {
        increment_counter!("libsql_server_embedded_replica_frames_replicated", "namespace" => self.namespace.to_string());
        self.embedded_replica_frames_replicated
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_embedded_replica_frames_replicated(&self) -> u64 {
        self.embedded_replica_frames_replicated
            .load(Ordering::Relaxed)
    }

    pub fn write_requests_delegated(&self) -> u64 {
        self.write_requests_delegated.load(Ordering::Relaxed)
    }

    pub fn set_current_frame_no(&self, fno: FrameNo) {
        gauge!("libsql_server_current_frame_no", fno as f64, "namespace" => self.namespace.to_string());
        self.current_frame_no.store(fno, Ordering::Relaxed);
    }

    pub(crate) fn get_current_frame_no(&self) -> FrameNo {
        self.current_frame_no.load(Ordering::Relaxed)
    }

    pub(crate) fn get_query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    pub(crate) fn get_query_latency(&self) -> u64 {
        self.query_latency.load(Ordering::Relaxed)
    }

    // TOOD: Update these metrics with namespace labels in the future so we can localize
    // issues to a specific namespace.
    fn update_query_metrics(&self, rows_read: u64, rows_written: u64, mem_used: u64, elapsed: u64) {
        increment_counter!("libsql_server_query_count");
        counter!("libsql_server_query_latency", elapsed);
        counter!("libsql_server_query_rows_read", rows_read);
        counter!("libsql_server_query_rows_written", rows_written);
        counter!("libsql_server_query_mem_used", mem_used);
    }

    pub(crate) fn id(&self) -> Option<Uuid> {
        self.id
    }

    /// Marks the namespace as used now.
    pub(crate) fn touch(&self) {
        self.last_used
            .store(Utc::now().timestamp().max(0) as u64, Ordering::Relaxed);
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// When the namespace was last used, if it ever was.
    pub(crate) fn last_used(&self) -> Option<DateTime<Utc>> {
        iso_date::from_secs(self.last_used.load(Ordering::Relaxed))
    }

    /// Writes the stats to disk, unless the namespace was not used since they last were.
    pub(crate) async fn persist(&self) -> anyhow::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if !self.dirty.swap(false, Ordering::Relaxed) {
            return Ok(());
        }

        let written = async {
            let temp_path = path.with_extension("tmp");
            tokio::fs::write(&temp_path, serde_json::to_vec(self)?).await?;
            tokio::fs::rename(temp_path, path).await?;
            anyhow::Ok(())
        }
        .await;
        if written.is_err() {
            // to be tried again
            self.dirty.store(true, Ordering::Relaxed);
        }

        written
    }
}

async fn spawn_stats_thread(
    stats: Weak<Stats>,
    mut receiver: mpsc::Receiver<StatsUpdateMessage>,
) -> anyhow::Result<()> {
    loop {
        match (receiver.recv().await, stats.upgrade()) {
            (Some(msg), Some(stats)) => stats.update(msg),
            _ => return Ok(()),
        }
    }
}

/// The stats are not worth more than that: what is lost if the server crashes is at most that
/// much of the activity of the namespace. They are also persisted when the namespace is shut
/// down.
const PERSIST_INTERVAL: Duration = Duration::from_secs(10 * 60);

async fn spawn_stats_persist_thread(stats: Weak<Stats>) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(PERSIST_INTERVAL).await;
        let Some(stats) = stats.upgrade() else {
            return Ok(());
        };
        if let Err(e) = stats.persist().await {
            tracing::error!("error persisting stats file: {e}");
        }
    }
}

/// A date kept as seconds since the unix epoch in an atomic, 0 for none
mod iso_date {
    use std::sync::atomic::{AtomicU64, Ordering};

    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn from_secs(secs: u64) -> Option<DateTime<Utc>> {
        (secs != 0)
            .then(|| DateTime::from_timestamp(secs as i64, 0))
            .flatten()
    }

    pub fn serialize<S: Serializer>(secs: &AtomicU64, serializer: S) -> Result<S::Ok, S::Error> {
        from_secs(secs.load(Ordering::Relaxed)).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<AtomicU64, D::Error> {
        let date = Option::<DateTime<Utc>>::deserialize(deserializer)?;
        Ok(AtomicU64::new(
            date.map_or(0, |d| d.timestamp().max(0) as u64),
        ))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    async fn load(path: &Path, join_set: &mut JoinSet<anyhow::Result<()>>) -> Arc<Stats> {
        Stats::new(NamespaceName::default(), path, join_set)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn persisted_when_used_since_last_time() {
        let tmp = tempfile::tempdir().unwrap();
        let stats_path = tmp.path().join("stats.json");
        let mut join_set = JoinSet::new();

        // nothing to persist for a namespace that was only loaded
        let stats = load(tmp.path(), &mut join_set).await;
        stats.persist().await.unwrap();
        assert!(!stats_path.exists());

        stats.update(StatsUpdateMessage {
            rows_read: 3,
            ..Default::default()
        });
        stats.persist().await.unwrap();

        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&stats_path).unwrap()).unwrap();
        assert_eq!(json["rows_read"], 3);
        let last_used = stats.last_used().unwrap();
        assert_eq!(
            json["last_used"],
            last_used.format("%Y-%m-%dT%H:%M:%SZ").to_string()
        );

        // not written again until the namespace is used again
        std::fs::remove_file(&stats_path).unwrap();
        stats.persist().await.unwrap();
        assert!(!stats_path.exists());
        stats.touch();
        stats.persist().await.unwrap();

        let reloaded = load(tmp.path(), &mut join_set).await;
        assert_eq!(reloaded.rows_read(), 3);
        assert_eq!(reloaded.last_used(), stats.last_used());
    }
}
