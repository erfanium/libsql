use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::connection::program::Program;
use crate::namespace::NamespaceName;

/// Maximum length of the SQL text reported in the process list.
const MAX_SQL_LENGTH: usize = 200;
/// A query whose heartbeat is older than this is considered blocked.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(1);

/// How often the progress handler may update the heartbeat (per active query).
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

static ACTIVE_QUERIES: LazyLock<RwLock<HashMap<usize, ActiveQuery>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

#[derive(Clone)]
struct ActiveQuery {
    namespace: NamespaceName,
    kind: &'static str,
    thread_id: std::thread::ThreadId,
    tid: i32,
    current_step: usize,
    total_steps: usize,
    current_sql: String,
    started_at: Instant,
    last_heartbeat_at: Instant,
}

/// Serialized view of an in-flight query, as returned by [snapshot].
#[derive(Debug, Clone, serde::Serialize)]
pub struct ActiveQuerySnapshot {
    pub conn_id: usize,
    /// Kind of job: `query` (statement execution) or `sync` (replication frame
    /// reads served to embedded replicas).
    pub kind: &'static str,
    pub thread_id: String,
    /// OS thread id (Linux only, `gettid`); 0 elsewhere.
    pub tid: i32,
    pub namespace: String,
    /// `running` when the query has made progress recently, `blocked` when it
    /// has been stuck (e.g. waiting for the per-namespace write lock) for more
    /// than [HEARTBEAT_TIMEOUT].
    pub state: &'static str,
    pub current_step: usize,
    pub total_steps: usize,
    pub current_sql: String,
    pub elapsed_ms: u64,
    pub last_heartbeat_ms_ago: u64,
    /// Cumulative CPU time (user + sys) consumed by the thread since the query
    /// started, in seconds. Linux only; 0 elsewhere.
    pub cpu_seconds: f64,
}

/// RAII guard that removes the entry from the registry on drop, so entries are
/// cleaned up even when the query is canceled or the thread panics.
pub(crate) struct ActiveQueryGuard {
    key: usize,
}

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        ACTIVE_QUERIES.write().remove(&self.key);
        tracing::debug!(target: "query_registry", conn = self.key, "query finished");
    }
}

/// Register a query about to be executed on a blocking thread.
pub(crate) fn register(key: usize, namespace: NamespaceName, pgm: &Program) -> ActiveQueryGuard {
    let current_sql = pgm
        .steps()
        .first()
        .map(|s| truncate(&s.query.stmt.stmt))
        .unwrap_or_default();
    insert_entry(
        key,
        namespace,
        "query",
        current_sql,
        0,
        pgm.steps().len(),
    )
}

/// Register a non-query job (e.g. a replication frame read) about to run on a
/// blocking thread.
pub(crate) fn register_job(
    key: usize,
    namespace: NamespaceName,
    label: String,
) -> ActiveQueryGuard {
    insert_entry(key, namespace, "sync", label, 0, 0)
}

fn insert_entry(
    key: usize,
    namespace: NamespaceName,
    kind: &'static str,
    current_sql: String,
    current_step: usize,
    total_steps: usize,
) -> ActiveQueryGuard {
    let now = Instant::now();
    let sql = current_sql.clone();
    let ns = namespace.to_string();
    ACTIVE_QUERIES.write().insert(
        key,
        ActiveQuery {
            namespace,
            kind,
            thread_id: std::thread::current().id(),
            tid: current_tid(),
            current_step,
            total_steps,
            current_sql,
            started_at: now,
            last_heartbeat_at: now,
        },
    );
    tracing::debug!(
        target: "query_registry",
        conn = key,
        namespace = %ns,
        kind,
        sql = %sql,
        "thread state: started"
    );
    ActiveQueryGuard { key }
}

/// Called from the blocking thread once it starts executing the query, to
/// record the thread that the query is running on.
pub(crate) fn set_thread_info(key: usize) {
    if let Some(entry) = ACTIVE_QUERIES.write().get_mut(&key) {
        entry.thread_id = std::thread::current().id();
        entry.tid = current_tid();
        tracing::debug!(
            target: "query_registry",
            conn = key,
            namespace = %entry.namespace,
            thread_id = ?entry.thread_id,
            tid = entry.tid,
            "thread state: executing"
        );
    }
}

/// Update the step progress and the SQL of the step being executed.
pub(crate) fn touch(key: usize, current_step: usize, total_steps: usize, sql: &str) {
    if let Some(entry) = ACTIVE_QUERIES.write().get_mut(&key) {
        entry.current_step = current_step;
        entry.total_steps = total_steps;
        entry.current_sql = truncate(sql);
        entry.last_heartbeat_at = Instant::now();
        tracing::debug!(
            target: "query_registry",
            conn = key,
            namespace = %entry.namespace,
            step = current_step + 1,
            total_steps,
            sql = %entry.current_sql,
            "thread state: step"
        );
    }
}

/// Cheap liveness heartbeat, called from SQLite's progress handler. Only
/// updates the timestamp, never the SQL.
pub(crate) fn heartbeat(key: usize) {
    if let Some(entry) = ACTIVE_QUERIES.write().get_mut(&key) {
        entry.last_heartbeat_at = Instant::now();
        tracing::trace!(
            target: "query_registry",
            conn = key,
            namespace = %entry.namespace,
            "thread state: heartbeat"
        );
    }
}

/// Snapshot of all currently executing queries, sorted by elapsed time
/// (longest running first).
pub fn snapshot() -> Vec<ActiveQuerySnapshot> {
    let now = Instant::now();
    let mut queries: Vec<(usize, ActiveQuery)> =
        ACTIVE_QUERIES.read().iter().map(|(k, v)| (*k, v.clone())).collect();
    queries.sort_by(|a, b| b.1.started_at.cmp(&a.1.started_at));

    queries
        .into_iter()
        .map(|(key, q)| {
            let last_heartbeat_ms_ago = now.duration_since(q.last_heartbeat_at).as_millis() as u64;
            ActiveQuerySnapshot {
                conn_id: key,
                kind: q.kind,
                thread_id: format!("{:?}", q.thread_id),
                tid: q.tid,
                namespace: q.namespace.to_string(),
                state: if last_heartbeat_ms_ago
                    < HEARTBEAT_TIMEOUT.as_millis() as u64
                {
                    "running"
                } else {
                    "blocked"
                },
                current_step: q.current_step,
                total_steps: q.total_steps,
                current_sql: q.current_sql,
                elapsed_ms: now.duration_since(q.started_at).as_millis() as u64,
                last_heartbeat_ms_ago,
                cpu_seconds: thread_cpu_seconds(q.tid),
            }
        })
        .collect()
}

fn truncate(sql: &str) -> String {
    let sql = sql.trim();
    if sql.len() <= MAX_SQL_LENGTH {
        sql.to_string()
    } else {
        format!("{}...", &sql[..MAX_SQL_LENGTH])
    }
}

fn current_tid() -> i32 {
    #[cfg(target_os = "linux")]
    {
        nix::unistd::gettid().as_raw()
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

#[cfg(target_os = "linux")]
static CLK_TCK: LazyLock<f64> = LazyLock::new(|| {
    nix::unistd::sysconf(nix::unistd::SysconfVar::CLK_TCK)
        .ok()
        .flatten()
        .map(|v| v as f64)
        .filter(|v| *v > 0.0)
        .unwrap_or(100.0)
});

/// Cumulative user+sys CPU seconds consumed by the thread so far. Only
/// available on Linux, where the tid is a real OS thread id.
#[cfg(target_os = "linux")]
fn thread_cpu_seconds(tid: i32) -> f64 {
    if tid <= 0 {
        return 0.0;
    }
    let Ok(content) = std::fs::read_to_string(format!("/proc/self/task/{tid}/stat")) else {
        return 0.0;
    };
    // Format: pid (comm) state ppid ... utime stime ...
    // The comm may contain spaces or ')', so everything after the last ')' is
    // the numeric field list, starting with state (field 3).
    let Some((_, fields_str)) = content.rsplit_once(')') else {
        return 0.0;
    };
    let fields: Vec<&str> = fields_str.split_whitespace().collect();
    let get = |i: usize| {
        fields
            .get(i)
            .and_then(|f| f.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    // utime is field 14, stime is field 15: index 11 and 12 after the state.
    (get(11) + get(12)) / *CLK_TCK
}

#[cfg(not(target_os = "linux"))]
fn thread_cpu_seconds(_tid: i32) -> f64 {
    0.0
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn register_touch_snapshot_and_guard_removal() {
        let pgm = Program::seq(&["SELECT 1", "SELECT 2"]);
        let key = 42;
        let namespace = NamespaceName::from_string("test-ns".to_string()).unwrap();

        let guard = register(key, namespace, &pgm);
        let entries = snapshot();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.conn_id, key);
        assert_eq!(entry.kind, "query");
        assert_eq!(entry.namespace, "test-ns");
        assert_eq!(entry.current_sql, "SELECT 1");
        assert_eq!(entry.total_steps, 2);
        assert_eq!(entry.state, "running");
        assert!(entry.elapsed_ms < 1000);

        touch(key, 1, 2, "SELECT 2");
        let entry = &snapshot()[0];
        assert_eq!(entry.current_step, 1);
        assert_eq!(entry.current_sql, "SELECT 2");

        heartbeat(key);
        let entry = &snapshot()[0];
        assert_eq!(entry.state, "running");

        drop(guard);
        assert!(snapshot().is_empty());
    }

    #[test]
    fn truncates_long_sql() {
        let long_sql = "SELECT ".repeat(100);
        assert!(truncate(&long_sql).len() <= MAX_SQL_LENGTH + 3);
    }

    #[test]
    fn register_sync_job() {
        let namespace = NamespaceName::from_string("replica-ns".to_string()).unwrap();
        let guard = register_job(7, namespace, "sync frames: frame 42".to_string());
        let entry = &snapshot()[0];
        assert_eq!(entry.conn_id, 7);
        assert_eq!(entry.kind, "sync");
        assert_eq!(entry.namespace, "replica-ns");
        assert_eq!(entry.current_sql, "sync frames: frame 42");
        assert_eq!(entry.total_steps, 0);
        drop(guard);
        assert!(snapshot().is_empty());
    }
}
