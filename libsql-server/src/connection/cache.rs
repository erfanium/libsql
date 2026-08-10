//! Per-namespace connection cache for the admin query API.
//!
//! `admin_query` used to create a brand-new database connection (SQLite open,
//! WAL wrapper + replication logger attach) for every request, which is the
//! dominant per-request cost and is serialized through the connection
//! throttle. The cache keeps exactly one connection per namespace: the first
//! request creates it, every following request *waits* for it to be released
//! instead of opening a new one. A namespace is therefore always served by a
//! single connection, and connection creation happens only once per
//! namespace (per database generation).
//!
//! Note that the cached connection holds a permit on the connection
//! throttle's semaphore and an open file descriptor for as long as it stays
//! cached.
//!
//! Lifecycle: the cache is owned by the database it belongs to (via
//! [`crate::database::DatabaseWithCache`]). When the database is destroyed,
//! shut down or reloaded, the cache is closed: the idle connection is
//! dropped and all parked `take()` calls return `None`, so a cached
//! connection can never outlive the database generation (WAL/replication
//! log) it belongs to. Connections checked out at the moment of eviction
//! keep the database's WAL wrapper alive for the duration of the request,
//! matching the pre-cache behavior.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

use crate::connection::MakeConnection;
use crate::database::Connection;

pub struct ConnCache {
    closed: AtomicBool,
    /// The cached connection, when idle.
    conn: Mutex<Option<Connection>>,
    /// Whether a connection has been created for this database generation.
    /// The first request observes `false` and is responsible for creating
    /// the connection; everyone else waits for `available`.
    conn_created: AtomicBool,
    /// Woken when a connection is released back to the cache.
    available: Notify,
    /// Creates the connection on the first request.
    maker: Arc<dyn MakeConnection<Connection = Connection>>,
}

impl fmt::Debug for ConnCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnCache")
            .field("closed", &self.closed)
            .field("conn_created", &self.conn_created)
            .finish_non_exhaustive()
    }
}

impl ConnCache {
    pub fn new(maker: Arc<dyn MakeConnection<Connection = Connection>>) -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            conn: Mutex::new(None),
            conn_created: AtomicBool::new(false),
            available: Notify::new(),
            maker,
        })
    }

    /// Wait until the cached connection is available and return it. The
    /// first request per database generation creates the connection; later
    /// requests wait for it to be released instead of creating a new one.
    /// Returns `None` when the cache is closed (the namespace was
    /// evicted/destroyed) or the initial connection creation failed; parked
    /// waiters wake up and fail instead of creating a connection against a
    /// dying namespace.
    pub async fn take(&self) -> Option<Connection> {
        loop {
            if self.closed.load(Ordering::Acquire) {
                return None;
            }
            let notified = self.available.notified();
            let conn = self.conn.lock().await.take();
            if conn.is_some() {
                return conn;
            }
            if self.closed.load(Ordering::Acquire) {
                return None;
            }
            // Slot is empty: either the connection is checked out, or it was
            // never created. Only the first request creates it.
            if !self.conn_created.swap(true, Ordering::AcqRel) {
                return match self.maker.create().await {
                    Ok(conn) => Some(conn),
                    Err(e) => {
                        tracing::error!("error creating cached connection: {e}");
                        self.conn_created.store(false, Ordering::Release);
                        self.available.notify_waiters();
                        None
                    }
                };
            }
            notified.await;
        }
    }

    /// Release the connection back to the cache and wake one waiter, if any.
    /// Connections returned after the cache was closed are dropped.
    ///
    /// The connection's throttle permit is released when it goes idle, so
    /// cached connections don't hold semaphore permits forever (a cache
    /// holding more connections than `SQLD_MAX_CONCURRENT_CONNECTIONS` would
    /// otherwise starve all further connection creation).
    pub async fn put(&self, conn: Connection) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let mut slot = self.conn.lock().await;
        if slot.is_none() {
            let mut conn = conn;
            conn.release_throttle_permit();
            *slot = Some(conn);
            self.available.notify_one();
        }
    }

    /// Close the cache: drop the idle connection and wake all parked
    /// waiters, which observe the closed flag and return `None`. Called when
    /// the owning database is destroyed or shut down.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.conn.lock().await.take();
        self.available.notify_waiters();
    }

    pub fn idle_count(&self) -> usize {
        usize::from(self.conn.try_lock().map(|g| g.is_some()).unwrap_or(false))
    }
}
