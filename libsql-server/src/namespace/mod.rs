use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use chrono::NaiveDateTime;
use futures_core::{Future, Stream};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::connection::config::DatabaseConfig;
use crate::connection::Connection as _;
use crate::database::Database;
use crate::replication::ReplicationInfo;
use crate::stats::Stats;
use crate::BLOCKING_RT;

use self::meta_store::MetaStoreHandle;
pub use self::name::NamespaceName;
pub use self::store::NamespaceStore;

pub mod broadcasters;
pub(crate) mod configurator;
pub mod meta_store;
mod name;
pub mod replication_wal;
mod schema_lock;
mod store;

pub type ResetCb = Box<dyn Fn(ResetOp) + Send + Sync + 'static>;
pub type ResolveNamespacePathFn =
    Arc<dyn Fn(&NamespaceName) -> crate::Result<Arc<Path>> + Sync + Send + 'static>;

pub enum ResetOp {
    Reset(NamespaceName),
}

#[derive(Clone, Debug)]
pub enum NamespaceBottomlessDbId {
    Namespace(String),
    NotProvided,
}

impl NamespaceBottomlessDbId {
    fn from_config(config: &DatabaseConfig) -> NamespaceBottomlessDbId {
        match config.bottomless_db_id.clone() {
            Some(db_id) => NamespaceBottomlessDbId::Namespace(db_id),
            None => NamespaceBottomlessDbId::NotProvided,
        }
    }
}

#[derive(Clone, Debug)]
pub enum NamespaceBottomlessDbIdInit {
    Provided(NamespaceBottomlessDbId),
    FetchFromConfig,
}

/// A namespace isolates the resources pertaining to a database of type T
#[derive(Debug)]
pub struct Namespace {
    pub db: Database,
    name: NamespaceName,
    /// The set of tasks associated with this namespace
    tasks: JoinSet<anyhow::Result<()>>,
    stats: Arc<Stats>,
    db_config_store: MetaStoreHandle,
    path: Arc<Path>,
}

impl Namespace {
    pub(crate) fn name(&self) -> &NamespaceName {
        &self.name
    }

    async fn destroy(mut self) -> anyhow::Result<()> {
        self.tasks.shutdown().await;
        self.db.destroy();
        Ok(())
    }

    async fn checkpoint(&self) -> anyhow::Result<()> {
        let conn = self.db.connection_maker().create().await?;
        conn.vacuum_if_needed().await?;
        conn.checkpoint().await?;
        Ok(())
    }

    async fn compact(&self) -> anyhow::Result<bool> {
        let Some(logger) = self.db.logger() else {
            anyhow::bail!("namespace is not a primary, cannot compact");
        };
        BLOCKING_RT
            .spawn_blocking(move || logger.force_compact())
            .await?
    }

    fn replication_info(&self) -> ReplicationInfo {
        let mut info = match self.db.logger() {
            Some(logger) => {
                let mut info = logger.replication_info();
                if let Database::Schema(_) = &self.db {
                    info.kind = "schema";
                }
                info
            }
            None => {
                let mut info = ReplicationInfo::replica();
                let ri = self.stats.get_current_frame_no();
                if ri > 0 {
                    info.replication_index = Some(ri);
                }
                info
            }
        };
        if info.replication_index.is_none() {
            let ri = self.stats.get_current_frame_no();
            if ri > 0 {
                info.replication_index = Some(ri);
            }
        }
        info
    }

    async fn shutdown(mut self, should_checkpoint: bool) -> anyhow::Result<()> {
        self.tasks.shutdown().await;
        if should_checkpoint {
            self.checkpoint().await?;
        }
        self.db.shutdown().await?;
        if let Err(e) = tokio::fs::remove_file(self.path.join(".sentinel")).await {
            tracing::error!("unable to remove .sentinel file: {}", e);
        }
        Ok(())
    }

    pub fn config(&self) -> Arc<DatabaseConfig> {
        self.db_config_store.get()
    }

    pub fn config_version(&self) -> usize {
        self.db_config_store.version()
    }


    pub fn stats(&self) -> Arc<Stats> {
        self.stats.clone()
    }

    pub fn config_changed(&self) -> impl Future<Output = ()> {
        self.db_config_store.changed()
    }
}

pub type DumpStream =
    Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static + Unpin>;

#[derive(Default)]
pub enum RestoreOption {
    /// Restore database state from the most recent version found in a backup.
    #[default]
    Latest,
    /// Restore database from SQLite dump.
    Dump(DumpStream),
    /// Restore database state to a backup version equal to specific generation.
    Generation(Uuid),
    /// Restore database state to a backup version present at a specific point in time.
    /// Granularity depends of how frequently WAL log pages are being snapshotted.
    PointInTime(NaiveDateTime),
}
