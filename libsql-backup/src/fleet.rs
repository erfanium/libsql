//! Backing up many namespaces: listing them on sqld's admin API, one backup per namespace at a
//! time, a bounded number of namespaces at once.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::backup::{Backup, Outcome};
use crate::sync::SyncClient;

#[derive(Clone)]
pub struct AdminClient {
    http: reqwest::Client,
    url: String,
    key: Option<String>,
}

#[derive(Deserialize)]
struct ListNamespacesResp {
    namespaces: Vec<NamespaceListEntry>,
    next: Option<String>,
}

#[derive(Deserialize)]
struct NamespaceListEntry {
    name: String,
    #[serde(default)]
    is_shared_schema: bool,
}

impl AdminClient {
    pub fn new(http: reqwest::Client, url: &str, key: Option<String>) -> Self {
        Self {
            http,
            url: url.trim_end_matches('/').to_string(),
            key,
        }
    }

    /// All the namespaces that can be backed up. Shared schema databases can't be synced.
    pub async fn namespaces(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut req = self
                .http
                .get(format!("{}/v1/namespaces", self.url))
                .query(&[("limit", "1000")]);
            if let Some(after) = &after {
                req = req.query(&[("after", after)]);
            }
            if let Some(key) = &self.key {
                req = req.bearer_auth(key);
            }
            let resp = req.send().await.context("listing namespaces")?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                bail!("listing namespaces failed with {status}: {body}");
            }
            let page: ListNamespacesResp = resp.json().await.context("listing namespaces")?;
            out.extend(
                page.namespaces
                    .into_iter()
                    .filter(|n| !n.is_shared_schema)
                    .map(|n| n.name),
            );
            match page.next {
                Some(next) => after = Some(next),
                None => return Ok(out),
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub struct NamespaceResult {
    pub namespace: String,
    #[serde(flatten)]
    pub result: ResultOrError,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum ResultOrError {
    Ok(Outcome),
    Err { result: &'static str, error: String },
}

impl NamespaceResult {
    fn error(namespace: &str, e: anyhow::Error) -> Self {
        Self {
            namespace: namespace.to_string(),
            result: ResultOrError::Err {
                result: "error",
                error: format!("{e:#}"),
            },
        }
    }

    pub fn is_err(&self) -> bool {
        matches!(self.result, ResultOrError::Err { .. })
    }
}

/// Runs backups, never two of the same namespace at once in this process. Other processes are
/// kept out by the conditional write of the status.
#[derive(Clone)]
pub struct Fleet {
    backup: Backup,
    http: reqwest::Client,
    /// The sqld backed up by `--all` and the scheduler.
    default_sqld: Option<SyncClient>,
    /// Key sent to a sqld when the request does not give one.
    default_key: Option<String>,
    admin: Option<AdminClient>,
    concurrency: usize,
    locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

pub struct FleetOptions {
    pub sqld_url: Option<String>,
    pub default_key: Option<String>,
    pub admin: Option<AdminClient>,
    pub concurrency: usize,
}

impl Fleet {
    pub fn new(backup: Backup, http: reqwest::Client, options: FleetOptions) -> Self {
        let default_key = options.default_key.filter(|k| !k.is_empty());
        let default_sqld = options
            .sqld_url
            .map(|url| SyncClient::new(http.clone(), &url, default_key.clone()));
        Self {
            backup,
            http,
            default_sqld,
            default_key,
            admin: options.admin,
            concurrency: options.concurrency.max(1),
            locks: Default::default(),
        }
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn backup_client(&self) -> &Backup {
        &self.backup
    }

    pub fn default_key(&self) -> Option<&str> {
        self.default_key.as_deref()
    }

    /// A client of the sqld at `source`.
    pub fn sqld(&self, source: &str, key: Option<String>) -> SyncClient {
        SyncClient::new(self.http.clone(), source, key)
    }

    fn default_sqld(&self) -> Result<&SyncClient> {
        self.default_sqld
            .as_ref()
            .context("no sqld to back up from: set --sqld-url")
    }

    pub async fn namespaces(&self) -> Result<Vec<String>> {
        match &self.admin {
            Some(admin) => admin.namespaces().await,
            None => bail!("backing up all namespaces needs sqld's admin API (--sqld-admin-url)"),
        }
    }

    /// Backs up `namespace` from the default sqld.
    pub async fn backup_default(&self, namespace: &str) -> NamespaceResult {
        match self.default_sqld() {
            Ok(sqld) => self.backup_one(sqld, namespace).await,
            Err(e) => NamespaceResult::error(namespace, e),
        }
    }

    pub async fn backup_one(&self, sqld: &SyncClient, namespace: &str) -> NamespaceResult {
        let lock = self
            .locks
            .lock()
            .await
            .entry(namespace.to_string())
            .or_default()
            .clone();
        let result = {
            let _guard = lock.lock().await;
            self.backup.backup(sqld, namespace).await
        };
        // forget the lock when no one else waits on it, the map would grow forever otherwise
        {
            let mut locks = self.locks.lock().await;
            if Arc::strong_count(&lock) == 2 {
                locks.remove(namespace);
            }
        }

        match result {
            Ok(outcome) => {
                tracing::info!(namespace, ?outcome, "backup done");
                NamespaceResult {
                    namespace: namespace.to_string(),
                    result: ResultOrError::Ok(outcome),
                }
            }
            Err(e) => {
                tracing::error!(namespace, "backup failed: {e:#}");
                NamespaceResult::error(namespace, e)
            }
        }
    }

    /// Backs up every namespace of the default sqld.
    pub async fn backup_all(&self) -> Result<Vec<NamespaceResult>> {
        let sqld = self.default_sqld()?;
        let namespaces = self.namespaces().await?;
        tracing::info!("backing up {} namespaces", namespaces.len());
        Ok(futures::stream::iter(namespaces)
            .map(|ns| async move { self.backup_one(sqld, &ns).await })
            .buffer_unordered(self.concurrency)
            .collect()
            .await)
    }
}
