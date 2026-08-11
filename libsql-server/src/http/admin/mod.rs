//! Namespace management API and Prometheus metrics.
//!
//! The namespace management routes (`/v1/namespaces/*`) implement the
//! upstream sqld admin API and are registered on the main user router,
//! protected by the admin API key. Prometheus metrics are exported at
//! `/metrics` on the same router; [`init_metrics`] installs the global
//! recorder and keeps the tokio runtime gauges updated.

use std::sync::Mutex;
use std::time::Duration;

use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use chrono::NaiveDateTime;
use hyper::{Body, Request};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::connection::config::{DatabaseConfig, DurabilityMode};
use crate::error::Error;
use crate::http::user::AppState;
use crate::namespace::{NamespaceName, RestoreOption};
use crate::replication::ReplicationInfo;
use crate::LIBSQL_PAGE_SIZE;

pub mod api;
pub mod openapi;
pub mod stats;

/// The global Prometheus handle, installed once by [`init_metrics`].
static PROM_HANDLE: Mutex<Option<PrometheusHandle>> = Mutex::new(None);

/// Initialize the Prometheus recorder and spawn the task that keeps the
/// tokio runtime gauges up to date. Idempotent: only the first call
/// installs the recorder.
pub fn init_metrics() {
    {
        let mut handle = PROM_HANDLE.lock().unwrap();
        if handle.is_some() {
            return;
        }

        tracing::info!("initializing prometheus metrics");
        let app_label = std::env::var("SQLD_APP_LABEL").ok();
        let ver = env!("CARGO_PKG_VERSION");

        let builder = PrometheusBuilder::new().idle_timeout(
            metrics_util::MetricKindMask::ALL,
            Some(Duration::from_secs(120)),
        );
        let builder = match app_label {
            Some(app_label) => builder
                .add_global_label("app", app_label)
                .add_global_label("version", ver),
            None => builder,
        };
        let prom_handle = builder
            .install_recorder()
            .expect("metrics recorder can only be installed once");
        *handle = Some(prom_handle);
    }

    tokio::task::spawn(async move {
        loop {
            let runtime = tokio::runtime::Handle::current();
            let metrics = runtime.metrics();
            crate::metrics::TOKIO_RUNTIME_BLOCKING_QUEUE_DEPTH
                .set(metrics.blocking_queue_depth() as f64);
            crate::metrics::TOKIO_RUNTIME_INJECTION_QUEUE_DEPTH
                .set(metrics.injection_queue_depth() as f64);
            crate::metrics::TOKIO_RUNTIME_NUM_BLOCKING_THREADS
                .set(metrics.num_blocking_threads() as f64);
            crate::metrics::TOKIO_RUNTIME_NUM_IDLE_BLOCKING_THREADS
                .set(metrics.num_idle_blocking_threads() as f64);
            crate::metrics::TOKIO_RUNTIME_NUM_WORKERS.set(metrics.num_workers() as f64);

            crate::metrics::TOKIO_RUNTIME_IO_DRIVER_FD_DEREGISTERED_COUNT
                .absolute(metrics.io_driver_fd_deregistered_count() as u64);
            crate::metrics::TOKIO_RUNTIME_IO_DRIVER_FD_REGISTERED_COUNT
                .absolute(metrics.io_driver_fd_registered_count() as u64);
            crate::metrics::TOKIO_RUNTIME_IO_DRIVER_READY_COUNT
                .absolute(metrics.io_driver_ready_count() as u64);
            crate::metrics::TOKIO_RUNTIME_REMOTE_SCHEDULE_COUNT
                .absolute(metrics.remote_schedule_count() as u64);

            crate::metrics::SERVER_COUNT.set(1.0);
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });
}

/// Render all registered Prometheus metrics as text. Served at `/metrics`.
pub async fn render_metrics() -> String {
    PROM_HANDLE
        .lock()
        .unwrap()
        .as_ref()
        .map(|h| h.render())
        .unwrap_or_default()
}

/// Router-wide middleware for the namespace management API: rejects
/// requests without a valid admin key and requests sent while the admin key
/// is not configured.
async fn admin_auth(
    AxumState(admin_key): AxumState<Option<String>>,
    req: Request<Body>,
    next: Next<Body>,
) -> axum::response::Response {
    let Some(admin_key) = &admin_key else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": "admin not configured" })),
        )
            .into_response();
    };
    let auth = req
        .headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED);
    let Ok(auth) = auth else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({ "error": "Unauthorized" })))
            .into_response();
    };
    let mut split = auth.split_whitespace();
    if split.next().map(|s| s.eq_ignore_ascii_case("bearer")) != Some(true)
        || split.next() != Some(admin_key.as_str())
    {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({ "error": "Unauthorized" })))
            .into_response();
    }
    next.run(req).await
}

/// Build the namespace management router, merged into the main user router.
pub fn admin_routes(admin_key: Option<String>) -> Router<AppState> {
    Router::new()
        .route("/v1/namespaces/:namespace/config", get(handle_get_config).post(handle_post_config))
        .route("/v1/namespaces/:namespace/fork/:to", post(handle_fork_namespace))
        .route("/v1/namespaces/:namespace/create", post(handle_create_namespace))
        .route("/v1/namespaces/:namespace/checkpoint", post(handle_checkpoint))
        .route("/v1/namespaces/:namespace/compact", post(handle_compact))
        .route("/v1/namespaces/:namespace/replication", get(handle_replication))
        .route("/v1/namespaces/:namespace", delete(handle_delete_namespace))
        .route("/v1/namespaces/:namespace/stats", get(handle_stats))
        .route("/v1/namespaces/:namespace/stats/:stats_type", delete(handle_delete_stats))
        .route("/v1/diagnostics", get(handle_diagnostics))
        .route("/metrics", get(render_metrics))
        .route_layer(middleware::from_fn_with_state(admin_key, admin_auth))
}

async fn handle_get_index() -> &'static str {
    "Welcome to the sqld admin API"
}

#[derive(Debug, Deserialize, Serialize)]
struct HttpDatabaseConfig {
    block_reads: bool,
    block_writes: bool,
    #[serde(default)]
    block_reason: Option<String>,
    #[serde(default)]
    max_db_size: Option<bytesize::ByteSize>,
    #[serde(default)]
    heartbeat_url: Option<String>,
    #[serde(default)]
    jwt_key: Option<String>,
    #[serde(default)]
    allow_attach: bool,
    #[serde(default)]
    txn_timeout_s: Option<u64>,
    #[serde(default)]
    durability_mode: Option<DurabilityMode>,
}

async fn handle_get_config(
    AxumState(app_state): AxumState<AppState>,
    Path(namespace): Path<String>,
) -> crate::Result<Json<HttpDatabaseConfig>> {
    let store = app_state
        .namespaces
        .config_store(NamespaceName::from_string(namespace)?)
        .await?;
    let config = store.get();
    let max_db_size = bytesize::ByteSize::b(config.max_db_pages * LIBSQL_PAGE_SIZE);
    let resp = HttpDatabaseConfig {
        block_reads: config.block_reads,
        block_writes: config.block_writes,
        block_reason: config.block_reason.clone(),
        max_db_size: Some(max_db_size),
        heartbeat_url: config.heartbeat_url.clone().map(|u| u.into()),
        jwt_key: config.jwt_key.clone(),
        allow_attach: config.allow_attach,
        txn_timeout_s: config.txn_timeout.map(|d| d.as_secs() as u64),
        durability_mode: Some(config.durability_mode),
    };
    Ok(Json(resp))
}

async fn handle_post_config(
    AxumState(app_state): AxumState<AppState>,
    Path(namespace): Path<String>,
    Json(req): Json<HttpDatabaseConfig>,
) -> crate::Result<()> {
    let store = app_state
        .namespaces
        .config_store(NamespaceName::from_string(namespace.clone())?)
        .await?;
    let original = (*store.get()).clone();
    let mut updated = original.clone();
    updated.block_reads = req.block_reads;
    updated.block_writes = req.block_writes;
    updated.block_reason = req.block_reason;
    updated.allow_attach = req.allow_attach;
    updated.txn_timeout = req.txn_timeout_s.map(Duration::from_secs);
    if let Some(size) = req.max_db_size {
        updated.max_db_pages = size.as_u64() / LIBSQL_PAGE_SIZE;
    }
    if let Some(url) = req.heartbeat_url {
        updated.heartbeat_url = Some(Url::parse(&url)?);
    }
    updated.jwt_key = req.jwt_key;
    if let Some(mode) = req.durability_mode {
        updated.durability_mode = mode;
    }

    store.store(updated.clone()).await?;
    tracing::info!(
        message = "updated db config",
        namespace = namespace,
        block_writes_before = original.block_writes,
        block_writes_after = updated.block_writes,
        block_reads_before = original.block_reads,
        block_reads_after = updated.block_reads,
        allow_attach_before = original.allow_attach,
        allow_attach_after = updated.allow_attach,
        max_db_pages_before = original.max_db_pages,
        max_db_pages_after = updated.max_db_pages,
        durability_mode_before = original.durability_mode.to_string(),
        durability_mode_after = updated.durability_mode.to_string(),
    );

    Ok(())
}

#[derive(Debug, Deserialize)]
struct CreateNamespaceReq {
    max_db_size: Option<bytesize::ByteSize>,
    heartbeat_url: Option<String>,
    bottomless_db_id: Option<String>,
    jwt_key: Option<String>,
    txn_timeout_s: Option<u64>,
    max_row_size: Option<u64>,
    /// If true, current namespace acts as a DB used solely for multi-db schema updates.
    #[serde(default)]
    shared_schema: bool,
    /// If some, this is a [NamespaceName] reference to a shared schema DB.
    #[serde(default)]
    shared_schema_name: Option<NamespaceName>,
    #[serde(default)]
    allow_attach: bool,
    #[serde(default)]
    durability_mode: Option<DurabilityMode>,
}

async fn handle_create_namespace(
    AxumState(app_state): AxumState<AppState>,
    Path(namespace): Path<NamespaceName>,
    Json(req): Json<CreateNamespaceReq>,
) -> crate::Result<()> {
    let mut config = DatabaseConfig::default();

    if let Some(jwt_key) = req.jwt_key {
        config.jwt_key = Some(jwt_key);
    }

    if req.shared_schema_name.is_some() && req.bottomless_db_id.is_some() {
        return Err(Error::SharedSchemaUsageError(
            "database using shared schema database cannot be created from a dump".to_string(),
        ));
    }

    if let Some(ns) = req.shared_schema_name {
        if req.shared_schema {
            return Err(Error::SharedSchemaCreationError(
                "shared schema database cannot reference another shared schema".to_string(),
            ));
        }
        if !app_state.namespaces.exists(&ns).await {
            return Err(Error::NamespaceDoesntExist(ns.to_string()));
        }

        config.shared_schema_name = Some(ns);
    }

    config.bottomless_db_id = req.bottomless_db_id;
    config.is_shared_schema = req.shared_schema;
    config.heartbeat_url = req.heartbeat_url.as_deref().map(Url::parse).transpose()?;
    config.txn_timeout = req.txn_timeout_s.map(Duration::from_secs);
    config.max_row_size = req.max_row_size.unwrap_or(config.max_row_size);
    config.allow_attach = req.allow_attach;
    if let Some(max_db_size) = req.max_db_size {
        config.max_db_pages = max_db_size.as_u64() / LIBSQL_PAGE_SIZE;
    }
    config.durability_mode = req.durability_mode.unwrap_or(DurabilityMode::default());

    app_state
        .namespaces
        .create(namespace, RestoreOption::Latest, config)
        .await?;

    Ok(())
}

#[derive(Debug, Deserialize)]
struct ForkNamespaceReq {
    timestamp: NaiveDateTime,
}

async fn handle_fork_namespace(
    AxumState(app_state): AxumState<AppState>,
    Path((from, to)): Path<(String, String)>,
    req: Option<Json<ForkNamespaceReq>>,
) -> crate::Result<()> {
    let timestamp = req.map(|v| v.timestamp);
    let from = NamespaceName::from_string(from)?;
    let to = NamespaceName::from_string(to)?;
    let from_store = app_state.namespaces.config_store(from.clone()).await?;
    let from_config = from_store.get();
    if from_config.is_shared_schema {
        return Err(Error::SharedSchemaUsageError(
            "database cannot be forked from a shared schema".to_string(),
        ));
    }
    let to_config = (*from_config).clone();
    app_state
        .namespaces
        .fork(from, to, to_config, timestamp)
        .await?;

    Ok(())
}

#[derive(Deserialize, Default)]
struct DeleteNamespaceReq {
    #[serde(default)]
    pub keep_backup: bool,
}

async fn handle_delete_namespace(
    AxumState(app_state): AxumState<AppState>,
    Path(namespace): Path<String>,
    payload: Option<Json<DeleteNamespaceReq>>,
) -> crate::Result<()> {
    let prune_all = match payload {
        Some(req) => !req.keep_backup,
        None => true,
    };

    app_state
        .namespaces
        .destroy(NamespaceName::from_string(namespace)?, prune_all)
        .await?;
    Ok(())
}

async fn handle_checkpoint(
    AxumState(app_state): AxumState<AppState>,
    Path(namespace): Path<NamespaceName>,
) -> crate::Result<()> {
    app_state.namespaces.checkpoint(namespace).await?;
    Ok(())
}

async fn handle_compact(
    AxumState(app_state): AxumState<AppState>,
    Path(namespace): Path<NamespaceName>,
) -> crate::Result<()> {
    app_state.namespaces.compact(namespace).await?;
    Ok(())
}

async fn handle_replication(
    AxumState(app_state): AxumState<AppState>,
    Path(namespace): Path<NamespaceName>,
) -> crate::Result<Json<ReplicationInfo>> {
    let info = app_state.namespaces.replication_info(namespace).await?;
    Ok(Json(info))
}

async fn handle_stats(
    AxumState(app_state): AxumState<AppState>,
    Path(namespace): Path<String>,
) -> crate::Result<Json<stats::StatsResponse>> {
    let namespace = NamespaceName::from_string(namespace)?;
    let stats = app_state
        .namespaces
        .with(namespace, |ns| ns.stats())
        .await?;
    let resp: stats::StatsResponse = stats.as_ref().into();

    Ok(Json(resp))
}

async fn handle_delete_stats(
    AxumState(app_state): AxumState<AppState>,
    Path((namespace, stats_type)): Path<(String, String)>,
) -> crate::Result<()> {
    let namespace = NamespaceName::from_string(namespace)?;
    let stats = app_state
        .namespaces
        .with(namespace, |ns| ns.stats())
        .await?;
    match stats_type.as_str() {
        "top" => stats.reset_top_queries(),
        "slowest" => stats.reset_slowest_queries(),
        _ => return Err(crate::error::Error::Internal("Invalid stats type".into())),
    }

    Ok(())
}

async fn handle_diagnostics(
    AxumState(app_state): AxumState<AppState>,
) -> crate::Result<Json<Vec<String>>> {
    use crate::connection::Connection;
    use crate::hrana::http::stream;

    let server = app_state.hrana_http_srv.as_ref();
    let stream_state = server.stream_state().lock();
    let handles = stream_state.handles();
    let mut diagnostics: Vec<String> = Vec::with_capacity(handles.len());
    for handle in handles.values() {
        let handle_info: String = match handle {
            stream::Handle::Available(stream) => match &stream.db {
                Some(db) => db.diagnostics(),
                None => "[BUG] available-but-closed".into(),
            },
            stream::Handle::Acquired => "acquired".into(),
            stream::Handle::Expired => "expired".into(),
        };
        diagnostics.push(handle_info);
    }
    drop(stream_state);

    tracing::trace!("diagnostics: {diagnostics:?}");
    Ok(Json(diagnostics))
}
