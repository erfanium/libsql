use axum::body::StreamBody;
use axum::extract::{Path, Query, State};
use axum::middleware::Next;
use axum::routing::delete;
use axum::Json;
use chrono::NaiveDateTime;
use futures::{SinkExt, StreamExt, TryStreamExt};
use hyper::{Body, Request, StatusCode};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::io::{CopyToBytes, ReaderStream, SinkWriter};
use tokio_util::sync::PollSender;
use tower_http::trace::DefaultOnResponse;
use url::Url;

use crate::connection::config::{DatabaseConfig, DurabilityMode};
use crate::error::{Error, LoadDumpError};
use crate::hrana;
use crate::namespace::{DumpStream, NamespaceName, NamespaceStore, RestoreOption};
use crate::net::Connector;
use crate::replication::ReplicationInfo;
use crate::LIBSQL_PAGE_SIZE;

pub mod openapi;
pub mod stats;

struct AppState<C> {
    namespaces: NamespaceStore,
    user_http_server: Arc<hrana::http::Server>,
    connector: C,
    set_env_filter: Option<Box<dyn Fn(&str) -> anyhow::Result<()> + Sync + Send + 'static>>,
}

/// The admin API, served on the same port as the user API: every route of it wants the admin
/// key as a bearer token.
pub fn router<C>(
    user_http_server: Arc<hrana::http::Server>,
    namespaces: NamespaceStore,
    connector: C,
    auth: Arc<str>,
    set_env_filter: Option<Box<dyn Fn(&str) -> anyhow::Result<()> + Sync + Send + 'static>>,
) -> axum::Router
where
    C: Connector,
{
    fn trace_request<B>(req: &Request<B>, span: &tracing::Span) {
        let _s = span.enter();

        tracing::debug!("{} {} {:?}", req.method(), req.uri(), req.headers());
    }

    use axum::routing::{get, post};
    let router = axum::Router::new()
        .route("/v1/namespaces", get(handle_list_namespaces))
        .route(
            "/v1/namespaces/:namespace/config",
            get(handle_get_config).post(handle_post_config),
        )
        .route(
            "/v1/namespaces/:namespace/fork/:to",
            post(handle_fork_namespace),
        )
        .route(
            "/v1/namespaces/:namespace/create",
            post(handle_create_namespace),
        )
        .route(
            "/v1/namespaces/:namespace/checkpoint",
            post(handle_checkpoint),
        )
        .route("/v1/namespaces/:namespace/compact", post(handle_compact))
        .route(
            "/v1/namespaces/:namespace/replication",
            get(handle_replication),
        )
        .route("/v1/namespaces/:namespace", delete(handle_delete_namespace))
        .route("/v1/namespaces/:namespace/stats", get(stats::handle_stats))
        .route("/v1/diagnostics", get(handle_diagnostics))
        .route("/profile/heap/enable", post(enable_profile_heap))
        .route("/profile/heap/disable/:id", post(disable_profile_heap))
        .route("/profile/heap/:id", delete(delete_profile_heap))
        .route("/log-filter", post(handle_set_log_filter))
        .with_state(Arc::new(AppState {
            namespaces: namespaces.clone(),
            connector,
            user_http_server,
            set_env_filter,
        }))
        .layer(
            tower_http::trace::TraceLayer::new_for_http()
                .on_request(trace_request)
                .on_response(
                    DefaultOnResponse::new()
                        .level(tracing::Level::DEBUG)
                        .latency_unit(tower_http::LatencyUnit::Micros),
                ),
        );

    router
        .layer(axum::middleware::from_fn_with_state(auth, auth_middleware))
        // what the API looks like is no secret: the docs are readable without the key
        .merge(openapi::router())
}

async fn auth_middleware<B>(
    State(auth): State<Arc<str>>,
    request: Request<B>,
    next: Next<B>,
) -> Result<axum::response::Response, StatusCode> {
    let key = request
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, key)| key.trim());

    if key != Some(auth.as_ref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(request).await)
}

const DEFAULT_LIST_LIMIT: u64 = 100;
const MAX_LIST_LIMIT: u64 = 1000;

#[derive(Debug, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
struct ListNamespacesParams {
    /// Cursor: list the namespaces whose name sorts after this one. Pass the `next` of the
    /// previous page.
    after: Option<String>,
    /// Page size, 100 by default and at most 1000.
    limit: Option<u64>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
struct ListNamespacesResp {
    namespaces: Vec<NamespaceListEntry>,
    /// Cursor of the next page, absent on the last one.
    next: Option<String>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
struct NamespaceListEntry {
    name: String,
    /// The namespace is a shared schema database: its schema changes go to the databases linked
    /// to it.
    is_shared_schema: bool,
    /// The shared schema database this namespace is linked to, if any: its schema changes are
    /// meant to come from there.
    shared_schema_name: Option<String>,
}

#[utoipa::path(
    get,
    path = "/v1/namespaces",
    tag = "namespaces",
    summary = "List the namespaces, in name order, a page at a time",
    params(ListNamespacesParams),
    responses((status = 200, body = ListNamespacesResp)),
    security(("admin_key" = [])),
)]
async fn handle_list_namespaces<C>(
    State(app_state): State<Arc<AppState<C>>>,
    Query(params): Query<ListNamespacesParams>,
) -> crate::Result<Json<ListNamespacesResp>> {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT);
    let (page, next) = app_state
        .namespaces
        .meta_store()
        .list(params.after, limit)
        .await?;
    let namespaces = page
        .into_iter()
        .map(|(name, config)| NamespaceListEntry {
            name: name.to_string(),
            is_shared_schema: config.is_shared_schema,
            shared_schema_name: config.shared_schema_name.map(|n| n.to_string()),
        })
        .collect();
    Ok(Json(ListNamespacesResp { namespaces, next }))
}

#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/config",
    tag = "namespaces",
    summary = "Configuration of a namespace",
    params(("namespace" = String, Path, description = "Name of the namespace")),
    responses((status = 200, body = HttpDatabaseConfig), (status = 404, description = "No such namespace")),
    security(("admin_key" = [])),
)]
async fn handle_get_config<C: Connector>(
    State(app_state): State<Arc<AppState<C>>>,
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
        allow_attach: config.allow_attach,
        txn_timeout_s: config.txn_timeout.map(|d| d.as_secs() as u64),
        durability_mode: Some(config.durability_mode),
    };
    Ok(Json(resp))
}

#[utoipa::path(
    get,
    path = "/v1/diagnostics",
    tag = "server",
    summary = "State of the connection of every open Hrana stream",
    responses((status = 200, body = Vec<String>)),
    security(("admin_key" = [])),
)]
async fn handle_diagnostics<C>(
    State(app_state): State<Arc<AppState<C>>>,
) -> crate::Result<Json<Vec<String>>> {
    use crate::connection::Connection;
    use hrana::http::stream;

    let server = app_state.user_http_server.as_ref();
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

#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
struct HttpDatabaseConfig {
    block_reads: bool,
    block_writes: bool,
    #[serde(default)]
    block_reason: Option<String>,
    #[serde(default)]
    #[schema(value_type = Option<String>, example = "1GB")]
    max_db_size: Option<bytesize::ByteSize>,
    #[serde(default)]
    heartbeat_url: Option<String>,
    #[serde(default)]
    allow_attach: bool,
    #[serde(default)]
    txn_timeout_s: Option<u64>,
    #[serde(default)]
    durability_mode: Option<DurabilityMode>,
}

#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/config",
    tag = "namespaces",
    summary = "Update the configuration of a namespace",
    params(("namespace" = String, Path, description = "Name of the namespace")),
    request_body = HttpDatabaseConfig,
    responses((status = 200)),
    security(("admin_key" = [])),
)]
async fn handle_post_config<C>(
    State(app_state): State<Arc<AppState<C>>>,
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

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct CreateNamespaceReq {
    #[schema(value_type = Option<String>)]
    dump_url: Option<Url>,
    #[schema(value_type = Option<String>, example = "1GB")]
    max_db_size: Option<bytesize::ByteSize>,
    heartbeat_url: Option<String>,
    bottomless_db_id: Option<String>,
    txn_timeout_s: Option<u64>,
    max_row_size: Option<u64>,
    /// If true, current namespace acts as a DB used solely for multi-db schema updates.
    #[serde(default)]
    shared_schema: bool,
    /// If some, this is a [NamespaceName] reference to a shared schema DB.
    #[serde(default)]
    #[schema(value_type = Option<String>)]
    shared_schema_name: Option<NamespaceName>,
    #[serde(default)]
    allow_attach: bool,
    #[serde(default)]
    durability_mode: Option<DurabilityMode>,
}

#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/create",
    tag = "namespaces",
    summary = "Create a namespace",
    params(("namespace" = String, Path, description = "Name of the namespace")),
    request_body = CreateNamespaceReq,
    responses((status = 200)),
    security(("admin_key" = [])),
)]
async fn handle_create_namespace<C: Connector>(
    State(app_state): State<Arc<AppState<C>>>,
    Path(namespace): Path<NamespaceName>,
    Json(req): Json<CreateNamespaceReq>,
) -> crate::Result<()> {
    let mut config = DatabaseConfig::default();

    if req.shared_schema_name.is_some() && req.dump_url.is_some() {
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
        // TODO: move this check into meta store
        if !app_state.namespaces.exists(&ns).await {
            return Err(Error::NamespaceDoesntExist(ns.to_string()));
        }

        config.shared_schema_name = Some(ns);
    }

    let dump = match req.dump_url {
        Some(ref url) => {
            RestoreOption::Dump(dump_stream_from_url(url, app_state.connector.clone()).await?)
        }
        None => RestoreOption::Latest,
    };

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

    app_state.namespaces.create(namespace, dump, config).await?;

    Ok(())
}

#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct ForkNamespaceReq {
    timestamp: NaiveDateTime,
}

#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/fork/{to}",
    tag = "namespaces",
    summary = "Copy a namespace into a new one",
    params(("namespace" = String, Path, description = "Name of the namespace"), ("to" = String, Path, description = "Name of the namespace to create")),
    request_body = Option<ForkNamespaceReq>,
    responses((status = 200)),
    security(("admin_key" = [])),
)]
async fn handle_fork_namespace<C>(
    State(app_state): State<Arc<AppState<C>>>,
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

async fn dump_stream_from_url<C>(url: &Url, connector: C) -> Result<DumpStream, LoadDumpError>
where
    C: Connector,
{
    match url.scheme() {
        "http" | "https" => {
            let client = hyper::client::Client::builder().build::<_, Body>(connector);
            let uri = url
                .as_str()
                .parse()
                .map_err(|_| LoadDumpError::InvalidDumpUrl)?;
            let resp = client.get(uri).await?;
            let body = resp
                .into_body()
                .map_err(|e| std::io::Error::new(ErrorKind::Other, e));
            Ok(Box::new(body))
        }
        "file" => {
            let path = PathBuf::from(url.path());
            if !path.is_absolute() {
                return Err(LoadDumpError::DumpFilePathNotAbsolute);
            }

            if !path.try_exists()? {
                return Err(LoadDumpError::DumpFileDoesntExist);
            }

            if !path.is_file() {
                return Err(LoadDumpError::NotAFile);
            }

            let f = tokio::fs::File::open(path).await?;

            Ok(Box::new(ReaderStream::new(f)))
        }
        scheme => Err(LoadDumpError::UnsupportedUrlScheme(scheme.to_string())),
    }
}

#[derive(Deserialize, Default, utoipa::ToSchema)]
struct DeleteNamespaceReq {
    #[serde(default)]
    pub keep_backup: bool,
}

#[utoipa::path(
    delete,
    path = "/v1/namespaces/{namespace}",
    tag = "namespaces",
    summary = "Delete a namespace",
    params(("namespace" = String, Path, description = "Name of the namespace")),
    request_body = Option<DeleteNamespaceReq>,
    responses((status = 200)),
    security(("admin_key" = [])),
)]
async fn handle_delete_namespace<C>(
    State(app_state): State<Arc<AppState<C>>>,
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

#[utoipa::path(
    post,
    path = "/log-filter",
    tag = "server",
    summary = "Replace the log filter of the server, in the syntax of `RUST_LOG`",
    request_body = String,
    responses((status = 200)),
    security(("admin_key" = [])),
)]
async fn handle_set_log_filter<C>(
    State(app_state): State<Arc<AppState<C>>>,
    body: String,
) -> crate::Result<()> {
    if let Some(ref cb) = app_state.set_env_filter {
        cb(&body)?;
    }
    Ok(())
}

#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/compact",
    tag = "namespaces",
    summary = "Compact the replication log of a namespace into a snapshot",
    params(("namespace" = String, Path, description = "Name of the namespace")),
    responses((status = 200)),
    security(("admin_key" = [])),
)]
async fn handle_compact<C>(
    State(app_state): State<Arc<AppState<C>>>,
    Path(namespace): Path<NamespaceName>,
) -> crate::Result<()> {
    app_state.namespaces.compact(namespace).await?;
    Ok(())
}

#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/replication",
    tag = "namespaces",
    summary = "Where the replication log of a namespace is at",
    params(("namespace" = String, Path, description = "Name of the namespace")),
    responses((status = 200, body = ReplicationInfo)),
    security(("admin_key" = [])),
)]
async fn handle_replication<C>(
    State(app_state): State<Arc<AppState<C>>>,
    Path(namespace): Path<NamespaceName>,
) -> crate::Result<Json<ReplicationInfo>> {
    let info = app_state.namespaces.replication_info(namespace).await?;
    Ok(Json(info))
}

#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/checkpoint",
    tag = "namespaces",
    summary = "Checkpoint the WAL of a namespace",
    params(("namespace" = String, Path, description = "Name of the namespace")),
    responses((status = 200)),
    security(("admin_key" = [])),
)]
async fn handle_checkpoint<C>(
    State(app_state): State<Arc<AppState<C>>>,
    Path(namespace): Path<NamespaceName>,
) -> crate::Result<()> {
    app_state.namespaces.checkpoint(namespace).await?;
    Ok(())
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct EnableHeapProfileRequest {
    #[serde(default)]
    max_stack_depth: Option<usize>,
    #[serde(default)]
    max_trackers: Option<usize>,
    #[serde(default)]
    tracker_event_buffer_size: Option<usize>,
    #[serde(default)]
    sample_rate: Option<f64>,
}

#[utoipa::path(
    post,
    path = "/profile/heap/enable",
    tag = "server",
    summary = "Start a heap profile",
    request_body = EnableHeapProfileRequest,
    responses((status = 200, body = String, description = "Id of the profile")),
    security(("admin_key" = [])),
)]
async fn enable_profile_heap(Json(req): Json<EnableHeapProfileRequest>) -> crate::Result<String> {
    let path = tokio::task::spawn_blocking(move || {
        rheaper::enable_tracking(rheaper::TrackerConfig {
            max_stack_depth: req.max_stack_depth.unwrap_or(30),
            max_trackers: req.max_trackers.unwrap_or(200),
            tracker_event_buffer_size: req.tracker_event_buffer_size.unwrap_or(5_000),
            sample_rate: req.sample_rate.unwrap_or(1.0),
            profile_dir: PathBuf::from("heap_profile"),
        })
        .map_err(|e| crate::Error::Anyhow(anyhow::anyhow!("{e}")))
    })
    .await??;

    Ok(path.file_name().unwrap().to_str().unwrap().to_string())
}

#[utoipa::path(
    post,
    path = "/profile/heap/disable/{id}",
    tag = "server",
    summary = "Stop a heap profile and download it",
    params(("id" = String, Path, description = "Id of the profile")),
    responses((status = 200, content_type = "application/x-tar")),
    security(("admin_key" = [])),
)]
async fn disable_profile_heap(Path(profile): Path<String>) -> impl axum::response::IntoResponse {
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(1);
    tokio::task::spawn_blocking(move || {
        rheaper::disable_tracking();
        let profile_dir = PathBuf::from("heap_profile").join(&profile);
        let sink =
            PollSender::new(tx).sink_map_err(|_| std::io::Error::from(ErrorKind::BrokenPipe));
        let writer = tokio_util::io::SyncIoBridge::new(SinkWriter::new(CopyToBytes::new(sink)));
        let mut builder = tar::Builder::new(writer);
        if let Err(e) = builder.append_dir_all(&profile, &profile_dir) {
            tracing::error!("io error sending trace: {e}");
            return;
        }
        if let Err(e) = builder.finish() {
            tracing::error!("io error sending trace: {e}");
            return;
        }
    });

    let stream =
        tokio_stream::wrappers::ReceiverStream::new(rx).map(|b| Result::<_, Infallible>::Ok(b));
    let body = StreamBody::new(stream);

    body
}

#[utoipa::path(
    delete,
    path = "/profile/heap/{id}",
    tag = "server",
    summary = "Delete a heap profile",
    params(("id" = String, Path, description = "Id of the profile")),
    responses((status = 200)),
    security(("admin_key" = [])),
)]
async fn delete_profile_heap(Path(profile): Path<String>) -> crate::Result<()> {
    let profile_dir = PathBuf::from("heap_profile").join(&profile);
    tokio::fs::remove_dir_all(&profile_dir).await?;
    Ok(())
}
