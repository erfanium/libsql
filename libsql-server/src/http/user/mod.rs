pub mod db_factory;
mod dump;
mod extract;
mod listen;
mod result_builder;
mod sync;
mod trace;
mod types;
#[macro_use]
pub mod timing;

use std::sync::Arc;

use anyhow::Context;
use axum::extract::{FromRequest, Path as AxumPath, State as AxumState};
use axum::http::HeaderValue;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{middleware, Router};
use axum_extra::middleware::option_layer;
use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use hyper::{header, Body, Request, Response, StatusCode};
use libsql_replication::rpc::replication::replication_log_server::{
    ReplicationLog, ReplicationLogServer,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Number;
use tonic::transport::Server;

use tower_http::compression::predicate::NotForContentType;
use tower_http::compression::{DefaultPredicate, Predicate};
use tower_http::{compression::CompressionLayer, cors};

use crate::config::AdminApiConfig;
use crate::connection::{Connection, RequestContext};
use crate::error::Error;
use crate::http::user::db_factory::MakeConnectionExtractorPath;
use crate::http::user::timing::timings_middleware;
use crate::http::user::types::HttpQuery;
use crate::metrics::LEGACY_HTTP_CALL;
use crate::namespace::NamespaceStore;
use crate::net::{Accept, Connector};
use crate::query::{self, Query};
use crate::query_analysis::{predict_final_state, Statement, TxnStatus};
use crate::query_result_builder::QueryResultBuilder;
use crate::rpc::proxy::rpc::proxy_server::{Proxy, ProxyServer};
use crate::rpc::replication::replication_log::ReplicationLogService;
use crate::schema::{MigrationDetails, MigrationSummary};
use crate::utils::services::idle_shutdown::IdleShutdownKicker;
use crate::version;
use crate::{hrana, TaskManager};

use self::db_factory::MakeConnectionExtractor;
use self::result_builder::JsonHttpPayloadBuilder;
use self::types::QueryObject;

impl TryFrom<query::Value> for serde_json::Value {
    type Error = Error;

    fn try_from(value: query::Value) -> Result<Self, Self::Error> {
        let value = match value {
            query::Value::Null => serde_json::Value::Null,
            query::Value::Integer(i) => serde_json::Value::Number(Number::from(i)),
            query::Value::Real(x) => {
                serde_json::Value::Number(Number::from_f64(x).ok_or_else(|| {
                    Error::DbValueError(format!(
                        "Cannot to convert database value `{x}` to a JSON number"
                    ))
                })?)
            }
            query::Value::Text(s) => serde_json::Value::String(s),
            query::Value::Blob(v) => serde_json::json!({
                "base64": BASE64_STANDARD_NO_PAD.encode(v),
            }),
        };

        Ok(value)
    }
}

/// Encodes a query response rows into json
#[derive(Debug, Serialize)]
#[allow(dead_code)]
struct RowsResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

fn parse_queries(queries: Vec<QueryObject>) -> crate::Result<Vec<Query>> {
    let mut out = Vec::with_capacity(queries.len());
    for query in queries {
        let mut iter = Statement::parse(&query.q);
        let stmt = iter.next().transpose()?.unwrap_or_default();
        if iter.next().is_some() {
            return Err(Error::FailedToParse("found more than one command in a single statement string. It is allowed to issue only one command per string.".to_string()));
        }
        let query = Query {
            stmt,
            params: query.params.0,
            want_rows: true,
        };

        out.push(query);
    }

    // It's too complicated to predict the state of a transaction with savepoints in legacy http,
    // forbid them instead.
    if out
        .iter()
        .any(|q| q.stmt.kind.is_release() || q.stmt.kind.is_release())
    {
        return Err(Error::QueryError(
            "savepoints are not supported in HTTP API, use hrana protocol instead".to_string(),
        ));
    }

    match predict_final_state(TxnStatus::Init, out.iter().map(|q| &q.stmt)) {
        TxnStatus::Txn => {
            return Err(Error::QueryError(
                "interactive transaction not allowed in HTTP queries".to_string(),
            ))
        }
        TxnStatus::Init => (),
        // maybe we should err here, but let's sqlite deal with that.
        TxnStatus::Invalid => (),
    }

    Ok(out)
}

async fn handle_query(
    ctx: RequestContext,
    MakeConnectionExtractor(connection_maker): MakeConnectionExtractor,
    Json(query): Json<HttpQuery>,
) -> Result<axum::response::Response, Error> {
    LEGACY_HTTP_CALL.increment(1);
    let batch = parse_queries(query.statements)?;

    let db = connection_maker.create().await?;

    let builder = JsonHttpPayloadBuilder::new();
    let builder = db
        .execute_batch_or_rollback(batch, ctx, builder, query.replication_index)
        .await?;

    let res = (
        [(header::CONTENT_TYPE, "application/json")],
        builder.into_ret(),
    );
    Ok(res.into_response())
}

async fn show_console(
    AxumState(AppState { enable_console, .. }): AxumState<AppState>,
) -> impl IntoResponse {
    if enable_console {
        Html(std::include_str!("console.html")).into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn handle_health() -> Response<Body> {
    // return empty OK
    Response::new(Body::empty())
}

async fn handle_version() -> Response<Body> {
    let version = version::version();
    Response::new(Body::from(version))
}

async fn handle_fallback() -> impl IntoResponse {
    (StatusCode::NOT_FOUND).into_response()
}

async fn handle_hrana_pipeline(
    AxumState(state): AxumState<AppState>,
    MakeConnectionExtractorPath(connection_maker): MakeConnectionExtractorPath,
    ctx: RequestContext,
    axum::extract::Path((_, version)): axum::extract::Path<(String, String)>,
    req: Request<Body>,
) -> Result<Response<Body>, Error> {
    let hrana_version = match version.as_str() {
        "2" => hrana::Version::Hrana2,
        "3" => hrana::Version::Hrana3,
        _ => return Err(Error::InvalidPath("invalid hrana version".to_string())),
    };
    Ok(state
        .hrana_http_srv
        .handle_request(
            connection_maker,
            ctx,
            req,
            hrana::http::Endpoint::Pipeline,
            hrana_version,
            hrana::Encoding::Json,
        )
        .await?)
}

/// Router wide state that each request has access too via
/// axum's `State` extractor.
#[derive(Clone)]
pub(crate) struct AppState {
    namespaces: NamespaceStore,
    hrana_http_srv: Arc<hrana::http::Server>,
    enable_console: bool,
    disable_default_namespace: bool,
    disable_namespaces: bool,
    primary_url: Option<String>,
}

pub struct UserApi<A, P, S, C> {
    pub http_acceptor: Option<A>,
    pub namespaces: NamespaceStore,
    pub idle_shutdown_kicker: Option<IdleShutdownKicker>,
    pub proxy_service: P,
    pub replication_service: S,
    pub disable_default_namespace: bool,
    pub disable_namespaces: bool,
    pub enable_console: bool,
    pub self_url: Option<String>,
    pub primary_url: Option<String>,
    pub disable_metrics: bool,
    /// The admin API is served next to the user API when it is configured
    pub admin_api_config: Option<AdminApiConfig<C>>,
    pub set_log_level: Option<Box<dyn Fn(&str) -> anyhow::Result<()> + Send + Sync + 'static>>,
}

impl<A, P, S, C> UserApi<A, P, S, C>
where
    A: Accept,
    P: Proxy,
    S: ReplicationLog,
    C: Connector,
{
    pub fn configure(self, task_manager: &mut TaskManager) -> Arc<hrana::http::Server> {
        let hrana_http_srv = Arc::new(hrana::http::Server::new(self.self_url.clone()));

        task_manager.spawn_until_shutdown({
            let server = hrana_http_srv.clone();
            async move {
                server.run_expire().await;
                Ok(())
            }
        });

        if let Some(acceptor) = self.http_acceptor {
            // a replica has no log to serve: its sync requests fail with "not a primary"
            let sync_service = Arc::new(ReplicationLogService::new(
                self.namespaces.clone(),
                self.idle_shutdown_kicker.clone(),
                self.disable_namespaces,
                true,
                false,
            ));

            let state = AppState {
                hrana_http_srv: hrana_http_srv.clone(),
                enable_console: self.enable_console,
                namespaces: self.namespaces.clone(),
                disable_default_namespace: self.disable_default_namespace,
                disable_namespaces: self.disable_namespaces,
                primary_url: self.primary_url.clone(),
            };

            macro_rules! handle_hrana {
                ($endpoint:expr, $version:expr, $encoding:expr,) => {{
                    async fn handle_hrana(
                        AxumState(state): AxumState<AppState>,
                        MakeConnectionExtractor(connection_maker): MakeConnectionExtractor,
                        ctx: RequestContext,
                        req: Request<Body>,
                    ) -> Result<Response<Body>, Error> {
                        Ok(state
                            .hrana_http_srv
                            .handle_request(
                                connection_maker,
                                ctx,
                                req,
                                $endpoint,
                                $version,
                                $encoding,
                            )
                            .await?)
                    }
                    handle_hrana
                }};
            }

            let app = Router::new()
                .route("/", post(handle_query))
                .route("/version", get(handle_version))
                .route("/console", get(show_console))
                .route("/health", get(handle_health))
                .route("/dump", get(dump::handle_dump))
                .route("/beta/listen", get(listen::handle_listen))
                .route("/v2", get(crate::hrana::http::handle_index))
                .route(
                    "/v2/pipeline",
                    post(handle_hrana!(
                        hrana::http::Endpoint::Pipeline,
                        hrana::Version::Hrana2,
                        hrana::Encoding::Json,
                    )),
                )
                .route("/v3", get(crate::hrana::http::handle_index))
                .route(
                    "/v3/pipeline",
                    post(handle_hrana!(
                        hrana::http::Endpoint::Pipeline,
                        hrana::Version::Hrana3,
                        hrana::Encoding::Json,
                    )),
                )
                .route(
                    "/v3/cursor",
                    post(handle_hrana!(
                        hrana::http::Endpoint::Cursor,
                        hrana::Version::Hrana3,
                        hrana::Encoding::Json,
                    )),
                )
                .route("/v3-protobuf", get(crate::hrana::http::handle_index))
                .route(
                    "/v3-protobuf/pipeline",
                    post(handle_hrana!(
                        hrana::http::Endpoint::Pipeline,
                        hrana::Version::Hrana3,
                        hrana::Encoding::Protobuf,
                    )),
                )
                .route(
                    "/v3-protobuf/cursor",
                    post(handle_hrana!(
                        hrana::http::Endpoint::Cursor,
                        hrana::Version::Hrana3,
                        hrana::Encoding::Protobuf,
                    )),
                )
                // turso dev routes
                .route(
                    "/dev/:namespace/v:version/pipeline",
                    post(handle_hrana_pipeline),
                )
                .route("/v1/jobs", get(handle_get_migrations))
                .route("/v1/jobs/:job_id", get(handle_get_migration_details))
                .layer(middleware::from_fn(timings_middleware))
                .route(
                    "/sync",
                    get(move |AxumState(state), headers, query| {
                        sync::handle_sync(sync_service.clone(), state, headers, query)
                    }),
                )
                .with_state(state)
                .merge(crate::http::metrics::router(self.disable_metrics));

            let app = match self.admin_api_config {
                Some(config) => app.merge(crate::http::admin::router(
                    hrana_http_srv.clone(),
                    self.namespaces.clone(),
                    config.connector,
                    config.auth_key.into(),
                    self.set_log_level,
                )),
                None => app,
            };

            // Merge the grpc based axum router into our regular http router
            let replication = ReplicationLogServer::new(self.replication_service);
            let write_proxy = ProxyServer::new(self.proxy_service);

            let grpc_router = Server::builder()
                .accept_http1(true)
                .add_service(tonic_web::enable(replication))
                .add_service(tonic_web::enable(write_proxy))
                .into_router();

            let router = app.merge(grpc_router);

            let router = router
                .layer(option_layer(self.idle_shutdown_kicker.clone()))
                .layer(
                    tower_http::trace::TraceLayer::new_for_grpc()
                        .on_eos(trace::eos)
                        .on_request(trace::request)
                        .on_response(trace::response)
                        .on_failure(trace::failure),
                )
                .layer(CompressionLayer::new().compress_when(
                    // TODO: remove this when we upgrade tower-http to 0.5.3
                    DefaultPredicate::new().and(NotForContentType::new("text/event-stream")),
                ))
                .layer(
                    cors::CorsLayer::new()
                        .allow_methods(cors::AllowMethods::any())
                        .allow_headers(cors::Any)
                        .allow_origin(cors::Any),
                );

            let router = router.fallback(handle_fallback);
            let h2c = crate::h2c::H2cMaker::new(router);

            task_manager.spawn_with_shutdown_notify(|shutdown| async move {
                hyper::server::Server::builder(acceptor)
                    .serve(h2c)
                    .with_graceful_shutdown(shutdown.notified())
                    .await
                    .context("http server")?;
                Ok(())
            });
        }
        hrana_http_srv
    }
}

#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct Json<T>(pub T);

#[tonic::async_trait]
impl<S, T, B> FromRequest<S, B> for Json<T>
where
    T: DeserializeOwned,
    B: hyper::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S: Send + Sync,
{
    type Rejection = axum::extract::rejection::JsonRejection;

    async fn from_request(mut req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let headers = req.headers_mut();

        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        axum::Json::from_request(req, state)
            .await
            .map(|t| Json(t.0))
    }
}

async fn handle_get_migrations(
    AxumState(app_state): AxumState<AppState>,
    ctx: RequestContext,
) -> crate::Result<axum::Json<MigrationSummary>> {
    {
        // validate if this is a valid target for the request
        let store = app_state
            .namespaces
            .config_store(ctx.namespace().clone())
            .await?;
        let config = (*store.get()).clone();
        if !config.is_shared_schema {
            tracing::warn!("invalid namespace: target is not a shared schema");
            return Err(Error::InvalidNamespace);
        }
    }

    let meta_store = app_state.namespaces.meta_store();
    let summary = meta_store
        .get_migrations_summary(ctx.namespace().clone())
        .await?;

    Ok(axum::Json(summary))
}

async fn handle_get_migration_details(
    AxumState(app_state): AxumState<AppState>,
    AxumPath(job_id): AxumPath<u64>,
    ctx: RequestContext,
) -> crate::Result<axum::Json<MigrationDetails>> {
    {
        // validate if this is a valid target for the request
        let store = app_state
            .namespaces
            .config_store(ctx.namespace().clone())
            .await?;
        let config = (*store.get()).clone();
        if !config.is_shared_schema {
            tracing::warn!("invalid namespace: target is not a shared schema");
            return Err(Error::InvalidNamespace);
        }
    }

    let meta_store = app_state.namespaces.meta_store();
    let details = meta_store
        .get_migration_details(ctx.namespace().clone(), job_id)
        .await?;
    match details {
        Some(details) => Ok(axum::Json(details)),
        None => Err(crate::Error::MigrationJobNotFound),
    }
}
