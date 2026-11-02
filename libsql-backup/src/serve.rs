//! `serve`: an HTTP API to trigger backups, and an optional scheduler backing up every
//! namespace once per interval.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::{Path, State};
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::fleet::{Fleet, NamespaceResult};
use crate::status;

pub struct ServeOptions {
    pub listen: SocketAddr,
    pub auth_key: Option<String>,
    pub schedule_interval: Option<Duration>,
}

pub async fn serve(fleet: Fleet, options: ServeOptions) -> Result<()> {
    if let Some(interval) = options.schedule_interval {
        tokio::spawn(scheduler(fleet.clone(), interval));
    }

    let auth: Arc<Option<String>> = Arc::new(options.auth_key);
    let api = Router::new()
        .route("/backup", post(backup))
        .route("/status/:namespace", get(get_status))
        .layer(middleware::from_fn_with_state(auth, auth_middleware))
        .with_state(fleet);
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .merge(api);

    tracing::info!("listening on {}", options.listen);
    axum::Server::bind(&options.listen)
        .serve(app.into_make_service())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn auth_middleware<B>(
    State(auth): State<Arc<Option<String>>>,
    request: Request<B>,
    next: Next<B>,
) -> Result<Response, StatusCode> {
    if let Some(expected) = auth.as_deref() {
        let key = request
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, key)| key.trim());
        if key != Some(expected) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(next.run(request).await)
}

fn error(status: StatusCode, e: anyhow::Error) -> Response {
    (
        status,
        Json(serde_json::json!({ "error": format!("{e:#}") })),
    )
        .into_response()
}

#[derive(serde::Deserialize)]
struct BackupRequest {
    namespace: String,
    /// URL of the sqld to back up from, e.g. `http://sqld:8080`.
    source: String,
    /// Bearer key for that sqld. Defaults to `SQLD_DEFAULT_KEY`.
    #[serde(default)]
    key: Option<String>,
}

async fn backup(State(fleet): State<Fleet>, Json(req): Json<BackupRequest>) -> Response {
    if req.namespace.is_empty() || req.source.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            anyhow::anyhow!("`namespace` and `source` are required"),
        );
    }
    let key = match req.key.filter(|k| !k.is_empty()) {
        Some(key) => key,
        None => match fleet.default_key() {
            Some(key) => key.to_string(),
            None => {
                return error(
                    StatusCode::BAD_REQUEST,
                    anyhow::anyhow!("no sqld key: pass `key` or set SQLD_DEFAULT_KEY"),
                )
            }
        },
    };
    let sqld = fleet.sqld(&req.source, Some(key));
    let result = fleet.backup_one(&sqld, &req.namespace).await;
    let status = if result.is_err() {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::OK
    };
    (status, Json(result)).into_response()
}

async fn get_status(State(fleet): State<Fleet>, Path(namespace): Path<String>) -> Response {
    match status::read(fleet.backup_client().store(), &namespace).await {
        Ok(Some(current)) => Json(current.status).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

#[derive(serde::Serialize)]
pub struct Summary {
    pub total: usize,
    pub failed: usize,
    pub results: Vec<NamespaceResult>,
}

impl Summary {
    pub fn new(results: Vec<NamespaceResult>) -> Self {
        Self {
            total: results.len(),
            failed: results.iter().filter(|r| r.is_err()).count(),
            results,
        }
    }
}

/// Backs up every namespace once per `interval`, each at its own offset in the interval so the
/// load on sqld and S3 is spread out.
async fn scheduler(fleet: Fleet, interval: Duration) {
    let concurrency = Arc::new(Semaphore::new(fleet.concurrency()));
    loop {
        let round_start = Instant::now();
        match fleet.namespaces().await {
            Ok(namespaces) => {
                let mut planned: Vec<(Duration, String)> = namespaces
                    .into_iter()
                    .map(|ns| (offset_in(&ns, interval), ns))
                    .collect();
                planned.sort();
                tracing::info!("scheduled {} namespaces over {interval:?}", planned.len());
                for (offset, ns) in planned {
                    tokio::time::sleep_until(round_start + offset).await;
                    let permit = concurrency.clone().acquire_owned().await.unwrap();
                    let fleet = fleet.clone();
                    tokio::spawn(async move {
                        fleet.backup_default(&ns).await;
                        drop(permit);
                    });
                }
            }
            Err(e) => tracing::error!("scheduled backup: {e:#}"),
        }
        tokio::time::sleep_until(round_start + interval).await;
    }
}

/// The same namespace gets the same slot in every round.
fn offset_in(namespace: &str, interval: Duration) -> Duration {
    let mut hasher = DefaultHasher::new();
    namespace.hash(&mut hasher);
    let millis = interval.as_millis().max(1) as u64;
    Duration::from_millis(hasher.finish() % millis)
}
