//! Shared platform handlers served on the main user router: the in-flight
//! query processlist and `/info`.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;

/// Live process list of in-flight queries across all namespaces: which
/// thread, namespace, statement and step each query is on, how long it has
/// been running, and how much CPU its thread consumed. Mirrors MySQL's
/// `SHOW PROCESSLIST` for debugging which namespaces and jobs burn CPU.
async fn handle_queries() -> axum::response::Response {
    axum::Json(crate::query_registry::snapshot()).into_response()
}

/// The embedded-replica client fetches `GET /info` to learn the current
/// replication generation before syncing frames. The upstream sqld does not
/// implement this endpoint; return 404 so the client falls back to the
/// gRPC replication handshake, matching the reference server behavior.
pub async fn handle_info() -> axum::response::Response {
    StatusCode::NOT_FOUND.into_response()
}

/// Build the platform SQL surface router, merged into the main user router.
pub fn platform_routes() -> Router<crate::http::user::AppState> {
    Router::new()
        .route("/api/queries", get(handle_queries))
        .route("/info", get(handle_info))
}
