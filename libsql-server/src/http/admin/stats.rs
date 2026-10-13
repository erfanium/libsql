use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;

use axum::extract::{Path, State};
use axum::Json;
use uuid::Uuid;

use crate::namespace::NamespaceName;
use crate::replication::FrameNo;
use crate::stats::Stats;

use super::AppState;

#[derive(Serialize, utoipa::ToSchema)]
pub struct StatsResponse {
    #[schema(value_type = Option<String>)]
    pub id: Option<Uuid>,
    pub rows_read_count: u64,
    pub rows_written_count: u64,
    pub storage_bytes_used: u64,
    pub write_requests_delegated: u64,
    pub replication_index: FrameNo,
    pub embedded_replica_frames_replicated: u64,
    pub query_count: u64,
    pub elapsed_ms: f64,
    /// null if the namespace was never used
    pub last_used: Option<DateTime<Utc>>,
}

impl From<&Stats> for StatsResponse {
    fn from(stats: &Stats) -> Self {
        Self {
            id: stats.id(),
            rows_read_count: stats.rows_read(),
            rows_written_count: stats.rows_written(),
            storage_bytes_used: stats.storage_bytes_used(),
            write_requests_delegated: stats.write_requests_delegated(),
            replication_index: stats.get_current_frame_no(),
            embedded_replica_frames_replicated: stats.get_embedded_replica_frames_replicated(),
            query_count: stats.get_query_count(),
            elapsed_ms: stats.get_query_latency() as f64 / 1_000.0,
            last_used: stats.last_used(),
        }
    }
}

impl From<Stats> for StatsResponse {
    fn from(stats: Stats) -> Self {
        (&stats).into()
    }
}

#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/stats",
    tag = "namespaces",
    summary = "Usage statistics of a namespace",
    params(("namespace" = String, Path, description = "Name of the namespace")),
    responses((status = 200, body = StatsResponse)),
    security(("admin_key" = [])),
)]
pub(super) async fn handle_stats<C>(
    State(app_state): State<Arc<AppState<C>>>,
    Path(namespace): Path<String>,
) -> crate::Result<Json<StatsResponse>> {
    let stats = app_state
        .namespaces
        .stats(NamespaceName::from_string(namespace)?)
        .await?;
    let resp: StatsResponse = stats.as_ref().into();

    Ok(Json(resp))
}
