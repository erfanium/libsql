use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::Query;
use axum::response::{IntoResponse, Response};
use hyper::header::{HeaderName, CONTENT_TYPE};
use hyper::{HeaderMap, StatusCode};
use libsql_replication::rpc::replication::sync_response::Message as SyncMessage;
use libsql_replication::rpc::replication::{SyncError, SyncResponse, NAMESPACE_DOESNT_EXIST};
use prost::Message as _;
use tokio_stream::StreamExt as _;
use tonic::{Code, Status};

use crate::rpc::replication::replication_log::{ReplicationLogService, SyncQuery};

use super::db_factory::namespace_from_headers;
use super::AppState;

const ESTIMATED_FRAMES: HeaderName = HeaderName::from_static("x-sync-estimated-frames");

/// The replica's position is in the query string, its namespace is the one of any other request.
/// The body is a stream of length-delimited `SyncResponse`s, about how many of them are frames
/// is in a header.
pub(super) async fn handle_sync(
    service: Arc<ReplicationLogService>,
    state: AppState,
    headers: HeaderMap,
    Query(query): Query<SyncQuery>,
) -> Response {
    let namespace = match namespace_from_headers(
        &headers,
        state.disable_default_namespace,
        state.disable_namespaces,
    ) {
        Ok(namespace) => namespace,
        Err(e) => return e.into_response(),
    };

    let sync = match service.sync(namespace, query).await {
        Ok(sync) => sync,
        Err(status) => return error_response(&status),
    };
    let mut messages = sync.messages;

    let body = async_stream::stream! {
        while let Some(message) = messages.next().await {
            // the http status is already sent: the failure is the last message of the body
            let (message, failed) = match message {
                Ok(message) => (message, false),
                Err(status) => {
                    let error = SyncError { message: status.message().to_string() };
                    (SyncResponse { message: Some(SyncMessage::Error(error)) }, true)
                }
            };
            yield Ok::<_, Infallible>(message.encode_length_delimited_to_vec());
            if failed {
                break;
            }
        }
    };

    (
        [
            (CONTENT_TYPE, "application/x-protobuf".to_string()),
            (ESTIMATED_FRAMES, sync.estimated_frame_count.to_string()),
        ],
        axum::body::StreamBody::new(body),
    )
        .into_response()
}

fn error_response(status: &Status) -> Response {
    let code = match status.code() {
        Code::FailedPrecondition if status.message() == NAMESPACE_DOESNT_EXIST => {
            StatusCode::NOT_FOUND
        }
        // LOG_MISMATCH, and nothing else: the replica discards its database on it
        Code::Aborted => StatusCode::CONFLICT,
        Code::InvalidArgument | Code::FailedPrecondition => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let body = serde_json::json!({ "error": status.message() });

    (code, axum::Json(body)).into_response()
}
