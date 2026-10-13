use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::Query;
use hyper::header::{HeaderName, HeaderValue, CONTENT_TYPE};
use hyper::{Body, Request, Response, StatusCode};
use libsql_replication::rpc::replication::sync_response::Message as SyncMessage;
use libsql_replication::rpc::replication::{SyncError, SyncResponse, NAMESPACE_DOESNT_EXIST};
use prost::Message as _;
use tokio_stream::StreamExt as _;
use tonic::metadata::MetadataMap;
use tonic::{Code, Status};

use crate::rpc::replication::replication_log::{ReplicationLogService, SyncQuery};

const ESTIMATED_FRAMES: HeaderName = HeaderName::from_static("x-sync-estimated-frames");

/// The replica's position is in the query string, the namespace in the headers. The body is a
/// stream of length-delimited `SyncResponse`s, about how many of them are frames is in a header.
pub(super) async fn handle_sync(
    service: Arc<ReplicationLogService>,
    Query(query): Query<SyncQuery>,
    req: Request<Body>,
) -> Response<Body> {
    let (parts, _) = req.into_parts();
    let mut req = tonic::Request::new(query);
    *req.metadata_mut() = MetadataMap::from_headers(parts.headers);

    let sync = match service.sync(req).await {
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

    let mut response = Response::new(Body::wrap_stream(body));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/x-protobuf"),
    );
    response
        .headers_mut()
        .insert(ESTIMATED_FRAMES, sync.estimated_frame_count.into());
    response
}

fn error_response(status: &Status) -> Response<Body> {
    let code = match status.code() {
        Code::FailedPrecondition if status.message() == NAMESPACE_DOESNT_EXIST => {
            StatusCode::NOT_FOUND
        }
        // LOG_MISMATCH, and nothing else: the replica discards its database on it
        Code::Aborted => StatusCode::CONFLICT,
        Code::InvalidArgument | Code::FailedPrecondition => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let body = serde_json::json!({ "error": status.message() }).to_string();
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = code;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}
