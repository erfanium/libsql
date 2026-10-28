use std::io;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use axum::extract::State as AxumState;
use axum::response::{IntoResponse, Response};
use axum::Json;
use futures::TryStreamExt;
use hyper::{Body, Request, StatusCode};
use serde_json::json;
use tokio_stream::StreamExt as _;
use tokio_util::codec::FramedRead;
use tokio_util::io::StreamReader;

use crate::connection::bulk_load::{apply_load, LoadError, LoadFrameCodec};
use crate::connection::Connection as _;
use crate::error::Error;
use crate::BLOCKING_RT;

use super::db_factory::namespace_from_headers;
use super::AppState;

const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const READ_BUFFER_BYTES: usize = 256 * 1024;

pub(super) async fn handle_load(
    AxumState(state): AxumState<AppState>,
    req: Request<Body>,
) -> crate::Result<Response> {
    let (parts, body) = req.into_parts();
    let namespace = namespace_from_headers(
        &parts.headers,
        state.disable_default_namespace,
        state.disable_namespaces,
    )?;

    let conn_maker = state
        .namespaces
        .with(namespace, |ns| {
            if !ns.db.is_primary() {
                return Err(Error::NotAPrimary);
            }
            let config = ns.config();
            let blocked = config.block_reads
                || config.block_writes
                || ns
                    .db
                    .block_writes()
                    .is_some_and(|b| b.load(Ordering::SeqCst));
            if blocked {
                return Err(Error::Blocked(config.block_reason.clone()));
            }
            Ok(ns.db.connection_maker())
        })
        .await??;

    let conn = conn_maker.create().await?;

    let body = body.timeout(IDLE_TIMEOUT).map(|chunk| match chunk {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(e)) => Err(io::Error::new(io::ErrorKind::Other, e)),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "no data received from the client for too long",
        )),
    });
    let mut frames = FramedRead::with_capacity(
        StreamReader::new(Box::pin(body.into_stream())),
        LoadFrameCodec::default(),
        READ_BUFFER_BYTES,
    );
    let runtime = tokio::runtime::Handle::current();

    let started = Instant::now();
    let result = BLOCKING_RT
        .spawn_blocking(move || {
            let frames = std::iter::from_fn(|| runtime.block_on(frames.next()));
            conn.with_raw(|conn| apply_load(conn, frames))
        })
        .await
        .map_err(|e| Error::Internal(format!("load task failed: {e}")))?;

    let response = match result {
        Ok(summary) => Json(json!({
            "rows": summary.rows,
            "tables": summary.tables,
            "duration_ms": started.elapsed().as_millis() as u64,
        }))
        .into_response(),
        Err(err) => {
            let status = match &err {
                LoadError::Io(e) if e.kind() == io::ErrorKind::TimedOut => {
                    StatusCode::REQUEST_TIMEOUT
                }
                LoadError::Sqlite { source, .. }
                    if source.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy) =>
                {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                _ => StatusCode::BAD_REQUEST,
            };
            tracing::warn!("load failed: {err}");
            (status, Json(json!({ "error": err.to_string() }))).into_response()
        }
    };
    Ok(response)
}
