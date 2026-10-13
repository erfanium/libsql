use std::collections::HashSet;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures_core::Future;
pub use libsql_replication::rpc::replication as rpc;
use libsql_replication::rpc::replication::log_offset::WalFlavor;
use libsql_replication::rpc::replication::replication_log_server::ReplicationLog;
use libsql_replication::rpc::replication::sync_response::Message as SyncMessage;
use libsql_replication::rpc::replication::{
    Frame, Frames, HelloRequest, HelloResponse, LogOffset, SyncResponse, SyncTrailer,
    NAMESPACE_DOESNT_EXIST, NEED_SNAPSHOT_ERROR_MSG, NO_HELLO_ERROR_MSG, SESSION_TOKEN_KEY,
};
use md5::{Digest, Md5};
use tokio_stream::StreamExt as _;
use tonic::transport::server::TcpConnectInfo;
use tonic::Status;
use uuid::Uuid;

use crate::connection::config::DatabaseConfig;
use crate::namespace::{NamespaceName, NamespaceStore};
use crate::replication::primary::frame_stream::FrameStream;
use crate::replication::{LogReadError, ReplicationLogger};
use crate::stats::Stats;
use crate::utils::services::idle_shutdown::IdleShutdownKicker;

use crate::rpc::extract_namespace;

pub struct ReplicationLogService {
    namespaces: NamespaceStore,
    idle_shutdown_layer: Option<IdleShutdownKicker>,
    disable_namespaces: bool,
    session_token: Bytes,
    collect_stats: bool,
    // whether this is an internal service. If it is an internal service, auth is checked for
    // proxied requests
    service_internal: bool,

    //deprecated:
    generation_id: Uuid,
    replicas_with_hello: RwLock<HashSet<(SocketAddr, NamespaceName)>>,
}

pub const MAX_FRAMES_PER_BATCH: usize = 1024;

impl ReplicationLogService {
    pub fn new(
        namespaces: NamespaceStore,
        idle_shutdown_layer: Option<IdleShutdownKicker>,
        disable_namespaces: bool,
        collect_stats: bool,
        service_internal: bool,
    ) -> Self {
        let session_token = Uuid::new_v4().to_string().into();
        Self {
            namespaces,
            session_token,
            idle_shutdown_layer,
            disable_namespaces,
            collect_stats,
            generation_id: Uuid::new_v4(),
            replicas_with_hello: Default::default(),
            service_internal,
        }
    }

    fn verify_session_token<R>(
        &self,
        req: &tonic::Request<R>,
        version: usize,
    ) -> Result<(), Status> {
        let no_hello = || Err(Status::failed_precondition(NO_HELLO_ERROR_MSG));
        match req.metadata().get(SESSION_TOKEN_KEY) {
            Some(token) => {
                let session_token_hash = self.encode_session_token(version);

                if token.as_bytes() != session_token_hash.to_string().as_bytes() {
                    return no_hello();
                }
            }
            None => {
                // legacy: old replicas used stateful session management
                let replica_addr = req
                    .remote_addr()
                    .ok_or(Status::internal("No remote RPC address"))?;
                {
                    let namespace = extract_namespace(self.disable_namespaces, req)?;
                    let guard = self.replicas_with_hello.read().unwrap();
                    if !guard.contains(&(replica_addr, namespace)) {
                        return no_hello();
                    }
                }
            }
        }

        Ok(())
    }

    async fn logger_from_namespace<T>(
        &self,
        namespace: NamespaceName,
        req: &tonic::Request<T>,
        verify_session: bool,
    ) -> Result<
        (
            Arc<ReplicationLogger>,
            Arc<DatabaseConfig>,
            usize,
            Arc<Stats>,
            impl Future<Output = ()>,
        ),
        Status,
    > {
        let (logger, config, version, stats, config_changed) = self
            .namespaces
            .with(namespace, |ns| -> Result<_, Status> {
                let logger = ns
                    .db
                    .logger()
                    .ok_or_else(|| Status::invalid_argument("not a primary"))?;
                let config_changed = ns.config_changed();
                let config = ns.config();
                let version = ns.config_version();
                let stats = ns.stats();

                Ok((logger, config, version, stats, config_changed))
            })
            .await
            .map_err(|e| {
                if let crate::error::Error::NamespaceDoesntExist(_) = e.as_ref() {
                    Status::failed_precondition(NAMESPACE_DOESNT_EXIST)
                } else {
                    Status::internal(e.to_string())
                }
            })??;

        if verify_session {
            self.verify_session_token(req, version)?;
        }

        // a replica that syncs is a use of the namespace, even when there is nothing to send
        stats.touch();

        Ok((logger, config, version, stats, config_changed))
    }

    fn encode_session_token(&self, version: usize) -> Uuid {
        let mut sha = Md5::new();
        sha.update(&self.session_token[..]);
        sha.update(version.to_le_bytes());

        let num = sha.finalize();
        let num = u128::from_le_bytes(num.into());
        Uuid::from_u128(num)
    }
}

/// The frames of the replica are not the first ones of the primary's log
pub const LOG_MISMATCH: &str = "LOG_MISMATCH";

pub struct Sync {
    /// about how many frames `messages` has: only good for showing progress
    pub estimated_frame_count: u64,
    pub messages: BoxStream<'static, Result<SyncResponse, Status>>,
}

/// What a replica holds: the query string of a sync request
#[derive(Debug, Default, serde::Deserialize)]
pub struct SyncQuery {
    /// first frame the replica is missing
    #[serde(default)]
    pub next_offset: u64,
    /// id of the log the replica's frames come from, empty when it has none
    #[serde(default)]
    pub log_id: String,
}

impl ReplicationLogService {
    /// The frames that bring a replica to the primary's current index, then the trailer, in one
    /// response. Needs no session token.
    pub async fn sync(&self, namespace: NamespaceName, req: SyncQuery) -> Result<Sync, Status> {
        let (logger, config, _, stats, _) = self
            .logger_from_namespace(namespace, &tonic::Request::new(()), false)
            .await?;

        if config.is_shared_schema && !self.service_internal {
            return Err(tonic::Status::failed_precondition(
                "cannot replicate a shared schema db",
            ));
        }

        let stats = if self.collect_stats {
            Some(stats)
        } else {
            None
        };

        let log_id = logger.log_id().to_string();
        // first frame that the log does not have yet
        let log_end = (*logger.new_frame_notifier.borrow()).map_or(0, |frame_no| frame_no + 1);
        // a replica without a log is cloned. One that holds frames this log cannot continue
        // (other log, or it is ahead of the primary) is refused: it is its own to discard them
        let clone = req.log_id.is_empty();
        if !clone && (req.log_id != log_id || req.next_offset > log_end) {
            return Err(Status::aborted(LOG_MISMATCH));
        }
        let mut offset = if clone { 0 } else { req.next_offset };

        // frames before the log's first one were compacted: they are served from the snapshots
        // that hold them, the rest from the log. A log never compacted has no snapshot directory
        let log_start = logger.log_file.read().header().start_frame_no.get();
        let mut snapshots = Vec::new();
        let mut estimated_frame_count = 0;
        while offset < log_start {
            let snapshot = logger
                .get_snapshot_file(offset)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            let Some(snapshot) = snapshot else { break };
            let header = snapshot.header();
            estimated_frame_count += header.frame_count.get();
            let snapshot_end = header.end_frame_no.get() + 1;
            snapshots.push((snapshot, offset));
            offset = snapshot_end;
        }
        estimated_frame_count += log_end.saturating_sub(offset);

        let log = StreamGuard::new(
            FrameStream::new(logger, offset, false, None, stats.clone())
                .map_err(|e| Status::internal(e.to_string()))?,
            self.idle_shutdown_layer.clone(),
        )
        .map(map_frame_stream_output);

        let frame = |frame: Frame| SyncResponse {
            message: Some(SyncMessage::Frame(frame.data)),
        };
        let stream = async_stream::try_stream! {
            for (snapshot, from) in snapshots {
                let frames = snapshot_stream::make_snapshot_stream(snapshot, from, stats.clone());
                tokio::pin!(frames);
                while let Some(next) = frames.next().await {
                    yield frame(next?);
                }
            }

            // the log is read past the index seen above, up to what is committed by now
            let mut next_offset = offset;
            tokio::pin!(log);
            while let Some(next) = log.next().await {
                yield frame(next?);
                next_offset += 1;
            }

            yield SyncResponse {
                message: Some(SyncMessage::Trailer(SyncTrailer { log_id, next_offset })),
            };
        };

        Ok(Sync {
            estimated_frame_count,
            messages: Box::pin(stream),
        })
    }
}

fn map_frame_stream_output(
    r: Result<(libsql_replication::frame::Frame, Option<DateTime<Utc>>), LogReadError>,
) -> Result<Frame, Status> {
    match r {
        Ok((frame, ts)) => Ok(Frame {
            data: frame.bytes(),
            timestamp: ts.map(|ts| ts.timestamp_millis()),
            durable_frame_no: None,
        }),
        Err(LogReadError::SnapshotRequired) => Err(Status::new(
            tonic::Code::FailedPrecondition,
            NEED_SNAPSHOT_ERROR_MSG,
        )),
        Err(LogReadError::Error(e)) => Err(Status::new(tonic::Code::Internal, e.to_string())),
        // this error should be caught before, but we handle it nicely anyways
        Err(LogReadError::Ahead) => Err(Status::new(
            tonic::Code::OutOfRange,
            "frame not yet available",
        )),
    }
}

pub struct StreamGuard<S> {
    s: S,
    idle_shutdown_layer: Option<IdleShutdownKicker>,
}

impl<S> StreamGuard<S> {
    fn new(s: S, mut idle_shutdown_layer: Option<IdleShutdownKicker>) -> Self {
        if let Some(isl) = idle_shutdown_layer.as_mut() {
            isl.add_connected_replica()
        }
        Self {
            s,
            idle_shutdown_layer,
        }
    }
}

impl<S> Drop for StreamGuard<S> {
    fn drop(&mut self) {
        if let Some(isl) = self.idle_shutdown_layer.as_mut() {
            isl.remove_connected_replica()
        }
    }
}

impl<S: futures::stream::Stream + Unpin> futures::stream::Stream for StreamGuard<S> {
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().s).poll_next(cx)
    }
}

#[tonic::async_trait]
impl ReplicationLog for ReplicationLogService {
    type LogEntriesStream = BoxStream<'static, Result<Frame, Status>>;
    type SnapshotStream = BoxStream<'static, Result<Frame, Status>>;

    async fn log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::LogEntriesStream>, Status> {
        if let WalFlavor::Libsql = req.get_ref().wal_flavor() {
            return Err(Status::invalid_argument("libsql wal not supported"));
        }
        let namespace = super::super::extract_namespace(self.disable_namespaces, &req)?;

        let (logger, _, _, stats, config_changed) = self
            .logger_from_namespace(namespace.clone(), &req, true)
            .await?;

        let stats = if self.collect_stats {
            Some(stats)
        } else {
            None
        };

        let req = req.into_inner();

        let mut stream = StreamGuard::new(
            FrameStream::new(logger, req.next_offset, true, None, stats)
                .map_err(|e| Status::internal(e.to_string()))?,
            self.idle_shutdown_layer.clone(),
        )
        .map(map_frame_stream_output);

        // if only tokio_stream had futures::Stream::take_until...
        let stream = async_stream::stream! {
            tokio::pin!(config_changed);
            loop {
                tokio::select! {
                    _ = &mut config_changed => {
                        break
                    },
                    Some(next) = stream.next() => {
                        yield next
                    }
                    else => break,
                }
            }
        };

        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn batch_log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Frames>, Status> {
        if let WalFlavor::Libsql = req.get_ref().wal_flavor() {
            return Err(Status::invalid_argument("libsql wal not supported"));
        }
        let namespace = super::super::extract_namespace(self.disable_namespaces, &req)?;

        let (logger, _, _, stats, _) = self
            .logger_from_namespace(namespace.clone(), &req, true)
            .await?;

        let stats = if self.collect_stats {
            Some(stats)
        } else {
            None
        };

        let req = req.into_inner();

        let frames = StreamGuard::new(
            FrameStream::new(
                logger,
                req.next_offset,
                false,
                Some(MAX_FRAMES_PER_BATCH),
                stats,
            )
            .map_err(|e| Status::internal(e.to_string()))?,
            self.idle_shutdown_layer.clone(),
        )
        .map(map_frame_stream_output)
        .collect::<Result<Vec<_>, _>>()
        .await?;

        Ok(tonic::Response::new(Frames { frames }))
    }

    async fn hello(
        &self,
        req: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloResponse>, Status> {
        let namespace = super::super::extract_namespace(self.disable_namespaces, &req)?;

        // legacy support
        if req.get_ref().handshake_version.is_none() {
            req.extensions().get::<TcpConnectInfo>().unwrap();
            let replica_addr = req
                .remote_addr()
                .ok_or(Status::internal("No remote RPC address"))?;

            {
                let mut guard = self.replicas_with_hello.write().unwrap();
                guard.insert((replica_addr, namespace.clone()));
            }
        }
        let (logger, config, version, _, _) =
            self.logger_from_namespace(namespace, &req, false).await?;

        // If we are a shared schema and serving externally (aka to embedded replica's) then
        // return an error.
        if config.is_shared_schema && !self.service_internal {
            return Err(tonic::Status::failed_precondition(
                "cannot replicate a shared schema db",
            ));
        }

        let session_hash = self.encode_session_token(version);

        let response = HelloResponse {
            log_id: logger.log_id().to_string(),
            session_token: session_hash.to_string().into(),
            generation_id: self.generation_id.to_string(),
            generation_start_index: 0,
            current_replication_index: *logger.new_frame_notifier.borrow(),
            config: Some(config.as_ref().into()),
        };

        Ok(tonic::Response::new(response))
    }

    async fn snapshot(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, Status> {
        if let WalFlavor::Libsql = req.get_ref().wal_flavor() {
            return Err(Status::invalid_argument("libsql wal not supported"));
        }
        let namespace = super::super::extract_namespace(self.disable_namespaces, &req)?;

        let (logger, _, _, stats, _) = self.logger_from_namespace(namespace, &req, true).await?;

        let stats = if self.collect_stats {
            Some(stats)
        } else {
            None
        };

        let req = req.into_inner();

        let offset = req.next_offset;
        match logger.get_snapshot_file(offset).await {
            Ok(Some(snapshot)) => Ok(tonic::Response::new(Box::pin(
                snapshot_stream::make_snapshot_stream(snapshot, offset, stats),
            ))),
            Ok(None) => Err(Status::new(tonic::Code::Unavailable, "snapshot not found")),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.to_string())),
        }
    }
}

mod snapshot_stream {
    use std::sync::Arc;

    use futures::{Stream, StreamExt};
    use libsql_replication::frame::FrameNo;
    use libsql_replication::rpc::replication::Frame;
    use libsql_replication::snapshot::SnapshotFile;
    use tonic::Status;

    use crate::stats::Stats;

    pub fn make_snapshot_stream(
        snapshot: SnapshotFile,
        offset: FrameNo,
        stats: Option<Arc<Stats>>,
    ) -> impl Stream<Item = Result<Frame, Status>> {
        let size_after = snapshot.header().size_after;
        let frames = snapshot.into_stream_mut_from(offset).peekable();
        async_stream::stream! {
            tokio::pin!(frames);
            while let Some(frame) = frames.next().await {
                match frame {
                    Ok(mut frame) => {
                        // this is the last frame we're sending for this snapshot, set the
                        // frame_no
                        if frames.as_mut().peek().await.is_none() {
                            frame.header_mut().size_after = size_after;
                        }

                        if let Some(stats) = &stats {
                            stats.inc_embedded_replica_frames_replicated();
                        }

                        yield Ok(Frame {
                            data: libsql_replication::frame::Frame::from(frame).bytes(),
                            timestamp: None,
                            durable_frame_no: None,
                        });
                    }
                    Err(e) => {
                        yield Err(Status::new(
                                tonic::Code::Internal,
                                e.to_string(),
                        ));
                        break;
                    }
                }
            }
        }
    }
}
