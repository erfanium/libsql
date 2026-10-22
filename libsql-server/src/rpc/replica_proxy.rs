use hyper::Uri;
use libsql_replication::rpc::proxy::{
    proxy_client::ProxyClient, proxy_server::Proxy, Ack, DescribeRequest, DescribeResult,
    DisconnectMessage, ExecReq, ExecResp, ExecuteResults, ProgramReq,
};
use tokio_stream::StreamExt;
use tonic::{transport::Channel, Request, Status};

pub struct ReplicaProxyService {
    client: ProxyClient<Channel>,
    disable_namespaces: bool,
}

impl ReplicaProxyService {
    pub fn new(channel: Channel, uri: Uri, disable_namespaces: bool) -> Self {
        let client = ProxyClient::with_origin(channel, uri);
        Self {
            client,
            disable_namespaces,
        }
    }

    /// The request is forwarded as it is: the primary resolves its namespace again
    fn check_namespace<T>(&self, req: &Request<T>) -> Result<(), Status> {
        super::extract_namespace(self.disable_namespaces, req)?;
        Ok(())
    }
}

#[tonic::async_trait]
impl Proxy for ReplicaProxyService {
    type StreamExecStream = tonic::codec::Streaming<ExecResp>;

    async fn stream_exec(
        &self,
        req: tonic::Request<tonic::Streaming<ExecReq>>,
    ) -> Result<tonic::Response<Self::StreamExecStream>, tonic::Status> {
        tracing::debug!("stream_exec");

        let (meta, ext, mut stream) = req.into_parts();
        let stream = async_stream::stream! {
            while let Some(it) = stream.next().await {
                match it {
                    Ok(it) => yield it,
                    Err(e) => {
                        // close the stream on error
                        tracing::warn!("error proxying stream request: {e}");
                        break
                    },
                }
            }
        };
        let req = tonic::Request::from_parts(meta, ext, stream);
        self.check_namespace(&req)?;
        let mut client = self.client.clone();
        client.stream_exec(req).await
    }

    async fn execute(
        &self,
        req: tonic::Request<ProgramReq>,
    ) -> Result<tonic::Response<ExecuteResults>, tonic::Status> {
        tracing::debug!("execute");
        self.check_namespace(&req)?;

        let mut client = self.client.clone();
        client.execute(req).await
    }

    //TODO: also handle cleanup on peer disconnect
    async fn disconnect(
        &self,
        msg: tonic::Request<DisconnectMessage>,
    ) -> Result<tonic::Response<Ack>, tonic::Status> {
        self.check_namespace(&msg)?;

        let mut client = self.client.clone();
        client.disconnect(msg).await
    }

    async fn describe(
        &self,
        req: tonic::Request<DescribeRequest>,
    ) -> Result<tonic::Response<DescribeResult>, tonic::Status> {
        self.check_namespace(&req)?;

        let mut client = self.client.clone();
        client.describe(req).await
    }
}
