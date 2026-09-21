use std::fmt::Display;
use std::str::FromStr;

use dialoguer::BasicHistory;
use tokio_stream::StreamExt as _;
use tonic::metadata::AsciiMetadataValue;

use crate::namespace::NamespaceName;
mod rpc {
    #![allow(clippy::all)]
    include!("generated/admin_shell.rs");
}

pub struct AdminShellClient {
    remote_url: String,
    auth: Option<String>,
}

impl AdminShellClient {
    pub fn new(remote_url: String, auth: Option<String>) -> Self {
        Self { remote_url, auth }
    }

    pub async fn run_namespace(&self, namespace: &str) -> anyhow::Result<()> {
        let namespace = NamespaceName::from_string(namespace.to_string())?;
        let mut client = rpc::admin_shell_service_client::AdminShellServiceClient::connect(
            self.remote_url.clone(),
        )
        .await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let req_stream = tokio_stream::wrappers::ReceiverStream::new(receiver);

        let mut req = tonic::Request::new(req_stream);
        req.metadata_mut().insert(
            "x-namespace",
            AsciiMetadataValue::from_str(namespace.as_str()).unwrap(),
        );

        if let Some(ref auth) = self.auth {
            req.metadata_mut().insert(
                "authorization",
                AsciiMetadataValue::from_str(&format!("basic {auth}")).unwrap(),
            );
        }

        let mut resp_stream = client.shell(req).await?.into_inner();

        let mut history = BasicHistory::new();
        loop {
            // this is blocking, but the shell runs in it's own process with no other tasks, so
            // that's ok
            let prompt = dialoguer::Input::<String>::new()
                .with_prompt("> ")
                .history_with(&mut history)
                .interact_text();

            match prompt {
                Ok(query) => {
                    let q = rpc::Query { query };
                    sender.send(q).await?;
                    match resp_stream.next().await {
                        Some(Ok(rpc::Response {
                            resp: Some(rpc::response::Resp::Rows(rows)),
                        })) => {
                            println!("{}", RowsFormatter(rows));
                        }
                        Some(Ok(rpc::Response {
                            resp: Some(rpc::response::Resp::Error(rpc::Error { error })),
                        })) => {
                            println!("query error: {error}");
                        }
                        Some(Err(e)) => {
                            println!("rpc error: {}", e.message());
                            break;
                        }
                        _ => break,
                    }
                }
                Err(e) => println!("error: {e}"),
            }
        }

        Ok(())
    }
}

struct RowsFormatter(rpc::Rows);

impl Display for RowsFormatter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for row in self.0.rows.iter() {
            let mut is_first = true;
            for value in row.values.iter() {
                if !is_first {
                    f.write_str(", ")?;
                }
                is_first = false;

                match value.value.as_ref().unwrap() {
                    rpc::value::Value::Null(_) => f.write_str("null")?,
                    rpc::value::Value::Real(x) => write!(f, "{x}")?,
                    rpc::value::Value::Integer(i) => write!(f, "{i}")?,
                    rpc::value::Value::Text(s) => f.write_str(&s)?,
                    rpc::value::Value::Blob(b) => {
                        for x in b {
                            write!(f, "{x:0x}")?
                        }
                    }
                }
            }

            writeln!(f)?;
        }

        Ok(())
    }
}
