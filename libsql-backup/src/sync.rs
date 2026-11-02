//! Client of sqld's `GET /sync` route (see `libsql-replication/proto/replication_log.proto`).
//!
//! The body is a stream of length-delimited `SyncResponse`s: frames, then a trailer. A body
//! without its trailer is incomplete and must be discarded.

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use futures::StreamExt;
use prost::Message;
use reqwest::StatusCode;

use crate::frame::Frame;

#[derive(Clone, PartialEq, prost::Message)]
pub struct SyncTrailer {
    #[prost(string, tag = "1")]
    pub log_id: String,
    #[prost(uint64, tag = "2")]
    pub next_offset: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SyncError {
    #[prost(string, tag = "1")]
    pub message: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SyncResponse {
    #[prost(oneof = "sync_response::Message", tags = "1, 2, 3")]
    pub message: Option<sync_response::Message>,
}

pub mod sync_response {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Message {
        #[prost(bytes = "bytes", tag = "1")]
        Frame(bytes::Bytes),
        #[prost(message, tag = "2")]
        Trailer(super::SyncTrailer),
        #[prost(message, tag = "3")]
        Error(super::SyncError),
    }
}

/// Where the replica (us) stands in the primary's log.
#[derive(Debug, Clone)]
pub struct Position {
    pub log_id: String,
    pub next_offset: u64,
}

pub enum SyncOutcome {
    /// The primary's log is not the one our position comes from: start over with a clone.
    LogMismatch,
    Stream(SyncStream),
}

pub enum SyncItem {
    Frame(Frame),
    Trailer(SyncTrailer),
}

#[derive(Clone)]
pub struct SyncClient {
    http: reqwest::Client,
    url: String,
    key: Option<String>,
}

impl SyncClient {
    /// `key` is sent as a bearer token, for a sqld behind an authenticating proxy.
    pub fn new(http: reqwest::Client, base_url: &str, key: Option<String>) -> Self {
        Self {
            http,
            url: format!("{}/sync", base_url.trim_end_matches('/')),
            key: key.filter(|k| !k.is_empty()),
        }
    }

    /// Without a position, sqld clones the whole database from frame 0.
    pub async fn sync(&self, namespace: &str, from: Option<&Position>) -> Result<SyncOutcome> {
        let mut req = self.http.get(&self.url).header("x-namespace", namespace);
        if let Some(key) = &self.key {
            req = req.bearer_auth(key);
        }
        if let Some(from) = from {
            req = req.query(&[
                ("next_offset", from.next_offset.to_string()),
                ("log_id", from.log_id.clone()),
            ]);
        }
        let resp = req
            .send()
            .await
            .with_context(|| format!("GET /sync for `{namespace}`"))?;
        match resp.status() {
            StatusCode::OK => Ok(SyncOutcome::Stream(SyncStream::new(resp))),
            StatusCode::CONFLICT => Ok(SyncOutcome::LogMismatch),
            StatusCode::NOT_FOUND => bail!("namespace `{namespace}` does not exist"),
            status => {
                let body = resp.text().await.unwrap_or_default();
                bail!("GET /sync for `{namespace}` failed with {status}: {body}")
            }
        }
    }
}

pub struct SyncStream {
    body: futures::stream::BoxStream<'static, reqwest::Result<Bytes>>,
    decoder: Decoder,
    done: bool,
}

impl SyncStream {
    fn new(resp: reqwest::Response) -> Self {
        Self {
            body: resp.bytes_stream().boxed(),
            decoder: Decoder::default(),
            done: false,
        }
    }

    /// The next frame, or the trailer, which is always the last item.
    pub async fn next(&mut self) -> Result<SyncItem> {
        if self.done {
            bail!("sync stream already finished");
        }
        loop {
            if let Some(message) = self.decoder.next()? {
                return match message.message {
                    Some(sync_response::Message::Frame(bytes)) => {
                        Ok(SyncItem::Frame(Frame::new(bytes)?))
                    }
                    Some(sync_response::Message::Trailer(trailer)) => {
                        self.done = true;
                        Ok(SyncItem::Trailer(trailer))
                    }
                    Some(sync_response::Message::Error(e)) => {
                        Err(anyhow!("sqld failed the sync: {}", e.message))
                    }
                    // a message this client does not know: skip it, as the protocol says
                    None => continue,
                };
            }
            match self.body.next().await {
                Some(chunk) => self.decoder.push(&chunk.context("reading /sync body")?),
                None => bail!("/sync response ended before its trailer"),
            }
        }
    }
}

/// Splits a byte stream into length-delimited protobuf messages.
#[derive(Default)]
pub(crate) struct Decoder {
    buf: BytesMut,
}

impl Decoder {
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    pub(crate) fn next(&mut self) -> Result<Option<SyncResponse>> {
        let Some((len, varint_len)) = read_varint(&self.buf)? else {
            return Ok(None);
        };
        let len = usize::try_from(len).context("message too large")?;
        if self.buf.len() < varint_len + len {
            return Ok(None);
        }
        self.buf.advance(varint_len);
        let message = self.buf.split_to(len).freeze();
        Ok(Some(
            SyncResponse::decode(message).context("invalid SyncResponse")?,
        ))
    }
}

/// `None` when the buffer does not hold the whole varint yet.
fn read_varint(buf: &[u8]) -> Result<Option<(u64, usize)>> {
    let mut value = 0u64;
    for (i, byte) in buf.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok(Some((value, i + 1)));
        }
    }
    if buf.len() >= 10 {
        bail!("invalid length prefix in /sync body");
    }
    Ok(None)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frame::test::frame;
    use libsql_replication::rpc::replication as theirs;

    fn encode_theirs() -> Vec<u8> {
        let mut out = Vec::new();
        for f in [frame(1, 1, 0, 1), frame(2, 2, 2, 2)] {
            let msg = theirs::SyncResponse {
                message: Some(theirs::sync_response::Message::Frame(
                    f.as_bytes().to_vec().into(),
                )),
            };
            out.extend(msg.encode_length_delimited_to_vec());
        }
        let trailer = theirs::SyncResponse {
            message: Some(theirs::sync_response::Message::Trailer(
                theirs::SyncTrailer {
                    log_id: "log".into(),
                    next_offset: 3,
                },
            )),
        };
        out.extend(trailer.encode_length_delimited_to_vec());
        out
    }

    #[test]
    fn decodes_sqld_messages_split_anywhere() {
        let bytes = encode_theirs();
        for chunk_size in [1, 7, 4096, bytes.len()] {
            let mut decoder = Decoder::default();
            let mut frames = Vec::new();
            let mut trailer = None;
            for chunk in bytes.chunks(chunk_size) {
                decoder.push(chunk);
                while let Some(msg) = decoder.next().unwrap() {
                    match msg.message.unwrap() {
                        sync_response::Message::Frame(b) => {
                            frames.push(Frame::new(b).unwrap().page_no())
                        }
                        sync_response::Message::Trailer(t) => trailer = Some(t),
                        sync_response::Message::Error(_) => panic!(),
                    }
                }
            }
            assert_eq!(frames, vec![1, 2]);
            let trailer = trailer.unwrap();
            assert_eq!(trailer.log_id, "log");
            assert_eq!(trailer.next_offset, 3);
        }
    }
}
