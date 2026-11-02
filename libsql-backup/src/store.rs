//! The S3 bucket that holds the backups.

use anyhow::{anyhow, bail, Context, Result};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

use crate::config::S3Config;

/// Parts of a multipart upload. S3 wants at least 5MB for every part but the last.
const PART_SIZE: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct Store {
    client: Client,
    bucket: String,
    prefix: String,
}

/// What a conditional write expects to find in place of the object.
#[derive(Debug, Clone)]
pub enum Expect {
    /// The object must not exist.
    Absent,
    /// The object must still have this etag.
    Etag(String),
}

#[derive(thiserror::Error, Debug)]
#[error("`{0}` was changed by someone else")]
pub struct Conflict(pub String);

pub struct ListedObject {
    pub key: String,
    pub last_modified: Option<DateTime<Utc>>,
}

impl Store {
    pub async fn new(config: &S3Config) -> Result<Self> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(config.region.clone()));
        if let Some(endpoint) = &config.endpoint {
            loader = loader.endpoint_url(endpoint);
        }
        if let (Some(id), Some(secret)) = (&config.access_key_id, &config.secret_access_key) {
            loader = loader.credentials_provider(Credentials::new(
                id,
                secret,
                config.session_token.clone(),
                None,
                "libsql-backup",
            ));
        }
        let sdk = loader.load().await;
        let s3 = aws_sdk_s3::config::Builder::from(&sdk)
            // custom endpoints (MinIO & co) rarely do virtual-hosted buckets
            .force_path_style(config.endpoint.is_some())
            .build();
        Ok(Self {
            client: Client::from_conf(s3),
            bucket: config.bucket.clone(),
            prefix: config.prefix.trim_matches('/').to_string(),
        })
    }

    /// Full key of `path` under the configured prefix.
    pub fn key(&self, path: &str) -> String {
        if self.prefix.is_empty() {
            path.to_string()
        } else {
            format!("{}/{path}", self.prefix)
        }
    }

    /// The object and its etag, `None` when it does not exist.
    pub async fn get(&self, key: &str) -> Result<Option<(Bytes, String)>> {
        let resp = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(SdkError::ServiceError(e)) if e.err().is_no_such_key() => return Ok(None),
            Err(e) => return Err(anyhow!(e).context(format!("GET {key}"))),
        };
        let etag = resp.e_tag().unwrap_or_default().to_string();
        let body = resp
            .body
            .collect()
            .await
            .with_context(|| format!("reading {key}"))?;
        Ok(Some((body.into_bytes(), etag)))
    }

    /// Streams the object into `writer`.
    pub async fn download<W: AsyncWrite + Unpin>(&self, key: &str, writer: &mut W) -> Result<()> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("GET {key}"))?;
        let mut body = resp.body.into_async_read();
        tokio::io::copy(&mut body, writer)
            .await
            .with_context(|| format!("downloading {key}"))?;
        Ok(())
    }

    pub async fn put(&self, key: &str, body: Bytes) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .with_context(|| format!("PUT {key}"))?;
        Ok(())
    }

    /// Writes the object only if it is still as expected. Fails with [`Conflict`] otherwise.
    pub async fn put_if(&self, key: &str, body: Bytes, expect: Expect) -> Result<()> {
        // the SDK version of the workspace has no typed conditional writes: set the headers
        let (header, value) = match expect {
            Expect::Absent => ("if-none-match", "*".to_string()),
            Expect::Etag(etag) => ("if-match", etag),
        };
        let res = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type("application/json")
            .body(ByteStream::from(body))
            .customize()
            .mutate_request(move |req| {
                req.headers_mut().insert(header, value.clone());
            })
            .send()
            .await;
        match res {
            Ok(_) => Ok(()),
            Err(SdkError::ServiceError(e))
                if matches!(e.raw().status().as_u16(), 409 | 412)
                    || matches!(
                        e.err().code(),
                        Some("PreconditionFailed" | "ConditionalRequestConflict")
                    ) =>
            {
                Err(Conflict(key.to_string()).into())
            }
            Err(e) => Err(anyhow!(e).context(format!("PUT {key}"))),
        }
    }

    /// Uploads everything `reader` yields, in parts when it is large.
    pub async fn upload<R: AsyncRead + Unpin>(&self, key: &str, mut reader: R) -> Result<()> {
        let first = read_part(&mut reader).await?;
        if first.len() < PART_SIZE {
            return self.put(key, first.into()).await;
        }

        let upload = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("starting multipart upload of {key}"))?;
        let upload_id = upload
            .upload_id()
            .ok_or_else(|| anyhow!("no upload id for {key}"))?
            .to_string();

        let res = self.upload_parts(key, &upload_id, first, &mut reader).await;
        if res.is_err() {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
        }
        res
    }

    async fn upload_parts<R: AsyncRead + Unpin>(
        &self,
        key: &str,
        upload_id: &str,
        first: Vec<u8>,
        reader: &mut R,
    ) -> Result<()> {
        let mut parts = Vec::new();
        let mut part = first;
        loop {
            let number = parts.len() as i32 + 1;
            let resp = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(number)
                .body(ByteStream::from(part))
                .send()
                .await
                .with_context(|| format!("uploading part {number} of {key}"))?;
            parts.push(
                CompletedPart::builder()
                    .part_number(number)
                    .set_e_tag(resp.e_tag().map(str::to_string))
                    .build(),
            );
            part = read_part(reader).await?;
            if part.is_empty() {
                break;
            }
        }
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .with_context(|| format!("completing multipart upload of {key}"))?;
        Ok(())
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<ListedObject>> {
        let mut out = Vec::new();
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let page = page.with_context(|| format!("listing {prefix}"))?;
            for object in page.contents() {
                let Some(key) = object.key() else { continue };
                out.push(ListedObject {
                    key: key.to_string(),
                    last_modified: object
                        .last_modified()
                        .and_then(|t| DateTime::from_timestamp(t.secs(), t.subsec_nanos())),
                });
            }
        }
        Ok(out)
    }

    pub async fn delete(&self, keys: &[String]) -> Result<()> {
        for chunk in keys.chunks(1000) {
            let objects = chunk
                .iter()
                .map(|key| ObjectIdentifier::builder().key(key).build())
                .collect::<Result<Vec<_>, _>>()?;
            let resp = self
                .client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(
                    Delete::builder()
                        .set_objects(Some(objects))
                        .quiet(true)
                        .build()?,
                )
                .send()
                .await
                .context("deleting objects")?;
            if let Some(err) = resp.errors().first() {
                bail!(
                    "deleting {}: {}",
                    err.key().unwrap_or_default(),
                    err.message().unwrap_or_default()
                );
            }
        }
        Ok(())
    }
}

async fn read_part<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let mut part = Vec::with_capacity(PART_SIZE);
    (&mut *reader)
        .take(PART_SIZE as u64)
        .read_to_end(&mut part)
        .await?;
    Ok(part)
}
