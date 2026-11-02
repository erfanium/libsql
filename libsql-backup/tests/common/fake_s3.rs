//! A tiny in-memory S3, path-style, with what libsql-backup uses: objects, conditional puts,
//! ListObjectsV2, DeleteObjects and multipart uploads. Requests are not authenticated.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;

#[derive(Clone)]
pub struct Object {
    pub body: Bytes,
    pub etag: String,
}

#[derive(Default)]
pub struct Inner {
    pub objects: BTreeMap<String, Object>,
    uploads: HashMap<String, BTreeMap<u32, Bytes>>,
    next_id: u64,
}

#[derive(Clone, Default)]
pub struct FakeS3 {
    pub inner: Arc<Mutex<Inner>>,
}

impl FakeS3 {
    /// Serves on a random local port, returns the endpoint URL.
    pub async fn start(&self) -> String {
        let app = Router::new()
            .route("/:bucket", any(bucket_request))
            .route("/:bucket/", any(bucket_request))
            .route("/:bucket/*key", any(object))
            .layer(axum::extract::DefaultBodyLimit::disable())
            .with_state(self.clone());
        let server = axum::Server::bind(&SocketAddr::from(([127, 0, 0, 1], 0)))
            .serve(app.into_make_service());
        let addr = server.local_addr();
        tokio::spawn(server);
        format!("http://{addr}")
    }

    pub fn keys(&self) -> Vec<String> {
        self.inner.lock().unwrap().objects.keys().cloned().collect()
    }
}

fn xml(status: StatusCode, body: String) -> Response {
    (
        status,
        [("content-type", "application/xml")],
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>{body}"),
    )
        .into_response()
}

fn s3_error(status: StatusCode, code: &str) -> Response {
    xml(
        status,
        format!("<Error><Code>{code}</Code><Message>{code}</Message></Error>"),
    )
}

#[allow(clippy::format_collect)]
async fn bucket_request(
    State(s3): State<FakeS3>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    method: axum::http::Method,
    body: Bytes,
) -> Response {
    let mut inner = s3.inner.lock().unwrap();
    if method == axum::http::Method::GET {
        let prefix = query.get("prefix").cloned().unwrap_or_default();
        let contents: String = inner
            .objects
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, o)| {
                format!(
                    "<Contents><Key>{k}</Key><LastModified>2000-01-01T00:00:00.000Z</LastModified>\
                     <ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                    o.etag,
                    o.body.len()
                )
            })
            .collect();
        return xml(
            StatusCode::OK,
            format!(
                "<ListBucketResult><Name>{bucket}</Name><Prefix>{prefix}</Prefix>\
                 <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
            ),
        );
    }
    if method == axum::http::Method::POST && query.contains_key("delete") {
        let body = String::from_utf8_lossy(&body);
        for part in body.split("<Key>").skip(1) {
            if let Some((key, _)) = part.split_once("</Key>") {
                inner.objects.remove(key);
            }
        }
        return xml(StatusCode::OK, "<DeleteResult></DeleteResult>".into());
    }
    s3_error(StatusCode::NOT_IMPLEMENTED, "NotImplemented")
}

async fn object(
    State(s3): State<FakeS3>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    method: axum::http::Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let key = key.trim_start_matches('/').to_string();
    // `/bucket/?delete` and the like are bucket requests
    if key.is_empty() {
        return bucket_request(State(s3), Path(bucket), Query(query), method, body).await;
    }
    let mut inner = s3.inner.lock().unwrap();
    inner.next_id += 1;
    let etag = format!("\"{}\"", inner.next_id);

    match method {
        axum::http::Method::GET => match inner.objects.get(&key) {
            Some(o) => (StatusCode::OK, [("etag", o.etag.clone())], o.body.clone()).into_response(),
            None => s3_error(StatusCode::NOT_FOUND, "NoSuchKey"),
        },
        axum::http::Method::PUT => {
            if let (Some(part), Some(id)) = (query.get("partNumber"), query.get("uploadId")) {
                let Some(upload) = inner.uploads.get_mut(id) else {
                    return s3_error(StatusCode::NOT_FOUND, "NoSuchUpload");
                };
                upload.insert(part.parse().unwrap(), body);
                return (StatusCode::OK, [("etag", etag)]).into_response();
            }
            let current = inner.objects.get(&key).map(|o| o.etag.clone());
            if let Some(v) = headers.get("if-none-match") {
                if v == "*" && current.is_some() {
                    return s3_error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
                }
            }
            if let Some(v) = headers.get("if-match") {
                if current.as_deref() != Some(v.to_str().unwrap()) {
                    return s3_error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
                }
            }
            inner.objects.insert(
                key,
                Object {
                    body,
                    etag: etag.clone(),
                },
            );
            (StatusCode::OK, [("etag", etag)]).into_response()
        }
        axum::http::Method::POST if query.contains_key("uploads") => {
            let id = format!("upload-{}", inner.next_id);
            inner.uploads.insert(id.clone(), BTreeMap::new());
            xml(
                StatusCode::OK,
                format!(
                    "<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key>\
                     <UploadId>{id}</UploadId></InitiateMultipartUploadResult>"
                ),
            )
        }
        axum::http::Method::POST if query.contains_key("uploadId") => {
            let Some(parts) = inner.uploads.remove(&query["uploadId"]) else {
                return s3_error(StatusCode::NOT_FOUND, "NoSuchUpload");
            };
            let mut all = Vec::new();
            for part in parts.values() {
                all.extend_from_slice(part);
            }
            inner.objects.insert(
                key.clone(),
                Object {
                    body: all.into(),
                    etag: etag.clone(),
                },
            );
            xml(
                StatusCode::OK,
                format!(
                    "<CompleteMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key>\
                     <ETag>{etag}</ETag></CompleteMultipartUploadResult>"
                ),
            )
        }
        axum::http::Method::DELETE => {
            if let Some(id) = query.get("uploadId") {
                inner.uploads.remove(id);
            } else {
                inner.objects.remove(&key);
            }
            StatusCode::NO_CONTENT.into_response()
        }
        _ => s3_error(StatusCode::NOT_IMPLEMENTED, "NotImplemented"),
    }
}
