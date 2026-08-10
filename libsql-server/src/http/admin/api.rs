//! Admin platform: database lifecycle, tokens, admin query, UI.
//!
//! The admin router is mounted under `/admin` and can be served either
//! nested on the main user port or on a dedicated port
//! (`ADMIN_LISTEN_ADDR`). Namespace lifecycle is performed
//! in-process through the `NamespaceStore` — there is no separate admin
//! HTTP API.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::extract::{Path as AxumPath, Query, State, State as AxumState};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::Utc;
use ed25519_dalek::{Signer, SigningKey};
use hyper::{Body, Request};
use rand::rngs::OsRng;
use rusqlite::Connection as SqliteConnection;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

use crate::auth::Authenticated;
use crate::connection::config::DatabaseConfig;
use crate::connection::{Connection, RequestContext};
use crate::hrana;
use crate::namespace::{NamespaceName, RestoreOption};
use crate::query::Params;

use crate::http::user::result_builder::JsonHttpPayloadBuilder;
use crate::http::user::types::{QueryObject, QueryParams};
use crate::query_result_builder::QueryResultBuilder;

/// Admin platform configuration, populated from the environment.
#[derive(Clone, Debug)]
pub struct AdminConfig {
    pub admin_key: String,
    pub version: String,
    pub data_dir: PathBuf,
    pub sqld_data_dir: PathBuf,
    pub public_dir: Option<PathBuf>,
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub connection_url: Option<String>,
    /// Address of the dedicated admin listener (dashboard + admin API).
    pub admin_listen_addr: SocketAddr,
}

impl AdminConfig {
    pub fn connection_url(&self) -> String {
        if let Some(url) = &self.connection_url {
            return url.clone();
        }
        let default_port = if self.scheme == "https" { 443 } else { 80 };
        let port_str = if self.port == default_port {
            String::new()
        } else {
            format!(":{}", self.port)
        };
        format!("{}://{}{}", self.scheme, self.host, port_str)
    }
}

/// Shared admin platform state, stored in the axum `AppState`.
pub struct AdminState {
    pub config: AdminConfig,
    pub metadata: Metadata,
}

#[derive(Clone, serde::Serialize)]
pub struct DatabaseRecord {
    pub id: String,
    pub namespace: String,
    pub jwt_public_key: String,
    pub jwt_private_key: String,
    /// The app access token (full rw/ro permissions), returned verbatim by
    /// `GET /api/databases/:id/token` until the token is regenerated.
    #[serde(skip_serializing)]
    pub token: String,
    pub created_at: String,
    pub last_accessed_at: Option<String>,
}

fn row_to_record(row: &rusqlite::Row) -> rusqlite::Result<DatabaseRecord> {
    Ok(DatabaseRecord {
        id: row.get(0)?,
        namespace: row.get(1)?,
        jwt_public_key: row.get(2)?,
        jwt_private_key: row.get(3)?,
        token: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
        created_at: row.get(5)?,
        last_accessed_at: row.get(6)?,
    })
}

/// Metadata store: a local SQLite database with the platform records.
pub struct Metadata {
    db: Mutex<SqliteConnection>,
}

impl Metadata {
    pub fn open(data_dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let conn = SqliteConnection::open(data_dir.join("meta.db"))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS databases (
                id TEXT PRIMARY KEY,
                namespace TEXT UNIQUE NOT NULL,
                jwt_public_key TEXT NOT NULL,
                jwt_private_key TEXT NOT NULL,
                token TEXT,
                created_at TEXT NOT NULL,
                last_accessed_at TEXT
            )",
        )?;
        // Schema migration for stores created before the token column.
        // The column stays nullable; it is filled in when a token is
        // regenerated (create / rename / rotate).
        let has_token = {
            let mut stmt = conn.prepare(
                "SELECT COUNT(*) FROM pragma_table_info('databases') WHERE name = 'token'",
            )?;
            let count: i64 = stmt.query_row([], |row| row.get(0))?;
            count > 0
        };
        if !has_token {
            conn.execute_batch("ALTER TABLE databases ADD COLUMN token TEXT")?;
        }
        Ok(Self {
            db: Mutex::new(conn),
        })
    }

    pub async fn insert(&self, record: &DatabaseRecord) -> anyhow::Result<()> {
        let conn = self.db.lock().await;
        conn.execute(
            "INSERT INTO databases (id, namespace, jwt_public_key, jwt_private_key, token, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                record.id,
                record.namespace,
                record.jwt_public_key,
                record.jwt_private_key,
                record.token,
                record.created_at
            ],
        )?;
        Ok(())
    }

    pub async fn get(&self, id: &str) -> anyhow::Result<Option<DatabaseRecord>> {
        let conn = self.db.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, namespace, jwt_public_key, jwt_private_key, token, created_at, last_accessed_at
             FROM databases WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map([id], row_to_record)?;
        Ok(rows.next().transpose()?)
    }

    pub async fn list(&self) -> anyhow::Result<Vec<DatabaseRecord>> {
        let conn = self.db.lock().await;
        let mut stmt = conn.prepare(
            "SELECT id, namespace, jwt_public_key, jwt_private_key, token, created_at, last_accessed_at
             FROM databases ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map([], row_to_record)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub async fn delete(&self, id: &str) -> anyhow::Result<()> {
        let conn = self.db.lock().await;
        conn.execute("DELETE FROM databases WHERE id = ?1", [id])?;
        Ok(())
    }

    pub async fn replace(&self, old_id: &str, record: &DatabaseRecord) -> anyhow::Result<()> {
        let conn = self.db.lock().await;
        let tx = conn.unchecked_transaction()?;
        tx.execute("DELETE FROM databases WHERE id = ?1", [old_id])?;
        tx.execute(
            "INSERT INTO databases (id, namespace, jwt_public_key, jwt_private_key, token, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                record.id,
                record.namespace,
                record.jwt_public_key,
                record.jwt_private_key,
                record.token,
                record.created_at
            ],
        )?;
        tx.commit()?;
        Ok(())
    }
}

// ─── JWT ──────────────────────────────────────────────────────────────

/// Generate an Ed25519 keypair. Returns the signing key and the base64url
/// (no padding) raw 32-byte public key, the format sqld's `jwt_key` expects.
pub fn generate_keypair() -> (SigningKey, String) {
    let signing_key = SigningKey::generate(&mut OsRng);
    let public_key_b64 = URL_SAFE_NO_PAD.encode(signing_key.verifying_key().to_bytes());
    (signing_key, public_key_b64)
}

/// Sign a JWT with the same claims format the platform has always used:
/// `{"p": {"rw": {"ns": [ns]}, "ro": {"ns": [ns]}}, "exp": <unix>}`, EdDSA.
pub fn sign_token(namespace: &str, signing_key: &SigningKey, expires_in_secs: i64) -> String {
    let header = json!({ "alg": "EdDSA", "typ": "JWT" });
    let claims = json!({
        "p": {
            "rw": { "ns": [namespace] },
            "ro": { "ns": [namespace] },
        },
        "exp": Utc::now().timestamp() + expires_in_secs,
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
    let claims_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
    let signing_input = format!("{header_b64}.{claims_b64}");
    let signature = URL_SAFE_NO_PAD.encode(signing_key.sign(signing_input.as_bytes()).to_bytes());
    format!("{signing_input}.{signature}")
}

// ─── In-process namespace lifecycle ───────────────────────────────────

async fn create_namespace(
    state: &crate::http::user::AppState,
    namespace: &str,
    jwt_key: &str,
) -> anyhow::Result<()> {
    let mut config = DatabaseConfig::default();
    config.jwt_key = Some(jwt_key.to_string());
    state
        .namespaces
        .create(
            NamespaceName::from_string(namespace.to_string())?,
            RestoreOption::Latest,
            config,
        )
        .await?;
    Ok(())
}

async fn delete_namespace(
    state: &crate::http::user::AppState,
    namespace: &str,
) -> anyhow::Result<()> {
    state
        .namespaces
        .destroy(NamespaceName::from_string(namespace.to_string())?, true)
        .await?;
    Ok(())
}

async fn update_jwt_key(
    state: &crate::http::user::AppState,
    namespace: &str,
    jwt_key: &str,
) -> anyhow::Result<()> {
    let store = state
        .namespaces
        .config_store(NamespaceName::from_string(namespace.to_string())?)
        .await?;
    let mut config = (*store.get()).clone();
    config.jwt_key = Some(jwt_key.to_string());
    store.store(config).await?;
    Ok(())
}

// ─── Auth helpers ─────────────────────────────────────────────────────

fn require_admin(headers: &HeaderMap, config: &AdminConfig) -> Result<(), StatusCode> {
    let auth = headers
        .get("authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let auth = auth.to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
    let mut split = auth.split_whitespace();
    if split.next().map(|s| s.eq_ignore_ascii_case("bearer")) != Some(true) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if split.next() != Some(config.admin_key.as_str()) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

fn error_response(status: StatusCode, message: &str) -> axum::response::Response {
    (status, Json(json!({ "error": message }))).into_response()
}

/// Router-wide middleware for the admin API: rejects requests without a
/// valid admin key and requests sent while admin is not configured.
/// The `/api/auth/verify` endpoint is exempted, since it is how the admin UI
/// checks the key supplied in the request body.
async fn admin_auth(
    State(state): State<crate::http::user::AppState>,
    req: Request<Body>,
    next: Next<Body>,
) -> axum::response::Response {
    if req.uri().path() == "/api/auth/verify" {
        return next.run(req).await;
    }
    let Some(admin) = &state.admin else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "admin not configured");
    };
    if let Err(status) = require_admin(req.headers(), &admin.config) {
        return error_response(status, "Unauthorized");
    }
    next.run(req).await
}

/// The admin middleware guarantees admin is configured, so handlers can
/// fetch the state without repeating the 503 check.
fn admin(state: &crate::http::user::AppState) -> &AdminState {
    state
        .admin
        .as_ref()
        .expect("admin middleware guarantees admin is configured")
}

/// Look up a database record by id, returning a 404 response when missing.
async fn get_record_or_404(
    admin: &AdminState,
    id: &str,
) -> Result<DatabaseRecord, axum::response::Response> {
    admin
        .metadata
        .get(id)
        .await
        .ok()
        .flatten()
        .ok_or_else(|| error_response(StatusCode::NOT_FOUND, "Database not found"))
}

// ─── Database stats ───────────────────────────────────────────────────

async fn read_stats(sqld_data_dir: &Path, namespace: &str) -> Option<Value> {
    let path = sqld_data_dir.join("dbs").join(namespace).join("stats.json");
    let data = tokio::fs::read_to_string(path).await.ok()?;
    serde_json::from_str(&data).ok()
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

async fn enrich_database(state: &AdminState, record: &DatabaseRecord) -> Value {
    let stats = read_stats(&state.config.sqld_data_dir, &record.namespace).await;
    let storage_bytes = stats
        .as_ref()
        .and_then(|s| s.get("storage_bytes_used"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let rows_written = stats
        .as_ref()
        .and_then(|s| s.get("rows_written"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let rows_read = stats
        .as_ref()
        .and_then(|s| s.get("rows_read"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let query_count = stats
        .as_ref()
        .and_then(|s| s.get("query_count"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let query_latency = stats
        .as_ref()
        .and_then(|s| s.get("query_latency"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let conn_url = state.config.connection_url();

    json!({
        "id": record.id,
        "namespace": record.namespace,
        "created_at": record.created_at,
        "last_accessed_at": record.last_accessed_at,
        "storage_bytes": storage_bytes,
        "storage_bytes_formatted": if stats.is_some() { json!(format_bytes(storage_bytes)) } else { Value::Null },
        "rows_written": rows_written,
        "rows_read": rows_read,
        "query_count": query_count,
        "query_latency_ms": query_latency,
        "host": conn_url.trim_start_matches("http://").trim_start_matches("https://"),
        "connection_url": conn_url,
    })
}

// ─── Handlers ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateDatabaseReq {
    id: Option<String>,
}

#[derive(Deserialize)]
pub struct RenameDatabaseReq {
    id: Option<String>,
}

#[derive(Deserialize)]
pub struct VerifyKeyReq {
    key: Option<String>,
}

#[derive(Deserialize)]
pub struct AdminQueryReq {
    namespace: String,
    statements: Option<Vec<QueryObject>>,
    sql: Option<String>,
}

async fn verify_key(
    AxumState(state): AxumState<crate::http::user::AppState>,
    Json(body): Json<VerifyKeyReq>,
) -> Json<Value> {
    let valid = match &state.admin {
        Some(admin) => {
            body.key.is_some() && body.key.as_deref() == Some(admin.config.admin_key.as_str())
        }
        None => false,
    };
    Json(json!({ "valid": valid }))
}

async fn list_databases(
    AxumState(state): AxumState<crate::http::user::AppState>,
) -> axum::response::Response {
    let admin = admin(&state);
    match admin.metadata.list().await {
        Ok(records) => {
            let mut databases = Vec::with_capacity(records.len());
            for record in records.iter() {
                databases.push(enrich_database(admin, record).await);
            }
            (Json(json!({ "databases": databases }))).into_response()
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn create_database(
    AxumState(state): AxumState<crate::http::user::AppState>,
    Json(body): Json<CreateDatabaseReq>,
) -> axum::response::Response {
    let admin = admin(&state);

    let Some(id) = body.id else {
        return error_response(StatusCode::BAD_REQUEST, "id is required");
    };
    if id.is_empty()
        || id.len() > 36
        || !id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "id must be 1-36 characters, only [0-9a-z]",
        );
    }
    if admin.metadata.get(&id).await.ok().flatten().is_some() {
        return error_response(
            StatusCode::CONFLICT,
            "A database with this id already exists",
        );
    }

    let (signing_key, public_key_b64) = generate_keypair();

    if let Err(e) = create_namespace(&state, &id, &public_key_b64).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to create namespace: {}", e),
        );
    }

    let token = sign_token(&id, &signing_key, 90 * 24 * 3600);
    let record = DatabaseRecord {
        id: id.clone(),
        namespace: id.clone(),
        jwt_public_key: public_key_b64,
        jwt_private_key: URL_SAFE_NO_PAD.encode(signing_key.to_bytes()),
        token: token.clone(),
        created_at: Utc::now().to_rfc3339(),
        last_accessed_at: None,
    };
    if let Err(e) = admin.metadata.insert(&record).await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }

    let conn_url = admin.config.connection_url();
    (
        StatusCode::CREATED,
        Json(json!({
            "id": record.id,
            "namespace": record.namespace,
            "token": token,
            "host": conn_url.trim_start_matches("http://").trim_start_matches("https://"),
            "connection_url": conn_url,
            "created_at": record.created_at,
        })),
    )
        .into_response()
}

async fn get_database(
    AxumState(state): AxumState<crate::http::user::AppState>,
    AxumPath(id): AxumPath<String>,
) -> axum::response::Response {
    let admin = admin(&state);
    let record = match get_record_or_404(admin, &id).await {
        Ok(record) => record,
        Err(resp) => return resp,
    };
    (Json(enrich_database(admin, &record).await)).into_response()
}

async fn rename_database(
    AxumState(state): AxumState<crate::http::user::AppState>,
    AxumPath(old_id): AxumPath<String>,
    Json(body): Json<RenameDatabaseReq>,
) -> axum::response::Response {
    let admin = admin(&state);

    let Some(new_id) = body.id else {
        return error_response(StatusCode::BAD_REQUEST, "id is required");
    };
    if new_id.is_empty()
        || new_id.len() > 36
        || !new_id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "id must be 1-36 characters, only [0-9a-z]",
        );
    }
    if new_id == old_id {
        return error_response(
            StatusCode::BAD_REQUEST,
            "new id must be different from current id",
        );
    }

    let old_record = match get_record_or_404(admin, &old_id).await {
        Ok(record) => record,
        Err(resp) => return resp,
    };
    if admin.metadata.get(&new_id).await.ok().flatten().is_some() {
        return error_response(
            StatusCode::CONFLICT,
            "A database with this id already exists",
        );
    }

    let (signing_key, public_key_b64) = generate_keypair();

    if let Err(e) = create_namespace(&state, &new_id, &public_key_b64).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to create namespace: {}", e),
        );
    }

    // Copy the database file to the new namespace.
    let old_db_path = admin
        .config
        .sqld_data_dir
        .join("dbs")
        .join(&old_record.namespace)
        .join("dbs")
        .join("default");
    let new_db_path = admin
        .config
        .sqld_data_dir
        .join("dbs")
        .join(&new_id)
        .join("dbs")
        .join("default");
    if old_db_path.exists() {
        let _ = tokio::fs::copy(&old_db_path, &new_db_path).await;
    }

    if let Err(e) = delete_namespace(&state, &old_record.namespace).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to delete old namespace: {}", e),
        );
    }

    let token = sign_token(&new_id, &signing_key, 90 * 24 * 3600);
    let record = DatabaseRecord {
        id: new_id.clone(),
        namespace: new_id.clone(),
        jwt_public_key: public_key_b64,
        jwt_private_key: URL_SAFE_NO_PAD.encode(signing_key.to_bytes()),
        token: token.clone(),
        created_at: old_record.created_at,
        last_accessed_at: None,
    };
    if let Err(e) = admin.metadata.replace(&old_id, &record).await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }

    let conn_url = admin.config.connection_url();
    Json(json!({
        "id": record.id,
        "namespace": record.namespace,
        "token": token,
        "host": conn_url.trim_start_matches("http://").trim_start_matches("https://"),
        "connection_url": conn_url,
        "created_at": record.created_at,
    }))
    .into_response()
}

async fn rotate_token(
    AxumState(state): AxumState<crate::http::user::AppState>,
    AxumPath(id): AxumPath<String>,
) -> axum::response::Response {
    let admin = admin(&state);

    let record = match get_record_or_404(admin, &id).await {
        Ok(record) => record,
        Err(resp) => return resp,
    };

    let (signing_key, public_key_b64) = generate_keypair();

    if let Err(e) = update_jwt_key(&state, &record.namespace, &public_key_b64).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to update namespace config: {}", e),
        );
    }

    let token = sign_token(&record.namespace, &signing_key, 90 * 24 * 3600);
    let mut new_record = record.clone();
    new_record.jwt_public_key = public_key_b64;
    new_record.jwt_private_key = URL_SAFE_NO_PAD.encode(signing_key.to_bytes());
    new_record.token = token.clone();
    if let Err(e) = admin.metadata.replace(&id, &new_record).await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }

    let conn_url = admin.config.connection_url();
    Json(json!({
        "id": new_record.id,
        "namespace": new_record.namespace,
        "token": token,
        "host": conn_url.trim_start_matches("http://").trim_start_matches("https://"),
        "connection_url": conn_url,
    }))
    .into_response()
}

/// Flush the namespace WAL into the main `.db` file and compact it
/// (checkpoint with TRUNCATE + vacuum). After this the WAL is empty; on a
/// clean server shutdown SQLite also removes the `-wal`/`-shm` files,
/// leaving a single `data` file.
async fn checkpoint_database(
    AxumState(state): AxumState<crate::http::user::AppState>,
    AxumPath(id): AxumPath<String>,
) -> axum::response::Response {
    let admin = admin(&state);

    let record = match get_record_or_404(admin, &id).await {
        Ok(record) => record,
        Err(resp) => return resp,
    };

    let namespace = match NamespaceName::from_string(record.namespace.clone()) {
        Ok(namespace) => namespace,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid namespace"),
    };

    if let Err(e) = state.namespaces.checkpoint(namespace.clone()).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to checkpoint namespace {}: {}", namespace, e),
        );
    }

    Json(json!({
        "checkpointed": true,
        "id": record.id,
        "namespace": record.namespace,
    }))
    .into_response()
}

/// Return the current app access token (full rw/ro permissions) for a
/// database. This is not a renew route: the stored token is returned
/// verbatim, and only changes when the token is regenerated (create /
/// rename / rotate).
async fn get_database_token(
    AxumState(state): AxumState<crate::http::user::AppState>,
    AxumPath(id): AxumPath<String>,
) -> axum::response::Response {
    let admin = admin(&state);

    let record = match get_record_or_404(admin, &id).await {
        Ok(record) => record,
        Err(resp) => return resp,
    };
    if record.token.is_empty() {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "no access token stored for this database",
        );
    }

    let conn_url = admin.config.connection_url();
    Json(json!({
        "id": record.id,
        "namespace": record.namespace,
        "token": record.token,
        "host": conn_url.trim_start_matches("http://").trim_start_matches("https://"),
        "connection_url": conn_url,
    }))
    .into_response()
}

async fn delete_database(
    AxumState(state): AxumState<crate::http::user::AppState>,
    AxumPath(id): AxumPath<String>,
) -> axum::response::Response {
    let admin = admin(&state);

    let record = match get_record_or_404(admin, &id).await {
        Ok(record) => record,
        Err(resp) => return resp,
    };

    if let Err(e) = delete_namespace(&state, &record.namespace).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to delete namespace: {}", e),
        );
    }

    if let Err(e) = admin.metadata.delete(&id).await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }

    Json(json!({
        "deleted": true,
        "id": record.id,
        "namespace": record.namespace,
    }))
    .into_response()
}

/// Live process list of in-flight queries across all namespaces: which
/// thread, namespace, statement and step each query is on, how long it has
/// been running, and how much CPU its thread consumed. Mirrors MySQL's
/// `SHOW PROCESSLIST` for debugging which namespaces and jobs burn CPU.
async fn handle_queries() -> axum::response::Response {
    Json(crate::query_registry::snapshot()).into_response()
}

/// Let an admin run SQL against any database. The namespace is taken from
/// the request body; authentication is the admin key, not a database JWT.
async fn admin_query(
    AxumState(state): AxumState<crate::http::user::AppState>,
    Json(body): Json<AdminQueryReq>,
) -> axum::response::Response {
    let namespace = match NamespaceName::from_string(body.namespace.clone()) {
        Ok(namespace) => namespace,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid namespace"),
    };

    let statements = match (body.statements, body.sql) {
        (Some(statements), _) => statements,
        (None, Some(sql)) => vec![QueryObject {
            q: sql,
            params: QueryParams(Params::empty()),
        }],
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "provide either 'sql' or 'statements' array",
            )
        }
    };

    let batch = match crate::http::user::parse_queries(statements) {
        Ok(batch) => batch,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let auth = Authenticated::FullAccess;
    let conn_cache = match state
        .namespaces
        .with_authenticated(namespace.clone(), auth.clone(), |ns| ns.db.conn_cache())
        .await
    {
        Ok(cache) => cache,
        Err(e) => return error_response(StatusCode::NOT_FOUND, &e.to_string()),
    };
    let ctx = RequestContext::new(auth, namespace, state.namespaces.meta_store().clone());
    // Wait for the cached connection to become available — never create a
    // second connection while one is in use, so each namespace is served by
    // exactly one connection. `None` means the cache was closed by namespace
    // eviction/shutdown.
    let db = match conn_cache.take().await {
        Some(db) => db,
        None => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "namespace is shutting down or was evicted",
            )
        }
    };
    let builder = JsonHttpPayloadBuilder::new();
    let result = db
        .execute_batch_or_rollback(batch, ctx, builder, None)
        .await;
    // Always return the connection, even on error, or the next request would
    // wait on the cache forever.
    conn_cache.put(db).await;
    match result {
        Ok(builder) => (
            [(hyper::header::CONTENT_TYPE, "application/json")],
            builder.into_ret(),
        )
            .into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// The embedded-replica client fetches `GET /info` to learn the current
/// replication generation before syncing frames. The upstream sqld does not
/// implement this endpoint; return 404 so the client falls back to the
/// gRPC replication handshake, matching the reference server behavior.
pub async fn handle_info() -> axum::response::Response {
    StatusCode::NOT_FOUND.into_response()
}

#[derive(Deserialize)]
pub struct AdminPipelineQuery {
    ns: Option<String>,
}

async fn handle_admin_pipeline_impl(
    state: crate::http::user::AppState,
    query: AdminPipelineQuery,
    req: Request<Body>,
    encoding: hrana::Encoding,
) -> axum::response::Response {
    let Some(ns) = query.ns else {
        return error_response(StatusCode::BAD_REQUEST, "missing 'ns' query parameter");
    };
    let namespace = match NamespaceName::from_string(ns) {
        Ok(namespace) => namespace,
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "invalid namespace"),
    };

    let auth = Authenticated::FullAccess;
    let connection_maker = match state
        .namespaces
        .with_authenticated(namespace.clone(), auth.clone(), |ns| {
            ns.db.connection_maker()
        })
        .await
    {
        Ok(maker) => maker,
        Err(e) => return error_response(StatusCode::NOT_FOUND, &e.to_string()),
    };
    let ctx = RequestContext::new(auth, namespace, state.namespaces.meta_store().clone());
    match state
        .hrana_http_srv
        .handle_request(
            connection_maker,
            ctx,
            req,
            hrana::http::Endpoint::Pipeline,
            hrana::Version::Hrana3,
            encoding,
        )
        .await
    {
        Ok(resp) => resp.into_response(),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

/// Admin-authenticated Hrana v3 pipeline: the admin key can target any
/// database, selected with `?ns=<database id>`. Supports batching and
/// baton-based connection reuse, unlike `/api/query`.
async fn handle_admin_pipeline(
    AxumState(state): AxumState<crate::http::user::AppState>,
    Query(query): Query<AdminPipelineQuery>,
    req: Request<Body>,
) -> axum::response::Response {
    handle_admin_pipeline_impl(state, query, req, hrana::Encoding::Json).await
}

/// Admin-authenticated Hrana v3 pipeline with protobuf encoding.
async fn handle_admin_pipeline_protobuf(
    AxumState(state): AxumState<crate::http::user::AppState>,
    Query(query): Query<AdminPipelineQuery>,
    req: Request<Body>,
) -> axum::response::Response {
    handle_admin_pipeline_impl(state, query, req, hrana::Encoding::Protobuf).await
}

// ─── Admin router ─────────────────────────────────────────────────────

/// Build the standalone admin router, served on the dedicated admin
/// listener. The admin UI is served at the root path: the dashboard at
/// `/`, assets under `/assets/*`, and the API under `/api/*`.
pub fn admin_router(admin: Option<Arc<AdminState>>, state: crate::http::user::AppState) -> Router {
    Router::new()
        .route("/api/databases", get(list_databases).post(create_database))
        .route(
            "/api/databases/:id",
            get(get_database)
                .patch(rename_database)
                .delete(delete_database),
        )
        .route(
            "/api/databases/:id/token",
            get(get_database_token).post(rotate_token),
        )
        .route("/api/databases/:id/checkpoint", post(checkpoint_database))
        .route("/api/auth/verify", post(verify_key))
        .route("/api/query", post(admin_query))
        .route("/api/queries", get(handle_queries))
        .route("/metrics", get(crate::http::admin::render_metrics))
        .route("/admin/v3/pipeline", post(handle_admin_pipeline))
        .route(
            "/admin/v3-protobuf/pipeline",
            post(handle_admin_pipeline_protobuf),
        )
        // Auth + admin-config check for every API route (not the admin
        // UI fallback).
        .route_layer(middleware::from_fn_with_state(state.clone(), admin_auth))
        .fallback_service(AdminStatic(admin))
        .with_state(state)
}

/// Static file service for the admin UI: serves files from `public_dir`
/// with an SPA fallback to `index.html`. Returns a JSON 404 for unmatched
/// API paths.
#[derive(Clone)]
struct AdminStatic(Option<Arc<AdminState>>);

impl tower::Service<Request<Body>> for AdminStatic {
    type Response = Response<Body>;
    type Error = std::convert::Infallible;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let admin = self.0.clone();
        let path = req.uri().path().to_string();
        Box::pin(async move {
            let response = match &admin {
                Some(state) if !path.starts_with("/api/") => {
                    serve_static(state.clone(), &path).await
                }
                _ => hyper::Response::new(Body::from(r#"{"error":"Not found"}"#)),
            };
            Ok::<_, std::convert::Infallible>(response)
        })
    }
}

/// Serve a static file from the admin UI build, falling back to
/// `index.html` (SPA). Returns 404 when the UI is not built.
pub async fn serve_static(admin: Arc<AdminState>, request_path: &str) -> Response<Body> {
    let Some(public_dir) = &admin.config.public_dir else {
        return hyper::Response::new(Body::from(r#"{"error":"Not found"}"#));
    };

    let mut path = public_dir.join(request_path.trim_start_matches('/'));
    if path.is_dir() {
        path = path.join("index.html");
    }

    if !path.exists() {
        // SPA fallback: any unmatched path serves index.html
        path = public_dir.join("index.html");
    }

    let Ok(mut file) = tokio::fs::File::open(&path).await else {
        return hyper::Response::new(Body::from(r#"{"error":"Not found"}"#));
    };
    let mut contents = Vec::new();
    if file.read_to_end(&mut contents).await.is_err() {
        return hyper::Response::new(Body::from(r#"{"error":"failed to read file"}"#));
    }

    let content_type = match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "application/javascript",
        Some("css") => "text/css",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", content_type)
        .body(Body::from(contents))
        .unwrap()
}
