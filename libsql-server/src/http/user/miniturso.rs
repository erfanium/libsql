//! MiniTurso admin platform: database lifecycle, tokens, admin query, UI.
//!
//! The admin router is mounted under `/admin` and can be served either
//! nested on the main user port or on a dedicated port
//! (`MINITURSO_ADMIN_LISTEN_ADDR`). Namespace lifecycle is performed
//! in-process through the `NamespaceStore` — there is no separate admin
//! HTTP API.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use axum::extract::{Path as AxumPath, State as AxumState};
use axum::http::{HeaderMap, StatusCode};
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

use crate::auth::Authenticated;
use crate::connection::config::DatabaseConfig;
use crate::connection::{Connection, RequestContext};
use crate::namespace::{NamespaceName, RestoreOption};
use crate::query::Params;

use crate::query_result_builder::QueryResultBuilder;
use super::result_builder::JsonHttpPayloadBuilder;
use super::types::{QueryObject, QueryParams};

/// Admin platform configuration, populated from the environment.
#[derive(Clone, Debug)]
pub struct MinitursoConfig {
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

impl MinitursoConfig {
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
pub struct MinitursoState {
    pub config: MinitursoConfig,
    pub metadata: Metadata,
}

#[derive(Clone, serde::Serialize)]
pub struct DatabaseRecord {
    pub id: String,
    pub namespace: String,
    pub jwt_public_key: String,
    pub jwt_private_key: String,
    pub created_at: String,
    pub last_accessed_at: Option<String>,
}

fn row_to_record(row: &rusqlite::Row) -> rusqlite::Result<DatabaseRecord> {
    Ok(DatabaseRecord {
        id: row.get(0)?,
        namespace: row.get(1)?,
        jwt_public_key: row.get(2)?,
        jwt_private_key: row.get(3)?,
        created_at: row.get(4)?,
        last_accessed_at: row.get(5)?,
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
                created_at TEXT NOT NULL,
                last_accessed_at TEXT
            )",
        )?;
        Ok(Self {
            db: Mutex::new(conn),
        })
    }

    pub fn insert(&self, record: &DatabaseRecord) -> anyhow::Result<()> {
        let conn = self.db.lock().unwrap();
        conn.execute(
            "INSERT INTO databases (id, namespace, jwt_public_key, jwt_private_key, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                record.id,
                record.namespace,
                record.jwt_public_key,
                record.jwt_private_key,
                record.created_at
            ],
        )?;
        Ok(())
    }

    pub fn get(&self, id: &str) -> anyhow::Result<Option<DatabaseRecord>> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, namespace, jwt_public_key, jwt_private_key, created_at, last_accessed_at
             FROM databases WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map([id], row_to_record)?;
        Ok(rows.next().transpose()?)
    }

    pub fn list(&self) -> anyhow::Result<Vec<DatabaseRecord>> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, namespace, jwt_public_key, jwt_private_key, created_at, last_accessed_at
             FROM databases ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map([], row_to_record)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn delete(&self, id: &str) -> anyhow::Result<()> {
        let conn = self.db.lock().unwrap();
        conn.execute("DELETE FROM databases WHERE id = ?1", [id])?;
        Ok(())
    }

    pub fn replace(&self, old_id: &str, record: &DatabaseRecord) -> anyhow::Result<()> {
        let conn = self.db.lock().unwrap();
        let tx = conn.unchecked_transaction()?;
        tx.execute("DELETE FROM databases WHERE id = ?1", [old_id])?;
        tx.execute(
            "INSERT INTO databases (id, namespace, jwt_public_key, jwt_private_key, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                record.id,
                record.namespace,
                record.jwt_public_key,
                record.jwt_private_key,
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
    state: &super::AppState,
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

async fn delete_namespace(state: &super::AppState, namespace: &str) -> anyhow::Result<()> {
    state
        .namespaces
        .destroy(NamespaceName::from_string(namespace.to_string())?, true)
        .await?;
    Ok(())
}

async fn update_jwt_key(
    state: &super::AppState,
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

fn require_admin(headers: &HeaderMap, config: &MinitursoConfig) -> Result<(), StatusCode> {
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

// ─── Database stats ───────────────────────────────────────────────────

fn read_stats(sqld_data_dir: &Path, namespace: &str) -> Option<Value> {
    let path = sqld_data_dir.join("dbs").join(namespace).join("stats.json");
    let data = std::fs::read_to_string(path).ok()?;
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

fn enrich_database(state: &MinitursoState, record: &DatabaseRecord) -> Value {
    let stats = read_stats(&state.config.sqld_data_dir, &record.namespace);
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

pub async fn verify_key(
    AxumState(state): AxumState<super::AppState>,
    Json(body): Json<VerifyKeyReq>,
) -> Json<Value> {
    let valid = match &state.miniturso {
        Some(miniturso) => {
            body.key.is_some() && body.key.as_deref() == Some(miniturso.config.admin_key.as_str())
        }
        None => false,
    };
    Json(json!({ "valid": valid }))
}

pub async fn list_databases(
    AxumState(state): AxumState<super::AppState>,
    headers: HeaderMap,
) -> axum::response::Response {
    let Some(miniturso) = &state.miniturso else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "miniturso not configured");
    };
    if let Err(status) = require_admin(&headers, &miniturso.config) {
        return error_response(status, "Unauthorized");
    }
    match miniturso.metadata.list() {
        Ok(records) => {
            let databases: Vec<Value> = records
                .iter()
                .map(|record| enrich_database(miniturso, record))
                .collect();
            (Json(json!({ "databases": databases }))).into_response()
        }
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

pub async fn create_database(
    AxumState(state): AxumState<super::AppState>,
    headers: HeaderMap,
    Json(body): Json<CreateDatabaseReq>,
) -> axum::response::Response {
    let Some(miniturso) = &state.miniturso else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "miniturso not configured");
    };
    if let Err(status) = require_admin(&headers, &miniturso.config) {
        return error_response(status, "Unauthorized");
    }

    let Some(id) = body.id else {
        return error_response(StatusCode::BAD_REQUEST, "id is required");
    };
    if id.is_empty()
        || id.len() > 36
        || !id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "id must be 1-36 characters, only [0-9a-z]",
        );
    }
    if miniturso.metadata.get(&id).ok().flatten().is_some() {
        return error_response(StatusCode::CONFLICT, "A database with this id already exists");
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
        created_at: Utc::now().to_rfc3339(),
        last_accessed_at: None,
    };
    if let Err(e) = miniturso.metadata.insert(&record) {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }

    let conn_url = miniturso.config.connection_url();
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

pub async fn get_database(
    AxumState(state): AxumState<super::AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> axum::response::Response {
    let Some(miniturso) = &state.miniturso else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "miniturso not configured");
    };
    if let Err(status) = require_admin(&headers, &miniturso.config) {
        return error_response(status, "Unauthorized");
    }
    match miniturso.metadata.get(&id) {
        Ok(Some(record)) => (Json(enrich_database(miniturso, &record))).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "Database not found"),
        Err(e) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

pub async fn rename_database(
    AxumState(state): AxumState<super::AppState>,
    headers: HeaderMap,
    AxumPath(old_id): AxumPath<String>,
    Json(body): Json<RenameDatabaseReq>,
) -> axum::response::Response {
    let Some(miniturso) = &state.miniturso else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "miniturso not configured");
    };
    if let Err(status) = require_admin(&headers, &miniturso.config) {
        return error_response(status, "Unauthorized");
    }

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

    let Some(old_record) = miniturso.metadata.get(&old_id).ok().flatten() else {
        return error_response(StatusCode::NOT_FOUND, "Database not found");
    };
    if miniturso.metadata.get(&new_id).ok().flatten().is_some() {
        return error_response(StatusCode::CONFLICT, "A database with this id already exists");
    }

    let (signing_key, public_key_b64) = generate_keypair();

    if let Err(e) = create_namespace(&state, &new_id, &public_key_b64).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to create namespace: {}", e),
        );
    }

    // Copy the database file to the new namespace.
    let old_db_path = miniturso
        .config
        .sqld_data_dir
        .join("dbs")
        .join(&old_record.namespace)
        .join("dbs")
        .join("default");
    let new_db_path = miniturso
        .config
        .sqld_data_dir
        .join("dbs")
        .join(&new_id)
        .join("dbs")
        .join("default");
    if old_db_path.exists() {
        let _ = std::fs::copy(&old_db_path, &new_db_path);
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
        created_at: old_record.created_at,
        last_accessed_at: None,
    };
    if let Err(e) = miniturso.metadata.replace(&old_id, &record) {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }

    let conn_url = miniturso.config.connection_url();
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

pub async fn rotate_token(
    AxumState(state): AxumState<super::AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> axum::response::Response {
    let Some(miniturso) = &state.miniturso else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "miniturso not configured");
    };
    if let Err(status) = require_admin(&headers, &miniturso.config) {
        return error_response(status, "Unauthorized");
    }

    let Some(record) = miniturso.metadata.get(&id).ok().flatten() else {
        return error_response(StatusCode::NOT_FOUND, "Database not found");
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
    if let Err(e) = miniturso.metadata.replace(&id, &new_record) {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }

    let conn_url = miniturso.config.connection_url();
    Json(json!({
        "id": new_record.id,
        "namespace": new_record.namespace,
        "token": token,
        "host": conn_url.trim_start_matches("http://").trim_start_matches("https://"),
        "connection_url": conn_url,
    }))
    .into_response()
}

pub async fn delete_database(
    AxumState(state): AxumState<super::AppState>,
    headers: HeaderMap,
    AxumPath(id): AxumPath<String>,
) -> axum::response::Response {
    let Some(miniturso) = &state.miniturso else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "miniturso not configured");
    };
    if let Err(status) = require_admin(&headers, &miniturso.config) {
        return error_response(status, "Unauthorized");
    }

    let Some(record) = miniturso.metadata.get(&id).ok().flatten() else {
        return error_response(StatusCode::NOT_FOUND, "Database not found");
    };

    if let Err(e) = delete_namespace(&state, &record.namespace).await {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to delete namespace: {}", e),
        );
    }

    if let Err(e) = miniturso.metadata.delete(&id) {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string());
    }

    Json(json!({
        "deleted": true,
        "id": record.id,
        "namespace": record.namespace,
    }))
    .into_response()
}

/// Let an admin run SQL against any database. The namespace is taken from
/// the request body; authentication is the admin key, not a database JWT.
pub async fn admin_query(
    AxumState(state): AxumState<super::AppState>,
    headers: HeaderMap,
    Json(body): Json<AdminQueryReq>,
) -> axum::response::Response {
    let Some(miniturso) = &state.miniturso else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "miniturso not configured");
    };
    if let Err(status) = require_admin(&headers, &miniturso.config) {
        return error_response(status, "Unauthorized");
    }

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

    let batch = match super::parse_queries(statements) {
        Ok(batch) => batch,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let auth = Authenticated::FullAccess;
    let connection_maker = match state
        .namespaces
        .with_authenticated(namespace.clone(), auth.clone(), |ns| ns.db.connection_maker())
        .await
    {
        Ok(maker) => maker,
        Err(e) => return error_response(StatusCode::NOT_FOUND, &e.to_string()),
    };
    let ctx = RequestContext::new(auth, namespace, state.namespaces.meta_store().clone());
    let db = match connection_maker.create().await {
        Ok(db) => db,
        Err(e) => return error_response(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    let builder = JsonHttpPayloadBuilder::new();
    match db.execute_batch_or_rollback(batch, ctx, builder, None).await {
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

// ─── Admin router ─────────────────────────────────────────────────────

/// Build the standalone admin router, served on the dedicated admin
/// listener. The admin UI is served at the root path: the dashboard at
/// `/`, assets under `/assets/*`, and the API under `/api/*`.
pub fn admin_router(miniturso: Option<Arc<MinitursoState>>, state: super::AppState) -> Router {
    Router::new()
        .route(
            "/api/databases",
            get(list_databases).post(create_database),
        )
        .route(
            "/api/databases/:id",
            get(get_database)
                .patch(rename_database)
                .delete(delete_database),
        )
        .route("/api/databases/:id/token", post(rotate_token))
        .route("/api/auth/verify", post(verify_key))
        .route("/api/query", post(admin_query))
        .fallback_service(AdminStatic(miniturso))
        .with_state(state)
}

/// Static file service for the admin UI: serves files from `public_dir`
/// with an SPA fallback to `index.html`. Returns a JSON 404 for unmatched
/// API paths.
#[derive(Clone)]
struct AdminStatic(Option<Arc<MinitursoState>>);

impl tower::Service<Request<Body>> for AdminStatic {
    type Response = Response<Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let miniturso = self.0.clone();
        let path = req.uri().path().to_string();
        Box::pin(async move {
            let response = match &miniturso {
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
pub async fn serve_static(miniturso: Arc<MinitursoState>, request_path: &str) -> Response<Body> {
    let Some(public_dir) = &miniturso.config.public_dir else {
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
