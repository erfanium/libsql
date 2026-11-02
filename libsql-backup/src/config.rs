use std::time::Duration;

use clap::Args;

#[derive(Args, Debug, Clone)]
pub struct SqldConfig {
    /// sqld's user API, where `/sync` is served. Used by the CLI and the scheduler; requests to
    /// `serve` name their own source.
    #[clap(long, env = "LIBSQL_BACKUP_SQLD_URL")]
    pub sqld_url: Option<String>,
    /// Bearer key sent to sqld when a request does not give its own.
    #[clap(long, env = "SQLD_DEFAULT_KEY", hide_env_values = true)]
    pub sqld_default_key: Option<String>,
    /// sqld's admin API, used to list the namespaces of `--all`.
    #[clap(long, env = "LIBSQL_BACKUP_SQLD_ADMIN_URL")]
    pub sqld_admin_url: Option<String>,
    /// Bearer key of sqld's admin API.
    #[clap(long, env = "LIBSQL_BACKUP_SQLD_ADMIN_KEY", hide_env_values = true)]
    pub sqld_admin_key: Option<String>,
    /// Timeout of one request to sqld, a whole clone included.
    #[clap(long, env = "LIBSQL_BACKUP_SQLD_TIMEOUT", default_value = "30m", value_parser = humantime::parse_duration)]
    pub sqld_timeout: Duration,
}

#[derive(Args, Debug, Clone)]
pub struct S3Config {
    #[clap(long = "s3-bucket", env = "LIBSQL_BACKUP_S3_BUCKET")]
    pub bucket: String,
    /// Key prefix under which the namespaces are stored.
    #[clap(
        long = "s3-prefix",
        env = "LIBSQL_BACKUP_S3_PREFIX",
        default_value = ""
    )]
    pub prefix: String,
    /// Custom endpoint, e.g. for MinIO. Buckets are then addressed path-style.
    #[clap(long = "s3-endpoint", env = "LIBSQL_BACKUP_S3_ENDPOINT")]
    pub endpoint: Option<String>,
    #[clap(
        long = "s3-region",
        env = "LIBSQL_BACKUP_S3_REGION",
        default_value = "us-east-1"
    )]
    pub region: String,
    /// Static credentials. The default AWS credential chain is used without them.
    #[clap(long = "s3-access-key-id", env = "LIBSQL_BACKUP_S3_ACCESS_KEY_ID")]
    pub access_key_id: Option<String>,
    #[clap(
        long = "s3-secret-access-key",
        env = "LIBSQL_BACKUP_S3_SECRET_ACCESS_KEY",
        hide_env_values = true
    )]
    pub secret_access_key: Option<String>,
    #[clap(
        long = "s3-session-token",
        env = "LIBSQL_BACKUP_S3_SESSION_TOKEN",
        hide_env_values = true
    )]
    pub session_token: Option<String>,
}

#[derive(Args, Debug, Clone)]
pub struct BackupConfig {
    /// A new full snapshot is made rather than keeping more WAL segments than this.
    #[clap(long, env = "LIBSQL_BACKUP_MAX_WAL_FILES", default_value_t = 8)]
    pub max_wal_files: usize,
    /// Pages per WAL segment (2048 pages is ~8MB before compression).
    #[clap(long, env = "LIBSQL_BACKUP_MAX_WAL_PAGES", default_value_t = 2048)]
    pub max_wal_pages: usize,
    #[clap(long, env = "LIBSQL_BACKUP_ZSTD_LEVEL", default_value_t = 3)]
    pub zstd_level: i32,
    /// Namespaces backed up at the same time by `--all`.
    #[clap(long, env = "LIBSQL_BACKUP_CONCURRENCY", default_value_t = 8)]
    pub concurrency: usize,
}
