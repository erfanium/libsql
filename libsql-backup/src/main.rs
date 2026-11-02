use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use libsql_backup::backup::{Backup, BackupOptions};
use libsql_backup::config::{BackupConfig, S3Config, SqldConfig};
use libsql_backup::fleet::{AdminClient, Fleet, FleetOptions};
use libsql_backup::serve::{self, ServeOptions, Summary};
use libsql_backup::status;
use libsql_backup::store::Store;

#[derive(Parser, Debug)]
#[clap(version, about = "Page-level S3 backups of sqld namespaces")]
struct Cli {
    #[clap(flatten)]
    s3: S3Config,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Backs up one namespace, or all of them.
    Backup {
        /// Namespace to back up.
        #[clap(required_unless_present = "all", conflicts_with = "all")]
        namespace: Option<String>,
        /// Backs up every namespace listed by sqld's admin API.
        #[clap(long)]
        all: bool,
        #[clap(flatten)]
        sqld: SqldConfig,
        #[clap(flatten)]
        backup: BackupConfig,
    },
    /// Prints the status of a namespace's backup.
    Status { namespace: String },
    /// Serves an HTTP API to trigger backups, and backs up on a schedule.
    Serve {
        #[clap(long, env = "LIBSQL_BACKUP_LISTEN", default_value = "0.0.0.0:8090")]
        listen: SocketAddr,
        /// Bearer key required by the API.
        #[clap(long, env = "LIBSQL_BACKUP_AUTH_KEY", hide_env_values = true)]
        auth_key: Option<String>,
        /// Backs up every namespace once per interval (e.g. `24h`), spread over the interval.
        #[clap(long, env = "LIBSQL_BACKUP_SCHEDULE_INTERVAL", value_parser = humantime::parse_duration)]
        schedule_interval: Option<Duration>,
        #[clap(flatten)]
        sqld: SqldConfig,
        #[clap(flatten)]
        backup: BackupConfig,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    match run(Cli::parse()).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<ExitCode> {
    let store = Store::new(&cli.s3).await?;
    match cli.command {
        Command::Backup {
            namespace,
            all,
            sqld,
            backup,
        } => {
            let fleet = make_fleet(store, &sqld, &backup)?;
            let results = if all {
                fleet.backup_all().await?
            } else {
                vec![fleet.backup_default(&namespace.unwrap()).await]
            };
            let summary = Summary::new(results);
            print_json(&summary)?;
            Ok(if summary.failed > 0 {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            })
        }
        Command::Status { namespace } => match status::read(&store, &namespace).await? {
            Some(current) => {
                print_json(&current.status)?;
                Ok(ExitCode::SUCCESS)
            }
            None => bail!("`{namespace}` has no backup"),
        },
        Command::Serve {
            listen,
            auth_key,
            schedule_interval,
            sqld,
            backup,
        } => {
            let fleet = make_fleet(store, &sqld, &backup)?;
            serve::serve(
                fleet,
                ServeOptions {
                    listen,
                    auth_key,
                    schedule_interval,
                },
            )
            .await?;
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn make_fleet(store: Store, sqld: &SqldConfig, backup: &BackupConfig) -> Result<Fleet> {
    anyhow::ensure!(backup.max_wal_pages > 0, "--max-wal-pages must be positive");
    let http = reqwest::Client::builder()
        .timeout(sqld.sqld_timeout)
        .pool_max_idle_per_host(backup.concurrency.max(1))
        .build()
        .context("building HTTP client")?;
    let admin = sqld
        .sqld_admin_url
        .as_deref()
        .map(|url| AdminClient::new(http.clone(), url, sqld.sqld_admin_key.clone()));
    let options = BackupOptions {
        max_wal_files: backup.max_wal_files,
        max_wal_pages: backup.max_wal_pages,
        zstd_level: backup.zstd_level,
    };
    Ok(Fleet::new(
        Backup::new(store, options),
        http,
        FleetOptions {
            sqld_url: sqld.sqld_url.clone(),
            default_key: sqld.sqld_default_key.clone(),
            admin,
            concurrency: backup.concurrency,
        },
    ))
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
