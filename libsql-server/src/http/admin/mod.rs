//! Platform admin API: database lifecycle, admin querying, and
//! Prometheus metrics.
//!
//! The admin router (see [`api::admin_router`]) is served on the
//! dedicated admin listener (`ADMIN_LISTEN_ADDR`, default
//! `127.0.0.1:3001`). Prometheus metrics are exported at `/metrics` on the
//! same listener; [`init_metrics`] installs the global recorder and keeps
//! the tokio runtime gauges updated.

use std::sync::Mutex;
use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

pub mod api;
pub mod stats;

/// The global Prometheus handle, installed once by [`init_metrics`].
static PROM_HANDLE: Mutex<Option<PrometheusHandle>> = Mutex::new(None);

/// Initialize the Prometheus recorder and spawn the task that keeps the
/// tokio runtime gauges up to date. Idempotent: only the first call
/// installs the recorder.
pub fn init_metrics() {
    {
        let mut handle = PROM_HANDLE.lock().unwrap();
        if handle.is_some() {
            return;
        }

        tracing::info!("initializing prometheus metrics");
        let app_label = std::env::var("SQLD_APP_LABEL").ok();
        let ver = env!("CARGO_PKG_VERSION");

        let builder = PrometheusBuilder::new().idle_timeout(
            metrics_util::MetricKindMask::ALL,
            Some(Duration::from_secs(120)),
        );
        let builder = match app_label {
            Some(app_label) => builder
                .add_global_label("app", app_label)
                .add_global_label("version", ver),
            None => builder,
        };
        let prom_handle = builder
            .install_recorder()
            .expect("metrics recorder can only be installed once");
        *handle = Some(prom_handle);
    }

    tokio::task::spawn(async move {
        loop {
            let runtime = tokio::runtime::Handle::current();
            let metrics = runtime.metrics();
            crate::metrics::TOKIO_RUNTIME_BLOCKING_QUEUE_DEPTH
                .set(metrics.blocking_queue_depth() as f64);
            crate::metrics::TOKIO_RUNTIME_INJECTION_QUEUE_DEPTH
                .set(metrics.injection_queue_depth() as f64);
            crate::metrics::TOKIO_RUNTIME_NUM_BLOCKING_THREADS
                .set(metrics.num_blocking_threads() as f64);
            crate::metrics::TOKIO_RUNTIME_NUM_IDLE_BLOCKING_THREADS
                .set(metrics.num_idle_blocking_threads() as f64);
            crate::metrics::TOKIO_RUNTIME_NUM_WORKERS.set(metrics.num_workers() as f64);

            crate::metrics::TOKIO_RUNTIME_IO_DRIVER_FD_DEREGISTERED_COUNT
                .absolute(metrics.io_driver_fd_deregistered_count() as u64);
            crate::metrics::TOKIO_RUNTIME_IO_DRIVER_FD_REGISTERED_COUNT
                .absolute(metrics.io_driver_fd_registered_count() as u64);
            crate::metrics::TOKIO_RUNTIME_IO_DRIVER_READY_COUNT
                .absolute(metrics.io_driver_ready_count() as u64);
            crate::metrics::TOKIO_RUNTIME_REMOTE_SCHEDULE_COUNT
                .absolute(metrics.remote_schedule_count() as u64);

            crate::metrics::SERVER_COUNT.set(1.0);
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });
}

/// Render all registered Prometheus metrics as text. Served at `/metrics`
/// by the admin router.
pub async fn render_metrics() -> String {
    PROM_HANDLE
        .lock()
        .unwrap()
        .as_ref()
        .map(|h| h.render())
        .unwrap_or_default()
}
