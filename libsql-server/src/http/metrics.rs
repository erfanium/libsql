//! `/metrics`: the Prometheus metrics of the server, served on the port of the user API to anyone
//! who asks.

use std::cell::OnceCell;
use std::time::Duration;

use axum::extract::State;
use axum::routing::get;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use parking_lot::Mutex;

#[derive(Clone)]
struct Metrics {
    handle: Option<PrometheusHandle>,
}

impl Metrics {
    fn render(&self) -> String {
        self.handle.as_ref().map(|h| h.render()).unwrap_or_default()
    }
}

static PROM_HANDLE: Mutex<OnceCell<PrometheusHandle>> = Mutex::new(OnceCell::new());

/// With `disable_metrics`, nothing is recorded and `/metrics` is empty.
pub fn router(disable_metrics: bool) -> axum::Router {
    let app_label = std::env::var("SQLD_APP_LABEL").ok();
    let ver = env!("CARGO_PKG_VERSION");

    let prom_handle = if !disable_metrics {
        let lock = PROM_HANDLE.lock();
        let prom_handle = lock.get_or_init(|| {
            tracing::info!("initializing prometheus metrics");
            let b = PrometheusBuilder::new().idle_timeout(
                metrics_util::MetricKindMask::ALL,
                Some(Duration::from_secs(120)),
            );

            if let Some(app_label) = app_label {
                b.add_global_label("app", app_label)
                    .add_global_label("version", ver)
                    .install_recorder()
                    .unwrap()
            } else {
                b.install_recorder().unwrap()
            }
        });

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

        Some(prom_handle.clone())
    } else {
        None
    };

    metrics::increment_counter!("libsql_server_count");

    axum::Router::new()
        .route("/metrics", get(handle_metrics))
        .with_state(Metrics {
            handle: prom_handle,
        })
}

async fn handle_metrics(State(metrics): State<Metrics>) -> String {
    metrics.render()
}
