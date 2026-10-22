//! Error reporting to Sentry.
//!
//! Only issues are sent: `tracing::error!` events and panics. No performance tracing,
//! no spans and no logs. `warn!` events are kept as breadcrumbs on the next issue.
//!
//! Reporting is disabled unless `SENTRY_DSN` is set. `SENTRY_ENVIRONMENT` and
//! `SENTRY_RELEASE` are read from the environment too.

use std::time::Duration;

use sentry::integrations::tracing::EventFilter;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer as _;

/// Starts the Sentry client. Keep the guard alive until the process exits: dropping it
/// flushes pending events.
pub fn init() -> sentry::ClientInitGuard {
    sentry::init(sentry::ClientOptions {
        release: sentry::release_name!(),
        // stack traces give better issue grouping than our interpolated log messages
        attach_stacktrace: true,
        // queries and namespaces can carry user data
        send_default_pii: false,
        traces_sample_rate: 0.0,
        ..Default::default()
    })
}

/// A tracing layer that turns `error!` events into Sentry issues.
pub fn tracing_layer<S>() -> impl tracing_subscriber::Layer<S>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    sentry::integrations::tracing::layer()
        .event_filter(|md| match *md.level() {
            // events bridged from the `log` crate. In this workspace only the SQL parser
            // uses it, and it logs client syntax errors at ERROR with query text in them.
            _ if md.target() == "log" => EventFilter::Ignore,
            tracing::Level::ERROR => EventFilter::Event,
            tracing::Level::WARN => EventFilter::Breadcrumb,
            _ => EventFilter::Ignore,
        })
        .span_filter(|_| false)
        // without its own filter, this layer would enable every debug/trace callsite
        .with_filter(LevelFilter::WARN)
}

/// Sends pending events. Call before `std::process::exit` or `abort`, which skip the guard's drop.
pub fn flush() {
    if let Some(client) = sentry::Hub::current().client() {
        client.flush(Some(Duration::from_secs(2)));
    }
}
