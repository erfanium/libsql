//! Page-level backups of sqld namespaces to S3.
//!
//! The backup acts like an embedded replica: it pulls frames from sqld's `GET /sync` route and
//! stores them as a full compressed snapshot plus a few compressed WAL segments. It keeps no
//! state of its own: everything it needs is in the bucket, `status.json` being the commit point.

pub mod backup;
pub mod config;
pub mod dbfile;
pub mod fleet;
pub mod frame;
pub mod serve;
pub mod status;
pub mod store;
pub mod sync;
