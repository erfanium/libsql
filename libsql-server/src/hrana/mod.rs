use std::fmt;

pub mod batch;
mod cursor;
pub mod http;
mod result_builder;
pub mod stmt;
mod stmt_cache;
pub use libsql_hrana::proto;

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub enum Version {
    Hrana2,
    Hrana3,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Encoding {
    Json,
    Protobuf,
}

/// An unrecoverable protocol error that should close the HTTP stream. A correct
/// client should never trigger any of these errors.
#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
    #[error("Cannot deserialize client message from JSON: {source}")]
    JsonDeserialize { source: serde_json::Error },
    #[error("Could not decode client message from Protobuf: {source}")]
    ProtobufDecode { source: prost::DecodeError },

    #[error("Either `sql` or `sql_id` are required, but not both")]
    SqlIdAndSqlGiven,
    #[error("Either `sql` or `sql_id` are required")]
    SqlIdOrSqlNotGiven,
    #[error("SQL text {sql_id} not found")]
    SqlNotFound { sql_id: i32 },
    #[error("SQL text {sql_id} already exists")]
    SqlExists { sql_id: i32 },

    #[error("Invalid reference to step in a batch condition")]
    BatchCondBadStep,

    #[error("Received an invalid baton")]
    BatonInvalid,
    #[error("Received a baton that has already been used")]
    BatonReused,
    #[error("Stream for this baton was closed")]
    BatonStreamClosed,

    #[error("{what} is only supported in protocol version {min_version} and higher")]
    NotSupported {
        what: &'static str,
        min_version: Version,
    },

    #[error("BatchCond type not recognized")]
    NoneBatchCond,
    #[error("Value type not recognized")]
    NoneValue,
    #[error("StreamRequest type not recognized")]
    NoneStreamRequest,
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Version::Hrana2 => write!(f, "hrana2"),
            Version::Hrana3 => write!(f, "hrana3"),
        }
    }
}
