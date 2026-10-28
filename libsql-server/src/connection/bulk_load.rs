use std::collections::HashMap;
use std::io;

use bytes::BytesMut;
use libsql_replication::rpc::load::{load_frame::Kind, value::Value as V, LoadFrame, Value};
use prost::Message;
use rusqlite::types::Null;
use rusqlite::{Connection, Statement};
use serde::Serialize;
use tokio_util::codec::Decoder;

pub const MAX_FRAME_BYTES: usize = 64 << 20;
const MAX_COLUMNS: usize = 1000;
const LOAD_CACHE_KIB: u32 = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("failed to read the load stream: {0}")]
    Io(#[from] io::Error),
    #[error("invalid load stream at frame {frame}: {message}")]
    Protocol { frame: u64, message: String },
    #[error("sqlite error at frame {frame}: {source}")]
    Sqlite { frame: u64, source: rusqlite::Error },
}

fn protocol(frame: u64, message: impl Into<String>) -> LoadError {
    LoadError::Protocol {
        frame,
        message: message.into(),
    }
}

#[derive(Debug, Default, Serialize)]
pub struct LoadSummary {
    pub rows: u64,
    pub tables: HashMap<String, u64>,
}

#[derive(Default)]
pub struct LoadFrameCodec {
    frames: u64,
}

impl Decoder for LoadFrameCodec {
    type Item = LoadFrame;
    type Error = LoadError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<LoadFrame>, LoadError> {
        let frame = self.frames + 1;
        let len = match prost::decode_length_delimiter(&src[..]) {
            Ok(len) => len,
            Err(_) if src.len() < 10 => return Ok(None),
            Err(e) => return Err(protocol(frame, format!("invalid frame length: {e}"))),
        };
        if len > MAX_FRAME_BYTES {
            return Err(protocol(
                frame,
                format!("frame of {len} bytes exceeds the {MAX_FRAME_BYTES} bytes limit"),
            ));
        }
        let total = prost::length_delimiter_len(len) + len;
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }
        let message = LoadFrame::decode_length_delimited(src.split_to(total).freeze())
            .map_err(|e| protocol(frame, format!("malformed frame: {e}")))?;
        self.frames = frame;
        Ok(Some(message))
    }
}

struct TxnGuard<'a> {
    conn: &'a Connection,
    cache_size: i64,
    committed: bool,
}

impl Drop for TxnGuard<'_> {
    fn drop(&mut self) {
        if !self.committed && !self.conn.is_autocommit() {
            if let Err(e) = self.conn.execute_batch("ROLLBACK") {
                tracing::error!("failed to roll back load: {e}");
            }
        }
        let restore = format!("PRAGMA cache_size = {}", self.cache_size);
        if let Err(e) = self.conn.execute_batch(&restore) {
            tracing::error!("failed to restore cache_size after load: {e}");
        }
    }
}

struct Target<'a> {
    name: String,
    ncols: usize,
    stmt: Statement<'a>,
    rows: u64,
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn bind(stmt: &mut Statement, index: usize, value: &Value) -> rusqlite::Result<()> {
    match &value.value {
        None => stmt.raw_bind_parameter(index, Null),
        Some(V::Integer(i)) => stmt.raw_bind_parameter(index, i),
        Some(V::Real(f)) => stmt.raw_bind_parameter(index, f),
        Some(V::Text(s)) => stmt.raw_bind_parameter(index, s.as_str()),
        Some(V::Blob(b)) => stmt.raw_bind_parameter(index, b.as_slice()),
    }
}

pub fn apply_load(
    conn: &Connection,
    frames: impl IntoIterator<Item = Result<LoadFrame, LoadError>>,
) -> Result<LoadSummary, LoadError> {
    let mut frames = frames.into_iter();
    let mut index: u64 = 0;
    let sqlite = |frame: u64| move |source: rusqlite::Error| LoadError::Sqlite { frame, source };

    let cache_size: i64 = conn
        .query_row("PRAGMA cache_size", [], |row| row.get(0))
        .map_err(sqlite(0))?;
    conn.execute_batch(&format!("PRAGMA cache_size = -{LOAD_CACHE_KIB}"))
        .map_err(sqlite(0))?;
    let mut guard = TxnGuard {
        conn,
        cache_size,
        committed: false,
    };
    conn.execute_batch("BEGIN IMMEDIATE").map_err(sqlite(0))?;

    let mut targets: HashMap<u32, Target> = HashMap::new();
    let mut total: u64 = 0;

    loop {
        let Some(frame) = frames.next() else {
            return Err(protocol(index, "stream ended before the End frame"));
        };
        let frame = frame?;
        index += 1;

        match frame.kind {
            Some(Kind::Table(table)) => {
                if targets.contains_key(&table.id) {
                    return Err(protocol(
                        index,
                        format!("table id {} declared twice", table.id),
                    ));
                }
                let ncols = table.columns.len();
                if ncols == 0 || ncols > MAX_COLUMNS {
                    return Err(protocol(
                        index,
                        format!(
                            "table `{}` must have 1 to {MAX_COLUMNS} columns, got {ncols}",
                            table.name
                        ),
                    ));
                }
                let columns = table
                    .columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let params = vec!["?"; ncols].join(", ");
                let sql = format!(
                    "INSERT OR REPLACE INTO {} ({columns}) VALUES ({params})",
                    quote_ident(&table.name)
                );
                let stmt = conn.prepare(&sql).map_err(sqlite(index))?;
                targets.insert(
                    table.id,
                    Target {
                        name: table.name,
                        ncols,
                        stmt,
                        rows: 0,
                    },
                );
            }
            Some(Kind::Rows(rows)) => {
                let Some(target) = targets.get_mut(&rows.table_id) else {
                    return Err(protocol(
                        index,
                        format!("rows for undeclared table id {}", rows.table_id),
                    ));
                };
                let row_count = rows.row_count as usize;
                if rows.values.len() != row_count * target.ncols {
                    return Err(protocol(
                        index,
                        format!(
                            "rows frame for `{}` has {} values, expected {row_count} rows x {} columns",
                            target.name,
                            rows.values.len(),
                            target.ncols
                        ),
                    ));
                }
                for row in rows.values.chunks_exact(target.ncols) {
                    for (i, value) in row.iter().enumerate() {
                        bind(&mut target.stmt, i + 1, value).map_err(sqlite(index))?;
                    }
                    target.stmt.raw_execute().map_err(sqlite(index))?;
                }
                target.rows += row_count as u64;
                total += row_count as u64;
            }
            Some(Kind::End(end)) => {
                if end.total_rows != total {
                    return Err(protocol(
                        index,
                        format!("End frame says {} rows, received {total}", end.total_rows),
                    ));
                }
                if let Some(extra) = frames.next() {
                    extra?;
                    return Err(protocol(index + 1, "data after the End frame"));
                }
                break;
            }
            None => return Err(protocol(index, "frame without a kind")),
        }
    }

    let tables = targets
        .into_values()
        .map(|target| (target.name, target.rows))
        .collect();

    conn.execute_batch("COMMIT").map_err(sqlite(index))?;
    guard.committed = true;

    Ok(LoadSummary {
        rows: total,
        tables,
    })
}

#[cfg(test)]
mod test {
    use libsql_replication::rpc::load::{End, Rows, Table};

    use super::*;

    fn frame(kind: Kind) -> Vec<u8> {
        LoadFrame { kind: Some(kind) }.encode_length_delimited_to_vec()
    }

    fn table(id: u32, name: &str, columns: &[&str]) -> Vec<u8> {
        frame(Kind::Table(Table {
            id,
            name: name.into(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
        }))
    }

    fn rows(table_id: u32, row_count: u32, values: Vec<Option<V>>) -> Vec<u8> {
        frame(Kind::Rows(Rows {
            table_id,
            row_count,
            values: values.into_iter().map(|value| Value { value }).collect(),
        }))
    }

    fn end(total_rows: u64) -> Vec<u8> {
        frame(Kind::End(End { total_rows }))
    }

    fn frames_of(stream: &[u8]) -> Vec<Result<LoadFrame, LoadError>> {
        let mut codec = LoadFrameCodec::default();
        let mut buf = BytesMut::new();
        let mut out = Vec::new();
        for chunk in stream.chunks(7) {
            buf.extend_from_slice(chunk);
            loop {
                match codec.decode(&mut buf) {
                    Ok(Some(frame)) => out.push(Ok(frame)),
                    Ok(None) => break,
                    Err(e) => {
                        out.push(Err(e));
                        return out;
                    }
                }
            }
        }
        match codec.decode_eof(&mut buf) {
            Ok(Some(frame)) => out.push(Ok(frame)),
            Ok(None) => {}
            Err(e) => out.push(Err(e)),
        }
        out
    }

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER, r REAL, b BLOB, \"we\"\"ird\" TEXT);
             CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT);
             INSERT INTO t (id, n) VALUES ('keep', 1);",
        )
        .unwrap();
        conn
    }

    fn cache_size(conn: &Connection) -> i64 {
        conn.query_row("PRAGMA cache_size", [], |r| r.get(0))
            .unwrap()
    }

    fn count(conn: &Connection, table: &str) -> i64 {
        conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn loads_interleaved_tables_and_replaces() {
        let conn = db();
        let stream = [
            table(1, "t", &["id", "n", "r", "b", "we\"ird"]),
            table(2, "u", &["id", "name"]),
            rows(
                1,
                2,
                vec![
                    Some(V::Text("a".into())),
                    Some(V::Integer(-5)),
                    Some(V::Real(1.5)),
                    Some(V::Blob(vec![1, 2])),
                    None,
                    Some(V::Text("keep".into())),
                    Some(V::Integer(i64::MAX)),
                    None,
                    None,
                    Some(V::Text("x".into())),
                ],
            ),
            rows(
                2,
                1,
                vec![Some(V::Integer(7)), Some(V::Text("seven".into()))],
            ),
            rows(
                2,
                1,
                vec![Some(V::Integer(7)), Some(V::Text("SEVEN".into()))],
            ),
            end(4),
        ]
        .concat();

        let summary = apply_load(&conn, frames_of(&stream)).unwrap();
        assert_eq!(summary.rows, 4);
        assert_eq!(summary.tables["t"], 2);
        assert_eq!(summary.tables["u"], 2);
        assert!(conn.is_autocommit());
        assert_eq!(cache_size(&conn), -2000);

        let (n, r, b): (i64, f64, Vec<u8>) = conn
            .query_row("SELECT n, r, b FROM t WHERE id = 'a'", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
        assert_eq!((n, r, b), (-5, 1.5, vec![1, 2]));
        let n: i64 = conn
            .query_row("SELECT n FROM t WHERE id = 'keep'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, i64::MAX);
        let name: String = conn
            .query_row("SELECT name FROM u WHERE id = 7", [], |r| r.get(0))
            .unwrap();
        assert_eq!(name, "SEVEN");
        assert_eq!(count(&conn, "u"), 1);
    }

    fn assert_rolled_back(stream: Vec<u8>) -> LoadError {
        let conn = db();
        let err = apply_load(&conn, frames_of(&stream)).unwrap_err();
        assert!(conn.is_autocommit());
        assert_eq!(cache_size(&conn), -2000);
        assert_eq!(count(&conn, "t"), 1);
        assert_eq!(count(&conn, "u"), 0);
        err
    }

    fn some_rows() -> Vec<u8> {
        [
            table(2, "u", &["id", "name"]),
            rows(2, 1, vec![Some(V::Integer(1)), None]),
        ]
        .concat()
    }

    #[test]
    fn missing_end_rolls_back() {
        let err = assert_rolled_back(some_rows());
        assert!(matches!(err, LoadError::Protocol { .. }), "{err}");
    }

    #[test]
    fn truncated_frame_rolls_back() {
        let mut stream = [some_rows(), end(1)].concat();
        stream.pop();
        let err = assert_rolled_back(stream);
        assert!(matches!(err, LoadError::Io(_)), "{err}");
    }

    #[test]
    fn wrong_total_rolls_back() {
        assert_rolled_back([some_rows(), end(2)].concat());
    }

    #[test]
    fn trailing_data_rolls_back() {
        assert_rolled_back([some_rows(), end(1), vec![0]].concat());
    }

    #[test]
    fn unknown_column_rolls_back() {
        let err =
            assert_rolled_back([some_rows(), table(1, "t", &["id", "nope"]), end(1)].concat());
        assert!(matches!(err, LoadError::Sqlite { .. }), "{err}");
    }

    #[test]
    fn value_count_mismatch_rolls_back() {
        assert_rolled_back(
            [
                some_rows(),
                rows(2, 2, vec![Some(V::Integer(2)), None, Some(V::Integer(3))]),
                end(3),
            ]
            .concat(),
        );
    }

    #[test]
    fn undeclared_table_rolls_back() {
        assert_rolled_back([some_rows(), rows(9, 0, vec![]), end(1)].concat());
    }

    #[test]
    fn constraint_violation_rolls_back() {
        let conn = db();
        conn.execute_batch("CREATE TABLE nn (id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
            .unwrap();
        let stream = [
            some_rows(),
            table(3, "nn", &["id", "v"]),
            rows(3, 1, vec![Some(V::Integer(1)), None]),
            end(2),
        ]
        .concat();
        let err = apply_load(&conn, frames_of(&stream)).unwrap_err();
        assert!(matches!(err, LoadError::Sqlite { .. }), "{err}");
        assert_eq!(count(&conn, "u"), 0);
    }

    #[test]
    fn oversized_frame_is_rejected_before_reading() {
        let mut stream = some_rows();
        prost::encoding::encode_varint(MAX_FRAME_BYTES as u64 + 1, &mut stream);
        let err = assert_rolled_back(stream);
        assert!(matches!(err, LoadError::Protocol { frame: 3, .. }), "{err}");
    }
}
