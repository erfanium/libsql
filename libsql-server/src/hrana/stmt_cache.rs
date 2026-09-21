use anyhow::{bail, Result};
use moka::sync::Cache;
use std::sync::LazyLock;

use crate::query_analysis::Statement;

use super::stmt::StmtError;

/// Parsing a statement into an AST is expensive and the result depends only on
/// the SQL text — bind values do not influence the classification. Hrana
/// clients commonly re-send byte-identical statements (e.g. batched
/// `INSERT OR REPLACE`), so the parsed statement is memoized by SQL text.
///
/// The cache is bounded by the combined byte size of key + statement, so a
/// stream of unique huge statements cannot grow it without limit.
const PARSED_STMT_CACHE_BYTES: u64 = 64 * 1024 * 1024;

static PARSED_STMT_CACHE: LazyLock<Cache<String, Statement>> = LazyLock::new(|| {
    Cache::builder()
        .weigher(|key: &String, stmt: &Statement| {
            (key.len() + stmt.stmt.len()).min(u32::MAX as usize) as u32
        })
        .max_capacity(PARSED_STMT_CACHE_BYTES)
        .build()
});

/// Parses a single statement, memoizing the result by SQL text.
pub(super) fn parse_cached(sql: &str) -> Result<Statement> {
    if let Some(stmt) = PARSED_STMT_CACHE.get(sql) {
        return Ok(stmt);
    }
    let stmt = parse_uncached(sql)?;
    PARSED_STMT_CACHE.insert(sql.to_string(), stmt.clone());
    Ok(stmt)
}

/// Uncached single-statement parse + validation. A cache entry is exactly this
/// result, so it only runs once per distinct SQL text.
fn parse_uncached(sql: &str) -> Result<Statement> {
    let mut stmt_iter = Statement::parse(sql);
    let stmt = match stmt_iter.next() {
        Some(Ok(stmt)) => stmt,
        Some(Err(err)) => bail!(StmtError::SqlParse { source: err }),
        None => bail!(StmtError::SqlNoStmt),
    };

    if stmt_iter.next().is_some() {
        bail!(StmtError::SqlManyStmts)
    }

    Ok(stmt)
}
