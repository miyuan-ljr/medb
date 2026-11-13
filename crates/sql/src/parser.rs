//! 语句解析封装。

use common::error::{Error, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn parse_statements(sql: &str) -> Result<Vec<Statement>> {
    Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| Error::sql(format!("parse failed: {e}")))
}

pub fn parse_single(sql: &str) -> Result<Statement> {
    let mut statements = parse_statements(sql)?;
    match statements.len() {
        0 => Err(Error::sql("empty statement")),
        1 => Ok(statements.remove(0)),
        _ => Err(Error::sql("multi-statement is not supported yet")),
    }
}
