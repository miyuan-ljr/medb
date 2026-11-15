//! 查询处理器:PG simple query 协议 → Engine → pgwire Response。
//!
//! 支持一条 Query 消息里带多条语句(分号分隔),逐条产出响应;
//! 任一语句失败即返回 ErrorResponse,连接保持可用。
//! extended query protocol(Parse/Bind/Execute)经占位符 `$n` 绑定参数:
//! 计划期把占位符替换为绑定值字面量(类型保真);Describe 以 NULL 绑定
//! 推导输出列 schema。

use std::sync::Arc;

use async_trait::async_trait;
use common::error::ErrorCode;
use common::schema::{ColumnDesc, Row};
use common::types::{DataType, ScalarValue};
use futures::Sink;
use futures::stream;
use parking_lot::Mutex;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::stmt::QueryParser;
use pgwire::api::portal::Format;
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult, SEVERITY_ERROR};
use pgwire::messages::PgWireBackendMessage;
use tracing::warn;

use crate::engine::{Engine, Outcome};
use crate::session::Session;

/// 服务端共享状态(每连接一份 Handlers,共享状态)。
#[derive(Clone)]
pub struct ServerState {
    pub engine: Arc<Engine>,
}

#[derive(Clone)]
pub struct Handlers {
    state: ServerState,
}

impl Handlers {
    pub fn new(state: ServerState) -> Self {
        Self { state }
    }
}

impl PgWireServerHandlers for Handlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::new(self.clone())
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::new(self.clone())
    }
}

#[async_trait]
impl SimpleQueryHandler for Handlers {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // 连接级 Session(事务缓冲等)挂到 pgwire 的 session extensions
        let session = client
            .session_extensions()
            .get_or_insert_with(|| Mutex::new(Session::new(self.state.engine.clone())));
        let mut session = session.lock();
        match session.execute_script(query) {
            Ok(outcomes) => {
                let mut responses = Vec::with_capacity(outcomes.len());
                for outcome in outcomes {
                    match outcome {
                        Outcome::Query { schema, rows } => {
                            responses.push(Response::Query(query_response(schema, rows)?));
                        }
                        Outcome::Tag { prefix, rows } => {
                            let mut tag = Tag::new(&prefix);
                            if let Some(n) = rows {
                                tag = tag.with_rows(n as usize);
                            }
                            responses.push(Response::Execution(tag));
                        }
                    }
                }
                Ok(responses)
            }
            Err(err) => {
                warn!(error = %err, "statement failed");
                Ok(vec![Response::Error(Box::new(error_to_info(&err)))])
            }
        }
    }
}

/// 引擎结果 → QueryResponse(文本格式行)。
fn query_response(schema: Vec<ColumnDesc>, rows: Vec<Row>) -> PgWireResult<QueryResponse> {
    let fields: Arc<Vec<FieldInfo>> = Arc::new(
        schema
            .iter()
            .map(|c| {
                FieldInfo::new(
                    c.name.clone(),
                    None,
                    None,
                    pg_type(c.ty.clone()),
                    FieldFormat::Text,
                )
            })
            .collect(),
    );
    let mut data_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut encoder = DataRowEncoder::new(fields.clone());
        for value in row {
            let text = value.to_text();
            encoder.encode_field(&text)?;
        }
        data_rows.push(encoder.take_row());
    }
    Ok(QueryResponse::new(
        fields,
        stream::iter(data_rows.into_iter().map(Ok)),
    ))
}

fn pg_type(ty: DataType) -> Type {
    match ty {
        DataType::Boolean => Type::BOOL,
        DataType::Int64 => Type::INT8,
        DataType::Float64 => Type::FLOAT8,
        DataType::Date => Type::DATE,
        DataType::Utf8 => Type::TEXT,
        DataType::Binary => Type::BYTEA,
        // 数组/向量以 PG 文本格式输出,声明为 TEXT 保证任意客户端可读
        DataType::Array(_) | DataType::Vector(_) => Type::TEXT,
    }
}

fn error_to_info(err: &common::error::Error) -> ErrorInfo {
    let code = match err.code() {
        ErrorCode::NotImplemented | ErrorCode::NotSupported => "0A000", // feature_not_supported
        ErrorCode::MemoryLimitExceeded => "53200",                      // out_of_memory
        ErrorCode::InvalidArgument => "22023",                          // invalid_parameter_value
        ErrorCode::Sql => "42601",                                      // syntax_error
        ErrorCode::Catalog => "42P01",                                  // undefined_table
        ErrorCode::Corruption => "XX001",                               // data_corrupted
        _ => "XX000",                                                   // internal_error
    };
    ErrorInfo::new(SEVERITY_ERROR.to_owned(), code.to_owned(), err.to_string())
}

// ---------- extended query protocol ----------

/// 预编译语句:原始 SQL(Parse 期只做语法检查;占位符在 Bind 后的计划期解析)。
#[derive(Clone)]
pub struct MedbPrepared {
    sql: String,
}

fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        SEVERITY_ERROR.to_owned(),
        code.to_owned(),
        message,
    )))
}

/// 统计 SQL 文本中的 `$n` 占位符数(字符串字面量与注释之外取最大序号)。
fn count_placeholders(sql: &str) -> usize {
    let mut max = 0usize;
    let mut chars = sql.chars().peekable();
    let mut in_string = false;
    while let Some(c) = chars.next() {
        match c {
            '\'' => in_string = !in_string,
            '$' if !in_string => {
                let mut num = String::new();
                while chars.peek_if_ascii_digit() {
                    num.push(chars.next().expect("peeked"));
                }
                if !num.is_empty() {
                    max = max.max(num.parse::<usize>().unwrap_or(0));
                }
            }
            _ => {}
        }
    }
    max
}

trait PeekDigits {
    fn peek_if_ascii_digit(&mut self) -> bool;
}

impl<I: Iterator<Item = char>> PeekDigits for std::iter::Peekable<I> {
    fn peek_if_ascii_digit(&mut self) -> bool {
        self.peek().is_some_and(|c| c.is_ascii_digit())
    }
}

/// 文本参数值 → 标量(类型推断:整数/浮点/布尔/日期/字符串)。
/// 协议参数无声明类型时的实用近似;声明类型推断(DESCRIBE 反推)见路线图。
fn guess_scalar(v: Option<String>) -> ScalarValue {
    let Some(t) = v else {
        return ScalarValue::Null;
    };
    if let Ok(i) = t.parse::<i64>() {
        return ScalarValue::Int64(i);
    }
    if let Ok(f) = t.parse::<f64>() {
        return ScalarValue::Float64(f);
    }
    match t.as_str() {
        "true" | "false" => return ScalarValue::Boolean(t == "true"),
        _ => {}
    }
    if looks_like_date(&t) {
        if let Ok(d) = common::types::parse_date_text(&t) {
            return d;
        }
    }
    ScalarValue::Utf8(t)
}

/// YYYY-MM-DD 形状(占位符类型推断用)。
fn looks_like_date(t: &str) -> bool {
    let b = t.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b.iter().enumerate().all(|(i, c)| {
            i == 4 || i == 7 || c.is_ascii_digit()
        })
}

#[async_trait]
impl QueryParser for Handlers {
    type Statement = MedbPrepared;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Option<Type>],
    ) -> PgWireResult<Option<MedbPrepared>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        // 语法预检(参数绑定推迟到计划期)
        sql::parser::parse_statements(trimmed).map_err(|e| {
            user_error("42601".into(), format!("parse failed: {e}"))
        })?;
        Ok(Some(MedbPrepared {
            sql: trimmed.to_owned(),
        }))
    }

    fn get_parameter_types(&self, stmt: &MedbPrepared) -> PgWireResult<Vec<Type>> {
        // 参数实际类型在计划期由绑定值决定;Describe 声明 TEXT 占位
        Ok((0..count_placeholders(&stmt.sql)).map(|_| Type::TEXT).collect())
    }

    fn get_result_schema(
        &self,
        stmt: &MedbPrepared,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let schema = self.state.engine.describe(&stmt.sql, count_placeholders(&stmt.sql)).map_err(|e| {
            user_error("42601".into(), e.to_string())
        })?;
        Ok(fields_of(&schema))
    }
}

/// 列 schema → Describe 响应字段(文本格式)。
fn fields_of(schema: &[ColumnDesc]) -> Vec<FieldInfo> {
    schema
        .iter()
        .map(|c| {
            FieldInfo::new(
                c.name.clone(),
                None,
                None,
                pg_type(c.ty.clone()),
                FieldFormat::Text,
            )
        })
        .collect()
}

#[async_trait]
impl ExtendedQueryHandler for Handlers {
    type Statement = MedbPrepared;
    type QueryParser = Handlers;

    fn query_parser(&self) -> Arc<Handlers> {
        Arc::new(self.clone())
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<MedbPrepared>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let session = client
            .session_extensions()
            .get_or_insert_with(|| Mutex::new(Session::new(self.state.engine.clone())));
        let mut session = session.lock();

        let n = portal.parameter_len();
        let mut params = Vec::with_capacity(n);
        for i in 0..n {
            let text: Option<String> = portal.parameter(i, &Type::TEXT)?;
            params.push(guess_scalar(text));
        }
        let sql = portal.statement.statement.sql.clone();
        match session.execute_prepared(&sql, &params) {
            Ok(outcome) => match outcome {
                Outcome::Query { schema, rows } => {
                    Ok(Response::Query(query_response(schema, rows)?))
                }
                Outcome::Tag { prefix, rows } => {
                    let mut tag = Tag::new(&prefix);
                    if let Some(n) = rows {
                        tag = tag.with_rows(n as usize);
                    }
                    Ok(Response::Execution(tag))
                }
            },
            Err(err) => {
                warn!(error = %err, "statement failed");
                Ok(Response::Error(Box::new(error_to_info(&err))))
            }
        }
    }
}
