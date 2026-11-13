//! 查询处理器:PG simple query 协议 → Engine → pgwire Response。
//!
//! 支持一条 Query 消息里带多条语句(分号分隔),逐条产出响应;
//! 任一语句失败即返回 ErrorResponse,连接保持可用。
//! extended query protocol(预编译语句)未实现,回退 pgwire 默认错误。

use std::sync::Arc;

use async_trait::async_trait;
use common::error::ErrorCode;
use common::schema::{ColumnDesc, Row};
use common::types::DataType;
use futures::Sink;
use futures::stream;
use parking_lot::Mutex;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
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
