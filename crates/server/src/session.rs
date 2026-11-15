//! 会话:连接级状态(事务缓冲)+ RC 快照管理。
//!
//! 隔离级别:READ COMMITTED —— 每条语句开始时刷新快照(last_seq),
//! 语句内一致;事务的写入暂存缓冲,COMMIT 原子落盘(其它会话在此之前不可见);
//! 语句通过数据源 overlay 读到自己的缓冲写入(读己之写)。

use std::sync::Arc;

use common::error::{Error, Result};
use common::types::ScalarValue;
use sql::parser::parse_statements;
use sql::planner::{PlanContext, plan_statement};
use sqlparser::ast::Statement;

use crate::engine::{Engine, Outcome, StatementCtx, TxnBuffer};

/// 连接会话(由 pgwire SessionExtensions 按 连接 一份持有)。
pub struct Session {
    engine: Arc<Engine>,
    txn: Option<TxnBuffer>,
}

impl Session {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine, txn: None }
    }

    pub fn engine(&self) -> Arc<Engine> {
        self.engine.clone()
    }

    pub fn in_transaction(&self) -> bool {
        self.txn.is_some()
    }

    /// 执行一段 SQL(可含多条语句)。事务控制语句(BEGIN/COMMIT/ROLLBACK)
    /// 在此拦截;遇错即停(已执行语句不回滚 —— v1 无语句级保存点)。
    pub fn execute_script(&mut self, sql: &str) -> Result<Vec<Outcome>> {
        // 管理命令(sqlparser 不解析;截断分号后按关键字识别)
        let trimmed = sql.trim().trim_end_matches(';').trim();
        match trimmed.to_ascii_uppercase().as_str() {
            "FLUSH TABLES" | "FLUSH" => {
                let flushed = self.engine.flush()?;
                return Ok(vec![Outcome::Tag {
                    prefix: if flushed {
                        "FLUSH".into()
                    } else {
                        "FLUSH OK".into()
                    },
                    rows: None,
                }]);
            }
            "STATS" => {
                let stats = self.engine.storage_stats();
                let text = format!(
                    "memtable {} bytes, imm {} | {}",
                    stats.memtable_bytes,
                    stats.imm_count,
                    stats
                        .level_files
                        .iter()
                        .map(|(lvl, files, bytes)| format!("L{lvl}: {files} files / {bytes} bytes"))
                        .collect::<Vec<_>>()
                        .join(" | ")
                );
                return Ok(vec![Outcome::Query {
                    schema: vec![common::schema::ColumnDesc::new(
                        "stats".to_string(),
                        common::types::DataType::Utf8,
                        true,
                    )],
                    rows: vec![vec![ScalarValue::Utf8(text)]],
                }]);
            }
            _ => {}
        }

        let statements = parse_statements(sql)?;
        let mut outcomes = Vec::with_capacity(statements.len());
        for stmt in &statements {
            outcomes.push(self.execute_one(stmt, &[])?);
        }
        Ok(outcomes)
    }

    /// 执行带绑定参数的单条语句(extended query protocol 的 Bind 后调用)。
    /// 占位符 `$n` 在计划期替换为绑定值字面量(类型保真)。
    pub fn execute_prepared(&mut self, sql: &str, params: &[ScalarValue]) -> Result<Outcome> {
        let statements = parse_statements(sql)?;
        if statements.len() != 1 {
            return Err(Error::sql(
                "extended query must contain exactly one statement",
            ));
        }
        self.execute_one(&statements.into_iter().next().expect("checked above"), params)
    }

    fn execute_one(&mut self, stmt: &Statement, params: &[ScalarValue]) -> Result<Outcome> {
        match stmt {
            Statement::StartTransaction { .. } => {
                if self.txn.is_some() {
                    return Err(Error::sql(
                        "transaction already in progress (nested transactions not supported)",
                    ));
                }
                self.txn = Some(TxnBuffer::default());
                Ok(Outcome::Tag {
                    prefix: "BEGIN".into(),
                    rows: None,
                })
            }
            Statement::Commit { .. } => {
                if let Some(buf) = self.txn.take() {
                    if !buf.is_empty() {
                        self.engine.commit_txn(buf)?;
                    }
                }
                Ok(Outcome::Tag {
                    prefix: "COMMIT".into(),
                    rows: None,
                })
            }
            Statement::Rollback { .. } => {
                self.txn = None;
                Ok(Outcome::Tag {
                    prefix: "ROLLBACK".into(),
                    rows: None,
                })
            }
            Statement::AlterTable(_) => {
                // DDL 自动提交;事务内执行会产生 schema 与缓冲的新旧混杂 → 禁止
                if self.txn.is_some() {
                    return Err(Error::sql(
                        "ALTER TABLE cannot run inside a transaction block",
                    ));
                }
                let mut sctx = self.engine.begin_autocommit();
                let data = self.engine.data_source_for_ctx(&sctx);
                let catalog = self.engine.catalog();
                let ctx = PlanContext {
                    batch_size: self.engine.config().engine.batch_size,
                    catalog: &*catalog,
                    data,
                    ctes: &[],
                    params,
                };
                let plan = plan_statement(stmt, &ctx)?;
                self.engine.execute_plan(&mut sctx, plan)
            }
            _ => {
                // RC:语句开始刷新快照;事务缓冲引用借出(overlay 进数据源)
                let mut sctx: StatementCtx<'_> = self.engine.begin_statement(self.txn.as_mut());
                let data = self.engine.data_source_for_ctx(&sctx);
                let catalog = self.engine.catalog();
                let ctx = PlanContext {
                    batch_size: self.engine.config().engine.batch_size,
                    catalog: &*catalog,
                    data,
                    ctes: &[],
                    params,
                };
                let plan = plan_statement(stmt, &ctx)?;
                self.engine.execute_plan(&mut sctx, plan)
            }
        }
    }
}
