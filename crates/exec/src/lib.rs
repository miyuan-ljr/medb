//! 向量化执行引擎。
//!
//! 模块划分:
//! - [`vector`] / [`chunk`]:列向量与批的物理表示(null 位图、类型化快速通道);
//! - [`expr`]:标量表达式与向量化求值内核;
//! - [`agg`]:聚合函数描述与类型推导;
//! - [`executor`]:批处理拉取框架 + 查询级内存上下文;
//! - [`operators`]:物理算子(VALUES / Filter / Project / Limit / SeqScan /
//!   HashAgg / Sort)。

pub mod agg;
pub mod chunk;
pub mod executor;
pub mod expr;
pub mod expr_compile;
pub mod functions;
pub mod operators;
pub mod parallel;
pub mod vector;

pub use chunk::DataChunk;
pub use executor::{BoxedExecutor, ExecuteContext, Executor};
pub use expr::{BinaryOp, Expr};
pub use vector::{ColumnData, Datum, Validity, Vector};
