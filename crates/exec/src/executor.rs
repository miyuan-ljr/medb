//! 执行器框架:批处理拉取模型(Volcano 与向量化折中 —— next_batch 而非 next_row)。
//!
//! 约定:
//! - `Ok(None)` 表示流水线结束;
//! - 每批通过 `ctx.mem` 在查询配额树上记账,批对象 drop 时自动归还;
//! - 算子输出 schema 在计划期静态确定(`schema()`),供 server 生成 RowDescription。

use common::error::Result;
use common::schema::ColumnDesc;

use crate::chunk::DataChunk;
use memory::MemoryContext;

pub struct ExecuteContext {
    pub batch_size: usize,
    pub mem: MemoryContext,
    /// 并行度(扫描解码 / JOIN 探测组装的线程数);默认 1 = 串行。
    pub threads: usize,
}

impl ExecuteContext {
    pub fn new(batch_size: usize, mem: MemoryContext) -> Self {
        Self {
            batch_size,
            mem,
            threads: 1,
        }
    }

    /// 指定并行度构造。
    pub fn with_threads(batch_size: usize, mem: MemoryContext, threads: usize) -> Self {
        Self {
            batch_size,
            mem,
            threads: threads.max(1),
        }
    }
}

pub trait Executor: Send {
    /// 产出下一批;流水线结束返回 Ok(None)。
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>>;

    /// 输出列描述(计划期静态确定)。
    fn schema(&self) -> &[ColumnDesc];
}

pub type BoxedExecutor = Box<dyn Executor>;
