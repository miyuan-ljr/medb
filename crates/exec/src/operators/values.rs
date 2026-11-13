//! VALUES 数据源:把计划期物化的行按 batch_size 切批输出。
//! `SELECT 1` 这类无 FROM 查询、以及未来的常量折叠都从这里出发。

use common::error::Result;
use common::schema::{ColumnDesc, Row};
use common::types::{DataType, ScalarValue};

use crate::chunk::DataChunk;
use crate::executor::{ExecuteContext, Executor};

pub struct ValuesExec {
    schema: Vec<ColumnDesc>,
    rows: Vec<Row>,
    next: usize,
}

impl ValuesExec {
    pub fn new(schema: Vec<ColumnDesc>, rows: Vec<Row>) -> Self {
        Self {
            schema,
            rows,
            next: 0,
        }
    }

    /// 与 schema 同构的单行 null 占位(无 FROM 的 SELECT 用)。
    pub fn one_row(schema: Vec<ColumnDesc>) -> Self {
        let row: Row = schema.iter().map(|_| ScalarValue::Null).collect();
        Self::new(schema, vec![row])
    }
}

impl Executor for ValuesExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        if self.next >= self.rows.len() {
            return Ok(None);
        }
        let end = (self.next + ctx.batch_size).min(self.rows.len());
        let slice = &self.rows[self.next..end];
        let dtypes: Vec<DataType> = self.schema.iter().map(|c| c.ty.clone()).collect();
        let chunk = DataChunk::with_rows(&dtypes, slice, &ctx.mem.tracker)?;
        self.next = end;
        Ok(Some(chunk))
    }

    fn schema(&self) -> &[ColumnDesc] {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use memory::{MemoryContext, MemoryTracker};

    #[test]
    fn batches_by_ctx_size() {
        let schema = vec![ColumnDesc::new("v", DataType::Int64, true)];
        let rows: Vec<Row> = (0..5).map(|i| vec![ScalarValue::Int64(i)]).collect();
        let mut exec = ValuesExec::new(schema, rows);
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(2, MemoryContext::new(&root, "q", 4096));
        let mut out = Vec::new();
        while let Some(chunk) = exec.next_batch(&mut ctx).unwrap() {
            out.push(chunk.num_rows());
        }
        assert_eq!(out, vec![2, 2, 1]);
        assert_eq!(root.used(), 0, "结束后配额应全部归还");
    }
}
