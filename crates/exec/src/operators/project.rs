//! Project:对每个输入批求值全部输出表达式,逐列向量化。

use common::error::Result;
use common::schema::ColumnDesc;

use crate::chunk::DataChunk;
use crate::executor::{BoxedExecutor, ExecuteContext, Executor};
use crate::expr::Expr;

pub struct ProjectExec {
    input: BoxedExecutor,
    schema: Vec<ColumnDesc>,
    exprs: Vec<Expr>,
}

impl ProjectExec {
    /// `schema` 为输出列描述(名字来自别名或表达式文本,类型来自静态推导)。
    pub fn new(input: BoxedExecutor, schema: Vec<ColumnDesc>, exprs: Vec<Expr>) -> Self {
        Self {
            input,
            schema,
            exprs,
        }
    }
}

impl Executor for ProjectExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        let Some(batch) = self.input.next_batch(ctx)? else {
            return Ok(None);
        };
        let columns = self
            .exprs
            .iter()
            .map(|e| e.eval(&batch))
            .collect::<Result<Vec<_>>>()?;
        let out = DataChunk::with_memory(columns, &ctx.mem.tracker)?;
        Ok(Some(out))
    }

    fn schema(&self) -> &[ColumnDesc] {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecuteContext;
    use crate::operators::values::ValuesExec;
    use crate::vector::Vector;
    use common::types::{DataType, ScalarValue};
    use memory::{MemoryContext, MemoryTracker};

    #[test]
    fn project_computes_expressions() {
        let input_schema = vec![ColumnDesc::new("a", DataType::Int64, true)];
        let values = Box::new(ValuesExec::new(
            input_schema,
            vec![vec![ScalarValue::Int64(5)]],
        ));
        let out_schema = vec![ColumnDesc::new("a_plus", DataType::Int64, true)];
        let mut project = ProjectExec::new(
            values,
            out_schema,
            vec![Expr::binary(
                crate::expr::BinaryOp::Add,
                Expr::Column(0),
                Expr::Literal(ScalarValue::Int64(1)),
            )],
        );
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(64, MemoryContext::new(&root, "q", 4096));
        let mut results = Vec::new();
        while let Some(chunk) = project.next_batch(&mut ctx).unwrap() {
            for i in 0..chunk.num_rows() {
                results.push(chunk.row(i)[0].clone());
            }
        }
        assert_eq!(results, vec![ScalarValue::Int64(6)]);
        assert_eq!(root.used(), 0);
    }

    #[test]
    fn project_all_null_input() {
        let input_schema = vec![ColumnDesc::new("a", DataType::Int64, true)];
        let values = Box::new(ValuesExec::one_row(input_schema));
        let out_schema = vec![ColumnDesc::new("c", DataType::Int64, true)];
        let mut project = ProjectExec::new(
            values,
            out_schema,
            vec![Expr::binary(
                crate::expr::BinaryOp::Add,
                Expr::Column(0),
                Expr::Literal(ScalarValue::Int64(1)),
            )],
        );
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(64, MemoryContext::new(&root, "q", 4096));
        let chunk = project.next_batch(&mut ctx).unwrap().unwrap();
        assert!(chunk.column(0).null_at(0));
    }

    #[test]
    fn empty_pipeline_yields_nothing() {
        let input_schema = vec![ColumnDesc::new("a", DataType::Int64, true)];
        let values = Box::new(ValuesExec::new(input_schema, Vec::new()));
        let out_schema = vec![ColumnDesc::new("a", DataType::Int64, true)];
        let mut project = ProjectExec::new(values, out_schema, vec![Expr::Column(0)]);
        let root = MemoryTracker::new_root("t", None);
        let mut ctx = ExecuteContext::new(64, MemoryContext::new(&root, "q", 4096));
        assert!(project.next_batch(&mut ctx).unwrap().is_none());
        let _ = Vector::int64s(vec![]); // keep import used
    }
}
