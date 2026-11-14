//! Filter:谓词向量化求值 → 选择位图 → 批压缩。整批被过滤掉时继续拉取下一批。

use common::error::Result;
use common::schema::ColumnDesc;
use common::types::DataType;

use crate::chunk::DataChunk;
use crate::expr_compile::CompiledExpr;
use crate::executor::{BoxedExecutor, ExecuteContext, Executor};
use crate::expr::Expr;
use crate::vector::Vector;

pub struct FilterExec {
    input: BoxedExecutor,
    schema: Vec<ColumnDesc>,
    /// JIT 编译后的谓词程序(常量折叠/代数化简/CSE 已在编译期应用)
    predicate: CompiledExpr,
}

impl FilterExec {
    pub fn new(input: BoxedExecutor, predicate: Expr) -> Self {
        let schema = input.schema().to_vec();
        Self {
            input,
            schema,
            predicate: crate::expr_compile::compile(&predicate),
        }
    }
}

impl Executor for FilterExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        loop {
            let Some(batch) = self.input.next_batch(ctx)? else {
                return Ok(None);
            };
            let mask = self.predicate.eval(&batch)?;
            if mask.data_type() != DataType::Boolean {
                return Err(common::error::Error::sql(format!(
                    "filter predicate must return boolean, got {:?}",
                    mask.data_type()
                )));
            }
            // null 按 false(不通过)处理
            let selection: Vec<bool> = (0..mask.len())
                .map(|i| mask.bool_values()[i] && !mask.null_at(i))
                .collect();
            if selection.iter().all(|s| !*s) {
                continue; // 全过滤,继续拉下一批
            }
            let out = batch.select(&selection, &ctx.mem.tracker)?;
            return Ok(Some(out));
        }
    }

    fn schema(&self) -> &[ColumnDesc] {
        &self.schema
    }
}

/// 谓词向量化求值的独立入口(便于单测与其他算子复用)。
pub fn eval_predicate(predicate: &Expr, batch: &DataChunk) -> Result<Vector> {
    predicate.eval(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::ExecuteContext;
    use crate::operators::values::ValuesExec;
    use common::types::ScalarValue;
    use memory::{MemoryContext, MemoryTracker};
    use std::sync::Arc;

    fn setup() -> (BoxedExecutor, Arc<MemoryTracker>) {
        let schema = vec![ColumnDesc::new("v", DataType::Int64, true)];
        let exec = Box::new(ValuesExec::new(
            schema,
            (0..10).map(|i| vec![ScalarValue::Int64(i)]).collect(),
        ));
        (exec, MemoryTracker::new_root("t", None))
    }

    #[test]
    fn filter_keeps_matching_rows() {
        let (input, root) = setup();
        let mut filter = FilterExec::new(
            input,
            Expr::binary(
                crate::expr::BinaryOp::Lt,
                Expr::Column(0),
                Expr::Literal(ScalarValue::Int64(3)),
            ),
        );
        let mut ctx = ExecuteContext::new(4, MemoryContext::new(&root, "q", 4096));
        let mut collected = Vec::new();
        while let Some(chunk) = filter.next_batch(&mut ctx).unwrap() {
            for i in 0..chunk.num_rows() {
                collected.push(chunk.row(i)[0].clone());
            }
        }
        assert_eq!(
            collected,
            vec![
                ScalarValue::Int64(0),
                ScalarValue::Int64(1),
                ScalarValue::Int64(2)
            ]
        );
        assert_eq!(root.used(), 0);
    }
}
