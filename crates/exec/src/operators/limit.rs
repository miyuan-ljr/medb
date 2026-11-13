//! Limit/Offset:跳过 offset 行后,输出至多 limit 行(None = 不限制)。

use common::error::Result;
use common::schema::ColumnDesc;

use crate::chunk::DataChunk;
use crate::executor::{BoxedExecutor, ExecuteContext, Executor};

pub struct LimitExec {
    input: BoxedExecutor,
    schema: Vec<ColumnDesc>,
    to_skip: usize,
    remaining: Option<u64>,
}

impl LimitExec {
    pub fn new(input: BoxedExecutor, offset: usize, limit: Option<u64>) -> Self {
        let schema = input.schema().to_vec();
        Self {
            input,
            schema,
            to_skip: offset,
            remaining: limit,
        }
    }
}

impl Executor for LimitExec {
    fn next_batch(&mut self, ctx: &mut ExecuteContext) -> Result<Option<DataChunk>> {
        if self.remaining == Some(0) {
            return Ok(None);
        }
        loop {
            let Some(mut batch) = self.input.next_batch(ctx)? else {
                return Ok(None);
            };
            // 先处理 offset
            if self.to_skip > 0 {
                let skip = self.to_skip.min(batch.num_rows());
                self.to_skip -= skip;
                let keep: Vec<bool> = (0..batch.num_rows()).map(|i| i >= skip).collect();
                if keep.iter().all(|s| !*s) {
                    continue;
                }
                batch = batch.select(&keep, &ctx.mem.tracker)?;
            }
            if batch.num_rows() == 0 {
                continue;
            }
            // 再处理 limit
            let out = match self.remaining {
                None => batch,
                Some(remaining) => {
                    let take = (remaining as usize).min(batch.num_rows());
                    self.remaining = Some(remaining - take as u64);
                    batch.head(take, &ctx.mem.tracker)?
                }
            };
            if out.num_rows() == 0 {
                continue;
            }
            return Ok(Some(out));
        }
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
    use common::types::{DataType, ScalarValue};
    use memory::{MemoryContext, MemoryTracker};

    fn values_exec() -> BoxedExecutor {
        let schema = vec![ColumnDesc::new("v", DataType::Int64, true)];
        Box::new(ValuesExec::new(
            schema,
            (0..10).map(|i| vec![ScalarValue::Int64(i)]).collect(),
        ))
    }

    fn drain(exec: &mut dyn Executor, root: &std::sync::Arc<MemoryTracker>) -> Vec<ScalarValue> {
        let mut ctx = ExecuteContext::new(3, MemoryContext::new(root, "q", 4096));
        let mut out = Vec::new();
        while let Some(chunk) = exec.next_batch(&mut ctx).unwrap() {
            for i in 0..chunk.num_rows() {
                out.push(chunk.row(i)[0].clone());
            }
        }
        out
    }

    #[test]
    fn limit_and_offset() {
        let root = MemoryTracker::new_root("t", None);
        let mut exec = LimitExec::new(values_exec(), 2, Some(3));
        let got = drain(&mut exec, &root);
        assert_eq!(
            got,
            vec![
                ScalarValue::Int64(2),
                ScalarValue::Int64(3),
                ScalarValue::Int64(4)
            ]
        );
        assert_eq!(root.used(), 0);
    }

    #[test]
    fn offset_only() {
        let root = MemoryTracker::new_root("t", None);
        let mut exec = LimitExec::new(values_exec(), 8, None);
        let got = drain(&mut exec, &root);
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn offset_beyond_end_is_empty() {
        let root = MemoryTracker::new_root("t", None);
        let mut exec = LimitExec::new(values_exec(), 100, None);
        assert!(drain(&mut exec, &root).is_empty());
    }

    #[test]
    fn zero_limit_short_circuits() {
        let root = MemoryTracker::new_root("t", None);
        let mut exec = LimitExec::new(values_exec(), 0, Some(0));
        assert!(drain(&mut exec, &root).is_empty());
    }
}
