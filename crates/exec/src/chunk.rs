//! DataChunk:向量化引擎中行 batch 的物理表示 = 列向量数组 + 行数不变式。
//!
//! 每个批在构造时按 `approx_size` 在查询配额树上记账(`Reservation`),
//! chunk drop 时自动归还 —— 内存统计随对象生命周期,无泄漏。

use std::sync::Arc;

use common::error::{Error, Result};
use common::schema::{ColumnDesc, Row};
use common::types::{DataType, ScalarValue};
use memory::{MemoryTracker, Reservation};

use crate::vector::Vector;

#[derive(Debug)]
pub struct DataChunk {
    columns: Vec<Vector>,
    num_rows: usize,
    _mem: Option<Reservation>,
}

impl DataChunk {
    /// 构造(不记账,用于测试/零成本场景)。
    pub fn new(columns: Vec<Vector>) -> Result<Self> {
        if columns.is_empty() {
            return Err(Error::invalid_argument("chunk requires >=1 column"));
        }
        let num_rows = columns[0].len();
        if let Some(bad) = columns.iter().position(|c| c.len() != num_rows) {
            return Err(Error::invalid_argument(format!(
                "column {bad} length {} != chunk length {num_rows}",
                columns[bad].len()
            )));
        }
        Ok(Self {
            columns,
            num_rows,
            _mem: None,
        })
    }

    /// 构造并在 query tracker 上按近似大小记账。
    pub fn with_memory(columns: Vec<Vector>, tracker: &Arc<MemoryTracker>) -> Result<Self> {
        let chunk = Self::new(columns)?;
        let bytes = chunk.approx_size();
        let _mem = tracker.try_reserve(bytes)?;
        Ok(Self {
            _mem: Some(_mem),
            ..chunk
        })
    }

    pub fn columns(&self) -> &[Vector] {
        &self.columns
    }

    /// 取出全部列(消耗 chunk;列存路径组装用)。
    pub fn into_columns(self) -> Vec<Vector> {
        self.columns
    }

    pub fn column(&self, i: usize) -> &Vector {
        &self.columns[i]
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn schema(&self) -> Vec<ColumnDesc> {
        self.columns
            .iter()
            .map(|c| ColumnDesc::new(c.data_type().name(), c.data_type(), true))
            .collect()
    }

    /// 行视图(调试与行式逃生通道)。
    pub fn row(&self, i: usize) -> Row {
        self.columns.iter().map(|c| c.scalar_at(i)).collect()
    }

    /// 由 schema + 行数据构建(VALUES 算子入口)。
    pub fn from_rows(dtypes: &[DataType], rows: &[Row]) -> Result<Self> {
        let columns = dtypes
            .iter()
            .enumerate()
            .map(|(i, ty)| Vector::from_scalars(ty.clone(), rows.iter().map(|r| r[i].clone())))
            .collect::<Result<Vec<_>>>()?;
        Self::new(columns)
    }

    pub fn with_rows(
        dtypes: &[DataType],
        rows: &[Row],
        tracker: &Arc<MemoryTracker>,
    ) -> Result<Self> {
        let chunk = Self::from_rows(dtypes, rows)?;
        let _mem = tracker.try_reserve(chunk.approx_size())?;
        Ok(Self {
            _mem: Some(_mem),
            ..chunk
        })
    }

    /// 按选择位图压缩(与 filter 内核配套),在 tracker 上记账结果批。
    pub fn select(&self, selection: &[bool], tracker: &Arc<MemoryTracker>) -> Result<Self> {
        let columns = self
            .columns
            .iter()
            .map(|c| c.select(selection))
            .collect::<Result<Vec<_>>>()?;
        let chunk = Self::new(columns)?;
        let _mem = tracker.try_reserve(chunk.approx_size())?;
        Ok(Self {
            _mem: Some(_mem),
            ..chunk
        })
    }

    /// 截取前 n 行。
    pub fn head(&self, n: usize, tracker: &Arc<MemoryTracker>) -> Result<Self> {
        let columns = self.columns.iter().map(|c| c.head(n)).collect();
        let chunk = Self::new(columns)?;
        let _mem = tracker.try_reserve(chunk.approx_size())?;
        Ok(Self {
            _mem: Some(_mem),
            ..chunk
        })
    }

    pub fn approx_size(&self) -> usize {
        self.columns.iter().map(|c| c.approx_size()).sum::<usize>()
            + std::mem::size_of::<Vector>() * self.columns.len()
    }
}

/// 单行近似字节数(行级物化记账用;标量列按 ScalarValue 大小,文本/二进制按实际长度)。
pub fn row_approx_bytes(dtypes: &[DataType], row: &[ScalarValue]) -> usize {
    let mut n = std::mem::size_of::<ScalarValue>() * row.len();
    for (ty, v) in dtypes.iter().zip(row) {
        match (ty, v) {
            (DataType::Utf8, ScalarValue::Utf8(s)) => n += s.len(),
            (DataType::Binary, ScalarValue::Binary(b)) => n += b.len(),
            (DataType::Array(_), ScalarValue::List(items)) => {
                n += std::mem::size_of::<ScalarValue>() * items.len();
            }
            (DataType::Vector(dim), ScalarValue::Vector(_vals)) => {
                n += 4 * dim; // dim 个 f32
            }
            _ => {}
        }
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::types::ScalarValue;
    use memory::MemoryTracker;

    fn tracker() -> Arc<MemoryTracker> {
        MemoryTracker::new_root("test", None)
    }

    #[test]
    fn build_and_read_rows() {
        let chunk = DataChunk::new(vec![
            Vector::int64s(vec![1, 2]),
            Vector::utf8s(vec!["a".into(), "b".into()]),
        ])
        .unwrap();
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(
            chunk.row(1),
            vec![ScalarValue::Int64(2), ScalarValue::Utf8("b".into())]
        );
        assert_eq!(chunk.schema()[1].ty, DataType::Utf8);
    }

    #[test]
    fn ragged_columns_rejected() {
        assert!(DataChunk::new(vec![Vector::int64s(vec![1]), Vector::int64s(vec![1, 2])]).is_err());
        assert!(DataChunk::new(vec![]).is_err());
    }

    #[test]
    fn memory_reservation_lifecycle() {
        let root = tracker();
        let rows: Vec<Row> = (0..100).map(|_| vec![ScalarValue::Int64(7)]).collect();
        let chunk = DataChunk::with_rows(&[DataType::Int64], &rows, &root).unwrap();
        assert!(root.used() > 0);
        let bytes = root.used();
        drop(chunk);
        assert_eq!(root.used(), 0);
        assert!(bytes >= 100 * 8);
    }

    #[test]
    fn select_and_head_keep_invariants() {
        let root = tracker();
        let chunk = DataChunk::with_rows(
            &[DataType::Int64, DataType::Utf8],
            &[
                vec![ScalarValue::Int64(1), ScalarValue::Utf8("x".into())],
                vec![ScalarValue::Null, ScalarValue::Utf8("yy".into())],
                vec![ScalarValue::Int64(3), ScalarValue::Utf8("z".into())],
            ],
            &root,
        )
        .unwrap();
        let sel = chunk.select(&[true, true, false], &root).unwrap();
        assert_eq!(sel.num_rows(), 2);
        assert!(sel.column(0).null_at(1));
        let head = sel.head(1, &root).unwrap();
        assert_eq!(head.num_rows(), 1);
    }
}
