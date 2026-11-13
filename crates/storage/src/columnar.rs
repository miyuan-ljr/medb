//! HTAP 列存副本(v1 钩子)。
//!
//! 定位:行存(LSM)是 TP 主库;本模块按表维护一份"键 → 行"的列视图,
//! TP 写入经 CDC 应用到这里,AP 扫描按列读取。
//!
//! v1 是全量行的内存映射,足以打通链路;生产化方向(路线图 M4):
//! 列式 segment 文件 + 增量 delta 行 + 后台 merge,以及字典/RLE 编码。
//!
//! 扫描顺序:按键排序(BTreeMap),保证同一表两次 scan_column 列内对齐。

use std::collections::BTreeMap;

use common::error::{Error, Result};
use common::ids::TableId;
use common::schema::{Row, TableSchema};
use common::types::DataType;
use parking_lot::Mutex;

/// 列的简单向量表示(与 exec::Vector 解耦,避免 storage 反向依赖执行引擎;
/// AP 路径在 exec 侧转换)。
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnVector {
    Bool(Vec<bool>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Utf8(Vec<String>),
    /// 整列尚未写入(空表/未应用)。
    Empty,
}

impl ColumnVector {
    pub fn len(&self) -> usize {
        match self {
            ColumnVector::Bool(v) => v.len(),
            ColumnVector::Int64(v) => v.len(),
            ColumnVector::Float64(v) => v.len(),
            ColumnVector::Utf8(v) => v.len(),
            ColumnVector::Empty => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn push_scalar(&mut self, v: &common::types::ScalarValue) -> Result<()> {
        use common::types::ScalarValue as SV;
        match (self, v) {
            (ColumnVector::Bool(vec), SV::Boolean(b)) => vec.push(*b),
            (ColumnVector::Int64(vec), SV::Int64(i)) => vec.push(*i),
            (ColumnVector::Float64(vec), SV::Float64(f)) => vec.push(*f),
            (ColumnVector::Utf8(vec), SV::Utf8(s)) => vec.push(s.clone()),
            (col, SV::Null) => {
                let _ = col;
                // v1 不支持可空列副本(null 由行内默认值替代),TODO 支持位图
            }
            (col, v) => {
                return Err(Error::invalid_argument(format!(
                    "columnar type mismatch: {:?} vs {:?}",
                    std::mem::discriminant(col),
                    v
                )));
            }
        }
        Ok(())
    }
}

pub trait ColumnarReplica: Send + Sync {
    /// CDC upsert:整行写入/覆盖。
    fn upsert(&self, table: &TableSchema, key: &[u8], row: Row) -> Result<()>;

    /// CDC delete。
    fn delete(&self, table: TableId, key: &[u8]) -> Result<()>;

    /// 读取某列(键序对齐)。列号越界返回错误。
    fn scan_column(&self, table: TableId, column: usize) -> Result<ColumnVector>;

    /// 整表移除(DROP TABLE 时调用)。
    fn drop_table(&self, table: TableId) -> Result<()>;

    fn row_count(&self, table: TableId) -> usize;
}

#[derive(Default)]
struct ColumnTable {
    rows: BTreeMap<Vec<u8>, Row>,
}

pub struct InMemoryColumnarReplica {
    tables: Mutex<std::collections::HashMap<u64, ColumnTable>>,
}

impl InMemoryColumnarReplica {
    pub fn new() -> Self {
        Self {
            tables: Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl Default for InMemoryColumnarReplica {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarReplica for InMemoryColumnarReplica {
    fn upsert(&self, table: &TableSchema, key: &[u8], row: Row) -> Result<()> {
        if row.len() != table.columns.len() {
            return Err(Error::invalid_argument(format!(
                "row arity {} != table '{}' column count {}",
                row.len(),
                table.name,
                table.columns.len()
            )));
        }
        self.tables
            .lock()
            .entry(table.id)
            .or_default()
            .rows
            .insert(key.to_vec(), row);
        Ok(())
    }

    fn delete(&self, table: TableId, key: &[u8]) -> Result<()> {
        if let Some(t) = self.tables.lock().get_mut(&table.0) {
            t.rows.remove(key);
        }
        Ok(())
    }

    fn scan_column(&self, table: TableId, column: usize) -> Result<ColumnVector> {
        let tables = self.tables.lock();
        let Some(t) = tables.get(&table.0) else {
            return Ok(ColumnVector::Empty);
        };
        if t.rows.is_empty() {
            return Ok(ColumnVector::Empty);
        }
        // 列号越界检查 + 类型推断(以首行为准;v1 要求列内类型一致,可空列见路线图)
        let first = t.rows.values().next().unwrap();
        let Some(v) = first.get(column) else {
            return Err(Error::invalid_argument(format!(
                "column {column} out of range"
            )));
        };
        let ty = v.data_type().ok_or_else(|| {
            Error::invalid_argument(format!("cannot infer type of column {column}"))
        })?;
        let mut out = match ty {
            DataType::Boolean => ColumnVector::Bool(Vec::new()),
            DataType::Int64 => ColumnVector::Int64(Vec::new()),
            DataType::Float64 => ColumnVector::Float64(Vec::new()),
            DataType::Utf8 => ColumnVector::Utf8(Vec::new()),
            // 列存副本 v1 仅支持标量列;LOB/数组/向量列的 AP 扫描走行存路径,
            // 此处返回空(与未写入语义一致)
            DataType::Binary | DataType::Array(_) | DataType::Vector(_) => {
                return Ok(ColumnVector::Empty);
            }
        };
        for row in t.rows.values() {
            let v = row
                .get(column)
                .ok_or_else(|| Error::invalid_argument(format!("column {column} out of range")))?;
            out.push_scalar(v)?;
        }
        Ok(out)
    }

    fn drop_table(&self, table: TableId) -> Result<()> {
        self.tables.lock().remove(&table.0);
        Ok(())
    }

    fn row_count(&self, table: TableId) -> usize {
        self.tables
            .lock()
            .get(&table.0)
            .map(|t| t.rows.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::types::ScalarValue;

    fn schema() -> TableSchema {
        TableSchema {
            id: 1,
            name: "t".into(),
            columns: vec![
                common::schema::ColumnDesc::new("id", DataType::Int64, false),
                common::schema::ColumnDesc::new("name", DataType::Utf8, true),
            ],
            primary_key: Vec::new(),
            version: 1,
        }
    }

    #[test]
    fn upsert_delete_scan() {
        let replica = InMemoryColumnarReplica::new();
        let s = schema();
        replica
            .upsert(
                &s,
                b"k1",
                vec![ScalarValue::Int64(1), ScalarValue::Utf8("a".into())],
            )
            .unwrap();
        replica
            .upsert(
                &s,
                b"k2",
                vec![ScalarValue::Int64(2), ScalarValue::Utf8("b".into())],
            )
            .unwrap();
        // 覆盖写
        replica
            .upsert(
                &s,
                b"k1",
                vec![ScalarValue::Int64(10), ScalarValue::Utf8("z".into())],
            )
            .unwrap();
        assert_eq!(replica.row_count(TableId(1)), 2);

        let ids = replica.scan_column(TableId(1), 0).unwrap();
        assert_eq!(ids, ColumnVector::Int64(vec![10, 2])); // 键序 k1 < k2
        let names = replica.scan_column(TableId(1), 1).unwrap();
        assert_eq!(names, ColumnVector::Utf8(vec!["z".into(), "b".into()]));

        replica.delete(TableId(1), b"k1").unwrap();
        assert_eq!(replica.row_count(TableId(1)), 1);
        assert_eq!(
            replica.scan_column(TableId(1), 0).unwrap(),
            ColumnVector::Int64(vec![2])
        );
        assert_eq!(
            replica.scan_column(TableId(99), 0).unwrap(),
            ColumnVector::Empty
        );
    }

    #[test]
    fn arity_mismatch_rejected() {
        let replica = InMemoryColumnarReplica::new();
        let s = schema();
        assert!(
            replica
                .upsert(&s, b"k", vec![ScalarValue::Int64(1)])
                .is_err()
        );
    }
}
