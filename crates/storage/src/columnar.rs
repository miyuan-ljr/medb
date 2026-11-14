//! HTAP 列存副本(v1)。
//!
//! 定位:行存(LSM)是 TP 主库;本模块按表维护一份"键 → 行"的列视图,
//! TP 写入经 CDC 应用到这里,AP 扫描按列读取。
//!
//! - **NULL 语义**:每列带 null 位图(Some(mask) / None=全非空),null 不再
//!   被静默替换为默认值 —— 列存路径与行存路径行为一致;
//! - **类型支持**:Boolean/Int64/Float64/Utf8/Date;Binary/Array/Vector 列
//!   暂不入列存(scan_column 返回 Empty,AP 扫描回退行存路径);
//! - 扫描顺序:按键排序(BTreeMap),同一表两次 scan_column 列内对齐。
//!
//! 生产化方向(路线图 M4):列式 segment 文件 + 增量 delta 行 + 后台 merge,
//! 以及字典/RLE 编码。

use std::collections::BTreeMap;

use common::error::{Error, Result};
use common::ids::TableId;
use common::schema::{Row, TableSchema};
use common::types::DataType;
use parking_lot::Mutex;

/// 列的向量表示(值 + 可选 null 位图;与 exec::Vector 解耦,避免 storage
/// 反向依赖执行引擎,AP 路径在 exec 侧转换)。
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnVector {
    pub values: ColumnValues,
    /// Some(mask):位图(true = 该行为 null);None = 全部非空。
    /// mask 长度恒等于 values 长度。
    pub nulls: Option<Vec<bool>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValues {
    Bool(Vec<bool>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Utf8(Vec<String>),
    /// 日期列(距 1970-01-01 天数)。
    Date(Vec<i32>),
    /// 整列尚未写入(空表/不支持的类型)。
    Empty,
}

impl ColumnVector {
    pub fn values_len(&self) -> usize {
        match &self.values {
            ColumnValues::Bool(v) => v.len(),
            ColumnValues::Int64(v) => v.len(),
            ColumnValues::Float64(v) => v.len(),
            ColumnValues::Utf8(v) => v.len(),
            ColumnValues::Date(v) => v.len(),
            ColumnValues::Empty => 0,
        }
    }

    pub fn len(&self) -> usize {
        self.values_len()
    }

    pub fn is_empty(&self) -> bool {
        self.values_len() == 0
    }

    /// 追加一个值;null 只置位图并压入类型默认值(保持值/位图对齐)。
    fn push_scalar(&mut self, v: &common::types::ScalarValue) -> Result<()> {
        use common::types::ScalarValue as SV;
        if v.is_null() {
            let len = self.len();
            let nulls = self.nulls.get_or_insert_with(|| vec![false; len]);
            nulls.push(true);
            match &mut self.values {
                ColumnValues::Bool(vec) => vec.push(false),
                ColumnValues::Int64(vec) => vec.push(0),
                ColumnValues::Float64(vec) => vec.push(0.0),
                ColumnValues::Utf8(vec) => vec.push(String::new()),
                ColumnValues::Date(vec) => vec.push(0),
                ColumnValues::Empty => {}
            }
            return Ok(());
        }
        // 非空值:若此前已有 null,位图补一个 false
        if let Some(nulls) = &mut self.nulls {
            nulls.push(false);
        }
        match (&mut self.values, v) {
            (ColumnValues::Bool(vec), SV::Boolean(b)) => vec.push(*b),
            (ColumnValues::Int64(vec), SV::Int64(i)) => vec.push(*i),
            (ColumnValues::Float64(vec), SV::Float64(f)) => vec.push(*f),
            (ColumnValues::Utf8(vec), SV::Utf8(s)) => vec.push(s.clone()),
            (ColumnValues::Date(vec), SV::Date(d)) => vec.push(*d),
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

    /// 按声明类型读取某列(键序对齐;null 由位图表达)。列号越界返回错误。
    /// Binary/Array/Vector 列返回 Empty(AP 扫描回退行存路径)。
    fn scan_column(&self, table: TableId, column: usize, ty: &DataType) -> Result<ColumnVector>;

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

    fn scan_column(&self, table: TableId, column: usize, ty: &DataType) -> Result<ColumnVector> {
        let tables = self.tables.lock();
        let Some(t) = tables.get(&table.0) else {
            return Ok(ColumnVector {
                values: ColumnValues::Empty,
                nulls: None,
            });
        };
        if t.rows.is_empty() {
            return Ok(ColumnVector {
                values: ColumnValues::Empty,
                nulls: None,
            });
        }
        let mut out = match ty {
            DataType::Boolean => ColumnVector {
                values: ColumnValues::Bool(Vec::new()),
                nulls: None,
            },
            DataType::Int64 => ColumnVector {
                values: ColumnValues::Int64(Vec::new()),
                nulls: None,
            },
            DataType::Float64 => ColumnVector {
                values: ColumnValues::Float64(Vec::new()),
                nulls: None,
            },
            DataType::Utf8 => ColumnVector {
                values: ColumnValues::Utf8(Vec::new()),
                nulls: None,
            },
            DataType::Date => ColumnVector {
                values: ColumnValues::Date(Vec::new()),
                nulls: None,
            },
            // LOB/数组/向量列暂不入列存:返回 Empty(AP 扫描回退行存路径)
            DataType::Binary | DataType::Array(_) | DataType::Vector(_) => {
                return Ok(ColumnVector {
                    values: ColumnValues::Empty,
                    nulls: None,
                });
            }
        };
        for row in t.rows.values() {
            let v = row
                .get(column)
                .ok_or_else(|| Error::invalid_argument(format!("column {column} out of range")))?;
            out.push_scalar(v)?;
        }
        // 位图全 false → 丢弃(None 表示全非空)
        if out.nulls.as_ref().is_some_and(|m| !m.iter().any(|b| *b)) {
            out.nulls = None;
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
    use common::schema::ColumnDesc;
    use common::types::ScalarValue;

    fn schema() -> TableSchema {
        TableSchema {
            id: 1,
            name: "t".into(),
            columns: vec![
                ColumnDesc::new("id", DataType::Int64, false),
                ColumnDesc::new("name", DataType::Utf8, true),
                ColumnDesc::new("d", DataType::Date, true),
            ],
            primary_key: Vec::new(),
            version: 1,
        }
    }

    #[test]
    fn upsert_delete_scan_with_nulls() {
        let replica = InMemoryColumnarReplica::new();
        let s = schema();
        replica
            .upsert(
                &s,
                b"k1",
                vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Utf8("a".into()),
                    ScalarValue::Date(100),
                ],
            )
            .unwrap();
        replica
            .upsert(
                &s,
                b"k2",
                vec![
                    ScalarValue::Int64(2),
                    ScalarValue::Null,
                    ScalarValue::Date(200),
                ],
            )
            .unwrap();
        // 覆盖写
        replica
            .upsert(
                &s,
                b"k1",
                vec![
                    ScalarValue::Int64(10),
                    ScalarValue::Utf8("z".into()),
                    ScalarValue::Null,
                ],
            )
            .unwrap();
        assert_eq!(replica.row_count(TableId(1)), 2);

        let ids = replica
            .scan_column(TableId(1), 0, &DataType::Int64)
            .unwrap();
        assert_eq!(ids.values, ColumnValues::Int64(vec![10, 2])); // 键序 k1 < k2
        assert_eq!(ids.nulls, None);

        let names = replica.scan_column(TableId(1), 1, &DataType::Utf8).unwrap();
        assert_eq!(
            names.values,
            ColumnValues::Utf8(vec!["z".into(), String::new()])
        );
        let nulls = names.nulls.expect("name 列应含 null 位图");
        assert_eq!(nulls, vec![false, true]);

        let dates = replica.scan_column(TableId(1), 2, &DataType::Date).unwrap();
        assert_eq!(dates.values, ColumnValues::Date(vec![0, 200]));
        assert_eq!(
            dates.nulls.expect("date 列应含 null 位图"),
            vec![true, false]
        );

        replica.delete(TableId(1), b"k1").unwrap();
        assert_eq!(replica.row_count(TableId(1)), 1);
        let ids = replica
            .scan_column(TableId(1), 0, &DataType::Int64)
            .unwrap();
        assert_eq!(ids.values, ColumnValues::Int64(vec![2]));

        // 空表/未知表 → Empty
        assert!(
            replica
                .scan_column(TableId(99), 0, &DataType::Int64)
                .unwrap()
                .is_empty()
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
