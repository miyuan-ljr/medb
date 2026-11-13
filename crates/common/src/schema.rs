//! 表 schema 与列描述。行(Row)= 标量值序列,与 schema 按位置对应。

use serde::{Deserialize, Serialize};

use crate::types::{DataType, ScalarValue};

pub type Row = Vec<ScalarValue>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDesc {
    pub name: String,
    pub ty: DataType,
    pub nullable: bool,
}

impl ColumnDesc {
    pub fn new(name: impl Into<String>, ty: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            ty,
            nullable,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub id: u64,
    pub name: String,
    pub columns: Vec<ColumnDesc>,
    /// 主键列名(隐藏主键模型:物理行键永远是 table_id ++ row_id,
    /// 用户主键只声明唯一性约束,由引擎维护唯一索引)。
    #[serde(default)]
    pub primary_key: Vec<String>,
    /// schema 版本(每次 ALTER 递增,从 1 开始)。旧行/旧语句通过
    /// 前缀兼容解码(缺列补 NULL、多列截断)跨版本可见。
    #[serde(default)]
    pub version: u64,
}

impl TableSchema {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// 主键列下标(按主键声明顺序);无主键返回 None。
    pub fn pk_column_indexes(&self) -> Option<Vec<usize>> {
        if self.primary_key.is_empty() {
            return None;
        }
        let mut out = Vec::with_capacity(self.primary_key.len());
        for name in &self.primary_key {
            let idx = self
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
                .unwrap_or_else(|| {
                    panic!("pk column '{name}' missing in schema (catalog corrupt)")
                });
            out.push(idx);
        }
        Some(out)
    }
}
