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

/// 二级索引元数据(列存 segment 上的索引树;catalog 持久化)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexMeta {
    /// 表内唯一 id(分配 = 当前最大 +1;0 保留给 PK 唯一索引)。
    pub id: u64,
    /// 表内唯一名称(CREATE INDEX 给出)。
    pub name: String,
    /// 索引列名(v1 单列)。
    pub column: String,
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
    /// 二级索引(旧 catalog JSON 无此字段 → 空,前向兼容)。
    #[serde(default)]
    pub indexes: Vec<IndexMeta>,
}

impl TableSchema {
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// 按名称查索引(表内唯一;大小写敏感与列名一致)。
    pub fn find_index(&self, name: &str) -> Option<&IndexMeta> {
        self.indexes.iter().find(|i| i.name == name)
    }

    /// 某列上的索引(v1 每列至多一棵索引树)。
    pub fn index_on_column(&self, col_idx: usize) -> Option<&IndexMeta> {
        let col = self.columns.get(col_idx)?;
        self.indexes.iter().find(|i| i.column == col.name)
    }

    /// 下一个可用索引 id。
    pub fn next_index_id(&self) -> u64 {
        self.indexes.iter().map(|i| i.id).max().unwrap_or(0) + 1
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
