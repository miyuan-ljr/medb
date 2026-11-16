//! 元数据目录:表 schema 的注册与查找。
//!
//! v1 进程内实现;持久化 catalog(WAL 化的 DDL 日志)见路线图。
//! `CatalogStore` trait 为未来替换持久实现留出接口。

use std::collections::BTreeMap;

use common::error::{Error, Result};
use common::ids::IdGenerator;
use common::schema::TableSchema;
use parking_lot::RwLock;

pub trait CatalogStore: Send + Sync {
    /// 注册表;id 为 0 时自动分配。重名报错。
    fn create_table(&self, schema: TableSchema) -> Result<TableSchema>;
    fn drop_table(&self, name: &str) -> Result<TableSchema>;
    /// 整体替换表 schema(ALTER 后调用;version 必须比当前大)。
    fn update_table(&self, schema: TableSchema) -> Result<TableSchema>;
    fn get_table(&self, name: &str) -> Option<TableSchema>;
    fn get_table_by_id(&self, id: u64) -> Option<TableSchema>;
    fn list_tables(&self) -> Vec<TableSchema>;
}

pub struct InMemoryCatalog {
    tables: RwLock<BTreeMap<String, TableSchema>>,
    id_gen: IdGenerator,
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(BTreeMap::new()),
            id_gen: IdGenerator::new(1000),
        }
    }
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogStore for InMemoryCatalog {
    fn create_table(&self, mut schema: TableSchema) -> Result<TableSchema> {
        let mut tables = self.tables.write();
        if tables.contains_key(&schema.name) {
            return Err(Error::catalog(format!(
                "table '{}' already exists",
                schema.name
            )));
        }
        if schema.id == 0 {
            schema.id = self.id_gen.next();
        } else {
            // 恢复路径:持久化 id 不经过 next() → 推进生成器,避免
            // 重启后建表撞上恢复表的 id(行区间按 table id 划分,撞车 = 串读)
            self.id_gen.bump_past(schema.id);
        }
        let created = schema.clone();
        tables.insert(schema.name.clone(), schema);
        Ok(created)
    }

    fn drop_table(&self, name: &str) -> Result<TableSchema> {
        let mut tables = self.tables.write();
        tables
            .remove(name)
            .ok_or_else(|| Error::catalog(format!("table '{name}' not found")))
    }

    fn update_table(&self, schema: TableSchema) -> Result<TableSchema> {
        let mut tables = self.tables.write();
        let current = tables
            .get(&schema.name)
            .ok_or_else(|| Error::catalog(format!("table '{}' not found", schema.name)))?;
        if schema.id != current.id {
            return Err(Error::catalog(format!(
                "table '{}' id mismatch ({} != {})",
                schema.name, schema.id, current.id
            )));
        }
        if schema.version <= current.version {
            return Err(Error::catalog(format!(
                "table '{}' schema version must increase ({} <= {})",
                schema.name, schema.version, current.version
            )));
        }
        let updated = schema.clone();
        tables.insert(schema.name.clone(), schema);
        Ok(updated)
    }

    fn get_table(&self, name: &str) -> Option<TableSchema> {
        self.tables.read().get(name).cloned()
    }

    fn get_table_by_id(&self, id: u64) -> Option<TableSchema> {
        self.tables.read().values().find(|t| t.id == id).cloned()
    }

    fn list_tables(&self) -> Vec<TableSchema> {
        self.tables.read().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::schema::ColumnDesc;
    use common::types::DataType;

    fn schema(name: &str) -> TableSchema {
        TableSchema {
            id: 0,
            name: name.into(),
            columns: vec![ColumnDesc::new("v", DataType::Int64, true)],
            primary_key: Vec::new(),
            indexes: Vec::new(),
            version: 1,
        }
    }

    #[test]
    fn create_get_drop() {
        let catalog = InMemoryCatalog::new();
        let created = catalog.create_table(schema("t1")).unwrap();
        assert!(created.id > 0, "id 自动分配");
        assert!(catalog.get_table("t1").is_some());
        assert!(catalog.get_table_by_id(created.id).is_some());

        assert!(catalog.create_table(schema("t1")).is_err(), "重名拒绝");
        let dropped = catalog.drop_table("t1").unwrap();
        assert_eq!(dropped.id, created.id);
        assert!(catalog.get_table("t1").is_none());
        assert!(catalog.drop_table("nope").is_err());
    }
}
