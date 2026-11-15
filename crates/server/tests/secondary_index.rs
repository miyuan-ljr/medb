//! 二级索引 e2e:DDL + 基于代价的索引选择 + 索引查询正确性。
//!
//! 架构约定:索引树建在列存 segment 上(每段每列一棵,flush 时随段重建),
//! memtable 增量不在树里 —— 查询时与 delta 合并(更新取新版本、删除剔除)。
//! 优化器用统计(NDV/min-max)估算命中行,行代价 × INDEX_COST_FACTOR 仍小于
//! 全扫才选索引;EXPLAIN 可观察选择结果。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use catalog::CatalogStore;
use common::config::Config;
use common::types::ScalarValue;
use server::engine::{Engine, Outcome};

fn open(dir: &Path) -> Arc<Engine> {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    Engine::open(config).unwrap()
}

fn flush(engine: &Arc<Engine>) {
    server::session::Session::new(engine.clone())
        .execute_script("FLUSH TABLES")
        .unwrap();
}

fn idx_file(dir: &Path, table_id: u64, col_idx: u32) -> Option<PathBuf> {
    let prefix = format!("{table_id}.");
    std::fs::read_dir(dir.join("columnar"))
        .ok()?
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with(&prefix) && n.ends_with(&format!(".{col_idx}.idx")))
                .unwrap_or(false)
        })
}

fn tid(engine: &Arc<Engine>, table: &str) -> u64 {
    engine.catalog().get_table(table).unwrap().id
}

fn rows_of(outcomes: Vec<Outcome>) -> Vec<Vec<ScalarValue>> {
    match outcomes.into_iter().next() {
        Some(Outcome::Query { rows, .. }) => rows,
        Some(Outcome::Tag { prefix, rows }) => {
            vec![vec![ScalarValue::Utf8(format!("{prefix} {}", rows.unwrap_or(0)))]]
        }
        None => Vec::new(),
        other => panic!("unexpected outcome: {other:?}"),
    }
}

fn s(i: i64) -> ScalarValue {
    ScalarValue::Int64(i)
}

fn explain(engine: &Arc<Engine>, sql: &str) -> String {
    let rows = rows_of(engine.execute_script(sql).unwrap());
    let mut text = String::new();
    for r in &rows {
        match &r[0] {
            ScalarValue::Utf8(line) => {
                text.push_str(line);
                text.push('\n');
            }
            other => panic!("explain not text: {other:?}"),
        }
    }
    text
}

/// 建表 + 装载 n 行(id = 0..n,val = id×10)+ flush。
fn load(engine: &Arc<Engine>, n: i64) {
    engine
        .execute_script("CREATE TABLE t (id BIGINT NOT NULL, val BIGINT NOT NULL, tag TEXT)")
        .unwrap();
    let mut values = String::new();
    for i in 0..n {
        if i > 0 {
            values.push_str(", ");
        }
        values.push_str(&format!("({i}, {}, 't{}')", i * 10, i % 7));
    }
    engine
        .execute_script(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    flush(&engine);
}

#[test]
fn create_index_and_point_range_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load(&engine, 200);

    // 无索引 → 全扫
    let plan = explain(&engine, "EXPLAIN SELECT * FROM t WHERE id = 42");
    assert!(!plan.contains("Index Scan"), "建索引前不应有 Index Scan: {plan}");

    engine
        .execute_script("CREATE INDEX idx_id ON t (id)")
        .unwrap();
    // 段已存在 → 立即建树
    assert!(
        idx_file(dir.path(), tid(&engine, "t"), 0).is_some(),
        "CREATE INDEX 应立即写索引文件(段已存在)"
    );

    let plan = explain(&engine, "EXPLAIN SELECT * FROM t WHERE id = 42");
    assert!(plan.contains("Index Scan"), "等值点查应走索引: {plan}");

    // 点查
    assert_eq!(
        rows_of(
            engine
                .execute_script("SELECT id, val FROM t WHERE id = 42")
                .unwrap()
        ),
        vec![vec![s(42), s(420)]]
    );
    // 范围(闭/开混合)
    let rows = rows_of(
        engine
            .execute_script("SELECT id FROM t WHERE id >= 100 AND id < 110 ORDER BY id")
            .unwrap(),
    );
    assert_eq!(
        rows,
        (100..110).map(|i| vec![s(i)]).collect::<Vec<_>>(),
        "索引范围查询"
    );
    // 反向范围 + 残余谓词(id > 195 命中索引,val > 1950 走残余;
    // val = id*10 → 196..199 全部同时满足两条件)
    let rows = rows_of(
        engine
            .execute_script(
                "SELECT id FROM t WHERE id > 195 AND val > 1950 ORDER BY id DESC",
            )
            .unwrap(),
    );
    assert_eq!(
        rows,
        (195..199).rev().map(|i| vec![s(i + 1)]).collect::<Vec<_>>(),
        "索引范围 + 残余谓词 + DESC"
    );

    // 非索引列谓词 → 全扫(无该列索引)
    let plan = explain(&engine, "EXPLAIN SELECT * FROM t WHERE val = 420");
    assert!(!plan.contains("Index Scan"), "无索引列不应走索引: {plan}");

    // 非选择性范围 → 优化器不选索引(代价劣于全扫)
    let plan = explain(&engine, "EXPLAIN SELECT * FROM t WHERE id < 1000000");
    assert!(!plan.contains("Index Scan"), "覆盖全表的范围不应走索引: {plan}");
}

#[test]
fn index_sees_memtable_overlay() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load(&engine, 100);
    engine
        .execute_script("CREATE INDEX idx_id ON t (id)")
        .unwrap();

    // 段后变更:新增(只在 memtable,不在索引树)+ 更新 + 删除
    engine
        .execute_script("INSERT INTO t VALUES (200, 2000, 'new')")
        .unwrap();
    engine
        .execute_script("UPDATE t SET val = 999 WHERE id = 50")
        .unwrap();
    engine
        .execute_script("DELETE FROM t WHERE id = 51")
        .unwrap();

    // delta 中的新行可被等值查到
    assert_eq!(
        rows_of(engine.execute_script("SELECT val FROM t WHERE id = 200").unwrap()),
        vec![vec![s(2000)]]
    );
    // 更新行:索引给出 segment 旧版本,memtable 覆盖解析后取新值
    assert_eq!(
        rows_of(engine.execute_script("SELECT val FROM t WHERE id = 50").unwrap()),
        vec![vec![s(999)]]
    );
    // 删除行:候选被墓碑剔除
    assert_eq!(
        rows_of(engine.execute_script("SELECT id FROM t WHERE id = 51").unwrap()),
        Vec::<Vec<ScalarValue>>::new()
    );
    // 一致性:索引路径与行存路径全量一致
    let via_index = rows_of(engine.execute_script("SELECT COUNT(*) FROM t WHERE id < 1000").unwrap());
    assert_eq!(via_index, vec![vec![s(100)]], "100 - 1 删 + 1 增");
}

#[test]
fn index_rebuilds_on_flush_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    {
        let engine = open(&data_dir);
        load(&engine, 50);
        engine.execute_script("CREATE INDEX idx_id ON t (id)").unwrap();
        engine.execute_script("INSERT INTO t VALUES (500, 5000, 'late')").unwrap();
        // flush:memtable 落 SST → segment 代次 +1 → 索引树随新段重建
        flush(&engine);
        assert_eq!(
            rows_of(engine.execute_script("SELECT val FROM t WHERE id = 500").unwrap()),
            vec![vec![s(5000)]],
            "flush 后新行已入段,索引可查"
        );
    }
    {
        // 重启:代次由文件名发现,索引文件同前缀即可用
        let engine = open(&data_dir);
        assert_eq!(
            rows_of(engine.execute_script("SELECT val FROM t WHERE id = 500").unwrap()),
            vec![vec![s(5000)]]
        );
        let plan = explain(&engine, "EXPLAIN SELECT * FROM t WHERE id = 500");
        assert!(plan.contains("Index Scan"), "重启后索引仍被选择: {plan}");
    }
}

#[test]
fn drop_index_and_ddl_guards() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load(&engine, 30);
    engine.execute_script("CREATE INDEX idx_id ON t (id)").unwrap();

    // 重复创建 → 报错;IF NOT EXISTS → 容忍
    assert!(engine.execute_script("CREATE INDEX idx_id ON t (id)").is_err());
    engine
        .execute_script("CREATE INDEX IF NOT EXISTS idx_id ON t (id)")
        .unwrap();
    // 同列第二索引 → 报错
    assert!(engine.execute_script("CREATE INDEX idx_id2 ON t (id)").is_err());
    // 唯一索引 / 多列 / 表达式 → 不支持
    assert!(engine.execute_script("CREATE UNIQUE INDEX u ON t (id)").is_err());
    assert!(engine.execute_script("CREATE INDEX m ON t (id, val)").is_err());
    // 不存在的表/列 → 报错
    assert!(engine.execute_script("CREATE INDEX x ON nope (id)").is_err());
    assert!(engine.execute_script("CREATE INDEX x ON t (nope)").is_err());

    // DROP INDEX:其余列文件保留,查询退回全扫
    engine.execute_script("DROP INDEX idx_id").unwrap();
    assert!(idx_file(dir.path(), tid(&engine, "t"), 0).is_none());
    let plan = explain(&engine, "EXPLAIN SELECT * FROM t WHERE id = 42");
    assert!(!plan.contains("Index Scan"), "删索引后走全扫: {plan}");
    // 语义不受影响(表只有 id 0..29 → 点查 id = 4)
    assert_eq!(
        rows_of(engine.execute_script("SELECT COUNT(*) FROM t WHERE id = 4").unwrap()),
        vec![vec![s(1)]]
    );
    // DROP 不存在的索引 → 报错;IF EXISTS → 容忍
    assert!(engine.execute_script("DROP INDEX idx_id").is_err());
    engine.execute_script("DROP INDEX IF EXISTS idx_id").unwrap();
    // ALTER TABLE ... DROP INDEX 同义
    engine.execute_script("CREATE INDEX idx_id ON t (id)").unwrap();
    engine
        .execute_script("ALTER TABLE t DROP INDEX idx_id")
        .unwrap();
    assert!(idx_file(dir.path(), tid(&engine, "t"), 0).is_none());
}

#[test]
fn alter_invalidates_index_until_flush() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    {
        let engine = open(&data_dir);
        load(&engine, 20);
        engine.execute_script("CREATE INDEX idx_id ON t (id)").unwrap();
        assert!(idx_file(&data_dir, tid(&engine, "t"), 0).is_some());
        // ALTER:段失效(索引文件一并删除)→ 查询回退全扫,结果正确
        engine.execute_script("ALTER TABLE t ADD COLUMN note TEXT").unwrap();
        assert!(idx_file(&data_dir, tid(&engine, "t"), 0).is_none());
        assert_eq!(
            rows_of(engine.execute_script("SELECT id FROM t WHERE id = 7").unwrap()),
            vec![vec![s(7)]]
        );
        flush(&engine);
    }
    {
        // flush 重建后:段 + 索引(新 schema 宽度)恢复
        let engine = open(&data_dir);
        assert!(idx_file(&data_dir, tid(&engine, "t"), 0).is_some());
        assert_eq!(
            rows_of(engine.execute_script("SELECT id, note FROM t WHERE id = 7").unwrap()),
            vec![vec![s(7), ScalarValue::Null]]
        );
    }
}

#[test]
fn index_on_text_column() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE users (name TEXT NOT NULL, score BIGINT NOT NULL)")
        .unwrap();
    let mut values = String::new();
    for i in 0..80 {
        if i > 0 {
            values.push_str(", ");
        }
        values.push_str(&format!("('user{:03}', {i})", i));
    }
    engine
        .execute_script(&format!("INSERT INTO users VALUES {values}"))
        .unwrap();
    flush(&engine);
    engine
        .execute_script("CREATE INDEX idx_name ON users (name)")
        .unwrap();
    let plan = explain(&engine, "EXPLAIN SELECT * FROM users WHERE name = 'user042'");
    assert!(plan.contains("Index Scan"), "Utf8 等值应走索引: {plan}");
    assert_eq!(
        rows_of(
            engine
                .execute_script("SELECT score FROM users WHERE name = 'user042'")
                .unwrap()
        ),
        vec![vec![s(42)]]
    );
    // Utf8 范围(前缀区间)
    let rows = rows_of(
        engine
            .execute_script(
                "SELECT COUNT(*) FROM users WHERE name >= 'user070' AND name < 'user080'",
            )
            .unwrap(),
    );
    assert_eq!(rows, vec![vec![s(10)]]);
}
