//! 代价优化器与统计信息集成测试:
//! EXPLAIN 计划树、ANALYZE、统计持久化与增量维护、CBO 连接顺序重排。

use std::sync::Arc;

use catalog::CatalogStore;
use common::config::Config;
use common::types::ScalarValue;
use server::engine::{Engine, Outcome};

fn open(dir: &std::path::Path) -> Arc<Engine> {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    Engine::open(config).unwrap()
}

fn explain_lines(engine: &Arc<Engine>, sql: &str) -> Vec<String> {
    match engine.execute_script(sql).unwrap().pop().unwrap() {
        Outcome::Query { rows, .. } => rows
            .into_iter()
            .map(|r| match &r[0] {
                ScalarValue::Utf8(s) => s.clone(),
                other => panic!("explain row not text: {other:?}"),
            })
            .collect(),
        other => panic!("expected query outcome, got {other:?}"),
    }
}

fn load_connector_schema(engine: &Arc<Engine>) {
    engine
        .execute_script(
            "CREATE TABLE a (k BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL); \
             CREATE TABLE b (k BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL); \
             CREATE TABLE c (k BIGINT NOT NULL PRIMARY KEY, z BIGINT NOT NULL)",
        )
        .unwrap();
    // a: 10 行(连接枢纽);b: 10 行;c: 5000 行(与 b 无连接条件)
    let mut stmts = String::new();
    for i in 0..10 {
        stmts.push_str(&format!(
            "INSERT INTO a VALUES ({i}, {i}); INSERT INTO b VALUES ({i}, {i}); "
        ));
    }
    for i in 0..5000 {
        stmts.push_str(&format!("INSERT INTO c VALUES ({}, {}); ", i, i % 7));
    }
    engine.execute_script(&stmts).unwrap();
}

/// EXPLAIN:返回计划文本(根在前),不执行查询。
#[test]
fn explain_returns_plan_tree() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_connector_schema(&engine);
    let lines = explain_lines(
        &engine,
        "EXPLAIN SELECT b.y FROM a JOIN b ON a.k = b.k WHERE a.x < 5",
    );
    let joined = lines.join("\n");
    assert!(joined.contains("HashJoin"), "plan = {joined}");
    assert!(joined.contains("on:"), "应有哈希键: {joined}");
    assert!(joined.contains("filter:"), "应有下推过滤: {joined}");
    // 根为最终投影,连接树在其下
    assert!(lines[0].starts_with("Project"), "plan = {lines:?}");
}

/// 统计自动 ANALYZE:首次规划即有统计;EXPLAIN 估行数非默认值。
#[test]
fn auto_analyze_feeds_estimates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_connector_schema(&engine);
    let lines = explain_lines(&engine, "EXPLAIN SELECT COUNT(*) FROM c WHERE z = 1");
    // c: 5000 行,z 取值 0..6(ndv 7)→ 等值选择性 1/7 → 估行数 ≈ 714
    // (默认基数 1000 或全表 5000 都不是这个数 → 证明用了 NDV 统计)
    let joined = lines.join("\n");
    assert!(joined.contains("rows≈714"), "应基于真实统计估算: {joined}");
}

/// CBO 重排:FROM b, c, a(b 与 c 之间无连接条件)→ 计划不得出现无键的
/// 笛卡尔连接;每个连接步骤都必须带等值键(枢纽表 a 先接入)。
#[test]
fn cbo_avoids_cartesian_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_connector_schema(&engine);
    let lines = explain_lines(
        &engine,
        "EXPLAIN SELECT a.x FROM b, c, a WHERE b.k = a.k AND c.k = a.k",
    );
    for line in lines.iter().filter(|l| l.contains("HashJoin")) {
        assert!(line.contains("on:"), "禁止无键笛卡尔连接: {lines:?}");
    }
    // 每步累计基数都被等值条件压住(b×c 笛卡尔会有 ~50000 行中间结果)
    assert!(
        !lines.iter().any(|l| l.contains("rows≈50000")),
        "不应出现笛卡尔中间结果: {lines:?}"
    );
}

/// 统计增量维护:INSERT/DELETE 后 row_count 即时更新(无需 ANALYZE)。
#[test]
fn stats_track_dml() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (id BIGINT, name TEXT)")
        .unwrap();
    engine
        .execute_script("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    // 第一次查询触发自动 ANALYZE
    engine.execute_script("SELECT COUNT(*) FROM t").unwrap();
    let schema = engine.catalog().get_table("t").unwrap();
    let s0 = engine.table_stats(schema.id).unwrap();
    assert_eq!(s0.row_count, 2);
    assert_eq!(s0.column("id").unwrap().min, Some(ScalarValue::Int64(1)));
    assert_eq!(s0.column("id").unwrap().max, Some(ScalarValue::Int64(2)));

    engine
        .execute_script("INSERT INTO t VALUES (9, 'z')")
        .unwrap();
    let s1 = engine.table_stats(schema.id).unwrap();
    assert_eq!(s1.row_count, 3, "INSERT 增量记账");
    assert_eq!(s1.column("id").unwrap().max, Some(ScalarValue::Int64(9)));

    engine.execute_script("DELETE FROM t WHERE id = 1").unwrap();
    let s2 = engine.table_stats(schema.id).unwrap();
    assert_eq!(s2.row_count, 2, "DELETE 增量记账");
}

/// ANALYZE TABLE:显式重建统计;统计持久化,重启后仍在。
#[test]
fn analyze_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    {
        let engine = open(&data_dir);
        engine
            .execute_script("CREATE TABLE t (id BIGINT, name TEXT)")
            .unwrap();
        engine
            .execute_script("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
            .unwrap();
        let outcome = engine
            .execute_script("ANALYZE TABLE t")
            .unwrap()
            .pop()
            .unwrap();
        match outcome {
            Outcome::Tag { prefix, .. } => assert_eq!(prefix, "ANALYZE"),
            other => panic!("expected analyze tag, got {other:?}"),
        }
        let schema = engine.catalog().get_table("t").unwrap();
        let s = engine.table_stats(schema.id).unwrap();
        assert_eq!(s.row_count, 3);
        assert_eq!(s.column("name").unwrap().ndv, 3);
        assert!(!s.needs_analyze(), "ANALYZE 后应视为新鲜");
    }
    // 重启:统计从 LSM 恢复
    let engine = open(&data_dir);
    let schema = engine.catalog().get_table("t").unwrap();
    let s = engine.table_stats(schema.id).unwrap();
    assert_eq!(s.row_count, 3, "统计应跨重启恢复");
    assert_eq!(s.column("name").unwrap().ndv, 3);
}

/// DROP TABLE 清理统计;ANALYZE 全表形式可用。
#[test]
fn analyze_all_and_drop_cleans_stats() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine.execute_script("CREATE TABLE t (id BIGINT)").unwrap();
    engine.execute_script("INSERT INTO t VALUES (1)").unwrap();
    engine.execute_script("ANALYZE").unwrap();
    let schema = engine.catalog().get_table("t").unwrap();
    assert!(engine.table_stats(schema.id).is_some());
    engine.execute_script("DROP TABLE t").unwrap();
    assert!(engine.table_stats(schema.id).is_none(), "DROP 应清理统计");
}

/// CBO 选择性估算改善连接顺序:过滤后更小的表作探测侧,EXPLAIN 中
/// 估行数反映选择性(等值条件 1/ndv)。
#[test]
fn selectivity_shrinks_scan_estimates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (id BIGINT, g BIGINT)")
        .unwrap();
    let mut stmts = String::new();
    for i in 0..200 {
        stmts.push_str(&format!("INSERT INTO t VALUES ({i}, {}); ", i % 4));
    }
    engine.execute_script(&stmts).unwrap();
    let lines = explain_lines(&engine, "EXPLAIN SELECT id FROM t WHERE g = 1");
    // g 有 4 个不同值 → 等值选择性 1/4 → 估行数 ≈ 50(而非 200)
    let joined = lines.join("\n");
    assert!(joined.contains("rows≈50"), "等值选择性 1/4: {joined}");
}
