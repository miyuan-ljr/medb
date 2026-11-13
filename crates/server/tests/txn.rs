//! 事务(RC)+ schema 多版本(ALTER ADD COLUMN)集成测试。
//!
//! RC 语义:事务内语句开始时刷新快照;COMMIT 原子落盘(单条 WAL 记录);
//! 未提交写入对其它会话不可见(缓冲 overlay 隔离);DDL 不允许在事务内。

use std::sync::Arc;

use common::config::Config;
use common::types::ScalarValue;
use server::engine::{Engine, Outcome};

fn open(dir: &std::path::Path) -> Arc<Engine> {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    Engine::open(config).unwrap()
}

fn rows_text(outcomes: Vec<Outcome>) -> Vec<Vec<ScalarValue>> {
    match outcomes.into_iter().next() {
        Some(Outcome::Query { rows, .. }) => rows,
        Some(Outcome::Tag { prefix, rows }) => {
            vec![vec![ScalarValue::Utf8(format!(
                "{prefix} {}",
                rows.unwrap_or(0)
            ))]]
        }
        None => Vec::new(),
    }
}

fn s(i: i64) -> ScalarValue {
    ScalarValue::Int64(i)
}

fn t(text: &str) -> ScalarValue {
    ScalarValue::Utf8(text.to_string())
}

#[test]
fn read_committed_isolation_between_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (id BIGINT, v TEXT)")
        .unwrap();
    engine
        .execute_script("INSERT INTO t VALUES (1, 'a')")
        .unwrap();

    // 会话 A 开事务写入(缓冲,不落盘)
    let mut a = server::session::Session::new(engine.clone()); // Arc<Engine> 移动进 Session
    a.execute_script("BEGIN").unwrap();
    a.execute_script("INSERT INTO t VALUES (2, 'b')").unwrap();
    // 读己之写
    let outcomes = a.execute_script("SELECT * FROM t").unwrap();
    assert_eq!(rows_text(outcomes).len(), 2);

    // 会话 B(autocommit)看不到 A 的未提交写入
    let outcomes = engine.execute_script("SELECT * FROM t").unwrap();
    let rows = rows_text(outcomes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![s(1), t("a")]);

    // B 在 A 提交前开始自己的事务并写入同一表(不同行)
    let mut b = server::session::Session::new(engine.clone());
    b.execute_script("BEGIN").unwrap();
    b.execute_script("INSERT INTO t VALUES (3, 'c')").unwrap();

    // A 提交
    let outcomes = a.execute_script("COMMIT").unwrap();
    assert_eq!(
        rows_text(outcomes),
        vec![vec![ScalarValue::Utf8("COMMIT 0".into())]]
    );

    // B 的语句(RC:新语句刷新快照)现在能看到 A 的提交 + 自己缓冲的写入
    let outcomes = b.execute_script("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(3)]]);
    // B 提交
    b.execute_script("COMMIT").unwrap();

    // 全部落盘
    let outcomes = engine.execute_script("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(3)]]);
}

#[test]
fn rollback_discards_buffered_writes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine.execute_script("CREATE TABLE t (id BIGINT)").unwrap();
    engine.execute_script("INSERT INTO t VALUES (1)").unwrap();

    let mut sess = server::session::Session::new(engine.clone());
    sess.execute_script("BEGIN").unwrap();
    sess.execute_script("INSERT INTO t VALUES (2)").unwrap();
    sess.execute_script("UPDATE t SET id = 99").unwrap();
    sess.execute_script("DELETE FROM t WHERE id = 1").unwrap();
    // 回滚前:缓冲内全部生效(两行都被 UPDATE 成 id=99;DELETE 无命中)
    let outcomes = sess.execute_script("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(2)]]);
    sess.execute_script("ROLLBACK").unwrap();

    // 回滚后:数据回到事务前
    let outcomes = engine.execute_script("SELECT * FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(1)]]);
}

#[test]
fn transaction_errors_and_rules() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine.execute_script("CREATE TABLE t (id BIGINT)").unwrap();

    let mut sess = server::session::Session::new(engine.clone());
    sess.execute_script("BEGIN").unwrap();
    assert!(sess.execute_script("BEGIN").is_err(), "嵌套事务应报错");
    assert!(
        sess.execute_script("ALTER TABLE t ADD COLUMN x BIGINT")
            .is_err(),
        "事务内 DDL 应报错"
    );
    sess.execute_script("COMMIT").unwrap();
    // 事务外 COMMIT/ROLLBACK 是无害空操作
    sess.execute_script("COMMIT").unwrap();
    sess.execute_script("ROLLBACK").unwrap();
}

#[test]
fn flush_tables_creates_sst_and_keeps_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let mut sess = server::session::Session::new(engine.clone());
    sess.execute_script("CREATE TABLE t (id BIGINT)").unwrap();
    sess.execute_script("INSERT INTO t VALUES (1)").unwrap();
    sess.execute_script("INSERT INTO t VALUES (2)").unwrap();

    // 转储前:sst 目录为空
    let sst_dir = dir.path().join("sst");
    assert_eq!(std::fs::read_dir(&sst_dir).unwrap().count(), 0);

    // FLUSH TABLES:memtable → SST,WAL 重置
    let outcomes = sess.execute_script("FLUSH TABLES").unwrap();
    match &outcomes[0] {
        Outcome::Tag { prefix, .. } => assert_eq!(prefix, "FLUSH"),
        other => panic!("expected flush tag, got {other:?}"),
    }
    assert!(std::fs::read_dir(&sst_dir).unwrap().count() >= 1);
    assert_eq!(
        std::fs::read_dir(dir.path().join("wal")).unwrap().count() >= 1,
        true
    );

    // 数据仍可读(来自 SST),再次 FLUSH 无脏数据 → FLUSH OK
    let outcomes = sess.execute_script("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(1)], vec![s(2)]]);
    let outcomes = sess.execute_script("FLUSH TABLES").unwrap();
    match &outcomes[0] {
        Outcome::Tag { prefix, .. } => assert_eq!(prefix, "FLUSH OK"),
        other => panic!("expected flush-ok tag, got {other:?}"),
    }
    // 继续写入照常
    sess.execute_script("INSERT INTO t VALUES (3)").unwrap();
    let outcomes = sess.execute_script("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(3)]]);
    let _ = t("keep");
}

#[test]
fn alter_add_column_multiversion_schema() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (id BIGINT, name TEXT)")
        .unwrap();
    engine
        .execute_script("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();

    // ALTER 尾部追加可空列:版本 +1,旧行补 NULL
    engine
        .execute_script("ALTER TABLE t ADD COLUMN score DOUBLE")
        .unwrap();
    let outcomes = engine.execute_script("SELECT id, score FROM t").unwrap();
    let rows = rows_text(outcomes);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], ScalarValue::Null);
    assert_eq!(rows[1][1], ScalarValue::Null);

    // 新行带该列
    engine
        .execute_script("INSERT INTO t VALUES (3, 'c', 3.5)")
        .unwrap();
    let outcomes = engine
        .execute_script("SELECT id, score FROM t ORDER BY id")
        .unwrap();
    let rows = rows_text(outcomes);
    assert_eq!(rows[2], vec![s(3), ScalarValue::Float64(3.5)]);

    // UPDATE 新列
    engine
        .execute_script("UPDATE t SET score = 9.5 WHERE id = 1")
        .unwrap();
    let outcomes = engine
        .execute_script("SELECT score FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![ScalarValue::Float64(9.5)]]);

    // 重启:versioned schema 持久化,旧行(3 列)与新行一致可读
    drop(engine);
    let engine = open(dir.path());
    let outcomes = engine
        .execute_script("SELECT id, name, score FROM t ORDER BY id")
        .unwrap();
    let rows = rows_text(outcomes);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][2], ScalarValue::Float64(9.5));
    assert_eq!(rows[1][2], ScalarValue::Null);
}

#[test]
fn restart_recovers_catalog_data_and_row_ids() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        engine
            .execute_script("CREATE TABLE t (id BIGINT, name TEXT)")
            .unwrap();
        engine
            .execute_script("INSERT INTO t VALUES (1, 'a')")
            .unwrap();
        engine
            .execute_script("INSERT INTO t VALUES (2, 'b')")
            .unwrap();
    }
    {
        let engine = open(dir.path());
        let outcomes = engine.execute_script("SELECT * FROM t").unwrap();
        assert_eq!(
            rows_text(outcomes),
            vec![vec![s(1), t("a")], vec![s(2), t("b")]]
        );
        engine
            .execute_script("INSERT INTO t VALUES (3, 'c')")
            .unwrap();
        let outcomes = engine.execute_script("SELECT * FROM t").unwrap();
        let all = rows_text(outcomes);
        assert_eq!(all.len(), 3);
        assert_eq!(all[2], vec![s(3), t("c")]);
    }
}
