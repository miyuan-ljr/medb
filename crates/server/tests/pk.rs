//! 主键约束(隐藏 row_id 物理键 + 用户主键唯一索引)端到端测试。

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
        _ => Vec::new(),
    }
}

fn s(i: i64) -> ScalarValue {
    ScalarValue::Int64(i)
}

fn t(text: &str) -> ScalarValue {
    ScalarValue::Utf8(text.to_string())
}

#[test]
fn pk_duplicate_insert_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)")
        .unwrap();
    engine
        .execute_script("INSERT INTO t VALUES (1, 'a')")
        .unwrap();

    let err = engine
        .execute_script("INSERT INTO t VALUES (1, 'dup')")
        .unwrap_err();
    assert_eq!(err.code(), common::error::ErrorCode::Sql);

    // 原数据完好
    let outcomes = engine.execute_script("SELECT * FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(1), t("a")]]);

    // 不同主键值正常
    engine
        .execute_script("INSERT INTO t VALUES (2, 'b')")
        .unwrap();
    let outcomes = engine.execute_script("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(2)]]);
}

#[test]
fn pk_update_conflict_and_move() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)")
        .unwrap();
    engine
        .execute_script("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();

    // 更新到已存在的主键 → 冲突
    assert!(
        engine
            .execute_script("UPDATE t SET id = 2 WHERE id = 1")
            .is_err()
    );
    // 数据未被破坏
    let outcomes = engine
        .execute_script("SELECT id FROM t WHERE name = 'a'")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(1)]]);

    // 更新到空闲主键 → 成功(索引条目:旧删新写)
    engine
        .execute_script("UPDATE t SET id = 10 WHERE id = 1")
        .unwrap();
    let outcomes = engine
        .execute_script("SELECT id FROM t ORDER BY id")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(2)], vec![s(10)]]);

    // UPDATE 同值(自身)→ 放行
    engine
        .execute_script("UPDATE t SET id = 10, name = 'x' WHERE id = 10")
        .unwrap();
    let outcomes = engine
        .execute_script("SELECT name FROM t WHERE id = 10")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![t("x")]]);

    // 删除后主键释放 → 可重新插入
    engine
        .execute_script("DELETE FROM t WHERE id = 10")
        .unwrap();
    engine
        .execute_script("INSERT INTO t VALUES (10, 'reused')")
        .unwrap();
    let outcomes = engine.execute_script("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(2)]]);
}

#[test]
fn pk_txn_commit_conflict_detected() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)")
        .unwrap();

    // A 事务内插入 id=5(缓冲)
    let mut a = server::session::Session::new(engine.clone());
    a.execute_script("BEGIN").unwrap();
    a.execute_script("INSERT INTO t VALUES (5, 'from-a')")
        .unwrap();

    // B 提交 id=5(先落盘)
    let mut b = server::session::Session::new(engine.clone());
    b.execute_script("BEGIN").unwrap();
    b.execute_script("INSERT INTO t VALUES (5, 'from-b')")
        .unwrap();
    b.execute_script("COMMIT").unwrap();

    // A COMMIT 时唯一性探测发现 B 的已提交条目 → 冲突
    assert!(a.execute_script("COMMIT").is_err());

    // 只有一行(from-b),A 的写入全部丢弃
    let outcomes = engine.execute_script("SELECT v FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![t("from-b")]]);

    // A 的新事务可以正常插入其它主键
    let mut a2 = server::session::Session::new(engine.clone());
    a2.execute_script("BEGIN").unwrap();
    a2.execute_script("INSERT INTO t VALUES (6, 'ok')").unwrap();
    a2.execute_script("COMMIT").unwrap();
    let outcomes = engine.execute_script("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(2)]]);
}

#[test]
fn composite_pk_uniqueness() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script(
            "CREATE TABLE t (a BIGINT NOT NULL, b TEXT NOT NULL, v TEXT, PRIMARY KEY (a, b))",
        )
        .unwrap();
    engine
        .execute_script("INSERT INTO t VALUES (1, 'x', 'r1')")
        .unwrap();

    // 相同组合 → 冲突
    assert!(
        engine
            .execute_script("INSERT INTO t VALUES (1, 'x', 'dup')")
            .is_err()
    );
    // 任一分量不同 → 允许
    engine
        .execute_script("INSERT INTO t VALUES (1, 'y', 'r2')")
        .unwrap();
    engine
        .execute_script("INSERT INTO t VALUES (2, 'x', 'r3')")
        .unwrap();

    // 主键列强制 NOT NULL
    assert!(
        engine
            .execute_script("INSERT INTO t VALUES (NULL, 'x', 'r4')")
            .is_err()
    );

    // 组合更新:制造与 (1,'x') 相同的组合 → 冲突
    assert!(
        engine
            .execute_script("UPDATE t SET b = 'x' WHERE v = 'r2'")
            .is_err()
    );
    // 合法移动
    engine
        .execute_script("UPDATE t SET b = 'z' WHERE v = 'r2'")
        .unwrap();
    let outcomes = engine
        .execute_script("SELECT a, b FROM t WHERE v = 'r2'")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(1), t("z")]]);
}

#[test]
fn pk_constraint_persists_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        engine
            .execute_script("CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)")
            .unwrap();
        engine
            .execute_script("INSERT INTO t VALUES (1, 'a')")
            .unwrap();
        engine.flush().unwrap();
    }
    {
        let engine = open(dir.path());
        // 主键约束随 schema 持久化恢复
        assert!(
            engine
                .execute_script("INSERT INTO t VALUES (1, 'dup')")
                .is_err()
        );
        engine
            .execute_script("INSERT INTO t VALUES (2, 'b')")
            .unwrap();
        let outcomes = engine.execute_script("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows_text(outcomes), vec![vec![s(2)]]);
    }
}
