//! Engine 集成测试:SQL → 计划 → 执行 → LSM 落库的完整链路,
//! 覆盖建表/插入/查询(WHERE/LIMIT/投影)/删除/错误路径/重启恢复。

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
fn create_insert_select_delete_full_flow() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());

    // 建表
    let tag = engine
        .execute_script("CREATE TABLE t (id BIGINT NOT NULL, name TEXT, score DOUBLE)")
        .unwrap()
        .pop()
        .unwrap();
    let Outcome::Tag { prefix, rows } = tag else {
        panic!("expected tag");
    };
    assert_eq!(prefix, "CREATE TABLE");
    assert!(rows.is_none());

    // 插入(多行、列清单、表达式)
    let outcomes = engine
        .execute_script(
            "INSERT INTO t VALUES (1, 'a', 1.5); INSERT INTO t (id, name) VALUES (2, 'b'); \
             INSERT INTO t VALUES (3 + 0, 'c', 3.0)",
        )
        .unwrap();
    assert_eq!(outcomes.len(), 3);
    for o in &outcomes {
        let Outcome::Tag { prefix, rows } = o else {
            panic!("expected tag");
        };
        assert_eq!(prefix, "INSERT 0");
        assert_eq!(*rows, Some(1u64));
    }

    // 全表:插入序(row_id 单调)
    let outcomes = engine.execute_script("SELECT * FROM t").unwrap();
    assert_eq!(
        rows_text(outcomes),
        vec![
            vec![s(1), t("a"), ScalarValue::Float64(1.5)],
            vec![s(2), t("b"), ScalarValue::Null],
            vec![s(3), t("c"), ScalarValue::Float64(3.0)],
        ]
    );

    // WHERE + 投影 + 别名 + LIMIT
    let outcomes = engine
        .execute_script("SELECT name, id * 10 AS x FROM t WHERE id >= 2 LIMIT 1")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![t("b"), s(20)]]);

    // 限定列名
    let outcomes = engine
        .execute_script("SELECT t.id FROM t WHERE t.name = 'c'")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(3)]]);

    // DELETE WHERE
    let tag = engine
        .execute_script("DELETE FROM t WHERE id = 2")
        .unwrap()
        .pop()
        .unwrap();
    let Outcome::Tag { prefix, rows } = tag else {
        panic!("expected tag");
    };
    assert_eq!(prefix, "DELETE");
    assert_eq!(rows, Some(1));

    let outcomes = engine.execute_script("SELECT * FROM t").unwrap();
    let remaining = rows_text(outcomes);
    assert_eq!(remaining.len(), 2);
    assert!(!remaining.iter().any(|r| r[0] == s(2)));

    // DELETE 无 WHERE = 全删
    let outcomes = engine.execute_script("DELETE FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![t("DELETE 2")]]);
    assert!(
        engine.execute_script("SELECT * FROM t").unwrap().is_empty()
            || matches!(
                engine.execute_script("SELECT * FROM t").unwrap().first(),
                Some(Outcome::Query { rows, .. }) if rows.is_empty()
            )
    );

    // 建表后未插入:空结果
    engine
        .execute_script("CREATE TABLE empty_t (a BIGINT)")
        .unwrap();
    let outcomes = engine.execute_script("SELECT * FROM empty_t").unwrap();
    match outcomes.first() {
        Some(Outcome::Query { rows, .. }) => assert!(rows.is_empty()),
        other => panic!("expected empty query outcome, got {other:?}"),
    }
}

#[test]
fn update_aggregate_order_by_insert_select() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE emp (id BIGINT NOT NULL, dept TEXT, salary DOUBLE)")
        .unwrap();
    engine
        .execute_script(
            "INSERT INTO emp VALUES (1, 'eng', 100.0), (2, 'eng', 200.0), (3, 'ops', 50.0)",
        )
        .unwrap();

    // UPDATE(含表达式赋值,基于旧行)
    let outcomes = engine
        .execute_script("UPDATE emp SET salary = salary * 2 WHERE dept = 'eng'")
        .unwrap();
    match &outcomes[0] {
        Outcome::Tag { prefix, rows } => {
            assert_eq!(prefix, "UPDATE");
            assert_eq!(*rows, Some(2));
        }
        other => panic!("expected update tag, got {other:?}"),
    }
    let outcomes = engine
        .execute_script("SELECT salary FROM emp WHERE id = 1")
        .unwrap();
    match &outcomes[0] {
        Outcome::Query { rows, .. } => assert_eq!(rows[0][0], ScalarValue::Float64(200.0)),
        other => panic!("expected query, got {other:?}"),
    }

    // 聚合:GROUP BY + HAVING(eng: 200+400=600;ops: 50)
    let outcomes = engine
        .execute_script(
            "SELECT dept, COUNT(*) AS n, SUM(salary) AS total FROM emp GROUP BY dept HAVING SUM(salary) >= 300",
        )
        .unwrap();
    match &outcomes[0] {
        Outcome::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], ScalarValue::Utf8("eng".into()));
            assert_eq!(rows[0][1], ScalarValue::Int64(2));
            assert_eq!(rows[0][2], ScalarValue::Float64(600.0));
        }
        other => panic!("expected query, got {other:?}"),
    }

    // 全表聚合
    let outcomes = engine
        .execute_script("SELECT COUNT(*), SUM(salary) FROM emp")
        .unwrap();
    match &outcomes[0] {
        Outcome::Query { rows, .. } => {
            assert_eq!(rows[0][0], ScalarValue::Int64(3));
            assert_eq!(rows[0][1], ScalarValue::Float64(650.0)); // 200+400+50
        }
        other => panic!("expected query, got {other:?}"),
    }

    // ORDER BY(表达式 + DESC + LIMIT)
    let outcomes = engine
        .execute_script("SELECT id, salary * 2 AS d FROM emp ORDER BY salary DESC LIMIT 1")
        .unwrap();
    match &outcomes[0] {
        Outcome::Query { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], ScalarValue::Int64(2));
            assert_eq!(rows[0][1], ScalarValue::Float64(800.0));
        }
        other => panic!("expected query, got {other:?}"),
    }

    // INSERT SELECT
    engine
        .execute_script("CREATE TABLE emp_copy (id BIGINT, dept TEXT)")
        .unwrap();
    let outcomes = engine
        .execute_script(
            "INSERT INTO emp_copy (id, dept) SELECT id, dept FROM emp WHERE salary >= 50",
        )
        .unwrap();
    match &outcomes[0] {
        Outcome::Tag { prefix, rows } => {
            assert_eq!(prefix, "INSERT 0");
            assert_eq!(*rows, Some(3));
        }
        other => panic!("expected insert tag, got {other:?}"),
    }
    let outcomes = engine
        .execute_script("SELECT id FROM emp_copy ORDER BY id")
        .unwrap();
    match &outcomes[0] {
        Outcome::Query { rows, .. } => assert_eq!(rows.len(), 3),
        other => panic!("expected query, got {other:?}"),
    }

    // UPDATE 无 WHERE = 全表
    let outcomes = engine.execute_script("UPDATE emp SET salary = 1").unwrap();
    match &outcomes[0] {
        Outcome::Tag { rows, .. } => assert_eq!(*rows, Some(3)),
        other => panic!("expected update tag, got {other:?}"),
    }
    let outcomes = engine
        .execute_script("SELECT COUNT(*) FROM emp WHERE salary = 1")
        .unwrap();
    match &outcomes[0] {
        Outcome::Query { rows, .. } => assert_eq!(rows[0][0], ScalarValue::Int64(3)),
        other => panic!("expected query, got {other:?}"),
    }
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
    // 重开(新 Engine,同目录):catalog 与数据都从 LSM 恢复
    {
        let engine = open(dir.path());
        let outcomes = engine.execute_script("SELECT * FROM t").unwrap();
        assert_eq!(
            rows_text(outcomes),
            vec![vec![s(1), t("a")], vec![s(2), t("b")]]
        );
        // row_id 水位恢复:新插入不复用旧 id(不会覆盖旧行)
        engine
            .execute_script("INSERT INTO t VALUES (3, 'c')")
            .unwrap();
        let outcomes = engine.execute_script("SELECT * FROM t").unwrap();
        let all = rows_text(outcomes);
        assert_eq!(all.len(), 3);
        assert_eq!(all[2], vec![s(3), t("c")]);
    }
}

#[test]
fn drop_table_removes_metadata_and_data() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (id BIGINT, name TEXT)")
        .unwrap();
    engine
        .execute_script("INSERT INTO t VALUES (1, 'a')")
        .unwrap();
    engine.execute_script("DROP TABLE t").unwrap();

    assert!(engine.execute_script("SELECT * FROM t").is_err());
    assert!(
        engine
            .execute_script("INSERT INTO t VALUES (2, 'b')")
            .is_err()
    );

    // 同名重建:全新空表,不看到旧行
    engine
        .execute_script("CREATE TABLE t (id BIGINT, name TEXT)")
        .unwrap();
    let outcomes = engine.execute_script("SELECT * FROM t").unwrap();
    match outcomes.first() {
        Some(Outcome::Query { rows, .. }) => assert!(rows.is_empty()),
        other => panic!("expected empty query outcome, got {other:?}"),
    }
}

#[test]
fn error_paths_are_clean() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (id BIGINT NOT NULL, name TEXT)")
        .unwrap();

    // 语法/语义错误:连接式调用应返回 Err 而非 panic
    for sql in [
        "SELECT * FROM missing",
        "INSERT INTO missing VALUES (1)",
        "INSERT INTO t VALUES (NULL, 'x')", // NOT NULL
        "INSERT INTO t VALUES ('x', 'y')",  // 类型不匹配
        "INSERT INTO t VALUES (1)",         // 列数不足
        "SELECT nope FROM t",
        "THIS IS NOT SQL",
    ] {
        assert!(engine.execute_script(sql).is_err(), "应失败: {sql}");
    }
    // 失败后引擎仍可用
    engine
        .execute_script("INSERT INTO t VALUES (1, 'ok')")
        .unwrap();
    let outcomes = engine.execute_script("SELECT name FROM t").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![t("ok")]]);
}

#[test]
fn multi_statement_script_executes_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let outcomes = engine
        .execute_script(
            "CREATE TABLE t (a BIGINT); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT a FROM t",
        )
        .unwrap();
    assert_eq!(outcomes.len(), 4);
    assert!(matches!(outcomes[3], Outcome::Query { ref rows, .. } if rows.len() == 2));
}
