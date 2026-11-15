//! 绑定参数端到端测试:占位符 `$n` 计划期替换(extended query protocol 的
//! SQL 层行为;pgwire 解码层见 handlers.rs)。

use std::sync::Arc;

use common::config::Config;
use common::types::{DataType, ScalarValue};
use server::engine::{Engine, Outcome};
use server::session::Session;

fn open(dir: &std::path::Path) -> Arc<Engine> {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    Engine::open(config).unwrap()
}

fn rows_of(outcome: Outcome) -> Vec<Vec<ScalarValue>> {
    match outcome {
        Outcome::Query { rows, .. } => rows,
        Outcome::Tag { prefix, rows } => {
            vec![vec![ScalarValue::Utf8(format!(
                "{prefix} {}",
                rows.unwrap_or(0)
            ))]]
        }
    }
}

#[test]
fn placeholder_select_insert_and_errors() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script(
            "CREATE TABLE t (k BIGINT NOT NULL, name TEXT NOT NULL, score DOUBLE)",
        )
        .unwrap();

    let mut session = Session::new(engine.clone());

    // INSERT:三列全部走绑定参数
    let outcome = session
        .execute_prepared(
            "INSERT INTO t VALUES ($1, $2, $3)",
            &[ScalarValue::Int64(1), ScalarValue::Utf8("a".into()), ScalarValue::Float64(0.5)],
        )
        .unwrap();
    assert_eq!(rows_of(outcome), vec![vec![ScalarValue::Utf8("INSERT 0 1".into())]]);

    // SELECT:同一条预编译语句用不同参数反复执行
    for (k, name) in [(2, "b"), (3, "c")] {
        session
            .execute_prepared(
                "INSERT INTO t VALUES ($1, $2, NULL)",
                &[ScalarValue::Int64(k), ScalarValue::Utf8(name.into())],
            )
            .unwrap();
    }
    let outcome = session
        .execute_prepared(
            "SELECT name FROM t WHERE k >= $1 AND k < $2 ORDER BY name",
            &[ScalarValue::Int64(2), ScalarValue::Int64(4)],
        )
        .unwrap();
    assert_eq!(
        rows_of(outcome),
        vec![vec![ScalarValue::Utf8("b".into())], vec![ScalarValue::Utf8("c".into())]]
    );

    // 同一语句同一参数重复执行(幂等解码)
    let outcome = session
        .execute_prepared(
            "SELECT COUNT(*) FROM t WHERE score IS NOT NULL",
            &[],
        )
        .unwrap();
    assert_eq!(rows_of(outcome), vec![vec![ScalarValue::Int64(1)]]);

    // 占位符越界
    let err = session
        .execute_prepared("SELECT * FROM t WHERE k = $5", &[])
        .unwrap_err();
    assert!(err.to_string().contains("out of range"), "{err}");

    // 日期参数类型保真(不降级为字符串比较)
    engine
        .execute_script("CREATE TABLE d (dt DATE NOT NULL, v BIGINT)")
        .unwrap();
    engine
        .execute_script("INSERT INTO d VALUES (DATE '2026-01-01', 1), (DATE '2026-06-01', 2)")
        .unwrap();
    let outcome = session
        .execute_prepared(
            "SELECT v FROM d WHERE dt >= $1 ORDER BY v",
            &[ScalarValue::Date(11)], // 1970-01-12;早于两行 → 全命中
        )
        .unwrap();
    assert_eq!(
        rows_of(outcome),
        vec![vec![ScalarValue::Int64(1)], vec![ScalarValue::Int64(2)]]
    );
}

#[test]
fn describe_query_schema_and_dml_empty() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (k BIGINT NOT NULL, name TEXT)")
        .unwrap();
    // 查询:NULL 绑定推导列名/类型
    let schema = engine
        .describe("SELECT k, name FROM t WHERE k = $1 AND name = $2", 2)
        .unwrap();
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].name, "k");
    assert_eq!(schema[0].ty, DataType::Int64);
    assert_eq!(schema[1].ty, DataType::Utf8);
    // DML:空 schema(不计划写语句 —— INSERT VALUES 计划期求值会触发约束)
    let schema = engine.describe("INSERT INTO t VALUES ($1, $2)", 2).unwrap();
    assert!(schema.is_empty());
}
