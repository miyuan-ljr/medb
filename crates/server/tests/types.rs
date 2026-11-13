//! LOB / 数组 / 向量 类型端到端测试。

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

#[test]
fn lob_array_vector_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script(
            "CREATE TABLE items (id BIGINT NOT NULL, data BLOB, tags BIGINT[], embed VECTOR(3))",
        )
        .unwrap();

    // BLOB 用 \x 十六进制字符串;数组用 {...};向量用 '[...]'(pgvector 风格)
    engine
        .execute_script(
            "INSERT INTO items VALUES (1, '\\xdeadbeef', '{10,20,NULL}', '[1.0,2.0,3.0]')",
        )
        .unwrap();
    engine
        .execute_script("INSERT INTO items VALUES (2, NULL, ARRAY[7,8], '[0.5,0.5,1.0]')")
        .unwrap();

    // 查询:文本格式输出
    let outcomes = engine
        .execute_script("SELECT data, tags, embed FROM items ORDER BY id")
        .unwrap();
    let rows = rows_text(outcomes);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0][0],
        ScalarValue::Binary(vec![0xde, 0xad, 0xbe, 0xef])
    );
    assert_eq!(
        rows[0][1],
        ScalarValue::List(vec![
            ScalarValue::Int64(10),
            ScalarValue::Int64(20),
            ScalarValue::Null,
        ])
    );
    assert_eq!(
        rows[0][2],
        ScalarValue::Vector(vec![1.0f32, 2.0f32, 3.0f32])
    );
    assert_eq!(rows[1][0], ScalarValue::Null);
    assert_eq!(
        rows[1][1],
        ScalarValue::List(vec![ScalarValue::Int64(7), ScalarValue::Int64(8)])
    );

    // WHERE 相等过滤(blob 比较)
    let outcomes = engine
        .execute_script("SELECT id FROM items WHERE data = '\\xdeadbeef'")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![ScalarValue::Int64(1)]]);

    // UPDATE 向量
    engine
        .execute_script("UPDATE items SET embed = '[9.0,9.0,9.0]' WHERE id = 2")
        .unwrap();
    let outcomes = engine
        .execute_script("SELECT embed FROM items WHERE id = 2")
        .unwrap();
    assert_eq!(
        rows_text(outcomes),
        vec![vec![ScalarValue::Vector(vec![9.0f32, 9.0f32, 9.0f32])]]
    );

    // 转储 → SST 后仍可读(跨介质)
    engine.flush().unwrap();
    let outcomes = engine
        .execute_script("SELECT data FROM items WHERE id = 1")
        .unwrap();
    assert_eq!(
        rows_text(outcomes),
        vec![vec![ScalarValue::Binary(vec![0xde, 0xad, 0xbe, 0xef])]]
    );

    // 重启恢复(含 catalog 中带新类型的 schema)
    drop(engine);
    let engine = open(dir.path());
    let outcomes = engine
        .execute_script("SELECT tags FROM items ORDER BY id")
        .unwrap();
    let rows = rows_text(outcomes);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0][0],
        ScalarValue::List(vec![
            ScalarValue::Int64(10),
            ScalarValue::Int64(20),
            ScalarValue::Null,
        ])
    );

    // 错误路径:维度不匹配 / 数组元素类型不符
    assert!(
        engine
            .execute_script("INSERT INTO items VALUES (3, NULL, NULL, '[1.0,2.0]')")
            .is_err()
    );
    assert!(
        engine
            .execute_script("INSERT INTO items VALUES (3, NULL, '{x,y}', NULL)")
            .is_err()
    );
    assert!(
        engine
            .execute_script("ALTER TABLE items ADD COLUMN bad VECTOR(0)")
            .is_err()
    );
}
