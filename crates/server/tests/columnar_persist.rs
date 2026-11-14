//! 列存副本持久化 e2e:快照 + LSM 增量重放(水位线)。
//!
//! 场景:
//! 1. 建表 + 装载 → FLUSH TABLES(写列存快照)→ 重启:快照加载,行数正确;
//! 2. 快照后继续 UPDATE/DELETE/INSERT(只在 LSM,seq > 水位)→ 重启:重放正确;
//! 3. 不 FLUSH 直接重启(快照缺失)→ 全量重建路径正确;
//! 4. 列存路径(AP)在重启后读数正确;
//! 5. DROP TABLE 清理快照文件。

use std::path::Path;
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

/// FLUSH TABLES 是 Session 级命令(与协议层一致)。
fn flush(engine: &Arc<Engine>) {
    server::session::Session::new(engine.clone())
        .execute_script("FLUSH TABLES")
        .unwrap();
}

/// lineitem 表的真实 table id(catalog 从 1000 起分配)。
fn lineitem_id(engine: &Arc<Engine>) -> u64 {
    engine.catalog().get_table("lineitem").unwrap().id
}

fn snapshot_file(dir: &Path, table_id: u64) -> std::path::PathBuf {
    dir.join("columnar").join(format!("{table_id}.col"))
}

fn count(engine: &Arc<Engine>, table: &str) -> u64 {
    match engine
        .execute_script(&format!("SELECT COUNT(*) FROM {table}"))
        .unwrap()
        .pop()
        .unwrap()
    {
        Outcome::Query { rows, .. } => match &rows[0][0] {
            ScalarValue::Int64(v) => *v as u64,
            other => panic!("count not int: {other:?}"),
        },
        other => panic!("expected query, got {other:?}"),
    }
}

fn sum(engine: &Arc<Engine>, expr: &str) -> f64 {
    match engine
        .execute_script(&format!("SELECT SUM({expr}) FROM lineitem"))
        .unwrap()
        .pop()
        .unwrap()
    {
        Outcome::Query { rows, .. } => match &rows[0][0] {
            ScalarValue::Float64(v) => *v,
            ScalarValue::Int64(v) => *v as f64,
            ScalarValue::Null => 0.0,
            other => panic!("sum not numeric: {other:?}"),
        },
        other => panic!("expected query, got {other:?}"),
    }
}

#[test]
fn columnar_snapshot_and_replay_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    // —— 第一段:建表 + 装载 + FLUSH(写快照)——
    {
        let engine = open(&data_dir);
        engine
            .execute_script(
                "CREATE TABLE lineitem (\
                 l_orderkey BIGINT NOT NULL, \
                 l_quantity BIGINT NOT NULL, \
                 l_extendedprice DOUBLE NOT NULL, \
                 l_shipdate DATE NOT NULL)",
            )
            .unwrap();
        engine
            .execute_script(
                "INSERT INTO lineitem VALUES \
                 (1, 10, 100.0, DATE '2024-01-01'), \
                 (2, 20, 200.0, DATE '2024-02-01'), \
                 (3, 30, 300.0, DATE '2024-03-01')",
            )
            .unwrap();
        assert_eq!(count(&engine, "lineitem"), 3);
        flush(&engine);
        assert!(
            snapshot_file(&data_dir, lineitem_id(&engine)).exists(),
            "FLUSH 应写列存快照"
        );
    }

    // —— 第二段:重启(快照加载)→ 快照后变更(仅 LSM,水位重放)→ 重启 ——
    {
        let engine = open(&data_dir);
        let li_id = lineitem_id(&engine);
        assert_eq!(count(&engine, "lineitem"), 3, "快照恢复行数");
        assert_eq!(sum(&engine, "l_quantity"), 60.0, "快照恢复数量列");

        // 快照之后的变更:更新 + 删除 + 新增(全部只落 LSM)
        engine
            .execute_script("UPDATE lineitem SET l_quantity = 99 WHERE l_orderkey = 1")
            .unwrap();
        engine
            .execute_script("DELETE FROM lineitem WHERE l_orderkey = 2")
            .unwrap();
        engine
            .execute_script("INSERT INTO lineitem VALUES (4, 40, 400.0, DATE '2024-04-01')")
            .unwrap();
        assert_eq!(count(&engine, "lineitem"), 3);
    }

    // —— 第三段:重启(快照 + 增量重放)——
    {
        let engine = open(&data_dir);
        let li_id = lineitem_id(&engine);
        assert_eq!(count(&engine, "lineitem"), 3, "重放后行数(2 删 1 增)");
        assert_eq!(
            sum(&engine, "l_quantity"),
            99.0 + 30.0 + 40.0,
            "重放应包含 UPDATE/INSERT,排除 DELETE"
        );
        // AP 列存路径在重启后读数正确
        let mut outcomes = engine
            .execute_script(
                "SELECT l_orderkey, l_shipdate FROM lineitem \
                 WHERE l_shipdate >= DATE '2024-03-01' ORDER BY l_orderkey",
            )
            .unwrap();
        match outcomes.pop().unwrap() {
            Outcome::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![
                        vec![
                            ScalarValue::Int64(3),
                            ScalarValue::Date(common::types::days_from_civil(2024, 3, 1))
                        ],
                        vec![
                            ScalarValue::Int64(4),
                            ScalarValue::Date(common::types::days_from_civil(2024, 4, 1))
                        ],
                    ]
                );
            }
            other => panic!("expected query, got {other:?}"),
        }
    }
}

#[test]
fn no_flush_rebuild_still_correct() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    {
        let engine = open(&data_dir);
        engine
            .execute_script(
                "CREATE TABLE lineitem (l_orderkey BIGINT NOT NULL, l_quantity BIGINT NOT NULL)",
            )
            .unwrap();
        engine
            .execute_script("INSERT INTO lineitem VALUES (1, 10), (2, 20)")
            .unwrap();
        // 不 FLUSH:无快照文件(增量次数远低于阈值)
    }
    {
        let engine = open(&data_dir);
        assert_eq!(count(&engine, "lineitem"), 2, "无快照 → 全量重建路径");
    }
}

#[test]
fn corrupt_snapshot_falls_back_to_rebuild() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    {
        let engine = open(&data_dir);
        engine
            .execute_script(
                "CREATE TABLE lineitem (l_orderkey BIGINT NOT NULL, l_quantity BIGINT NOT NULL)",
            )
            .unwrap();
        engine
            .execute_script("INSERT INTO lineitem VALUES (1, 10), (2, 20), (3, 30)")
            .unwrap();
        flush(&engine);
        assert!(snapshot_file(&data_dir, lineitem_id(&engine)).exists());
    }
    // 损坏快照(翻转中段字节)
    let li_id = {
        let engine = open(&data_dir);
        lineitem_id(&engine)
    };
    let snap = snapshot_file(&data_dir, li_id);
    let mut bytes = std::fs::read(&snap).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&snap, &bytes).unwrap();

    let engine = open(&data_dir);
    assert_eq!(
        count(&engine, "lineitem"),
        3,
        "快照损坏 → 告警 + 全量重建,数据不丢"
    );
}

#[test]
fn drop_table_removes_snapshot_file() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let engine = open(&data_dir);
    engine
        .execute_script(
            "CREATE TABLE lineitem (l_orderkey BIGINT NOT NULL, l_quantity BIGINT NOT NULL)",
        )
        .unwrap();
    engine
        .execute_script("INSERT INTO lineitem VALUES (1, 10)")
        .unwrap();
    flush(&engine);
    let li_id = lineitem_id(&engine);
    assert!(snapshot_file(&data_dir, li_id).exists());
    engine.execute_script("DROP TABLE lineitem").unwrap();
    assert!(
        !snapshot_file(&data_dir, li_id).exists(),
        "DROP 应清理列存快照文件"
    );
}
