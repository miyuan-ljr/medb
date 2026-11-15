//! 列存 segment 持久化 e2e:flush 写段 + 重启段发现 + memtable overlay。
//!
//! 架构约定:segment 只覆盖 SST 数据;memtable 增量由 AP 读路径 overlay
//! 合并(WAL 重放后同样成立)。场景:
//! 1. 建表 + 装载 → FLUSH(写 segment)→ 重启:段发现,读数正确;
//! 2. 段之后 UPDATE/DELETE/INSERT(仅 memtable/WAL)→ 重启:overlay 正确;
//! 3. 损坏 segment → AP 查询回退行存,数据不丢;
//! 4. DROP TABLE 清理 segment 文件;
//! 5. 不 FLUSH 直接重启(无段)→ 行存路径正确。

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

/// FLUSH TABLES 是 Session 级命令(与协议层一致)。
fn flush(engine: &Arc<Engine>) {
    server::session::Session::new(engine.clone())
        .execute_script("FLUSH TABLES")
        .unwrap();
}

fn lineitem_id(engine: &Arc<Engine>) -> u64 {
    engine.catalog().get_table("lineitem").unwrap().id
}

/// 该表当前 segment 文件(至多一份;旧代在重建时清理)。
fn segment_file(dir: &Path, table_id: u64) -> Option<PathBuf> {
    let cdir = dir.join("columnar");
    let prefix = format!("{table_id}.");
    std::fs::read_dir(cdir)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with(&prefix) && n.ends_with(".seg"))
                .unwrap_or(false)
        })
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

fn sum(engine: &Arc<Engine>, table: &str, expr: &str) -> f64 {
    match engine
        .execute_script(&format!("SELECT SUM({expr}) FROM {table}"))
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
fn columnar_segment_and_overlay_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    // —— 第一段:建表 + 装载 + FLUSH(写 segment)——
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
        let li_id = lineitem_id(&engine);
        assert!(
            segment_file(&data_dir, li_id).is_some(),
            "FLUSH 应写列存 segment"
        );
    }

    // —— 第二段:重启(段发现)→ 段后变更(仅 memtable,overlay 合并)→ 重启 ——
    {
        let engine = open(&data_dir);
        assert_eq!(count(&engine, "lineitem"), 3, "段发现恢复行数");
        assert_eq!(sum(&engine, "lineitem", "l_quantity"), 60.0, "段恢复数量列");

        // 段之后的变更:更新 + 删除 + 新增(全部只进 memtable/WAL)
        engine
            .execute_script("UPDATE lineitem SET l_quantity = 99 WHERE l_orderkey = 1")
            .unwrap();
        engine
            .execute_script("DELETE FROM lineitem WHERE l_orderkey = 2")
            .unwrap();
        engine
            .execute_script("INSERT INTO lineitem VALUES (4, 40, 400.0, DATE '2024-04-01')")
            .unwrap();
        assert_eq!(count(&engine, "lineitem"), 3, "overlay 后行数(1 改 1 删 1 增)");
        assert_eq!(
            sum(&engine, "lineitem", "l_quantity"),
            99.0 + 30.0 + 40.0,
            "overlay 应包含 UPDATE/INSERT,排除 DELETE"
        );
    }

    // —— 第三段:重启(WAL 重放 memtable → overlay 依旧成立)——
    {
        let engine = open(&data_dir);
        assert_eq!(count(&engine, "lineitem"), 3, "重启后 overlay 行数");
        assert_eq!(sum(&engine, "lineitem", "l_quantity"), 99.0 + 30.0 + 40.0);
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
fn overlay_hits_segment_after_in_process_flush() {
    // flush 钩子:进程内 FLUSH 后 segment 立即包含 SST 数据;
    // 后续写仍在 memtable → overlay 合并,读数始终一致。
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE lineitem (l_orderkey BIGINT NOT NULL, l_quantity BIGINT NOT NULL)")
        .unwrap();
    engine
        .execute_script("INSERT INTO lineitem VALUES (1, 10), (2, 20)")
        .unwrap();
    flush(&engine);
    assert_eq!(count(&engine, "lineitem"), 2, "flush 后段 + 空 overlay");
    engine
        .execute_script("INSERT INTO lineitem VALUES (3, 30)")
        .unwrap();
    engine
        .execute_script("UPDATE lineitem SET l_quantity = 99 WHERE l_orderkey = 1")
        .unwrap();
    assert_eq!(count(&engine, "lineitem"), 3, "段 + memtable overlay");
    assert_eq!(sum(&engine, "lineitem", "l_quantity"), 99.0 + 20.0 + 30.0);
}

#[test]
fn corrupt_segment_falls_back_to_row_store() {
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
    }
    // 损坏 segment(翻转中段字节)
    let seg = {
        let engine = open(&data_dir);
        let id = lineitem_id(&engine);
        segment_file(&data_dir, id).unwrap()
    };
    let mut bytes = std::fs::read(&seg).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&seg, &bytes).unwrap();

    let engine = open(&data_dir);
    assert_eq!(
        count(&engine, "lineitem"),
        3,
        "段损坏 → 告警 + 行存回退,数据不丢"
    );
}

#[test]
fn drop_table_removes_segment_file() {
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
    assert!(segment_file(&data_dir, li_id).is_some());
    engine.execute_script("DROP TABLE lineitem").unwrap();
    assert!(
        segment_file(&data_dir, li_id).is_none(),
        "DROP 应清理列存 segment 文件"
    );
}

#[test]
fn stale_segment_detected_at_open_crash_window() {
    // 模拟 crash 窗口:flush 完成(L0 SST 已含数据)但 segment 重建尚未发生
    // → 把 segment 文件名的 max_seq 改小,让打开期检测判定"段过期"。
    // 期望:AP 回退行存,读数仍正确(派生数据允许过期,不允许错)。
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
    }
    let seg = {
        let engine = open(&data_dir);
        let id = lineitem_id(&engine);
        let seg = segment_file(&data_dir, id).unwrap();
        // 伪造陈旧:文件名 max_seq 改小(SST 的真实 max_seq 必然更大)
        let name = seg.file_name().unwrap().to_str().unwrap().to_string();
        let stem = name.strip_suffix(".seg").unwrap();
        let mut parts: Vec<String> = stem.split('.').map(String::from).collect();
        let real_max: u64 = parts[2].parse().unwrap();
        assert!(real_max > 0, "测试前置:flush 后 max_seq 应 > 0");
        parts[2] = (real_max - 1).to_string();
        let forged = format!("{}.seg", parts.join("."));
        std::fs::rename(&seg, seg.with_file_name(forged)).unwrap();
        seg
    };
    let engine = open(&data_dir);
    // 检测后旧文件应被无视(行存回退);数据完整
    assert_eq!(
        count(&engine, "lineitem"),
        3,
        "段陈旧 → 行存回退,数据不丢"
    );
    // 下次 flush 重建后段恢复且代次继续增长(旧伪造文件被清理)
    let id = lineitem_id(&engine);
    engine
        .execute_script("INSERT INTO lineitem VALUES (4, 40)")
        .unwrap();
    flush(&engine);
    let new_seg = segment_file(&data_dir, id).expect("flush 后应有新段");
    assert_ne!(new_seg, seg, "重建应产生新代文件");
    assert_eq!(count(&engine, "lineitem"), 4);
}

#[test]
fn ap_multi_chunk_delta_semantics() {
    // memtable 增量 = 向量化 delta 块(segment 块 + delta 块拼接):
    // update/delete 打影子剔除 segment 行,insert 落 delta 块;
    // 谓词/排序跨块求值,结果与行存路径一致。
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script(
            "CREATE TABLE lineitem (\
             l_orderkey BIGINT NOT NULL PRIMARY KEY, \
             l_quantity BIGINT NOT NULL, \
             l_shipdate DATE NOT NULL)",
        )
        .unwrap();
    engine
        .execute_script(
            "INSERT INTO lineitem VALUES \
             (1, 10, DATE '2024-01-01'), \
             (2, 20, DATE '2024-02-01'), \
             (3, 30, DATE '2024-03-01'), \
             (4, 40, DATE '2024-04-01'), \
             (5, 50, DATE '2024-05-01')",
        )
        .unwrap();
    flush(&engine);
    // segment 块 = 5 行;memtable delta:update 1 行 + delete 1 行 + insert 2 行
    engine
        .execute_script("UPDATE lineitem SET l_quantity = 99 WHERE l_orderkey = 3")
        .unwrap();
    engine
        .execute_script("DELETE FROM lineitem WHERE l_orderkey = 4")
        .unwrap();
    engine
        .execute_script(
            "INSERT INTO lineitem VALUES \
             (6, 60, DATE '2024-06-01'), \
             (7, 70, DATE '2024-07-01')",
        )
        .unwrap();

    // 拼接语义:6 行(5 - 1 删 + 2 增),被更新的行只出现一次(新值)
    assert_eq!(count(&engine, "lineitem"), 6);
    assert_eq!(
        sum(&engine, "lineitem", "l_quantity"),
        10.0 + 20.0 + 99.0 + 50.0 + 60.0 + 70.0
    );

    // 谓词跨两块求值:segment 块命中 (1,5) + delta 块命中 (6,7),排除影子 (3,4)
    let mut outcomes = engine
        .execute_script(
            "SELECT l_orderkey FROM lineitem \
             WHERE l_shipdate >= DATE '2024-01-15' AND l_shipdate < DATE '2024-06-15' \
             ORDER BY l_orderkey",
        )
        .unwrap();
    match outcomes.pop().unwrap() {
        Outcome::Query { rows, .. } => assert_eq!(
            rows,
            vec![
                vec![ScalarValue::Int64(2)],
                vec![ScalarValue::Int64(3)], // update 后仍在段内键序位,新值由 delta 块提供
                vec![ScalarValue::Int64(5)],
                vec![ScalarValue::Int64(6)],
            ]
        ),
        other => panic!("expected query, got {other:?}"),
    }

    // ORDER BY DESC + LIMIT 跨块
    let mut outcomes = engine
        .execute_script(
            "SELECT l_orderkey, l_quantity FROM lineitem \
             ORDER BY l_orderkey DESC LIMIT 2",
        )
        .unwrap();
    match outcomes.pop().unwrap() {
        Outcome::Query { rows, .. } => assert_eq!(
            rows,
            vec![
                vec![ScalarValue::Int64(7), ScalarValue::Int64(70)],
                vec![ScalarValue::Int64(6), ScalarValue::Int64(60)],
            ]
        ),
        other => panic!("expected query, got {other:?}"),
    }
}

#[test]
fn ap_multi_batch_split_across_chunks() {
    // segment 块 > batch_size(默认 2048)+ delta 块 → 执行器逐块切批交付,
    // 聚合结果与单块一致。
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL)")
        .unwrap();
    let mut values = String::new();
    for i in 0..5000u64 {
        if i > 0 {
            values.push_str(", ");
        }
        values.push_str(&format!("({i}, {i})"));
    }
    engine
        .execute_script(&format!("INSERT INTO t VALUES {values}"))
        .unwrap();
    flush(&engine);
    engine.execute_script("INSERT INTO t VALUES (9999, 9999)").unwrap();
    assert_eq!(count(&engine, "t"), 5001);
    assert_eq!(sum(&engine, "t", "b"), (0..5000u64).sum::<u64>() as f64 + 9999.0);
    // 谓词跨批:只命中 delta 块一行
    let mut outcomes = engine
        .execute_script("SELECT a FROM t WHERE b = 9999")
        .unwrap();
    match outcomes.pop().unwrap() {
        Outcome::Query { rows, .. } => assert_eq!(rows, vec![vec![ScalarValue::Int64(9999)]]),
        other => panic!("expected query, got {other:?}"),
    }
}

#[test]
fn no_flush_row_store_still_correct() {
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
        // 不 FLUSH:无 segment 文件
        assert!(segment_file(&data_dir, lineitem_id(&engine)).is_none());
    }
    {
        let engine = open(&data_dir);
        assert_eq!(count(&engine, "lineitem"), 2, "无段 → 行存路径");
    }
}
