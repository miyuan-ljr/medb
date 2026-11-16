//! 并发健壮性测试:DDL / DML / DQL 任意交错 + 重启后建表的 id 分配。
//!
//! 断言的底线:
//! - 任何交错不得 panic / 死锁 / 越过内存限额崩溃;
//! - 语句级错误必须是"预期类"(表不存在 / 唯一冲突等目录或约束错误);
//! - 并发同 PK 插入最终最多一行存活;
//! - ALTER 重写期间并发 DELETE 的行不得复活(见 resurrect 断言)。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use common::config::Config;
use common::error::Error;
use server::engine::{Engine, Outcome};
use server::session::Session;

fn open(dir: &std::path::Path) -> Arc<Engine> {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    Engine::open(config).unwrap()
}

/// 并发下允许的"预期错误":目标消失/重复/约束。其余一律视为 bug。
fn expected_error(e: &Error) -> bool {
    let s = e.to_string();
    s.contains("not found")
        || s.contains("already exists")
        || s.contains("duplicate key")
        || s.contains("ambiguous")
        || s.contains("cannot run inside a transaction block")
        || s.contains("NOT NULL")
}

fn setup(engine: &Arc<Engine>) {
    engine
        .execute_script(
            "CREATE TABLE kv (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, tag TEXT NOT NULL)",
        )
        .unwrap();
    let mut script = String::new();
    for i in 0..200 {
        script.push_str(&format!("INSERT INTO kv VALUES ({}, {}, 'init');\n", i, i * 10));
    }
    engine.execute_script(&script).unwrap();
}

/// ① 混合负载压测:DML ×4 + DQL ×4 + DDL ×2 + 事务 ×1 同时跑,
/// 全员无 panic、错误都在预期类;结束后表仍然可用且 PK 唯一。
#[test]
fn mixed_ddl_dml_dql_stress() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    setup(&engine);
    let stop = Arc::new(AtomicU64::new(0));
    let errors: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let mut handles = Vec::new();

    // DML ×4:插入新行 / 更新 / 删除再插回(自增 id 段互不重叠)
    for w in 0..4u64 {
        let engine = engine.clone();
        let stop = stop.clone();
        let errors = errors.clone();
        handles.push(std::thread::spawn(move || {
            let mut i = 10_000u64 + w * 5_000;
            let mut n = 0u64;
            while stop.load(Ordering::Relaxed) == 0 && n < 60 {
                let id = i;
                let r = engine.execute_script(&format!(
                    "INSERT INTO kv VALUES ({id}, {v}, 'w{w}');",
                    v = id % 97
                ));
                if let Err(e) = r {
                    if !expected_error(&e) {
                        errors.lock().unwrap().push(format!("insert: {e}"));
                    }
                }
                if n % 3 == 0 {
                    if let Err(e) = engine
                        .execute_script(&format!("DELETE FROM kv WHERE id = {id}"))
                    {
                        if !expected_error(&e) {
                            errors.lock().unwrap().push(format!("delete: {e}"));
                        }
                    }
                }
                if n % 5 == 0 {
                    if let Err(e) = engine.execute_script(&format!(
                        "UPDATE kv SET v = v + 1 WHERE id = {}",
                        200 + (n % 150)
                    )) {
                        if !expected_error(&e) {
                            errors.lock().unwrap().push(format!("update: {e}"));
                        }
                    }
                }
                i += 1;
                n += 1;
            }
        }));
    }

    // DQL ×4:聚合 / 点查 / JOIN / 全扫,全部断言成功
    for _ in 0..4 {
        let engine = engine.clone();
        let stop = stop.clone();
        let errors = errors.clone();
        handles.push(std::thread::spawn(move || {
            let qs = [
                "SELECT l.tag, COUNT(*) FROM kv l GROUP BY l.tag",
                "SELECT v FROM kv WHERE id = 42",
                "SELECT COUNT(*) FROM kv a, kv b WHERE a.id = b.id",
                "SELECT SUM(v) FROM kv WHERE id < 100",
            ];
            while stop.load(Ordering::Relaxed) == 0 {
                for q in qs {
                    if let Err(e) = engine.execute_script(q) {
                        if !expected_error(&e) {
                            errors.lock().unwrap().push(format!("query '{q}': {e}"));
                            return;
                        }
                    }
                }
            }
        }));
    }

    // DDL ×2:建/删辅助表 + 加列/建索引/删索引 循环
    for w in 0..2u64 {
        let engine = engine.clone();
        let stop = stop.clone();
        let errors = errors.clone();
        handles.push(std::thread::spawn(move || {
            let mut n = 0u64;
            while stop.load(Ordering::Relaxed) == 0 && n < 25 {
                let ddl = [
                    format!("CREATE TABLE tmp{w}_{n} (k BIGINT NOT NULL, s TEXT)"),
                    format!("INSERT INTO tmp{w}_{n} VALUES (1, 'x')"),
                    format!("ALTER TABLE tmp{w}_{n} ADD COLUMN extra TEXT"),
                    format!("CREATE INDEX idx_tmp{w}_{n} ON tmp{w}_{n} (k)"),
                    format!("DROP INDEX idx_tmp{w}_{n}"),
                    format!("DROP TABLE tmp{w}_{n}"),
                ];
                for stmt in ddl {
                    if let Err(e) = engine.execute_script(&stmt) {
                        if !expected_error(&e) {
                            errors.lock().unwrap().push(format!("ddl '{stmt}': {e}"));
                        }
                    }
                }
                n += 1;
            }
        }));
    }

    // 事务 ×1:Session 层走 BEGIN/写/COMMIT 与 BEGIN/写/ROLLBACK 交替
    let engine2 = engine.clone();
    let errors2 = errors.clone();
    handles.push(std::thread::spawn(move || {
        let mut session = Session::new(engine2);
        let mut i = 50_000u64;
        for n in 0..30 {
            let r = if n % 2 == 0 {
                session
                    .execute_prepared(
                        &format!("INSERT INTO kv VALUES ({i}, 1, 'txn')"),
                        &[],
                    )
                    .map(|_| Outcome::Tag {
                        prefix: "INSERT".into(),
                        rows: None,
                    })
            } else {
                // BEGIN → 写 → ROLLBACK:回滚后行不可见
                session
                    .execute_script(&format!(
                        "BEGIN;INSERT INTO kv VALUES ({i}, 2, 'rollback');ROLLBACK;"
                    ))
                    .map(|_| Outcome::Tag {
                        prefix: "TXN".into(),
                        rows: None,
                    })
            };
            if let Err(e) = r {
                if !expected_error(&e) {
                    errors2.lock().unwrap().push(format!("txn: {e}"));
                }
            }
            i += 1;
        }
    }));

    // 主线程等 3 秒后放行
    std::thread::sleep(std::time::Duration::from_secs(3));
    stop.store(1, Ordering::Relaxed);
    for h in handles {
        h.join().expect("worker thread panicked");
    }

    let errs = errors.lock().unwrap();
    assert!(errs.is_empty(), "并发错误 {}: {:?}", errs.len(), &errs[..errs.len().min(5)]);

    // 结束后:PK 唯一性(按 id 分组计数最大值 = 1)
    let outcomes = engine
        .execute_script("SELECT id, COUNT(*) AS c FROM kv GROUP BY id ORDER BY c DESC LIMIT 1")
        .unwrap();
    match outcomes.into_iter().next() {
        Some(Outcome::Query { rows, .. }) => {
            let max_dup = match &rows[0][1] {
                common::types::ScalarValue::Int64(v) => *v,
                other => panic!("unexpected {other:?}"),
            };
            assert_eq!(max_dup, 1, "PK 冲突:重复最多的 id 有 {max_dup} 行");
        }
        _ => panic!("no result"),
    }
}

/// ② ALTER 重写期间并发 DELETE:已确认删除的行不得复活。
/// (ALTER 的"读旧行 → 重写"若不感知并发删除墓碑,重写批次会盖掉墓碑。)
#[test]
fn alter_concurrent_delete_no_resurrect() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)")
        .unwrap();
    let mut script = String::new();
    for i in 0..500 {
        script.push_str(&format!("INSERT INTO t VALUES ({i}, {i});\n"));
    }
    engine.execute_script(&script).unwrap();

    let victim = 7u64; // 要删除的行
    let e2 = engine.clone();
    let deleter = std::thread::spawn(move || e2.execute_script(&format!("DELETE FROM t WHERE id = {victim}")));

    // 与删除并发发起 ALTER(读 500 行 → 重写 500 行)
    let alter = engine.execute_script("ALTER TABLE t ADD COLUMN extra TEXT");
    let del = deleter.join().expect("deleter panicked");
    del.unwrap();
    alter.unwrap(); // 允许成功;失败也必须是预期类

    // 删除已确认 → 任何时刻之后该行都不应再可见
    engine.flush().unwrap();
    let outcomes = engine
        .execute_script(&format!("SELECT COUNT(*) FROM t WHERE id = {victim}"))
        .unwrap();
    match outcomes.into_iter().next() {
        Some(Outcome::Query { rows, .. }) => {
            let n = match &rows[0][0] {
                common::types::ScalarValue::Int64(v) => *v,
                other => panic!("unexpected {other:?}"),
            };
            assert_eq!(n, 0, "已删除的行复活了(DELETE 被 ALTER 重写覆盖)");
        }
        _ => panic!("no result"),
    }
}

/// ③ 重启后建新表:表 id 不得与恢复表撞车(行区间按 id 划分)。
#[test]
fn new_table_after_restart_no_id_collision() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        engine
            .execute_script("CREATE TABLE a (k BIGINT NOT NULL, v TEXT NOT NULL)")
            .unwrap();
        engine
            .execute_script("INSERT INTO a VALUES (1, 'a1'), (2, 'a2')")
            .unwrap();
    }
    {
        let engine = open(dir.path());
        // 重启后新建表 —— id_gen 若未推进,会拿到与表 a 相同的 id
        engine
            .execute_script("CREATE TABLE b (k BIGINT NOT NULL, v TEXT NOT NULL)")
            .unwrap();
        engine
            .execute_script("INSERT INTO b VALUES (100, 'b100'), (101, 'b101')")
            .unwrap();
        let from_b = engine.execute_script("SELECT v FROM b ORDER BY v").unwrap();
        let rows = match from_b.into_iter().next() {
            Some(Outcome::Query { rows, .. }) => rows,
            _ => panic!("no result"),
        };
        let texts: Vec<String> = rows
            .iter()
            .map(|r| match &r[0] {
                common::types::ScalarValue::Utf8(s) => s.clone(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(texts, vec!["b100", "b101"], "表 b 读到了别人的数据(id 撞车)");
        // 反向:a 也不能看到 b 的行
        let from_a = engine.execute_script("SELECT v FROM a ORDER BY v").unwrap();
        let rows = match from_a.into_iter().next() {
            Some(Outcome::Query { rows, .. }) => rows,
            _ => panic!("no result"),
        };
        let texts: Vec<String> = rows
            .iter()
            .map(|r| match &r[0] {
                common::types::ScalarValue::Utf8(s) => s.clone(),
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        assert_eq!(texts, vec!["a1", "a2"], "表 a 读到了表 b 的数据(id 撞车)");
    }
}
