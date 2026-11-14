//! CASE WHEN / IN / 非相关子查询 端到端测试(以 TPC-H Q7/Q12/Q18 形状为标准)。

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

fn f(x: f64) -> ScalarValue {
    ScalarValue::Float64(x)
}

fn t(text: &str) -> ScalarValue {
    ScalarValue::Utf8(text.to_string())
}

/// Q7 形状素材:nation(lineitem 的国家标签)+ lineitem(含日期与金额)。
fn load_q7_shape(engine: &Arc<Engine>) {
    engine
        .execute_script(
            "CREATE TABLE lineitem (\
             l_orderkey BIGINT NOT NULL, \
             l_extendedprice DECIMAL(15,2) NOT NULL, \
             l_discount DECIMAL(15,2) NOT NULL, \
             l_shipdate DATE NOT NULL, \
             l_nation TEXT NOT NULL)",
        )
        .unwrap();
    engine
        .execute_script(
            "INSERT INTO lineitem VALUES \
             (1, 100.0, 0.25, DATE '1995-01-15', 'FRANCE'), \
             (2, 200.0, 0.50, DATE '1995-06-01', 'GERMANY'), \
             (3, 300.0, 0.25, DATE '1996-02-01', 'FRANCE'), \
             (4, 400.0, 0.50, DATE '1994-01-01', 'GERMANY'), \
             (5, 500.0, 0.00, DATE '1996-05-01', 'FRANCE')",
        )
        .unwrap();
}

/// Q7 形状:SUM(CASE WHEN 双边国家 THEN extprice*(1-discount) ELSE 0 END)
/// 按年分组(GROUP BY 内含 EXTRACT)。
#[test]
fn q7_case_inside_sum_grouped_by_year() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_q7_shape(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT EXTRACT(YEAR FROM l_shipdate) AS l_year, \
                    SUM(CASE WHEN l_nation = 'FRANCE' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS revenue \
             FROM lineitem \
             WHERE l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' \
             GROUP BY EXTRACT(YEAR FROM l_shipdate) \
             ORDER BY l_year",
        )
        .unwrap();
    // 1995:FRANCE 行 100×0.75 = 75(GERMANY 行 CASE 为 0);1996:300×0.75 + 500 = 725
    assert_eq!(
        rows_text(outcomes),
        vec![vec![s(1995), f(75.0)], vec![s(1996), f(725.0)]]
    );
}

/// Q12 形状:IN 值列表 + CASE 计数聚合(按组统计早于某日期的行数)。
#[test]
fn q12_in_list_with_case_count() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_q7_shape(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT l_nation, COUNT(*) AS line_count, \
                    SUM(CASE WHEN l_shipdate < DATE '1995-06-01' THEN 1 ELSE 0 END) AS early_count \
             FROM lineitem \
             WHERE l_nation IN ('FRANCE', 'GERMANY') \
             GROUP BY l_nation \
             ORDER BY l_nation",
        )
        .unwrap();
    // FRANCE 3 行(早发货 1);GERMANY 2 行(1995-06-01 不严格早于 → 早发货 1)
    assert_eq!(
        rows_text(outcomes),
        vec![
            vec![t("FRANCE"), s(3), s(1)],
            vec![t("GERMANY"), s(2), s(1)],
        ]
    );
}

/// Q18 形状:IN 子查询(分组聚合 + HAVING 的结果集)作为外层过滤。
#[test]
fn q18_in_subquery_with_group_having() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_q7_shape(&engine);
    // 追加一行使 orderkey 1 成为"大订单"(行数 > 1)
    engine
        .execute_script("INSERT INTO lineitem VALUES (1, 50.0, 0.00, DATE '1995-03-01', 'GERMANY')")
        .unwrap();
    let outcomes = engine
        .execute_script(
            "SELECT SUM(l_extendedprice) AS total, COUNT(*) AS n \
             FROM lineitem \
             WHERE l_orderkey IN (\
                 SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING COUNT(*) > 1\
             )",
        )
        .unwrap();
    // 仅 orderkey 1 满足 HAVING → 100 + 50 = 150,共 2 行
    assert_eq!(rows_text(outcomes), vec![vec![f(150.0), s(2)]]);
}

/// 标量子查询:WHERE 比较与 HAVING 阈值。
#[test]
fn scalar_subquery_in_where_and_having() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_q7_shape(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT COUNT(*) AS n FROM lineitem \
             WHERE l_extendedprice > (SELECT AVG(l_extendedprice) FROM lineitem)",
        )
        .unwrap();
    // AVG = 300;> 300 的有 2 行(400/500)
    assert_eq!(rows_text(outcomes), vec![vec![s(2)]]);
    let outcomes2 = engine
        .execute_script(
            "SELECT l_nation FROM lineitem GROUP BY l_nation \
             HAVING SUM(l_extendedprice) > (SELECT AVG(l_extendedprice) FROM lineitem) \
             ORDER BY l_nation",
        )
        .unwrap();
    // 各国总额 FRANCE 900 / GERMANY 600,均 > 300
    assert_eq!(
        rows_text(outcomes2),
        vec![vec![t("FRANCE")], vec![t("GERMANY")]]
    );
}

/// EXISTS / NOT EXISTS:非相关子查询物化为布尔。
#[test]
fn exists_subquery_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_q7_shape(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT COUNT(*) AS n FROM lineitem \
             WHERE EXISTS (SELECT l_orderkey FROM lineitem WHERE l_nation = 'FRANCE')",
        )
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(5)]]);
    let outcomes2 = engine
        .execute_script(
            "SELECT COUNT(*) AS n FROM lineitem \
             WHERE NOT EXISTS (SELECT l_orderkey FROM lineitem WHERE l_nation = 'JAPAN')",
        )
        .unwrap();
    assert_eq!(rows_text(outcomes2), vec![vec![s(5)]]);
}

// ---------- 谓词下推 + AP 列存路径(读路径正确性) ----------

/// 列存路径(全标量表,自动提交):SELECT/WHERE/GROUP BY 走列式读取,
/// 结果与行存一致(含 NULL 语义 —— 列存位图)。
#[test]
fn columnar_path_matches_row_store() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script(
            "CREATE TABLE ap_t (id BIGINT NOT NULL, grp TEXT NOT NULL, val DOUBLE, d DATE)",
        )
        .unwrap();
    engine
        .execute_script(
            "INSERT INTO ap_t VALUES \
             (1, 'a', 1.5, DATE '2024-01-01'), \
             (2, 'b', NULL, DATE '2024-01-02'), \
             (3, 'a', 3.0, NULL), \
             (4, 'a', NULL, NULL)",
        )
        .unwrap();
    // 全标量表 → 列存路径;过滤 + NULL 语义
    let outcomes = engine
        .execute_script(
            "SELECT grp, COUNT(val) AS n, SUM(val) AS s FROM ap_t WHERE id >= 2 GROUP BY grp ORDER BY grp",
        )
        .unwrap();
    assert_eq!(
        rows_text(outcomes),
        vec![
            vec![t("a"), s(1), f(3.0)],
            vec![t("b"), s(0), ScalarValue::Null],
        ]
    );
    // 点查(谓词下推到解码/列块)
    let outcomes = engine
        .execute_script("SELECT val FROM ap_t WHERE d = DATE '2024-01-02'")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![ScalarValue::Null]]);
}

/// 列存回退行存:事务内(副本看不到本事务写缓冲)读己之写仍正确。
#[test]
fn txn_reads_fall_back_to_row_store() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE ap_t2 (id BIGINT NOT NULL, v BIGINT NOT NULL)")
        .unwrap();
    engine
        .execute_script("INSERT INTO ap_t2 VALUES (1, 10)")
        .unwrap();
    // BEGIN 内写入(列存副本不含)→ 读己之写必须走行存回退
    let mut sess = server::session::Session::new(engine.clone());
    sess.execute_script("BEGIN").unwrap();
    sess.execute_script("INSERT INTO ap_t2 VALUES (2, 20)")
        .unwrap();
    let outcomes = sess.execute_script("SELECT SUM(v) FROM ap_t2").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(30)]], "读己之写");
    sess.execute_script("COMMIT").unwrap();
    let outcomes = engine.execute_script("SELECT SUM(v) FROM ap_t2").unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(30)]], "提交后可见");
}

/// LOB 列(不可列式)自动回退行存,行为不变。
#[test]
fn lob_columns_fall_back() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE lob_t (id BIGINT NOT NULL, blob BYTEA NOT NULL)")
        .unwrap();
    engine
        .execute_script("INSERT INTO lob_t VALUES (1, '\\xDEADBEEF')")
        .unwrap();
    let outcomes = engine
        .execute_script("SELECT COUNT(*) FROM lob_t WHERE id = 1")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(1)]]);
}
