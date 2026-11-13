//! TPC-H 形状集成测试:以 Q1/Q3/Q6 的简化形态为验收标准,
//! 覆盖 DATE 字面量与比较、DECIMAL→Float64、多表 JOIN(哈希等值 +
//! 非等值 ON 残余)、LEFT JOIN NULL 补齐、WHERE 条件下推路由、
//! LIKE / SUBSTRING / EXTRACT / INTERVAL 日期算术。

use std::sync::Arc;

use common::config::Config;
use common::types::{ScalarValue, days_from_civil};
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

fn d(y: i32, m: u32, dd: u32) -> ScalarValue {
    ScalarValue::Date(days_from_civil(y, m, dd))
}

/// 建表 + 装载 TPC-H 子集数据(customer/orders/lineitem)。
fn load_tpch_subset(engine: &Arc<Engine>) {
    engine
        .execute_script(
            "CREATE TABLE customer (\
             c_custkey BIGINT NOT NULL PRIMARY KEY, \
             c_name TEXT NOT NULL, \
             c_mktsegment TEXT NOT NULL)",
        )
        .unwrap();
    engine
        .execute_script(
            "CREATE TABLE orders (\
             o_orderkey BIGINT NOT NULL PRIMARY KEY, \
             o_custkey BIGINT NOT NULL, \
             o_orderdate DATE NOT NULL)",
        )
        .unwrap();
    engine
        .execute_script(
            "CREATE TABLE lineitem (\
             l_orderkey BIGINT NOT NULL, \
             l_partkey BIGINT NOT NULL, \
             l_quantity BIGINT NOT NULL, \
             l_extendedprice DECIMAL(15,2) NOT NULL, \
             l_discount DECIMAL(15,2) NOT NULL, \
             l_returnflag CHAR(1) NOT NULL, \
             l_shipdate DATE NOT NULL)",
        )
        .unwrap();
    engine
        .execute_script(
            "INSERT INTO customer VALUES \
             (1, 'Customer#000000001', 'BUILDING'), \
             (2, 'Customer#000000002', 'AUTOMOBILE'), \
             (3, 'Customer#000000003', 'BUILDING'), \
             (4, 'Customer#000000004', 'MACHINERY')",
        )
        .unwrap();
    engine
        .execute_script(
            "INSERT INTO orders VALUES \
             (1000, 1, DATE '1995-03-05'), \
             (2000, 2, DATE '1996-12-31'), \
             (3000, 3, DATE '1994-07-01'), \
             (4000, 9, DATE '1996-01-02')",
        )
        .unwrap();
    engine
        .execute_script(
            "INSERT INTO lineitem VALUES \
             (1000, 11, 15, 100.0, 0.25, 'R', DATE '1995-03-10'), \
             (1000, 12, 20, 200.0, 0.50, 'A', DATE '1995-04-01'), \
             (2000, 21, 30, 300.0, 0.25, 'A', DATE '1997-01-05'), \
             (3000, 31, 10, 50.0, 0.25, 'R', DATE '1994-07-15'), \
             (3000, 32, 6, 16.0, 0.50, 'A', DATE '1994-09-09'), \
             (4000, 41, 5, 25.0, 0.00, 'N', DATE '1996-01-10')",
        )
        .unwrap();
}

/// Q6 形状:单表多谓词过滤 + 表达式聚合(促销收入)。
/// 命中行:(3000,31) 50×0.25 = 12.5;(3000,32) 16×0.5 = 8.0 → 20.5。
#[test]
fn q6_style_filter_and_expression_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_tpch_subset(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT SUM(l_extendedprice * l_discount) AS revenue \
             FROM lineitem \
             WHERE l_shipdate >= DATE '1994-01-01' \
               AND l_shipdate < DATE '1995-01-01' \
               AND l_discount >= 0.20 AND l_discount <= 0.60 \
               AND l_quantity < 15",
        )
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![f(20.5)]]);
}

/// Q1 形状:GROUP BY + 多聚合 + ORDER BY 输出列名。
#[test]
fn q1_style_group_by_and_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_tpch_subset(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT l_returnflag, COUNT(*) AS cnt, SUM(l_quantity) AS sum_qty, \
                    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price \
             FROM lineitem \
             WHERE l_shipdate <= DATE '1997-02-01' \
             GROUP BY l_returnflag \
             ORDER BY l_returnflag",
        )
        .unwrap();
    assert_eq!(
        rows_text(outcomes),
        vec![
            vec![t("A"), s(3), s(56), f(333.0)],
            vec![t("N"), s(1), s(5), f(25.0)],
            vec![t("R"), s(2), s(25), f(112.5)],
        ]
    );
}

/// Q3 形状:三表 JOIN + 各表谓词下推 + 分组聚合 + ORDER BY DESC + LIMIT。
#[test]
fn q3_style_three_table_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_tpch_subset(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT l.l_orderkey AS order_key, \
                    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, \
                    o.o_orderdate AS order_date \
             FROM customer c \
             JOIN orders o ON c.c_custkey = o.o_custkey \
             JOIN lineitem l ON l.l_orderkey = o.o_orderkey \
             WHERE c.c_mktsegment = 'BUILDING' \
               AND o.o_orderdate < DATE '1995-03-15' \
               AND l.l_shipdate > DATE '1995-03-15' \
             GROUP BY l.l_orderkey, o.o_orderdate \
             ORDER BY revenue DESC, order_key \
             LIMIT 3",
        )
        .unwrap();
    // 仅 order 1000 的 (1000,12) 行满足全部条件:200×0.5 = 100.0
    assert_eq!(
        rows_text(outcomes),
        vec![vec![s(1000), f(100.0), d(1995, 3, 5)]]
    );
}

/// LEFT JOIN:未命中行保留、右列补 NULL;悬空订单不出现。
#[test]
fn left_join_preserves_unmatched_left_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_tpch_subset(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT c_custkey, o_orderkey \
             FROM customer c LEFT JOIN orders o ON c.c_custkey = o.o_custkey \
             ORDER BY c_custkey, o_orderkey",
        )
        .unwrap();
    assert_eq!(
        rows_text(outcomes),
        vec![
            vec![s(1), s(1000)],
            vec![s(2), s(2000)],
            vec![s(3), s(3000)],
            vec![s(4), ScalarValue::Null],
        ]
    );
}

/// LEFT JOIN ON 非等值残余:匹配期判定,未过条件的左行仍 NULL 补齐;
/// WHERE(保留侧条件)在连接后过滤。
#[test]
fn left_join_on_residual_and_where_routing() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_tpch_subset(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT c.c_custkey, o.o_orderkey \
             FROM customer c LEFT JOIN orders o \
               ON c.c_custkey = o.o_custkey AND o.o_orderdate >= DATE '1996-01-01' \
             WHERE c.c_custkey <= 3 \
             ORDER BY c.c_custkey",
        )
        .unwrap();
    assert_eq!(
        rows_text(outcomes),
        vec![
            vec![s(1), ScalarValue::Null],
            vec![s(2), s(2000)],
            vec![s(3), ScalarValue::Null],
        ]
    );
}

/// 标量函数与 LIKE:UPPER / SUBSTRING / EXTRACT(YEAR)。
#[test]
fn functions_and_like_over_join() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_tpch_subset(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT UPPER(c_mktsegment) AS seg, \
                    SUBSTRING(c_name FROM 1 FOR 8) AS prefix, \
                    EXTRACT(YEAR FROM o_orderdate) AS yr \
             FROM customer JOIN orders ON c_custkey = o_custkey \
             WHERE c_name LIKE 'Customer#%' \
             ORDER BY yr, prefix \
             LIMIT 2",
        )
        .unwrap();
    assert_eq!(
        rows_text(outcomes),
        vec![
            vec![t("BUILDING"), t("Customer"), s(1994)],
            vec![t("BUILDING"), t("Customer"), s(1995)],
        ]
    );
}

/// INTERVAL 日期算术:DATE 字面量减 180 天作为过滤边界(1995-12-03)。
#[test]
fn date_arithmetic_with_interval() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_tpch_subset(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT o_orderkey FROM orders \
             WHERE o_orderdate >= DATE '1996-06-01' - INTERVAL '180' DAY \
             ORDER BY o_orderkey",
        )
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(2000)], vec![s(4000)]]);
}

/// Date 列与普通字符串字面量比较(隐式解析为日期)。
#[test]
fn date_column_compared_with_string_literal() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_tpch_subset(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT o_orderkey FROM orders WHERE o_orderdate < '1996-01-01' \
             ORDER BY o_orderkey",
        )
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(1000)], vec![s(3000)]]);
}

/// DECIMAL(p,s) 列类型映射为 Float64。
#[test]
fn decimal_column_maps_to_float64() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine
        .execute_script("CREATE TABLE dec_t (id BIGINT, amount DECIMAL(15,2))")
        .unwrap();
    engine
        .execute_script("INSERT INTO dec_t VALUES (1, 12345.67)")
        .unwrap();
    let outcomes = engine
        .execute_script("SELECT id, amount FROM dec_t")
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(1), f(12345.67)]]);
}

/// JOIN 计数:内连接丢弃悬空侧订单(4000 无客户)。
#[test]
fn inner_join_drops_dangling_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    load_tpch_subset(&engine);
    let outcomes = engine
        .execute_script(
            "SELECT COUNT(*) FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey",
        )
        .unwrap();
    assert_eq!(rows_text(outcomes), vec![vec![s(3)]]);
}
