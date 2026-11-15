//! TPC-H 性能基准(默认 #[ignore],显式运行):
//!   cargo test -p medb-server --test tpch_bench --release -- --ignored --nocapture
//!
//! SF 0.1:lineitem 6 万 / partsupp 8 万 / part 2 万 / customer 1.5 万 /
//! orders 1.5 万 / supplier 2000 / nation 25 / region 5。
//! 数据由确定性伪随机生成(可复现),覆盖当前支持的 11 条查询
//! (Q1/Q3/Q5/Q6/Q7/Q10/Q11/Q12/Q14/Q18/Q19),逐条计时(含解析+计划+执行)。

use std::sync::Arc;
use std::time::Instant;

use common::config::Config;
use common::types::{ScalarValue, civil_from_days, days_from_civil};
use server::engine::{Engine, Outcome};

const LINEITEM_CT: usize = 60_000;
const ORDERS_CT: usize = 15_000;
const CUSTOMER_CT: usize = 15_000;
const PART_CT: usize = 20_000;
const PARTSUPP_CT: usize = PART_CT * 4;
const SUPPLIER_CT: usize = 2_000;
const INSERT_BATCH: usize = 1000;

// ---------- 伪随机(xorshift64,确定性) ----------

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }
}

// ---------- 日期 ----------

fn day(y: i32, m: u32, d: u32) -> i32 {
    days_from_civil(y, m, d)
}

fn fmt_date(day_num: i32) -> String {
    let (y, m, d) = civil_from_days(day_num);
    format!("{y:04}-{m:02}-{d:02}")
}

// ---------- 词表 ----------

const NATIONS: [&str; 25] = [
    "ALGERIA",
    "ARGENTINA",
    "BRAZIL",
    "CANADA",
    "EGYPT",
    "ETHIOPIA",
    "FRANCE",
    "GERMANY",
    "INDIA",
    "INDONESIA",
    "IRAN",
    "IRAQ",
    "JAPAN",
    "JORDAN",
    "KENYA",
    "MOROCCO",
    "MOZAMBIQUE",
    "PERU",
    "CHINA",
    "ROMANIA",
    "SAUDI ARABIA",
    "VIETNAM",
    "RUSSIA",
    "UNITED KINGDOM",
    "UNITED STATES",
];
const REGIONS: [&str; 5] = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];
const SEGMENTS: [&str; 5] = [
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "MACHINERY",
    "HOUSEHOLD",
];
const SHIPMODES: [&str; 7] = [
    "AIR",
    "AIR REG",
    "BOAT",
    "DO NOT DELIVER",
    "MAIL",
    "RAIL",
    "TRUCK",
];
const PRIORITIES: [&str; 5] = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
const TYPE_A: [&str; 6] = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"];
const TYPE_B: [&str; 5] = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"];
const TYPE_C: [&str; 5] = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"];
const CONTAINER_A: [&str; 5] = ["SMALL", "MED", "LARGE", "ECONOMY", "JUMBO"];
const CONTAINER_B: [&str; 5] = ["CASE", "BOX", "BAG", "JAR", "PACK"];
const WORDS: [&str; 16] = [
    "furiously",
    "stealthy",
    "packages",
    "pending",
    "carefully",
    "special",
    "excuses",
    "deposits",
    "silent",
    "ironic",
    "asymptotes",
    "blithely",
    "fluffily",
    "boldly",
    "quickly",
    "regular",
];

fn filler(rng: &mut Rng, max_words: usize) -> String {
    let n = rng.range(5, max_words as u64) as usize;
    (0..n)
        .map(|i| {
            let mut w = WORDS[rng.below(WORDS.len() as u64) as usize].to_string();
            if i % 4 == 0 {
                w.push_str(" s");
                w.push_str(&rng.below(90_000_000).to_string());
            }
            w
        })
        .collect::<Vec<_>>()
        .join(" ")
}

// ---------- INSERT 构造 ----------

fn insert_batches(
    table: &str,
    cols: &str,
    mut row: impl FnMut(&mut Rng) -> String,
    rng: &mut Rng,
    count: usize,
) -> Vec<String> {
    let mut stmts = Vec::new();
    let mut batch = String::with_capacity(128 * 1024);
    let mut n = 0usize;
    while n < count {
        batch.clear();
        batch.push_str(&format!("INSERT INTO {table} ({cols}) VALUES "));
        let end = (n + INSERT_BATCH).min(count);
        for i in n..end {
            if i > n {
                batch.push(',');
            }
            batch.push_str(&row(rng));
        }
        stmts.push(batch.clone());
        n = end;
    }
    stmts
}

fn run_statements(engine: &Arc<Engine>, stmts: &[String]) {
    for s in stmts {
        engine.execute_script(s).unwrap();
    }
}

// ---------- 数据生成 ----------

fn ddl_statements() -> Vec<String> {
    let mut stmts: Vec<String> = vec![
        String::from("CREATE TABLE region (r_regionkey BIGINT NOT NULL, r_name TEXT NOT NULL)"),
        String::from(
            "CREATE TABLE nation (n_nationkey BIGINT NOT NULL, n_name TEXT NOT NULL, n_regionkey BIGINT NOT NULL)",
        ),
        String::from(
            "CREATE TABLE supplier (s_suppkey BIGINT NOT NULL, s_name TEXT NOT NULL, s_address TEXT NOT NULL, s_nationkey BIGINT NOT NULL, s_phone TEXT NOT NULL, s_acctbal DOUBLE NOT NULL, s_comment TEXT NOT NULL)",
        ),
        String::from(
            "CREATE TABLE part (p_partkey BIGINT NOT NULL, p_brand TEXT NOT NULL, p_type TEXT NOT NULL, p_size BIGINT NOT NULL, p_container TEXT NOT NULL)",
        ),
        String::from(
            "CREATE TABLE partsupp (ps_partkey BIGINT NOT NULL, ps_suppkey BIGINT NOT NULL, ps_availqty BIGINT NOT NULL, ps_supplycost DOUBLE NOT NULL)",
        ),
        String::from(
            "CREATE TABLE customer (c_custkey BIGINT NOT NULL, c_name TEXT NOT NULL, c_address TEXT NOT NULL, c_nationkey BIGINT NOT NULL, c_phone TEXT NOT NULL, c_acctbal DOUBLE NOT NULL, c_mktsegment TEXT NOT NULL, c_comment TEXT NOT NULL)",
        ),
        String::from(
            "CREATE TABLE orders (o_orderkey BIGINT NOT NULL, o_custkey BIGINT NOT NULL, o_orderdate DATE NOT NULL, o_shippriority BIGINT NOT NULL, o_totalprice DOUBLE NOT NULL, o_orderpriority TEXT NOT NULL)",
        ),
        String::from(
            "CREATE TABLE lineitem (l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL, l_quantity BIGINT NOT NULL, l_extendedprice DOUBLE NOT NULL, l_discount DOUBLE NOT NULL, l_tax DOUBLE NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_returnflag TEXT NOT NULL, l_linestatus TEXT NOT NULL, l_shipmode TEXT NOT NULL, l_comment TEXT NOT NULL)",
        ),
    ];
    let regions: Vec<String> = REGIONS
        .iter()
        .enumerate()
        .map(|(i, name)| format!("({}, '{}')", i, name))
        .collect();
    stmts.push(format!(
        "INSERT INTO region (r_regionkey, r_name) VALUES {}",
        regions.join(",")
    ));
    let nations: Vec<String> = NATIONS
        .iter()
        .enumerate()
        .map(|(i, name)| format!("({}, '{}', {})", i, name, i % 5))
        .collect();
    stmts.push(format!(
        "INSERT INTO nation (n_nationkey, n_name, n_regionkey) VALUES {}",
        nations.join(",")
    ));
    stmts
}

fn create_tables(engine: &Arc<Engine>) {
    run_statements(engine, &ddl_statements());
}

/// 全部装载语句(schema + 数据),供 medb 与 PostgreSQL 吃同一份输入。
fn load_statements() -> Vec<String> {
    let mut stmts = ddl_statements();
    let mut rng = Rng::new(11);
    stmts.extend(insert_batches(
        "supplier",
        "s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment",
        |r| {
            let k = r.range(1, SUPPLIER_CT as u64);
            let nat = r.below(25) as i64;
            format!(
                "({}, 'Supplier#{:09}', '{}', {}, '{:02}{:03}-{:03}-{:04}', {:.2}, '{}')",
                k,
                k,
                filler(r, 25),
                nat,
                10 + nat % 30,
                r.below(1000),
                r.below(1000),
                r.below(10000),
                (r.below(1_100_000) as f64 / 100.0) - 10.0,
                filler(r, 40),
            )
        },
        &mut rng,
        SUPPLIER_CT,
    ));
    let mut rng = Rng::new(22);
    stmts.extend(insert_batches(
        "part",
        "p_partkey, p_brand, p_type, p_size, p_container",
        |r| {
            let k = r.range(1, PART_CT as u64);
            format!(
                "({}, 'Brand#{}{}', '{} {} {}', {}, '{} {}')",
                k,
                r.range(1, 5),
                r.range(1, 5),
                TYPE_A[r.below(6) as usize],
                TYPE_B[r.below(5) as usize],
                TYPE_C[r.below(5) as usize],
                r.range(1, 50),
                CONTAINER_A[r.below(5) as usize],
                CONTAINER_B[r.below(5) as usize],
            )
        },
        &mut rng,
        PART_CT,
    ));
    let mut rng = Rng::new(33);
    stmts.extend(insert_batches(
        "partsupp",
        "ps_partkey, ps_suppkey, ps_availqty, ps_supplycost",
        |r| {
            let p = r.range(1, PART_CT as u64);
            let s = p % SUPPLIER_CT as u64 + 1;
            format!(
                "({}, {}, {}, {:.2})",
                p,
                s,
                r.range(1, 9999),
                r.range(100, 1000) as f64 / 10.0,
            )
        },
        &mut rng,
        PARTSUPP_CT,
    ));
    let mut rng = Rng::new(44);
    stmts.extend(insert_batches(
        "customer",
        "c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment",
        |r| {
            let k = r.range(1, CUSTOMER_CT as u64);
            let nat = r.below(25) as i64;
            format!(
                "({}, 'Customer#{:09}', '{}', {}, '{:02}{:03}-{:03}-{:04}', {:.2}, '{}', '{}')",
                k,
                k,
                filler(r, 25),
                nat,
                10 + nat % 30,
                r.below(1000),
                r.below(1000),
                r.below(10000),
                (r.below(1_100_000) as f64 / 100.0) - 10.0,
                SEGMENTS[r.below(5) as usize],
                filler(r, 40),
            )
        },
        &mut rng,
        CUSTOMER_CT,
    ));
    stmts.extend(orders_lineitem_statements(day(1998, 9, 2)));
    stmts
}

fn orders_lineitem_statements(cutoff: i32) -> Vec<String> {
    let mut rng = Rng::new(55);
    let base = day(1992, 1, 1);
    let mut stmts = Vec::new();
    let mut n = 0usize;
    while n < ORDERS_CT {
        let mut orders_batch = String::with_capacity(128 * 1024);
        let mut li_batch = String::with_capacity(512 * 1024);
        orders_batch.push_str("INSERT INTO orders (o_orderkey, o_custkey, o_orderdate, o_shippriority, o_totalprice, o_orderpriority) VALUES ");
        li_batch.push_str("INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate, l_commitdate, l_receiptdate, l_returnflag, l_linestatus, l_shipmode, l_comment) VALUES ");
        let end = (n + INSERT_BATCH).min(ORDERS_CT);
        let mut first_order = true;
        let mut first_li = true;
        for i in n..end {
            let ok = (i + 1) as u64;
            let ck = rng.range(1, CUSTOMER_CT as u64);
            let odate = base + rng.below(2555) as i32;
            let mut total = 0.0f64;
            let lcount = 1 + rng.below(7);
            let mut li_rows = Vec::with_capacity(lcount as usize);
            for _ in 0..lcount {
                let partkey = rng.range(1, PART_CT as u64);
                let suppkey = partkey % SUPPLIER_CT as u64 + 1;
                let qty = rng.range(1, 50);
                let price = (rng.range(900, 90_000) as f64) / 100.0;
                let ext = qty as f64 * price;
                let disc = rng.below(10) as f64 / 100.0;
                let tax = rng.below(8) as f64 / 100.0;
                let ship = odate + rng.below(121) as i32 + 1;
                let commit = ship + rng.below(30) as i32;
                let receipt = commit + rng.below(40) as i32 + 1;
                let (flag, status) = if receipt <= cutoff {
                    (if ship <= commit { "A" } else { "R" }, "F")
                } else {
                    ("N", "O")
                };
                total += ext * (1.0 - disc);
                li_rows.push(format!(
                    "({}, {}, {}, {}, {:.2}, {:.2}, {:.2}, DATE '{}', DATE '{}', DATE '{}', '{}', '{}', '{}', '{}')",
                    ok,
                    partkey,
                    suppkey,
                    qty,
                    ext,
                    disc,
                    tax,
                    fmt_date(ship),
                    fmt_date(commit),
                    fmt_date(receipt),
                    flag,
                    status,
                    SHIPMODES[rng.below(7) as usize],
                    filler(&mut rng, 15),
                ));
            }
            if !first_order {
                orders_batch.push(',');
            }
            orders_batch.push_str(&format!(
                "({}, {}, DATE '{}', 0, {:.2}, '{}')",
                ok,
                ck,
                fmt_date(odate),
                total,
                PRIORITIES[rng.below(5) as usize],
            ));
            first_order = false;
            for lr in &li_rows {
                if !first_li {
                    li_batch.push(',');
                }
                li_batch.push_str(lr);
                first_li = false;
            }
        }
        n = end;
        stmts.push(orders_batch);
        stmts.push(li_batch);
    }
    stmts
}

// ---------- 查询(TPC-H 参数取常量) ----------

const QUERIES: &[(&str, &str)] = &[
    (
        "Q1",
        "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, AVG(l_discount) AS avg_disc, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
    ),
    (
        "Q3",
        "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15' GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, l_orderkey LIMIT 10",
    ),
    (
        "Q5",
        "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= DATE '1996-01-01' AND o_orderdate < DATE '1997-01-01' GROUP BY n_name ORDER BY revenue DESC",
    ),
    (
        "Q6",
        "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.02 AND 0.09 AND l_quantity < 24",
    ),
    (
        "Q7",
        "SELECT n_name, SUM(CASE WHEN n_name = 'FRANCE' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS france_revenue, SUM(CASE WHEN n_name = 'GERMANY' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS germany_revenue FROM customer, orders, lineitem, supplier, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' AND (n_name = 'FRANCE' OR n_name = 'GERMANY') GROUP BY n_name ORDER BY n_name",
    ),
    (
        "Q10",
        "SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue, c_acctbal, n_name, c_address, c_phone, c_comment FROM customer, orders, lineitem, nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01' AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey, c_name, c_acctbal, c_address, c_phone, c_comment, n_name ORDER BY revenue DESC LIMIT 20",
    ),
    (
        "Q11",
        "SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY ps_partkey HAVING SUM(ps_supplycost * ps_availqty) > (SELECT SUM(ps_supplycost * ps_availqty) FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') * 0.0001 ORDER BY value DESC",
    ),
    (
        "Q12",
        "SELECT l_shipmode, SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count, SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count FROM orders, lineitem WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01' GROUP BY l_shipmode ORDER BY l_shipmode",
    ),
    (
        "Q14",
        "SELECT 100.00 * SUM(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'",
    ),
    (
        "Q18",
        "SELECT c_name, c_custkey, o_orderkey, o_totalprice, o_orderdate FROM customer, orders WHERE o_custkey = c_custkey AND o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 150) GROUP BY c_name, c_custkey, o_orderkey, o_totalprice, o_orderdate ORDER BY o_totalprice DESC, o_orderdate LIMIT 100",
    ),
    (
        "Q19",
        "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem, part WHERE (l_shipmode = 'AIR' AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_quantity >= 10 AND l_quantity <= 20 AND l_partkey = p_partkey AND p_brand = 'Brand#12' AND p_container IN ('SMALL CASE', 'SMALL BOX') AND p_size >= 1 AND p_size <= 5) OR (l_shipmode = 'AIR' AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_quantity >= 10 AND l_quantity <= 30 AND l_partkey = p_partkey AND p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX') AND p_size >= 1 AND p_size <= 10) OR (l_shipmode = 'AIR' AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_quantity >= 20 AND l_quantity <= 40 AND l_partkey = p_partkey AND p_brand = 'Brand#34' AND p_container IN ('LARGE CASE', 'LARGE PACK') AND p_size >= 1 AND p_size <= 15)",
    ),
];

fn exec(engine: &Arc<Engine>, sql: &str) -> usize {
    match engine.execute_script(sql).unwrap().pop().unwrap() {
        Outcome::Query { rows, .. } => rows.len(),
        Outcome::Tag { rows, .. } => rows.unwrap_or(0) as usize,
    }
}

fn query_count(engine: &Arc<Engine>, table: &str) -> u64 {
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

fn open_engine(config: &Config) -> Arc<Engine> {
    Engine::open(config.clone()).unwrap()
}

#[test]
#[ignore = "显式运行:cargo test -p medb-server --test tpch_bench --release -- --ignored --nocapture"]
fn tpch_benchmark() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    let engine = open_engine(&config);
    let t = Instant::now();
    let stmts = load_statements();
    let gen_elapsed = t.elapsed();
    let t = Instant::now();
    run_statements(&engine, &stmts);
    let load_main = t.elapsed();
    println!(
        "load: {} statements (gen {:?}, execute {:?})",
        stmts.len(),
        gen_elapsed,
        load_main
    );

    // **基准场景约定:查询前把 memtable 刷进 SST**(合并 + 重建列存副本)。
    // AP 读路径 = SST 列存副本 + memtable 增量合并;分析型查询应运行在
    // "数据已合并" 的稳态 —— 脏 memtable 会让每条查询为 delta 合并买单,
    // 那是写多读少场景的代价,不属于本基准的测量目标。
    let t = Instant::now();
    engine.flush().unwrap();
    let t_flush_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!("flush-before-query: {:?}", t.elapsed());

    for (t, expect) in [
        ("lineitem", 0usize),
        ("orders", ORDERS_CT),
        ("customer", CUSTOMER_CT),
        ("part", PART_CT),
        ("partsupp", PARTSUPP_CT),
        ("supplier", SUPPLIER_CT),
    ] {
        let n = query_count(&engine, t);
        println!("count {t}: {n} (expect ~{expect})");
        if expect > 0 {
            assert_eq!(n, expect as u64, "行数不符: {t}");
        }
    }

    println!(
        "\n{:<6} {:>8} {:>10} {:>10} {:>8}",
        "query", "rows", "warm(ms)", "avg(ms)", "min(ms)"
    );
    let mut total = 0.0f64;
    let mut records: Vec<BenchRecord> = Vec::with_capacity(QUERIES.len());
    for (name, sql) in QUERIES {
        let t0 = Instant::now();
        let rows = exec(&engine, sql); // warm(含首次自动 ANALYZE)
        let warm = t0.elapsed().as_secs_f64() * 1000.0;
        let mut times = Vec::new();
        for _ in 0..5 {
            let t = Instant::now();
            let r = exec(&engine, sql);
            times.push(t.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(r, rows, "行数不稳定: {name}");
        }
        let avg = times.iter().sum::<f64>() / times.len() as f64;
        let min = times.iter().cloned().fold(f64::MAX, f64::min);
        total += avg;
        records.push(BenchRecord {
            query: name.to_string(),
            rows,
            avg_ms: avg,
            min_ms: min,
        });
        println!(
            "{:<6} {:>8} {:>10.1} {:>10.1} {:>8.1}",
            name, rows, warm, avg, min
        );
    }
    println!("{:<6} {:>20.1}", "TOTAL(avg sum)", total);

    // ---------- 逐 SQL 对比报告(与上次基线 Δ% / 占比 / 对 PG 比值) ----------
    report_and_save_baseline(
        &records,
        total,
        load_main.as_secs_f64() * 1000.0,
        t_flush_ms,
    );
}

// ---------- 基线对比(每次运行持久化 + 与上次逐 SQL 对比) ----------

/// PostgreSQL 18.6 同数据同机参考值(EXPLAIN ANALYZE 计时,默认配置,
/// 2026-11 实测)。机器/配置变化时仅作量级参考。
const PG_REFERENCE_MS: &[(&str, f64)] = &[
    ("Q1", 16.7),
    ("Q3", 10.3),
    ("Q5", 9.3),
    ("Q6", 3.3),
    ("Q7", 8.3),
    ("Q10", 3.2),
    ("Q11", 6.9),
    ("Q12", 5.9),
    ("Q14", 4.9),
    ("Q18", 16.4),
    ("Q19", 4.6),
];

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct BenchRecord {
    query: String,
    rows: usize,
    avg_ms: f64,
    min_ms: f64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Baseline {
    profile: String,
    load_execute_ms: f64,
    flush_ms: f64,
    total_avg_ms: f64,
    queries: Vec<BenchRecord>,
    saved_at: String,
}

fn baseline_path() -> std::path::PathBuf {
    std::path::PathBuf::from("target/tpch_baseline.json")
}

fn history_path() -> std::path::PathBuf {
    std::path::PathBuf::from("target/tpch_history.jsonl")
}

fn build_profile_tag() -> String {
    if cfg!(debug_assertions) {
        "debug".to_string()
    } else {
        "release".to_string()
    }
}

/// 打印与上次基线的逐 SQL 对比表,并把本次结果持久化为新基线 + 追加历史。
///
/// 表格列:avg(本次均值)/ prev(上次基线)/ Δ%(变化)/ share(占总耗时)/
/// vsPG(与 PG 参考值的倍数)。**优化方向看两列**:share 大 = 总耗时大头;
/// vsPG 大 = 相对主流库差距最大的形状(单表聚合 / 连接 / 子查询)。
fn report_and_save_baseline(records: &[BenchRecord], total: f64, load_ms: f64, flush_ms: f64) {
    let profile = build_profile_tag();
    let prev: Option<Baseline> = std::fs::read(baseline_path())
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());

    let comparable = prev
        .as_ref()
        .is_some_and(|p| p.profile == profile && p.total_avg_ms > 0.0);
    if let Some(p) = &prev {
        if !comparable {
            println!(
                "\n[note] 上次基线 profile={} 与本次({})不可比,跳过 Δ%",
                p.profile, profile
            );
        }
    }

    println!(
        "\n{:<6} {:>8} {:>9} {:>9} {:>9} {:>8} {:>7}",
        "query", "avg(ms)", "prev", "Δ%", "share%", "vsPG", "min(ms)"
    );
    for r in records {
        let pg = PG_REFERENCE_MS
            .iter()
            .find(|(n, _)| *n == r.query)
            .map(|(_, v)| *v)
            .unwrap_or(f64::NAN);
        let share = if total > 0.0 { r.avg_ms / total * 100.0 } else { 0.0 };
        let vs_pg = if pg > 0.0 { r.avg_ms / pg } else { f64::NAN };
        let (prev_avg, delta) = if comparable {
            prev.as_ref()
                .unwrap()
                .queries
                .iter()
                .find(|p| p.query == r.query)
                .map(|p| {
                    let d = if p.avg_ms > 0.0 {
                        (r.avg_ms - p.avg_ms) / p.avg_ms * 100.0
                    } else {
                        f64::NAN
                    };
                    (p.avg_ms, d)
                })
                .unwrap_or((f64::NAN, f64::NAN))
        } else {
            (f64::NAN, f64::NAN)
        };
        println!(
            "{:<6} {:>8.1} {:>9.1} {:>+8.1}% {:>7.1}% {:>6.2}x {:>7.1}",
            r.query, r.avg_ms, prev_avg, delta, share, vs_pg, r.min_ms
        );
    }
    let prev_total = if comparable {
        prev.as_ref().unwrap().total_avg_ms
    } else {
        f64::NAN
    };
    let total_delta = if comparable {
        (total - prev_total) / prev_total * 100.0
    } else {
        f64::NAN
    };
    println!(
        "{:<6} {:>8.1} {:>9.1} {:>+8.1}% {:>7.1}% {:>6.2}x",
        "TOTAL",
        total,
        prev_total,
        total_delta,
        100.0,
        total / PG_REFERENCE_MS.iter().map(|(_, v)| v).sum::<f64>()
    );

    // 优化方向提示:占比前三(总耗时大头)+ 相对 PG 差距前三(形状差距)
    let mut by_share = records.to_vec();
    by_share.sort_by(|a, b| b.avg_ms.partial_cmp(&a.avg_ms).unwrap());
    let hints: Vec<String> = by_share
        .iter()
        .take(3)
        .map(|r| {
            let pg = PG_REFERENCE_MS
                .iter()
                .find(|(n, _)| *n == r.query)
                .map(|(_, v)| *v)
                .unwrap_or(1.0);
            format!(
                "{} {:.1}ms (share {:.0}%, vsPG {:.1}x)",
                r.query,
                r.avg_ms,
                if total > 0.0 { r.avg_ms / total * 100.0 } else { 0.0 },
                r.avg_ms / pg
            )
        })
        .collect();
    println!("\n[next] 优化方向(占比前三): {}", hints.join(" | "));

    // 持久化:基线覆盖 + 历史追加
    let baseline = Baseline {
        profile,
        load_execute_ms: load_ms,
        flush_ms,
        total_avg_ms: total,
        queries: records.to_vec(),
        saved_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_default(),
    };
    if let Some(parent) = baseline_path().parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(
        baseline_path(),
        serde_json::to_string_pretty(&baseline).unwrap(),
    )
    .unwrap();
    let mut history_line = serde_json::to_string(&baseline).unwrap();
    history_line.push('\n');
    use std::io::Write;
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(history_path())
        .unwrap()
        .write_all(history_line.as_bytes())
        .unwrap();
    println!(
        "[saved] {} (基线) / {} (历史追加)",
        baseline_path().display(),
        history_path().display()
    );
}

/// 把装载 SQL 与 11 条查询导出到目录(默认 target/tpch_sql),供
/// PostgreSQL 等主流库吃**完全相同**的输入做对比(默认 #[ignore])。
#[test]
#[ignore = "显式运行:cargo test -p medb-server --test tpch_bench --release tpch_dump_sql -- --ignored --nocapture"]
fn tpch_dump_sql() {
    let dir = std::path::Path::new("target/tpch_sql");
    std::fs::create_dir_all(dir).unwrap();
    let mut schema = String::new();
    let mut data = String::new();
    for stmt in load_statements() {
        if stmt.starts_with("CREATE TABLE") {
            schema.push_str(&stmt);
            schema.push_str(";\n");
        } else {
            data.push_str(&stmt);
            data.push_str(";\n");
        }
    }
    std::fs::write(dir.join("schema.sql"), schema).unwrap();
    std::fs::write(dir.join("load.sql"), data).unwrap();
    let queries: String = QUERIES
        .iter()
        .map(|(name, sql)| format!("\\echo {name}\nEXPLAIN (ANALYZE, TIMING OFF)\n{sql};\n"))
        .collect();
    std::fs::write(dir.join("queries.sql"), queries).unwrap();
    println!("dumped to {}/{{schema,load,queries}}.sql", dir.display());
}
