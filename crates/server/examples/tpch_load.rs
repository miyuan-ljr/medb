//! TPC-H 批量导入:`|` 分隔 .tbl 文件(官方 dbgen / DuckDB dbgen 导出格式)
//! 直灌 LSM(StatementPlan::Insert,绕开 SQL 解析)。
//!
//! 用法:
//!   cargo run --release -p medb-server --example tpch_load -- <data_dir> <tbl_dir>
//!
//! 建表结构 = tools/tpch 的基准 schema;导入完成后执行一次 FLUSH,
//! 由 flush 钩子把 SST 重建为列存 segment(AP 路径生效)。

use std::io::{BufRead, BufReader};
use std::sync::Arc;

use common::config::Config;
use common::error::{Error, Result};
use common::types::{DataType, ScalarValue};
use catalog::CatalogStore;
use server::engine::Engine;
use sql::planner::StatementPlan;

const SCHEMA_DDLS: &[&str] = &[
    "CREATE TABLE region (r_regionkey BIGINT NOT NULL, r_name TEXT NOT NULL)",
    "CREATE TABLE nation (n_nationkey BIGINT NOT NULL, n_name TEXT NOT NULL, n_regionkey BIGINT NOT NULL)",
    "CREATE TABLE supplier (s_suppkey BIGINT NOT NULL, s_name TEXT NOT NULL, s_address TEXT NOT NULL, s_nationkey BIGINT NOT NULL, s_phone TEXT NOT NULL, s_acctbal DOUBLE NOT NULL, s_comment TEXT NOT NULL)",
    "CREATE TABLE part (p_partkey BIGINT NOT NULL, p_brand TEXT NOT NULL, p_type TEXT NOT NULL, p_size BIGINT NOT NULL, p_container TEXT NOT NULL)",
    "CREATE TABLE partsupp (ps_partkey BIGINT NOT NULL, ps_suppkey BIGINT NOT NULL, ps_availqty BIGINT NOT NULL, ps_supplycost DOUBLE NOT NULL)",
    "CREATE TABLE customer (c_custkey BIGINT NOT NULL, c_name TEXT NOT NULL, c_address TEXT NOT NULL, c_nationkey BIGINT NOT NULL, c_phone TEXT NOT NULL, c_acctbal DOUBLE NOT NULL, c_mktsegment TEXT NOT NULL, c_comment TEXT NOT NULL)",
    "CREATE TABLE orders (o_orderkey BIGINT NOT NULL, o_custkey BIGINT NOT NULL, o_orderdate DATE NOT NULL, o_shippriority BIGINT NOT NULL, o_totalprice DOUBLE NOT NULL, o_orderpriority TEXT NOT NULL)",
    "CREATE TABLE lineitem (l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL, l_quantity BIGINT NOT NULL, l_extendedprice DOUBLE NOT NULL, l_discount DOUBLE NOT NULL, l_tax DOUBLE NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_returnflag TEXT NOT NULL, l_linestatus TEXT NOT NULL, l_shipmode TEXT NOT NULL, l_comment TEXT NOT NULL)",
];

/// 导入顺序:维度表在前,事实表在后。
const TABLES: &[(&str, &[DataType])] = &[
    ("region", &[DataType::Int64, DataType::Utf8]),
    ("nation", &[DataType::Int64, DataType::Utf8, DataType::Int64]),
    (
        "supplier",
        &[
            DataType::Int64,
            DataType::Utf8,
            DataType::Utf8,
            DataType::Int64,
            DataType::Utf8,
            DataType::Float64,
            DataType::Utf8,
        ],
    ),
    (
        "part",
        &[
            DataType::Int64,
            DataType::Utf8,
            DataType::Utf8,
            DataType::Int64,
            DataType::Utf8,
        ],
    ),
    (
        "partsupp",
        &[
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Float64,
        ],
    ),
    (
        "customer",
        &[
            DataType::Int64,
            DataType::Utf8,
            DataType::Utf8,
            DataType::Int64,
            DataType::Utf8,
            DataType::Float64,
            DataType::Utf8,
            DataType::Utf8,
        ],
    ),
    (
        "orders",
        &[
            DataType::Int64,
            DataType::Int64,
            DataType::Date,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
        ],
    ),
    (
        "lineitem",
        &[
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Int64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Date,
            DataType::Date,
            DataType::Date,
            DataType::Utf8,
            DataType::Utf8,
            DataType::Utf8,
            DataType::Utf8,
        ],
    ),
];

const BATCH_ROWS: usize = 25_000;

fn parse_field(field: &str, ty: &DataType) -> Result<ScalarValue> {
    match ty {
        DataType::Int64 => {
            // DuckDB dbgen 把数量列存为 DECIMAL(6,2),导出形如 17.00 → 四舍五入
            if let Ok(i) = field.parse::<i64>() {
                return Ok(ScalarValue::Int64(i));
            }
            let f = field
                .parse::<f64>()
                .map_err(|e| Error::sql(format!("bad BIGINT '{field}': {e}")))?;
            Ok(ScalarValue::Int64(f.round() as i64))
        }
        DataType::Float64 => field
            .parse::<f64>()
            .map(ScalarValue::Float64)
            .map_err(|e| Error::sql(format!("bad DOUBLE '{field}': {e}"))),
        DataType::Utf8 => {
            // DuckDB CSV 写出器会给部分文本字段加引号:剥离成对引号与 "" 转义
            let s = if field.len() >= 2 && field.starts_with('"') && field.ends_with('"') {
                field[1..field.len() - 1].replace("\"\"", "\"")
            } else {
                field.to_string()
            };
            Ok(ScalarValue::Utf8(s))
        }
        DataType::Date => common::types::parse_date_text(field),
        other => Err(Error::sql(format!("unsupported column type {other:?}"))),
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: tpch_load <data_dir> <tbl_dir>");
        std::process::exit(2);
    }
    let mut config = Config::default();
    config.storage.data_dir = args[1].clone().into();
    // 导入期把写缓冲放大到 512MB,减少中途 flush(flush 钩子会全量重建
    // 所有表的列存 segment);结束后统一 FLUSH 一次。
    config.storage.write_buffer_bytes = 512 << 20;

    let engine = Engine::open(config)?;
    let t_start = std::time::Instant::now();

    for ddl in SCHEMA_DDLS {
        engine.execute_script(ddl)?;
    }
    println!("schema created ({:.1}s)", t_start.elapsed().as_secs_f64());

    for (name, types) in TABLES {
        let path = format!("{}/{}.tbl", args[2], name);
        let file = std::fs::File::open(&path)
            .map_err(|e| Error::sql(format!("open {path}: {e}")))?;
        let reader = BufReader::with_capacity(8 << 20, file);
        let table_schema = engine
            .catalog()
            .get_table(name)
            .ok_or_else(|| Error::sql(format!("table '{name}' not found")))?;

        let mut rows: Vec<common::schema::Row> = Vec::with_capacity(BATCH_ROWS);
        let mut total = 0usize;
        let t_table = std::time::Instant::now();
        let mut lines = reader.lines();
        lines.next(); // 首行 = DuckDB 导出的表头
        for line in lines {
            let line = line?;
            let line = line.trim_end_matches('\r');
            if line.is_empty() {
                continue;
            }
            let mut row: common::schema::Row = Vec::with_capacity(types.len());
            for (field, ty) in line.split('|').zip(types.iter()) {
                row.push(parse_field(field, ty)?);
            }
            rows.push(row);
            if rows.len() >= BATCH_ROWS {
                total += flush_batch(&engine, table_schema.clone(), &mut rows)?;
            }
        }
        total += flush_batch(&engine, table_schema.clone(), &mut rows)?;
        println!(
            "{name}: {total} 行,{:.1}s",
            t_table.elapsed().as_secs_f64()
        );
    }

    println!("flush(重建列存 segment)...");
    engine.flush()?;
    println!("total={:.1}s", t_start.elapsed().as_secs_f64());
    Ok(())
}

fn flush_batch(
    engine: &Arc<Engine>,
    table: common::schema::TableSchema,
    rows: &mut Vec<common::schema::Row>,
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    let n = rows.len();
    let mut sctx = engine.begin_autocommit();
    engine.execute_plan(&mut sctx, StatementPlan::Insert { table, rows: std::mem::take(rows) })?;
    Ok(n)
}
