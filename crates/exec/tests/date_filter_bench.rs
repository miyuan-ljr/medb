//! 日期过滤管线分步计时(定位 Q1 形状的高通过率 Date 谓词开销)。

use exec::chunk::DataChunk;
use exec::expr::{BinaryOp, Expr};
use exec::vector::{ColumnData, Vector};
use common::types::{DataType, ScalarValue};
use std::time::Instant;

fn date_vec(n: usize) -> Vector {
    let vals: Vec<ScalarValue> = (0..n).map(|i| ScalarValue::Date(10000 + (i % 9000) as i32)).collect();
    Vector::from_scalars(DataType::Date, vals).unwrap()
}

fn int_vec(n: usize) -> Vector {
    Vector::int64s((0..n as i64).collect())
}

fn utf8_vec(n: usize) -> Vector {
    let vals: Vec<ScalarValue> = (0..n).map(|i| ScalarValue::Utf8(format!("s{}", i % 100))).collect();
    Vector::from_scalars(DataType::Utf8, vals).unwrap()
}

#[test]
fn date_filter_pipeline_steps() {
    let n = 60_000;
    // 14 列,模拟 lineitem chunk
    let mut cols: Vec<Vector> = Vec::new();
    cols.push(date_vec(n)); // shipdate
    cols.push(date_vec(n));
    cols.push(int_vec(n));
    cols.push(int_vec(n));
    cols.push(Vector::float64s(vec![0.05; n]));
    cols.push(Vector::float64s(vec![0.01; n]));
    cols.push(Vector::float64s(vec![0.02; n]));
    cols.push(int_vec(n));
    cols.push(int_vec(n));
    cols.push(int_vec(n));
    cols.push(utf8_vec(n));
    cols.push(utf8_vec(n));
    cols.push(utf8_vec(n));
    cols.push(utf8_vec(n));
    let chunk = DataChunk::new(cols).unwrap();

    let pred = Expr::binary(
        BinaryOp::LtEq,
        Expr::Column(0),
        Expr::Literal(ScalarValue::Date(1998)),
    );

    // ① 谓词求值(3 次取最小)
    let mut t_eval = u128::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let mask = pred.eval(&chunk).unwrap();
        t_eval = t_eval.min(t.elapsed().as_micros());
        assert_eq!(mask.len(), n);
    }

    // ② mask → sel
    let t = Instant::now();
    let mask = pred.eval(&chunk).unwrap();
    let sel: Vec<bool> = (0..n).map(|i| !mask.null_at(i) && mask.bool_values()[i]).collect();
    let t_mask = t.elapsed().as_micros();

    // ③ chunk.select
    let mut t_select = u128::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let tracker = std::sync::Arc::new(memory::MemoryTracker::new_root("bench", None));
        let out = chunk.select(&sel, &tracker).unwrap();
        t_select = t_select.min(t.elapsed().as_micros());
        assert_eq!(out.num_rows(), sel.iter().filter(|s| **s).count());
    }

    // ④ 内核单测:Date 列 vs 广播字面量
    let lit = exec::expr::broadcast(&ScalarValue::Date(1998), n).unwrap();
    let col = date_vec(n);
    let mut t_kernel = u128::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let out = exec::expr::eval_binary(BinaryOp::LtEq, &col, &lit).unwrap();
        t_kernel = t_kernel.min(t.elapsed().as_micros());
        assert_eq!(out.len(), n);
    }

    println!("eval={t_eval}µs mask={t_mask}µs select={t_select}µs kernel={t_kernel}µs");
}
