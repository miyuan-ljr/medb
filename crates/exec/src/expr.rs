//! 标量表达式与向量化求值内核。
//!
//! 求值单位是"整列":内核函数在 `&[i64]` / `&[f64]` 等切片上写紧密循环,
//! 避免逐行虚调用与标量装箱 —— 这是向量化引擎性能的基本盘。
//! null 语义:比较与算术按三值逻辑传播(null 与任何值运算 → null);
//! And/Or 当前把 null 按 false 处理(简化,3VL 见路线图)。

use common::error::{Error, Result};
use common::types::{DataType, ScalarValue};
use std::fmt;

use crate::vector::{ColumnData, Datum, Vector};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    /// SQL LIKE(% 任意序列,_ 单字符)
    Like,
    /// SQL NOT LIKE
    NotLike,
}

impl BinaryOp {
    pub fn symbol(&self) -> &'static str {
        match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Eq => "=",
            BinaryOp::NotEq => "<>",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::And => "AND",
            BinaryOp::Or => "OR",
            BinaryOp::Like => "LIKE",
            BinaryOp::NotLike => "NOT LIKE",
        }
    }

    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
        )
    }

    pub fn is_like(&self) -> bool {
        matches!(self, BinaryOp::Like | BinaryOp::NotLike)
    }

    pub fn is_logic(&self) -> bool {
        matches!(self, BinaryOp::And | BinaryOp::Or)
    }

    pub fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
        )
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.symbol())
    }
}

#[derive(Debug, Clone)]
pub enum Expr {
    /// 输入批中的列下标。
    Column(usize),
    Literal(ScalarValue),
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// 标量函数调用(name 小写;聚合函数不在执行表达式里,见 AggHashExec)。
    Function {
        name: String,
        args: Vec<Expr>,
    },
    /// IS NULL / IS NOT NULL。
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(i) => write!(f, "#{i}"),
            Expr::Literal(v) => write!(f, "{v}"),
            Expr::Binary { op, left, right } => write!(f, "({left} {op} {right})"),
            Expr::Function { name, args } => {
                write!(
                    f,
                    "{name}({})",
                    args.iter()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            Expr::IsNull { expr, negated } => {
                if *negated {
                    write!(f, "({expr} IS NOT NULL)")
                } else {
                    write!(f, "({expr} IS NULL)")
                }
            }
        }
    }
}

impl Expr {
    pub fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// 对一个批求值,输出整列。
    pub fn eval(&self, chunk: &crate::chunk::DataChunk) -> Result<Vector> {
        match self {
            Expr::Column(i) => Ok(chunk.column(*i).clone()),
            Expr::Literal(v) => broadcast(v, chunk.num_rows()),
            Expr::Binary { op, left, right } => {
                let l = left.eval(chunk)?;
                let r = right.eval(chunk)?;
                eval_binary(*op, &l, &r)
            }
            Expr::Function { name, args } => {
                let cols = args
                    .iter()
                    .map(|a| a.eval(chunk))
                    .collect::<Result<Vec<_>>>()?;
                crate::functions::eval_function(name, &cols)
            }
            Expr::IsNull { expr, negated } => {
                let v = expr.eval(chunk)?;
                let out: Vec<bool> = (0..v.len()).map(|i| v.null_at(i) != *negated).collect();
                Ok(Vector::bools(out))
            }
        }
    }

    /// 静态类型推导(计划期生成输出 schema 用);Null 字面量类型未知返回 None。
    pub fn return_type(&self, input: &[common::schema::ColumnDesc]) -> Result<Option<DataType>> {
        match self {
            Expr::Column(i) => input
                .get(*i)
                .map(|c| Some(c.ty.clone()))
                .ok_or_else(|| Error::invalid_argument(format!("column #{i} out of range"))),
            Expr::Literal(v) => Ok(v.data_type()),
            Expr::Function { name, args } => {
                let arg_types = args
                    .iter()
                    .map(|a| a.return_type(input))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Some(crate::functions::function_return_type(
                    name, &arg_types,
                )?))
            }
            Expr::IsNull { .. } => Ok(Some(DataType::Boolean)),
            Expr::Binary { op, left, right } => {
                let lt = left.return_type(input)?;
                let rt = right.return_type(input)?;
                if op.is_comparison() || op.is_like() {
                    return Ok(Some(DataType::Boolean));
                }
                if op.is_logic() {
                    for t in [lt, rt] {
                        if let Some(t) = t {
                            if t != DataType::Boolean {
                                return Err(Error::sql(format!(
                                    "AND/OR require boolean operands, got {t:?}"
                                )));
                            }
                        }
                    }
                    return Ok(Some(DataType::Boolean));
                }
                // 算术:Int64 与 Float64 混算提升为 Float64;日期 ± 整数天数
                match (&lt, &rt) {
                    (Some(DataType::Float64), _) | (_, Some(DataType::Float64))
                        if op.is_arithmetic() =>
                    {
                        Ok(Some(DataType::Float64))
                    }
                    (Some(DataType::Int64), Some(DataType::Int64)) if op.is_arithmetic() => {
                        Ok(Some(DataType::Int64))
                    }
                    // 日期算术:DATE ± BIGINT(天)→ DATE;DATE - DATE → BIGINT 天数
                    (Some(DataType::Date), Some(DataType::Int64))
                        if matches!(op, BinaryOp::Add | BinaryOp::Sub) =>
                    {
                        Ok(Some(DataType::Date))
                    }
                    (Some(DataType::Int64), Some(DataType::Date)) if *op == BinaryOp::Add => {
                        Ok(Some(DataType::Date))
                    }
                    (Some(DataType::Date), Some(DataType::Date)) if *op == BinaryOp::Sub => {
                        Ok(Some(DataType::Int64))
                    }
                    _ => Err(Error::sql(format!(
                        "type mismatch in arithmetic: {lt:?} {} {rt:?}",
                        op.symbol()
                    ))),
                }
            }
        }
    }
}

/// 标量广播为整列。
pub fn broadcast(v: &ScalarValue, len: usize) -> Result<Vector> {
    // List/Vector 字面量的目标类型从值本身推导(data_type)
    let ty = v.data_type();
    if let Some(ty) = ty {
        if !ty.is_scalar() {
            return Vector::from_scalars(ty, std::iter::repeat_n(v.clone(), len));
        }
    }
    match v {
        ScalarValue::Null => Ok(Vector::all_null(DataType::Boolean, len)),
        ScalarValue::Boolean(_) => {
            Vector::from_scalars(DataType::Boolean, std::iter::repeat_n(v.clone(), len))
        }
        ScalarValue::Int64(_) => {
            Vector::from_scalars(DataType::Int64, std::iter::repeat_n(v.clone(), len))
        }
        ScalarValue::Float64(_) => {
            Vector::from_scalars(DataType::Float64, std::iter::repeat_n(v.clone(), len))
        }
        ScalarValue::Date(_) => {
            Vector::from_scalars(DataType::Date, std::iter::repeat_n(v.clone(), len))
        }
        ScalarValue::Utf8(_) => {
            Vector::from_scalars(DataType::Utf8, std::iter::repeat_n(v.clone(), len))
        }
        // Binary/List/Vector 已由上方非标量分支处理;此处仅为穷尽性
        ScalarValue::Binary(_) | ScalarValue::List(_) | ScalarValue::Vector(_) => {
            unreachable!("non-scalar values handled above")
        }
    }
}

/// 向量化二元内核:按操作数类型分派到紧密循环。
pub fn eval_binary(op: BinaryOp, l: &Vector, r: &Vector) -> Result<Vector> {
    if l.len() != r.len() {
        return Err(Error::invalid_argument(format!(
            "operand length mismatch: {} vs {}",
            l.len(),
            r.len()
        )));
    }
    if op.is_logic() {
        return logic_kernel(op, l, r);
    }
    match (l.data_type(), r.data_type()) {
        (DataType::Int64, DataType::Int64) => {
            if op.is_comparison() {
                compare_kernel(l, r, |a, b| order_op(op, a.cmp(&b)))
            } else {
                int64_arith_kernel(op, l, r)
            }
        }
        (DataType::Float64, DataType::Float64) => {
            if op.is_comparison() {
                compare_kernel_f64(l, r, |a, b| {
                    order_op(op, a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal))
                })
            } else {
                float64_arith_kernel(op, l, r)
            }
        }
        (DataType::Int64, DataType::Float64) => {
            let l = promote_to_f64(l);
            eval_binary(op, &l, r)
        }
        (DataType::Float64, DataType::Int64) => {
            let r = promote_to_f64(r);
            eval_binary(op, l, &r)
        }
        (DataType::Date, DataType::Int64) if op.is_arithmetic() || op == BinaryOp::Sub => {
            date_shift_kernel(op, l, r)
        }
        (DataType::Int64, DataType::Date) if op == BinaryOp::Add => date_shift_kernel(op, r, l),
        (DataType::Date, DataType::Date) if op == BinaryOp::Sub => {
            // 日期差:返回天数(Int64)
            let out: Vec<i64> = (0..l.len())
                .map(|i| (l.datum(i).to_scalar(), r.datum(i).to_scalar()))
                .map(|(a, b)| match (a, b) {
                    (ScalarValue::Date(x), ScalarValue::Date(y)) => (x - y) as i64,
                    _ => 0,
                })
                .collect();
            let nulls = combine_null_masks(l, r, out.len());
            Vector::new(ColumnData::Int64(out), nulls)
        }
        (DataType::Date, DataType::Date) if op.is_comparison() => values_compare_kernel(op, l, r),
        // Date 列与字符串字面量比较:字符串按日期解析('1995-03-05' 风格)
        (DataType::Date, DataType::Utf8) if op.is_comparison() => {
            let r = coerce_nonscalar_side(r, &DataType::Date)?;
            eval_binary(op, l, &r)
        }
        (DataType::Utf8, DataType::Date) if op.is_comparison() => {
            let l = coerce_nonscalar_side(l, &DataType::Date)?;
            eval_binary(op, &l, r)
        }
        (DataType::Utf8, DataType::Utf8) if op.is_like() => like_kernel(op, l, r),
        (DataType::Utf8, DataType::Utf8) if op.is_comparison() => utf8_compare_kernel(op, l, r),
        // Binary / 数组:相等与序比较走通用 compare_values(null 参与 → false)
        (DataType::Binary, DataType::Binary) if op.is_comparison() => {
            values_compare_kernel(op, l, r)
        }
        (DataType::Array(_), DataType::Array(_)) if op.is_comparison() => {
            values_compare_kernel(op, l, r)
        }
        // LOB/数组/向量列与字符串字面量比较:字符串按目标类型解析(unknown literal)
        (DataType::Binary | DataType::Array(_) | DataType::Vector(_), DataType::Utf8)
            if op.is_comparison() =>
        {
            let r = coerce_nonscalar_side(r, &l.data_type())?;
            eval_binary(op, l, &r)
        }
        (DataType::Utf8, DataType::Binary | DataType::Array(_) | DataType::Vector(_))
            if op.is_comparison() =>
        {
            let l = coerce_nonscalar_side(l, &r.data_type())?;
            eval_binary(op, &l, r)
        }
        (a, b) => Err(Error::sql(format!(
            "unsupported operand types: {a:?} {} {b:?}",
            op.symbol()
        ))),
    }
}

/// 把 Utf8 列按目标非标量类型逐行解析(失败即报错)。
fn coerce_nonscalar_side(v: &Vector, target: &DataType) -> Result<Vector> {
    let mut items = Vec::with_capacity(v.len());
    for i in 0..v.len() {
        match v.datum(i) {
            Datum::Null => items.push(ScalarValue::Null),
            Datum::Utf8(s) => items.push(ScalarValue::from_text(target.clone(), s)?),
            other => {
                return Err(Error::invalid_argument(format!(
                    "cannot coerce {other:?} to {target:?}"
                )));
            }
        }
    }
    Vector::from_scalars(target.clone(), items)
}

/// 日期 ± 整数天数(Date ± Int64 → Date;probe 侧 Int64 在左时交换后调用)。
fn date_shift_kernel(op: BinaryOp, date_v: &Vector, days_v: &Vector) -> Result<Vector> {
    let shift_right = matches!(op, BinaryOp::Add);
    let mut out = Vec::with_capacity(date_v.len());
    for i in 0..date_v.len() {
        let (Datum::Date(d), Datum::Int64(n)) = (date_v.datum(i), days_v.datum(i)) else {
            return Err(Error::invalid_argument(
                "date arithmetic requires DATE and BIGINT operands",
            ));
        };
        let shifted = if shift_right {
            d.checked_add(n as i32)
        } else {
            d.checked_sub(n as i32)
        };
        out.push(ScalarValue::Date(
            shifted.ok_or_else(|| Error::sql("date arithmetic out of range"))?,
        ));
    }
    let nulls = combine_null_masks(date_v, days_v, out.len());
    Vector::new(
        ColumnData::Date(
            out.iter()
                .map(|v| match v {
                    ScalarValue::Date(d) => *d,
                    _ => 0,
                })
                .collect(),
        ),
        nulls,
    )
}

/// LIKE 内核:% 任意序列、_ 单字符、`\` 转义下一字符;NULL 参与 → false/NULL。
fn like_matches(text: &str, pattern: &str) -> bool {
    let s: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut si = 0usize;
    let mut pi = 0usize;
    let mut star_pi = None;
    let mut star_si = 0usize;
    while si < s.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == s[si]) {
            si += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '\\' && pi + 1 < p.len() {
            if p[pi + 1] == s[si] {
                si += 1;
                pi += 2;
            } else if star_pi.is_some() {
                si = star_si + 1;
                pi = star_pi.unwrap();
            } else {
                return false;
            }
        } else if pi < p.len() && p[pi] == '%' {
            star_pi = Some(pi);
            star_si = si;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            si = star_si + 1;
            star_si += 1;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

fn like_kernel(op: BinaryOp, l: &Vector, r: &Vector) -> Result<Vector> {
    let negated = op == BinaryOp::NotLike;
    let mut out = Vec::with_capacity(l.len());
    for i in 0..l.len() {
        if l.null_at(i) || r.null_at(i) {
            out.push(false);
        } else {
            let m = like_matches(l.utf8_value(i), r.utf8_value(i));
            out.push(m != negated);
        }
    }
    Ok(Vector::bools(out))
}

#[inline]
fn order_op(op: BinaryOp, ord: std::cmp::Ordering) -> bool {
    use std::cmp::Ordering::*;
    match op {
        BinaryOp::Eq => ord == Equal,
        BinaryOp::NotEq => ord != Equal,
        BinaryOp::Lt => ord == Less,
        BinaryOp::LtEq => ord != Greater,
        BinaryOp::Gt => ord == Greater,
        BinaryOp::GtEq => ord != Less,
        _ => unreachable!("order_op called with non-comparison op"),
    }
}

/// 三值 null 合并:任一侧 null → 结果 null。
#[inline]
fn merge_nulls(a: bool, b: bool) -> bool {
    !(a || b)
}

fn combine_null_masks(l: &Vector, r: &Vector, len: usize) -> Option<crate::vector::Validity> {
    if !l.has_nulls() && !r.has_nulls() {
        return None;
    }
    let mut out = crate::vector::Validity::all_valid(len);
    for i in 0..len {
        out.set(i, merge_nulls(l.null_at(i), r.null_at(i)));
    }
    Some(out)
}

fn int64_arith_kernel(op: BinaryOp, l: &Vector, r: &Vector) -> Result<Vector> {
    let a = l.int64_values();
    let b = r.int64_values();
    let mut out = Vec::with_capacity(a.len());
    let mut err: Option<Error> = None;
    for i in 0..a.len() {
        let v = match op {
            BinaryOp::Add => a[i].checked_add(b[i]),
            BinaryOp::Sub => a[i].checked_sub(b[i]),
            BinaryOp::Mul => a[i].checked_mul(b[i]),
            BinaryOp::Div => {
                if b[i] == 0 {
                    return Err(Error::sql("division by zero"));
                }
                a[i].checked_div(b[i])
            }
            _ => unreachable!("int64_arith_kernel with non-arith op"),
        };
        match v {
            Some(v) => out.push(v),
            None => {
                err = Some(Error::sql(format!(
                    "integer overflow: {} {} {}",
                    a[i],
                    op.symbol(),
                    b[i]
                )));
                break;
            }
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    let nulls = combine_null_masks(l, r, out.len());
    Vector::new(crate::vector::ColumnData::Int64(out), nulls)
}

fn float64_arith_kernel(op: BinaryOp, l: &Vector, r: &Vector) -> Result<Vector> {
    let a = l.float64_values();
    let b = r.float64_values();
    let mut out = Vec::with_capacity(a.len());
    for i in 0..a.len() {
        let v = match op {
            BinaryOp::Add => a[i] + b[i],
            BinaryOp::Sub => a[i] - b[i],
            BinaryOp::Mul => a[i] * b[i],
            BinaryOp::Div => {
                if b[i] == 0.0 {
                    return Err(Error::sql("division by zero"));
                }
                a[i] / b[i]
            }
            _ => unreachable!("float64_arith_kernel with non-arith op"),
        };
        out.push(v);
    }
    let nulls = combine_null_masks(l, r, out.len());
    Vector::new(crate::vector::ColumnData::Float64(out), nulls)
}

fn compare_kernel(l: &Vector, r: &Vector, f: impl Fn(i64, i64) -> bool) -> Result<Vector> {
    let a = l.int64_values();
    let b = r.int64_values();
    let out: Vec<bool> = (0..a.len()).map(|i| f(a[i], b[i])).collect();
    let nulls = combine_null_masks(l, r, out.len());
    Vector::new(crate::vector::ColumnData::Bool(out), nulls)
}

fn compare_kernel_f64(l: &Vector, r: &Vector, f: impl Fn(f64, f64) -> bool) -> Result<Vector> {
    let a = l.float64_values();
    let b = r.float64_values();
    let out: Vec<bool> = (0..a.len()).map(|i| f(a[i], b[i])).collect();
    let nulls = combine_null_masks(l, r, out.len());
    Vector::new(crate::vector::ColumnData::Bool(out), nulls)
}

fn utf8_compare_kernel(op: BinaryOp, l: &Vector, r: &Vector) -> Result<Vector> {
    let out: Vec<bool> = (0..l.len())
        .map(|i| order_op(op, l.utf8_value(i).cmp(r.utf8_value(i))))
        .collect();
    let nulls = combine_null_masks(l, r, out.len());
    Vector::new(ColumnData::Bool(out), nulls)
}

/// 通用比较内核(compare_values 支持 Binary/List)。
fn values_compare_kernel(op: BinaryOp, l: &Vector, r: &Vector) -> Result<Vector> {
    let mut out = Vec::with_capacity(l.len());
    for i in 0..l.len() {
        if l.null_at(i) || r.null_at(i) {
            out.push(false);
        } else {
            let ord = crate::operators::hash_agg::compare_values(&l.scalar_at(i), &r.scalar_at(i))?;
            out.push(order_op(op, ord));
        }
    }
    Ok(Vector::bools(out))
}

/// And/Or:null 按 false 处理(3VL TODO)。
fn logic_kernel(op: BinaryOp, l: &Vector, r: &Vector) -> Result<Vector> {
    if l.data_type() != DataType::Boolean || r.data_type() != DataType::Boolean {
        return Err(Error::sql(format!(
            "AND/OR require boolean operands, got {:?} {} {:?}",
            l.data_type(),
            op.symbol(),
            r.data_type()
        )));
    }
    let a = l.bool_values();
    let b = r.bool_values();
    let mut out = Vec::with_capacity(a.len());
    match op {
        BinaryOp::And => {
            for i in 0..a.len() {
                out.push(la_or_null(l, i, a) && la_or_null(r, i, b));
            }
        }
        BinaryOp::Or => {
            for i in 0..a.len() {
                out.push(la_or_null(l, i, a) || la_or_null(r, i, b));
            }
        }
        _ => unreachable!("logic_kernel with non-logic op"),
    }
    Vector::bools(out).pipe_ok()
}

/// null 行读作 false。
#[inline]
fn la_or_null(v: &Vector, i: usize, values: &[bool]) -> bool {
    if v.null_at(i) { false } else { values[i] }
}

fn promote_to_f64(v: &Vector) -> Vector {
    Vector::float64s(v.int64_values().iter().map(|x| *x as f64).collect())
}

trait PipeOk: Sized {
    fn pipe_ok(self) -> Result<Self>;
}
impl PipeOk for Vector {
    fn pipe_ok(self) -> Result<Self> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::DataChunk;
    use crate::vector::Datum;
    use common::types::ScalarValue;

    fn chunk2(l: Vector, r: Vector) -> DataChunk {
        DataChunk::new(vec![l, r]).unwrap()
    }

    #[test]
    fn broadcast_literal() {
        let chunk = DataChunk::new(vec![Vector::int64s(vec![1, 2, 3])]).unwrap();
        let v = broadcast(&ScalarValue::Int64(7), 3).unwrap();
        assert_eq!(v.int64_values(), &[7, 7, 7]);
        let _ = &chunk;
    }

    #[test]
    fn int64_arithmetic_vectorized() {
        let chunk = chunk2(
            Vector::int64s(vec![1, 2, 3]),
            Vector::int64s(vec![10, 20, 30]),
        );
        let e = Expr::binary(BinaryOp::Add, Expr::Column(0), Expr::Column(1));
        let out = e.eval(&chunk).unwrap();
        assert_eq!(out.int64_values(), &[11, 22, 33]);
    }

    #[test]
    fn mixed_arithmetic_promotes_to_f64() {
        let chunk = chunk2(Vector::int64s(vec![1, 2]), Vector::float64s(vec![0.5, 0.5]));
        let e = Expr::binary(BinaryOp::Mul, Expr::Column(0), Expr::Column(1));
        let out = e.eval(&chunk).unwrap();
        assert_eq!(out.data_type(), DataType::Float64);
        assert_eq!(out.float64_values(), &[0.5, 1.0]);
    }

    #[test]
    fn division_by_zero_errors() {
        let chunk = chunk2(Vector::int64s(vec![1]), Vector::int64s(vec![0]));
        let e = Expr::binary(BinaryOp::Div, Expr::Column(0), Expr::Column(1));
        assert!(e.eval(&chunk).is_err());
    }

    #[test]
    fn integer_overflow_errors() {
        let chunk = chunk2(Vector::int64s(vec![i64::MAX]), Vector::int64s(vec![1]));
        let e = Expr::binary(BinaryOp::Add, Expr::Column(0), Expr::Column(1));
        assert!(e.eval(&chunk).is_err());
    }

    #[test]
    fn comparison_with_null_propagates() {
        let l = Vector::from_scalars(
            DataType::Int64,
            vec![ScalarValue::Int64(1), ScalarValue::Null],
        )
        .unwrap();
        let r = Vector::int64s(vec![1, 1]);
        let chunk = chunk2(l, r);
        let e = Expr::binary(BinaryOp::Eq, Expr::Column(0), Expr::Column(1));
        let out = e.eval(&chunk).unwrap();
        assert_eq!(out.bool_values(), &[true, false]);
        assert!(!out.null_at(0));
        assert!(out.null_at(1));
        assert_eq!(out.datum(1), Datum::Null);
    }

    #[test]
    fn utf8_comparison() {
        let chunk = chunk2(
            Vector::utf8s(vec!["apple".into(), "bee".into()]),
            Vector::utf8s(vec!["ant".into(), "bee".into()]),
        );
        let e = Expr::binary(BinaryOp::Lt, Expr::Column(0), Expr::Column(1));
        let out = e.eval(&chunk).unwrap();
        assert_eq!(out.bool_values(), &[false, false]);
        let e2 = Expr::binary(BinaryOp::Gt, Expr::Column(0), Expr::Column(1));
        let out2 = e2.eval(&chunk).unwrap();
        assert_eq!(out2.bool_values(), &[true, false]);
    }

    #[test]
    fn logic_and_or() {
        let chunk = chunk2(
            Vector::bools(vec![true, false, true]),
            Vector::bools(vec![true, true, false]),
        );
        let e = Expr::binary(BinaryOp::And, Expr::Column(0), Expr::Column(1));
        assert_eq!(e.eval(&chunk).unwrap().bool_values(), &[true, false, false]);
        let e = Expr::binary(BinaryOp::Or, Expr::Column(0), Expr::Column(1));
        assert_eq!(e.eval(&chunk).unwrap().bool_values(), &[true, true, true]);
    }

    #[test]
    fn nested_expression() {
        // (c0 + 1) * 2
        let chunk = DataChunk::new(vec![Vector::int64s(vec![1, 2, 3])]).unwrap();
        let e = Expr::binary(
            BinaryOp::Mul,
            Expr::binary(
                BinaryOp::Add,
                Expr::Column(0),
                Expr::Literal(ScalarValue::Int64(1)),
            ),
            Expr::Literal(ScalarValue::Int64(2)),
        );
        let out = e.eval(&chunk).unwrap();
        assert_eq!(out.int64_values(), &[4, 6, 8]);
    }

    #[test]
    fn return_type_inference() {
        let input = vec![common::schema::ColumnDesc::new("a", DataType::Int64, true)];
        let e = Expr::binary(
            BinaryOp::Add,
            Expr::Column(0),
            Expr::Literal(ScalarValue::Float64(1.0)),
        );
        assert_eq!(e.return_type(&input).unwrap(), Some(DataType::Float64));
        let e = Expr::binary(
            BinaryOp::Lt,
            Expr::Column(0),
            Expr::Literal(ScalarValue::Int64(1)),
        );
        assert_eq!(e.return_type(&input).unwrap(), Some(DataType::Boolean));
    }
}
