//! 标量函数:注册表、类型推导与逐行求值。
//!
//! 实现为"参数列 → 逐行标量计算"的内核(字符串/条件类函数本质是逐行的,
//! 向量化收益有限;数值函数同样走统一路径,保持实现简单)。
//!
//! NULL 语义(PG 对齐):多数函数任一参数为 NULL → 结果 NULL;
//! COALESCE 取第一个非 NULL;CONCAT 跳过 NULL;NULLIF 相等则 NULL。

use common::error::{Error, Result};
use common::types::{DataType, ScalarValue};

use crate::vector::Vector;

/// 支持的标量函数名(小写)。
pub const SCALAR_FUNCTIONS: &[&str] = &[
    "upper", "lower", "length", "trim", "reverse", "concat", "coalesce", "ifnull", "nullif", "abs",
    "round", "floor", "ceil", "ceiling", "sqrt", "mod", "power",
];

pub fn is_scalar_function(name: &str) -> bool {
    SCALAR_FUNCTIONS.contains(&name)
}

/// 静态类型推导。arg_types 中 None 表示 NULL 字面量/未知。
pub fn function_return_type(name: &str, args: &[Option<DataType>]) -> Result<DataType> {
    let arity_err = |want: &str| {
        Err(Error::sql(format!(
            "function {name}() requires {want} argument(s), got {}",
            args.len()
        )))
    };
    match name {
        "upper" | "lower" | "trim" | "reverse" => {
            if args.len() != 1 {
                return arity_err("1");
            }
            require_text(name, args[0].clone())
        }
        "length" => {
            if args.len() != 1 {
                return arity_err("1");
            }
            require_text(name, args[0].clone())?;
            Ok(DataType::Int64)
        }
        "concat" => Ok(DataType::Utf8),
        "coalesce" | "ifnull" => {
            if args.is_empty() {
                return arity_err("at least 1");
            }
            Ok(args
                .iter()
                .find_map(|t| t.clone())
                .unwrap_or(DataType::Utf8))
        }
        "nullif" => {
            if args.len() != 2 {
                return arity_err("2");
            }
            Ok(args[0].clone().unwrap_or(DataType::Utf8))
        }
        "abs" => {
            if args.len() != 1 {
                return arity_err("1");
            }
            match args[0] {
                Some(DataType::Int64) | Some(DataType::Float64) | None => {
                    Ok(args[0].clone().unwrap_or(DataType::Int64))
                }
                Some(ref t) => Err(numeric_err(name, t)),
            }
        }
        "round" => {
            if args.is_empty() || args.len() > 2 {
                return arity_err("1 or 2");
            }
            for t in args.iter().flatten() {
                if !matches!(t, DataType::Int64 | DataType::Float64) {
                    return Err(numeric_err(name, t));
                }
            }
            // 双参(带小数位)或浮点参数 → Float64;单整型参数 → 原样
            if args.len() == 2 || matches!(args[0], Some(DataType::Float64) | None) {
                Ok(DataType::Float64)
            } else {
                Ok(DataType::Int64)
            }
        }
        "floor" | "ceil" | "ceiling" => {
            if args.len() != 1 {
                return arity_err("1");
            }
            match args[0] {
                Some(DataType::Int64) | None => Ok(DataType::Int64),
                Some(DataType::Float64) => Ok(DataType::Float64),
                Some(ref t) => Err(numeric_err(name, t)),
            }
        }
        "sqrt" | "power" => {
            if name == "sqrt" && args.len() != 1 {
                return arity_err("1");
            }
            if name == "power" && args.len() != 2 {
                return arity_err("2");
            }
            for t in args.iter().flatten() {
                if !matches!(t, DataType::Int64 | DataType::Float64) {
                    return Err(numeric_err(name, t));
                }
            }
            Ok(DataType::Float64)
        }
        "mod" => {
            if args.len() != 2 {
                return arity_err("2");
            }
            for t in args.iter().flatten() {
                if !matches!(t, DataType::Int64 | DataType::Float64) {
                    return Err(numeric_err(name, t));
                }
            }
            if matches!(args[0], Some(DataType::Float64))
                || matches!(args[1], Some(DataType::Float64))
            {
                Ok(DataType::Float64)
            } else {
                Ok(DataType::Int64)
            }
        }
        other => Err(Error::not_supported(format!("function {other}()"))),
    }
}

fn require_text(name: &str, t: Option<DataType>) -> Result<DataType> {
    match t {
        Some(DataType::Utf8) | None => Ok(DataType::Utf8),
        Some(ref t) => Err(Error::sql(format!(
            "function {name}() requires TEXT argument, got {}",
            t.name()
        ))),
    }
}

fn numeric_err(name: &str, t: &DataType) -> Error {
    Error::sql(format!(
        "function {name}() requires numeric argument, got {}",
        t.name()
    ))
}

/// 逐行求值:args 为等长的参数列向量。
pub fn eval_function(name: &str, args: &[Vector]) -> Result<Vector> {
    let len = args.first().map_or(0, |a| a.len());
    for a in args {
        if a.len() != len {
            return Err(Error::invalid_argument(format!(
                "function {name}(): argument length mismatch"
            )));
        }
    }
    let mut out: Vec<ScalarValue> = Vec::with_capacity(len);
    for i in 0..len {
        let vals: Vec<ScalarValue> = args.iter().map(|a| a.scalar_at(i)).collect();
        out.push(dispatch(name, &vals)?);
    }
    // 输出类型从逐行求值结果推导(首个非 NULL 值;全 NULL → BOOLEAN 全空列)。
    // 不能用参数列的 data_type 推导:NULL 字面量广播为 BOOLEAN 全空列,
    // 会带偏 COALESCE 等函数的结果类型。
    let out_type = out
        .iter()
        .find_map(|v| v.data_type())
        .unwrap_or(DataType::Boolean);
    Vector::from_scalars(out_type, out)
}

fn dispatch(name: &str, vals: &[ScalarValue]) -> Result<ScalarValue> {
    match name {
        "coalesce" => Ok(vals
            .iter()
            .find(|v| !v.is_null())
            .cloned()
            .unwrap_or(ScalarValue::Null)),
        "ifnull" => {
            if vals[0].is_null() {
                Ok(vals[1].clone())
            } else {
                Ok(vals[0].clone())
            }
        }
        "nullif" => {
            if vals[0].is_null() {
                return Ok(ScalarValue::Null);
            }
            if !vals[1].is_null() && vals[0] == vals[1] {
                Ok(ScalarValue::Null)
            } else {
                Ok(vals[0].clone())
            }
        }
        "concat" => {
            let mut s = String::new();
            for v in vals {
                // NULL 跳过(PG concat 语义)
                if v.is_null() {
                    continue;
                }
                let (_, part) = text_of(v, name)?;
                s.push_str(&part);
            }
            Ok(ScalarValue::Utf8(s))
        }
        _ => {
            // 其余函数:任一参数为 NULL → 结果 NULL
            if vals.iter().any(|v| v.is_null()) {
                return Ok(ScalarValue::Null);
            }
            match name {
                "upper" => Ok(ScalarValue::Utf8(text_of(&vals[0], name)?.1.to_uppercase())),
                "lower" => Ok(ScalarValue::Utf8(text_of(&vals[0], name)?.1.to_lowercase())),
                "trim" => Ok(ScalarValue::Utf8(
                    text_of(&vals[0], name)?.1.trim().to_string(),
                )),
                "reverse" => Ok(ScalarValue::Utf8(
                    text_of(&vals[0], name)?.1.chars().rev().collect(),
                )),
                "length" => Ok(ScalarValue::Int64(
                    text_of(&vals[0], name)?.1.chars().count() as i64,
                )),
                "abs" => match vals[0] {
                    ScalarValue::Int64(v) => v
                        .checked_abs()
                        .map(ScalarValue::Int64)
                        .ok_or_else(|| Error::sql("integer overflow in ABS")),
                    ScalarValue::Float64(v) => Ok(ScalarValue::Float64(v.abs())),
                    ref other => Err(arg_err(name, other)),
                },
                "round" => match vals {
                    [ScalarValue::Int64(v)] => Ok(ScalarValue::Int64(*v)),
                    [ScalarValue::Float64(v)] => Ok(ScalarValue::Float64(v.round())),
                    [ScalarValue::Int64(v), ScalarValue::Int64(d)] => {
                        Ok(ScalarValue::Float64(round_to(*v as f64, *d as i32)))
                    }
                    [ScalarValue::Float64(v), ScalarValue::Int64(d)] => {
                        Ok(ScalarValue::Float64(round_to(*v, *d as i32)))
                    }
                    [ScalarValue::Int64(v), ScalarValue::Float64(d)] => {
                        Ok(ScalarValue::Float64(round_to(*v as f64, *d as i32)))
                    }
                    _ => Err(arg_err(name, &vals[0])),
                },
                "floor" => match vals[0] {
                    ScalarValue::Int64(v) => Ok(ScalarValue::Int64(v)),
                    ScalarValue::Float64(v) => Ok(ScalarValue::Float64(v.floor())),
                    ref other => Err(arg_err(name, other)),
                },
                "ceil" | "ceiling" => match vals[0] {
                    ScalarValue::Int64(v) => Ok(ScalarValue::Int64(v)),
                    ScalarValue::Float64(v) => Ok(ScalarValue::Float64(v.ceil())),
                    ref other => Err(arg_err(name, other)),
                },
                "sqrt" => match vals[0] {
                    ScalarValue::Int64(v) => {
                        if v < 0 {
                            Err(Error::sql("cannot take square root of a negative number"))
                        } else {
                            Ok(ScalarValue::Float64((v as f64).sqrt()))
                        }
                    }
                    ScalarValue::Float64(v) => {
                        if v < 0.0 {
                            Err(Error::sql("cannot take square root of a negative number"))
                        } else {
                            Ok(ScalarValue::Float64(v.sqrt()))
                        }
                    }
                    ref other => Err(arg_err(name, other)),
                },
                "mod" => match vals {
                    [ScalarValue::Int64(a), ScalarValue::Int64(b)] => {
                        if *b == 0 {
                            Err(Error::sql("division by zero in MOD"))
                        } else {
                            Ok(ScalarValue::Int64(a % b))
                        }
                    }
                    [a, b] => {
                        let (x, y) = (num_of(a)?, num_of(b)?);
                        if y == 0.0 {
                            Err(Error::sql("division by zero in MOD"))
                        } else {
                            Ok(ScalarValue::Float64(x % y))
                        }
                    }
                    _ => Err(arg_err(name, &vals[0])),
                },
                "power" => match vals {
                    [a, b] => Ok(ScalarValue::Float64(num_of(a)?.powf(num_of(b)?))),
                    _ => Err(arg_err(name, &vals[0])),
                },
                other => Err(Error::not_supported(format!("function {other}()"))),
            }
        }
    }
}

fn round_to(x: f64, digits: i32) -> f64 {
    let factor = 10f64.powi(digits.clamp(-16, 16));
    (x * factor).round() / factor
}

fn arg_err(name: &str, v: &ScalarValue) -> Error {
    Error::sql(format!("function {name}(): incompatible argument {:?}", v))
}

fn num_of(v: &ScalarValue) -> Result<f64> {
    match v {
        ScalarValue::Int64(i) => Ok(*i as f64),
        ScalarValue::Float64(f) => Ok(*f),
        other => Err(arg_err("numeric function", other)),
    }
}

/// (归属函数名, 文本内容);非文本报错。
fn text_of(v: &ScalarValue, name: &str) -> Result<(String, String)> {
    match v {
        ScalarValue::Utf8(s) => Ok((name.to_string(), s.clone())),
        other => Err(arg_err(name, other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::vector::Vector;
    use common::types::DataType;

    fn col(values: Vec<ScalarValue>) -> Vector {
        Vector::from_scalars(
            values
                .first()
                .and_then(|v| v.data_type())
                .unwrap_or(DataType::Int64),
            values,
        )
        .unwrap()
    }

    fn eval(name: &str, args: Vec<Vector>) -> Vec<ScalarValue> {
        let v = eval_function(name, &args).unwrap();
        (0..v.len()).map(|i| v.scalar_at(i)).collect()
    }

    #[test]
    fn string_functions() {
        let out = eval(
            "upper",
            vec![col(vec![
                ScalarValue::Utf8("abc".into()),
                ScalarValue::Null,
            ])],
        );
        assert_eq!(
            out,
            vec![ScalarValue::Utf8("ABC".into()), ScalarValue::Null]
        );

        let out = eval(
            "length",
            vec![col(vec![
                ScalarValue::Utf8("hello".into()),
                ScalarValue::Utf8("中文".into()),
            ])],
        );
        assert_eq!(out, vec![ScalarValue::Int64(5), ScalarValue::Int64(2)]);

        let out = eval("trim", vec![col(vec![ScalarValue::Utf8("  x  ".into())])]);
        assert_eq!(out, vec![ScalarValue::Utf8("x".into())]);
    }

    #[test]
    fn concat_skips_nulls() {
        let out = eval(
            "concat",
            vec![
                col(vec![ScalarValue::Utf8("a".into())]),
                col(vec![ScalarValue::Null]),
                col(vec![ScalarValue::Utf8("b".into())]),
            ],
        );
        assert_eq!(out, vec![ScalarValue::Utf8("ab".into())]);
    }

    #[test]
    fn coalesce_first_non_null() {
        let out = eval(
            "coalesce",
            vec![
                col(vec![ScalarValue::Null]),
                col(vec![ScalarValue::Int64(5)]),
                col(vec![ScalarValue::Int64(7)]),
            ],
        );
        assert_eq!(out, vec![ScalarValue::Int64(5)]);
    }

    #[test]
    fn nullif_compares() {
        let out = eval(
            "nullif",
            vec![
                col(vec![ScalarValue::Int64(1), ScalarValue::Int64(3)]),
                col(vec![ScalarValue::Int64(1), ScalarValue::Int64(4)]),
            ],
        );
        assert_eq!(out, vec![ScalarValue::Null, ScalarValue::Int64(3)]);
    }

    #[test]
    fn math_functions() {
        let out = eval(
            "abs",
            vec![col(vec![ScalarValue::Int64(-5), ScalarValue::Null])],
        );
        assert_eq!(out, vec![ScalarValue::Int64(5), ScalarValue::Null]);

        let out = eval(
            "round",
            vec![
                col(vec![ScalarValue::Float64(3.567)]),
                col(vec![ScalarValue::Int64(2)]),
            ],
        );
        assert_eq!(out, vec![ScalarValue::Float64(3.57)]);

        let out = eval("sqrt", vec![col(vec![ScalarValue::Float64(9.0)])]);
        assert_eq!(out, vec![ScalarValue::Float64(3.0)]);
        assert!(eval_function("sqrt", &vec![col(vec![ScalarValue::Float64(-1.0)])]).is_err());

        let out = eval(
            "mod",
            vec![
                col(vec![ScalarValue::Int64(7)]),
                col(vec![ScalarValue::Int64(3)]),
            ],
        );
        assert_eq!(out, vec![ScalarValue::Int64(1)]);
    }

    #[test]
    fn return_type_inference() {
        assert_eq!(
            function_return_type("length", &[Some(DataType::Utf8)]).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            function_return_type("abs", &[Some(DataType::Float64)]).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            function_return_type(
                "coalesce",
                &[None, Some(DataType::Int64), Some(DataType::Int64)]
            )
            .unwrap(),
            DataType::Int64
        );
        assert!(function_return_type("length", &[Some(DataType::Int64)]).is_err());
        assert!(function_return_type("no_such_fn", &[]).is_err());
    }
}
