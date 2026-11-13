//! 数据类型与标量值。
//!
//! 基础类型:BOOLEAN / BIGINT / FLOAT8 / TEXT(兼作 CLOB);
//! LOB:BLOB(二进制,VARBINARY/BINARY 亦映射至此);
//! 数组:任意标量类型的一维数组 `BIGINT[]` / `TEXT[]`(存储为值的列表);
//! 向量:固定维度浮点 `VECTOR(n)`(面向 embedding 场景,f32 存储)。
//!
//! 编码协议见 codec;文本格式对齐 PG:bytea `\x...`、数组 `{1,2,"a"}`、
//! 向量 `[1.0,2.0,3.0]`。

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int64,
    Float64,
    Utf8,
    /// 日期(内部 = 距 1970-01-01 的天数,i32)。文本格式 YYYY-MM-DD。
    Date,
    /// 二进制 LOB(BLOB / BYTEA / VARBINARY)。
    Binary,
    /// 一维数组(元素必须是标量类型;嵌套数组不支持)。
    Array(Box<DataType>),
    /// 固定维度浮点向量(embedding)。
    Vector(usize),
}

impl DataType {
    /// PG 风格类型名(用于 RowDescription / DDL / 错误消息)。
    pub fn name(&self) -> String {
        match self {
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::Int64 => "BIGINT".to_string(),
            DataType::Float64 => "DOUBLE PRECISION".to_string(),
            DataType::Utf8 => "TEXT".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Binary => "BYTEA".to_string(),
            DataType::Array(elem) => format!("{}[]", elem.name()),
            DataType::Vector(dim) => format!("VECTOR({dim})"),
        }
    }

    /// 是否为标量类型(非 LOB/数组/向量)。
    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            DataType::Boolean
                | DataType::Int64
                | DataType::Float64
                | DataType::Utf8
                | DataType::Date
        )
    }

    pub fn from_name(name: &str) -> Option<DataType> {
        match name.to_ascii_uppercase().as_str() {
            "BOOLEAN" | "BOOL" => Some(DataType::Boolean),
            "BIGINT" | "INT8" => Some(DataType::Int64),
            "DOUBLE PRECISION" | "FLOAT8" | "DOUBLE" => Some(DataType::Float64),
            "TEXT" | "VARCHAR" | "STRING" => Some(DataType::Utf8),
            "DATE" => Some(DataType::Date),
            "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => Some(DataType::Binary),
            _ => None,
        }
    }
}

/// 标量值(单值),是列存 Vector 的最小单元。Null 不携带类型。
#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    /// 日期:距 1970-01-01 的天数。
    Date(i32),
    /// 二进制 LOB。
    Binary(Vec<u8>),
    /// 数组(元素可为 NULL)。
    List(Vec<ScalarValue>),
    /// 定长浮点向量。
    Vector(Vec<f32>),
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (Self::Int64(a), Self::Int64(b)) => a == b,
            (Self::Float64(a), Self::Float64(b)) => a == b || (a.is_nan() && b.is_nan()),
            (Self::Utf8(a), Self::Utf8(b)) => a == b,
            (Self::Date(a), Self::Date(b)) => a == b,
            (Self::Binary(a), Self::Binary(b)) => a == b,
            (Self::List(a), Self::List(b)) => a == b,
            (Self::Vector(a), Self::Vector(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|(x, y)| x.to_bits() == y.to_bits())
            }
            _ => false,
        }
    }
}

impl Eq for ScalarValue {}

impl std::hash::Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // NaN == NaN(见 PartialEq),哈希同样把 NaN 归一
        match self {
            ScalarValue::Null => state.write_u8(0),
            ScalarValue::Boolean(b) => {
                state.write_u8(1);
                b.hash(state);
            }
            ScalarValue::Int64(v) => {
                state.write_u8(2);
                v.hash(state);
            }
            ScalarValue::Float64(v) => {
                state.write_u8(3);
                (if v.is_nan() { f64::NAN } else { *v })
                    .to_bits()
                    .hash(state);
            }
            ScalarValue::Utf8(s) => {
                state.write_u8(4);
                s.hash(state);
            }
            ScalarValue::Date(d) => {
                state.write_u8(5);
                d.hash(state);
            }
            ScalarValue::Binary(b) => {
                state.write_u8(6);
                b.hash(state);
            }
            ScalarValue::List(items) => {
                state.write_u8(7);
                items.hash(state);
            }
            ScalarValue::Vector(vs) => {
                state.write_u8(8);
                state.write_u64(vs.len() as u64);
                for v in vs {
                    v.to_bits().hash(state);
                }
            }
        }
    }
}

impl ScalarValue {
    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }

    /// Null 返回 None;否则返回其类型。List 的元素类型取第一个非 NULL 元素推导。
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Boolean(_) => Some(DataType::Boolean),
            ScalarValue::Int64(_) => Some(DataType::Int64),
            ScalarValue::Float64(_) => Some(DataType::Float64),
            ScalarValue::Utf8(_) => Some(DataType::Utf8),
            ScalarValue::Date(_) => Some(DataType::Date),
            ScalarValue::Binary(_) => Some(DataType::Binary),
            ScalarValue::List(items) => items
                .iter()
                .find_map(|v| v.data_type())
                .map(|t| DataType::Array(Box::new(t))),
            ScalarValue::Vector(vs) => Some(DataType::Vector(vs.len())),
        }
    }

    /// PG 文本格式输出;Null → None(DataRow 中直接编码为 NULL)。
    pub fn to_text(&self) -> Option<String> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Boolean(b) => Some(b.to_string()),
            ScalarValue::Int64(v) => Some(v.to_string()),
            ScalarValue::Float64(v) => Some(format!("{v}")),
            ScalarValue::Utf8(s) => Some(s.clone()),
            ScalarValue::Date(d) => {
                let (y, m, dd) = civil_from_days(*d);
                Some(format!("{y:04}-{m:02}-{dd:02}"))
            }
            ScalarValue::Binary(b) => Some(format!(
                "\\x{}",
                b.iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            )),
            ScalarValue::List(items) => Some(format_array(items)),
            ScalarValue::Vector(vs) => Some(format!(
                "[{}]",
                vs.iter()
                    .map(|v| format!("{v}"))
                    .collect::<Vec<_>>()
                    .join(",")
            )),
        }
    }

    /// 从文本反解指定类型的值(服务端接收参数 / 字符串字面量收敛用)。
    pub fn from_text(ty: DataType, s: &str) -> Result<Self> {
        match ty {
            DataType::Boolean => Ok(ScalarValue::Boolean(
                s.eq_ignore_ascii_case("t") || s.eq_ignore_ascii_case("true") || s == "1",
            )),
            DataType::Int64 => s
                .parse::<i64>()
                .map(ScalarValue::Int64)
                .map_err(|e| Error::invalid_argument(format!("bad value '{s}' for BIGINT: {e}"))),
            DataType::Float64 => s
                .parse::<f64>()
                .map(ScalarValue::Float64)
                .map_err(|e| Error::invalid_argument(format!("bad value '{s}' for DOUBLE: {e}"))),
            DataType::Utf8 => Ok(ScalarValue::Utf8(s.to_string())),
            DataType::Date => parse_date_text(s),
            DataType::Binary => parse_binary_text(s),
            DataType::Array(elem) => parse_array_text(&elem, s),
            DataType::Vector(dim) => parse_vector_text(dim, s),
        }
    }
}

// ---------- 公历日期 <-> 天数(Hinnant 算法;纪元 = 1970-01-01) ----------

/// 天数 → (年, 月, 日)。该算法对任意 i32 天数有效(前推后拉 500 万年+)。
pub fn civil_from_days(z: i32) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// (年, 月, 日) → 天数。
pub fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 { y - 1 } else { y } as i64;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d as u64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    (era * 146_097 + doe as i64) as i32 - 719_468
}

/// `YYYY-MM-DD` → ScalarValue::Date。
pub fn parse_date_text(s: &str) -> Result<ScalarValue> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(Error::invalid_argument(format!(
            "bad date literal '{s}' (expected YYYY-MM-DD)"
        )));
    }
    let y: i32 = parts[0]
        .parse()
        .map_err(|_| Error::invalid_argument(format!("bad date literal '{s}': invalid year")))?;
    let m: u32 = parts[1]
        .parse()
        .map_err(|_| Error::invalid_argument(format!("bad date literal '{s}': invalid month")))?;
    let d: u32 = parts[2]
        .parse()
        .map_err(|_| Error::invalid_argument(format!("bad date literal '{s}': invalid day")))?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(Error::invalid_argument(format!(
            "bad date literal '{s}': month/day out of range"
        )));
    }
    Ok(ScalarValue::Date(days_from_civil(y, m, d)))
}

impl std::fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.to_text() {
            Some(s) => f.write_str(&s),
            None => f.write_str("NULL"),
        }
    }
}

/// PG 数组文本格式:`{1,2,NULL}`,字符串元素含特殊字符时加引号转义。
fn format_array(items: &[ScalarValue]) -> String {
    let mut out = String::from("{");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match item {
            ScalarValue::Null => out.push_str("NULL"),
            ScalarValue::Utf8(s) if needs_array_quotes(s) => {
                out.push('"');
                for ch in s.chars() {
                    if ch == '"' || ch == '\\' {
                        out.push('\\');
                    }
                    out.push(ch);
                }
                out.push('"');
            }
            ScalarValue::Utf8(s) => out.push_str(s),
            other => out.push_str(&other.to_text().unwrap_or_else(|| "NULL".into())),
        }
    }
    out.push('}');
    out
}

fn needs_array_quotes(s: &str) -> bool {
    s.is_empty()
        || s.eq_ignore_ascii_case("null")
        || s.chars()
            .any(|ch| matches!(ch, '"' | '\\' | '{' | '}' | ',') || ch.is_whitespace())
}

/// bytea 文本:`\xEC` 十六进制(允许省略 `\x` 前缀)。
fn parse_binary_text(s: &str) -> Result<ScalarValue> {
    let hex = s
        .strip_prefix("\\x")
        .or_else(|| s.strip_prefix("\\X"))
        .unwrap_or(s);
    if hex.len() % 2 != 0 {
        return Err(Error::invalid_argument(format!(
            "bad bytea literal '{s}': odd hex length"
        )));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for pair in bytes.chunks(2) {
        let hi = (pair[0] as char)
            .to_digit(16)
            .ok_or_else(|| Error::invalid_argument(format!("bad bytea literal '{s}'")))?;
        let lo = (pair[1] as char)
            .to_digit(16)
            .ok_or_else(|| Error::invalid_argument(format!("bad bytea literal '{s}'")))?;
        out.push((hi * 16 + lo) as u8);
    }
    Ok(ScalarValue::Binary(out))
}

/// 数组文本:`{1,2,NULL}` / `{"a","b c"}`。嵌套数组不支持。
fn parse_array_text(elem_ty: &DataType, s: &str) -> Result<ScalarValue> {
    if elem_ty.is_scalar() {
        // fallthrough
    } else {
        return Err(Error::not_supported("nested array types"));
    }
    let inner = s
        .trim()
        .strip_prefix('{')
        .and_then(|r| r.strip_suffix('}'))
        .ok_or_else(|| {
            Error::invalid_argument(format!("bad array literal '{s}' (expected '{{...}}')"))
        })?;
    let mut items = Vec::new();
    if inner.trim().is_empty() {
        return Ok(ScalarValue::List(items));
    }
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escaped = false;
    let mut elem_quoted = false;
    for ch in inner.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quotes => {
                escaped = true;
            }
            '"' => {
                in_quotes = !in_quotes;
                elem_quoted = true;
            }
            ',' if !in_quotes => {
                items.push(parse_array_elem(elem_ty, &current, elem_quoted)?);
                current.clear();
                elem_quoted = false;
            }
            _ => {
                current.push(ch);
            }
        }
    }
    items.push(parse_array_elem(elem_ty, &current, elem_quoted)?);
    Ok(ScalarValue::List(items))
}

fn parse_array_elem(elem_ty: &DataType, raw: &str, quoted: bool) -> Result<ScalarValue> {
    if !quoted && raw.trim().eq_ignore_ascii_case("null") {
        return Ok(ScalarValue::Null);
    }
    // 去掉未引用元素的空白(PG 语义:`{1, 2}` 与 `{1,2}` 等价)
    let raw = if quoted { raw } else { raw.trim() };
    ScalarValue::from_text(elem_ty.clone(), raw)
}

/// 向量文本:`[1.0,2.0,3.0]`(维度必须匹配)。
fn parse_vector_text(dim: usize, s: &str) -> Result<ScalarValue> {
    let inner = s
        .trim()
        .strip_prefix('[')
        .and_then(|r| r.strip_suffix(']'))
        .ok_or_else(|| {
            Error::invalid_argument(format!(
                "bad vector literal '{s}' (expected '[f32,f32,...]' with dim {dim})"
            ))
        })?;
    let mut vs = Vec::with_capacity(dim);
    for part in inner.split(',') {
        let v: f32 = part.trim().parse().map_err(|_| {
            Error::invalid_argument(format!("bad vector literal '{s}': invalid float"))
        })?;
        vs.push(v);
    }
    if vs.len() != dim {
        return Err(Error::invalid_argument(format!(
            "bad vector literal '{s}': expected dim {dim}, got {}",
            vs.len()
        )));
    }
    Ok(ScalarValue::Vector(vs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_text_roundtrip() {
        let v = ScalarValue::Binary(vec![0xec, 0x48, 0x00, 0xff]);
        let text = v.to_text().unwrap();
        assert_eq!(text, "\\xec4800ff");
        let back = ScalarValue::from_text(DataType::Binary, &text).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn array_text_roundtrip_strings_and_nulls() {
        let v = ScalarValue::List(vec![
            ScalarValue::Int64(1),
            ScalarValue::Null,
            ScalarValue::Int64(-3),
        ]);
        let text = v.to_text().unwrap();
        assert_eq!(text, "{1,NULL,-3}");
        let back =
            ScalarValue::from_text(DataType::Array(Box::new(DataType::Int64)), &text).unwrap();
        assert_eq!(back, v);

        let sv = ScalarValue::List(vec![
            ScalarValue::Utf8("a".into()),
            ScalarValue::Utf8("b c,\"x\"".into()),
            ScalarValue::Null,
            ScalarValue::Utf8("".into()),
        ]);
        let text = sv.to_text().unwrap();
        let back =
            ScalarValue::from_text(DataType::Array(Box::new(DataType::Utf8)), &text).unwrap();
        assert_eq!(back, sv);
    }

    #[test]
    fn vector_text_roundtrip_and_dim_check() {
        let v = ScalarValue::Vector(vec![1.0, -2.5, 0.25]);
        let text = v.to_text().unwrap();
        assert_eq!(text, "[1,-2.5,0.25]");
        let back = ScalarValue::from_text(DataType::Vector(3), &text).unwrap();
        assert_eq!(back, v);
        assert!(ScalarValue::from_text(DataType::Vector(2), &text).is_err());
        assert!(ScalarValue::from_text(DataType::Vector(3), "[1,2]").is_err());
    }

    #[test]
    fn data_type_inference_for_list_and_vector() {
        let list = ScalarValue::List(vec![
            ScalarValue::Null,
            ScalarValue::Int64(7),
            ScalarValue::Null,
        ]);
        assert_eq!(
            list.data_type(),
            Some(DataType::Array(Box::new(DataType::Int64)))
        );
        let vec3 = ScalarValue::Vector(vec![0.0; 3]);
        assert_eq!(vec3.data_type(), Some(DataType::Vector(3)));
        assert_eq!(ScalarValue::Null.data_type(), None);
    }

    #[test]
    fn hash_and_eq_for_new_types() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ScalarValue::Vector(vec![1.0, 2.0]));
        set.insert(ScalarValue::List(vec![ScalarValue::Utf8("x".into())]));
        set.insert(ScalarValue::Binary(vec![1, 2]));
        assert!(set.contains(&ScalarValue::Vector(vec![1.0, 2.0])));
        assert!(set.contains(&ScalarValue::List(vec![ScalarValue::Utf8("x".into())])));
        assert!(set.contains(&ScalarValue::Binary(vec![1, 2])));
        assert!(!set.contains(&ScalarValue::Vector(vec![1.0, 3.0])));
    }
}
