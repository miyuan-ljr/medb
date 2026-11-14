//! 行/值二进制编码。rowstore 的 value 与列存副本的行转换共用同一套编码。
//!
//! 布局:每个值一个 tag 字节,tag 后跟定长或 varint 前缀的载荷。
//! Int64 目前用固定 8 字节;varint 优化见路线图。

use crate::error::{Error, Result};
use crate::schema::Row;
use crate::types::ScalarValue;
use crate::util::{get_varint_u64, put_varint_u64};

const TAG_NULL: u8 = 0;
const TAG_FALSE: u8 = 1;
const TAG_TRUE: u8 = 2;
const TAG_INT64: u8 = 3;
const TAG_FLOAT64: u8 = 4;
const TAG_UTF8: u8 = 5;
const TAG_BINARY: u8 = 6;
const TAG_LIST: u8 = 7;
const TAG_VECTOR: u8 = 8;
const TAG_DATE: u8 = 9;

pub fn encode_value(v: &ScalarValue, buf: &mut Vec<u8>) {
    match v {
        ScalarValue::Null => buf.push(TAG_NULL),
        ScalarValue::Boolean(false) => buf.push(TAG_FALSE),
        ScalarValue::Boolean(true) => buf.push(TAG_TRUE),
        ScalarValue::Int64(i) => {
            buf.push(TAG_INT64);
            buf.extend_from_slice(&i.to_be_bytes());
        }
        ScalarValue::Float64(f) => {
            buf.push(TAG_FLOAT64);
            buf.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        ScalarValue::Utf8(s) => {
            buf.push(TAG_UTF8);
            put_varint_u64(s.len() as u64, buf);
            buf.extend_from_slice(s.as_bytes());
        }
        ScalarValue::Date(d) => {
            buf.push(TAG_DATE);
            buf.extend_from_slice(&d.to_be_bytes());
        }
        ScalarValue::Binary(b) => {
            buf.push(TAG_BINARY);
            put_varint_u64(b.len() as u64, buf);
            buf.extend_from_slice(b);
        }
        ScalarValue::List(items) => {
            buf.push(TAG_LIST);
            put_varint_u64(items.len() as u64, buf);
            for item in items {
                encode_value(item, buf);
            }
        }
        ScalarValue::Vector(vs) => {
            buf.push(TAG_VECTOR);
            put_varint_u64(vs.len() as u64, buf);
            for f in vs {
                buf.extend_from_slice(&f.to_bits().to_le_bytes());
            }
        }
    }
}

pub fn decode_value(buf: &mut &[u8]) -> Result<ScalarValue> {
    let tag = take_byte(buf)?;
    match tag {
        TAG_NULL => Ok(ScalarValue::Null),
        TAG_FALSE => Ok(ScalarValue::Boolean(false)),
        TAG_TRUE => Ok(ScalarValue::Boolean(true)),
        TAG_INT64 => {
            if buf.len() < 8 {
                return Err(short("i64"));
            }
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[..8]);
            *buf = &buf[8..];
            Ok(ScalarValue::Int64(i64::from_be_bytes(b)))
        }
        TAG_FLOAT64 => {
            if buf.len() < 8 {
                return Err(short("f64"));
            }
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[..8]);
            *buf = &buf[8..];
            Ok(ScalarValue::Float64(f64::from_bits(u64::from_be_bytes(b))))
        }
        TAG_UTF8 => {
            let len = get_varint_u64(buf)? as usize;
            if buf.len() < len {
                return Err(short("utf8 payload"));
            }
            let bytes = &buf[..len];
            *buf = &buf[len..];
            String::from_utf8(bytes.to_vec())
                .map(ScalarValue::Utf8)
                .map_err(|e| Error::corruption(format!("invalid utf8 in row: {e}")))
        }
        TAG_BINARY => {
            let len = get_varint_u64(buf)? as usize;
            if buf.len() < len {
                return Err(short("binary payload"));
            }
            let bytes = buf[..len].to_vec();
            *buf = &buf[len..];
            Ok(ScalarValue::Binary(bytes))
        }
        TAG_DATE => {
            if buf.len() < 4 {
                return Err(short("date"));
            }
            let mut b = [0u8; 4];
            b.copy_from_slice(&buf[..4]);
            *buf = &buf[4..];
            Ok(ScalarValue::Date(i32::from_be_bytes(b)))
        }
        TAG_LIST => {
            let count = get_varint_u64(buf)? as usize;
            let mut items = Vec::with_capacity(count.min(1024));
            for _ in 0..count {
                items.push(decode_value(buf)?);
            }
            Ok(ScalarValue::List(items))
        }
        TAG_VECTOR => {
            let count = get_varint_u64(buf)? as usize;
            if buf.len() < count * 4 {
                return Err(short("vector payload"));
            }
            let mut vs = Vec::with_capacity(count);
            for chunk in buf[..count * 4].chunks_exact(4) {
                vs.push(f32::from_bits(u32::from_le_bytes(
                    chunk.try_into().unwrap(),
                )));
            }
            *buf = &buf[count * 4..];
            Ok(ScalarValue::Vector(vs))
        }
        other => Err(Error::corruption(format!("unknown value tag {other:#x}"))),
    }
}

/// 追加编码一行到 buf(无行内长度前缀,由调用方决定外层封装)。
pub fn encode_row(row: &[ScalarValue], buf: &mut Vec<u8>) {
    for v in row {
        encode_value(v, buf);
    }
}

pub fn encode_row_to_vec(row: &[ScalarValue]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(row.len() * 9);
    encode_row(row, &mut buf);
    buf
}

/// 解码 buf 中剩余全部值为一行;残余字节视为错误。
pub fn decode_row(buf: &mut &[u8]) -> Result<Row> {
    let mut row = Row::new();
    while !buf.is_empty() {
        row.push(decode_value(buf)?);
    }
    Ok(row)
}

/// 解码一行,但只物化 `needed` 标记的列;未标记列**跳过字节**不物化,
/// 槽位填 Null(投影下推:省掉未引用列的分配,尤其 Utf8/Binary)。
/// 跳过 = 读 tag + 越过载荷(编码自描述,游标可安全推进)。
pub fn decode_row_partial(buf: &mut &[u8], needed: &[bool]) -> Result<Row> {
    let mut row = Row::with_capacity(needed.len());
    for need in needed {
        if *need {
            row.push(decode_value(buf)?);
        } else {
            skip_value(buf)?;
            row.push(ScalarValue::Null);
        }
    }
    Ok(row)
}

/// 越过一个编码值(不物化);仅推进游标。
pub fn skip_value(buf: &mut &[u8]) -> Result<()> {
    let tag = take_byte(buf)?;
    match tag {
        TAG_NULL | TAG_FALSE | TAG_TRUE => Ok(()),
        TAG_INT64 | TAG_FLOAT64 => {
            if buf.len() < 8 {
                return Err(short("fixed 8B value"));
            }
            *buf = &buf[8..];
            Ok(())
        }
        TAG_DATE => {
            if buf.len() < 4 {
                return Err(short("date value"));
            }
            *buf = &buf[4..];
            Ok(())
        }
        TAG_UTF8 | TAG_BINARY => {
            let len = get_varint_u64(buf)? as usize;
            if buf.len() < len {
                return Err(short("payload"));
            }
            *buf = &buf[len..];
            Ok(())
        }
        TAG_LIST => {
            let n = get_varint_u64(buf)? as usize;
            for _ in 0..n {
                skip_value(buf)?;
            }
            Ok(())
        }
        TAG_VECTOR => {
            let n = get_varint_u64(buf)? as usize;
            let bytes = n * 4; // f32 × 维度
            if buf.len() < bytes {
                return Err(short("vector payload"));
            }
            *buf = &buf[bytes..];
            Ok(())
        }
        other => Err(Error::invalid_argument(format!(
            "unknown value tag {other:#x}"
        ))),
    }
}

fn take_byte(buf: &mut &[u8]) -> Result<u8> {
    if buf.is_empty() {
        return Err(short("value tag"));
    }
    let b = buf[0];
    *buf = &buf[1..];
    Ok(b)
}

fn short(what: &str) -> Error {
    Error::corruption(format!("truncated row buffer while reading {what}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_row() {
        let row: Row = vec![
            ScalarValue::Null,
            ScalarValue::Boolean(true),
            ScalarValue::Boolean(false),
            ScalarValue::Int64(-42),
            ScalarValue::Int64(i64::MAX),
            ScalarValue::Float64(3.5),
            ScalarValue::Utf8("hello 世界".to_string()),
            ScalarValue::Binary(vec![0xec, 0x48, 0x00]),
            ScalarValue::List(vec![
                ScalarValue::Int64(1),
                ScalarValue::Null,
                ScalarValue::Utf8("x".to_string()),
                ScalarValue::List(vec![ScalarValue::Int64(9)]),
            ]),
            ScalarValue::Vector(vec![1.5f32, -2.0f32, 0.25f32]),
        ];
        let bytes = encode_row_to_vec(&row);
        let mut slice = bytes.as_slice();
        let back = decode_row(&mut slice).unwrap();
        assert_eq!(back, row);
        assert!(slice.is_empty());
    }

    #[test]
    fn truncated_new_types_are_errors() {
        let full = encode_row_to_vec(&[ScalarValue::Vector(vec![1.0, 2.0])]);
        assert!(decode_row(&mut &full[..full.len() - 4]).is_err());
        let list = encode_row_to_vec(&[ScalarValue::List(vec![ScalarValue::Int64(7)])]);
        assert!(decode_row(&mut &list[..list.len() - 1]).is_err());
        assert!(decode_row(&mut &[TAG_VECTOR, 200][..]).is_err());
    }

    #[test]
    fn truncated_buffer_is_error() {
        let row: Row = vec![ScalarValue::Utf8("abc".to_string())];
        let bytes = encode_row_to_vec(&row);
        assert!(decode_row(&mut &bytes[..bytes.len() - 1]).is_err());
        assert!(decode_row(&mut &[TAG_INT64][..]).is_err());
    }
}

#[cfg(test)]
mod partial_tests {
    use super::*;

    #[test]
    fn skip_value_advances_correctly() {
        let mut buf = Vec::new();
        encode_value(&ScalarValue::Int64(7), &mut buf);
        encode_value(&ScalarValue::Utf8("hello".into()), &mut buf);
        encode_value(&ScalarValue::Float64(1.5), &mut buf);
        encode_value(&ScalarValue::Date(100), &mut buf);
        let mut s = &buf[..];
        // 跳过 Int64 和 Utf8,保留 Float64/Date
        skip_value(&mut s).unwrap();
        skip_value(&mut s).unwrap();
        let v1 = decode_value(&mut s).unwrap();
        let v2 = decode_value(&mut s).unwrap();
        assert_eq!(v1, ScalarValue::Float64(1.5));
        assert_eq!(v2, ScalarValue::Date(100));
        assert!(s.is_empty());
    }

    #[test]
    fn partial_decode_keeps_needed_cols_only() {
        let row: Row = vec![
            ScalarValue::Int64(1),
            ScalarValue::Utf8("hello world".into()),
            ScalarValue::Float64(2.5),
            ScalarValue::Null,
        ];
        let mut buf = Vec::new();
        encode_row(&row, &mut buf);
        let needed = [true, false, true, false];
        let mut s = &buf[..];
        let partial = decode_row_partial(&mut s, &needed).unwrap();
        assert_eq!(partial.len(), 4);
        assert_eq!(partial[0], ScalarValue::Int64(1));
        assert_eq!(partial[1], ScalarValue::Null, "剪枝列 → Null 占位");
        assert_eq!(partial[2], ScalarValue::Float64(2.5));
        assert_eq!(partial[3], ScalarValue::Null);
    }
}

#[cfg(test)]
mod partial_tests2 {
    use super::*;

    /// Date 是 4 字节列,验证剪枝含 Date 时游标推进正确。
    #[test]
    fn partial_decode_with_date_column() {
        let row: Row = vec![
            ScalarValue::Int64(1),
            ScalarValue::Utf8("hello world".into()),
            ScalarValue::Float64(2.5),
            ScalarValue::Null,
            ScalarValue::Date(100),
            ScalarValue::Int64(9),
        ];
        // 剪枝含 4 字节的 Date 列(游标推进正确性);末列保留
        let needed = [true, false, true, false, true, true];
        let mut buf = Vec::new();
        encode_row(&row, &mut buf);
        let mut s = &buf[..];
        let partial = decode_row_partial(&mut s, &needed).unwrap();
        assert_eq!(partial.len(), 6);
        assert_eq!(partial[0], ScalarValue::Int64(1));
        assert_eq!(partial[1], ScalarValue::Null, "剪枝列 → Null 占位");
        assert_eq!(partial[2], ScalarValue::Float64(2.5));
        assert_eq!(partial[3], ScalarValue::Null);
        assert_eq!(partial[4], ScalarValue::Date(100));
        assert_eq!(
            partial[5],
            ScalarValue::Int64(9),
            "末列(剪 Date 后)游标应精确推进"
        );
    }
}
