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
