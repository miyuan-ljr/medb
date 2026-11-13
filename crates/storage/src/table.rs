//! 表数据在 LSM 中的键布局。
//!
//! - 行键:`table_id u64BE ++ row_id u64BE`。u64 BE 保证按表前缀聚簇、
//!   同表内 row_id 升序 = 插入序;row_id 来自全局序列(last_seq,恢复时
//!   取 WAL/SST 最大值),单调且重启不复用,不会覆盖旧行。
//!   row_id 高位保留:有效范围 < 2^63(首字节 ≤ 0x7F)。
//! - 唯一索引键:`table_id u64BE ++ [0xFF] ++ index_id u64BE ++ 编码主键 ++ row_id`。
//!   0xFF 标记字节(≥ 行键首字节上界 0x7F)把索引空间与行空间隔离,
//!   行扫描/索引探测互不串扰。
//! - 表元数据(目录):保留表 0,键 = `[0u8;8] ++ 表名`,值为 serde_json
//!   编码的 TableSchema —— catalog 因此持久化并随 LSM 恢复。
//!
//! 范围扫描边界:行 [table_id ++ 00×8, table_id ++ FF×8);表 id ≥ 1,与
//! catalog 空间不相交。

use common::error::{Error, Result};

pub const CATALOG_TABLE_ID: u64 = 0;
pub const PK_INDEX_ID: u64 = 0;
/// row_id 上界(高位保留给索引键标记);达到即报"行号空间耗尽"。
pub const ROW_ID_MAX: u64 = (1u64 << 63) - 1;
const KEY_LEN: usize = 16;

/// 行键:table_id ++ row_id(各 8 字节 BE)。
pub fn row_key(table_id: u64, row_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(KEY_LEN);
    buf.extend_from_slice(&table_id.to_be_bytes());
    buf.extend_from_slice(&row_id.to_be_bytes());
    buf
}

/// 行键反解:(table_id, row_id);长度不符返回 None。
pub fn parse_row_key(key: &[u8]) -> Option<(u64, u64)> {
    if key.len() != KEY_LEN {
        return None;
    }
    Some((
        u64::from_be_bytes(key[..8].try_into().unwrap()),
        u64::from_be_bytes(key[8..].try_into().unwrap()),
    ))
}

/// 表的行扫描范围 [from, to)。row_id 高位保留(row_id < 2^63),
/// 高半区 [table_id ++ 80×8, ...) 是唯一索引键空间。
pub fn row_range(table_id: u64) -> (Vec<u8>, Vec<u8>) {
    let mut from = table_id.to_be_bytes().to_vec();
    from.extend_from_slice(&[0u8; 8]);
    let mut to = table_id.to_be_bytes().to_vec();
    to.push(0x80);
    to.extend_from_slice(&[0u8; 8]);
    (from, to)
}

/// 目录键:保留表 0 前缀 ++ 表名。
pub fn catalog_key(name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + name.len());
    buf.extend_from_slice(&CATALOG_TABLE_ID.to_be_bytes());
    buf.extend_from_slice(name.as_bytes());
    buf
}

/// 目录扫描范围 [from, to):覆盖表 0 全部键,不落到任何数据表。
pub fn catalog_range() -> (Vec<u8>, Vec<u8>) {
    (
        row_key(CATALOG_TABLE_ID, 0),
        row_key(CATALOG_TABLE_ID, u64::MAX),
    )
}

/// 目录键中剥出表名。
pub fn catalog_name(key: &[u8]) -> Result<&str> {
    if key.len() < 8 || &key[..8] != &CATALOG_TABLE_ID.to_be_bytes() {
        return Err(Error::corruption("bad catalog key prefix"));
    }
    std::str::from_utf8(&key[8..])
        .map_err(|e| Error::corruption(format!("catalog key not utf8: {e}")))
}

// ---------- 唯一索引(PK)键布局 ----------
//
// 索引键:`table_id ++ index_id ++ 可排序编码的主键值`,值 = row_id u64BE。
// 编码必须 (1) 保序 (2) 前缀隔离(任意值的编码都不是其它值编码 + 追加字节的
// 前缀),唯一性探测才能用极窄区间 [enc, inc(enc)) 精确圈定。
// 编码方案(escape + 终止符):NULL=[00];Bool=[01,b];Int64=[02, v^符号位 8B BE];
// Float64=[03, 全序变换 8B BE];Utf8/Binary=[04/05, 00→00FF 转义, 0000 终止]。
// NULL 恒排最前;终止符 0000 小于任何转义 continuation(00FF)与普通字节(≥01),
// 因此"较短值"恒小于其扩展值 ✓。

use common::types::ScalarValue;

/// 主键组件 → 可排序编码。仅接受标量与 Binary(数组/向量不可排序)。
pub fn encode_pk_component(v: &ScalarValue) -> Result<Vec<u8>> {
    const ESCAPE: u8 = 0x00;
    const ESCAPED: u8 = 0xFF;
    let mut out = Vec::new();
    match v {
        ScalarValue::Null => out.push(0x00),
        ScalarValue::Boolean(b) => {
            out.push(0x01);
            out.push(*b as u8);
        }
        ScalarValue::Int64(i) => {
            out.push(0x02);
            // 符号位翻转:补码 BE 字节序 == 数值序
            out.extend_from_slice(&(*i ^ (1i64 << 63)).to_be_bytes());
        }
        ScalarValue::Float64(f) => {
            out.push(0x03);
            // IEEE 754 全序变换(等价 f64::total_cmp 的位运算形式)
            let bits = f.to_bits();
            let transformed = if bits & (1 << 63) != 0 {
                !bits
            } else {
                bits | (1 << 63)
            };
            out.extend_from_slice(&transformed.to_be_bytes());
        }
        ScalarValue::Utf8(s) => {
            out.push(0x04);
            for byte in s.as_bytes() {
                if *byte == ESCAPE {
                    out.extend_from_slice(&[ESCAPE, ESCAPED]);
                } else {
                    out.push(*byte);
                }
            }
            out.extend_from_slice(&[0x00, 0x00]);
        }
        ScalarValue::Binary(b) => {
            out.push(0x05);
            for byte in b {
                if *byte == ESCAPE {
                    out.extend_from_slice(&[ESCAPE, ESCAPED]);
                } else {
                    out.push(*byte);
                }
            }
            out.extend_from_slice(&[0x00, 0x00]);
        }
        other => {
            return Err(Error::invalid_argument(format!(
                "type {:?} cannot be used as primary key",
                other.data_type()
            )));
        }
    }
    Ok(out)
}

/// 索引键前缀:table_id ++ [0xFF 标记] ++ index_id。
/// 0xFF > 任何行键的 row_id 高字节(row_id < 2^63 → 首字节 ≤ 0x7F),
/// 因此索引键恒在行扫描范围之外。
pub fn index_prefix(table_id: u64, index_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(17);
    buf.extend_from_slice(&table_id.to_be_bytes());
    buf.push(0xFF);
    buf.extend_from_slice(&index_id.to_be_bytes());
    buf
}

/// 索引条目键:prefix ++ enc ++ row_id(8B BE)。row_id 进键尾:
/// 同主键的全部条目恰好落在 [prefix++enc++00×8, prefix++enc++FF×8] 内,
/// 相邻主键值的键因终止符(0000 < 任何扩展字节)必然在区间外 → 唯一性探测精确。
/// 值:Some(空) = 存活条目,None = 墓碑。
pub fn index_entry_key(table_id: u64, index_id: u64, enc: &[u8], row_id: u64) -> Vec<u8> {
    let mut key = index_prefix(table_id, index_id);
    key.extend_from_slice(enc);
    key.extend_from_slice(&row_id.to_be_bytes());
    key
}

/// row_id 从条目键尾部反解。
pub fn row_id_from_entry_key(key: &[u8]) -> Result<u64> {
    if key.len() < 8 {
        return Err(Error::corruption("index entry key too short"));
    }
    Ok(u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap()))
}

/// 唯一性探测区间 [from, to):精确覆盖"编码恰好等于 enc"的全部条目
/// (含 row_id = 0xFF×8 的极端条目;to 为排他上界,多补一个 0x00)。
pub fn index_probe_range(table_id: u64, index_id: u64, enc: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut prefix = index_prefix(table_id, index_id);
    prefix.extend_from_slice(enc);
    let mut from = prefix.clone();
    from.extend_from_slice(&[0u8; 8]);
    let mut to = prefix;
    to.extend_from_slice(&[0xFFu8; 8]);
    to.push(0x00);
    (from, to)
}

/// 对完整键尾部 +1(进位);全 0xFF 返回 None(理论不可达:tag 字节 < 0xFF)。
pub fn probe_range_for(key: &[u8]) -> (Vec<u8>, Vec<u8>) {
    (
        key.to_vec(),
        inc_last_byte(key).unwrap_or_else(|| {
            let mut v = key.to_vec();
            v.push(0x00);
            v
        }),
    )
}

/// 字节序列尾部 +1(进位);全 0xFF 返回 None。
fn inc_last_byte(key: &[u8]) -> Option<Vec<u8>> {
    let mut out = key.to_vec();
    for byte in out.iter_mut().rev() {
        if *byte == 0xFF {
            *byte = 0x00;
        } else {
            *byte += 1;
            return Some(out);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::types::ScalarValue;

    #[test]
    fn pk_encoding_is_order_preserving_and_prefix_free() {
        use ScalarValue::{Binary as SB, Int64 as SI, Utf8 as SU};
        let enc = |v: &ScalarValue| encode_pk_component(v).unwrap();

        // 保序:数值/字符串/二进制
        assert!(enc(&SI(1)) < enc(&SI(2)));
        assert!(enc(&SI(-1)) < enc(&SI(0)));
        assert!(enc(&SI(i64::MIN)) < enc(&SI(i64::MAX)));
        assert!(enc(&SU("a".into())) < enc(&SU("ab".into())));
        assert!(enc(&SU("ab".into())) < enc(&SU("ab\x00".into())));
        assert!(enc(&SU("".into())) < enc(&SU("a".into())));
        assert!(enc(&SB(vec![1])).to_vec() < enc(&SB(vec![1, 2])).to_vec());

        // 前缀隔离:enc("ab") 的探测区间必须恰好覆盖 "ab" 条目,
        // 不含 "abc"/"ab\x00"/"ab\x01"/"abzz" 的条目
        let (lo, hi) = index_probe_range(1, PK_INDEX_ID, &enc(&SU("ab".into())));
        for probe in [
            SU("ab".into()),     // 应命中
            SU("abc".into()),    // 不应命中
            SU("ab\x00".into()), // 不应命中
            SU("ab\x01".into()), // 不应命中
            SU("abzz".into()),   // 不应命中
        ] {
            let k = index_entry_key(1, PK_INDEX_ID, &enc(&probe), 42);
            let in_range = k >= lo && k < hi;
            assert_eq!(
                in_range,
                probe == SU("ab".into()),
                "probe {probe:?} in_range={in_range}"
            );
        }
    }

    #[test]
    fn null_sorts_first_and_probes_exactly() {
        let null_enc = encode_pk_component(&ScalarValue::Null).unwrap();
        let (lo, hi) = index_probe_range(1, PK_INDEX_ID, &null_enc);
        let k_null = index_entry_key(1, PK_INDEX_ID, &null_enc, 42);
        assert!(k_null >= lo && k_null < hi);
        let int_enc = encode_pk_component(&ScalarValue::Int64(5)).unwrap();
        let k_int = index_entry_key(1, PK_INDEX_ID, &int_enc, 42);
        assert!(k_int >= hi, "NULL 编码必须排在任何非 NULL 之外");
    }

    #[test]
    fn probe_range_inc_handles_carry() {
        // Int64 编码尾部 0xFF:进位跨字节;row_id = 0xFF×8 的极端条目仍在区间内
        let enc = encode_pk_component(&ScalarValue::Int64(-1)).unwrap(); // 尾字节 0xFF
        let (from, to) = index_probe_range(1, PK_INDEX_ID, &enc);
        let extreme = index_entry_key(1, PK_INDEX_ID, &enc, u64::MAX);
        assert!(extreme >= from && extreme < to);
        // 相邻编码(-2 与 0)的条目在区间外
        let below = index_entry_key(
            1,
            PK_INDEX_ID,
            &encode_pk_component(&ScalarValue::Int64(-2)).unwrap(),
            1,
        );
        let above = index_entry_key(
            1,
            PK_INDEX_ID,
            &encode_pk_component(&ScalarValue::Int64(0)).unwrap(),
            1,
        );
        assert!(below < from);
        assert!(above >= to);
    }

    #[test]
    fn row_range_scoped_and_ordered() {
        let (from, to) = row_range(7);
        let k1 = row_key(7, 1);
        let k2 = row_key(7, 2);
        let other = row_key(8, 1);
        assert!(k1 >= from && k1 < to);
        assert!(k2 < to);
        assert!(other >= to, "表 8 的键必须在表 7 范围之外");
        assert!(parse_row_key(&k2) == Some((7, 2)));
        assert_eq!(parse_row_key(&[0u8; 5]), None);
    }

    #[test]
    fn row_id_order_equals_insertion_order() {
        // u64 BE 编码下数值序 == 字节序 → 扫描即插入序
        let a = row_key(1, 3);
        let b = row_key(1, 10);
        assert!(a < b);
    }

    #[test]
    fn catalog_space_disjoint_from_tables() {
        let (cfrom, cto) = catalog_range();
        let (from, to) = row_range(1);
        assert!(cto <= from, "catalog 范围必须在表 1 之前");
        assert!(cfrom <= cto);
        assert!(
            row_key(1, ROW_ID_MAX) < to,
            "最大合法 row_id(高位保留区边界内)不越界"
        );
        // 索引键空间(table_id ++ 0xFF ++ ...)在行范围之外
        let idx_key = index_entry_key(
            1,
            PK_INDEX_ID,
            &encode_pk_component(&ScalarValue::Int64(1)).unwrap(),
            1,
        );
        assert!(idx_key >= to, "索引键必须在行扫描范围之外");

        let key = catalog_key("users");
        let name = catalog_name(&key).unwrap();
        assert_eq!(name, "users");
        assert!(catalog_name(&[0u8; 4]).is_err());
        assert!(catalog_name(&[1u8, 0, 0, 0, 0, 0, 0, 0, b'x']).is_err());
    }
}
