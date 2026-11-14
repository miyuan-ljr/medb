//! 列存副本快照文件:每表一份 `{table_id}.col`,列格式 + CRC 校验 + 原子替换。
//!
//! 文件布局(小端):
//! ```text
//! magic "MEDBCOL1" (8B)
//! version u32 = 1
//! table_id u64
//! watermark u64          // 已镜像的 LSM 最大 seq;恢复时重放 seq > watermark
//! num_cols u32
//! num_rows u64
//! keys: num_rows × (u32 len + bytes)          // 行键(row_key)
//! columns: num_cols × num_rows × codec::encode_value(cell)  // 列主序
//! crc32 u32               // 以上全部字节
//! ```
//! 写入 = tmp 文件 + rename 原子替换;损坏(CRC/magic/版本)读出报错,
//! 调用方回退全量重建(副本是派生数据,安全)。

use std::io::Write;
use std::path::{Path, PathBuf};

use common::codec;
use common::error::{Error, Result};
use common::schema::{Row, TableSchema};
use common::types::{DataType, ScalarValue};
use common::util::crc32;

use crate::columnar::{ColumnValues, ColumnVector};

const MAGIC: &[u8; 8] = b"MEDBCOL1";
const VERSION: u32 = 1;

pub fn snapshot_path(dir: &Path, table_id: u64) -> PathBuf {
    dir.join(format!("{table_id}.col"))
}

/// 副本快照:行键 + 行数据 + 水位线。
pub struct Snapshot {
    pub watermark: u64,
    pub entries: Vec<(Vec<u8>, Row)>,
}

fn type_tag(ty: &DataType) -> u32 {
    match ty {
        DataType::Boolean => 0,
        DataType::Int64 => 1,
        DataType::Float64 => 2,
        DataType::Utf8 => 3,
        DataType::Date => 4,
        DataType::Binary => 5,
        DataType::Array(_) => 6,
        DataType::Vector(_) => 7,
    }
}

fn value_from_tag_and_scalar(tag: u32, sv: &ScalarValue) -> Result<ScalarValue> {
    let expect = |want: u32| {
        if tag == want {
            Ok(())
        } else {
            Err(Error::corruption(format!(
                "columnar snapshot type tag mismatch: {tag} vs {want}"
            )))
        }
    };
    match sv {
        ScalarValue::Null => Ok(ScalarValue::Null), // 类型由列头决定,先保留 Null
        ScalarValue::Boolean(_) => {
            expect(0)?;
            Ok(sv.clone())
        }
        ScalarValue::Int64(_) => {
            expect(1)?;
            Ok(sv.clone())
        }
        ScalarValue::Float64(_) => {
            expect(2)?;
            Ok(sv.clone())
        }
        ScalarValue::Utf8(_) => {
            expect(3)?;
            Ok(sv.clone())
        }
        ScalarValue::Date(_) => {
            expect(4)?;
            Ok(sv.clone())
        }
        ScalarValue::Binary(_) => {
            expect(5)?;
            Ok(sv.clone())
        }
        ScalarValue::List(_) => {
            expect(6)?;
            Ok(sv.clone())
        }
        ScalarValue::Vector(_) => {
            expect(7)?;
            Ok(sv.clone())
        }
    }
}

/// 写快照:entries 为副本当前全量(键 + 行);tmp + rename 原子替换。
pub fn write_snapshot(
    path: &Path,
    table_id: u64,
    watermark: u64,
    entries: &[(Vec<u8>, Row)],
    schema: &TableSchema,
) -> Result<()> {
    let mut buf = Vec::with_capacity(64 * 1024);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&table_id.to_le_bytes());
    buf.extend_from_slice(&watermark.to_le_bytes());
    buf.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for c in &schema.columns {
        buf.extend_from_slice(&type_tag(&c.ty).to_le_bytes());
    }
    // 行键
    for (key, _) in entries {
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
    }
    // 列主序值(codec 标签编码,含 null)
    for (col, c) in schema.columns.iter().enumerate() {
        let _ = c;
        for (_, row) in entries {
            codec::encode_value(&row[col], &mut buf);
        }
    }
    let crc = crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let tmp = path.with_extension("col.tmp");
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all().ok();
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// 读快照;文件不存在 → Ok(None);magic/版本/CRC 不符 → Err(调用方回退重建)。
pub fn read_snapshot(path: &Path, table_id: u64) -> Result<Option<Snapshot>> {
    let Ok(bytes) = std::fs::read(path) else {
        return Ok(None);
    };
    if bytes.len() < 8 + 4 + 8 + 8 + 4 + 8 + 4 {
        return Err(Error::corruption("columnar snapshot too short"));
    }
    let body_len = bytes.len() - 4;
    let expect_crc = u32::from_le_bytes(bytes[body_len..].try_into().unwrap());
    if crc32(&bytes[..body_len]) != expect_crc {
        return Err(Error::corruption("columnar snapshot crc mismatch"));
    }
    let mut s = &bytes[..body_len];
    if &s[..8] != MAGIC {
        return Err(Error::corruption("columnar snapshot bad magic"));
    }
    s = &s[8..];
    let version = u32::from_le_bytes(s[..4].try_into().unwrap());
    if version != VERSION {
        return Err(Error::corruption(format!(
            "columnar snapshot version {version}"
        )));
    }
    s = &s[4..];
    let file_table_id = u64::from_le_bytes(s[..8].try_into().unwrap());
    if file_table_id != table_id {
        return Err(Error::corruption("columnar snapshot table id mismatch"));
    }
    s = &s[8..];
    let watermark = u64::from_le_bytes(s[..8].try_into().unwrap());
    s = &s[8..];
    let num_cols = u32::from_le_bytes(s[..4].try_into().unwrap()) as usize;
    s = &s[4..];
    let num_rows = u64::from_le_bytes(s[..8].try_into().unwrap()) as usize;
    s = &s[8..];
    let mut col_tags = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        if s.len() < 4 {
            return Err(Error::corruption("columnar snapshot truncated (types)"));
        }
        col_tags.push(u32::from_le_bytes(s[..4].try_into().unwrap()));
        s = &s[4..];
    }
    // 行键
    let mut keys = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        if s.len() < 4 {
            return Err(Error::corruption("columnar snapshot truncated (key)"));
        }
        let len = u32::from_le_bytes(s[..4].try_into().unwrap()) as usize;
        s = &s[4..];
        if s.len() < len {
            return Err(Error::corruption("columnar snapshot truncated (key body)"));
        }
        keys.push(s[..len].to_vec());
        s = &s[len..];
    }
    // 列主序值 → 行转置
    let mut columns: Vec<Vec<ScalarValue>> = Vec::with_capacity(num_cols);
    for &tag in &col_tags {
        let mut col = Vec::with_capacity(num_rows);
        for _ in 0..num_rows {
            let v = codec::decode_value(&mut s)?;
            col.push(value_from_tag_and_scalar(tag, &v)?);
        }
        columns.push(col);
    }
    if !s.is_empty() {
        // s 已裁剪到 body_len,尾部多余字节 = 解码不完整
        return Err(Error::corruption("columnar snapshot trailing bytes"));
    }
    // 转置为行
    let mut entries = Vec::with_capacity(num_rows);
    for r in 0..num_rows {
        let mut row = Vec::with_capacity(num_cols);
        for col in &columns {
            row.push(col[r].clone());
        }
        entries.push((keys[r].clone(), row));
    }
    Ok(Some(Snapshot { watermark, entries }))
}

/// 从 exec 无关的 ColumnVector 组装(供测试/调试;主路径走 codec 行编码)。
pub fn column_vector_roundtrip_check(cv: &ColumnVector) -> usize {
    match &cv.values {
        ColumnValues::Int64(v) => v.len(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::{ColumnarReplica, InMemoryColumnarReplica};
    use common::schema::ColumnDesc;
    use common::types::DataType as DT;

    fn schema() -> TableSchema {
        TableSchema {
            id: 7,
            name: "t".into(),
            columns: vec![
                ColumnDesc::new("id", DT::Int64, false),
                ColumnDesc::new("name", DT::Utf8, true),
                ColumnDesc::new("d", DT::Date, true),
            ],
            primary_key: Vec::new(),
            version: 1,
        }
    }

    #[test]
    fn snapshot_roundtrip_with_nulls_and_dates() {
        let dir = tempfile::tempdir().unwrap();
        let path = snapshot_path(dir.path(), 7);
        let entries: Vec<(Vec<u8>, Row)> = vec![
            (
                b"k1".to_vec(),
                vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Utf8("a".into()),
                    ScalarValue::Date(100),
                ],
            ),
            (
                b"k2".to_vec(),
                vec![
                    ScalarValue::Int64(2),
                    ScalarValue::Null,
                    ScalarValue::Date(200),
                ],
            ),
        ];
        write_snapshot(&path, 7, 42, &entries, &schema()).unwrap();
        let snap = read_snapshot(&path, 7).unwrap().expect("snapshot exists");
        assert_eq!(snap.watermark, 42);
        assert_eq!(snap.entries, entries);
        // 不存在的表
        assert!(
            read_snapshot(&snapshot_path(dir.path(), 8), 8)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn corrupted_snapshot_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = snapshot_path(dir.path(), 7);
        let entries: Vec<(Vec<u8>, Row)> = vec![(
            b"k1".to_vec(),
            vec![ScalarValue::Int64(1), ScalarValue::Null, ScalarValue::Null],
        )];
        write_snapshot(&path, 7, 1, &entries, &schema()).unwrap();
        // 破坏 body
        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(read_snapshot(&path, 7).is_err(), "CRC 应拦截损坏");
    }

    #[test]
    fn replica_collect_and_bulk_load() {
        let replica = InMemoryColumnarReplica::new();
        let s = schema();
        replica
            .upsert(
                &s,
                b"k1",
                vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Utf8("a".into()),
                    ScalarValue::Date(1),
                ],
            )
            .unwrap();
        let entries = replica.collect_table(7).expect("entry exists");
        assert_eq!(entries.len(), 1);
        assert!(replica.collect_table(99).is_none(), "未镜像的表 → None");
        // bulk_load 到另一个 id
        replica.bulk_load(2, entries.clone());
        assert_eq!(replica.row_count(common::ids::TableId(2)), 1);
    }
}
