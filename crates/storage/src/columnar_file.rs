//! 列存 segment 文件:每表一份(按代次编号),行格式存储 + CRC + 原子替换。
//!
//! 架构约定(★ 存储层方向):列存副本**只覆盖 SST 数据**——
//! memtable 的增量由 AP 读路径以 memtable overlay 合并(见 engine),
//! 因此 segment 天然落盘、随 SST 生命周期存在:
//!   * flush:memtable 增量(seq > 水位)写成新 segment → SST 同时诞生
//!   * compaction:L1 重写后按 post-GC 数据重建 segment(墓碑/旧版本自动清除)
//!   * DROP TABLE / ALTER:删除/失效该表 segment(AP 回退行存,flush 后恢复)
//!
//! 文件布局(小端):
//! ```text
//! magic "MEDBSEG2" (8B)
//! version u32 = 2
//! table_id u64
//! num_cols u32
//! col type tags: u32 × num_cols        // 列类型(DataType tag 映射)
//! num_rows u64
//! rows: num_rows × {
//!     key:  u32 len + bytes            // 行键(row_key)
//!     seq:  u64                        // 行版本(AP 合并用)
//!     row:  num_cols × codec::encode_value(cell)   // 列主序,含 null tag
//! }
//! crc32 u32
//! ```
//! 写入 = tmp + rename 原子替换;损坏读出报错,调用方回退行存(派生数据)。

use std::io::Write;
use std::path::{Path, PathBuf};

use common::codec;
use common::error::{Error, Result};
use common::schema::{Row, TableSchema};
use common::util::crc32;
use common::types::{DataType, ScalarValue};

const MAGIC: &[u8; 8] = b"MEDBSEG2";
const VERSION: u32 = 2;

/// 一行:行键 + 版本 seq + 解码前的列值集合(codec 行)。
#[derive(Debug, Clone)]
pub struct SegmentRow {
    pub key: Vec<u8>,
    pub seq: u64,
    pub row: Row,
}

/// 一个 segment 的解码视图。
#[derive(Debug, Clone)]
pub struct Segment {
    pub table_id: u64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub rows: Vec<SegmentRow>,
}

/// segment 文件名携带 max_seq:打开期无需读文件即可判断
/// "SST 里是否有比 segment 更新的数据"(crash 落在 flush 与重建之间的检测)。
pub fn segment_path(dir: &Path, table_id: u64, generation: u64, max_seq: u64) -> PathBuf {
    dir.join(format!("{table_id}.{generation:05}.{max_seq}.seg"))
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

/// 写 segment;rows 按行键升序(调用方排序)。tmp + rename 原子替换。
pub fn write_segment(
    path: &Path,
    table_id: u64,
    rows: &[SegmentRow],
    schema: &TableSchema,
) -> Result<()> {
    let mut buf = Vec::with_capacity(32 * 1024);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&table_id.to_le_bytes());
    buf.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());
    for c in &schema.columns {
        buf.extend_from_slice(&type_tag(&c.ty).to_le_bytes());
    }
    buf.extend_from_slice(&(rows.len() as u64).to_le_bytes());
    let mut min_seq = u64::MAX;
    let mut max_seq = 0u64;
    for r in rows {
        min_seq = min_seq.min(r.seq);
        max_seq = max_seq.max(r.seq);
    }
    if rows.is_empty() {
        min_seq = 0;
    }
    buf.extend_from_slice(&min_seq.to_le_bytes());
    buf.extend_from_slice(&max_seq.to_le_bytes());
    for r in rows {
        buf.extend_from_slice(&(r.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&r.key);
        buf.extend_from_slice(&r.seq.to_le_bytes());
        for cell in &r.row {
            codec::encode_value(cell, &mut buf);
        }
    }
    let crc = crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let tmp = path.with_extension("seg.tmp");
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

/// 读 segment;文件不存在 → Ok(None);损坏 → Err(调用方回退行存)。
pub fn read_segment(path: &Path, table_id: u64) -> Result<Option<Segment>> {
    let Ok(bytes) = std::fs::read(path) else {
        return Ok(None);
    };
    let Some(body_len) = bytes.len().checked_sub(4) else {
        return Err(Error::corruption("columnar segment too short"));
    };
    let expect_crc = u32::from_le_bytes(bytes[body_len..].try_into().unwrap());
    if crc32(&bytes[..body_len]) != expect_crc {
        return Err(Error::corruption("columnar segment crc mismatch"));
    }
    let mut s: &[u8] = &bytes[..body_len];
    if s.len() < 8 || &s[..8] != MAGIC {
        return Err(Error::corruption("columnar segment bad magic"));
    }
    s = &s[8..];
    let need = |n: usize, what: &str| -> Result<()> {
        if s.len() < n {
            return Err(Error::corruption(format!("segment truncated: {what}")));
        }
        Ok(())
    };
    need(4, "version")?;
    let version = u32::from_le_bytes(s[..4].try_into().unwrap());
    if version != VERSION {
        return Err(Error::corruption(format!("segment version {version}")));
    }
    s = &s[4..];
    need(8, "table id")?;
    let file_table_id = u64::from_le_bytes(s[..8].try_into().unwrap());
    if file_table_id != table_id {
        return Err(Error::corruption("segment table id mismatch"));
    }
    s = &s[8..];
    need(4, "col count")?;
    let num_cols = u32::from_le_bytes(s[..4].try_into().unwrap()) as usize;
    s = &s[4..];
    let mut col_tags = Vec::with_capacity(num_cols);
    for _ in 0..num_cols {
        need(4, "col tag")?;
        col_tags.push(u32::from_le_bytes(s[..4].try_into().unwrap()));
        s = &s[4..];
    }
    need(8, "row count")?;
    let num_rows = u64::from_le_bytes(s[..8].try_into().unwrap()) as usize;
    s = &s[8..];
    need(16, "seq range")?;
    let min_seq = u64::from_le_bytes(s[..8].try_into().unwrap());
    let max_seq = u64::from_le_bytes(s[8..16].try_into().unwrap());
    s = &s[16..];

    let mut rows = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        need(4, "key len")?;
        let klen = u32::from_le_bytes(s[..4].try_into().unwrap()) as usize;
        s = &s[4..];
        need(klen, "key")?;
        let key = s[..klen].to_vec();
        s = &s[klen..];
        need(8, "seq")?;
        let seq = u64::from_le_bytes(s[..8].try_into().unwrap());
        s = &s[8..];
        let mut row = Vec::with_capacity(num_cols);
        for &tag in &col_tags {
            let v = codec::decode_value(&mut s)?;
            check_cell_tag(tag, &v)?;
            row.push(v);
        }
        rows.push(SegmentRow { key, seq, row });
    }
    if !s.is_empty() {
        return Err(Error::corruption("columnar segment trailing bytes"));
    }
    Ok(Some(Segment {
        table_id,
        min_seq,
        max_seq,
        rows,
    }))
}

fn check_cell_tag(tag: u32, v: &ScalarValue) -> Result<()> {
    use common::types::ScalarValue as SV;
    let ok = match (tag, v) {
        (0, SV::Boolean(_) | SV::Null) => true,
        (1, SV::Int64(_) | SV::Null) => true,
        (2, SV::Float64(_) | SV::Null) => true,
        (3, SV::Utf8(_) | SV::Null) => true,
        (4, SV::Date(_) | SV::Null) => true,
        (5, SV::Binary(_) | SV::Null) => true,
        (6, SV::List(_) | SV::Null) => true,
        (7, SV::Vector(_) | SV::Null) => true,
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(Error::corruption(format!(
            "segment cell type mismatch: tag {tag} vs {v:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::schema::ColumnDesc;

    fn schema() -> TableSchema {
        TableSchema {
            id: 7,
            name: "t".into(),
            columns: vec![
                ColumnDesc::new("id", DataType::Int64, false),
                ColumnDesc::new("name", DataType::Utf8, true),
                ColumnDesc::new("d", DataType::Date, true),
            ],
            primary_key: Vec::new(),
            indexes: Vec::new(),
            version: 1,
        }
    }

    fn sample() -> Vec<SegmentRow> {
        vec![
            SegmentRow {
                key: b"k1".to_vec(),
                seq: 10,
                row: vec![
                    ScalarValue::Int64(1),
                    ScalarValue::Utf8("a".into()),
                    ScalarValue::Date(100),
                ],
            },
            SegmentRow {
                key: b"k2".to_vec(),
                seq: 11,
                row: vec![
                    ScalarValue::Int64(2),
                    ScalarValue::Null,
                    ScalarValue::Date(200),
                ],
            },
        ]
    }

    #[test]
    fn segment_roundtrip_with_nulls_and_dates() {
        let dir = tempfile::tempdir().unwrap();
        let path = segment_path(dir.path(), 7, 1, 11);
        write_segment(&path, 7, &sample(), &schema()).unwrap();
        let seg = read_segment(&path, 7).unwrap().expect("segment exists");
        assert_eq!(seg.table_id, 7);
        assert_eq!(seg.min_seq, 10);
        assert_eq!(seg.max_seq, 11);
        assert_eq!(seg.rows.len(), 2);
        assert_eq!(seg.rows[0].key, b"k1".to_vec());
        assert_eq!(seg.rows[1].row[1], ScalarValue::Null);
        assert_eq!(seg.rows[1].row[2], ScalarValue::Date(200));
        // 不存在的代次
        assert!(read_segment(&segment_path(dir.path(), 7, 99, 0), 7)
            .unwrap()
            .is_none());
    }

    #[test]
    fn corrupted_segment_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = segment_path(dir.path(), 7, 1, 11);
        write_segment(&path, 7, &sample(), &schema()).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(read_segment(&path, 7).is_err(), "CRC 应拦截损坏");
    }

    #[test]
    fn empty_segment_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = segment_path(dir.path(), 3, 1, 0);
        write_segment(&path, 3, &[], &schema()).unwrap();
        let seg = read_segment(&path, 3).unwrap().unwrap();
        assert_eq!(seg.rows.len(), 0);
        assert_eq!(seg.min_seq, 0);
    }

    #[test]
    fn file_name_roundtrip_carries_max_seq() {
        let name = segment_path(Path::new("/tmp"), 42, 7, 123)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(name, "42.00007.123.seg");
        assert_eq!(parse_segment_file_name(&name), Some((42, 7, 123)));
        assert_eq!(parse_segment_file_name("42.00007.123"), None);
        assert_eq!(parse_segment_file_name("42.00007.seg"), None);
        assert_eq!(parse_segment_file_name("x.seg"), None);
        assert_eq!(parse_segment_file_name("42.1.2.3.seg"), None);
    }
}


/// 从文件名解析 (table_id, generation, max_seq):
/// "{table_id}.{generation:05}.{max_seq}.seg"。
pub fn parse_segment_file_name(name: &str) -> Option<(u64, u64, u64)> {
    let stem = name.strip_suffix(".seg")?;
    let mut parts = stem.split('.');
    let tid = parts.next()?.parse::<u64>().ok()?;
    let generation = parts.next()?.parse::<u64>().ok()?;
    let max_seq = parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((tid, generation, max_seq))
}
