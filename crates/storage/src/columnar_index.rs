//! 列存索引文件:每 segment × 每索引列一棵 B 树(`BTreeMap<编码值, 行键列表>`)。
//!
//! 架构约定:二级索引建在列存 segment 上(与 segment 同生命周期):
//!   * segment 重建(flush 钩子)→ 为 catalog 中登记的每个索引列重建树;
//!   * 文件按 `{table_id}.{gen:05}.{max_seq}.{col_idx}.idx` 命名,与 segment
//!     同前缀 → 旧代清理/陈旧检测/ALTER 失效与 segment 文件同一套规则;
//!   * memtable 增量不在树里 —— 查询时由调用方与 delta 合并(见 engine)。
//!
//! 文件布局(小端):
//! ```text
//! magic "MEDBIDX1" (8B)
//! version u32 = 1
//! table_id u64
//! col_idx u32
//! entry_count u64
//! entries: entry_count × {          // 按编码值升序(BTreeMap 迭代序)
//!     enc:  u32 len + bytes         // memcomparable 编码的列值
//!     n:    u64                     // 该值的行键数
//!     keys: n × (u32 len + bytes)   // 行键升序
//! }
//! crc32 u32
//! ```
//! 写入 = tmp + rename 原子替换;损坏读出报错(派生数据,调用方可回退全扫)。

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use common::error::{Error, Result};
use common::util::crc32;

const MAGIC: &[u8; 8] = b"MEDBIDX1";
const VERSION: u32 = 1;

/// 一棵索引树:编码列值 → 行键列表(行键升序)。
pub type IndexTree = BTreeMap<Vec<u8>, Vec<Vec<u8>>>;

pub fn index_path(dir: &Path, table_id: u64, generation: u64, max_seq: u64, col_idx: u32) -> PathBuf {
    dir.join(format!("{table_id}.{generation:05}.{max_seq}.{col_idx}.idx"))
}

/// 从文件名解析 (table_id, generation, max_seq, col_idx)。
pub fn parse_index_file_name(name: &str) -> Option<(u64, u64, u64, u32)> {
    let stem = name.strip_suffix(".idx")?;
    let mut parts = stem.split('.');
    let tid = parts.next()?.parse::<u64>().ok()?;
    let generation = parts.next()?.parse::<u64>().ok()?;
    let max_seq = parts.next()?.parse::<u64>().ok()?;
    let col_idx = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((tid, generation, max_seq, col_idx))
}

fn put_len(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// 写索引树;tree 键序 = BTreeMap 迭代序(升序)。tmp + rename 原子替换。
pub fn write_index(path: &Path, table_id: u64, col_idx: u32, tree: &IndexTree) -> Result<()> {
    let mut buf = Vec::with_capacity(16 * 1024);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&table_id.to_le_bytes());
    buf.extend_from_slice(&col_idx.to_le_bytes());
    buf.extend_from_slice(&(tree.len() as u64).to_le_bytes());
    for (enc, keys) in tree {
        put_len(&mut buf, enc);
        buf.extend_from_slice(&(keys.len() as u64).to_le_bytes());
        for k in keys {
            put_len(&mut buf, k);
        }
    }
    let crc = crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let tmp = path.with_extension("idx.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_data()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// 读索引树;文件缺失返回 None(派生数据,调用方回退全扫)。
pub fn read_index(path: &Path) -> Result<Option<IndexTree>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    const HEADER: usize = 8 + 4 + 8 + 4 + 8;
    if bytes.len() < HEADER + 4 {
        return Err(Error::corruption("columnar index truncated"));
    }
    if &bytes[..8] != MAGIC {
        return Err(Error::corruption("columnar index magic mismatch"));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if version != VERSION {
        return Err(Error::corruption(format!("columnar index version {version}")));
    }
    let body_end = bytes.len() - 4;
    let stored_crc = u32::from_le_bytes(bytes[body_end..].try_into().unwrap());
    if crc32(&bytes[..body_end]) != stored_crc {
        return Err(Error::corruption("columnar index crc mismatch"));
    }
    let mut s = &bytes[HEADER..body_end];

    let mut tree = IndexTree::new();
    for _ in 0..u64::from_le_bytes(bytes[24..32].try_into().unwrap()) {
        let enc = take_len(&mut s)?;
        let n = take_u64(&mut s)?;
        let mut keys = Vec::with_capacity(n.min(1024) as usize);
        for _ in 0..n {
            keys.push(take_len(&mut s)?);
        }
        tree.insert(enc, keys);
    }
    if !s.is_empty() {
        return Err(Error::corruption("columnar index trailing bytes"));
    }
    Ok(Some(tree))
}

fn take_u64<'a>(s: &mut &'a [u8]) -> Result<u64> {
    if s.len() < 8 {
        return Err(Error::corruption("columnar index truncated"));
    }
    let v = u64::from_le_bytes(s[..8].try_into().unwrap());
    *s = &s[8..];
    Ok(v)
}

fn take_len<'a>(s: &mut &'a [u8]) -> Result<Vec<u8>> {
    if s.len() < 4 {
        return Err(Error::corruption("columnar index truncated"));
    }
    let len = u32::from_le_bytes(s[..4].try_into().unwrap()) as usize;
    *s = &s[4..];
    if s.len() < len {
        return Err(Error::corruption("columnar index truncated"));
    }
    let out = s[..len].to_vec();
    *s = &s[len..];
    Ok(out)
}

/// 从按行键升序的 (行键, 行) 序列构建索引树(值相同按行键序追加)。
pub fn build_tree<'a>(
    rows: impl Iterator<Item = (&'a [u8], Option<&'a common::types::ScalarValue>)>,
    encode: impl Fn(&common::types::ScalarValue) -> Result<Vec<u8>>,
) -> Result<IndexTree> {
    let mut tree = IndexTree::new();
    for (key, value) in rows {
        let Some(v) = value else { continue };
        let enc = encode(v)?;
        tree.entry(enc).or_default().push(key.to_vec());
    }
    Ok(tree)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::types::ScalarValue;

    #[test]
    fn index_roundtrip_and_range() {
        let dir = tempfile::tempdir().unwrap();
        let path = index_path(dir.path(), 42, 3, 99, 1);
        let mut tree = IndexTree::new();
        tree.insert(b"\x02\xa5\xfd\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(), vec![b"k2".to_vec()]);
        tree.insert(b"\x02\xa5\xfd\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(), vec![b"k1".to_vec(), b"k3".to_vec()]);
        write_index(&path, 42, 1, &tree).unwrap();

        let loaded = read_index(&path).unwrap().unwrap();
        assert_eq!(loaded.len(), 2);
        // 范围扫描:闭区间两键
        let hit: Vec<_> = loaded
            .range(b"\x02\xa5\xfd\x02".to_vec()..=b"\x02\xa5\xfd\x03\xff".to_vec())
            .flat_map(|(_, keys)| keys.iter().cloned())
            .collect();
        assert_eq!(hit, vec![b"k2".to_vec(), b"k1".to_vec(), b"k3".to_vec()]);
        let _ = (table_id_of(&path), col_idx_of(&path));
    }

    fn table_id_of(p: &Path) -> u64 {
        parse_index_file_name(p.file_name().unwrap().to_str().unwrap()).unwrap().0
    }
    fn col_idx_of(p: &Path) -> u32 {
        parse_index_file_name(p.file_name().unwrap().to_str().unwrap()).unwrap().3
    }

    #[test]
    fn corrupted_index_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = index_path(dir.path(), 1, 1, 1, 0);
        write_index(&path, 1, 0, &IndexTree::new()).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(read_index(&path).is_err());
    }

    #[test]
    fn missing_index_is_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_index(&index_path(dir.path(), 1, 1, 1, 0))
            .unwrap()
            .is_none());
    }

    #[test]
    fn build_tree_encodes_in_order() {
        let rows: Vec<(&[u8], Option<ScalarValue>)> = vec![
            (b"k1".as_slice(), Some(ScalarValue::Int64(5))),
            (b"k2".as_slice(), Some(ScalarValue::Int64(3))),
            (b"k3".as_slice(), None),
            (b"k4".as_slice(), Some(ScalarValue::Int64(3))),
        ];
        let tree = build_tree(
            rows.iter().map(|(k, v)| (*k, v.as_ref())),
            |v| encode_pk_component_test(v),
        )
        .unwrap();
        // 值 3 在前(BTreeMap 键序),其行键按输入序(升序)
        let keys: Vec<_> = tree.keys().collect();
        assert_eq!(keys.len(), 2);
        assert_eq!(tree[keys[0]], vec![b"k2".to_vec(), b"k4".to_vec()]);
        assert_eq!(tree[keys[1]], vec![b"k1".to_vec()]);
    }

    fn encode_pk_component_test(v: &ScalarValue) -> Result<Vec<u8>> {
        crate::table::encode_pk_component(v)
    }
}
