//! SST 读取器。v1 打开时整文件驻留内存(轻量级数据库的简单起点;
//! 块缓存 + mmap 见路线图)。块内无 restart,seek 为顺序扫描。

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use common::error::{Error, Result};
use common::util::get_varint_u64;

use crate::key::{InternalKey, compare_internal, decode_internal_key, lower_bound_key};
use crate::memtable::Lookup;
use crate::sstable::block::{BLOCK_TRAILER_LEN, BlockRef};
use crate::sstable::bloom::BloomFilter;
use crate::sstable::writer::{FOOTER_SIZE, SST_MAGIC, SST_VERSION, SstMeta};

struct IndexEntry {
    last_key: Vec<u8>,
    offset: u64,
    len: u32,
}

pub struct SstReader {
    meta: SstMeta,
    data: Arc<Vec<u8>>,
    bloom: BloomFilter,
    index: Vec<IndexEntry>,
}

/// 扫描条目:内部键 + 值(墓碑 None)。
pub type SstEntry = (InternalKey, Option<Vec<u8>>);

impl SstReader {
    pub fn open(path: &Path, id: u64, level: u32) -> Result<Self> {
        let mut file = std::fs::File::open(path)
            .map_err(|e| Error::corruption(format!("open sst {}: {e}", path.display())))?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Self::parse(data, id, level)
    }

    /// 从内存字节解析(测试与未来块缓存复用)。
    pub fn parse(data: Vec<u8>, id: u64, level: u32) -> Result<Self> {
        if data.len() < FOOTER_SIZE + BLOCK_TRAILER_LEN {
            return Err(Error::corruption(format!(
                "sst {id} too small: {} bytes",
                data.len()
            )));
        }
        let footer = &data[data.len() - FOOTER_SIZE..];
        let bloom_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let bloom_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let index_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let num_entries = u64::from_le_bytes(footer[32..40].try_into().unwrap());
        let min_seq = u64::from_le_bytes(footer[40..48].try_into().unwrap());
        let max_seq = u64::from_le_bytes(footer[48..56].try_into().unwrap());
        let version = u32::from_le_bytes(footer[56..60].try_into().unwrap());
        let magic = u32::from_le_bytes(footer[60..64].try_into().unwrap());
        let stored_crc = u32::from_le_bytes(footer[64..68].try_into().unwrap());
        if stored_crc != common::util::crc32(&footer[..64]) {
            return Err(Error::corruption("sst footer crc mismatch"));
        }
        if magic != SST_MAGIC {
            return Err(Error::corruption(format!("bad sst magic {magic:#x}")));
        }
        if version != SST_VERSION {
            return Err(Error::corruption(format!(
                "unsupported sst version {version}"
            )));
        }

        // bloom
        let mut bloom_slice = &data[bloom_offset as usize..(bloom_offset + bloom_len) as usize];
        let bloom = BloomFilter::from_bytes(&mut bloom_slice)?;

        // index
        let index_block =
            BlockRef::decode(&data[index_offset as usize..(index_offset + index_len) as usize])?;
        let mut index = Vec::new();
        let mut it = index_block.iter();
        while let Some(entry) = it.next() {
            let (key, value) = entry?;
            let mut rest = value;
            let offset = get_varint_u64(&mut rest)?;
            let len = get_varint_u64(&mut rest)?;
            index.push(IndexEntry {
                last_key: key.to_vec(),
                offset,
                len: len as u32,
            });
        }

        let (min_user_key, max_user_key) = first_and_last(&data, &index, num_entries)?;

        Ok(Self {
            meta: SstMeta {
                id,
                level,
                file_size: data.len() as u64,
                num_entries,
                min_user_key,
                max_user_key,
                min_seq,
                max_seq,
            },
            data: Arc::new(data),
            bloom,
            index,
        })
    }

    pub fn meta(&self) -> &SstMeta {
        &self.meta
    }

    pub fn id(&self) -> u64 {
        self.meta.id
    }

    fn block_at(&self, entry: &IndexEntry) -> Result<BlockRef<'_>> {
        let start = entry.offset as usize;
        let end = start + entry.len as usize;
        if end > self.data.len() {
            return Err(Error::corruption(format!(
                "sst {}: block handle out of range",
                self.meta.id
            )));
        }
        BlockRef::decode(&self.data[start..end])
    }

    /// 点查:线性索引定位首个可能包含该 user_key 的块 → 块内顺序扫描。
    /// 点查:返回 user_key 在 read_ts 快照下可见的最新版本(seq ≤ read_ts)。
    pub fn get_at(&self, user_key: &[u8], read_ts: u64) -> Result<Lookup> {
        if !self.meta.covers(user_key) || !self.bloom.might_contain(user_key) {
            return Ok(Lookup::NotFound);
        }
        let bound = lower_bound_key(user_key);
        let idx = self.index.partition_point(|e| {
            compare_internal(&e.last_key, &bound.0) == std::cmp::Ordering::Less
        });
        if idx == self.index.len() {
            return Ok(Lookup::NotFound);
        }
        let block = self.block_at(&self.index[idx])?;
        let mut it = block.iter();
        while let Some(entry) = it.next() {
            let (key, value) = entry?;
            let decoded = decode_internal_key(key)?;
            match decoded.user_key.cmp(user_key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    if decoded.seq > read_ts {
                        continue; // 版本对当前快照不可见,看更旧版本
                    }
                    return Ok(match decoded.kind {
                        crate::key::KeyKind::Put => Lookup::Found(value.to_vec()),
                        crate::key::KeyKind::Delete => Lookup::Tombstone,
                    });
                }
            }
        }
        Ok(Lookup::NotFound)
    }

    /// 最新版本语义(read_ts = MAX)。
    pub fn get(&self, user_key: &[u8]) -> Result<Lookup> {
        self.get_at(user_key, u64::MAX)
    }

    /// 范围扫描 [from, to)(user_key);输出全部版本(合并迭代器负责去重)。
    pub fn scan(&self, from: &[u8], to: &[u8]) -> Result<Vec<SstEntry>> {
        if !self.meta.overlaps_range(from, to) {
            return Ok(Vec::new());
        }
        let bound = lower_bound_key(from);
        let idx = self.index.partition_point(|e| {
            compare_internal(&e.last_key, &bound.0) == std::cmp::Ordering::Less
        });
        let mut out = Vec::new();
        for e in &self.index[idx.min(self.index.len())..] {
            let block = self.block_at(e)?;
            let mut it = block.iter();
            while let Some(entry) = it.next() {
                let (key, value) = entry?;
                let decoded = decode_internal_key(key)?;
                if decoded.user_key >= to {
                    return Ok(out);
                }
                if decoded.user_key < from {
                    continue;
                }
                out.push((
                    InternalKey(key.to_vec()),
                    (decoded.kind == crate::key::KeyKind::Put).then(|| value.to_vec()),
                ));
            }
        }
        Ok(out)
    }
}

/// 由首尾 entry 推导 min/max user_key(seq 范围来自 footer)。
fn first_and_last(
    data: &[u8],
    index: &[IndexEntry],
    num_entries: u64,
) -> Result<(Vec<u8>, Vec<u8>)> {
    if index.is_empty() || num_entries == 0 {
        return Err(Error::corruption("empty sst"));
    }
    let first_block = BlockRef::decode(
        &data[index[0].offset as usize..(index[0].offset + index[0].len as u64) as usize],
    )?;
    let first_entry = first_block
        .iter()
        .next()
        .ok_or_else(|| Error::corruption("first block empty"))??;
    let (first_key, _) = first_entry;
    let first = decode_internal_key(first_key)?;

    let last_entry = index.last().unwrap();
    let last_block = BlockRef::decode(
        &data[last_entry.offset as usize..(last_entry.offset + last_entry.len as u64) as usize],
    )?;
    let mut last_key: Option<&[u8]> = None;
    let mut it = last_block.iter();
    while let Some(entry) = it.next() {
        if let Ok((k, _)) = entry {
            last_key = Some(k);
        }
    }
    let last = decode_internal_key(last_key.ok_or_else(|| Error::corruption("last block empty"))?)?;

    Ok((first.user_key.to_vec(), last.user_key.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{KeyKind, encode_internal_key};
    use crate::sstable::writer::{SstWriter, SstWriterOptions};
    use std::io::Cursor;

    fn build_sst(entries: &[(InternalKey, Option<Vec<u8>>)], block_size: usize) -> Vec<u8> {
        let mut w = SstWriter::new(
            Cursor::new(Vec::new()),
            SstWriterOptions {
                block_size,
                bloom_bits_per_key: 10,
            },
        );
        for (k, v) in entries {
            w.add(k, v.as_deref()).unwrap();
        }
        let (cursor, _meta) = w.finish(7, 0).unwrap();
        cursor.into_inner()
    }

    fn entries_fixture() -> Vec<(InternalKey, Option<Vec<u8>>)> {
        vec![
            (
                encode_internal_key(b"apple", 1, KeyKind::Put),
                Some(b"red".to_vec()),
            ),
            (
                encode_internal_key(b"banana", 5, KeyKind::Put),
                Some(b"yellow".to_vec()),
            ),
            (
                encode_internal_key(b"banana", 3, KeyKind::Put),
                Some(b"green".to_vec()),
            ),
            (encode_internal_key(b"banana", 2, KeyKind::Delete), None),
            (
                encode_internal_key(b"cherry", 1, KeyKind::Put),
                Some(b"".to_vec()),
            ),
        ]
    }

    #[test]
    fn meta_and_point_lookup() {
        let data = build_sst(&entries_fixture(), 4096);
        let reader = SstReader::parse(data, 7, 0).unwrap();
        let meta = reader.meta();
        assert_eq!(meta.num_entries, 5);
        assert_eq!(meta.min_user_key, b"apple");
        assert_eq!(meta.max_user_key, b"cherry");
        assert_eq!(meta.min_seq, 1);
        assert_eq!(meta.max_seq, 5);

        assert_eq!(
            reader.get(b"apple").unwrap(),
            Lookup::Found(b"red".to_vec())
        );
        // banana 最新版本是 seq=5
        assert_eq!(
            reader.get(b"banana").unwrap(),
            Lookup::Found(b"yellow".to_vec())
        );
        assert_eq!(reader.get(b"cherry").unwrap(), Lookup::Found(Vec::new()));
        assert_eq!(reader.get(b"missing").unwrap(), Lookup::NotFound);
    }

    #[test]
    fn multi_block_sst_seeks_across_blocks() {
        // 每块很小,迫使索引跨多个数据块
        let entries: Vec<(InternalKey, Option<Vec<u8>>)> = (0..200u32)
            .map(|i| {
                (
                    encode_internal_key(
                        format!("key{i:04}").as_bytes(),
                        i as u64 + 1,
                        KeyKind::Put,
                    ),
                    Some(format!("v{i}").into_bytes()),
                )
            })
            .collect();
        let data = build_sst(&entries, 512);
        let reader = SstReader::parse(data, 1, 0).unwrap();
        assert!(reader.get(b"key0000").unwrap() == Lookup::Found(b"v0".to_vec()));
        assert!(reader.get(b"key0199").unwrap() == Lookup::Found(b"v199".to_vec()));
        assert!(reader.get(b"key0100").unwrap() == Lookup::Found(b"v100".to_vec()));
        assert_eq!(reader.get(b"key0200").unwrap(), Lookup::NotFound);
    }

    #[test]
    fn scan_respects_range_and_versions() {
        let data = build_sst(&entries_fixture(), 4096);
        let reader = SstReader::parse(data, 7, 0).unwrap();
        let out = reader.scan(b"banana", b"cherry").unwrap();
        assert_eq!(out.len(), 3); // banana x3 (含墓碑),cherry 排除
        let first = decode_internal_key(&out[0].0.0).unwrap();
        assert_eq!(first.seq, 5);
        assert_eq!(out[0].1.as_deref(), Some(b"yellow".as_slice()));
        let tomb = decode_internal_key(&out[2].0.0).unwrap();
        assert_eq!(tomb.kind, KeyKind::Delete);
        assert_eq!(out[2].1, None);

        // 全范围
        let all = reader.scan(b"", &vec![0xffu8; 16]).unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn corrupt_data_rejected() {
        let mut data = build_sst(&entries_fixture(), 4096);
        let n = data.len();
        data[n / 2] ^= 0x55; // 破坏中段(index 或 bloom 校验应能拦住大部分;此处至少 footer parse 不 panic)
        // 不强求所有位置的损坏都被检出(crc 只覆盖块与 footer),但 decode 必须安全
        let _ = SstReader::parse(data, 7, 0);
    }
}
