//! SST 写入器。
//!
//! 文件布局(v1,无前缀压缩):
//! ```text
//! [data block 0] [data block 1] ... [bloom block] [index block] [footer 68B]
//! ```
//! - index block 复用 Block 格式:entry = (块内最大内部键, varint(offset) + varint(len));
//! - footer:`bloom_off u64LE | bloom_len u64LE | index_off u64LE | index_len u64LE
//!            | num_entries u64LE | min_seq u64LE | max_seq u64LE
//!            | version u32LE | magic "LTB1" u32LE | crc32 u32LE`。

use std::io::Write;

use common::error::{Error, Result};
use common::util::crc32;

use crate::key::{InternalKey, decode_internal_key};
use crate::sstable::block::{BLOCK_TRAILER_LEN, BlockBuilder};
use crate::sstable::bloom::BloomFilter;

pub const SST_MAGIC: u32 = u32::from_le_bytes(*b"LTB1");
pub const SST_VERSION: u32 = 1;
pub const FOOTER_SIZE: usize = 8 * 7 + 4 * 3;

#[derive(Debug, Clone)]
pub struct SstWriterOptions {
    pub block_size: usize,
    pub bloom_bits_per_key: usize,
}

impl Default for SstWriterOptions {
    fn default() -> Self {
        Self {
            block_size: 16 << 10,
            bloom_bits_per_key: 10,
        }
    }
}

/// SST 元数据(内存态;落盘 manifest 见路线图)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstMeta {
    pub id: u64,
    pub level: u32,
    pub file_size: u64,
    pub num_entries: u64,
    pub min_user_key: Vec<u8>,
    pub max_user_key: Vec<u8>,
    pub min_seq: u64,
    pub max_seq: u64,
}

impl SstMeta {
    pub fn covers(&self, user_key: &[u8]) -> bool {
        user_key >= self.min_user_key.as_slice() && user_key <= self.max_user_key.as_slice()
    }

    pub fn overlaps_range(&self, from: &[u8], to: &[u8]) -> bool {
        // [from, to) 与 [min, max] 相交
        self.max_user_key.as_slice() >= from && self.min_user_key.as_slice() < to
    }
}

struct IndexEntry {
    last_key: Vec<u8>,
    offset: u64,
    len: u32,
}

pub struct SstWriter<W: Write> {
    out: W,
    opts: SstWriterOptions,
    offset: u64,
    block: BlockBuilder,
    index: Vec<IndexEntry>,
    bloom_keys: Vec<Vec<u8>>,
    num_entries: u64,
    min_key: Option<Vec<u8>>,
    max_key: Option<Vec<u8>>,
    min_seq: Option<u64>,
    max_seq: Option<u64>,
}

impl<W: Write> SstWriter<W> {
    pub fn new(out: W, opts: SstWriterOptions) -> Self {
        Self {
            out,
            block: BlockBuilder::new(opts.block_size),
            offset: 0,
            index: Vec::new(),
            bloom_keys: Vec::new(),
            num_entries: 0,
            min_key: None,
            max_key: None,
            min_seq: None,
            max_seq: None,
            opts,
        }
    }

    /// 追加一个版本;value 为 None 表示墓碑(kind 由内部键携带)。
    pub fn add(&mut self, internal_key: &InternalKey, value: Option<&[u8]>) -> Result<()> {
        let decoded = decode_internal_key(&internal_key.0)
            .map_err(|e| Error::invalid_argument(format!("bad internal key: {e}")))?;
        let v = value.unwrap_or(&[]);
        let next_len = internal_key.0.len() + v.len() + 10;
        if self.block.should_finish(next_len) {
            self.flush_data_block()?;
        }
        self.block.add(&internal_key.0, v)?;

        if self.min_key.is_none() {
            self.min_key = Some(decoded.user_key.to_vec());
        }
        self.max_key = Some(decoded.user_key.to_vec());
        self.min_seq = Some(self.min_seq.map_or(decoded.seq, |m| m.min(decoded.seq)));
        self.max_seq = Some(self.max_seq.map_or(decoded.seq, |m| m.max(decoded.seq)));
        self.bloom_keys.push(decoded.user_key.to_vec());
        self.num_entries += 1;
        Ok(())
    }

    fn flush_data_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let last_key = self.block.last_key().to_vec();
        let data = self.block.finish();
        let len = data.len();
        self.out.write_all(&data)?;
        self.index.push(IndexEntry {
            last_key,
            offset: self.offset,
            len: len as u32,
        });
        self.offset += len as u64;
        Ok(())
    }

    /// 收尾并落盘,返回 `(底层 W, 元数据)`(id/level 由调用方赋予)。
    pub fn finish(mut self, id: u64, level: u32) -> Result<(W, SstMeta)> {
        self.flush_data_block()?;

        // bloom block
        let refs: Vec<&[u8]> = self.bloom_keys.iter().map(|k| k.as_slice()).collect();
        let bloom = BloomFilter::build(&refs, self.opts.bloom_bits_per_key);
        let bloom_bytes = bloom.to_bytes();
        let bloom_offset = self.offset;
        self.out.write_all(&bloom_bytes)?;
        self.offset += bloom_bytes.len() as u64;

        // index block(复用 Block 格式)
        let mut index_builder = BlockBuilder::new(usize::MAX);
        for e in &self.index {
            let mut value = Vec::with_capacity(20);
            common::util::put_varint_u64(e.offset, &mut value);
            common::util::put_varint_u64(e.len as u64, &mut value);
            index_builder.add(&e.last_key, &value)?;
        }
        let index_bytes = index_builder.finish();
        let index_offset = self.offset;
        let index_len = index_bytes.len();
        self.out.write_all(&index_bytes)?;
        self.offset += index_len as u64;

        // footer
        let mut footer = Vec::with_capacity(FOOTER_SIZE);
        footer.extend_from_slice(&bloom_offset.to_le_bytes());
        footer.extend_from_slice(&(bloom_bytes.len() as u64).to_le_bytes());
        footer.extend_from_slice(&index_offset.to_le_bytes());
        footer.extend_from_slice(&(index_len as u64).to_le_bytes());
        footer.extend_from_slice(&self.num_entries.to_le_bytes());
        footer.extend_from_slice(&self.min_seq.unwrap_or(0).to_le_bytes());
        footer.extend_from_slice(&self.max_seq.unwrap_or(0).to_le_bytes());
        footer.extend_from_slice(&SST_VERSION.to_le_bytes());
        footer.extend_from_slice(&SST_MAGIC.to_le_bytes());
        let footer_crc = crc32(&footer);
        footer.extend_from_slice(&footer_crc.to_le_bytes());
        debug_assert_eq!(footer.len(), FOOTER_SIZE);
        self.out.write_all(&footer)?;
        self.out.flush()?;

        let meta = SstMeta {
            id,
            level,
            file_size: self.offset + footer.len() as u64,
            num_entries: self.num_entries,
            min_user_key: self
                .min_key
                .ok_or_else(|| Error::invalid_argument("finish called on empty sst writer"))?,
            max_user_key: self.max_key.expect("entries exist"),
            min_seq: self.min_seq.unwrap_or(0),
            max_seq: self.max_seq.unwrap_or(0),
        };
        Ok((self.out, meta))
    }
}

/// 数据块在文件中的位置(读取路径用)。
#[derive(Debug, Clone)]
pub struct BlockHandle {
    pub offset: u64,
    pub len: u32,
}

impl BlockHandle {
    pub fn on_disk_size(&self) -> usize {
        self.len as usize + BLOCK_TRAILER_LEN
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_constants_consistent() {
        assert_eq!(FOOTER_SIZE, 68);
    }
}
