//! SST 数据块:entry 序列 `[varint klen][key][varint vlen][value]`,
//! 尾部 4 字节 CRC32(body)。
//!
//! v1 不带 restart point(块内 seek 为线性扫描,块 16KiB 量级可接受);
//! 前缀压缩 + restart 数组见路线图。

use common::error::{Error, Result};
use common::util::{crc32, get_varint_u64, put_varint_u64};

pub const BLOCK_TRAILER_LEN: usize = 4;

pub struct BlockBuilder {
    buf: Vec<u8>,
    entries: usize,
    target_size: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(target_size: usize) -> Self {
        Self {
            // usize::MAX(索引块等"不分块"场景)只作为阈值,容量提示截断
            buf: Vec::with_capacity(target_size.min(1 << 24)),
            entries: 0,
            target_size: target_size.max(64),
            last_key: Vec::new(),
        }
    }

    /// 键必须按内部键序严格递增。
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.buf.is_empty()
            && crate::key::compare_internal(key, &self.last_key) != std::cmp::Ordering::Greater
        {
            return Err(Error::invalid_argument(
                "block entries must be added in ascending internal-key order",
            ));
        }
        put_varint_u64(key.len() as u64, &mut self.buf);
        self.buf.extend_from_slice(key);
        put_varint_u64(value.len() as u64, &mut self.buf);
        self.buf.extend_from_slice(value);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entries += 1;
        Ok(())
    }

    /// 当前 body + 即将加入的 entry 是否超过目标块大小。
    pub fn should_finish(&self, next_entry_len: usize) -> bool {
        !self.buf.is_empty()
            && self
                .buf
                .len()
                .saturating_add(next_entry_len)
                .saturating_add(BLOCK_TRAILER_LEN)
                > self.target_size
    }

    pub fn estimated_size(&self) -> usize {
        self.buf.len() + BLOCK_TRAILER_LEN
    }

    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    /// 序列化(body + CRC)。调用后 builder 复位。
    pub fn finish(&mut self) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.buf);
        let crc = crc32(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        self.entries = 0;
        self.last_key.clear();
        out
    }
}

/// 已解码块(body 的借用视图)。
pub struct BlockRef<'a> {
    body: &'a [u8],
}

impl<'a> BlockRef<'a> {
    /// data 含尾部 CRC;校验后返回 body 视图。
    pub fn decode(data: &'a [u8]) -> Result<Self> {
        if data.len() < BLOCK_TRAILER_LEN {
            return Err(Error::corruption("block too short"));
        }
        let split = data.len() - BLOCK_TRAILER_LEN;
        let body = &data[..split];
        let stored = u32::from_le_bytes(data[split..].try_into().unwrap());
        let actual = crc32(body);
        if stored != actual {
            return Err(Error::corruption(format!(
                "block crc mismatch: stored {stored:#x}, actual {actual:#x}"
            )));
        }
        Ok(Self { body })
    }

    pub fn iter(&self) -> BlockIter<'a> {
        BlockIter {
            body: self.body,
            pos: 0,
        }
    }
}

pub struct BlockIter<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> BlockIter<'a> {
    /// 下一个 entry(键为内部键原始字节)。数据损坏时返回 Err 并终止。
    pub fn next(&mut self) -> Option<Result<(&'a [u8], &'a [u8])>> {
        if self.pos >= self.body.len() {
            return None;
        }
        let mut rest = &self.body[self.pos..];
        let klen = match get_varint_u64(&mut rest) {
            Ok(v) => v as usize,
            Err(e) => return Some(Err(e)),
        };
        if rest.len() < klen {
            return Some(Err(Error::corruption("truncated key in block")));
        }
        let key = &rest[..klen];
        rest = &rest[klen..];
        let vlen = match get_varint_u64(&mut rest) {
            Ok(v) => v as usize,
            Err(e) => return Some(Err(e)),
        };
        if rest.len() < vlen {
            return Some(Err(Error::corruption("truncated value in block")));
        }
        let value = &rest[..vlen];
        self.pos = self.body.len() - rest.len() + vlen;
        Some(Ok((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{KeyKind, encode_internal_key};

    #[test]
    fn roundtrip_and_order_enforced() {
        let mut b = BlockBuilder::new(4096);
        let k1 = encode_internal_key(b"apple", 1, KeyKind::Put);
        let k2 = encode_internal_key(b"banana", 2, KeyKind::Put);
        let k3 = encode_internal_key(b"banana", 3, KeyKind::Delete);
        let k_bad = encode_internal_key(b"aardvark", 9, KeyKind::Put);
        b.add(&k1.0, b"v1").unwrap();
        b.add(&k3.0, b"").unwrap();
        b.add(&k2.0, b"v2").unwrap(); // banana: seq 3 在前、seq 2 在后 = 内部键升序,合法
        b.add(&k_bad.0, b"").unwrap_err(); // user_key 倒退 → 乱序被拒
        let data = b.finish();

        let block = BlockRef::decode(&data).unwrap();
        let mut it = block.iter();
        let (k, v) = it.next().unwrap().unwrap();
        assert_eq!(k, k1.0.as_slice());
        assert_eq!(v, b"v1");
        let (k, v) = it.next().unwrap().unwrap();
        assert_eq!(k, k3.0.as_slice());
        assert!(v.is_empty());
        let (k, _) = it.next().unwrap().unwrap();
        assert_eq!(k, k2.0.as_slice());
        assert!(it.next().is_none());
    }

    #[test]
    fn crc_mismatch_detected() {
        let mut b = BlockBuilder::new(4096);
        b.add(&encode_internal_key(b"k", 1, KeyKind::Put).0, b"v")
            .unwrap();
        let mut data = b.finish();
        let last = data.len() - 1;
        data[last] ^= 0xff;
        assert!(BlockRef::decode(&data).is_err());
    }

    #[test]
    fn should_finish_respects_target() {
        let mut b = BlockBuilder::new(128);
        assert!(!b.should_finish(50), "空块不应触发 finish");
        b.add(
            &encode_internal_key(b"a", 1, KeyKind::Put).0,
            vec![0u8; 100].as_slice(),
        )
        .unwrap();
        assert!(b.should_finish(50));
    }
}
