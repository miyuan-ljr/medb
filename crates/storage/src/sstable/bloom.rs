//! 经典 bloom filter(SST 元数据块):FNV-1a 单哈希 + 双重哈希探针。
//! 磁盘格式:`varint(bits) varint(probes) data[bytes]`,全部字节序为小端无关的原始位图。

use common::error::{Error, Result};
use common::util::{fnv1a64, get_varint_u64, put_varint_u64};

pub struct BloomFilter {
    bits: u64,
    probes: u32,
    data: Vec<u8>,
}

impl BloomFilter {
    /// keys: 全部 user_key;bits_per_key 决定误报率(~1% @10 bits/key)。
    pub fn build(keys: &[&[u8]], bits_per_key: usize) -> Self {
        let n = keys.len();
        if n == 0 {
            return Self {
                bits: 0,
                probes: 0,
                data: Vec::new(),
            };
        }
        let bits = ((n * bits_per_key.max(1)) as u64).max(64);
        let bytes = bits.div_ceil(8) as usize;
        let bits = bytes as u64 * 8;
        // k = bits/n/ln2,夹在 [1, 30]
        let k = ((bits / n as u64) as f64 * 0.69) as u32;
        let probes = k.clamp(1, 30);
        let mut data = vec![0u8; bytes];
        for key in keys {
            let h = fnv1a64(key);
            let delta = (h >> 17) | (h << 15); // 旋转,得到第二个独立探针序列
            for i in 0..probes {
                let idx = (h.wrapping_add((i as u64).wrapping_mul(delta))) % bits;
                data[(idx / 8) as usize] |= 1 << (idx % 8);
            }
        }
        Self { bits, probes, data }
    }

    /// 误报可能发生,漏报不会。空 filter(无 keys)恒 true(保守路径)。
    pub fn might_contain(&self, key: &[u8]) -> bool {
        if self.bits == 0 {
            return true;
        }
        let h = fnv1a64(key);
        let delta = (h >> 17) | (h << 15);
        (0..self.probes).all(|i| {
            let idx = (h.wrapping_add((i as u64).wrapping_mul(delta))) % self.bits;
            self.data[(idx / 8) as usize] & (1 << (idx % 8)) != 0
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.data.len() + 16);
        put_varint_u64(self.bits, &mut out);
        put_varint_u64(self.probes as u64, &mut out);
        out.extend_from_slice(&self.data);
        out
    }

    pub fn from_bytes(buf: &mut &[u8]) -> Result<Self> {
        let bits = get_varint_u64(buf)?;
        let probes = get_varint_u64(buf)? as u32;
        if bits == 0 {
            return Ok(Self {
                bits: 0,
                probes: 0,
                data: Vec::new(),
            });
        }
        let bytes = bits.div_ceil(8) as usize;
        if buf.len() < bytes {
            return Err(Error::corruption("truncated bloom filter block"));
        }
        let data = buf[..bytes].to_vec();
        *buf = &buf[bytes..];
        Ok(Self { bits, probes, data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("key-{i}").into_bytes())
            .collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let bloom = BloomFilter::build(&refs, 10);
        for k in &keys {
            assert!(bloom.might_contain(k), "must contain {k:?}");
        }
    }

    #[test]
    fn false_positive_rate_bounded() {
        let keys: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("key-{i}").into_bytes())
            .collect();
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let bloom = BloomFilter::build(&refs, 10);
        let absent: Vec<Vec<u8>> = (0..5000u32)
            .map(|i| format!("absent-{i}").into_bytes())
            .collect();
        let hits = absent.iter().filter(|k| bloom.might_contain(k)).count();
        let rate = hits as f64 / absent.len() as f64;
        assert!(rate < 0.05, "10 bits/key 应 ~1% 误报,实测 {rate:.3}");
    }

    #[test]
    fn roundtrip_bytes() {
        let keys: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let bloom = BloomFilter::build(&keys, 10);
        let bytes = bloom.to_bytes();
        let back = BloomFilter::from_bytes(&mut bytes.as_slice()).unwrap();
        for k in keys {
            assert!(back.might_contain(k));
        }
        assert_eq!(back.to_bytes(), bytes);
    }

    #[test]
    fn empty_bloom_is_conservative() {
        let bloom = BloomFilter::build(&[], 10);
        assert!(bloom.might_contain(b"anything"));
        let bytes = bloom.to_bytes();
        let back = BloomFilter::from_bytes(&mut bytes.as_slice()).unwrap();
        assert!(back.might_contain(b"anything"));
    }
}
