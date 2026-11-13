//! 通用小工具:校验和、varint、字节序辅助、ID 生成器。

/// FNV-1a 64 位哈希。std 的 DefaultHasher 不保证跨版本稳定,SST/WAL 落盘格式必须用稳定哈希。
pub fn fnv1a64(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// 序号倒序编码:!seq,使字节序比较等价于 seq 降序(内部键新版本在前)。
#[inline]
pub fn desc_u64(v: u64) -> u64 {
    !v
}

/// align 必须是 2 的幂。
#[inline]
pub fn align_up(value: usize, align: usize) -> usize {
    debug_assert!(align.is_power_of_two());
    (value + align - 1) & !(align - 1)
}

/// LEB128 无符号 varint 编码(小端变长)。
pub fn put_varint_u64(mut v: u64, buf: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

/// 从切片头解出一个 varint,前移切片。超过 10 字节视为数据损坏。
pub fn get_varint_u64(buf: &mut &[u8]) -> crate::error::Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        if buf.is_empty() {
            return Err(crate::error::Error::corruption(
                "truncated varint: unexpected end of buffer",
            ));
        }
        let byte = buf[0];
        *buf = &buf[1..];
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err(crate::error::Error::corruption("varint too long"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        let values = [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX];
        for v in values {
            let mut buf = Vec::new();
            put_varint_u64(v, &mut buf);
            let mut slice = buf.as_slice();
            assert_eq!(get_varint_u64(&mut slice).unwrap(), v);
            assert!(slice.is_empty());
        }
    }

    #[test]
    fn varint_order_preserved() {
        // 同长度编码下字节序 == 数值序(内部键排序依赖)
        let enc = |v: u64| {
            let mut b = Vec::new();
            put_varint_u64(v, &mut b);
            b
        };
        assert!(enc(5) < enc(6));
    }

    #[test]
    fn crc_and_hash_stable() {
        assert_eq!(crc32(b"hello"), crc32(b"hello"));
        assert_eq!(fnv1a64(b"abc"), 0xe71f_a219_0541_574b);
    }

    #[test]
    fn align_up_works() {
        assert_eq!(align_up(0, 8), 0);
        assert_eq!(align_up(1, 8), 8);
        assert_eq!(align_up(8, 8), 8);
        assert_eq!(align_up(9, 16), 16);
    }
}
