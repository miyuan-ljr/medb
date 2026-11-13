//! 内部键编码:`user_key ++ [!seq (8B, BE) ++ kind (1B)]`。
//!
//! 同一 user_key 内,seq 大者其 !seq 字节序小 → 任何按内部键有序的介质
//! (memtable 跳表、SST)迭代时新版本在前,点查/合并迭代天然取最新。
//!
//! 比较器必须"先比 user_key(memcmp),再比 9 字节 trailer(倒序)",
//! 不能对原始字节做 memcmp —— user_key "ab" 与 "abc" 的内部键
//! 在纯字节序上会被 trailer 字节干扰。

use std::cmp::Ordering;

use common::error::{Error, Result};

pub const TRAILER_LEN: usize = 9;
/// seq == u64::MAX 的保留值(查询下界哨兵用),正常写入不会分配。
pub const SEQ_MAX: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    Put = 1,
    Delete = 2,
}

impl KeyKind {
    pub fn from_u8(b: u8) -> Result<KeyKind> {
        match b {
            1 => Ok(KeyKind::Put),
            2 => Ok(KeyKind::Delete),
            other => Err(Error::corruption(format!("unknown key kind {other:#x}"))),
        }
    }
}

/// 内部键(原始字节)。跨 memtable/SST 的统一有序表示。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey(pub Vec<u8>);

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_internal(&self.0, &other.0)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl AsRef<[u8]> for InternalKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// 内部键比较:先 user_key memcmp,再 trailer 字节序(trailer 已倒序编码,直接比即可)。
pub fn compare_internal(a: &[u8], b: &[u8]) -> Ordering {
    let ua = &a[..a.len() - TRAILER_LEN];
    let ub = &b[..b.len() - TRAILER_LEN];
    match ua.cmp(ub) {
        Ordering::Equal => a[a.len() - TRAILER_LEN..].cmp(&b[b.len() - TRAILER_LEN..]),
        ord => ord,
    }
}

pub fn encode_internal_key(user_key: &[u8], seq: u64, kind: KeyKind) -> InternalKey {
    let mut buf = Vec::with_capacity(user_key.len() + TRAILER_LEN);
    buf.extend_from_slice(user_key);
    buf.extend_from_slice(&common::util::desc_u64(seq).to_be_bytes());
    buf.push(kind as u8);
    InternalKey(buf)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedKey<'a> {
    pub user_key: &'a [u8],
    pub seq: u64,
    pub kind: KeyKind,
}

pub fn decode_internal_key(raw: &[u8]) -> Result<DecodedKey<'_>> {
    if raw.len() < TRAILER_LEN {
        return Err(Error::corruption(format!(
            "internal key too short: {} bytes",
            raw.len()
        )));
    }
    let split = raw.len() - TRAILER_LEN;
    let mut trailer = [0u8; 8];
    trailer.copy_from_slice(&raw[split..split + 8]);
    Ok(DecodedKey {
        user_key: &raw[..split],
        seq: !u64::from_be_bytes(trailer),
        kind: KeyKind::from_u8(raw[raw.len() - 1])?,
    })
}

/// 查询下界:某 user_key 全部版本的 start(sentinel trailer 为全零字节,小于任何真实版本)。
pub fn lower_bound_key(user_key: &[u8]) -> InternalKey {
    let mut buf = Vec::with_capacity(user_key.len() + TRAILER_LEN);
    buf.extend_from_slice(user_key);
    buf.extend_from_slice(&[0u8; TRAILER_LEN]);
    InternalKey(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let raw = encode_internal_key(b"user1", 42, KeyKind::Put);
        let k = decode_internal_key(&raw.0).unwrap();
        assert_eq!(k.user_key, b"user1");
        assert_eq!(k.seq, 42);
        assert_eq!(k.kind, KeyKind::Put);
    }

    #[test]
    fn same_user_key_newer_versions_sort_first() {
        let s1 = encode_internal_key(b"k", 1, KeyKind::Put);
        let s2 = encode_internal_key(b"k", 2, KeyKind::Put);
        let s10 = encode_internal_key(b"k", 10, KeyKind::Delete);
        assert!(s10 < s2);
        assert!(s2 < s1);
    }

    #[test]
    fn user_key_order_is_lexicographic() {
        let ab = encode_internal_key(b"ab", 100, KeyKind::Put);
        let abc = encode_internal_key(b"abc", 1, KeyKind::Put);
        assert!(ab < abc, "user_key 'ab' 必须排在 'abc' 前,不受 seq 影响");
        let a = encode_internal_key(b"a", SEQ_MAX, KeyKind::Put);
        assert!(a < ab);
    }

    #[test]
    fn lower_bound_is_minimum_for_user_key() {
        let bound = lower_bound_key(b"k");
        for seq in [0u64, 1, 7, u64::MAX - 1] {
            for kind in [KeyKind::Put, KeyKind::Delete] {
                assert!(encode_internal_key(b"k", seq, kind) >= bound);
            }
        }
        // 且不能“串”到更大 user_key 的键上
        let ka = encode_internal_key(b"ka", 1, KeyKind::Put);
        assert!(ka > bound);
    }

    #[test]
    fn short_key_rejected() {
        assert!(decode_internal_key(&[0u8; 5]).is_err());
    }
}
