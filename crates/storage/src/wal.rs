//! WAL(Write-Ahead Log):段文件 + 帧格式。
//!
//! 帧布局:`[u32 LE payload_len][u32 LE crc32(payload)][payload]`,整帧追加。
//! payload = 一个写入批(varint count;每项 varint seq + varint klen + key
//! + 1 字节值标志 + 可选 varint vlen + value)。
//!
//! 崩溃恢复:按段序号顺序重放;遇到 EOF / 半帧 / CRC 不符即认为尾部损坏,
//! 截断点之后的数据丢弃并告警(与LevelDB语义一致:v1 不做跨块分帧,
//! 单帧最大 64MiB 保护,超出按损坏处理)。

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use common::error::{Error, Result};
use common::util::{crc32, get_varint_u64, put_varint_u64};
use tracing::warn;

pub const MAX_WAL_PAYLOAD: usize = 64 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    pub seq: u64,
    pub user_key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

pub struct WalOptions {
    pub dir: PathBuf,
    pub segment_bytes: u64,
    pub sync: bool,
}

pub fn encode_batch(entries: &[WalEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    put_varint_u64(entries.len() as u64, &mut buf);
    for e in entries {
        put_varint_u64(e.seq, &mut buf);
        put_varint_u64(e.user_key.len() as u64, &mut buf);
        buf.extend_from_slice(&e.user_key);
        match &e.value {
            Some(v) => {
                buf.push(1);
                put_varint_u64(v.len() as u64, &mut buf);
                buf.extend_from_slice(v);
            }
            None => buf.push(0),
        }
    }
    buf
}

pub fn decode_batch(mut buf: &[u8]) -> Result<Vec<WalEntry>> {
    let count = get_varint_u64(&mut buf)? as usize;
    let mut out = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let seq = get_varint_u64(&mut buf)?;
        let klen = get_varint_u64(&mut buf)? as usize;
        if buf.len() < klen {
            return Err(Error::corruption("truncated key in wal payload"));
        }
        let user_key = buf[..klen].to_vec();
        buf = &buf[klen..];
        let has_value = if buf.is_empty() {
            return Err(Error::corruption("truncated value flag in wal payload"));
        } else {
            let b = buf[0];
            buf = &buf[1..];
            b == 1
        };
        let value = if has_value {
            let vlen = get_varint_u64(&mut buf)? as usize;
            if buf.len() < vlen {
                return Err(Error::corruption("truncated value in wal payload"));
            }
            let v = buf[..vlen].to_vec();
            buf = &buf[vlen..];
            Some(v)
        } else {
            None
        };
        out.push(WalEntry {
            seq,
            user_key,
            value,
        });
    }
    if !buf.is_empty() {
        return Err(Error::corruption("trailing bytes in wal payload"));
    }
    Ok(out)
}

pub struct WalWriter {
    opts: WalOptions,
    writer: BufWriter<File>,
    segment: u64,
    written: u64,
}

fn segment_path(dir: &Path, segment: u64) -> PathBuf {
    dir.join(format!("{segment:020}.wal"))
}

impl WalWriter {
    pub fn open(opts: WalOptions) -> Result<Self> {
        std::fs::create_dir_all(&opts.dir)?;
        let segment = latest_segment(&opts.dir)?;
        let path = segment_path(&opts.dir, segment);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        let written = file.metadata()?.len();
        Ok(Self {
            writer: BufWriter::new(file),
            opts,
            segment,
            written,
        })
    }

    pub fn append(&mut self, entries: &[WalEntry]) -> Result<()> {
        let payload = encode_batch(entries);
        if payload.len() > MAX_WAL_PAYLOAD {
            return Err(Error::invalid_argument(format!(
                "wal payload too large: {} bytes",
                payload.len()
            )));
        }
        let mut frame = Vec::with_capacity(payload.len() + 8);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);
        self.writer.write_all(&frame)?;
        self.written += frame.len() as u64;
        self.writer.flush()?;
        if self.opts.sync {
            self.writer.get_ref().sync_data()?;
        }
        if self.written >= self.opts.segment_bytes {
            self.rotate()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.segment += 1;
        self.written = 0;
        let path = segment_path(&self.opts.dir, self.segment);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// 丢弃全部 WAL 数据(flush 已把 memtable 落 SST 时调用)。
    pub fn reset(&mut self) -> Result<()> {
        self.writer.flush()?;
        let dir = self.opts.dir.clone();
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "wal") {
                let _ = std::fs::remove_file(path);
            }
        }
        self.segment += 1;
        self.written = 0;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(segment_path(&dir, self.segment))?;
        self.writer = BufWriter::new(file);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        if self.opts.sync {
            self.writer.get_ref().sync_data()?;
        }
        Ok(())
    }
}

fn latest_segment(dir: &Path) -> Result<u64> {
    let mut max = 0u64;
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "wal") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(v) = stem.parse::<u64>() {
                    max = max.max(v);
                }
            }
        }
    }
    Ok(max)
}

/// 重放全部段文件,按顺序把每条记录交给 sink;返回见到的最大 seq。
/// 尾部半帧 / CRC 不符按损坏处理:停止该文件并继续后续段(正常崩溃场景即最后一段)。
pub fn replay(dir: &Path, mut sink: impl FnMut(WalEntry) -> Result<()>) -> Result<u64> {
    let mut max_seq = 0u64;
    if !dir.exists() {
        return Ok(0);
    }
    let mut segments: Vec<u64> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "wal") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                if let Ok(v) = stem.parse::<u64>() {
                    segments.push(v);
                }
            }
        }
    }
    segments.sort_unstable();

    for segment in segments {
        let path = segment_path(dir, segment);
        let mut file = match File::open(&path) {
            Ok(f) => BufReader::new(f),
            Err(_) => continue,
        };
        loop {
            let mut header = [0u8; 8];
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(_) => break, // EOF(含半帧头)
            }
            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
            let stored_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
            if len > MAX_WAL_PAYLOAD {
                warn!(
                    segment,
                    len, "wal record implausibly long, stop replay here"
                );
                break;
            }
            let mut payload = vec![0u8; len];
            if file.read_exact(&mut payload).is_err() {
                warn!(
                    segment,
                    len, "wal record truncated at tail, stop replay here"
                );
                break;
            }
            if crc32(&payload) != stored_crc {
                warn!(segment, "wal record crc mismatch, stop replay here");
                break;
            }
            for entry in decode_batch(&payload)? {
                max_seq = max_seq.max(entry.seq);
                sink(entry)?;
            }
        }
    }
    Ok(max_seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn opts(dir: &Path) -> WalOptions {
        WalOptions {
            dir: dir.to_path_buf(),
            segment_bytes: 1024,
            sync: false,
        }
    }

    #[test]
    fn batch_codec_roundtrip() {
        let entries = vec![
            WalEntry {
                seq: 1,
                user_key: b"a".to_vec(),
                value: Some(b"v".to_vec()),
            },
            WalEntry {
                seq: 2,
                user_key: b"b".to_vec(),
                value: None,
            },
            WalEntry {
                seq: 3,
                user_key: "中文".as_bytes().to_vec(),
                value: Some(Vec::new()),
            },
        ];
        let bytes = encode_batch(&entries);
        assert_eq!(decode_batch(&bytes).unwrap(), entries);
    }

    #[test]
    fn append_replay_and_rotation() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut w = WalWriter::open(opts(dir)).unwrap();
        // 写 100 条,段阈值 1KiB → 会触发 rotate
        for i in 0..100u64 {
            w.append(&[WalEntry {
                seq: i + 1,
                user_key: format!("key{i}").into_bytes(),
                value: Some(vec![b'v'; 40]),
            }])
            .unwrap();
        }
        let segments = fs::read_dir(dir).unwrap().count();
        assert!(segments > 1, "应当发生段轮转,实际 {segments} 段");

        let mut seen = 0;
        let max_seq = replay(dir, |_| {
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 100);
        assert_eq!(max_seq, 100);
    }

    #[test]
    fn torn_tail_is_truncated() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        {
            let mut w = WalWriter::open(opts(dir)).unwrap();
            w.append(&[WalEntry {
                seq: 1,
                user_key: b"a".to_vec(),
                value: Some(b"1".to_vec()),
            }])
            .unwrap();
            w.append(&[WalEntry {
                seq: 2,
                user_key: b"b".to_vec(),
                value: Some(b"2".to_vec()),
            }])
            .unwrap();
        }
        // 模拟崩溃:最后一帧被截断
        let path = segment_path(dir, 0);
        let meta = fs::metadata(&path).unwrap();
        let mut content = fs::read(&path).unwrap();
        content.truncate(meta.len() as usize - 2);
        fs::write(&path, &content).unwrap();

        let mut seen = Vec::new();
        let max_seq = replay(dir, |e| {
            seen.push(e.seq);
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![1], "只有第一帧完整");
        assert_eq!(max_seq, 1);
    }

    #[test]
    fn reset_drops_all_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut w = WalWriter::open(opts(dir)).unwrap();
        w.append(&[WalEntry {
            seq: 1,
            user_key: b"a".to_vec(),
            value: None,
        }])
        .unwrap();
        w.reset().unwrap();
        let mut seen = 0;
        let max_seq = replay(dir, |_| {
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 0);
        assert_eq!(max_seq, 0);
    }
}
