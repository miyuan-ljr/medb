//! 表统计信息:代价优化器的输入。
//!
//! v1 维护策略:
//! - `row_count` 随写路径精确增量(INSERT +n / DELETE -n / UPDATE 不变);
//! - `min`/`max` 只扩不缩(删除不收窄,始终保持合法上界);
//! - `ndv` 来自 ANALYZE 全量(或抽样)统计 —— 写路径不做增量(增量 NDV 无法
//!   准确,只会越数越错);`ndv == 0` 表示"该列从未统计过",规划期据此自动触发
//!   ANALYZE(小表同步全量,见 Engine 的自动统计);
//! - 事务提交后整体标记 `stale`,下次规划自动重算。
//!
//! 持久化:与 catalog 同域(保留表 0),键 = `[0;8] ++ [0x01] ++ 表名`
//! (表名不可能以 0x01 开头,天然隔离),值 = 本模块的手写编码。

use std::collections::BTreeMap;

use crate::schema::{Row, TableSchema};
use crate::types::{self, ScalarValue};

/// 统计键标记字节(catalog 键 = `[0;8] ++ 表名`,表名不含 0x01)。
pub const STATS_MARKER: u8 = 0x01;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableStats {
    pub row_count: u64,
    /// 数据变动后未重新 ANALYZE(NDV/分布过期,行数增量仍准确)
    pub stale: bool,
    /// 列名(小写)→ 统计;缺列 = 未统计(规划期用默认估算)
    pub columns: BTreeMap<String, ColumnStats>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnStats {
    /// 0 = 从未统计(自动 ANALYZE 的触发条件)
    pub ndv: u64,
    pub null_count: u64,
    /// 只扩不缩的取值边界(规划期做范围选择性插值)
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
}

/// 值是否小于边界(不可比较类型视为否,不参与边界扩张)。
fn is_before(v: &ScalarValue, bound: &ScalarValue) -> bool {
    types::compare_values(v, bound).is_ok_and(|o| o.is_lt())
}

/// 值是否大于边界。
fn is_after(v: &ScalarValue, bound: &ScalarValue) -> bool {
    types::compare_values(v, bound).is_ok_and(|o| o.is_gt())
}

impl TableStats {
    pub fn column(&self, name: &str) -> Option<&ColumnStats> {
        self.columns.get(&name.to_ascii_lowercase())
    }

    pub fn null_frac(&self, name: &str) -> f64 {
        if self.row_count == 0 {
            return 0.0;
        }
        self.column(name)
            .map_or(0.0, |c| c.null_count as f64 / self.row_count as f64)
    }

    /// 是否需要(重新)ANALYZE:过期,或有列从未统计过(空表无需列统计)。
    pub fn needs_analyze(&self) -> bool {
        self.stale
            || (self.row_count > 0
                && (self.columns.is_empty() || self.columns.values().any(|c| c.ndv == 0)))
    }

    /// INSERT 增量记账:行数、NULL 计数、min/max 扩张(NDV 不动)。
    pub fn record_insert(&mut self, schema: &TableSchema, rows: &[Row]) {
        self.row_count += rows.len() as u64;
        if rows.is_empty() {
            return;
        }
        for (i, col) in schema.columns.iter().enumerate() {
            let entry = self
                .columns
                .entry(col.name.to_ascii_lowercase())
                .or_default();
            for row in rows {
                match &row[i] {
                    ScalarValue::Null => entry.null_count += 1,
                    v => {
                        if entry.min.as_ref().map_or(true, |m| is_before(v, m)) {
                            entry.min = Some(v.clone());
                        }
                        if entry.max.as_ref().map_or(true, |m| is_after(v, m)) {
                            entry.max = Some(v.clone());
                        }
                    }
                }
            }
        }
    }

    /// DELETE 增量记账:行数下调;分布信息标记过期(min/max/NDV 只会高估,
    /// 用作估算仍安全,但精确度下降 → stale 让规划期择机重算)。
    pub fn record_delete(&mut self, n: u64) {
        self.row_count = self.row_count.saturating_sub(n);
        self.stale = true;
    }

    /// UPDATE 增量记账:行数不变;被赋值列的 min/max 扩张(新值可能越界)。
    pub fn widen_column(&mut self, col: &str, vals: impl Iterator<Item = ScalarValue>) {
        let entry = self.columns.entry(col.to_ascii_lowercase()).or_default();
        for v in vals {
            match v {
                ScalarValue::Null => {}
                v => {
                    if entry.min.as_ref().map_or(true, |m| is_before(&v, m)) {
                        entry.min = Some(v.clone());
                    }
                    if entry.max.as_ref().map_or(true, |m| is_after(&v, m)) {
                        entry.max = Some(v.clone());
                    }
                }
            }
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(1u8); // version
        buf.extend_from_slice(&self.row_count.to_be_bytes());
        buf.push(u8::from(self.stale));
        buf.extend_from_slice(&(self.columns.len() as u32).to_be_bytes());
        for (name, c) in &self.columns {
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&c.ndv.to_be_bytes());
            buf.extend_from_slice(&c.null_count.to_be_bytes());
            encode_optional_scalar(&mut buf, &c.min);
            encode_optional_scalar(&mut buf, &c.max);
        }
        buf
    }

    pub fn decode(bytes: &[u8]) -> Option<TableStats> {
        let mut s = bytes;
        if s.first() != Some(&1u8) || s.len() < 10 {
            return None;
        }
        s = &s[1..];
        let row_count = u64::from_be_bytes(s[..8].try_into().ok()?);
        s = &s[8..];
        let stale = take_byte(&mut s)? == 1;
        let n = u32::from_be_bytes(s[..4].try_into().ok()?) as usize;
        s = &s[4..];
        let mut columns = BTreeMap::new();
        for _ in 0..n {
            let name_len = u32::from_be_bytes(s[..4].try_into().ok()?) as usize;
            s = &s[4..];
            if s.len() < name_len {
                return None;
            }
            let name = String::from_utf8(s[..name_len].to_vec()).ok()?;
            s = &s[name_len..];
            let ndv = u64::from_be_bytes(s[..8].try_into().ok()?);
            s = &s[8..];
            let null_count = u64::from_be_bytes(s[..8].try_into().ok()?);
            s = &s[8..];
            let min = decode_optional_scalar(&mut s)?;
            let max = decode_optional_scalar(&mut s)?;
            columns.insert(
                name,
                ColumnStats {
                    ndv,
                    null_count,
                    min,
                    max,
                },
            );
        }
        Some(TableStats {
            row_count,
            stale,
            columns,
        })
    }
}

fn encode_optional_scalar(buf: &mut Vec<u8>, v: &Option<ScalarValue>) {
    match v {
        Some(v) => {
            buf.push(1);
            crate::codec::encode_value(v, buf);
        }
        None => buf.push(0),
    }
}

fn decode_optional_scalar(s: &mut &[u8]) -> Option<Option<ScalarValue>> {
    match take_byte(s)? {
        0 => Some(None),
        _ => crate::codec::decode_value(s).ok().map(Some),
    }
}

fn take_byte(buf: &mut &[u8]) -> Option<u8> {
    let (&b, rest) = buf.split_first()?;
    *buf = rest;
    Some(b)
}

impl std::fmt::Display for TableStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "rows={}", self.row_count)?;
        if self.stale {
            write!(f, " (stale)")?;
        }
        for (name, c) in &self.columns {
            write!(f, ", {name}: ndv={}", c.ndv)?;
            if let (Some(min), Some(max)) = (&c.min, &c.max) {
                write!(f, " range=[{min}, {max}]")?;
            }
        }
        Ok(())
    }
}

/// 全量(或抽样)扫描建统计。抽样:行数超过 `sample_target` 时按等步长抽样,
/// NDV/NULL 计数为估计;min/max 仍取样本内边界(上界意义不变)。
pub fn compute_from_rows(schema: &TableSchema, rows: &[Row], sample_target: usize) -> TableStats {
    let stride = if rows.len() > sample_target && sample_target > 0 {
        rows.len() / sample_target + 1
    } else {
        1
    };
    let mut stats = TableStats {
        row_count: rows.len() as u64,
        stale: false,
        columns: BTreeMap::new(),
    };
    for (i, col) in schema.columns.iter().enumerate() {
        let entry = stats
            .columns
            .entry(col.name.to_ascii_lowercase())
            .or_default();
        let mut seen = std::collections::HashSet::new();
        for row in rows.iter().step_by(stride) {
            match &row[i] {
                ScalarValue::Null => entry.null_count += 1,
                v => {
                    seen.insert(v.clone());
                    if entry.min.as_ref().map_or(true, |m| is_before(v, m)) {
                        entry.min = Some(v.clone());
                    }
                    if entry.max.as_ref().map_or(true, |m| is_after(v, m)) {
                        entry.max = Some(v.clone());
                    }
                }
            }
        }
        // 抽样时 NDV 为样本规模(下界估计;min/max 仍是样本边界)
        entry.ndv = seen.len() as u64;
    }
    stats
}

/// 标量值 → f64 / 比较边界等公共能力在 `common::types`(compare_values /
/// scalar_to_f64);本模块只负责统计结构与生命周期。

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnDesc;
    use crate::types::DataType;

    fn schema() -> TableSchema {
        TableSchema {
            id: 1,
            name: "t".into(),
            columns: vec![
                ColumnDesc::new("id".to_string(), DataType::Int64, true),
                ColumnDesc::new("name".to_string(), DataType::Utf8, true),
            ],
            primary_key: vec![],
            version: 1,
            indexes: Vec::new(),
        }
    }

    fn row(id: i64, name: &str) -> Row {
        vec![ScalarValue::Int64(id), ScalarValue::Utf8(name.into())]
    }

    #[test]
    fn record_insert_tracks_count_nulls_and_bounds() {
        let s = schema();
        let mut st = TableStats::default();
        st.record_insert(
            &s,
            &[
                row(5, "e"),
                row(1, "a"),
                row(9, "zz"),
                vec![ScalarValue::Null, ScalarValue::Null],
            ],
        );
        assert_eq!(st.row_count, 4);
        let id = st.column("id").unwrap();
        assert_eq!(id.null_count, 1);
        assert_eq!(id.min, Some(ScalarValue::Int64(1)));
        assert_eq!(id.max, Some(ScalarValue::Int64(9)));
        let name = st.column("NAME").unwrap(); // 大小写不敏感
        assert_eq!(name.min, Some(ScalarValue::Utf8("a".into())));
        assert_eq!(name.max, Some(ScalarValue::Utf8("zz".into())));
        // 只扩不缩:插入更小值不覆盖边界(保持已见范围)
        st.record_insert(&s, &[row(-3, "b")]);
        assert_eq!(st.row_count, 5);
        assert_eq!(st.column("id").unwrap().min, Some(ScalarValue::Int64(-3)));
    }

    #[test]
    fn record_delete_counts_and_marks_stale() {
        let s = schema();
        let mut st = TableStats::default();
        st.record_insert(&s, &[row(1, "a"), row(2, "b")]);
        assert!(!st.needs_analyze() || st.columns.values().all(|c| c.ndv == 0));
        st.record_delete(1);
        assert_eq!(st.row_count, 1);
        assert!(st.stale);
    }

    #[test]
    fn codec_roundtrip() {
        let s = schema();
        let mut st = TableStats::default();
        st.record_insert(&s, &[row(1, "a"), row(2, "b")]);
        st.columns.get_mut("id").unwrap().ndv = 2;
        let bytes = st.encode();
        let back = TableStats::decode(&bytes).unwrap();
        assert_eq!(st, back);
        // 损坏输入
        assert!(TableStats::decode(&bytes[..3]).is_none());
        assert!(TableStats::decode(&[9, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_none());
    }

    #[test]
    fn compute_from_rows_basic_and_sample() {
        let s = schema();
        let rows: Vec<Row> = (0..1000).map(|i| row(i, &format!("n{i}"))).collect();
        let st = compute_from_rows(&s, &rows, usize::MAX);
        assert_eq!(st.row_count, 1000);
        assert_eq!(st.column("id").unwrap().ndv, 1000);
        assert_eq!(st.column("id").unwrap().min, Some(ScalarValue::Int64(0)));
        assert_eq!(st.column("id").unwrap().max, Some(ScalarValue::Int64(999)));
        // 抽样:行数与 NDV 为样本规模
        let st2 = compute_from_rows(&s, &rows, 100);
        assert!(st2.row_count == 1000); // 总行数仍精确
        assert!(st2.column("id").unwrap().ndv <= 100);
    }
}
