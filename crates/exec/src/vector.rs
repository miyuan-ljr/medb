//! 向量化列数据:数据列 + 可选 null 位图。
//!
//! 数值列用紧凑定长数组(向量化内核直接在 `&[i64]` 等切片上跑紧密循环);
//! 变长列用 `data + offsets`(类 Arrow 变长布局);null 用位图表示,
//! 无 null 的列不携带位图,零开销。

use common::error::{Error, Result};
use common::types::{DataType, ScalarValue};

/// null 位图:bit=1 表示该行有效。尾部落模的位无意义,访问由 len 把关。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Validity {
    len: usize,
    words: Vec<u64>,
}

impl Validity {
    pub fn all_valid(len: usize) -> Self {
        let words = (0..words_for(len))
            .map(|i| {
                let base = i * 64;
                let bits = (len - base).min(64);
                if bits == 64 {
                    u64::MAX
                } else {
                    (1u64 << bits) - 1
                }
            })
            .collect();
        Self { len, words }
    }

    pub fn all_null(len: usize) -> Self {
        Self {
            len,
            words: vec![0; words_for(len)],
        }
    }

    /// 从 Option 序列构建;全有效时返回 None(省掉位图)。
    pub fn from_opts(opts: &[Option<bool>]) -> Option<Validity> {
        if opts.iter().all(|o| o.is_some()) {
            return None;
        }
        let mut v = Self::all_valid(opts.len());
        for (i, o) in opts.iter().enumerate() {
            if o.is_none() {
                v.unset(i);
            }
        }
        Some(v)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn get(&self, i: usize) -> bool {
        debug_assert!(i < self.len);
        (self.words[i / 64] >> (i % 64)) & 1 != 0
    }

    #[inline]
    pub fn set(&mut self, i: usize, valid: bool) {
        debug_assert!(i < self.len);
        if valid {
            self.words[i / 64] |= 1 << (i % 64);
        } else {
            self.words[i / 64] &= !(1 << (i % 64));
        }
    }

    #[inline]
    pub fn unset(&mut self, i: usize) {
        self.set(i, false);
    }
}

#[inline]
fn words_for(len: usize) -> usize {
    len.div_ceil(64).max(1)
}

/// 列的实际数据(不含 null 位图)。
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    Bool(Vec<bool>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    /// 日期:距 1970-01-01 的天数。
    Date(Vec<i32>),
    /// offsets.len() == len + 1,data ≤ 4GiB(u32 偏移)。
    Utf8 {
        data: Vec<u8>,
        offsets: Vec<u32>,
    },
    /// 二进制 LOB,每行一个字节串。
    Binary(Vec<Vec<u8>>),
    /// 一维数组:elem 为元素类型(自描述),values 每行一个列表(null 行为空占位)。
    List {
        elem: Box<DataType>,
        values: Vec<Vec<ScalarValue>>,
    },
    /// 定长浮点向量:dim 自描述,null 行为空占位。
    Vector {
        dim: usize,
        values: Vec<Vec<f32>>,
    },
}

impl ColumnData {
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnData::Bool(_) => DataType::Boolean,
            ColumnData::Int64(_) => DataType::Int64,
            ColumnData::Float64(_) => DataType::Float64,
            ColumnData::Date(_) => DataType::Date,
            ColumnData::Utf8 { .. } => DataType::Utf8,
            ColumnData::Binary(_) => DataType::Binary,
            ColumnData::List { elem, .. } => DataType::Array(elem.clone()),
            ColumnData::Vector { dim, .. } => DataType::Vector(*dim),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnData::Bool(v) => v.len(),
            ColumnData::Int64(v) => v.len(),
            ColumnData::Float64(v) => v.len(),
            ColumnData::Date(v) => v.len(),
            ColumnData::Utf8 { offsets, .. } => offsets.len().saturating_sub(1),
            ColumnData::Binary(v) => v.len(),
            ColumnData::List { values, .. } => values.len(),
            ColumnData::Vector { values, .. } => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 内存占用近似值(记账用)。
    pub fn approx_size(&self) -> usize {
        match self {
            ColumnData::Bool(v) => v.len(),
            ColumnData::Int64(v) => v.len() * 8,
            ColumnData::Float64(v) => v.len() * 8,
            ColumnData::Date(v) => v.len() * 4,
            ColumnData::Utf8 { data, offsets } => data.len() + offsets.len() * 4,
            ColumnData::Binary(v) => v.iter().map(|b| b.len() + 8).sum(),
            ColumnData::List { values, .. } => values
                .iter()
                .map(|r| 8 + r.len() * std::mem::size_of::<ScalarValue>())
                .sum(),
            ColumnData::Vector { values, .. } => values.iter().map(|v| v.len() * 4 + 8).sum(),
        }
    }
}

/// 列向量 = 数据 + 可选 null 位图。
#[derive(Debug, Clone, PartialEq)]
pub struct Vector {
    data: ColumnData,
    nulls: Option<Validity>,
}

/// 行内取值(借用视图,Utf8 返回 &str 零拷贝)。
#[derive(Debug, Clone, PartialEq)]
pub enum Datum<'a> {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Date(i32),
    Utf8(&'a str),
    Binary(&'a [u8]),
    List(&'a [ScalarValue]),
    Vector(&'a [f32]),
}

impl Datum<'_> {
    pub fn to_scalar(&self) -> ScalarValue {
        match self {
            Datum::Null => ScalarValue::Null,
            Datum::Bool(b) => ScalarValue::Boolean(*b),
            Datum::Int64(v) => ScalarValue::Int64(*v),
            Datum::Float64(v) => ScalarValue::Float64(*v),
            Datum::Date(d) => ScalarValue::Date(*d),
            Datum::Utf8(s) => ScalarValue::Utf8(s.to_string()),
            Datum::Binary(b) => ScalarValue::Binary(b.to_vec()),
            Datum::List(items) => ScalarValue::List(items.to_vec()),
            Datum::Vector(vs) => ScalarValue::Vector(vs.to_vec()),
        }
    }

    pub fn to_text(&self) -> Option<String> {
        self.to_scalar().to_text()
    }
}

impl Vector {
    /// 构造并校验长度一致性。
    pub fn new(data: ColumnData, nulls: Option<Validity>) -> Result<Self> {
        let len = data.len();
        if let Some(n) = &nulls {
            if n.len() != len {
                return Err(Error::invalid_argument(format!(
                    "vector validity length {} != data length {len}",
                    n.len()
                )));
            }
        }
        if let ColumnData::Utf8 { data, offsets } = &data {
            if offsets.first() != Some(&0)
                || offsets.last() != Some(&(data.len() as u32))
                || offsets.windows(2).any(|w| w[0] > w[1])
            {
                return Err(Error::invalid_argument("utf8 vector offsets malformed"));
            }
        }
        Ok(Self { data, nulls })
    }

    // ---- 构造器(无 null 快捷方式)----

    pub fn bools(values: Vec<bool>) -> Self {
        Self {
            data: ColumnData::Bool(values),
            nulls: None,
        }
    }

    pub fn int64s(values: Vec<i64>) -> Self {
        Self {
            data: ColumnData::Int64(values),
            nulls: None,
        }
    }

    pub fn float64s(values: Vec<f64>) -> Self {
        Self {
            data: ColumnData::Float64(values),
            nulls: None,
        }
    }

    pub fn utf8s(values: Vec<String>) -> Self {
        let mut data = Vec::new();
        let mut offsets = Vec::with_capacity(values.len() + 1);
        offsets.push(0u32);
        for s in &values {
            data.extend_from_slice(s.as_bytes());
            offsets.push(data.len() as u32);
        }
        Self {
            data: ColumnData::Utf8 { data, offsets },
            nulls: None,
        }
    }

    /// 全 null 列。
    pub fn all_null(ty: DataType, len: usize) -> Self {
        let data = match ty {
            DataType::Boolean => ColumnData::Bool(vec![false; len]),
            DataType::Int64 => ColumnData::Int64(vec![0; len]),
            DataType::Float64 => ColumnData::Float64(vec![0.0; len]),
            DataType::Date => ColumnData::Date(vec![0; len]),
            DataType::Utf8 => ColumnData::Utf8 {
                data: Vec::new(),
                offsets: vec![0; len + 1],
            },
            DataType::Binary => ColumnData::Binary(vec![Vec::new(); len]),
            DataType::Array(elem) => ColumnData::List {
                elem,
                values: vec![Vec::new(); len],
            },
            DataType::Vector(dim) => ColumnData::Vector {
                dim,
                values: vec![Vec::new(); len],
            },
        };
        Self {
            data,
            nulls: Some(Validity::all_null(len)),
        }
    }

    /// 按目标类型从标量序列构建(执行器从行式数据走向列式的入口)。
    pub fn from_scalars(
        ty: DataType,
        values: impl IntoIterator<Item = ScalarValue>,
    ) -> Result<Self> {
        let values: Vec<ScalarValue> = values.into_iter().collect();
        let has_null = values.iter().any(|v| v.is_null());
        let mut nulls = has_null.then(|| {
            let mut v = Validity::all_valid(values.len());
            for (i, val) in values.iter().enumerate() {
                if val.is_null() {
                    v.unset(i);
                }
            }
            v
        });
        if values.is_empty() {
            nulls = None;
        }
        let data = match ty {
            DataType::Boolean => ColumnData::Bool(
                values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Boolean(b) => *b,
                        ScalarValue::Null => false,
                        other => panic!("expected boolean value, got {other:?}"),
                    })
                    .collect(),
            ),
            DataType::Int64 => ColumnData::Int64(
                values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Int64(i) => *i,
                        ScalarValue::Null => 0,
                        other => panic!("expected int64 value, got {other:?}"),
                    })
                    .collect(),
            ),
            DataType::Float64 => ColumnData::Float64(
                values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Float64(f) => *f,
                        ScalarValue::Null => 0.0,
                        other => panic!("expected float64 value, got {other:?}"),
                    })
                    .collect(),
            ),
            DataType::Date => ColumnData::Date(
                values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Date(d) => *d,
                        ScalarValue::Null => 0,
                        other => panic!("expected date value, got {other:?}"),
                    })
                    .collect(),
            ),
            DataType::Utf8 => ColumnData::Utf8 {
                data: {
                    let mut d = Vec::new();
                    for v in &values {
                        if let ScalarValue::Utf8(s) = v {
                            d.extend_from_slice(s.as_bytes());
                        }
                    }
                    d
                },
                offsets: {
                    let mut o = Vec::with_capacity(values.len() + 1);
                    o.push(0u32);
                    for v in &values {
                        match v {
                            ScalarValue::Utf8(s) => {
                                let last = o.last().copied().unwrap_or(0);
                                o.push(last + s.len() as u32);
                            }
                            ScalarValue::Null => {
                                let last = o.last().copied().unwrap_or(0);
                                o.push(last);
                            }
                            other => panic!("expected utf8 value, got {other:?}"),
                        }
                    }
                    o
                },
            },
            DataType::Binary => ColumnData::Binary(
                values
                    .iter()
                    .map(|v| match v {
                        ScalarValue::Binary(b) => b.clone(),
                        ScalarValue::Null => Vec::new(),
                        other => panic!("expected binary value, got {other:?}"),
                    })
                    .collect(),
            ),
            DataType::Array(elem) => {
                for v in &values {
                    match v {
                        ScalarValue::List(items) => {
                            // 元素类型一致性检查(标量元素)
                            if elem.is_scalar() {
                                for item in items {
                                    if let Some(t) = item.data_type() {
                                        if t != *elem && !item.is_null() {
                                            panic!("expected {:?} array element, got {t:?}", *elem);
                                        }
                                    }
                                }
                            }
                        }
                        ScalarValue::Null => {}
                        other => panic!("expected list value, got {other:?}"),
                    }
                }
                ColumnData::List {
                    elem: Box::new((*elem).clone()),
                    values: values
                        .iter()
                        .map(|v| match v {
                            ScalarValue::List(items) => items.clone(),
                            ScalarValue::Null => Vec::new(),
                            other => panic!("expected list value, got {other:?}"),
                        })
                        .collect(),
                }
            }
            DataType::Vector(dim) => {
                for v in &values {
                    match v {
                        ScalarValue::Vector(vs) => {
                            if vs.len() != dim {
                                panic!("expected vector dim {dim}, got {}", vs.len());
                            }
                        }
                        ScalarValue::Null => {}
                        other => panic!("expected vector value, got {other:?}"),
                    }
                }
                ColumnData::Vector {
                    dim,
                    values: values
                        .iter()
                        .map(|v| match v {
                            ScalarValue::Vector(vs) => vs.clone(),
                            ScalarValue::Null => Vec::new(),
                            other => panic!("expected vector value, got {other:?}"),
                        })
                        .collect(),
                }
            }
        };
        Self::new(data, nulls)
    }

    // ---- 元信息 ----

    pub fn data_type(&self) -> DataType {
        self.data.data_type()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn has_nulls(&self) -> bool {
        self.nulls.is_some()
    }

    #[inline]
    pub fn null_at(&self, i: usize) -> bool {
        self.nulls.as_ref().is_some_and(|n| !n.get(i))
    }

    pub fn approx_size(&self) -> usize {
        self.data.approx_size() + self.nulls.as_ref().map_or(0, |n| n.words.len() * 8)
    }

    // ---- 取值 ----

    pub fn datum(&self, i: usize) -> Datum<'_> {
        if self.null_at(i) {
            return Datum::Null;
        }
        match &self.data {
            ColumnData::Bool(v) => Datum::Bool(v[i]),
            ColumnData::Int64(v) => Datum::Int64(v[i]),
            ColumnData::Float64(v) => Datum::Float64(v[i]),
            ColumnData::Date(v) => Datum::Date(v[i]),
            ColumnData::Utf8 { data, offsets } => {
                let start = offsets[i] as usize;
                let end = offsets[i + 1] as usize;
                // null 位图缺失时可能有零长空洞(由构造器保证 offsets 单调)
                Datum::Utf8(std::str::from_utf8(&data[start..end]).unwrap_or(""))
            }
            ColumnData::Binary(v) => Datum::Binary(&v[i]),
            ColumnData::List { values, .. } => Datum::List(&values[i]),
            ColumnData::Vector { values, .. } => Datum::Vector(&values[i]),
        }
    }

    pub fn scalar_at(&self, i: usize) -> ScalarValue {
        self.datum(i).to_scalar()
    }

    // ---- 类型化快速通道(内核专用,类型不符 panic —— 执行期内部约定)----

    pub fn bool_values(&self) -> &[bool] {
        match &self.data {
            ColumnData::Bool(v) => v,
            _ => panic!("bool_values on non-bool vector"),
        }
    }

    pub fn int64_values(&self) -> &[i64] {
        match &self.data {
            ColumnData::Int64(v) => v,
            _ => panic!("int64_values on non-int64 vector"),
        }
    }

    pub fn float64_values(&self) -> &[f64] {
        match &self.data {
            ColumnData::Float64(v) => v,
            _ => panic!("float64_values on non-float64 vector"),
        }
    }

    /// 日期列的原始天数(i32,1970-01-01 起);非 Date 列 panic。
    pub fn date_values(&self) -> &[i32] {
        match &self.data {
            ColumnData::Date(v) => v,
            _ => panic!("date_values on non-date vector"),
        }
    }

    pub fn utf8_value(&self, i: usize) -> &str {
        match self.datum(i) {
            Datum::Utf8(s) => s,
            Datum::Null => "",
            _ => panic!("utf8_value on non-utf8 vector"),
        }
    }

    // ---- 变换 ----

    /// 按选择位图压缩(向量化 filter 的核心动作)。
    pub fn select(&self, selection: &[bool]) -> Result<Vector> {
        if selection.len() != self.len() {
            return Err(Error::invalid_argument(format!(
                "selection length {} != vector length {}",
                selection.len(),
                self.len()
            )));
        }
        let n: usize = selection.iter().filter(|s| **s).count();
        let has_null = self.nulls.is_some();
        let mut nulls = has_null.then(|| Validity::all_valid(n));
        let data = match &self.data {
            ColumnData::Bool(v) => ColumnData::Bool(
                v.iter()
                    .zip(selection)
                    .filter(|(_, s)| **s)
                    .map(|(x, _)| *x)
                    .collect(),
            ),
            ColumnData::Int64(v) => ColumnData::Int64(
                v.iter()
                    .zip(selection)
                    .filter(|(_, s)| **s)
                    .map(|(x, _)| *x)
                    .collect(),
            ),
            ColumnData::Float64(v) => ColumnData::Float64(
                v.iter()
                    .zip(selection)
                    .filter(|(_, s)| **s)
                    .map(|(x, _)| *x)
                    .collect(),
            ),
            ColumnData::Date(v) => ColumnData::Date(
                v.iter()
                    .zip(selection)
                    .filter(|(_, s)| **s)
                    .map(|(x, _)| *x)
                    .collect(),
            ),
            ColumnData::Utf8 { data: d, offsets } => {
                let mut out_data = Vec::with_capacity(d.len());
                let mut out_offsets = Vec::with_capacity(n + 1);
                out_offsets.push(0u32);
                let mut j = 0usize;
                for (i, keep) in selection.iter().enumerate() {
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    if *keep {
                        out_data.extend_from_slice(&d[start..end]);
                        out_offsets.push(out_data.len() as u32);
                        if let Some(nulls) = nulls.as_mut() {
                            nulls.set(j, self.null_at(i));
                        }
                        j += 1;
                    }
                }
                ColumnData::Utf8 {
                    data: out_data,
                    offsets: out_offsets,
                }
            }
            ColumnData::Binary(v) => ColumnData::Binary(
                v.iter()
                    .zip(selection)
                    .filter(|(_, s)| **s)
                    .map(|(x, _)| x.clone())
                    .collect(),
            ),
            ColumnData::List { elem, values } => ColumnData::List {
                elem: elem.clone(),
                values: values
                    .iter()
                    .zip(selection)
                    .filter(|(_, s)| **s)
                    .map(|(x, _)| x.clone())
                    .collect(),
            },
            ColumnData::Vector { dim, values } => ColumnData::Vector {
                dim: *dim,
                values: values
                    .iter()
                    .zip(selection)
                    .filter(|(_, s)| **s)
                    .map(|(x, _)| x.clone())
                    .collect(),
            },
        };
        // 非 Utf8 列的 null 位图按行同步压缩
        if let (Some(src), Some(dst)) = (self.nulls.as_ref(), nulls.as_mut()) {
            let mut j = 0usize;
            for (i, keep) in selection.iter().enumerate() {
                if *keep {
                    dst.set(j, src.get(i));
                    j += 1;
                }
            }
        }
        if n == 0 {
            nulls = None;
        }
        Self::new(data, nulls)
    }

    /// 截取前 n 行(limit 算子用)。
    pub fn head(&self, n: usize) -> Vector {
        self.slice(0, n)
    }

    /// 按行下标集合抽取(gather):数值列纯索引拷贝,Utf8 单缓冲重建
    /// (无逐值 String 分配),null 位图同步抽取。输出所有权必需的拷贝。
    pub fn gather(&self, idx: &[usize]) -> Vector {
        let data = match &self.data {
            ColumnData::Bool(v) => ColumnData::Bool(idx.iter().map(|&i| v[i]).collect()),
            ColumnData::Int64(v) => ColumnData::Int64(idx.iter().map(|&i| v[i]).collect()),
            ColumnData::Float64(v) => ColumnData::Float64(idx.iter().map(|&i| v[i]).collect()),
            ColumnData::Date(v) => ColumnData::Date(idx.iter().map(|&i| v[i]).collect()),
            ColumnData::Utf8 { data, offsets } => {
                let total: usize = idx
                    .iter()
                    .map(|&i| (offsets[i + 1] - offsets[i]) as usize)
                    .sum();
                let mut buf = Vec::with_capacity(total);
                let mut offs = Vec::with_capacity(idx.len() + 1);
                offs.push(0u32);
                for &i in idx {
                    let (s, e) = (offsets[i] as usize, offsets[i + 1] as usize);
                    buf.extend_from_slice(&data[s..e]);
                    offs.push(buf.len() as u32);
                }
                ColumnData::Utf8 { data: buf, offsets: offs }
            }
            ColumnData::Binary(v) => ColumnData::Binary(idx.iter().map(|&i| v[i].clone()).collect()),
            ColumnData::List { elem, values } => ColumnData::List {
                elem: elem.clone(),
                values: idx.iter().map(|&i| values[i].clone()).collect(),
            },
            ColumnData::Vector { dim, values } => ColumnData::Vector {
                dim: *dim,
                values: idx.iter().map(|&i| values[i].clone()).collect(),
            },
        };
        let nulls = self.nulls.as_ref().map(|v| {
            let mut out = Validity::all_valid(idx.len());
            for (i, &src) in idx.iter().enumerate() {
                out.set(i, v.get(src));
            }
            out
        });
        Self { data, nulls }
    }

    /// 纵向拼接(同类型列;null 位图合并)。分片产出的合并原语。
    pub fn concat(parts: &[Vector]) -> Result<Vector> {
        let Some(first) = parts.first() else {
            return Err(Error::invalid_argument("concat requires >=1 part"));
        };
        let ty = first.data_type();
        for p in parts {
            if p.data_type() != ty {
                return Err(Error::invalid_argument(format!(
                    "concat type mismatch: {ty:?} vs {:?}",
                    p.data_type()
                )));
            }
        }
        let total: usize = parts.iter().map(|p| p.len()).sum();
        let data = match &first.data {
            ColumnData::Bool(_) => ColumnData::Bool(
                parts.iter().flat_map(|p| p.bool_values().to_vec()).collect(),
            ),
            ColumnData::Int64(_) => ColumnData::Int64(
                parts.iter().flat_map(|p| p.int64_values().to_vec()).collect(),
            ),
            ColumnData::Float64(_) => ColumnData::Float64(
                parts.iter().flat_map(|p| p.float64_values().to_vec()).collect(),
            ),
            ColumnData::Date(_) => {
                // Date 无数值快速通道 → 借 datum 逐行取(与 Bool 同理,量小)
                let mut out = Vec::with_capacity(total);
                for p in parts {
                    for i in 0..p.len() {
                        match p.datum(i) {
                            Datum::Date(d) => out.push(d),
                            Datum::Null => out.push(0),
                            _ => panic!("concat type mismatch on date column"),
                        }
                    }
                }
                ColumnData::Date(out)
            }
            ColumnData::Utf8 { .. } => {
                let mut buf = Vec::new();
                let mut offs = Vec::with_capacity(total + 1);
                offs.push(0u32);
                for p in parts {
                    if let ColumnData::Utf8 { data, offsets } = &p.data {
                        for w in &offsets[1..] {
                            offs.push(buf.len() as u32 + *w);
                        }
                        buf.extend_from_slice(data);
                    } else {
                        unreachable!("concat utf8 part mismatch");
                    }
                }
                ColumnData::Utf8 { data: buf, offsets: offs }
            }
            ColumnData::Binary(_) => ColumnData::Binary(
                parts
                    .iter()
                    .flat_map(|p| match &p.data {
                        ColumnData::Binary(v) => v.clone(),
                        _ => unreachable!(),
                    })
                    .collect(),
            ),
            ColumnData::List { elem, .. } => ColumnData::List {
                elem: elem.clone(),
                values: parts
                    .iter()
                    .flat_map(|p| match &p.data {
                        ColumnData::List { values, .. } => values.clone(),
                        _ => unreachable!(),
                    })
                    .collect(),
            },
            ColumnData::Vector { dim, .. } => ColumnData::Vector {
                dim: *dim,
                values: parts
                    .iter()
                    .flat_map(|p| match &p.data {
                        ColumnData::Vector { values, .. } => values.clone(),
                        _ => unreachable!(),
                    })
                    .collect(),
            },
        };
        // null 位图合并(任一分部带位图才需要)
        let any_nulls = parts.iter().any(|p| p.nulls.is_some());
        let nulls = any_nulls.then(|| {
            let mut out = Validity::all_valid(total);
            let mut j = 0usize;
            for p in parts {
                for i in 0..p.len() {
                    out.set(j, p.null_at(i) == false);
                    j += 1;
                }
            }
            out
        });
        Ok(Self { data, nulls })
    }

    /// 截取 [offset, offset + len) 行窗口。
    /// **只拷贝窗口内数据**(交付给下游的所有权必需),源向量保持不变 ——
    /// 批切片零冗余拷贝的关键原语:剩余行复用同一份缓冲,不重压。
    pub fn slice(&self, offset: usize, len: usize) -> Vector {
        let len = len.min(self.len().saturating_sub(offset));
        let end = offset + len;
        let data = match &self.data {
            ColumnData::Bool(v) => ColumnData::Bool(v[offset..end].to_vec()),
            ColumnData::Int64(v) => ColumnData::Int64(v[offset..end].to_vec()),
            ColumnData::Float64(v) => ColumnData::Float64(v[offset..end].to_vec()),
            ColumnData::Date(v) => ColumnData::Date(v[offset..end].to_vec()),
            ColumnData::Utf8 { data, offsets } => ColumnData::Utf8 {
                data: data[offsets[offset] as usize..offsets[end] as usize].to_vec(),
                offsets: offsets[offset..=end]
                    .iter()
                    .map(|o| o - offsets[offset])
                    .collect(),
            },
            ColumnData::Binary(v) => ColumnData::Binary(v[offset..end].to_vec()),
            ColumnData::List { elem, values } => ColumnData::List {
                elem: elem.clone(),
                values: values[offset..end].to_vec(),
            },
            ColumnData::Vector { dim, values } => ColumnData::Vector {
                dim: *dim,
                values: values[offset..end].to_vec(),
            },
        };
        let nulls = self.nulls.as_ref().map(|v| {
            let mut out = Validity::all_valid(len);
            for i in 0..len {
                out.set(i, v.get(offset + i));
            }
            out
        });
        Self { data, nulls }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validity_bitmap_basics() {
        let mut v = Validity::all_valid(130);
        assert!(v.get(0) && v.get(64) && v.get(129));
        v.unset(129);
        assert!(!v.get(129));
        v.set(1, false);
        assert!(!v.get(1));
        assert_eq!(v.len(), 130);
    }

    #[test]
    fn from_opts_all_valid_has_no_bitmap() {
        let opts = vec![Some(true), Some(false)];
        assert!(Validity::from_opts(&opts).is_none());
        let opts = vec![Some(true), None];
        let v = Validity::from_opts(&opts).unwrap();
        assert!(v.get(0));
        assert!(!v.get(1));
    }

    #[test]
    fn from_scalars_with_nulls() {
        let v = Vector::from_scalars(
            DataType::Int64,
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Null,
                ScalarValue::Int64(3),
            ],
        )
        .unwrap();
        assert!(v.has_nulls());
        assert_eq!(v.int64_values(), &[1, 0, 3]);
        assert!(v.null_at(1));
        assert_eq!(v.scalar_at(2), ScalarValue::Int64(3));
    }

    #[test]
    fn utf8_offsets_and_datum() {
        let v = Vector::utf8s(vec![
            "alpha".to_string(),
            "".to_string(),
            "中文".to_string(),
        ]);
        assert_eq!(v.len(), 3);
        assert_eq!(v.datum(0), Datum::Utf8("alpha"));
        assert_eq!(v.datum(1), Datum::Utf8(""));
        assert_eq!(v.utf8_value(2), "中文");
        // offsets: [0, 5, 5, 11]
        if let ColumnData::Utf8 { offsets, .. } = &v.data {
            assert_eq!(offsets, &[0, 5, 5, 11]);
        } else {
            panic!("expected utf8 column");
        }
    }

    #[test]
    fn malformed_offsets_rejected() {
        let bad = ColumnData::Utf8 {
            data: b"abc".to_vec(),
            offsets: vec![0, 2, 4],
        };
        assert!(Vector::new(bad, None).is_err());
    }

    #[test]
    fn select_compacts_data_and_nulls() {
        let v = Vector::from_scalars(
            DataType::Int64,
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Null,
                ScalarValue::Int64(3),
                ScalarValue::Int64(4),
            ],
        )
        .unwrap();
        let out = v.select(&[true, false, false, true]).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out.int64_values(), &[1, 4]);
        assert_eq!(out.scalar_at(0), ScalarValue::Int64(1));
        assert_eq!(out.scalar_at(1), ScalarValue::Int64(4));
    }

    #[test]
    fn select_utf8_with_nulls() {
        let mut vals: Vec<ScalarValue> = Vec::new();
        vals.push(ScalarValue::Utf8("a".into()));
        vals.push(ScalarValue::Null);
        vals.push(ScalarValue::Utf8("bbb".into()));
        let v = Vector::from_scalars(DataType::Utf8, vals).unwrap();
        let out = v.select(&[false, true, true]).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.null_at(0));
        assert_eq!(out.utf8_value(1), "bbb");
    }

    #[test]
    fn head_slices() {
        let v = Vector::utf8s(vec!["a".into(), "bb".into(), "ccc".into()]);
        let out = v.head(2);
        assert_eq!(out.utf8_value(1), "bb");
        assert_eq!(out.len(), 2);
        assert_eq!(v.head(99).len(), 3);
    }

    #[test]
    fn length_mismatch_rejected() {
        assert!(Vector::new(ColumnData::Int64(vec![1, 2]), Some(Validity::all_valid(3))).is_err());
    }
}
