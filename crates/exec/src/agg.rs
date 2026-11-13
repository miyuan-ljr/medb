//! 聚合函数描述与类型推导(内核在 operators::hash_agg 中)。

use common::error::{Error, Result};
use common::types::DataType;

use crate::expr::Expr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::CountStar | AggFunc::Count => "COUNT",
            AggFunc::Sum => "SUM",
            AggFunc::Avg => "AVG",
            AggFunc::Min => "MIN",
            AggFunc::Max => "MAX",
        }
    }

    /// 输出类型;arg_type 为 None 表示 COUNT(*) 或参数为裸 NULL。
    pub fn return_type(&self, arg_type: Option<DataType>) -> Result<DataType> {
        match self {
            AggFunc::CountStar | AggFunc::Count => Ok(DataType::Int64),
            AggFunc::Sum => match arg_type {
                Some(DataType::Int64) | Some(DataType::Float64) | None => {
                    Ok(arg_type.unwrap_or(DataType::Int64))
                }
                Some(t) => Err(Error::sql(format!("SUM requires numeric input, got {t:?}"))),
            },
            AggFunc::Avg => match arg_type {
                Some(DataType::Int64) | Some(DataType::Float64) | None => Ok(DataType::Float64),
                Some(t) => Err(Error::sql(format!("AVG requires numeric input, got {t:?}"))),
            },
            AggFunc::Min | AggFunc::Max => Ok(arg_type.unwrap_or(DataType::Int64)),
        }
    }
}

/// 一个聚合输出列的描述。
#[derive(Debug, Clone)]
pub struct AggDesc {
    pub func: AggFunc,
    pub arg: Option<Expr>,
    pub out_type: DataType,
}

impl AggDesc {
    /// arg 是针对输入批的列引用表达式;arg_type 据其静态推导。
    pub fn new(
        func: AggFunc,
        arg: Option<Expr>,
        input: &[common::schema::ColumnDesc],
    ) -> Result<Self> {
        let arg_type = match &arg {
            Some(e) => e.return_type(input)?,
            None => None,
        };
        Ok(Self {
            func,
            arg,
            out_type: func.return_type(arg_type)?,
        })
    }
}
