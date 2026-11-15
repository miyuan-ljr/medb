//! SQL 前端:解析(sqlparser)+ 代价优化器 + 计划器。
//!
//! - `parser`:SQL 文本 → AST;
//! - `optimizer`:统计信息驱动的选择性/基数估算 + INNER 连接组的 DP 顺序枚举;
//! - `planner`:AST → 向量化执行计划(扫描下推 / 哈希连接 / 聚合 / 排序)。

pub mod decorrelate;
pub mod optimizer;
pub mod parser;
pub mod planner;
