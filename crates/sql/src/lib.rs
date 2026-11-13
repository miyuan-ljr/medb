//! SQL 前端:解析(sqlparser)+ 骨架计划器。
//!
//! 当前支持:无 FROM 的 `SELECT <标量表达式> [WHERE <常量表达式>] [LIMIT n [OFFSET m]]`,
//! 作为框架端到端贯通(客户端 → 解析 → 计划 → 向量化执行 → 协议输出)的最小闭环。
//! FROM 扫描 / JOIN / 聚合 / DDL 执行 见路线图 M3+;计划器结构已按可扩展方式组织。

pub mod parser;
pub mod planner;
