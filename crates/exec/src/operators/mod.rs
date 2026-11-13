//! 物理算子集合。新算子按同样模式追加:实现 `Executor`,批内做向量化动作。

pub mod filter;
pub mod hash_agg;
pub mod hash_join;
pub mod limit;
pub mod project;
pub mod seq_scan;
pub mod sort;
pub mod values;
