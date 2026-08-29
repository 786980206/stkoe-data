//! splayed-polars：Polars 深度集成（惰性扫描 + 谓词下推 + 列裁剪）。
//!
//! - [`anonymous::SplayedScan`] 实现 Polars 官方 `AnonymousScan`（惰性数据源）：
//!   `LazyFrame::anonymous_scan` 注册，支持 `filter(...)/select(...)` 惰性链。
//! - 谓词下推：pushdown 的过滤条件经 [`predicate`] 翻译进核心层
//!   （SYM 选择 / TIME 范围 / 值过滤），减少读取与转换；未翻译部分由
//!   polars 物理评估兜底（结果恒正确）。
//! - 列裁剪：只扫描 `with_columns` 涉及的列（投影下推）。
//! - 转换：经 Arrow C data interface（零拷贝）进 polars，见 [`arrowconv`]。
//!
//! 依赖：Polars 0.45（`polars` meta crate + `polars-arrow`/`polars-expr`）。

pub mod anonymous;
pub mod arrowconv;
pub mod predicate;

pub use anonymous::{
    SplayedScan, SplayedTableScan, splayed_lazyframe, splayed_lazyframe_table,
};