//! DuckDB 集成（`splayed-duckdb`）。
//!
//! 两条路径，按需开启：
//!
//! 1. **Arrow IPC 桥**（`feature = "arrow"`，默认开）：Splayed → CoreBatch →
//!    Arrow（零拷贝）→ IPC 文件，DuckDB 用 `read_arrow('file.arrow')` 读取。
//!    适合“外部文件交换”场景，代价是 IPC 序列化 + DuckDB 重新解析一段拷贝。
//! 2. **原生 DataChunk 路由**（无 arrow，`mod native`）：直接以 `CoreBatch`
//!    流喂给 DuckDB 扩展层（未来的 TableFunction / Copy 函数）。CoreBatch 的
//!    定长小端缓冲 + validity 位图与 DuckDB `Vector` 的 `data_ptr + NullMask`
//!    布局同构，扩展内可用 `Vector(LogicalType, data_ptr)` 零拷贝借用：
//!    - 数值/日期/时间戳列：数据缓冲原样（无拷贝）；
//!    - NULL：CoreBatch validity 位图 → DuckDB `NullMask`（n/8 字节小拷贝）；
//!    - SYM（字典列）：扩展侧展开为 DuckDB 字典/字符串向量（或复用元数据）。
//!
//! 详见 [`native`] 模块与 `plan.md` §10.4。

#[cfg(feature = "arrow")]
mod arrow_bridge;

pub mod ffi;
pub mod native;

#[cfg(feature = "arrow")]
pub use crate::arrow_bridge::{ExportError, build_arrow_schema, export_to_arrow_ipc};