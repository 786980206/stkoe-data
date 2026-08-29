//! Splayed umbrella crate。
//!
//! 层次（非并列组件），按引擎/用途分层：
//!
//! ```text
//! 外部应用
//!   │
//!   ▼
//! splayed::adbc   （feature "adbc"）   上层 ADBC 驱动：内部经 DataFusion 执行
//!   │                                   SQL，返回 Arrow 结果（不复实现 SQL 解析）
//!   ├▶ splayed::datafusion（feature）  DataFusion TableProvider + SQL 执行
//!   └▶ splayed::duckdb   （feature）   DuckDB 扩展 / Arrow IPC 桥
//!   │
//! splayed::core / format / codec       引擎无关核心：数据扫描与写入（CoreBatch）
//!   ▲
//!   │ 共享转换工具（feature "arrow"，默认开、可关）
//! splayed::arrow                       CoreBatch → Arrow 零拷贝转换
//! ```
//!
//! - **splayed-core**：引擎无关核心（扫描/写入），不依赖任何引擎。
//! - **splayed-datafusion**：DataFusion `TableProvider`（查询自定义格式）并
//!   提供 SQL 执行能力（`SessionContext`）。
//! - **splayed-duckdb**：DuckDB 扩展能力（当前为 Arrow IPC 桥）。
//! - **splayed-adbc**：**上层** ADBC 驱动，面向外部应用，内部使用 DataFusion
//!   执行 SQL（DuckDB 可作平替后端），返回 Arrow 结果。
//! - **splayed-arrow**：可选共享转换工具库，位于核心层之上，被需要 Arrow 的
//!   适配层（DataFusion / ADBC）复用；多个 Arrow 消费者时避免重复转换代码。
//!
//! 用法：`splayed = { features = ["arrow", "adbc", "duckdb"] }`（datafusion 由
//! adbc 隐式引入；也可显式开启）。

pub use splayed_codec as codec;
pub use splayed_core as core;
pub use splayed_format as format;

#[cfg(feature = "arrow")]
pub use splayed_arrow as arrow;
#[cfg(feature = "datafusion")]
pub use splayed_datafusion as datafusion;
#[cfg(feature = "duckdb")]
pub use splayed_duckdb as duckdb;
#[cfg(feature = "adbc")]
pub use splayed_adbc as adbc;