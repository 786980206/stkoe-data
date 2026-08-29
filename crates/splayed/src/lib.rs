//! Splayed umbrella crate.
//!
//! 引擎无关核心（format / codec / core / arrow）始终可用；**三个逻辑可选适配
//! 组件**通过 cargo features 开启，用于对接不同外部数据引擎：
//!
//! ```text
//! splayed { features = [...] }
//!   ├─ core / format / codec / arrow    （恒有，引擎无关，见 splayed_core::batch 的 CoreBatch）
//!   └─ 适配组件（可选）
//!      ├─ adbc        → ADBC 风格统一连接组件（splayed-adbc，引擎无关）
//!      ├─ datafusion  → DataFusion TableProvider 三层对接（splayed-datafusion）
//!      └─ duckdb      → DuckDB Arrow IPC 桥（splayed-duckdb）
//! ```
//!
//! 三个适配组件共享 `splayed_core`（Scanner/CoreBatch/Filter）与
//! `splayed_arrow::corebatch_into_record_batch`（零拷贝转换）作为统一底座，
//! 因此增加新引擎（Velox / Flink 等）只需新增一个 feature。
//!
//! 用法示例：
//! ```toml
//! [dependencies]
//! splayed = { path = "crates/splayed", features = ["adbc", "datafusion", "duckdb"] }
//! ```

pub use splayed_codec as codec;
pub use splayed_core as core;
pub use splayed_format as format;
pub use splayed_arrow as arrow;

#[cfg(feature = "adbc")]
pub use splayed_adbc as adbc;
#[cfg(feature = "datafusion")]
pub use splayed_datafusion as datafusion;
#[cfg(feature = "duckdb")]
pub use splayed_duckdb as duckdb;