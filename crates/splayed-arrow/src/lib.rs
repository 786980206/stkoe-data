//! Splayed V1 Arrow exchange layer.
//!
//! Provides the Arrow-input functions from `plan.md` §8.4 —与
//! `splayed-core` 的写接口对齐（输入/输出换成 Arrow）：
//! - `create_meta` — build `.meta` from an Arrow RecordBatch (TIME + SYM).
//! - `create_table` / `create_table_with_options` — one-shot create + fields.
//! - `update_table` / `update_table_with_options` — update existing fields.
//! - `update_meta` — 布局重排（新增 (SYM, TIME)），重散布全部字段。
//! - 分区写：`create_partitioned_table` / `append_partition` /
//!   `update_partition_table` / `update_partition_meta` / `drop_partition`
//!   （`FieldWriteOptions` 控制新字段是否直接以编码/压缩形式落盘）。
//!
//! 读侧便捷入口（供 Python / polars 消费）：
//! - `scan_dataset` / `scan_partitioned` — 扫描 → `Vec<RecordBatch>`。
//! - `read_subset` — 子集 `.sub.xxx` → `RecordBatch`（父行序）。
//!
//! Also provides Splayed ↔ Arrow conversion (plan §10.1):
//! - `column_view_to_arrow` — ColumnView → Arrow ArrayRef (NULL/NaN semantics)
//! - `arrow_to_splayed_type` / `splayed_to_arrow_type` — type mapping

pub mod arrow_conv;
pub mod corebatch_to_arrow;
pub mod meta_writer;
pub mod partition_writer;
pub mod scan;
pub mod subset;
pub mod table_writer;

pub use arrow_conv::{
    arrow_to_splayed_type, arrow_time_type, arrow_value_to_raw, column_view_to_arrow,
    splayed_to_arrow_type,
};
pub use corebatch_to_arrow::{
    core_type_to_arrow_type, corebatch_into_record_batch, corebatch_to_record_batch,
};
pub use meta_writer::{create_meta, CreateMetaError};
pub use partition_writer::{
    PartitionWriteInputArrow, append_partition, create_partitioned_table, drop_partition,
    update_partition_meta, update_partition_table,
};
pub use scan::{scan_dataset, scan_partitioned, ScanError};
pub use subset::read_subset;
pub use table_writer::{
    create_table, create_table_with_options, update_meta, update_table, update_table_with_options,
    TableError,
};

/// 新字段写选项（编码 + 压缩；默认 `PLAIN + NONE` 可写）。
///
/// 重导出 `splayed-core` 类型，消费端（Python / polars）直接用同一类型。
pub use splayed_core::FieldWriteOptions;
