//! Splayed V1 Arrow exchange layer.
//!
//! Provides the Arrow-input functions from `plan.md` §8.4:
//! - `create_meta` — build `.meta` from an Arrow RecordBatch (TIME + SYM).
//! - `create_table` — one-shot create_meta + create_field + update_field.
//! - `update_table` — update existing fields from an Arrow RecordBatch.
//!
//! Also provides Splayed ↔ Arrow conversion (plan §10.1):
//! - `column_view_to_arrow` — ColumnView → Arrow ArrayRef (NULL/NaN semantics)
//! - `arrow_to_splayed_type` / `splayed_to_arrow_type` — type mapping

pub mod arrow_conv;
pub mod corebatch_to_arrow;
pub mod meta_writer;
pub mod table_writer;

pub use arrow_conv::{
    arrow_to_splayed_type, arrow_time_type, arrow_value_to_raw, column_view_to_arrow,
    splayed_to_arrow_type,
};
pub use corebatch_to_arrow::{
    core_type_to_arrow_type, corebatch_into_record_batch, corebatch_to_record_batch,
};
pub use meta_writer::{create_meta, CreateMetaError};
pub use table_writer::{create_table, update_table, TableError};
