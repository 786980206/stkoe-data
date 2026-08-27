//! Splayed V1 Arrow exchange layer.
//!
//! Provides the Arrow-input functions from `plan.md` §8.4:
//! - `create_meta` — build `.meta` from an Arrow RecordBatch (TIME + SYM).
//! - `create_table` — one-shot create_meta + create_field + update_field.
//! - `update_table` — update existing fields from an Arrow RecordBatch.
//!
//! Also provides Splayed ↔ Arrow conversion (plan §10.1).

pub mod arrow_conv;
pub mod meta_writer;
pub mod table_writer;

pub use meta_writer::{create_meta, CreateMetaError};
pub use table_writer::{create_table, update_table, TableError};
