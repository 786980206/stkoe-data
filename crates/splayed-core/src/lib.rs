//! Splayed V1 core engine: reader, field writer, table writer, dataset management.
//!
//! This crate depends on `splayed-format` and `splayed-codec` but **not** Arrow.
//! It provides the pure-core operations: `create_field` (+
//! `create_field_with_data`), `update_field`, `delete_field`, `compact_field`,
//! the mmap-based reader, and the native table writers `create_meta` /
//! `create_table` / `update_table` (exchange layers convert their data to
//! [`TableColumn`] raw bytes — no Arrow needed).

pub mod batch;
pub mod column_view;
pub mod dataset;
pub mod field_writer;
pub mod reader;
pub mod scanner;
pub mod simd_filter;
pub mod table_writer;

pub use batch::{
    Bitmap, Buffer, CoreBatch, CoreColumn, CoreColumnKind, CoreField, CoreSchema, CoreStringDict,
    CoreTimeUnit, CoreType,
};
pub use column_view::{ColumnView, ColumnViewIter};
pub use dataset::{open_dataset, Dataset, DatasetError};
pub use field_writer::{
    create_field, create_field_with_data, delete_field, update_field, CreateFieldError,
    DeleteFieldError, UpdateError, UpdateItem,
};
pub use reader::{FieldReader, FieldStats, ReaderError};
pub use scanner::{
    Filter, FilterValue, OwnedScanBatches, ParallelScanBatches, ScanBatches, ScanPlan,
    ScanRequest, Scanner, ScannerError, SymbolSelection, TimeRange, scan_owned,
    scan_owned_parallel, split_ranges,
};
pub use table_writer::{
    TableColumn, TableError, create_meta, create_table, update_table,
};

// Re-export compact_field from splayed-codec (plan §8.4 places it in splayed-codec).
pub use splayed_codec::{compact_field, decompress_field_data, CompactError, DecompressError};
