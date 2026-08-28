//! Splayed V1 core engine: reader, field writer, dataset management.
//!
//! This crate depends on `splayed-format` and `splayed-codec` but **not** Arrow.
//! It provides the pure-core operations: `create_field`, `update_field`,
//! `delete_field`, `compact_field`, and the mmap-based reader.

pub mod column_view;
pub mod dataset;
pub mod field_writer;
pub mod reader;
pub mod scanner;
pub mod simd_filter;

pub use column_view::{ColumnView, ColumnViewIter};
pub use dataset::{open_dataset, Dataset, DatasetError};
pub use field_writer::{
    create_field, delete_field, update_field, CreateFieldError, DeleteFieldError, UpdateError,
    UpdateItem,
};
pub use reader::{FieldReader, ReaderError};
pub use scanner::{
    Filter, FilterValue, ScanBatchOwned, ScanBatches, ScanPlan, ScanRequest, Scanner,
    ScannerError, SymbolSelection, TimeRange,
};

// Re-export compact_field from splayed-codec (plan §8.4 places it in splayed-codec).
pub use splayed_codec::{compact_field, decompress_field_data, CompactError, DecompressError};
