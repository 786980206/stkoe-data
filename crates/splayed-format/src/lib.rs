//! Splayed V1 binary format definitions.
//!
//! This crate defines the on-disk layout for META and FIELD files.
//! It has **no dependency** on Arrow or any I/O library — just `bytemuck`
//! for zero-copy reinterpretation of header structs.
//!
//! See `plan.md` §5 for the authoritative specification.
//!
//! ## Modules
//! - [`types`] — `DataType`, NULL bit patterns, `RawValue`.
//! - [`header`] — 64-byte META/FIELD headers, magic, encoding/compression enums.
//! - [`meta`] — `MetaFile`, `MetaBuilder`, `SymIndexRecord`.
//! - [`subset`] — `.sub.xxx` subset files（父 `.meta` 网格子集，每 SYM 多区间）.
//! - [`field`] — FIELD data offset computation, header helpers.

pub mod field;
pub mod field_footer;
pub mod header;
pub mod meta;
pub mod subset;
pub mod types;

pub use field::{
    data_length, field_file_size, new_plain_field_header, row_byte_offset, validate_field_header,
};
pub use header::{
    Compression, Encoding, FieldHeader, MetaHeader, TimeType, DATA_OFFSET, FIELD_MAGIC,
    FORMAT_VERSION, HEADER_SIZE, META_FILE_NAME, META_MAGIC,
};
pub use meta::{MetaBuilder, MetaError, MetaFile, SymIndexRecord};
pub use subset::{SubsetBuilder, SubsetError, SubsetFile, SubsetHeader, SUBSET_MAGIC, SUBSET_PREFIX};
pub use types::{fill_null, DataType, RawValue};
