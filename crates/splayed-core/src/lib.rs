//! splayed-core：V2.0 引擎无关核心（Field / META / Dataset 三层 API）。
//!
//! 权威设计见 `docs/splayed-core.md`；磁盘格式见 `docs/splayed-format.md`。
//! 逻辑行空间 = 容量网格：三层 API 的 offset / length 含义一致，无需换算。

pub mod arena;
pub mod dataset;
pub mod error;
pub mod field_file;
pub mod meta_file;
pub mod scan;

pub use dataset::{
    create_dataset, create_dataset_index, delete_dataset, open_dataset, CreateDatasetOptions,
    DatasetFieldInit, DatasetHandle, DatasetScanner, DatasetStatistics,
    RESERVED_FIELD_NAMES, CHUNK_ROW_CAP,
};
pub use error::{CoreError, Mode};
pub use field_file::{
    cast_field_file, close_field_handle, compress_field_file, create_field_file,
    CreateFieldOptions, delete_field_file, decompress_field_file, open_field_file,
    rename_field_file, FieldChunkReader,
    FieldHandle, FieldInit, FieldScanner, StreamValues,
};
pub use meta_file::{create_meta_file, delete_meta_file, MetaBuilder, MetaHandle, MetaInfo};
pub use scan::{merge_ranges, CmpOp, Predicate, RowRange, Scalar, ScanRequest};

/// crate 统一 Result 别名。
pub type Result<T> = std::result::Result<T, CoreError>;
