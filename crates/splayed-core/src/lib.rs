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
    close_dataset, create_dataset, create_dataset_index, delete_dataset, drop_dataset,
    init_dataset, open_dataset, CreateDatasetOptions, DatasetFieldInit, DatasetHandle,
    DatasetScanner, DatasetStatistics, CHUNK_ROW_CAP, RESERVED_FIELD_NAMES,
};
pub use error::{CoreError, Mode};
pub use field_file::{
    cast_field, cast_field_file, cast_field_path, close_field, close_field_handle,
    column_view_to_owned_column, compress_field, compress_field_file, compress_field_path,
    create_field, create_field_file, decompress_field, decompress_field_file,
    decompress_field_path, delete_field_file, drop_field, drop_field_path, init_field,
    open_field, open_field_file, read_field_schema, rename_field, rename_field_file,
    CreateFieldOptions, FieldChunkReader, FieldHandle, FieldInit, FieldScanner, StreamValues,
};
pub use meta_file::{
    close_index, create_index, create_meta_file, delete_meta_file, drop_index, init_index,
    open_index, IndexHandle, MetaBuilder, MetaHandle, MetaInfo,
};
pub use scan::{merge_ranges, CmpOp, Predicate, RowRange, Scalar, ScanRequest};

/// crate 统一 Result 别名。
pub type Result<T> = std::result::Result<T, CoreError>;
