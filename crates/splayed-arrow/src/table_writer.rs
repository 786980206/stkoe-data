//! `create_table` / `update_table` / `update_meta` — Arrow RecordBatch → native
//! → core writer. See `plan.md` §8.4.
//!
//! These are thin Arrow→native adapters: the RecordBatch is converted to
//! [`splayed_core::TableColumn`] raw bytes and handed to the core engine
//! (`splayed_core::create_table` / `update_table` / `update_meta`), which owns
//! the actual file-format logic. Other exchange layers (e.g. a native DuckDB
//! DataChunk adapter) can write tables without ever going through Arrow.

use std::path::Path;

use arrow_array::{Array, RecordBatch, StringArray};
use splayed_core::{FieldWriteOptions, TableColumn, TableError as CoreTableError};
use splayed_format::{MetaFile, TimeType};

use crate::arrow_conv::{arrow_time_type, arrow_to_splayed_type, arrow_value_to_raw};

/// One-shot: create `.meta` + all FIELD files, each FIELD written once with data.
///
/// `sorted`: performance hint — input already ordered by (SYM, TIME) ascending
/// lets the core engine skip per-row global_row lookups (verified internally).
///
/// See `plan.md` §8.4 `create_table`.
pub fn create_table(
    folder: impl AsRef<Path>,
    data: &RecordBatch,
    sorted: bool,
) -> Result<(), TableError> {
    create_table_with_options(folder, data, sorted, FieldWriteOptions::default())
}

/// [`create_table`] + 新字段写选项（`opts` 非默认时全部 FIELD 直接以编码/
/// 压缩形式落盘，写后只读）。
pub fn create_table_with_options(
    folder: impl AsRef<Path>,
    data: &RecordBatch,
    sorted: bool,
    opts: FieldWriteOptions,
) -> Result<(), TableError> {
    let folder = folder.as_ref();
    let (time_type, syms, times, columns) = convert_batch(data)?;

    splayed_core::create_table_with_options(folder, time_type, &syms, &times, &columns, sorted, opts)
        .map_err(TableError::Core)?;
    Ok(())
}

/// Update existing FIELD files from an Arrow RecordBatch (in-place).
///
/// `create_missing_fields`: when a FIELD column in `data` doesn't exist on disk,
/// auto-create it as a new column (existing rows NULL) instead of erroring.
///
/// See `plan.md` §8.4 `update_table`.
pub fn update_table(
    folder: impl AsRef<Path>,
    data: &RecordBatch,
    _sorted: bool,
    create_missing_fields: bool,
) -> Result<(), TableError> {
    update_table_with_options(folder, data, create_missing_fields, FieldWriteOptions::default())
}

/// [`update_table`] + 新字段写选项（`opts` 非默认时**新创建**的字段直接以
/// 编码/压缩形式落盘，写后只读；已存在字段仍原地更新）。
pub fn update_table_with_options(
    folder: impl AsRef<Path>,
    data: &RecordBatch,
    create_missing_fields: bool,
    opts: FieldWriteOptions,
) -> Result<(), TableError> {
    let folder = folder.as_ref();
    let (_time_type, syms, times, columns) = convert_batch(data)?;

    splayed_core::update_table_with_options(
        folder,
        &syms,
        &times,
        &columns,
        create_missing_fields,
        opts,
    )
    .map_err(TableError::Core)?;
    Ok(())
}

/// 用新的 (SYM, TIME) 布局重写 `.meta` 并并发重散布全部字段（Arrow 输入）。
///
/// 仅提取输入中的 TIME + SYM 两列（字段列忽略），委托
/// `splayed_core::update_meta`（gather 重散布、generation 原子提交）。
///
/// `sorted`: 与 `create_meta` 一致，目前仅作占位（`MetaBuilder` 内部总是
/// 校验/排序）。
pub fn update_meta(
    folder: impl AsRef<Path>,
    data: &RecordBatch,
    _sorted: bool,
) -> Result<MetaFile, TableError> {
    let folder = folder.as_ref();
    let (time_type, syms, times) = convert_keys(data)?;
    splayed_core::update_meta(folder, time_type, &syms, &times).map_err(TableError::Core)
}

/// `convert_batch` 的输出：`(time_type, syms, times, columns)`。
pub(crate) type ConvertedBatch = (TimeType, Vec<String>, Vec<i64>, Vec<TableColumn>);

/// Convert a RecordBatch into native table-writer input.
///
/// Returns `(time_type, syms, times, columns)`; FIELD column raw bytes are in
/// input-row order (the core engine scatters them to global rows).
pub(crate) fn convert_batch(data: &RecordBatch) -> Result<ConvertedBatch, TableError> {
    let (time_type, syms, times) = convert_keys(data)?;
    let n = data.num_rows();
    let schema = data.schema();
    let (time_idx, sym_idx) = key_indices(data)?;

    let mut columns: Vec<TableColumn> = Vec::new();
    for (col_idx, field) in schema.fields().iter().enumerate() {
        if col_idx == time_idx || col_idx == sym_idx {
            continue;
        }
        let splayed_ty = arrow_to_splayed_type(field.data_type())
            .ok_or_else(|| TableError::UnsupportedType(field.data_type().clone()))?;
        let elem_sz = splayed_ty.size_of();
        let arr = data.column(col_idx);

        // Raw LE bytes in input-row order.
        let mut values = vec![0u8; n * elem_sz];
        for i in 0..n {
            let val = match arrow_value_to_raw(arr, i, splayed_ty) {
                Some(v) => v,
                None => splayed_format::RawValue::null(splayed_ty),
            };
            val.write_le(&mut values, i * elem_sz);
        }
        columns.push(TableColumn {
            name: field.name().clone(),
            data_type: splayed_ty,
            values,
        });
    }

    Ok((time_type, syms, times, columns))
}

/// 提取 TIME + SYM 两列（键），供 `create_meta` / `update_meta` 使用。
fn convert_keys(data: &RecordBatch) -> Result<(TimeType, Vec<String>, Vec<i64>), TableError> {
    let n = data.num_rows();
    let (time_idx, sym_idx) = key_indices(data)?;
    let schema = data.schema();
    let time_type = arrow_time_type(schema.field(time_idx).data_type())
        .ok_or(TableError::NoTimeColumn)?;

    let sym_array = data
        .column(sym_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(TableError::SymNotUtf8)?;
    let time_array = data.column(time_idx);

    let mut syms: Vec<String> = Vec::with_capacity(n);
    let mut times: Vec<i64> = Vec::with_capacity(n);
    for i in 0..n {
        if sym_array.is_null(i) {
            return Err(TableError::NullSym { row: i });
        }
        syms.push(sym_array.value(i).to_string());
        times.push(extract_time_i64(time_array.as_ref(), i, time_type)?);
    }
    Ok((time_type, syms, times))
}

/// 定位 TIME 列（Date32/Timestamp[µs]）与 SYM 列（Utf8）。
fn key_indices(data: &RecordBatch) -> Result<(usize, usize), TableError> {
    let schema = data.schema();
    let time_idx = schema
        .fields()
        .iter()
        .position(|f| arrow_time_type(f.data_type()).is_some())
        .ok_or(TableError::NoTimeColumn)?;
    let sym_idx = schema
        .fields()
        .iter()
        .position(|f| matches!(f.data_type(), arrow_schema::DataType::Utf8))
        .ok_or(TableError::NoSymColumn)?;
    Ok((time_idx, sym_idx))
}

fn extract_time_i64(
    arr: &dyn Array,
    i: usize,
    time_type: TimeType,
) -> Result<i64, TableError> {
    use arrow_array::{Date32Array, TimestampMicrosecondArray};
    if arr.is_null(i) {
        return Err(TableError::NullTime { row: i });
    }
    Ok(match time_type {
        TimeType::Date32 => {
            let a = arr.as_any().downcast_ref::<Date32Array>().unwrap();
            a.value(i) as i64
        }
        TimeType::TimestampUs => {
            let a = arr.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            a.value(i)
        }
    })
}

#[derive(Debug)]
pub enum TableError {
    NoTimeColumn,
    NoSymColumn,
    SymNotUtf8,
    NullSym { row: usize },
    NullTime { row: usize },
    UnsupportedType(arrow_schema::DataType),
    Io(std::io::Error),
    /// Error from the core engine (empty dir, length mismatch, (SYM,TIME) not
    /// in META, missing field, type mismatch, I/O, ...).
    Core(CoreTableError),
}

impl std::fmt::Display for TableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoTimeColumn => write!(f, "no TIME column found in input"),
            Self::NoSymColumn => write!(f, "no SYM (Utf8) column found in input"),
            Self::SymNotUtf8 => write!(f, "SYM column is not Utf8"),
            Self::NullSym { row } => write!(f, "null SYM at row {row}"),
            Self::NullTime { row } => write!(f, "null TIME at row {row}"),
            Self::UnsupportedType(ty) => write!(f, "unsupported Arrow type: {ty}"),
            Self::Io(e) => write!(f, "table io error: {e}"),
            Self::Core(e) => write!(f, "core table error: {e}"),
        }
    }
}
impl std::error::Error for TableError {}

impl From<CoreTableError> for TableError {
    fn from(e: CoreTableError) -> Self {
        TableError::Core(e)
    }
}