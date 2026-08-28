//! `create_table` and `update_table` — Arrow RecordBatch → Splayed dataset.
//! See `plan.md` §8.4.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use arrow_array::{Array, RecordBatch, StringArray};
use splayed_format::{DataType, TimeType};
use splayed_core::{
    create_field, open_dataset, update_field, UpdateItem,
};

use crate::arrow_conv::{arrow_time_type, arrow_to_splayed_type, arrow_value_to_raw};
use crate::meta_writer::create_meta;

/// One-shot: create `.meta` + all FIELD files + fill them with data.
///
/// See `plan.md` §8.4 `create_table`.
pub fn create_table(
    folder: impl AsRef<Path>,
    data: &RecordBatch,
    sorted: bool,
) -> Result<(), TableError> {
    let folder = folder.as_ref();

    // Directory must be empty or not exist.
    if folder.exists() {
        let entries: Vec<_> = fs::read_dir(folder).map_err(TableError::Io)?.collect();
        let non_empty = entries
            .into_iter()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name() != ".gitkeep");
        if non_empty {
            return Err(TableError::DirNotEmpty);
        }
    }
    fs::create_dir_all(folder).map_err(TableError::Io)?;

    // Step 1: create .meta (from TIME + SYM columns).
    let meta = create_meta(folder, data, sorted).map_err(TableError::CreateMeta)?;

    // Step 2: identify FIELD columns (all non-TIME, non-SYM columns).
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

    // Step 3: for each FIELD column, create_field + update_field.
    let n = data.num_rows();
    let sym_array = data
        .column(sym_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(TableError::SymNotUtf8)?;
    let time_array = data.column(time_idx);
    let time_type = arrow_time_type(schema.field(time_idx).data_type()).unwrap();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        if col_idx == time_idx || col_idx == sym_idx {
            continue;
        }
        let name = field.name().clone();
        let splayed_ty = arrow_to_splayed_type(field.data_type())
            .ok_or_else(|| TableError::UnsupportedType(field.data_type().clone()))?;

        let field_path = folder.join(&name);

        // create_field: pre-allocate with NULL.
        create_field(&field_path, splayed_ty).map_err(TableError::CreateField)?;

        // Build update items: for each row, map (sym, time) → global_row.
        let elem_sz = splayed_ty.size_of();
        let arr = data.column(col_idx);

        // Group consecutive rows that map to consecutive global rows for efficiency.
        let mut values: Vec<u8> = Vec::with_capacity(n * elem_sz);
        let mut start_row: Option<u32> = None;
        let mut prev_row: Option<u32> = None;
        let mut items: Vec<UpdateItem> = Vec::new();

        for i in 0..n {
            let sym = sym_array.value(i);
            let time = extract_time_i64(time_array.as_ref(), i, time_type)?;
            let global_row = meta
                .global_row(sym, time)
                .ok_or(TableError::SymTimeNotFound { row: i })?;

            // For V1 simplicity: batch consecutive rows that map to consecutive
            // global rows into a single UpdateItem; flush on non-contiguity.
            if let Some(prev) = prev_row {
                if global_row == prev + 1 {
                    // contiguous — extend current batch.
                    push_value(&mut values, arr.as_ref(), i, splayed_ty, elem_sz);
                    prev_row = Some(global_row);
                    continue;
                } else {
                    // non-contiguous — flush current batch.
                    let sr = start_row.unwrap();
                    items.push(UpdateItem::new(sr, std::mem::take(&mut values)));
                }
            }
            // start new batch.
            start_row = Some(global_row);
            prev_row = Some(global_row);
            push_value(&mut values, arr.as_ref(), i, splayed_ty, elem_sz);
        }
        // flush last batch.
        if let Some(sr) = start_row {
            items.push(UpdateItem::new(sr, values));
        }

        if !items.is_empty() {
            update_field(&field_path, &items).map_err(TableError::UpdateField)?;
        }
    }

    Ok(())
}

/// Update existing FIELD files from an Arrow RecordBatch.
///
/// See `plan.md` §8.4 `update_table`.
pub fn update_table(
    folder: impl AsRef<Path>,
    data: &RecordBatch,
    _sorted: bool,
) -> Result<(), TableError> {
    let folder = folder.as_ref();
    let dataset = open_dataset(folder).map_err(TableError::Dataset)?;
    let meta = &dataset.meta;

    // Find TIME and SYM columns in input.
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

    // Determine which FIELD files exist on disk.
    let existing: HashSet<String> = dataset
        .list_fields()
        .map_err(TableError::Io)?
        .into_iter()
        .collect();

    let n = data.num_rows();
    let sym_array = data
        .column(sym_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(TableError::SymNotUtf8)?;
    let time_array = data.column(time_idx);
    let time_type = arrow_time_type(schema.field(time_idx).data_type()).unwrap();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        if col_idx == time_idx || col_idx == sym_idx {
            continue;
        }
        let name = field.name().clone();
        if !existing.contains(&name) {
            return Err(TableError::FieldNotFound(name));
        }

        let splayed_ty = arrow_to_splayed_type(field.data_type())
            .ok_or_else(|| TableError::UnsupportedType(field.data_type().clone()))?;

        // Verify the incoming type matches the existing FIELD's type (C3 fix).
        let field_path = folder.join(&name);
        {
            let reader = splayed_core::FieldReader::open(&field_path)
                .map_err(|e| TableError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("open field '{name}' for type check: {e}"),
                )))?;
            let field_dt = reader.data_type();
            if field_dt != splayed_ty {
                return Err(TableError::TypeMismatch {
                    field: name,
                    expected: field_dt,
                    got: splayed_ty,
                });
            }
            // Drop reader before writing (Windows mmap safety).
            drop(reader);
        }

        let elem_sz = splayed_ty.size_of();
        let arr = data.column(col_idx);

        // Build update items.
        let mut values: Vec<u8> = Vec::with_capacity(n * elem_sz);
        let mut start_row: Option<u32> = None;
        let mut prev_row: Option<u32> = None;
        let mut items: Vec<UpdateItem> = Vec::new();

        for i in 0..n {
            let sym = sym_array.value(i);
            let time = extract_time_i64(time_array.as_ref(), i, time_type)?;
            let global_row = meta
                .global_row(sym, time)
                .ok_or(TableError::SymTimeNotFound { row: i })?;

            if let Some(prev) = prev_row {
                if global_row == prev + 1 {
                    push_value(&mut values, arr.as_ref(), i, splayed_ty, elem_sz);
                    prev_row = Some(global_row);
                    continue;
                } else {
                    let sr = start_row.unwrap();
                    items.push(UpdateItem::new(sr, std::mem::take(&mut values)));
                }
            }
            start_row = Some(global_row);
            prev_row = Some(global_row);
            push_value(&mut values, arr.as_ref(), i, splayed_ty, elem_sz);
        }
        if let Some(sr) = start_row {
            items.push(UpdateItem::new(sr, values));
        }

        if !items.is_empty() {
            update_field(&field_path, &items).map_err(TableError::UpdateField)?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn push_value(
    out: &mut Vec<u8>,
    arr: &dyn Array,
    i: usize,
    splayed_ty: DataType,
    elem_sz: usize,
) {
    let val = match arrow_value_to_raw(arr, i, splayed_ty) {
        Some(v) => v,
        None => splayed_format::RawValue::null(splayed_ty),
    };
    let old_len = out.len();
    out.resize(old_len + elem_sz, 0);
    val.write_le(out, old_len);
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
    DirNotEmpty,
    NoTimeColumn,
    NoSymColumn,
    SymNotUtf8,
    NullTime { row: usize },
    SymTimeNotFound { row: usize },
    FieldNotFound(String),
    /// The incoming Arrow type does not match the FIELD's stored DataType.
    TypeMismatch {
        field: String,
        expected: splayed_format::DataType,
        got: splayed_format::DataType,
    },
    UnsupportedType(arrow_schema::DataType),
    Io(std::io::Error),
    CreateMeta(crate::CreateMetaError),
    CreateField(splayed_core::CreateFieldError),
    UpdateField(splayed_core::UpdateError),
    Dataset(splayed_core::DatasetError),
}

impl std::fmt::Display for TableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DirNotEmpty => write!(f, "target directory is not empty"),
            Self::NoTimeColumn => write!(f, "no TIME column found in input"),
            Self::NoSymColumn => write!(f, "no SYM (Utf8) column found in input"),
            Self::SymNotUtf8 => write!(f, "SYM column is not Utf8"),
            Self::NullTime { row } => write!(f, "null TIME at row {row}"),
            Self::SymTimeNotFound { row } => {
                write!(f, "(SYM, TIME) at row {row} not found in META")
            }
            Self::FieldNotFound(name) => write!(f, "field '{name}' not found on disk"),
            Self::TypeMismatch { field, expected, got } => {
                write!(f, "type mismatch for field '{field}': expected {expected:?}, got {got:?}")
            }
            Self::UnsupportedType(ty) => write!(f, "unsupported Arrow type: {ty}"),
            Self::Io(e) => write!(f, "table io error: {e}"),
            Self::CreateMeta(e) => write!(f, "create_meta failed: {e}"),
            Self::CreateField(e) => write!(f, "create_field failed: {e}"),
            Self::UpdateField(e) => write!(f, "update_field failed: {e}"),
            Self::Dataset(e) => write!(f, "dataset open failed: {e}"),
        }
    }
}
impl std::error::Error for TableError {}
