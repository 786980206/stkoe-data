//! `create_table` and `update_table` — Arrow RecordBatch → native → core writer.
//! See `plan.md` §8.4.
//!
//! These are thin Arrow→native adapters: the RecordBatch is converted to
//! [`splayed_core::TableColumn`] raw bytes and handed to the core engine
//! (`splayed_core::create_table` / `update_table`), which owns the actual
//! file-format logic. Other exchange layers (e.g. a native DuckDB DataChunk
//! adapter) can write tables without ever going through Arrow.

use std::path::Path;

use arrow_array::{Array, RecordBatch, StringArray};
use splayed_core::{TableColumn, TableError as CoreTableError};
use splayed_format::TimeType;

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
    let folder = folder.as_ref();
    let (time_type, syms, times, columns) = convert_batch(data)?;

    splayed_core::create_table(folder, time_type, &syms, &times, &columns, sorted)
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
    let folder = folder.as_ref();
    let (_time_type, syms, times, columns) = convert_batch(data)?;

    splayed_core::update_table(folder, &syms, &times, &columns, create_missing_fields)
        .map_err(TableError::Core)?;
    Ok(())
}

/// Convert a RecordBatch into native table-writer input.
///
/// Returns `(time_type, syms, times, columns)`; FIELD column raw bytes are in
/// input-row order (the core engine scatters them to global rows).
fn convert_batch(data: &RecordBatch) -> Result<(TimeType, Vec<String>, Vec<i64>, Vec<TableColumn>), TableError> {
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
    let time_type = arrow_time_type(schema.field(time_idx).data_type())
        .ok_or(TableError::NoTimeColumn)?;

    let n = data.num_rows();
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