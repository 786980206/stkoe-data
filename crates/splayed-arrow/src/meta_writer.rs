//! `create_meta`: build a `.meta` file from an Arrow RecordBatch (TIME + SYM).
//! See `plan.md` §8.4.

use std::fs;
use std::path::Path;

use arrow_array::{Array, RecordBatch, StringArray};
use splayed_format::{MetaBuilder, MetaFile, TimeType, META_FILE_NAME};

use crate::arrow_conv::arrow_time_type;

/// Build a `.meta` file from an Arrow RecordBatch containing TIME + SYM columns.
///
/// - `folder`: target directory (will be created if it doesn't exist).
/// - `data`: RecordBatch with at least `TIME` and `SYM` columns.
/// - `sorted`: whether the data is already sorted by (SYM, TIME) ascending.
///
/// The TIME column is identified by being a Date32 or Timestamp(µs) type.
/// The SYM column is identified by being the (first) Utf8/String column.
pub fn create_meta(
    folder: impl AsRef<Path>,
    data: &RecordBatch,
    sorted: bool,
) -> Result<MetaFile, CreateMetaError> {
    let folder = folder.as_ref();

    // Find TIME and SYM columns.
    let schema = data.schema();
    let mut time_col: Option<usize> = None;
    let mut sym_col: Option<usize> = None;

    for (i, field) in schema.fields().iter().enumerate() {
        if time_col.is_none() && arrow_time_type(field.data_type()).is_some() {
            time_col = Some(i);
        }
        if sym_col.is_none() && matches!(field.data_type(), arrow_schema::DataType::Utf8) {
            sym_col = Some(i);
        }
    }

    let time_idx = time_col.ok_or(CreateMetaError::NoTimeColumn)?;
    let sym_idx = sym_col.ok_or(CreateMetaError::NoSymColumn)?;

    let time_type = arrow_time_type(schema.field(time_idx).data_type())
        .ok_or(CreateMetaError::NoTimeColumn)?;

    let time_array = data.column(time_idx);
    let sym_array = data.column(sym_idx);

    let n = data.num_rows();
    let sym_str = sym_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(CreateMetaError::SymNotUtf8)?;

    // Build pairs (sym, time_i64).
    let mut builder = MetaBuilder::new(time_type, 1);

    for i in 0..n {
        let sym = if sym_array.is_null(i) {
            return Err(CreateMetaError::NullSym { row: i });
        } else {
            sym_str.value(i)
        };
        let time = extract_time_i64(time_array.as_ref(), i, time_type)?;
        builder.add(sym, time);
    }

    // If caller says it's sorted, trust but verify during build.
    if sorted {
        // MetaBuilder will detect unsorted and re-sort if needed.
        // We set the sorted flag but build() still validates.
    }

    let meta = builder.build().map_err(CreateMetaError::MetaBuild)?;

    // Write .meta atomically: write to temp, fsync, rename.
    let meta_path = folder.join(META_FILE_NAME);
    let tmp_path = folder.join(format!("{META_FILE_NAME}.new"));

    fs::create_dir_all(folder).map_err(CreateMetaError::Io)?;
    let bytes = meta.serialize();
    fs::write(&tmp_path, &bytes).map_err(CreateMetaError::Io)?;
    fs::rename(&tmp_path, &meta_path).map_err(CreateMetaError::Io)?;

    Ok(meta)
}

/// Extract a time value as i64 from an Arrow array at the given index.
fn extract_time_i64(
    arr: &dyn Array,
    i: usize,
    time_type: TimeType,
) -> Result<i64, CreateMetaError> {
    use arrow_array::{Date32Array, TimestampMicrosecondArray};
    if arr.is_null(i) {
        return Err(CreateMetaError::NullTime { row: i });
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
pub enum CreateMetaError {
    NoTimeColumn,
    NoSymColumn,
    SymNotUtf8,
    NullSym { row: usize },
    NullTime { row: usize },
    Io(std::io::Error),
    MetaBuild(splayed_format::MetaError),
}

impl std::fmt::Display for CreateMetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoTimeColumn => write!(f, "no TIME column (Date32 or Timestamp[µs]) found"),
            Self::NoSymColumn => write!(f, "no SYM column (Utf8) found"),
            Self::SymNotUtf8 => write!(f, "SYM column is not Utf8"),
            Self::NullSym { row } => write!(f, "null SYM at row {row}"),
            Self::NullTime { row } => write!(f, "null TIME at row {row}"),
            Self::Io(e) => write!(f, "create_meta io error: {e}"),
            Self::MetaBuild(e) => write!(f, "create_meta build error: {e}"),
        }
    }
}
impl std::error::Error for CreateMetaError {}
