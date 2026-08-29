//! Table-level writers on **native** (non-Arrow) data: `create_meta`,
//! `create_table`, `update_table`.
//!
//! Exchange layers (Arrow, a future native DuckDB DataChunk adapter, …) convert
//! their own data into [`TableColumn`] raw byte form and call down here — the
//! engine itself never depends on Arrow.
//!
//! See `plan.md` §8.4 for the authoritative semantics.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use splayed_format::{DataType, MetaBuilder, MetaFile, TimeType, META_FILE_NAME};

use crate::field_writer::{create_field_with_data, update_field, UpdateItem};
use crate::{CreateFieldError, DatasetError, FieldReader, UpdateError, open_dataset};

/// One FIELD column in **input-row order**: raw little-endian values.
///
/// `values.len() = n_rows × data_type.size_of()`; `create_table` /
/// `update_table` place each input row at its `global_row` internally.
pub struct TableColumn {
    pub name: String,
    pub data_type: DataType,
    pub values: Vec<u8>,
}

/// Build + atomically write `.meta` from (SYM, TIME) pairs.
///
/// `sym` and `time` are aligned row slices (`sym[i]` ↔ `time[i]`).
pub fn create_meta(
    dir: impl AsRef<Path>,
    time_type: TimeType,
    sym: &[String],
    time: &[i64],
) -> Result<MetaFile, TableError> {
    let dir = dir.as_ref();
    if sym.len() != time.len() {
        return Err(TableError::LengthMismatch {
            field: "sym/time".to_string(),
            expected: sym.len(),
            got: time.len(),
        });
    }

    fs::create_dir_all(dir).map_err(TableError::Io)?;

    let mut builder = MetaBuilder::new(time_type, 1);
    for i in 0..sym.len() {
        builder.add(&sym[i], time[i]);
    }
    let meta = builder.build().map_err(TableError::Meta)?;

    // Atomic commit (plan §6): write .meta.new → fsync → rename.
    let meta_path = dir.join(META_FILE_NAME);
    let tmp_path = dir.join(format!("{META_FILE_NAME}.new"));
    let bytes = meta.serialize();
    {
        use std::io::Write;
        let mut file = fs::File::create(&tmp_path).map_err(TableError::Io)?;
        file.write_all(&bytes).map_err(TableError::Io)?;
        file.sync_all().map_err(TableError::Io)?;
    }
    fs::rename(&tmp_path, &meta_path).map_err(TableError::Io)?;

    Ok(meta)
}

/// One-shot table creation: `.meta` + each FIELD written once with data.
///
/// Values are given in input-row order and scattered to their global rows via
/// the (SYM, TIME) → global_row mapping; each FIELD file is written in a single
/// pass (`create_field_with_data`) instead of pre-allocate + update.
pub fn create_table(
    dir: impl AsRef<Path>,
    time_type: TimeType,
    sym: &[String],
    time: &[i64],
    columns: &[TableColumn],
) -> Result<MetaFile, TableError> {
    let dir = dir.as_ref();

    // Directory must be empty or not exist (plan §8.4 create_table constraint).
    if dir.exists() {
        let non_empty = fs::read_dir(dir)
            .map_err(TableError::Io)?
            .filter_map(|e| e.ok())
            .any(|e| e.file_name() != ".gitkeep");
        if non_empty {
            return Err(TableError::DirNotEmpty);
        }
    }

    let n = sym.len();
    for col in columns {
        let expected = n * col.data_type.size_of();
        if col.values.len() != expected {
            return Err(TableError::LengthMismatch {
                field: col.name.clone(),
                expected,
                got: col.values.len(),
            });
        }
    }

    // 1) .meta
    let meta = create_meta(dir, time_type, sym, time)?;

    // 2) each FIELD written once in global-row order.
    for col in columns {
        let elem_sz = col.data_type.size_of();
        // Buffer spans the FULL global row space — input rows may be sparse
        // (missing time points keep their pre-declared NULL sentinel).
        let mut global = vec![0u8; meta.total_rows() as usize * elem_sz];
        splayed_format::fill_null(&mut global, col.data_type);
        for i in 0..n {
            let gr = meta
                .global_row(&sym[i], time[i])
                .ok_or_else(|| TableError::SymTimeNotFound { row: i })? as usize;
            let src = &col.values[i * elem_sz..(i + 1) * elem_sz];
            global[gr * elem_sz..(gr + 1) * elem_sz].copy_from_slice(src);
        }
        create_field_with_data(dir.join(&col.name), col.data_type, &global)
            .map_err(TableError::CreateField)?;
    }

    Ok(meta)
}

/// In-place update of existing (SYM, TIME) cells (plan §8.4 update_table).
///
/// - Only positions present in META are updated; unknown (SYM, TIME) errors.
/// - A FIELD not present on disk errors (`FieldNotFound`); a type mismatch
///   errors (`TypeMismatch`).
pub fn update_table(
    dir: impl AsRef<Path>,
    sym: &[String],
    time: &[i64],
    columns: &[TableColumn],
) -> Result<(), TableError> {
    let dir = dir.as_ref();
    let dataset = open_dataset(dir).map_err(TableError::Dataset)?;
    let meta = &dataset.meta;

    let existing: HashSet<String> = dataset
        .list_fields()
        .map_err(TableError::Io)?
        .into_iter()
        .collect();

    let n = sym.len();
    for col in columns {
        if !existing.contains(&col.name) {
            return Err(TableError::FieldNotFound(col.name.clone()));
        }
        let expected = n * col.data_type.size_of();
        if col.values.len() != expected {
            return Err(TableError::LengthMismatch {
                field: col.name.clone(),
                expected,
                got: col.values.len(),
            });
        }

        let field_path = dir.join(&col.name);
        {
            // Verify the incoming type matches the FIELD's stored type, then
            // drop the reader before writing (Windows mmap safety).
            let reader = FieldReader::open(&field_path)
                .map_err(|e| TableError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("open field '{}' for type check: {e}", col.name),
                )))?;
            let field_dt = reader.data_type();
            if field_dt != col.data_type {
                return Err(TableError::TypeMismatch {
                    field: col.name.clone(),
                    expected: field_dt,
                    got: col.data_type,
                });
            }
            drop(reader);
        }

        // Build UpdateItems, batching consecutive global rows (V1 efficiency).
        let elem_sz = col.data_type.size_of();
        let mut values: Vec<u8> = Vec::with_capacity(n * elem_sz);
        let mut start_row: Option<u32> = None;
        let mut prev_row: Option<u32> = None;
        let mut items: Vec<UpdateItem> = Vec::new();

        for i in 0..n {
            let global_row = meta
                .global_row(&sym[i], time[i])
                .ok_or_else(|| TableError::SymTimeNotFound { row: i })?;

            if let Some(prev) = prev_row {
                if global_row == prev + 1 {
                    values.extend_from_slice(
                        &col.values[i * elem_sz..(i + 1) * elem_sz],
                    );
                    prev_row = Some(global_row);
                    continue;
                } else {
                    items.push(UpdateItem::new(
                        start_row.unwrap(),
                        std::mem::take(&mut values),
                    ));
                }
            }
            start_row = Some(global_row);
            prev_row = Some(global_row);
            values.extend_from_slice(&col.values[i * elem_sz..(i + 1) * elem_sz]);
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
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum TableError {
    DirNotEmpty,
    LengthMismatch { field: String, expected: usize, got: usize },
    SymTimeNotFound { row: usize },
    FieldNotFound(String),
    TypeMismatch { field: String, expected: DataType, got: DataType },
    Io(std::io::Error),
    Meta(splayed_format::MetaError),
    CreateField(CreateFieldError),
    UpdateField(UpdateError),
    Dataset(DatasetError),
}

impl std::fmt::Display for TableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DirNotEmpty => write!(f, "target directory is not empty"),
            Self::LengthMismatch { field, expected, got } => write!(
                f,
                "column '{field}' length {got} != expected {expected} (rows × element size)"
            ),
            Self::SymTimeNotFound { row } => {
                write!(f, "(SYM, TIME) at row {row} not found in META")
            }
            Self::FieldNotFound(name) => write!(f, "field '{name}' not found on disk"),
            Self::TypeMismatch { field, expected, got } => {
                write!(f, "type mismatch for field '{field}': expected {expected:?}, got {got:?}")
            }
            Self::Io(e) => write!(f, "table io error: {e}"),
            Self::Meta(e) => write!(f, "meta build error: {e}"),
            Self::CreateField(e) => write!(f, "create_field_with_data failed: {e}"),
            Self::UpdateField(e) => write!(f, "update_field failed: {e}"),
            Self::Dataset(e) => write!(f, "dataset open failed: {e}"),
        }
    }
}

impl std::error::Error for TableError {}