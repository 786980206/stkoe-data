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
///
/// `sorted`: a **performance hint** — when the input rows are already ordered by
/// (SYM, TIME) ascending, the scatter skips the per-row `global_row` binary
/// searches (O(n) window-cursor walk). The input is verified regardless: if it
/// turns out unsorted the general path is used, so results are always correct.
pub fn create_table(
    dir: impl AsRef<Path>,
    time_type: TimeType,
    sym: &[String],
    time: &[i64],
    columns: &[TableColumn],
    sorted: bool,
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

    // 1) .meta (MetaBuilder skips its internal sort when already ordered).
    let meta = create_meta(dir, time_type, sym, time)?;

    // 2) each FIELD written once in global-row order.
    let fast = sorted && is_sorted_by_sym_time(sym, time);
    for col in columns {
        let global = if fast {
            scatter_to_global_sorted(&meta, sym, time, col)?
        } else {
            scatter_to_global(&meta, sym, time, col)?
        };
        create_field_with_data(dir.join(&col.name), col.data_type, &global)
            .map_err(TableError::CreateField)?;
    }

    Ok(meta)
}

/// Is the input ordered by `(SYM asc, TIME asc)` lexicographically?
fn is_sorted_by_sym_time(sym: &[String], time: &[i64]) -> bool {
    for i in 0..sym.len().saturating_sub(1) {
        if sym[i] > sym[i + 1] || (sym[i] == sym[i + 1] && time[i] > time[i + 1]) {
            return false;
        }
    }
    true
}

/// Scatter an input-row-order column into global-row order.
///
/// The buffer spans the FULL global row space — input rows may be sparse
/// (missing time points keep their pre-declared NULL sentinel).
fn scatter_to_global(
    meta: &MetaFile,
    sym: &[String],
    time: &[i64],
    col: &TableColumn,
) -> Result<Vec<u8>, TableError> {
    let elem_sz = col.data_type.size_of();
    let mut global = vec![0u8; meta.total_rows() as usize * elem_sz];
    splayed_format::fill_null(&mut global, col.data_type);
    for i in 0..sym.len() {
        let gr = meta
            .global_row(&sym[i], time[i])
            .ok_or_else(|| TableError::SymTimeNotFound { row: i })? as usize;
        let src = &col.values[i * elem_sz..(i + 1) * elem_sz];
        global[gr * elem_sz..(gr + 1) * elem_sz].copy_from_slice(src);
    }
    Ok(global)
}

/// Fast-path scatter for input already ordered by `(SYM asc, TIME asc)`.
///
/// Global rows are then monotonically non-decreasing, so each row's global row
/// is found with a per-symbol binary search + an advancing TIME AXIS window
/// cursor — O(1) amortized per row instead of two binary searches each.
///
/// Semantics identical to [`scatter_to_global`] (NULL-filled gaps, input values
/// placed at their global rows). Callers must have verified sortedness.
fn scatter_to_global_sorted(
    meta: &MetaFile,
    sym: &[String],
    time: &[i64],
    col: &TableColumn,
) -> Result<Vec<u8>, TableError> {
    let elem_sz = col.data_type.size_of();
    let mut global = vec![0u8; meta.total_rows() as usize * elem_sz];
    splayed_format::fill_null(&mut global, col.data_type);

    let axis = &meta.time_axis;
    let mut sym_idx: Option<usize> = None;
    let mut window_cursor = 0usize; // advancing index into TIME AXIS

    for i in 0..sym.len() {
        let s = &sym[i];
        let t = time[i];

        // Advance to a new symbol once per group.
        if sym_idx.map_or(true, |si| meta.symbols[si] != *s) {
            let si = meta
                .symbols
                .binary_search_by(|x| x.as_str().cmp(s.as_str()))
                .map_err(|_| TableError::SymTimeNotFound { row: i })?;
            sym_idx = Some(si);
            window_cursor = meta.sym_index[si].time_start as usize;
        }

        let rec = &meta.sym_index[sym_idx.expect("set above")];
        let window_end = rec.time_start as usize + rec.time_count as usize;
        while window_cursor < window_end && axis[window_cursor] < t {
            window_cursor += 1;
        }
        if window_cursor >= window_end || axis[window_cursor] != t {
            return Err(TableError::SymTimeNotFound { row: i });
        }

        let gr = rec.row_start as usize + (window_cursor - rec.time_start as usize);
        let src = &col.values[i * elem_sz..(i + 1) * elem_sz];
        global[gr * elem_sz..(gr + 1) * elem_sz].copy_from_slice(src);
    }
    Ok(global)
}

/// In-place update of existing (SYM, TIME) cells (plan §8.4 update_table).
///
/// - Only positions present in META are updated; unknown (SYM, TIME) errors.
/// - A FIELD not present on disk: errors with `FieldNotFound` unless
///   `create_missing_fields` is set, in which case the field is **auto-created**
///   as a new column (all pre-existing rows NULL, input rows filled once).
/// - An existing FIELD with a different type errors (`TypeMismatch`).
pub fn update_table(
    dir: impl AsRef<Path>,
    sym: &[String],
    time: &[i64],
    columns: &[TableColumn],
    create_missing_fields: bool,
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
            if !create_missing_fields {
                return Err(TableError::FieldNotFound(col.name.clone()));
            }
            // New column: NULL history + this input's values (one-pass write).
            let global = scatter_to_global(meta, sym, time, col)?;
            create_field_with_data(dir.join(&col.name), col.data_type, &global)
                .map_err(TableError::CreateField)?;
            continue;
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