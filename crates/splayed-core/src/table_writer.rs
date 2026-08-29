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
// update_meta：以新的 (SYM, TIME) 布局重建 .meta，并**并发**把全部现有
// FIELD 文件重散布到新布局（原子提交，generation 屏障）。
// ---------------------------------------------------------------------------

/// 用新的 (SYM, TIME) 布局重写 `.meta`，并把**全部现有 FIELD 文件**并发
/// 重散布到新布局——语义与 `create_meta` 一致（`sym`/`time` 为逐行对齐数组），
/// 区别：
///
/// - 新布局第 g 行 = (新 sym, 新 time)：旧数据中同 (SYM, TIME) 的值被搬运
///   （gather），旧数据没有的格子填 NULL；FIELD 保持**同名**、重写为
///   PLAIN+NONE（可写），并重算 `null_count` 与统计 footer；
/// - generation 递增；`.meta` 用「写 `.meta.new` → fsync → rename」原子提交；
///   字段以 `name.tmp` 写入并 fsync 后逐个 rename——提交点之前，Reader 会因
///   generation 不匹配拒绝中间态（§8.4 不变量 5）；失败后**重跑本函数即可
///   完成提交**（重散布是确定性的、幂等）。
/// - 并行：字段彼此独立，`std::thread::scope` 每字段一线程并发重写；
///   gather 连续段走一次 `memcpy`（避免逐行 IO）。
///
/// `time_type` 允许变化（TIME 不落 FIELD 文件）。
///
/// ⚠ Windows：被 mmap 的字段无法 rename——调用方须先 drop 所有打开中的
/// `FieldReader` / provider / 连接再调用（与现有写路径一致）。
pub fn update_meta(
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

    // 1) 旧 meta（完整读入，不保持 mmap）。
    let meta_path = dir.join(META_FILE_NAME);
    let old_bytes = fs::read(&meta_path).map_err(TableError::Io)?;
    let old = MetaFile::deserialize(&old_bytes).map_err(TableError::Meta)?;

    // 2) 新 meta（generation 递增）；串行构建 gather 映射。
    let next_gen = old.header.generation.wrapping_add(1);
    let mut builder = MetaBuilder::new(time_type, next_gen);
    for i in 0..sym.len() {
        builder.add(&sym[i], time[i]);
    }
    let new_meta = builder.build().map_err(TableError::Meta)?;
    let gather = build_gather_map(&old, &new_meta);
    let n_rows = new_meta.total_rows() as usize;

    // 3) 先落 `.meta.new`（fsync，不 rename——提交点在最后）。
    let tmp_meta = dir.join(format!("{META_FILE_NAME}.new"));
    {
        use std::io::Write;
        let bytes = new_meta.serialize();
        let mut file = fs::File::create(&tmp_meta).map_err(TableError::Io)?;
        file.write_all(&bytes).map_err(TableError::Io)?;
        file.sync_all().map_err(TableError::Io)?;
    }

    // 4) 并发重写全部现有 FIELD（字段名 = 目录下除 .meta 外的文件）。
    let field_names = list_field_names(dir)?;
    if let Some(e) = rewrite_fields_concurrent(dir, &field_names, &gather, n_rows, next_gen) {
        // 失败：清理 .meta.new，保留旧 meta（重跑可完成提交）。
        let _ = fs::remove_file(&tmp_meta);
        return Err(e);
    }

    // 5) 提交点：rename `.meta.new` → `.meta`。
    fs::rename(&tmp_meta, &meta_path).map_err(TableError::Io)?;
    Ok(new_meta)
}

/// 目录下非 `.meta`（且非临时文件）的文件名 = 现有 FIELD 名。
fn list_field_names(dir: &Path) -> Result<Vec<String>, TableError> {
    let mut names = Vec::new();
    for entry in fs::read_dir(dir).map_err(TableError::Io)? {
        let entry = entry.map_err(TableError::Io)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == META_FILE_NAME || name.ends_with(".new") || name.ends_with(".tmp") {
            continue;
        }
        names.push(name);
    }
    names.sort();
    Ok(names)
}

/// 新布局每行 g → 旧布局行号（`None` = 旧数据无此 (SYM, TIME)）。
///
/// 旧查找：sym → SYM INDEX 记录；time 在该记录的 time_axis 区间内二分。
/// 新布局行号按 `MetaBuilder` 全局行 = SYM 升序 × TIME 升序。
fn build_gather_map(old: &MetaFile, new: &MetaFile) -> Vec<Option<u32>> {
    let old_sym_idx: std::collections::HashMap<&str, usize> = old
        .symbols
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();

    let mut gather = Vec::with_capacity(new.total_rows() as usize);
    for (si, rec) in new.sym_index.iter().enumerate() {
        let sym = new.symbols[si].as_str();
        let lo = rec.time_start as usize;
        // 该 SYM 的新 time 点序列。
        let old_row = old_sym_idx.get(sym).and_then(|&osi| {
            let orec = &old.sym_index[osi];
            let olo = orec.time_start as usize;
            let ohi = olo + orec.time_count as usize;
            Some((orec, olo, ohi))
        });
        for local in 0..rec.time_count as usize {
            let t = new.time_axis[lo + local];
            match old_row {
                Some((orec, olo, ohi)) => {
                    let pos = old.time_axis[olo..ohi].binary_search(&t).ok();
                    gather.push(pos.map(|p| orec.row_start + p as u32));
                }
                None => gather.push(None),
            }
        }
    }
    gather
}

/// 每字段一线程并发重写：读旧字段 → 按 gather 复制/填 NULL → 写 `name.tmp`
/// （新 generation、真实 null_count、统计 footer）→ fsync → rename。
///
/// 返回第一个错误（`None` = 全部成功）。确定性：失败后重跑 update_meta
/// 会产出相同的字段文件，可完成提交。
fn rewrite_fields_concurrent(
    dir: &Path,
    field_names: &[String],
    gather: &[Option<u32>],
    n_rows: usize,
    generation: u64,
) -> Option<TableError> {
    let result = std::thread::scope(|scope| {
        let handles: Vec<_> = field_names
            .iter()
            .map(|name| {
                scope.spawn(move || {
                    rewrite_field_to_layout(dir, name.as_str(), gather, n_rows, generation)
                })
            })
            .collect();

        let mut first_err: Option<TableError> = None;
        for h in handles {
            if first_err.is_some() {
                continue; // 已经拿到错误，其余线程继续收尾
            }
            match h.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => first_err = Some(e),
                Err(_) => {
                    first_err = Some(TableError::FieldThread(
                        "field rewrite thread panicked".to_string(),
                    ))
                }
            }
        }
        first_err
    });
    result
}

/// 单个字段：按 gather 把旧值搬运到新布局行并写 `name.tmp` + rename。
fn rewrite_field_to_layout(
    dir: &Path,
    name: &str,
    gather: &[Option<u32>],
    n_rows: usize,
    generation: u64,
) -> Result<(), TableError> {
    use std::io::Write;

    let path = dir.join(name);
    let reader = FieldReader::open(&path).map_err(TableError::Reader)?;
    let dt = reader.data_type();
    let w = dt.size_of();
    let nulls = dt.null_bytes();

    // Gather 到新布局（连续段一次 memcpy 优化）。
    let mut data = vec![0u8; n_rows * w];
    let mut g = 0usize;
    while g < n_rows {
        match gather[g] {
            Some(ro) => {
                let mut run = 1usize;
                while g + run < n_rows {
                    match gather[g + run] {
                        Some(ro2) if ro2 == ro + run as u32 => run += 1,
                        _ => break,
                    }
                }
                let raw = reader
                    .read_range_raw(ro, run)
                    .map_err(TableError::Reader)?;
                data[g * w..(g + run) * w].copy_from_slice(raw);
                g += run;
            }
            None => {
                data[g * w..(g + 1) * w].copy_from_slice(nulls);
                g += 1;
            }
        }
    }

    // 真实 NULL 计数 = 数据中哨兵位型行数（含从旧值搬运来的 NULL）。
    let null_count = data
        .chunks(w)
        .filter(|c| *c == nulls)
        .count() as u32;

    // Windows：先释放 mmap 才能 rename。
    drop(reader);

    // 统计 footer（按重散布后的数据重算）。
    let stats = splayed_format::field_footer::encode_footer(
        splayed_format::field_footer::compute_stats(dt, &data),
    );

    let tmp_path = path.with_extension("tmp");
    {
        let mut header = splayed_format::header::FieldHeader::new(
            dt,
            splayed_format::header::Encoding::Plain,
            splayed_format::header::Compression::None,
            generation,
            n_rows as u32,
            (n_rows * w) as u64,
        );
        header.null_count = null_count; // 真实 NULL 计数（区别于预分配语义）
        let mut file = fs::File::create(&tmp_path).map_err(TableError::Io)?;
        file.write_all(bytemuck::bytes_of(&header))
            .map_err(TableError::Io)?;
        file.write_all(&data).map_err(TableError::Io)?;
        file.write_all(&stats).map_err(TableError::Io)?;
        file.sync_all().map_err(TableError::Io)?;
    }
    fs::rename(&tmp_path, &path).map_err(TableError::Io)?;
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
    Reader(crate::ReaderError),
    /// Field 重写线程崩溃/聚合失败。
    FieldThread(String),
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
            Self::Reader(e) => write!(f, "field reader error: {e}"),
            Self::FieldThread(e) => write!(f, "field rewrite thread failed: {e}"),
        }
    }
}

impl std::error::Error for TableError {}