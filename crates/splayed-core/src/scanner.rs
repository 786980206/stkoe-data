//! Scanner — the self-built core scan engine.
//!
//! See `plan.md` §9 (Scanner API 与执行流程).
//!
//! Execution flow:
//! ```text
//! ScanRequest
//!   → META lookup (SYM pruning + TIME pruning)
//!   → row ranges
//!   → projection pruning
//!   → FIELD Reader (mmap / read)
//!   → filter
//!   → Batch
//! ```
//!
//! Key: `WHERE sym = ...` is NOT a scan-then-filter — it's a direct META → row
//! range lookup.  This is the performance-critical design.

use splayed_format::{DataType, RawValue, TimeType};

use std::sync::Arc;

use crate::batch::{
    Bitmap, Buffer, CoreBatch, CoreColumn, CoreField, CoreSchema, CoreStringDict, CoreTimeUnit,
    CoreType,
};
use crate::dataset::Dataset;
use crate::reader::FieldReader;
use crate::ColumnView;

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

/// Symbol selection: either all symbols, or a specific set.
#[derive(Debug, Clone)]
pub enum SymbolSelection {
    All,
    Symbols(Vec<String>),
}

impl SymbolSelection {
    pub fn all() -> Self {
        Self::All
    }
    pub fn syms<I, S>(syms: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Symbols(syms.into_iter().map(Into::into).collect())
    }
}

/// Time range filter `[start, end)`.  Time values are i64: for DATE32 it's
/// days since epoch, for TIMESTAMP_US it's microseconds.
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start: i64,
    pub end: i64,
}

impl TimeRange {
    /// Unbounded (covers all time).
    pub fn all() -> Self {
        Self {
            start: i64::MIN,
            end: i64::MAX,
        }
    }

    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }

    #[inline]
    pub fn contains(&self, t: i64) -> bool {
        t >= self.start && t < self.end
    }
}

/// A value-level filter predicate on a specific field column.
#[derive(Debug, Clone)]
pub enum Filter {
    /// `field > value`
    GreaterThan { field: String, value: FilterValue },
    /// `field >= value`
    GreaterOrEqual { field: String, value: FilterValue },
    /// `field < value`
    LessThan { field: String, value: FilterValue },
    /// `field <= value`
    LessOrEqual { field: String, value: FilterValue },
    /// `field == value`
    Equal { field: String, value: FilterValue },
    /// `field != value`
    NotEqual { field: String, value: FilterValue },
    /// `field IS NULL` — matches the type's NULL sentinel (null cells only).
    IsNull { field: String },
    /// `field IS NOT NULL` — matches non-NULL cells.
    IsNotNull { field: String },
}

/// A comparison value for filtering.  Typed to match the field's DataType.
#[derive(Debug, Clone)]
pub enum FilterValue {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Date32(i32),
    Date64(i64),
    TimestampUs(i64),
    /// 字符串字面量（声明式分区列 / 未来字符串字段；数值列对比返回不匹配）。
    String(String),
}

/// A scan request.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    pub columns: Vec<String>,
    pub symbols: SymbolSelection,
    pub time_range: TimeRange,
    /// Conjunctive value-level filters applied during the scan (AND semantics).
    pub filters: Vec<Filter>,
    pub batch_size: usize,
    pub parallelism: usize,
    /// 读取期截断：产出前 N 个**通过过滤**的行后停止扫描（`None` = 不限）。
    pub limit: Option<usize>,
}

impl ScanRequest {
    /// Create a scan request with default settings.
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            symbols: SymbolSelection::All,
            time_range: TimeRange::all(),
            filters: vec![],
            batch_size: 65536,
            parallelism: 1,
            limit: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Scan result: row ranges + column views
// ---------------------------------------------------------------------------

/// A resolved row range for a single SYM: `[row_start, row_start + count)`
/// in the global FIELD row space.
#[derive(Debug, Clone, Copy)]
pub struct RowRange {
    pub row_start: u32,
    pub count: u32,
    /// The symbol this range belongs to (for SYM column reconstruction).
    pub sym_idx: usize,
    /// The time axis indices this range covers: `[time_start_idx, time_start_idx + count)`.
    pub time_start_idx: u32,
}

// ---------------------------------------------------------------------------
// Scanner
// ---------------------------------------------------------------------------

/// The scanner over an open dataset.
pub struct Scanner<'ds> {
    dataset: &'ds Dataset,
}

/// The resolved scan plan: after META pruning, which row ranges to read.
pub struct ScanPlan {
    pub ranges: Vec<RowRange>,
    pub columns: Vec<String>,
    pub total_rows: usize,
}

impl<'ds> Scanner<'ds> {
    pub fn new(dataset: &'ds Dataset) -> Self {
        Self { dataset }
    }

    /// Phase 1: META lookup — resolve which row ranges to read.
    ///
    /// SYM pruning: only keep SYMs matching `symbols`.
    /// TIME pruning: for each SYM, find the intersection of its time interval
    ///   with `time_range` and compute the resulting row sub-range.
    pub fn plan(&self, request: &ScanRequest) -> Result<ScanPlan, ScannerError> {
        let meta = &self.dataset.meta;

        // --- SYM pruning ---
        let sym_indices: Vec<usize> = match &request.symbols {
            SymbolSelection::All => (0..meta.symbols.len()).collect(),
            SymbolSelection::Symbols(syms) => {
                let mut out = Vec::with_capacity(syms.len());
                for s in syms {
                    match meta.find_symbol(s) {
                        Some(idx) => out.push(idx),
                        None => return Err(ScannerError::SymbolNotFound(s.clone())),
                    }
                }
                out
            }
        };

        // --- TIME pruning + row range computation ---
        let mut ranges = Vec::new();
        let mut total_rows = 0usize;

        for sym_idx in sym_indices {
            let rec = &meta.sym_index[sym_idx];
            let time_count = rec.time_count as usize;

            // This SYM occupies time axis indices [time_start, time_start + time_count).
            // Find the intersection with [time_range.start, time_range.end).
            let sym_time_start_idx = rec.time_start as usize;

            // Binary search for the start of the time range within this SYM's interval.
            let interval_start = sym_time_start_idx;
            let interval_end = sym_time_start_idx + time_count;

            // Find the first time axis index >= request.time_range.start
            // that falls within [interval_start, interval_end).
            let range_start_idx = if request.time_range.start == i64::MIN {
                interval_start
            } else {
                // Binary search in time_axis for start
                match meta.time_axis.binary_search(&request.time_range.start) {
                    Ok(idx) => idx.max(interval_start),
                    Err(idx) => {
                        // idx is the insertion point; time_axis[idx] >= start
                        if idx >= interval_end {
                            continue; // no overlap
                        }
                        idx.max(interval_start)
                    }
                }
            };

            // Find the last time axis index < request.time_range.end
            let range_end_idx = if request.time_range.end == i64::MAX {
                interval_end
            } else {
                match meta.time_axis.binary_search(&request.time_range.end) {
                    Ok(idx) => idx.min(interval_end),
                    Err(idx) => idx.min(interval_end),
                }
            };

            if range_end_idx <= range_start_idx {
                continue; // empty range
            }

            let count = (range_end_idx - range_start_idx) as u32;
            let row_start = rec.row_start + (range_start_idx as u32 - rec.time_start);

            ranges.push(RowRange {
                row_start,
                count,
                sym_idx,
                time_start_idx: range_start_idx as u32,
            });
            total_rows += count as usize;
        }

        // 统计剪裁（FIELD footer 的 min/max）：任一 filter 与整列统计不相交
        // → 整表无匹配行，直接空计划（省去全部 FIELD 读取与解码）。
        //
        // 注意：IsNull/IsNotNull 不做计划级剪裁——header 的 null_count 是
        // 「预分配全 NULL」语义（§5.2.1），写路径不维护，交由行级过滤保证。
        for f in &request.filters {
            let name = f.field_name();
            let path = self.dataset.field_path(name);
            if !path.exists() {
                return Err(ScannerError::FieldNotFound(name.to_string())); // 审查期防御
            }
            let reader = FieldReader::open(&path)
                .map_err(|e| ScannerError::ReaderError(name.to_string(), e))?;
            let pruned = if let Some(st) = reader.stats() {
                let dt = reader.data_type();
                let min_v = RawValue::read_le(&st.min, 0, dt);
                let max_v = RawValue::read_le(&st.max, 0, dt);
                !matches_range(f, &min_v, &max_v)
            } else {
                false
            };
            if pruned {
                return Ok(ScanPlan {
                    ranges: Vec::new(),
                    columns: request.columns.clone(),
                    total_rows: 0,
                });
            }
        }

        Ok(ScanPlan {
            ranges,
            columns: request.columns.clone(),
            total_rows,
        })
    }

    /// Phase 2: Execute the scan plan — read FIELD data and produce batches.
    ///
    /// For each batch, reads `batch_size` rows across all requested columns.
    /// Filter is applied after reading (plan §9.3).
    ///
    /// Returns a `ScanBatches` iterator that yields `ScanBatch` items.
    pub fn scan<'r>(
        &self,
        plan: &'r ScanPlan,
        request: &ScanRequest,
    ) -> Result<ScanBatches<'_, 'r>, ScannerError> {
        // Verify all requested columns exist and open their readers.
        let mut readers: Vec<(String, FieldReader)> = Vec::with_capacity(plan.columns.len());
        for col_name in &plan.columns {
            let path = self.dataset.field_path(col_name);
            if !path.exists() {
                return Err(ScannerError::FieldNotFound(col_name.clone()));
            }
            let reader = FieldReader::open(&path)
                .map_err(|e| ScannerError::ReaderError(col_name.clone(), e))?;
            readers.push((col_name.clone(), reader));
        }

        // V1: every filter field must be in the projection (verified at scan time).
        // If there's a filter on a field not in the projection, it will error
        // during next_batch with FilterFieldNotProjected.

        let schema = scan_schema(self.dataset.meta.time_type(), &readers);
        let sym_dict = Arc::new(CoreStringDict::new(
            self.dataset.meta.symbols.clone(),
            true,
        ));

        Ok(ScanBatches {
            dataset: self.dataset,
            ranges: &plan.ranges,
            readers,
            filters: request.filters.clone(),
            batch_size: request.batch_size,
            limit: request.limit,
            emitted: 0,
            current_range_idx: 0,
            current_row_in_range: 0,
            exhausted: false,
            schema,
            sym_dict,
        })
    }

    /// Execute the scan plan with cross-SYM parallelism.
    ///
    /// Splits the row ranges into `request.parallelism` partitions at SYM
    /// boundaries, then reads each partition in a separate thread. Returns
    /// all batches collected from all threads, preserving SYM order.
    ///
    /// For `parallelism = 1`, falls through to the single-threaded path.
    pub fn scan_all_parallel(
        &self,
        plan: &ScanPlan,
        request: &ScanRequest,
    ) -> Result<Vec<CoreBatch>, ScannerError> {
        if request.parallelism <= 1 || plan.ranges.len() <= 1 {
            // Single-threaded path: use the existing iterator.
            let mut batches = self.scan(plan, request)?;
            let mut out = Vec::new();
            while let Some(batch) = batches.next_batch()? {
                out.push(batch);
            }
            return Ok(out);
        }

        // Partition ranges across threads (clone to make them 'static/Send).
        let n_threads = request.parallelism.min(plan.ranges.len());
        let chunk_size = plan.ranges.len().div_ceil(n_threads);
        let range_chunks: Vec<Vec<RowRange>> = plan
            .ranges
            .chunks(chunk_size)
            .map(|c| c.to_vec())
            .collect();

        // Prepare column paths (Send-safe: paths are just PathBuf).
        let col_paths: Vec<(String, std::path::PathBuf)> = plan
            .columns
            .iter()
            .map(|name| (name.clone(), self.dataset.field_path(name)))
            .collect();

        let filters = request.filters.clone();
        let batch_size = request.batch_size;
        // Wrap in Arc to avoid cloning the full time_axis per thread (P2).
        let time_axis: Arc<[i64]> = Arc::from(self.dataset.meta.time_axis.as_slice());

        let time_type = self.dataset.meta.time_type();
        let sym_dict = Arc::new(CoreStringDict::new(
            self.dataset.meta.symbols.clone(),
            true,
        ));

        // Spawn threads.
        let handles: Vec<_> = range_chunks
            .into_iter()
            .map(|chunk| {
                let col_paths = col_paths.clone();
                let filters = filters.clone();
                let time_axis = Arc::clone(&time_axis); // cheap ref-count clone
                let sym_dict = Arc::clone(&sym_dict);

                std::thread::spawn(move || -> Result<Vec<CoreBatch>, ScannerError> {
                    // Open readers in this thread.
                    let mut readers: Vec<(String, FieldReader)> = Vec::with_capacity(col_paths.len());
                    for (name, path) in &col_paths {
                        let reader = FieldReader::open(path)
                            .map_err(|e| ScannerError::ReaderError(name.clone(), e))?;
                        readers.push((name.clone(), reader));
                    }

                    let schema = scan_schema(time_type, &readers);

                    let mut out = Vec::new();
                    for range in chunk {
                        let mut current_row = 0u32;
                        let remaining = range.count;

                        while current_row < remaining {
                            let to_read = remaining - current_row;
                            let to_read = to_read.min(batch_size as u32) as usize;

                            let start_row = range.row_start + current_row;

                            // Read each column.
                            let mut col_data: Vec<(String, Vec<u8>, DataType)> =
                                Vec::with_capacity(readers.len());
                            let mut has_nulls: Vec<bool> = Vec::with_capacity(readers.len());
                            for (name, reader) in &readers {
                                let dt = reader.data_type();
                                let raw = reader
                                    .read_range_raw(start_row, to_read)
                                    .map_err(|e| ScannerError::ReaderError(name.clone(), e))?;
                                col_data.push((name.clone(), raw.to_vec(), dt));
                                has_nulls.push(reader.header().null_count > 0);
                            }

                            // Fill sym_indices and time_values.
                            let mut sym_indices = Vec::with_capacity(to_read);
                            let mut time_values = Vec::with_capacity(to_read);
                            for i in 0..to_read {
                                sym_indices.push(range.sym_idx);
                                let time_idx = range.time_start_idx as usize
                                    + current_row as usize
                                    + i;
                                if time_idx < time_axis.len() {
                                    time_values.push(time_axis[time_idx]);
                                } else {
                                    time_values.push(0);
                                }
                            }

                            let mut row_count = to_read;

                            // Apply all pushed-down value filters (shared with next_batch).
                            let (new_data, new_count) = apply_filters(
                                col_data,
                                &mut sym_indices,
                                &mut time_values,
                                &filters,
                                row_count,
                            )?;
                            col_data = new_data;
                            row_count = new_count;

                            if row_count > 0 {
                                let fields = col_data
                                    .into_iter()
                                    .zip(has_nulls)
                                    .map(|((name, bytes, ty), hn)| (name, bytes, ty, hn))
                                    .collect();
                                out.push(build_core_batch(
                                    &schema,
                                    &sym_dict,
                                    time_type,
                                    fields,
                                    &sym_indices,
                                    &time_values,
                                ));
                            }

                            current_row += to_read as u32;
                        }
                    }
                    Ok(out)
                })
            })
            .collect();

        // Collect results in order.
        let mut all_batches = Vec::new();
        for handle in handles {
            let batches = handle
                .join()
                .map_err(|_| ScannerError::ParallelScanPanic)?;
            all_batches.extend(batches?);
        }

        // 读取期截断（限值内切片，超出即停）。
        if let Some(lim) = request.limit {
            let mut total = 0usize;
            let mut out = Vec::new();
            for b in all_batches {
                let keep = lim.saturating_sub(total);
                if keep == 0 {
                    break;
                }
                if b.num_rows() > keep {
                    out.push(b.slice(0, keep));
                    break;
                }
                total += b.num_rows();
                out.push(b);
            }
            return Ok(out);
        }

        Ok(all_batches)
    }
}

// ---------------------------------------------------------------------------
// ScanBatches — lazy batch iterator
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// CoreBatch construction
// ---------------------------------------------------------------------------

/// Build the in-memory scan schema: [time, sym, requested fields...].
fn scan_schema(
    time_type: TimeType,
    readers: &[(String, FieldReader)],
) -> Arc<CoreSchema> {
    let time_ty = match time_type {
        TimeType::Date32 => CoreType::Date32,
        TimeType::TimestampUs => CoreType::Timestamp(CoreTimeUnit::Microsecond, None),
    };
    let mut fields = vec![
        CoreField::new("time", time_ty, false),
        CoreField::new("sym", CoreType::Utf8, false),
    ];
    for (name, reader) in readers {
        fields.push(CoreField::new(
            name.clone(),
            CoreType::from_disk(reader.data_type()),
            true,
        ));
    }
    Arc::new(CoreSchema::new(fields))
}

/// Scan `data` for NULL-sentinel bit patterns; returns a validity bitmap only
/// when at least one NULL was found (data buffer itself is used as-is).
fn build_validity(ty: DataType, data: &[u8], row_count: usize) -> Option<Bitmap> {
    let null_pat = ty.null_bytes();
    let sz = ty.size_of();
    let mut bm = Bitmap::with_all_valid(row_count);
    let mut found = false;
    for i in 0..row_count {
        let off = i * sz;
        if &data[off..off + sz] == null_pat {
            bm.set(i, false);
            found = true;
        }
    }
    found.then_some(bm)
}

/// Build a [`CoreBatch`] from filtered scan parts. Column order is
/// `[time, sym, fields...]`; `sym_indices`/`time_values` are per-row and
/// already filter-compacted.
fn build_core_batch(
    schema: &Arc<CoreSchema>,
    sym_dict: &Arc<CoreStringDict>,
    time_type: TimeType,
    fields: Vec<(String, Vec<u8>, DataType, bool)>,
    sym_indices: &[usize],
    time_values: &[i64],
) -> CoreBatch {
    let row_count = sym_indices.len();

    // TIME column — typed for zero-copy (Date32: i32, TimestampUs: i64).
    let mut time_buf = Buffer::with_capacity(row_count * 8);
    match time_type {
        TimeType::Date32 => {
            for t in time_values {
                time_buf.extend_from_slice(&(*t as i32).to_le_bytes());
            }
        }
        TimeType::TimestampUs => {
            for t in time_values {
                time_buf.extend_from_slice(&t.to_le_bytes());
            }
        }
    }
    let time_col = CoreColumn::primitive(
        CoreType::time_from_disk_meta(time_type),
        time_buf,
        None,
    );

    // SYM column — dictionary-encoded (u32 indices into the shared dict).
    let mut idx_buf = Buffer::with_capacity(row_count * 4);
    for &si in sym_indices {
        idx_buf.extend_from_slice(&(si as u32).to_le_bytes());
    }
    let sym_col = CoreColumn::dictionary(idx_buf, Arc::clone(sym_dict));

    // FIELD columns — raw bytes as-is + validity bitmap (skipped when the
    // FIELD header reported zero NULLs).
    let mut columns = Vec::with_capacity(2 + fields.len());
    columns.push(time_col);
    columns.push(sym_col);
    for (_, bytes, ty, has_nulls) in fields {
        let nulls = if has_nulls {
            build_validity(ty, &bytes, row_count)
        } else {
            None
        };
        columns.push(CoreColumn::primitive(
            CoreType::from_disk(ty),
            Buffer::from_vec(bytes),
            nulls,
        ));
    }

    CoreBatch::new(Arc::clone(schema), columns, row_count)
}

/// An iterator that lazily produces `CoreBatch` items.
///
/// For each `RowRange` in the plan, reads `batch_size` rows at a time across
/// all requested column readers.  Filter is applied row-by-row after reading.
pub struct ScanBatches<'ds, 'r> {
    dataset: &'ds Dataset,
    ranges: &'r [RowRange],
    readers: Vec<(String, FieldReader)>,
    filters: Vec<Filter>,
    batch_size: usize,
    limit: Option<usize>,
    emitted: usize,
    current_range_idx: usize,
    current_row_in_range: u32,
    exhausted: bool,
    schema: Arc<CoreSchema>,
    sym_dict: Arc<CoreStringDict>,
}

impl<'ds, 'r> ScanBatches<'ds, 'r> {
    /// Get the next batch of rows, or `None` if exhausted.
    pub fn next_batch(&mut self) -> Result<Option<CoreBatch>, ScannerError> {
        if self.exhausted {
            return Ok(None);
        }

        // Accumulate column data per column (concatenated across ranges).
        let mut col_bufs: Vec<Vec<u8>> = vec![Vec::new(); self.readers.len()];
        let mut sym_indices: Vec<usize> = Vec::new();
        let mut time_values: Vec<i64> = Vec::new();
        let mut row_count = 0usize;

        let batch_capacity = self.batch_size;

        while row_count < batch_capacity && self.current_range_idx < self.ranges.len() {
            let range = self.ranges[self.current_range_idx];
            let remaining_in_range = range.count - self.current_row_in_range;
            if remaining_in_range == 0 {
                self.current_range_idx += 1;
                self.current_row_in_range = 0;
                continue;
            }

            let to_read = remaining_in_range
                .min((batch_capacity - row_count) as u32)
                as usize;

            // Read `to_read` rows from each column reader and append.
            let start_row = range.row_start + self.current_row_in_range;
            for (i, (_, reader)) in self.readers.iter().enumerate() {
                let raw = reader
                    .read_range_raw(start_row, to_read)
                    .map_err(|e| ScannerError::ReaderError(self.readers[i].0.clone(), e))?;
                col_bufs[i].extend_from_slice(raw);
            }

            // Fill sym_indices and time_values for these rows.
            let meta = &self.dataset.meta;
            for i in 0..to_read {
                sym_indices.push(range.sym_idx);
                let time_idx = range.time_start_idx as usize + self.current_row_in_range as usize + i;
                if time_idx < meta.time_axis.len() {
                    time_values.push(meta.time_axis[time_idx]);
                } else {
                    time_values.push(0);
                }
            }

            row_count += to_read;
            self.current_row_in_range += to_read as u32;
        }

        if row_count == 0 {
            self.exhausted = true;
            return Ok(None);
        }

        // Build col_data from accumulated buffers (3-tuples; null hints kept
        // separately in column order).
        let col_data: Vec<(String, Vec<u8>, DataType)> = self
            .readers
            .iter()
            .zip(col_bufs)
            .map(|((name, reader), buf)| (name.clone(), buf, reader.data_type()))
            .collect();
        let has_nulls: Vec<bool> = self
            .readers
            .iter()
            .map(|(_, reader)| reader.header().null_count > 0)
            .collect();

        // Apply all pushed-down value filters (AND semantics; shared with
        // scan_all_parallel and scan_owned for identical behavior).
        let (col_data, row_count) = apply_filters(
            col_data,
            &mut sym_indices,
            &mut time_values,
            &self.filters,
            row_count,
        )?;

        self.exhausted = self.current_range_idx >= self.ranges.len();
        if row_count == 0 && self.exhausted {
            return Ok(None);
        }

        let fields = col_data
            .into_iter()
            .zip(has_nulls)
            .map(|((name, bytes, ty), hn)| (name, bytes, ty, hn))
            .collect();

        let truncated = truncate_batch(
            build_core_batch(
                &self.schema,
                &self.sym_dict,
                self.dataset.meta.time_type(),
                fields,
                &sym_indices,
                &time_values,
            ),
            &mut self.emitted,
            self.limit,
            &mut self.exhausted,
        )?;
        Ok(truncated)
    }
}

// ---------------------------------------------------------------------------
// Owned scan iterator (for cross-thread / async streaming)
// ---------------------------------------------------------------------------

/// An owned, `Send + 'static` scan batch iterator over an `Arc<Dataset>`.
///
/// DataFusion's provider needs a stream that can cross `spawn_blocking`
/// boundaries, so this owns the dataset, row ranges, readers, and filters —
/// it has no borrows and can be moved into a blocking task.
pub struct OwnedScanBatches {
    dataset: Arc<Dataset>,
    ranges: Vec<RowRange>,
    readers: Vec<(String, FieldReader)>,
    filters: Vec<Filter>,
    batch_size: usize,
    limit: Option<usize>,
    emitted: usize,
    current_range_idx: usize,
    current_row_in_range: u32,
    exhausted: bool,
    schema: Arc<CoreSchema>,
    sym_dict: Arc<CoreStringDict>,
}

/// Create an owned scan iterator over an `Arc<Dataset>` (no borrows).
///
/// This is the async-streaming entry point used by the DataFusion provider.
/// The returned iterator shares the exact batch/filter semantics of
/// [`ScanBatches`].
pub fn scan_owned(
    dataset: Arc<Dataset>,
    plan: &ScanPlan,
    request: &ScanRequest,
) -> Result<OwnedScanBatches, ScannerError> {
    // Verify all requested columns exist and open their readers.
    let mut readers: Vec<(String, FieldReader)> = Vec::with_capacity(plan.columns.len());
    for col_name in &plan.columns {
        let path = dataset.field_path(col_name);
        if !path.exists() {
            return Err(ScannerError::FieldNotFound(col_name.clone()));
        }
        let reader = FieldReader::open(&path)
            .map_err(|e| ScannerError::ReaderError(col_name.clone(), e))?;
        readers.push((col_name.clone(), reader));
    }

    let schema = scan_schema(dataset.meta.time_type(), &readers);
    let sym_dict = Arc::new(CoreStringDict::new(
        dataset.meta.symbols.clone(),
        true,
    ));

    Ok(OwnedScanBatches {
        dataset,
        ranges: plan.ranges.clone(),
        readers,
        filters: request.filters.clone(),
        batch_size: request.batch_size,
        limit: request.limit,
        emitted: 0,
        current_range_idx: 0,
        current_row_in_range: 0,
        exhausted: false,
        schema,
        sym_dict,
    })
}

impl OwnedScanBatches {
    /// Get the next batch of rows, or `None` if exhausted.
    ///
    /// Identical semantics to [`ScanBatches::next_batch`], but with owned
    /// state so the iterator is `Send + 'static`.
    pub fn next_batch(&mut self) -> Result<Option<CoreBatch>, ScannerError> {
        if self.exhausted {
            return Ok(None);
        }

        // Accumulate column data per column (concatenated across ranges).
        let mut col_bufs: Vec<Vec<u8>> = vec![Vec::new(); self.readers.len()];
        let mut sym_indices: Vec<usize> = Vec::new();
        let mut time_values: Vec<i64> = Vec::new();
        let mut row_count = 0usize;

        let batch_capacity = self.batch_size;

        while row_count < batch_capacity && self.current_range_idx < self.ranges.len() {
            let range = self.ranges[self.current_range_idx];
            let remaining_in_range = range.count - self.current_row_in_range;
            if remaining_in_range == 0 {
                self.current_range_idx += 1;
                self.current_row_in_range = 0;
                continue;
            }

            let to_read = remaining_in_range
                .min((batch_capacity - row_count) as u32)
                as usize;

            // Read `to_read` rows from each column reader and append.
            let start_row = range.row_start + self.current_row_in_range;
            for (i, (_, reader)) in self.readers.iter().enumerate() {
                let raw = reader
                    .read_range_raw(start_row, to_read)
                    .map_err(|e| ScannerError::ReaderError(self.readers[i].0.clone(), e))?;
                col_bufs[i].extend_from_slice(raw);
            }

            // Fill sym_indices and time_values for these rows.
            let meta = &self.dataset.meta;
            for i in 0..to_read {
                sym_indices.push(range.sym_idx);
                let time_idx = range.time_start_idx as usize + self.current_row_in_range as usize + i;
                if time_idx < meta.time_axis.len() {
                    time_values.push(meta.time_axis[time_idx]);
                } else {
                    time_values.push(0);
                }
            }

            row_count += to_read;
            self.current_row_in_range += to_read as u32;
        }

        if row_count == 0 {
            self.exhausted = true;
            return Ok(None);
        }

        // Build col_data from accumulated buffers (3-tuples).
        let col_data: Vec<(String, Vec<u8>, DataType)> = self
            .readers
            .iter()
            .zip(col_bufs)
            .map(|((name, reader), buf)| (name.clone(), buf, reader.data_type()))
            .collect();
        let has_nulls: Vec<bool> = self
            .readers
            .iter()
            .map(|(_, reader)| reader.header().null_count > 0)
            .collect();

        let (col_data, row_count) = apply_filters(
            col_data,
            &mut sym_indices,
            &mut time_values,
            &self.filters,
            row_count,
        )?;

        self.exhausted = self.current_range_idx >= self.ranges.len();
        if row_count == 0 && self.exhausted {
            return Ok(None);
        }

        let fields = col_data
            .into_iter()
            .zip(has_nulls)
            .map(|((name, bytes, ty), hn)| (name, bytes, ty, hn))
            .collect();

        let truncated = truncate_batch(
            build_core_batch(
                &self.schema,
                &self.sym_dict,
                self.dataset.meta.time_type(),
                fields,
                &sym_indices,
                &time_values,
            ),
            &mut self.emitted,
            self.limit,
            &mut self.exhausted,
        )?;
        Ok(truncated)
    }
}

// ---------------------------------------------------------------------------
// Range partitioning + parallel streaming scan
// ---------------------------------------------------------------------------

/// Split one range into pieces of at most `max_rows` rows.
///
/// A `RowRange` covers contiguous rows `[row_start, row_start+count)` and
/// contiguous TIME AXIS indices `[time_start_idx, time_start_idx+count)`, so
/// splitting is always sound — every piece is a valid, order-preserving range.
fn split_range(r: RowRange, max_rows: u32) -> Vec<RowRange> {
    if r.count <= max_rows {
        return vec![r];
    }
    let mut out = Vec::new();
    let mut done = 0u32;
    while done < r.count {
        let take = max_rows.min(r.count - done);
        out.push(RowRange {
            row_start: r.row_start + done,
            count: take,
            sym_idx: r.sym_idx,
            time_start_idx: r.time_start_idx + done,
        });
        done += take;
    }
    out
}

/// Partition `plan.ranges` into at most `n` roughly **row-balanced** groups.
///
/// - Oversized single ranges are first split so a huge symbol doesn't starve
///   the other partitions (skewed-symbol friendly).
/// - Groups keep the global `(sym, time)` order internally (they are contiguous
///   slices of the ordered range list).
/// - `n <= 1` or an empty plan → a single group with all ranges (identity).
pub fn split_ranges(plan: &ScanPlan, n: usize) -> Vec<Vec<RowRange>> {
    if n <= 1 || plan.ranges.is_empty() {
        return vec![plan.ranges.clone()];
    }
    let total: u64 = plan.ranges.iter().map(|r| r.count as u64).sum();
    let target = total.div_ceil(n as u64).max(1) as u32;

    // Split oversized ranges first so a huge symbol doesn't starve the other
    // partitions (skewed-symbol friendly).
    let mut pieces: Vec<RowRange> = Vec::new();
    for r in &plan.ranges {
        pieces.extend(split_range(*r, target));
    }
    if pieces.len() <= 1 {
        return vec![pieces];
    }

    // Consecutive packing: walk the (globally ordered) piece list and close a
    // group once it reaches ~`target` rows. Groups therefore stay contiguous
    // slices of the ordered list — every group is internally `(sym, time)`
    // ordered AND draining groups in order reproduces the global order.
    let mut groups: Vec<Vec<RowRange>> = vec![Vec::new()];
    let mut cur_rows = 0u64;
    for piece in pieces {
        let last = groups.last_mut().expect("always one group");
        if !last.is_empty() && cur_rows >= target as u64 && groups.len() < n {
            groups.push(Vec::new());
            cur_rows = 0;
        }
        groups.last_mut().expect("always one group").push(piece);
        cur_rows += piece.count as u64;
    }
    groups
}

/// An **ordered** parallel streaming scan over an `Arc<Dataset>`.
///
/// Built by [`scan_owned_parallel`]: `parallelism` producer threads each scan
/// one row-balanced range group via the standard [`scan_owned`] batch machinery
/// (readers, filters, sym/time, batch size — identical semantics), pushing
/// batches through per-group bounded channels. The consumer drains the groups
/// in order, so the stream is byte-for-byte equivalent to a serial scan.
pub struct ParallelScanBatches {
    receivers: Vec<std::sync::mpsc::Receiver<Result<CoreBatch, ScannerError>>>,
    current: usize,
    limit: Option<usize>,
    emitted: usize,
}

/// Create an ordered parallel scan stream ([`ParallelScanBatches`]).
///
/// `parallelism <= 1` or an empty range list yields the serial/empty behavior.
/// Threads are `std::thread::spawn` (not tied to any async runtime), which fits
/// both native consumers (DuckDB-style chunk threading) and `spawn_blocking`
/// adapters (DataFusion) alike.
pub fn scan_owned_parallel(
    dataset: Arc<Dataset>,
    plan: &ScanPlan,
    request: &ScanRequest,
    parallelism: usize,
) -> Result<ParallelScanBatches, ScannerError> {
    if plan.ranges.is_empty() {
        return Ok(ParallelScanBatches {
            receivers: Vec::new(),
            current: 0,
            limit: request.limit,
            emitted: 0,
        });
    }

    let groups = split_ranges(plan, parallelism);
    let mut receivers = Vec::with_capacity(groups.len());

    for group in groups {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Result<CoreBatch, ScannerError>>(2);
        receivers.push(rx);

        let dataset = Arc::clone(&dataset);
        let request = request.clone();
        let sub_plan = ScanPlan {
            columns: plan.columns.clone(),
            ranges: group,
            total_rows: 0,
        };
        std::thread::spawn(move || -> Result<(), ScannerError> {
            let mut it = scan_owned(dataset, &sub_plan, &request)?;
            loop {
                match it.next_batch() {
                    Ok(Some(b)) => {
                        if tx.send(Ok(b)).is_err() {
                            break; // consumer dropped (cancellation)
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        break;
                    }
                }
            }
            Ok(())
        });
    }

    Ok(ParallelScanBatches {
        receivers,
        current: 0,
        limit: request.limit,
        emitted: 0,
    })
}

impl ParallelScanBatches {
    /// Get the next batch in global (sym, time) order, or `None` when exhausted.
    ///
    /// Drains the per-group channels in order; a group whose producer has
    /// finished (channel disconnected) advances to the next group.
    pub fn next_batch(&mut self) -> Result<Option<CoreBatch>, ScannerError> {
        while self.current < self.receivers.len() {
            match self.receivers[self.current].recv() {
                Ok(item) => {
                    let batch = item?;
                    // 读取期截断（达到限值后停止拉取后续分组）。
                    let mut exhausted = false;
                    let out =
                        truncate_batch(batch, &mut self.emitted, self.limit, &mut exhausted)?;
                    if exhausted {
                        self.current = self.receivers.len(); // 停止继续 drain
                        return Ok(out);
                    }
                    return Ok(out);
                }
                Err(_) => self.current += 1, // this group's producer is done
            }
        }
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Filter evaluation
// ---------------------------------------------------------------------------

impl Filter {
    pub fn field_name(&self) -> &str {
        match self {
            Self::GreaterThan { field, .. }
            | Self::GreaterOrEqual { field, .. }
            | Self::LessThan { field, .. }
            | Self::LessOrEqual { field, .. }
            | Self::Equal { field, .. }
            | Self::NotEqual { field, .. }
            | Self::IsNull { field }
            | Self::IsNotNull { field } => field,
        }
    }
}

/// 一个投影列的缓冲：`(字段名, 原始字节, 数据类型)`。
pub(crate) type ColData = (String, Vec<u8>, DataType);

/// Apply a conjunction of value filters to a batch's column data, compacting
/// to only the rows that pass **all** of them (AND semantics).
///
/// Shared between `ScanBatches::next_batch`, `scan_all_parallel`, and
/// `OwnedScanBatches::next_batch` so every path behaves identically.
fn apply_filters(
    mut col_data: Vec<ColData>,
    sym_indices: &mut Vec<usize>,
    time_values: &mut Vec<i64>,
    filters: &[Filter],
    mut row_count: usize,
) -> Result<(Vec<ColData>, usize), ScannerError> {
    for filter in filters {
        let filter_field = filter.field_name();
        let idx = col_data
            .iter()
            .position(|(n, _, _)| n == filter_field)
            .ok_or_else(|| ScannerError::FilterFieldNotProjected(filter_field.to_string()))?;
        let filter_dt = col_data[idx].2;
        let filter_bytes = &col_data[idx].1;

        let passing = passing_indices(filter, filter_dt, filter_bytes, row_count);
        let (new_data, passing_count) =
            compact_passing(col_data, sym_indices, time_values, &passing);
        col_data = new_data;
        row_count = passing_count;
    }
    Ok((col_data, row_count))
}

/// Compute the row indices that pass the filter on a given column.
///
/// Shared between `next_batch` (single-threaded) and `scan_all_parallel` (parallel)
/// to ensure identical behavior including SIMD fast paths for f64/i64 columns.
///
/// Returns an error if the filter field is not in the projection.
fn passing_indices(
    filter: &Filter,
    filter_dt: DataType,
    filter_bytes: &[u8],
    row_count: usize,
) -> Vec<usize> {
    let view = ColumnView::new(filter_dt, filter_bytes, row_count);
    match filter_dt {
        DataType::Float64 => extract_f64_threshold(filter)
            .map(|t| simd_batch_filter_f64(filter_bytes, t, row_count, filter))
            .unwrap_or_else(|| scalar_filter(&view, filter, row_count)),
        DataType::Int64 => extract_i64_threshold(filter)
            .map(|t| simd_batch_filter_i64(filter_bytes, t, row_count, filter))
            .unwrap_or_else(|| scalar_filter(&view, filter, row_count)),
        _ => scalar_filter(&view, filter, row_count),
    }
}

/// Compact column data + sym/time vectors to only the passing rows.
fn compact_passing(
    col_data: Vec<(String, Vec<u8>, DataType)>,
    sym_indices: &mut Vec<usize>,
    time_values: &mut Vec<i64>,
    passing: &[usize],
) -> (Vec<(String, Vec<u8>, DataType)>, usize) {
    let passing_count = passing.len();
    let mut new_col_data = Vec::with_capacity(col_data.len());
    for (name, bytes, dt) in col_data {
        let sz = dt.size_of();
        let mut compacted = Vec::with_capacity(passing_count * sz);
        for &i in passing {
            let off = i * sz;
            compacted.extend_from_slice(&bytes[off..off + sz]);
        }
        new_col_data.push((name, compacted, dt));
    }
    let new_sym: Vec<usize> = passing.iter().map(|&i| sym_indices[i]).collect();
    let new_time: Vec<i64> = passing.iter().map(|&i| time_values[i]).collect();
    *sym_indices = new_sym;
    *time_values = new_time;
    (new_col_data, passing_count)
}

/// Scalar (non-SIMD) filter loop — used as fallback for non-numeric types.
#[inline]
fn scalar_filter(view: &ColumnView, filter: &Filter, row_count: usize) -> Vec<usize> {
    let mut passing = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let val = view.get(i).unwrap();
        if filter_passes(filter, &val) {
            passing.push(i);
        }
    }
    passing
}

/// Extract the f64 threshold from a Filter if the filter value is Float64.
fn extract_f64_threshold(filter: &Filter) -> Option<f64> {
    match filter {
        Filter::GreaterThan { value: FilterValue::Float64(f), .. }
        | Filter::GreaterOrEqual { value: FilterValue::Float64(f), .. }
        | Filter::LessThan { value: FilterValue::Float64(f), .. }
        | Filter::LessOrEqual { value: FilterValue::Float64(f), .. }
        | Filter::Equal { value: FilterValue::Float64(f), .. }
        | Filter::NotEqual { value: FilterValue::Float64(f), .. } => Some(*f),
        _ => None,
    }
}

/// Extract the i64 threshold from a Filter if the filter value is Int64.
fn extract_i64_threshold(filter: &Filter) -> Option<i64> {
    match filter {
        Filter::GreaterThan { value: FilterValue::Int64(i), .. }
        | Filter::GreaterOrEqual { value: FilterValue::Int64(i), .. }
        | Filter::LessThan { value: FilterValue::Int64(i), .. }
        | Filter::LessOrEqual { value: FilterValue::Int64(i), .. }
        | Filter::Equal { value: FilterValue::Int64(i), .. }
        | Filter::NotEqual { value: FilterValue::Int64(i), .. } => Some(*i),
        _ => None,
    }
}

/// SIMD-accelerated batch filter for f64 columns.
fn simd_batch_filter_f64(data: &[u8], threshold: f64, row_count: usize, filter: &Filter) -> Vec<usize> {
    match filter {
        Filter::GreaterThan { .. } => crate::simd_filter::batch_filter_f64(data, threshold, row_count, |v, t| v > t),
        Filter::GreaterOrEqual { .. } => crate::simd_filter::batch_filter_f64(data, threshold, row_count, |v, t| v >= t),
        Filter::LessThan { .. } => crate::simd_filter::batch_filter_f64(data, threshold, row_count, |v, t| v < t),
        Filter::LessOrEqual { .. } => crate::simd_filter::batch_filter_f64(data, threshold, row_count, |v, t| v <= t),
        Filter::Equal { .. } => crate::simd_filter::batch_filter_f64(data, threshold, row_count, |v, t| v == t),
        Filter::NotEqual { .. } => crate::simd_filter::batch_filter_f64(data, threshold, row_count, |v, t| v != t),
        // Only reachable for comparison filters (extract_f64_threshold filters
        // out the NULL-check variants before dispatching here).
        Filter::IsNull { .. } | Filter::IsNotNull { .. } => unreachable!("null-check is scalar"),
    }
}

/// SIMD-accelerated batch filter for i64 columns.
fn simd_batch_filter_i64(data: &[u8], threshold: i64, row_count: usize, filter: &Filter) -> Vec<usize> {
    match filter {
        Filter::GreaterThan { .. } => crate::simd_filter::batch_filter_i64(data, threshold, row_count, |v, t| v > t),
        Filter::GreaterOrEqual { .. } => crate::simd_filter::batch_filter_i64(data, threshold, row_count, |v, t| v >= t),
        Filter::LessThan { .. } => crate::simd_filter::batch_filter_i64(data, threshold, row_count, |v, t| v < t),
        Filter::LessOrEqual { .. } => crate::simd_filter::batch_filter_i64(data, threshold, row_count, |v, t| v <= t),
        Filter::Equal { .. } => crate::simd_filter::batch_filter_i64(data, threshold, row_count, |v, t| v == t),
        Filter::NotEqual { .. } => crate::simd_filter::batch_filter_i64(data, threshold, row_count, |v, t| v != t),
        Filter::IsNull { .. } | Filter::IsNotNull { .. } => unreachable!("null-check is scalar"),
    }
}

// ---------------------------------------------------------------------------
// Limit 截断 + 统计剪裁辅助
// ---------------------------------------------------------------------------

/// 读取期截断：按限值对批次切片；达到限值后置 `exhausted`。
///
/// 返回 `Ok(None)` 表示已达到限值（未来不再产出）。
fn truncate_batch(
    batch: CoreBatch,
    emitted: &mut usize,
    limit: Option<usize>,
    exhausted: &mut bool,
) -> Result<Option<CoreBatch>, ScannerError> {
    let Some(lim) = limit else {
        return Ok(Some(batch));
    };
    let keep = lim.saturating_sub(*emitted);
    if keep == 0 {
        *exhausted = true;
        return Ok(None);
    }
    if batch.num_rows() > keep {
        *emitted = lim;
        *exhausted = true;
        Ok(Some(batch.slice(0, keep)))
    } else {
        *emitted += batch.num_rows();
        Ok(Some(batch))
    }
}

/// 用列统计 `[min, max]`（已排除 NULL 哨兵）判断 filter 是否可能命中。
///
/// 边界语义直接复用行级 `filter_passes`：对降序/升序极值做保守判定。
/// `pub(crate)`：分区层（`partition.rs`）用它做「分区级统计剪裁」。
pub(crate) fn matches_range(filter: &Filter, min_v: &RawValue, max_v: &RawValue) -> bool {
    match filter {
        Filter::GreaterThan { .. } | Filter::GreaterOrEqual { .. } => {
            filter_passes(filter, max_v)
        }
        Filter::LessThan { .. } | Filter::LessOrEqual { .. } => filter_passes(filter, min_v),
        Filter::Equal { .. } => filter_passes(filter, min_v) || filter_passes(filter, max_v),
        // NotEqual / IsNull / IsNotNull：无法从 min/max 剪裁（IsNull/IsNotNull
        // 在 plan() 中按 null_count 处理）。
        _ => true,
    }
}

fn filter_passes(filter: &Filter, val: &splayed_format::RawValue) -> bool {
    match filter {
        // NULL checks inspect the NULL sentinel directly.
        Filter::IsNull { .. } => val.is_null(),
        Filter::IsNotNull { .. } => !val.is_null(),
        // NULL values never pass value-comparison filters.
        _ => {
            if val.is_null() {
                return false;
            }
            match filter {
                Filter::GreaterThan { value, .. } => compare(val, value).is_some_and(|c| c > 0),
                Filter::GreaterOrEqual { value, .. } => compare(val, value).is_some_and(|c| c >= 0),
                Filter::LessThan { value, .. } => compare(val, value).is_some_and(|c| c < 0),
                Filter::LessOrEqual { value, .. } => compare(val, value).is_some_and(|c| c <= 0),
                Filter::Equal { value, .. } => compare(val, value).is_some_and(|c| c == 0),
                Filter::NotEqual { value, .. } => compare(val, value).is_some_and(|c| c != 0),
                Filter::IsNull { .. } | Filter::IsNotNull { .. } => unreachable!("handled above"),
            }
        }
    }
}

/// Compare a RawValue against a FilterValue.  Returns None if types mismatch.
fn compare(val: &splayed_format::RawValue, fv: &FilterValue) -> Option<i8> {
    use splayed_format::DataType;
    match (val.ty, fv) {
        (DataType::Bool, FilterValue::Bool(b)) => {
            let v = val.as_bool()?;
            Some(if v == *b { 0 } else if !v { -1 } else { 1 })
        }
        (DataType::Int8, FilterValue::Int8(i)) => Some(val.as_i8()?.cmp(i) as i8),
        (DataType::Int16, FilterValue::Int16(i)) => Some(val.as_i16()?.cmp(i) as i8),
        (DataType::Int32, FilterValue::Int32(i)) => Some(val.as_i32()?.cmp(i) as i8),
        (DataType::Int32, FilterValue::Int64(i)) => Some((val.as_i32()? as i64).cmp(i) as i8),
        (DataType::Int64, FilterValue::Int64(i)) => Some(val.as_i64()?.cmp(i) as i8),
        (DataType::Int64, FilterValue::Int32(i)) => Some(val.as_i64()?.cmp(&(*i as i64)) as i8),
        (DataType::UInt8, FilterValue::UInt8(i)) => Some(val.as_u8()?.cmp(i) as i8),
        (DataType::UInt16, FilterValue::UInt16(i)) => Some(val.as_u16()?.cmp(i) as i8),
        (DataType::UInt32, FilterValue::UInt32(i)) => Some(val.as_u32()?.cmp(i) as i8),
        (DataType::UInt64, FilterValue::UInt64(i)) => Some(val.as_u64()?.cmp(i) as i8),
        (DataType::Float32, FilterValue::Float32(f)) => {
            let v = val.as_f32()?;
            Some(if v > *f { 1 } else if v < *f { -1 } else { 0 })
        }
        (DataType::Float64, FilterValue::Float64(f)) => {
            let v = val.as_f64()?;
            Some(if v > *f { 1 } else if v < *f { -1 } else { 0 })
        }
        (DataType::Date32, FilterValue::Date32(d)) => Some(val.as_i32()?.cmp(d) as i8),
        (DataType::Date64, FilterValue::Date64(d)) => Some(val.as_date64()?.cmp(d) as i8),
        (DataType::TimestampUs, FilterValue::TimestampUs(t)) => {
            Some(val.as_i64()?.cmp(t) as i8)
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ScannerError {
    SymbolNotFound(String),
    FieldNotFound(String),
    ReaderError(String, crate::ReaderError),
    FilterFieldNotProjected(String),
    ParallelScanPanic,
}

impl std::fmt::Display for ScannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SymbolNotFound(s) => write!(f, "symbol not found: {s}"),
            Self::FieldNotFound(s) => write!(f, "field not found: {s}"),
            Self::ReaderError(s, e) => write!(f, "reader error for '{s}': {e}"),
            Self::FilterFieldNotProjected(s) => {
                write!(f, "filter field '{s}' must be in projection (V1)")
            }
            Self::ParallelScanPanic => write!(f, "parallel scan thread panicked"),
        }
    }
}
impl std::error::Error for ScannerError {}

#[cfg(test)]
mod tests {
    use super::*;
    use splayed_format::{DataType, RawValue};

    #[test]
    fn time_range_contains() {
        let r = TimeRange::new(10, 20);
        assert!(r.contains(15));
        assert!(r.contains(10));
        assert!(!r.contains(20));
        assert!(!r.contains(5));

        let all = TimeRange::all();
        assert!(all.contains(0));
        assert!(all.contains(i64::MAX - 1));
    }

    #[test]
    fn symbol_selection_all() {
        let s = SymbolSelection::all();
        assert!(matches!(s, SymbolSelection::All));
    }

    #[test]
    fn filter_comparison_int() {
        let val = RawValue::from_i32(50);
        assert!(filter_passes(
            &Filter::GreaterThan { field: "x".into(), value: FilterValue::Int32(40) },
            &val
        ));
        assert!(!filter_passes(
            &Filter::GreaterThan { field: "x".into(), value: FilterValue::Int32(50) },
            &val
        ));
        assert!(filter_passes(
            &Filter::Equal { field: "x".into(), value: FilterValue::Int32(50) },
            &val
        ));
    }

    #[test]
    fn filter_comparison_float() {
        let val = RawValue::from_f64(3.25);
        assert!(filter_passes(
            &Filter::GreaterThan { field: "x".into(), value: FilterValue::Float64(3.0) },
            &val
        ));
        assert!(!filter_passes(
            &Filter::LessThan { field: "x".into(), value: FilterValue::Float64(3.0) },
            &val
        ));
    }

    #[test]
    fn filter_null_never_passes() {
        let null_val = RawValue::null(DataType::Int32);
        assert!(!filter_passes(
            &Filter::Equal { field: "x".into(), value: FilterValue::Int32(0) },
            &null_val
        ));
        assert!(!filter_passes(
            &Filter::GreaterThan { field: "x".into(), value: FilterValue::Int32(-1) },
            &null_val
        ));
    }
}
