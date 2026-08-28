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

use splayed_format::DataType;

use std::sync::Arc;

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
}

/// A comparison value for filtering.  Typed to match the field's DataType.
#[derive(Debug, Clone)]
pub enum FilterValue {
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Date32(i32),
    TimestampUs(i64),
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

/// A batch of scanned rows.  Each column is a `ColumnView` borrowed from
/// the underlying `FieldReader` mmap.
pub struct ScanBatch<'a> {
    /// Column name → ColumnView
    pub columns: Vec<(String, ColumnView<'a>)>,
    /// The number of rows in this batch.
    pub row_count: usize,
    /// The symbol index for each row (for SYM column reconstruction).
    pub sym_indices: Vec<usize>,
    /// The time value for each row (for TIME column reconstruction).
    pub time_values: Vec<i64>,
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
            let range_start_idx = if request.time_range.start <= i64::MIN {
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
            let range_end_idx = if request.time_range.end >= i64::MAX {
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

        Ok(ScanBatches {
            dataset: self.dataset,
            ranges: &plan.ranges,
            readers,
            filters: request.filters.clone(),
            batch_size: request.batch_size,
            current_range_idx: 0,
            current_row_in_range: 0,
            exhausted: false,
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
    ) -> Result<Vec<ScanBatchOwned>, ScannerError> {
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
        let chunk_size = (plan.ranges.len() + n_threads - 1) / n_threads;
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

        // Spawn threads.
        let handles: Vec<_> = range_chunks
            .into_iter()
            .map(|chunk| {
                let col_paths = col_paths.clone();
                let filters = filters.clone();
                let time_axis = Arc::clone(&time_axis); // cheap ref-count clone

                std::thread::spawn(move || -> Result<Vec<ScanBatchOwned>, ScannerError> {
                    // Open readers in this thread.
                    let mut readers: Vec<(String, FieldReader)> = Vec::with_capacity(col_paths.len());
                    for (name, path) in &col_paths {
                        let reader = FieldReader::open(path)
                            .map_err(|e| ScannerError::ReaderError(name.clone(), e))?;
                        readers.push((name.clone(), reader));
                    }

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
                            for (name, reader) in &readers {
                                let dt = reader.data_type();
                                let raw = reader
                                    .read_range_raw(start_row, to_read)
                                    .map_err(|e| ScannerError::ReaderError(name.clone(), e))?;
                                col_data.push((name.clone(), raw.to_vec(), dt));
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
                                out.push(ScanBatchOwned {
                                    columns: col_data,
                                    row_count,
                                    sym_indices,
                                    time_values,
                                });
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

        Ok(all_batches)
    }
}

// ---------------------------------------------------------------------------
// ScanBatches — lazy batch iterator
// ---------------------------------------------------------------------------

/// An iterator that lazily produces `ScanBatch` items.
///
/// For each `RowRange` in the plan, reads `batch_size` rows at a time across
/// all requested column readers.  Filter is applied row-by-row after reading.
pub struct ScanBatches<'ds, 'r> {
    dataset: &'ds Dataset,
    ranges: &'r [RowRange],
    readers: Vec<(String, FieldReader)>,
    filters: Vec<Filter>,
    batch_size: usize,
    current_range_idx: usize,
    current_row_in_range: u32,
    exhausted: bool,
}

impl<'ds, 'r> ScanBatches<'ds, 'r> {
    /// Get the next batch of rows, or `None` if exhausted.
    pub fn next_batch(&mut self) -> Result<Option<ScanBatchOwned>, ScannerError> {
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

        // Build col_data from accumulated buffers.
        let col_data: Vec<(String, Vec<u8>, DataType)> = self
            .readers
            .iter()
            .zip(col_bufs.into_iter())
            .map(|((name, reader), buf)| (name.clone(), buf, reader.data_type()))
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

        Ok(Some(ScanBatchOwned {
            columns: col_data,
            row_count,
            sym_indices,
            time_values,
        }))
    }
}

/// A batch with owned column data (copied from mmap for filter support).
pub struct ScanBatchOwned {
    pub columns: Vec<(String, Vec<u8>, DataType)>,
    pub row_count: usize,
    pub sym_indices: Vec<usize>,
    pub time_values: Vec<i64>,
}

impl ScanBatchOwned {
    /// Get a ColumnView over a column in this batch.
    pub fn column_view(&self, name: &str) -> Option<ColumnView<'_>> {
        for (n, bytes, dt) in &self.columns {
            if n == name {
                return Some(ColumnView::new(*dt, bytes, self.row_count));
            }
        }
        None
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
    current_range_idx: usize,
    current_row_in_range: u32,
    exhausted: bool,
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

    Ok(OwnedScanBatches {
        dataset,
        ranges: plan.ranges.clone(),
        readers,
        filters: request.filters.clone(),
        batch_size: request.batch_size,
        current_range_idx: 0,
        current_row_in_range: 0,
        exhausted: false,
    })
}

impl OwnedScanBatches {
    /// Get the next batch of rows, or `None` if exhausted.
    ///
    /// Identical semantics to [`ScanBatches::next_batch`], but with owned
    /// state so the iterator is `Send + 'static`.
    pub fn next_batch(&mut self) -> Result<Option<ScanBatchOwned>, ScannerError> {
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

        // Build col_data from accumulated buffers.
        let col_data: Vec<(String, Vec<u8>, DataType)> = self
            .readers
            .iter()
            .zip(col_bufs.into_iter())
            .map(|((name, reader), buf)| (name.clone(), buf, reader.data_type()))
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

        Ok(Some(ScanBatchOwned {
            columns: col_data,
            row_count,
            sym_indices,
            time_values,
        }))
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
            | Self::NotEqual { field, .. } => field,
        }
    }
}

/// Apply a conjunction of value filters to a batch's column data, compacting
/// to only the rows that pass **all** of them (AND semantics).
///
/// Shared between `ScanBatches::next_batch`, `scan_all_parallel`, and
/// `OwnedScanBatches::next_batch` so every path behaves identically.
fn apply_filters(
    mut col_data: Vec<(String, Vec<u8>, DataType)>,
    sym_indices: &mut Vec<usize>,
    time_values: &mut Vec<i64>,
    filters: &[Filter],
    mut row_count: usize,
) -> Result<(Vec<(String, Vec<u8>, DataType)>, usize), ScannerError> {
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
    }
}

fn filter_passes(filter: &Filter, val: &splayed_format::RawValue) -> bool {
    // NULL values never pass comparison filters.
    if val.is_null() {
        return false;
    }
    match filter {
        Filter::GreaterThan { value, .. } => compare(val, value).map_or(false, |c| c > 0),
        Filter::GreaterOrEqual { value, .. } => compare(val, value).map_or(false, |c| c >= 0),
        Filter::LessThan { value, .. } => compare(val, value).map_or(false, |c| c < 0),
        Filter::LessOrEqual { value, .. } => compare(val, value).map_or(false, |c| c <= 0),
        Filter::Equal { value, .. } => compare(val, value).map_or(false, |c| c == 0),
        Filter::NotEqual { value, .. } => compare(val, value).map_or(false, |c| c != 0),
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
        (DataType::Int32, FilterValue::Int32(i)) => Some(val.as_i32()?.cmp(i) as i8),
        (DataType::Int32, FilterValue::Int64(i)) => Some((val.as_i32()? as i64).cmp(i) as i8),
        (DataType::Int64, FilterValue::Int64(i)) => Some(val.as_i64()?.cmp(i) as i8),
        (DataType::Int64, FilterValue::Int32(i)) => Some(val.as_i64()?.cmp(&(*i as i64)) as i8),
        (DataType::Float32, FilterValue::Float32(f)) => {
            let v = val.as_f32()?;
            Some(if v > *f { 1 } else if v < *f { -1 } else { 0 })
        }
        (DataType::Float64, FilterValue::Float64(f)) => {
            let v = val.as_f64()?;
            Some(if v > *f { 1 } else if v < *f { -1 } else { 0 })
        }
        (DataType::Date32, FilterValue::Date32(d)) => Some(val.as_i32()?.cmp(d) as i8),
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
        let val = RawValue::from_f64(3.14);
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
