//! Integration tests for the Scanner API (plan §9).

use std::fs;
use std::sync::Arc;

use arrow_array::{Date32Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use splayed_arrow::create_table;
use splayed_core::{
    compact_field, open_dataset, Filter, FilterValue, ScanRequest, Scanner, SymbolSelection,
    TimeRange,
};

fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
        Field::new("volume", ArrowDT::Int64, true),
    ]));

    // SYM01: days 0,1,2,3,4   close = 100..104  vol = 1000..4000
    // SYM02: days 0,1,2,3,4   close = 200..204  vol = 2000..6000
    // SYM03: days 2,3,4       close = 300..302  vol = 3000..5000
    let time = Date32Array::from(vec![
        0, 1, 2, 3, 4,
        0, 1, 2, 3, 4,
        2, 3, 4,
    ]);
    let sym = StringArray::from(vec![
        Some("SYM01"), Some("SYM01"), Some("SYM01"), Some("SYM01"), Some("SYM01"),
        Some("SYM02"), Some("SYM02"), Some("SYM02"), Some("SYM02"), Some("SYM02"),
        Some("SYM03"), Some("SYM03"), Some("SYM03"),
    ]);
    let close = Float64Array::from(vec![
        Some(100.0), Some(101.0), Some(102.0), Some(103.0), Some(104.0),
        Some(200.0), Some(201.0), Some(202.0), Some(203.0), Some(204.0),
        Some(300.0), Some(301.0), Some(302.0),
    ]);
    let volume = Int64Array::from(vec![
        Some(1000), Some(2000), Some(3000), Some(4000), Some(5000),
        Some(2000), Some(3000), Some(4000), Some(5000), Some(6000),
        Some(3000), Some(4000), Some(5000),
    ]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(time),
            Arc::new(sym),
            Arc::new(close),
            Arc::new(volume),
        ],
    )
    .unwrap()
}

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("splayed_scan_{suffix}_{}", std::process::id()))
}

// ---------------------------------------------------------------------------
// CoreBatch decode helpers (layout: [time, sym, fields...], names via schema)
// ---------------------------------------------------------------------------

fn col_of(b: &splayed_core::CoreBatch, name: &str) -> usize {
    b.schema().index_of(name).expect("column present")
}

fn f64_at(b: &splayed_core::CoreBatch, name: &str, i: usize) -> f64 {
    let d = b.column(col_of(b, name)).data();
    f64::from_le_bytes(d[i * 8..i * 8 + 8].try_into().unwrap())
}

fn i64_at(b: &splayed_core::CoreBatch, name: &str, i: usize) -> i64 {
    let d = b.column(col_of(b, name)).data();
    i64::from_le_bytes(d[i * 8..i * 8 + 8].try_into().unwrap())
}

fn time_at(b: &splayed_core::CoreBatch, i: usize) -> i64 {
    let d = b.column(0).data();
    i32::from_le_bytes(d[i * 4..i * 4 + 4].try_into().unwrap()) as i64
}

fn sym_at(b: &splayed_core::CoreBatch, i: usize) -> usize {
    let d = b.column(1).dictionary_indices().unwrap();
    u32::from_le_bytes(d[i * 4..i * 4 + 4].try_into().unwrap()) as usize
}

// ---------------------------------------------------------------------------
// Projection + predicate (SYM + TIME) pruning
// ---------------------------------------------------------------------------

#[test]
fn scan_projection_and_sym_filter() {
    let dir = temp_dir("proj_sym");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    // SELECT close FROM dataset WHERE sym = 'SYM02'
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::syms(["SYM02"]),
        time_range: TimeRange::all(),
        filters: vec![],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };

    let plan = scanner.plan(&req).unwrap();
    assert_eq!(plan.total_rows, 5); // SYM02 has 5 time points (days 0-4)

    let mut batches = scanner.scan(&plan, &req).unwrap();
    let batch = batches.next_batch().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 5);
    assert_eq!(batch.num_columns(), 3); // time + sym + projected "close"

    // All rows should be SYM02 (sym_idx = 1)
    assert!((0..5).all(|i| sym_at(&batch, i) == 1));

    // close values: 200, 201, 202, 203, 204
    for i in 0..5 {
        assert_eq!(f64_at(&batch, "close", i), 200.0 + i as f64);
    }

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn scan_time_range_filter() {
    let dir = temp_dir("time_filter");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    // SELECT close FROM dataset WHERE time >= 2 AND time < 4 (all SYMs)
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::new(2, 4), // days 2,3
        filters: vec![],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };

    let plan = scanner.plan(&req).unwrap();
    // SYM01: days 2,3 → 2 rows
    // SYM02: days 2,3 → 2 rows
    // SYM03: days 2,3 → 2 rows (SYM03 starts at day 2)
    assert_eq!(plan.total_rows, 6);

    let mut batches = scanner.scan(&plan, &req).unwrap();
    let batch = batches.next_batch().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 6);

    // Verify time values are in range.
    for i in 0..batch.num_rows() {
        let t = time_at(&batch, i);
        assert!((2..4).contains(&t), "time {t} not in [2,4)");
    }

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn scan_sym_and_time_combined() {
    let dir = temp_dir("sym_time");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    // SELECT close, volume FROM dataset WHERE sym = 'SYM01' AND time >= 1 AND time < 4
    let req = ScanRequest {
        columns: vec!["close".into(), "volume".into()],
        symbols: SymbolSelection::syms(["SYM01"]),
        time_range: TimeRange::new(1, 4), // days 1,2,3
        filters: vec![],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };

    let plan = scanner.plan(&req).unwrap();
    assert_eq!(plan.total_rows, 3); // SYM01 days 1,2,3

    let mut batches = scanner.scan(&plan, &req).unwrap();
    let batch = batches.next_batch().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 4); // time + sym + close + volume

    // close: 101, 102, 103
    assert_eq!(f64_at(&batch, "close", 0), 101.0);
    assert_eq!(f64_at(&batch, "close", 1), 102.0);
    assert_eq!(f64_at(&batch, "close", 2), 103.0);

    // volume: 2000, 3000, 4000
    assert_eq!(i64_at(&batch, "volume", 0), 2000);
    assert_eq!(i64_at(&batch, "volume", 1), 3000);
    assert_eq!(i64_at(&batch, "volume", 2), 4000);

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Value filter
// ---------------------------------------------------------------------------

#[test]
fn scan_with_value_filter() {
    let dir = temp_dir("val_filter");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    // SELECT close FROM dataset WHERE close > 150 (all SYMs, all times)
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![Filter::GreaterThan {
            field: "close".into(),
            value: FilterValue::Float64(150.0),
        }],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };

    let plan = scanner.plan(&req).unwrap();
    let mut batches = scanner.scan(&plan, &req).unwrap();
    let batch = batches.next_batch().unwrap().unwrap();

    // close values > 150: SYM02 (200..204) = 5, SYM03 (300..302) = 3 → 8 total
    assert_eq!(batch.num_rows(), 8);

    // All values should be > 150.
    for i in 0..batch.num_rows() {
        let f = f64_at(&batch, "close", i);
        assert!(f > 150.0, "value {f} should be > 150");
    }

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Batch iteration
// ---------------------------------------------------------------------------

#[test]
fn scan_batch_iteration() {
    let dir = temp_dir("batch_iter");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    // Small batch size to force multiple batches.
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![],
        batch_size: 3,
        parallelism: 1,
        limit: None,
    };

    let plan = scanner.plan(&req).unwrap();
    assert_eq!(plan.total_rows, 13); // 5 + 5 + 3

    let mut batches = scanner.scan(&plan, &req).unwrap();
    let mut total = 0;
    while let Some(batch) = batches.next_batch().unwrap() {
        total += batch.num_rows();
    }
    assert_eq!(total, 13);

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Empty result
// ---------------------------------------------------------------------------

#[test]
fn scan_empty_result() {
    let dir = temp_dir("empty");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    // Non-existent symbol.
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::syms(["NOEXIST"]),
        time_range: TimeRange::all(),
        filters: vec![],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };

    let plan = scanner.plan(&req);
    assert!(plan.is_err()); // symbol not found

    // Time range with no overlap.
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::new(100, 200), // no data in days 100-200
        filters: vec![],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };

    let plan = scanner.plan(&req).unwrap();
    assert_eq!(plan.total_rows, 0);

    let mut batches = scanner.scan(&plan, &req).unwrap();
    assert!(batches.next_batch().unwrap().is_none());

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Compressed field scan (ZSTD)
// ---------------------------------------------------------------------------

#[test]
fn scan_compressed_field_zstd() {
    let dir = temp_dir("comp_zstd");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    // Compact the close field with ZSTD (must drop readers first on Windows).
    let dataset = open_dataset(&dir).unwrap();
    let close_path = dataset.field_path("close");
    compact_field(&close_path, splayed_format::Compression::Zstd).unwrap();

    // Now scan the compressed field — FieldReader should decompress on open.
    let scanner = Scanner::new(&dataset);
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };

    let plan = scanner.plan(&req).unwrap();
    assert_eq!(plan.total_rows, 13);

    let mut batches = scanner.scan(&plan, &req).unwrap();
    let batch = batches.next_batch().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 13);

    // Verify values: SYM01 close = 100..104, SYM02 = 200..204, SYM03 = 300..302
    // Row 0 = SYM01 day 0 = 100.0
    assert_eq!(f64_at(&batch, "close", 0), 100.0);
    // Row 5 = SYM02 day 0 = 200.0
    assert_eq!(f64_at(&batch, "close", 5), 200.0);
    // Row 10 = SYM03 day 0 = 300.0
    assert_eq!(f64_at(&batch, "close", 10), 300.0);

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Compressed field scan with filter (LZ4)
// ---------------------------------------------------------------------------

#[test]
fn scan_compressed_field_lz4_with_filter() {
    let dir = temp_dir("comp_lz4_filter");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    // Compact with LZ4.
    let dataset = open_dataset(&dir).unwrap();
    let close_path = dataset.field_path("close");
    compact_field(&close_path, splayed_format::Compression::Lz4).unwrap();

    // Scan with value filter: close > 150
    let scanner = Scanner::new(&dataset);
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![Filter::GreaterThan {
            field: "close".into(),
            value: FilterValue::Float64(150.0),
        }],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };

    let plan = scanner.plan(&req).unwrap();
    let mut batches = scanner.scan(&plan, &req).unwrap();
    let batch = batches.next_batch().unwrap().unwrap();

    // close > 150: SYM02 (5 rows) + SYM03 (3 rows) = 8 rows
    assert_eq!(batch.num_rows(), 8);

    for i in 0..batch.num_rows() {
        assert!(f64_at(&batch, "close", i) > 150.0);
    }

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Parallel scan
// ---------------------------------------------------------------------------

#[test]
fn scan_parallel_matches_sequential() {
    let dir = temp_dir("parallel");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    // Sequential scan
    let req_seq = ScanRequest {
        columns: vec!["close".into(), "volume".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };
    let plan_seq = scanner.plan(&req_seq).unwrap();
    let mut seq_batches = scanner.scan(&plan_seq, &req_seq).unwrap();
    let mut seq_rows = 0;
    while let Some(b) = seq_batches.next_batch().unwrap() {
        seq_rows += b.num_rows();
    }
    assert_eq!(seq_rows, 13);

    // Parallel scan (3 threads, one per SYM)
    let req_par = ScanRequest {
        columns: vec!["close".into(), "volume".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![],
        batch_size: 65536,
        parallelism: 3,
        limit: None,
    };
    let plan_par = scanner.plan(&req_par).unwrap();
    let par_batches = scanner.scan_all_parallel(&plan_par, &req_par).unwrap();
    let par_rows: usize = par_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(par_rows, 13);

    // Verify data correctness: check close values
    let plan_seq2 = scanner.plan(&req_seq).unwrap();
    let mut seq_iter = scanner.scan(&plan_seq2, &req_seq).unwrap();
    let seq_batch = seq_iter.next_batch().unwrap().unwrap();
    let par_batch = &par_batches[0];

    for i in 0..seq_batch.num_rows().min(par_batch.num_rows()) {
        assert_eq!(
            f64_at(&seq_batch, "close", i),
            f64_at(par_batch, "close", i)
        );
    }

    fs::remove_dir_all(&dir).ok();
}
