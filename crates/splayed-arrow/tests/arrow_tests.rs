//! Integration tests for Splayed → Arrow conversion (plan §10.1).
//!
//! Tests the full roundtrip: create_table → scan → column_view_to_arrow →
//! verify Arrow array values, NULL bitmap, NaN semantics.

use std::fs;
use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use splayed_arrow::{column_view_to_arrow, create_table};
use splayed_core::{open_dataset, ScanRequest, Scanner, SymbolSelection, TimeRange};

fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
        Field::new("flag", ArrowDT::Boolean, true),
        Field::new("vol", ArrowDT::Int64, true),
    ]));

    // SYM01: days 0,1   close = 100.0, NULL   flag = true, false   vol = 1000, NULL
    // SYM02: days 0,1   close = 200.0, 201.0   flag = NULL, true   vol = NULL, 2000
    let time = Date32Array::from(vec![0, 1, 0, 1]);
    let sym = StringArray::from(vec![
        Some("SYM01"), Some("SYM01"),
        Some("SYM02"), Some("SYM02"),
    ]);
    let close = Float64Array::from(vec![Some(100.0), None, Some(200.0), Some(201.0)]);
    let flag = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
    let vol = Int64Array::from(vec![Some(1000), None, None, Some(2000)]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(time),
            Arc::new(sym),
            Arc::new(close),
            Arc::new(flag),
            Arc::new(vol),
        ],
    )
    .unwrap()
}

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("splayed_arrow_{suffix}_{}", std::process::id()))
}

#[test]
fn scan_to_arrow_float64_with_null() {
    let dir = temp_dir("f64_null");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filter: None,
        batch_size: 65536,
        parallelism: 1,
    };

    let plan = scanner.plan(&req).unwrap();
    let mut batches = scanner.scan(&plan, &req).unwrap();
    let batch = batches.next_batch().unwrap().unwrap();

    assert_eq!(batch.row_count, 4);

    let view = batch.column_view("close").unwrap();
    let arr = column_view_to_arrow(&view);
    let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();

    // SYM01 day 0 = 100.0, SYM01 day 1 = NULL, SYM02 day 0 = 200.0, SYM02 day 1 = 201.0
    assert_eq!(f64_arr.len(), 4);
    assert!(!f64_arr.is_null(0));
    assert_eq!(f64_arr.value(0), 100.0);
    assert!(f64_arr.is_null(1)); // NULL
    assert!(!f64_arr.is_null(2));
    assert_eq!(f64_arr.value(2), 200.0);
    assert!(!f64_arr.is_null(3));
    assert_eq!(f64_arr.value(3), 201.0);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn scan_to_arrow_int64_with_null() {
    let dir = temp_dir("i64_null");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    let req = ScanRequest {
        columns: vec!["vol".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filter: None,
        batch_size: 65536,
        parallelism: 1,
    };

    let plan = scanner.plan(&req).unwrap();
    let mut batches = scanner.scan(&plan, &req).unwrap();
    let batch = batches.next_batch().unwrap().unwrap();

    let view = batch.column_view("vol").unwrap();
    let arr = column_view_to_arrow(&view);
    let i64_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();

    // vol: 1000, NULL, NULL, 2000
    assert_eq!(i64_arr.len(), 4);
    assert_eq!(i64_arr.value(0), 1000);
    assert!(i64_arr.is_null(1));
    assert!(i64_arr.is_null(2));
    assert_eq!(i64_arr.value(3), 2000);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn scan_to_arrow_bool_with_null() {
    let dir = temp_dir("bool_null");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    let req = ScanRequest {
        columns: vec!["flag".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filter: None,
        batch_size: 65536,
        parallelism: 1,
    };

    let plan = scanner.plan(&req).unwrap();
    let mut batches = scanner.scan(&plan, &req).unwrap();
    let batch = batches.next_batch().unwrap().unwrap();

    let view = batch.column_view("flag").unwrap();
    let arr = column_view_to_arrow(&view);
    let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();

    // flag: true, false, NULL, true
    assert_eq!(bool_arr.len(), 4);
    assert!(bool_arr.value(0));
    assert!(!bool_arr.value(1));
    assert!(bool_arr.is_null(2));
    assert!(bool_arr.value(3));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn scan_to_arrow_multi_column() {
    let dir = temp_dir("multi");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();

    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    let req = ScanRequest {
        columns: vec!["close".into(), "vol".into()],
        symbols: SymbolSelection::syms(["SYM02"]),
        time_range: TimeRange::all(),
        filter: None,
        batch_size: 65536,
        parallelism: 1,
    };

    let plan = scanner.plan(&req).unwrap();
    assert_eq!(plan.total_rows, 2);

    let mut batches = scanner.scan(&plan, &req).unwrap();
    let batch = batches.next_batch().unwrap().unwrap();
    assert_eq!(batch.row_count, 2);

    // close: 200.0, 201.0
    let close_view = batch.column_view("close").unwrap();
    let close_arr = column_view_to_arrow(&close_view);
    let f64_arr = close_arr.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(f64_arr.value(0), 200.0);
    assert_eq!(f64_arr.value(1), 201.0);

    // vol: NULL, 2000
    let vol_view = batch.column_view("vol").unwrap();
    let vol_arr = column_view_to_arrow(&vol_view);
    let i64_arr = vol_arr.as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(i64_arr.is_null(0));
    assert_eq!(i64_arr.value(1), 2000);

    fs::remove_dir_all(&dir).ok();
}
