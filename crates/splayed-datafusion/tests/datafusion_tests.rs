//! Integration tests for DataFusion TableProvider (plan §8).

use std::fs;
use std::sync::Arc;

use arrow_array::{
    Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use datafusion::prelude::SessionContext;
use splayed_arrow::create_table;
use splayed_datafusion::SplayedTableProvider;

fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
        Field::new("volume", ArrowDT::Int64, true),
    ]));

    // SYM01: days 0..4  close = 100..104  vol = 1000..4000
    // SYM02: days 0..4  close = 200..204  vol = 2000..6000
    let time = Date32Array::from(vec![
        0, 1, 2, 3, 4,
        0, 1, 2, 3, 4,
    ]);
    let sym = StringArray::from(vec![
        Some("SYM01"), Some("SYM01"), Some("SYM01"), Some("SYM01"), Some("SYM01"),
        Some("SYM02"), Some("SYM02"), Some("SYM02"), Some("SYM02"), Some("SYM02"),
    ]);
    let close = Float64Array::from(vec![
        Some(100.0), Some(101.0), Some(102.0), Some(103.0), Some(104.0),
        Some(200.0), Some(201.0), Some(202.0), Some(203.0), Some(204.0),
    ]);
    let volume = Int64Array::from(vec![
        Some(1000), Some(2000), Some(3000), Some(4000), Some(5000),
        Some(2000), Some(3000), Some(4000), Some(5000), Some(6000),
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
    let dir = std::env::temp_dir()
        .join(format!("splayed_df_{suffix}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

async fn run_query(
    dir: &std::path::Path,
    sql: &str,
) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    let provider = SplayedTableProvider::new(dir).unwrap();
    ctx.register_table("splayed", Arc::new(provider))
        .unwrap();
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

#[tokio::test]
async fn select_all() {
    let dir = temp_dir("select_all");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT * FROM splayed").await;
    assert_eq!(batches.len(), 1);
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn select_close_projection() {
    let dir = temp_dir("proj");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed").await;
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 10);

    // Verify close values
    let close_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(close_arr.value(0), 100.0);
    assert_eq!(close_arr.value(5), 200.0);

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn filter_sym_eq() {
    let dir = temp_dir("sym_eq");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE sym = 'SYM02'").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    // All should be SYM02 close values: 200..204
    let batch = &batches[0];
    let close_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for i in 0..5 {
        assert_eq!(close_arr.value(i), 200.0 + i as f64);
    }

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn filter_time_range() {
    let dir = temp_dir("time_range");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE time >= 2 AND time < 4").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // SYM01 days 2,3 + SYM02 days 2,3 = 4 rows
    assert_eq!(total_rows, 4);

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn filter_sym_and_time() {
    let dir = temp_dir("sym_time");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches =
        run_query(&dir, "SELECT close, volume FROM splayed WHERE sym = 'SYM01' AND time >= 1 AND time < 4")
            .await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // SYM01 days 1,2,3 = 3 rows
    assert_eq!(total_rows, 3);

    let batch = &batches[0];
    let close_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(close_arr.value(0), 101.0);
    assert_eq!(close_arr.value(1), 102.0);
    assert_eq!(close_arr.value(2), 103.0);

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn aggregate_group_by_sym() {
    let dir = temp_dir("agg");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches =
        run_query(&dir, "SELECT sym, AVG(close) FROM splayed GROUP BY sym ORDER BY sym").await;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2); // 2 symbols

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn count_rows() {
    let dir = temp_dir("count");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT COUNT(*) FROM splayed").await;
    let batch = &batches[0];
    let count_arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count_arr.value(0), 10);

    fs::remove_dir_all(&dir).ok();
}
