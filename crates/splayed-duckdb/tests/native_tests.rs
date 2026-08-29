//! 原生 DataChunk 路由集成测试：`scan_to_chunks` 直出 CoreBatch（无 Arrow 依赖）。
//! 数据准备用 splayed-arrow（仅 dev-dependency），被测 API 本身不触碰 Arrow。

use std::sync::Arc;

use arrow_array::{Float64Array, RecordBatch, StringArray as ArrowStringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use splayed_arrow::create_table;
use splayed_core::{ScanRequest, Filter, FilterValue};
use splayed_duckdb::native::scan_to_chunks;

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splayed_duck_native_{suffix}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn make_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("time", ArrowDT::Date32, false),
            Field::new("sym", ArrowDT::Utf8, false),
            Field::new("close", ArrowDT::Float64, true),
        ])),
        vec![
            Arc::new(arrow_array::Date32Array::from(vec![0, 1, 2, 0, 1, 2])),
            Arc::new(ArrowStringArray::from(vec![
                "SYM01", "SYM01", "SYM01", "SYM02", "SYM02", "SYM02",
            ])),
            Arc::new(Float64Array::from(vec![
                100.0, 101.0, 102.0, 200.0, 201.0, 202.0,
            ])),
        ],
    )
    .unwrap()
}

#[test]
fn scan_to_chunks_feeds_corebatches() {
    let dir = temp_dir("chunks");
    create_table(&dir, &make_batch(), true).unwrap();
    let dataset = Arc::new(splayed_core::open_dataset(&dir).unwrap());

    // 过滤 close > 150 → 剩下 SYM02 的 3 行。
    let req = ScanRequest {
        columns: vec!["close".to_string()],
        symbols: splayed_core::SymbolSelection::All,
        time_range: splayed_core::TimeRange::all(),
        filters: vec![Filter::GreaterThan {
            field: "close".into(),
            value: FilterValue::Float64(150.0),
        }],
        batch_size: 2, // 强制多批
        parallelism: 1,
    };

    let mut batches = Vec::new();
    let total = scan_to_chunks(Arc::clone(&dataset), &req, 1, |cb| {
        // 原生消费：只碰 CoreBatch（无 Arrow 类型）。
        let close = cb.column(2).data(); // close
        for i in 0..cb.num_rows() {
            batches.push(f64::from_le_bytes(
                close[i * 8..i * 8 + 8].try_into().unwrap(),
            ));
        }
    })
    .unwrap();

    assert_eq!(total, 3);
    assert_eq!(batches, vec![200.0, 201.0, 202.0]);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn scan_to_chunks_parallel_ordered() {
    let dir = temp_dir("chunks_par");
    create_table(&dir, &make_batch(), true).unwrap();
    let dataset = Arc::new(splayed_core::open_dataset(&dir).unwrap());

    let req = ScanRequest::new(vec!["close".to_string()]);
    let mut rows = Vec::new();
    let total = scan_to_chunks(Arc::clone(&dataset), &req, 4, |cb| {
        let time = cb.column(0).data();
        for i in 0..cb.num_rows() {
            rows.push(i32::from_le_bytes(time[i * 4..i * 4 + 4].try_into().unwrap()));
        }
    })
    .unwrap();

    assert_eq!(total, 6);
    // (sym,time) 保序：SYM01 t0..2, SYM02 t0..2。
    assert_eq!(rows, vec![0, 1, 2, 0, 1, 2]);

    std::fs::remove_dir_all(&dir).ok();
}