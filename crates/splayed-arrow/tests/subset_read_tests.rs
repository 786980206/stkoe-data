//! Integration tests for `splayed_arrow::read_subset` — 子集 `.sub.xxx` → Arrow。
//!
//! 覆盖：全字段/列子集读取、父全局行序、NULL 传播、列名校验、父表
//! `update_meta` 后的 `StaleParent` 检测。

use std::fs;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use splayed_arrow::{create_table, read_subset};
use splayed_core::{SubsetInput, create_subset};
use splayed_format::TimeType;

fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
        Field::new("flag", ArrowDT::Boolean, true),
        Field::new("vol", ArrowDT::Int64, true),
    ]));

    // SYM01: days 0,1   close = 100.0, NULL   flag = true, false   vol = 1000, NULL
    // SYM02: days 0,1   close = 200.0, 201.0  flag = NULL, true   vol = NULL, 2000
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
    std::env::temp_dir().join(format!("splayed_arrow_sub_{suffix}_{}", std::process::id()))
}

/// 父表 + 子集：SYM01 全两天 {0,1}，SYM02 只取 day0（两符号区间条数不同）。
fn make_subset(dir: &std::path::Path, sub_name: &str) {
    let inputs = vec![
        SubsetInput::new("SYM01", vec![(0i64, 2u32)]),
        SubsetInput::new("SYM02", vec![(0i64, 1u32)]),
    ];
    create_subset(dir, sub_name, &inputs).unwrap();
}

#[test]
fn read_subset_all_fields_parent_row_order() {
    let dir = temp_dir("all");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();
    make_subset(&dir, "hs300");

    let rb = read_subset(&dir, "hs300", &[]).expect("read_subset failed");
    assert_eq!(rb.num_rows(), 3); // SYM01×2 + SYM02×1

    // 列：time/sym/close/flag/vol（父全局行序）。
    let schema = rb.schema();
    let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(cols, ["time", "sym", "close", "flag", "vol"]);
    let time = rb.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
    assert_eq!(
        (0..rb.num_rows()).map(|i| time.value(i)).collect::<Vec<_>>(),
        vec![0, 1, 0]
    );
    let sym = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        (0..rb.num_rows()).map(|i| sym.value(i)).collect::<Vec<_>>(),
        vec!["SYM01", "SYM01", "SYM02"]
    );

    // close：SYM01 的 day1 是 NULL → 在子集内保持 NULL。
    let close = rb.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(close.is_null(1));
    assert_eq!(close.value(0), 100.0);
    assert_eq!(close.value(2), 200.0);

    let flag = rb.column(3).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(flag.value(0));
    assert!(!flag.value(1));
    assert!(flag.is_null(2));

    let vol = rb.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(vol.value(0), 1000);
    assert!(vol.is_null(1));
    assert!(vol.is_null(2));
}

#[test]
fn read_subset_column_selection() {
    let dir = temp_dir("cols");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();
    make_subset(&dir, "hs300");

    // 只选一列 → [time, sym, vol]。
    let rb = read_subset(&dir, "hs300", &["vol".into()]).unwrap();
    let schema = rb.schema();
    let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(cols, ["time", "sym", "vol"]);
    assert_eq!(rb.num_rows(), 3);
    let vol = rb.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(vol.value(0), 1000);

    // 未知字段 → ScanError::UnknownField。
    let err = read_subset(&dir, "hs300", &["nope".into()]).unwrap_err();
    assert!(err.to_string().contains("unknown field 'nope'"));
}

#[test]
fn read_subset_timestamp_type() {
    let dir = temp_dir("ts");
    let _ = fs::remove_dir_all(&dir);
    // 用 TimestampUs 建表（time 以微秒计）。
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Timestamp(arrow_schema::TimeUnit::Microsecond, None), false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    let time = arrow_array::TimestampMicrosecondArray::from(vec![
        1_000_000, 2_000_000, 1_000_000, 2_000_000,
    ]);
    let sym = StringArray::from(vec!["SYM01", "SYM01", "SYM02", "SYM02"]);
    let close = Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    let rb = RecordBatch::try_new(
        schema,
        vec![Arc::new(time), Arc::new(sym), Arc::new(close)],
    )
    .unwrap();
    create_table(&dir, &rb, true).unwrap();

    let inputs = vec![SubsetInput::new("SYM02", vec![(2_000_000i64, 1u32)])];
    create_subset(&dir, "ts", &inputs).unwrap();

    let out = read_subset(&dir, "ts", &["close".into()]).expect("read_subset ts failed");
    assert_eq!(out.num_rows(), 1);
    let time = out
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(time.value(0), 2_000_000);
    assert_eq!(out.schema().field(0).data_type(), &ArrowDT::Timestamp(arrow_schema::TimeUnit::Microsecond, None));
}

#[test]
fn read_subset_stale_parent_detected() {
    let dir = temp_dir("stale");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();
    make_subset(&dir, "hs300");
    assert!(read_subset(&dir, "hs300", &[]).is_ok());

    // 父表 update_meta（重排 → generation+1）→ 旧子集失效。
    let sym: Vec<String> = ["SYM01", "SYM01", "SYM01", "SYM02", "SYM02", "SYM02"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let time: Vec<i64> = vec![0, 1, 2, 0, 1, 2];
    splayed_core::update_meta(&dir, TimeType::Date32, &sym, &time).unwrap();

    let err = read_subset(&dir, "hs300", &[]).unwrap_err();
    assert!(err.to_string().contains("stale"), "got: {err}");
    assert!(err.to_string().contains("generation"), "got: {err}");
}

#[test]
fn read_subset_missing_file() {
    let dir = temp_dir("missing");
    let _ = fs::remove_dir_all(&dir);
    create_table(&dir, &make_batch(), true).unwrap();
    let err = read_subset(&dir, "nope", &[]).unwrap_err();
    assert!(err.to_string().contains("nope"), "got: {err}");
}
