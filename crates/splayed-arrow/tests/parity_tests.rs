//! splayed-arrow 与 splayed-core 写接口对齐测试：`create_table_with_options` /
//! `update_table_with_options` / `update_meta` / 分区写 + 便捷扫描
//! `scan_dataset` / `scan_partitioned`（RecordBatch 输入输出）。

use std::fs;
use std::sync::Arc;

use arrow_array::{Array, Date32Array, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use splayed_arrow::{
    FieldWriteOptions, PartitionWriteInputArrow, append_partition, create_partitioned_table,
    create_table_with_options, drop_partition, scan_dataset, scan_partitioned, update_meta,
    update_partition_meta, update_partition_table, update_table_with_options,
};
use splayed_core::{
    Filter, FilterValue, PartitionScanRequest, ScanRequest, SymbolSelection, TimeRange,
};
use splayed_format::{Compression, Encoding, TimeType};

/// 2 sym × 2 time；close 含 NULL。
fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
        Field::new("alpha", ArrowDT::Float64, true),
    ]));
    let time = Date32Array::from(vec![0, 1, 0, 1]);
    let sym = StringArray::from(vec![
        Some("SYM01"), Some("SYM01"), Some("SYM02"), Some("SYM02"),
    ]);
    let close = Float64Array::from(vec![Some(100.0), None, Some(200.0), Some(201.0)]);
    // 稀疏列：只有 SYM02 有值（模拟 90%+ NULL 因子）。
    let alpha = Float64Array::from(vec![None, None, Some(1.5), None]);
    RecordBatch::try_new(
        schema,
        vec![Arc::new(time), Arc::new(sym), Arc::new(close), Arc::new(alpha)],
    )
    .unwrap()
}

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splayed_arrow_parity_{suffix}_{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn rle_zstd() -> FieldWriteOptions {
    FieldWriteOptions {
        encoding: Encoding::Rle,
        compression: Compression::Zstd,
    }
}

fn scan_all(dir: &std::path::Path, columns: Vec<String>) -> RecordBatch {
    let req = ScanRequest {
        columns,
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };
    let batches = scan_dataset(dir, &req).unwrap();
    assert!(!batches.is_empty(), "at least one batch");
    batches.into_iter().next().unwrap()
}

// ---------------------------------------------------------------------------
// create_table_with_options：全字段直接写压缩 → scan 读回
// ---------------------------------------------------------------------------

#[test]
fn create_table_with_options_compressed_scan_back() {
    let dir = temp_dir("create_opts");
    create_table_with_options(&dir, &make_batch(), true, rle_zstd()).unwrap();

    let rb = scan_all(&dir, vec!["close".into(), "alpha".into()]);

    // sym 列（index 1）。
    let sym = rb.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(sym.value(0), "SYM01");
    assert_eq!(sym.value(2), "SYM02");

    // close 列（index 2）：NULL 在 (SYM01, day1)。
    let close = rb.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(close.value(0), 100.0);
    assert!(close.is_null(1));
    assert_eq!(close.value(2), 200.0);
    assert_eq!(close.value(3), 201.0);

    // alpha 列（index 3）：只有 SYM02/day0 有值。
    let alpha = rb.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(alpha.is_null(0));
    assert!(alpha.is_null(1));
    assert_eq!(alpha.value(2), 1.5);
    assert!(alpha.is_null(3));
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// update_table_with_options：追加稀疏因子列（压缩只读）
// ---------------------------------------------------------------------------

#[test]
fn update_table_with_options_new_compressed_field() {
    let dir = temp_dir("update_opts");
    // 先建 PLAIN 可写表（仅 close）。
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1, 0, 1])),
            Arc::new(StringArray::from(vec![Some("SYM01"), Some("SYM01"), Some("SYM02"), Some("SYM02")])),
            Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)])),
        ],
    )
    .unwrap();
    create_table_with_options(&dir, &batch, true, FieldWriteOptions::default()).unwrap();

    // 追加 alpha（create_missing），直接写 RLE+ZSTD。
    let alpha_schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("alpha", ArrowDT::Float64, true),
    ]));
    let alpha_batch = RecordBatch::try_new(
        alpha_schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1, 0, 1])),
            Arc::new(StringArray::from(vec![Some("SYM01"), Some("SYM01"), Some("SYM02"), Some("SYM02")])),
            Arc::new(Float64Array::from(vec![None, Some(2.5), None, None])),
        ],
    )
    .unwrap();
    update_table_with_options(&dir, &alpha_batch, true, rle_zstd()).unwrap();

    // alpha 压缩只读；close 仍 PLAIN 可写。
    let r_new = splayed_core::FieldReader::open(dir.join("alpha")).unwrap();
    assert_eq!(r_new.header().encoding().unwrap(), Encoding::Rle);
    assert_eq!(r_new.header().compression().unwrap(), Compression::Zstd);
    drop(r_new);
    let r_close = splayed_core::FieldReader::open(dir.join("close")).unwrap();
    assert_eq!(r_close.header().compression().unwrap(), Compression::None);
    drop(r_close);

    let rb = scan_all(&dir, vec!["close".into(), "alpha".into()]);
    let alpha = rb.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(alpha.is_null(0));
    assert_eq!(alpha.value(1), 2.5);
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// update_meta（Arrow）：扩展 TIME 轴，数据保留、新行 NULL
// ---------------------------------------------------------------------------

#[test]
fn update_meta_arrow_extends_time_axis() {
    let dir = temp_dir("update_meta");
    create_table_with_options(&dir, &make_batch(), true, FieldWriteOptions::default()).unwrap();

    // 新布局：SYM01/SYM02 × [0,1,2]。
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
    ]));
    let new_keys = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1, 2, 0, 1, 2])),
            Arc::new(StringArray::from(vec![
                Some("SYM01"), Some("SYM01"), Some("SYM01"),
                Some("SYM02"), Some("SYM02"), Some("SYM02"),
            ])),
        ],
    )
    .unwrap();
    let meta = update_meta(&dir, &new_keys, true).unwrap();
    assert_eq!(meta.total_rows(), 6, "3 time × 2 sym");

    let rb = scan_all(&dir, vec!["close".into()]);
    assert_eq!(rb.num_rows(), 6);
    let close = rb.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    // SYM01/day0=100, day1=NULL, day2=NULL；SYM02/day0=200, day1=201, day2=NULL。
    assert_eq!(close.value(0), 100.0);
    assert!(close.is_null(1));
    assert!(close.is_null(2));
    assert_eq!(close.value(3), 200.0);
    assert_eq!(close.value(4), 201.0);
    assert!(close.is_null(5));
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// scan_dataset：值过滤 + LIMIT 下推
// ---------------------------------------------------------------------------

#[test]
fn scan_dataset_filter_and_limit() {
    let dir = temp_dir("filter_limit");
    create_table_with_options(&dir, &make_batch(), true, FieldWriteOptions::default()).unwrap();

    use splayed_core::FilterValue;
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![Filter::GreaterOrEqual {
            field: "close".into(),
            value: FilterValue::Float64(200.0),
        }],
        batch_size: 65536,
        parallelism: 1,
        limit: Some(1),
    };
    let batches = scan_dataset(&dir, &req).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "LIMIT 1");
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 分区写（Arrow）+ scan_partitioned
// ---------------------------------------------------------------------------

fn partition_batch(sym: &str, days: &[i32], close: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(days.to_vec())),
            Arc::new(StringArray::from(vec![Some(sym); days.len()])),
            Arc::new(Float64Array::from(vec![Some(close); days.len()])),
        ],
    )
    .unwrap()
}

#[test]
fn partition_arrow_create_append_drop_scan() {
    let root = temp_dir("part_root");
    let in2024 = PartitionWriteInputArrow::new("month=2024-01", partition_batch("SYM01", &[0, 1], 100.0), true);
    let in2025 = PartitionWriteInputArrow::new("month=2025-01", partition_batch("SYM01", &[0, 1], 200.0), true);
    create_partitioned_table(&root, TimeType::Date32, &[in2024, in2025], rle_zstd()).unwrap();

    // 分区写压缩：读 header 校验。
    let r = splayed_core::FieldReader::open(root.join("month=2024-01").join("close")).unwrap();
    assert_eq!(r.header().encoding().unwrap(), Encoding::Rle);
    assert_eq!(r.header().compression().unwrap(), Compression::Zstd);
    drop(r);

    // scan_partitioned 合并两分区。
    let req = splayed_core::PartitionScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![],
        partition_filters: vec![],
        batch_size: 65536,
        parallelism: 1,
    };
    let batches = scan_partitioned(&root, &req).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4, "2 分区 × 2 行");

    // 分区列剪裁：只扫 2025。
    let req2025 = PartitionScanRequest {
        partition_filters: vec![Filter::Equal {
            field: "month".into(),
            value: FilterValue::String("2025-01".into()),
        }],
        ..req.clone()
    };
    let batches = scan_partitioned(&root, &req2025).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "只扫 month=2025-01");

    // append_partition（压缩）。
    let in2026 = PartitionWriteInputArrow::new("month=2026-01", partition_batch("SYM01", &[0, 1], 300.0), true);
    append_partition(&root, &in2026, rle_zstd()).unwrap();
    let batches = scan_partitioned(&root, &req).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 6);

    // drop_partition。
    drop_partition(&root, "month=2025-01").unwrap();
    let batches = scan_partitioned(&root, &req).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4, "drop 后剩两分区");
    let _ = fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// update_partition_table / update_partition_meta（Arrow）
// ---------------------------------------------------------------------------

#[test]
fn partition_arrow_update_table_and_meta() {
    let root = temp_dir("part_update");
    let in2024 = PartitionWriteInputArrow::new("month=2024-01", partition_batch("SYM01", &[0, 1], 100.0), true);
    create_partitioned_table(&root, TimeType::Date32, &[in2024], FieldWriteOptions::default())
        .unwrap();

    // update_partition_table：更新 2024 分区内已有行（覆盖 close）。
    let upd = partition_batch("SYM01", &[0], 111.0);
    update_partition_table(&root, &upd, false, Some("month=2024-01"), FieldWriteOptions::default())
        .unwrap();

    let req = splayed_core::PartitionScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![],
        partition_filters: vec![],
        batch_size: 65536,
        parallelism: 1,
    };
    let batches = scan_partitioned(&root, &req).unwrap();
    let rb = batches.into_iter().next().unwrap();
    let close = rb.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(close.value(0), 111.0, "覆盖成功");
    assert_eq!(close.value(1), 100.0);

    // update_partition_meta：新增分区 2025（压缩），既有 2024 布局不变。
    let in2025 = PartitionWriteInputArrow::new("month=2025-01", partition_batch("SYM01", &[0, 1], 200.0), true);
    update_partition_meta(&root, &[mk_2024_input(), in2025], rle_zstd()).unwrap();
    let batches = scan_partitioned(&root, &req).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4);
    let _ = fs::remove_dir_all(&root);
}

/// 重建 2024 分区输入（保持既有分区 → update_meta 走布局重排而非重建）。
fn mk_2024_input() -> PartitionWriteInputArrow {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    let data = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec![Some("SYM01"), Some("SYM01")])),
            Arc::new(Float64Array::from(vec![Some(100.0), Some(100.0)])),
        ],
    )
    .unwrap();
    PartitionWriteInputArrow::new("month=2024-01", data, true)
}
