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

// ---------------------------------------------------------------------------
// P0 correctness regressions
// ---------------------------------------------------------------------------

/// Every pushed-down value filter must be applied — not just the first.
#[tokio::test]
async fn multi_value_filters_all_applied() {
    let dir = temp_dir("multi_val");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches =
        run_query(&dir, "SELECT close, volume FROM splayed WHERE close > 100 AND volume > 3000").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // SYM01: close>100 (days 1-4) ∩ volume>3000 (days 3,4) = days 3,4 → 2
    // SYM02: close>100 (all)     ∩ volume>3000 (days 2,3,4) = days 2,3,4 → 3
    assert_eq!(total_rows, 5);

    let batch = &batches[0];
    let close = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let volume = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert!(close.value(i) > 100.0, "close must be > 100");
        assert!(volume.value(i) > 3000, "volume must be > 3000");
    }

    fs::remove_dir_all(&dir).ok();
}

/// `sym = '<missing>'` must return 0 rows — never an error.
#[tokio::test]
async fn unknown_sym_returns_empty() {
    let dir = temp_dir("unknown_sym");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE sym = 'NOPE'").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    fs::remove_dir_all(&dir).ok();
}

/// A value filter on a column that is not projected must still filter rows.
#[tokio::test]
async fn filter_column_not_projected() {
    let dir = temp_dir("not_proj");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE volume > 5000").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // volume > 5000: only SYM02 day 4 (6000) → 1 row, close = 204
    assert_eq!(total_rows, 1);
    let batch = &batches[0];
    let close = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(close.value(0), 204.0);

    fs::remove_dir_all(&dir).ok();
}

/// LIMIT must return at most n rows.
#[tokio::test]
async fn limit_rows() {
    let dir = temp_dir("limit");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed LIMIT 3").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    fs::remove_dir_all(&dir).ok();
}

/// `sym IN (...)` is not pushed down (P2) but must still be correct via
/// DataFusion's own filter.
#[tokio::test]
async fn sym_in_list_correct() {
    let dir = temp_dir("sym_in");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE sym IN ('SYM01','SYM02')").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10);

    fs::remove_dir_all(&dir).ok();
}

/// Contradictory symbol conjunction must return empty (intersection semantics).
#[tokio::test]
async fn contradictory_sym_filters_empty() {
    let dir = temp_dir("contra_sym");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE sym = 'SYM01' AND sym = 'SYM02'").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    fs::remove_dir_all(&dir).ok();
}

/// Extended fixed-width field types (Phase 2): Int16 / UInt32 / UInt64 / Date64
/// must survive the full create_table → scan → DataFusion path.
#[tokio::test]
async fn extended_field_types_query() {
    let dir = temp_dir("ext_df");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("time", ArrowDT::Date32, false),
            Field::new("sym", ArrowDT::Utf8, false),
            Field::new("i16", ArrowDT::Int16, true),
            Field::new("u32", ArrowDT::UInt32, true),
            Field::new("count", ArrowDT::UInt64, false),
            Field::new("d64", ArrowDT::Date64, true),
        ])),
        vec![
            Arc::new(Date32Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec![Some("SYM01"), Some("SYM02")])),
            Arc::new(arrow_array::Int16Array::from(vec![Some(7), None])),
            Arc::new(arrow_array::UInt32Array::from(vec![Some(10), Some(20)])),
            Arc::new(arrow_array::UInt64Array::from(vec![100, 200])),
            Arc::new(arrow_array::Date64Array::from(vec![Some(86400000), Some(172800000)])),
        ],
    )
    .unwrap();
    create_table(&dir, &batch, true).unwrap();

    // Full projection returns all 2 rows with correct values.
    let all = run_query(&dir, "SELECT i16, u32, count FROM splayed").await;
    assert_eq!(all.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    // Value filter on a UInt32 field (not pushed down, but correct via DF).
    let f = run_query(&dir, "SELECT count FROM splayed WHERE u32 >= 20").await;
    let total: usize = f.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
    let count = f
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .unwrap()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(count, vec![200]);

    // Empty (non-null) i16 values read back correctly.
    let i = run_query(&dir, "SELECT i16 FROM splayed WHERE i16 IS NOT NULL").await;
    let vals: Vec<i16> = i
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int16Array>()
                .unwrap()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(vals, vec![7]);

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Deferred items now implemented: sym IN (...), IS [NOT] NULL, identity casts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn in_filter_known_symbols() {
    let dir = temp_dir("in_known");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE sym IN ('SYM02')").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5); // SYM02 only

    fs::remove_dir_all(&dir).ok();
}

/// Unknown literals in an IN list must not leak rows from the known ones.
#[tokio::test]
async fn in_filter_mixed_unknown() {
    let dir = temp_dir("in_mixed");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE sym IN ('SYM01', 'NOPE')").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5); // SYM01 only — 'NOPE' yields nothing

    fs::remove_dir_all(&dir).ok();
}

/// An IN list with only unknown symbols must return 0 rows.
#[tokio::test]
async fn in_filter_all_unknown_empty() {
    let dir = temp_dir("in_unknown");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE sym IN ('NOPE1','NOPE2')").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    fs::remove_dir_all(&dir).ok();
}

/// A batch whose `close` column contains a NULL (Float64 canonical NaN).
fn make_null_close_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1, 2])),
            Arc::new(StringArray::from(vec![Some("SYM01"), Some("SYM01"), Some("SYM01")])),
            Arc::new(Float64Array::from(vec![Some(100.0), None, Some(102.0)])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn is_null_filter() {
    let dir = temp_dir("is_null");
    create_table(&dir, &make_null_close_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE close IS NULL").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn is_not_null_filter() {
    let dir = temp_dir("is_not_null");
    create_table(&dir, &make_null_close_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed WHERE close IS NOT NULL").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2); // skips the NULL row

    fs::remove_dir_all(&dir).ok();
}

/// An identity cast (`CAST(close AS DOUBLE)` on a Float64 column) must still
/// push down as a plain value filter.
#[tokio::test]
async fn identity_cast_filter() {
    let dir = temp_dir("cast_id");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches =
        run_query(&dir, "SELECT close FROM splayed WHERE CAST(close AS DOUBLE) > 103").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // close > 103: SYM01 day 4 (104) → 1; SYM02 all of 200-204 → 5. Total 6.
    assert_eq!(total_rows, 6);
    let batch = &batches[0];
    let close = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    for i in 0..batch.num_rows() {
        assert!(close.value(i) > 103.0);
    }

    fs::remove_dir_all(&dir).ok();
}

/// LIMIT 下推到扫描/执行：只返回前 N 行（读取期截断）。
#[tokio::test]
async fn limit_pushdown_rows() {
    let dir = temp_dir("limit_pd");
    create_table(&dir, &make_batch(), true).unwrap();

    let batches = run_query(&dir, "SELECT close FROM splayed LIMIT 3").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    let batch = &batches[0];
    let close = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        (0..close.len()).map(|i| close.value(i)).collect::<Vec<_>>(),
        vec![100.0, 101.0, 102.0]
    );

    fs::remove_dir_all(&dir).ok();
}

/// FIELD footer 统计（min/max）上报到 DataFusion `statistics()`。
#[tokio::test]
async fn statistics_exposes_min_max() {
    use datafusion::common::stats::Precision;
    use datafusion::catalog::TableProvider;

    let dir = temp_dir("stats_df");
    create_table(&dir, &make_batch(), true).unwrap();

    let provider = SplayedTableProvider::new(&dir).unwrap();
    let stats = provider.statistics().expect("statistics");
    assert_eq!(stats.num_rows, Precision::Exact(10));
    // column_statistics: [time, sym, close, volume]
    let close_stats = &stats.column_statistics[2];
    assert_eq!(
        close_stats.min_value,
        Precision::Exact(datafusion::common::ScalarValue::Float64(Some(100.0)))
    );
    assert_eq!(
        close_stats.max_value,
        Precision::Exact(datafusion::common::ScalarValue::Float64(Some(204.0)))
    );
    let vol_stats = &stats.column_statistics[3];
    assert_eq!(
        vol_stats.min_value,
        Precision::Exact(datafusion::common::ScalarValue::Int64(Some(1000)))
    );
    assert_eq!(
        vol_stats.max_value,
        Precision::Exact(datafusion::common::ScalarValue::Int64(Some(6000)))
    );

    fs::remove_dir_all(&dir).ok();
}

/// 扫描期统计剪裁：filter 与列 min/max 不相交 → 空结果（正确性端到端）。
#[tokio::test]
async fn stats_pruned_filter_empty() {
    let dir = temp_dir("prune_df");
    create_table(&dir, &make_batch(), true).unwrap();

    for sql in [
        "SELECT close FROM splayed WHERE close > 300",
        "SELECT close FROM splayed WHERE close > 204",
        "SELECT close FROM splayed WHERE close < 100",
        "SELECT close FROM splayed WHERE volume > 100000",
    ] {
        let batches = run_query(&dir, sql).await;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "sql: {sql}");
    }

    // 有命中时不误剪。
    let batches = run_query(&dir, "SELECT close FROM splayed WHERE close >= 200").await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    fs::remove_dir_all(&dir).ok();
}

/// `update_meta` 联动：core 重排布局后，`provider.reload()` 使 DataFusion
/// 看到新数据（schema/统计 重建）。
#[tokio::test]
async fn update_meta_reload_reflects() {
    use datafusion::catalog::TableProvider;
    use splayed_core::update_meta;

    let dir = temp_dir("um_reload");
    create_table(&dir, &make_batch(), true).unwrap(); // 2 sym × 5 time = 10 行

    let mut provider = splayed_datafusion::SplayedDatasetProvider::new(&dir).unwrap();
    let before = provider.statistics().unwrap();
    assert_eq!(before.num_rows, datafusion::common::stats::Precision::Exact(10));

    // core 重排：只保留 SYM01 的 t0..2（3 行）。
    let syms: Vec<String> = ["SYM01", "SYM01", "SYM01"]
        .map(|s| s.to_string())
        .to_vec();
    update_meta(&dir, splayed_format::TimeType::Date32, &syms, &[0, 1, 2]).unwrap();

    provider.reload().unwrap();
    let after = provider.statistics().unwrap();
    assert_eq!(after.num_rows, datafusion::common::stats::Precision::Exact(3));

    // 注册后查询也看到新布局。
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_table("splayed", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql("SELECT close FROM splayed ORDER BY time")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
    let close = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        (0..close.len()).map(|i| close.value(i)).collect::<Vec<_>>(),
        vec![100.0, 101.0, 102.0]
    );

    fs::remove_dir_all(&dir).ok();
}

/// 声明式分区列（key=value 目录）：schema 追加虚拟列、谓词下推到分区剪裁、
/// 扫描结果带常量分区列。
#[tokio::test]
async fn partition_column_sql() {
    let root = temp_dir("kv_df");
    create_table(&root.join("year=2024"), &make_batch(), true).unwrap(); // 10 行
    create_table(&root.join("year=2025"), &make_batch(), true).unwrap();

    // 等值 → 只扫 2024 分区；year 投影为常量列。
    let batches = run_query(&root, "SELECT close, year FROM splayed WHERE year = 2024").await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 10);
    let year = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    for i in 0..year.len() {
        assert_eq!(year.value(i), 2024);
    }

    // 数值范围 → 只扫 2025 分区。
    let b2 = run_query(&root, "SELECT close FROM splayed WHERE year > 2024").await;
    let total2: usize = b2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total2, 10);

    // 字符串字面量等值同样剪裁（year = '2024'）。
    let b3 = run_query(&root, "SELECT COUNT(*) AS c FROM splayed WHERE year = '2024'").await;
    let c = b3[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 10);

    // DISTINCT 分区列值 = 2。
    let b4 = run_query(&root, "SELECT COUNT(DISTINCT year) AS n FROM splayed").await;
    let n = b4[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 2);

    fs::remove_dir_all(&root).ok();
}

/// 分区级符号剪裁透传：符号只存在于部分分区时，core 分区计划剔除缺失分区的
/// 符号并剪掉该分区，dataset 扫描用有效子集（不再因缺符号报错）。
#[tokio::test]
async fn partition_symbol_prune_passes_override() {
    fn one_sym_batch(sym: &str, base: f64) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("time", ArrowDT::Date32, false),
                Field::new("sym", ArrowDT::Utf8, false),
                Field::new("close", ArrowDT::Float64, true),
            ])),
            vec![
                Arc::new(Date32Array::from(vec![0, 1, 2])),
                Arc::new(StringArray::from(vec![Some(sym); 3])),
                Arc::new(Float64Array::from(vec![base, base + 1.0, base + 2.0])),
            ],
        )
        .unwrap()
    }

    let root = temp_dir("sym_prune");
    create_table(&root.join("2024"), &one_sym_batch("SYM01", 100.0), true).unwrap();
    create_table(&root.join("2025"), &one_sym_batch("SYM02", 200.0), true).unwrap();

    // SYM02 只在 2025：2024 被剪掉，返回值 = 2025 的 3 行。
    let batches = run_query(&root, "SELECT close FROM splayed WHERE sym = 'SYM02'").await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
    let close = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(
        (0..close.len()).map(|i| close.value(i)).collect::<Vec<_>>(),
        vec![200.0, 201.0, 202.0]
    );

    fs::remove_dir_all(&root).ok();
}

/// 聚合下推：无过滤的 MIN/MAX/COUNT 命中 footer 统计 → 常量单行计划（扫描跳过）。
async fn ctx_with_rule() -> datafusion::prelude::SessionContext {
    let builder = datafusion::execution::SessionStateBuilder::new()
        .with_default_features()
        .with_optimizer_rule(std::sync::Arc::new(
            splayed_datafusion::SplayedStatsAggRule,
        ));
    let state = builder.build();
    let ctx = datafusion::prelude::SessionContext::new_with_state(state);
    let dir = temp_dir("agg_rule");
    create_table(&dir, &make_null_close_batch(), true).unwrap(); // close [100, None, 102]
    let provider = splayed_datafusion::SplayedDatasetProvider::new(&dir).unwrap();
    ctx.register_table("splayed", Arc::new(provider)).unwrap();
    ctx
}

#[tokio::test]
async fn stats_agg_rule_answers_without_filter() {
    let ctx = ctx_with_rule().await;

    // MIN/MAX/COUNT(*)/COUNT(col) → 常量答案。
    let batches = ctx
        .sql("SELECT MIN(close), MAX(close), COUNT(*), COUNT(close) FROM splayed")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let b = &batches[0];
    assert_eq!(b.num_rows(), 1);
    assert_eq!(
        b.column(0).as_any().downcast_ref::<arrow_array::Float64Array>().unwrap().value(0),
        100.0
    );
    assert_eq!(
        b.column(1).as_any().downcast_ref::<arrow_array::Float64Array>().unwrap().value(0),
        102.0
    );
    assert_eq!(
        b.column(2).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(0),
        3
    );
    assert_eq!(
        b.column(3).as_any().downcast_ref::<arrow_array::Int64Array>().unwrap().value(0),
        2 // close 有一个 NULL
    );

    // 带过滤 → 不改写，走正常扫描但仍正确。
    let batches2 = ctx
        .sql("SELECT COUNT(*) FROM splayed WHERE close IS NOT NULL")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let c = batches2[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 2);

    let dir = temp_dir("agg_rule_cleanup");
    let _ = std::fs::remove_dir_all(&dir);
}

/// 分区写能力联动：create_partitioned_table / update_partition_table /
/// append_partition 之后 `SplayedTableProvider::reload()` 让 SQL 立即看到。
#[tokio::test]
async fn partition_write_reload_reflects() {
    use splayed_core::{
        PartitionWriteInput, append_partition, create_partitioned_table, update_partition_table,
    };
    use splayed_format::{DataType as ST, RawValue, TimeType};

    let root = temp_dir("pw_reload");
    let mk = |vals: &[f64]| {
        let mut b = vec![0u8; vals.len() * 8];
        for (i, v) in vals.iter().enumerate() {
            RawValue::from_f64(*v).write_le(&mut b, i * 8);
        }
        b
    };
    let col = |name: &str, values: Vec<u8>| splayed_core::TableColumn {
        name: name.to_string(),
        data_type: ST::Float64,
        values,
    };
    let inp = |name: &str, base: f64| {
        let mut s = Vec::new();
        let mut t = Vec::new();
        let mut vals = Vec::new();
        for si in 0..2i64 {
            for ti in 0..3i64 {
                s.push(if si == 0 { "SYM01" } else { "SYM02" }.to_string());
                t.push(ti);
                vals.push(base + (si * 100) as f64 + ti as f64);
            }
        }
        PartitionWriteInput {
            name: name.to_string(),
            time_type: TimeType::Date32,
            sym: s,
            time: t,
            columns: vec![col("close", mk(&vals))],
            sorted: true,
        }
    };
    create_partitioned_table(&root, TimeType::Date32, &[inp("2024", 100.0), inp("2025", 500.0)])
        .unwrap();

    let mut provider = splayed_datafusion::SplayedTableProvider::new(&root).unwrap();
    let count = |ctx: SessionContext| async move {
        ctx.sql("SELECT COUNT(*) AS c FROM splayed")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0)
    };
    let ctx = SessionContext::new();
    ctx.register_table("splayed", Arc::new(provider.clone())).unwrap();
    assert_eq!(count(ctx.clone()).await, 12);

    // 格子更新（2024 的 SYM01 t0 → 999）+ reload。
    let cols = vec![col("close", mk(&[999.0]))];
    update_partition_table(
        &root,
        &["SYM01".to_string()],
        &[0i64],
        &cols,
        false,
        Some("2024"),
    )
    .unwrap();
    provider.reload().unwrap();
    let ctx2 = SessionContext::new();
    ctx2.register_table("splayed", Arc::new(provider.clone())).unwrap();
    let v = ctx2
        .sql("SELECT close FROM splayed WHERE time = 0 AND sym = 'SYM01'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(v, 999.0);

    // 追加分区 + reload → 行数变化。
    append_partition(&root, &inp("2026", 900.0)).unwrap();
    provider.reload().unwrap();
    let ctx3 = SessionContext::new();
    ctx3.register_table("splayed", Arc::new(provider)).unwrap();
    assert_eq!(count(ctx3.clone()).await, 18); // 12 + 6

    fs::remove_dir_all(&root).ok();
}
