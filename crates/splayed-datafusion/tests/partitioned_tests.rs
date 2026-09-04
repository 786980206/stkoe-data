//! Integration tests for the three-layer Splayed DataFusion integration:
//! single-dataset provider (Layer 1) → partitioned table (Layer 2) → DataFusion
//! binding (Layer 3).

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::{Date32Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use datafusion::common::stats::Precision;
use datafusion::datasource::TableProvider;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use splayed_arrow::create_table;
use splayed_datafusion::{
    SplayedDatasetProvider, SplayedSubsetFunction, SplayedTableFactory, SplayedTableFunction,
    SplayedTableProvider, register_splayed_table,
};

/// Global sequence so concurrently-running tests never share a temp dir.
static DIR_SEQ: AtomicUsize = AtomicUsize::new(0);

fn temp_dir(suffix: &str) -> PathBuf {
    let seq = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "splayed_part_{suffix}_{seq}_{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// A single-SYM batch over the given days with `close = base + i`.
fn make_batch(days: &[i32], base: f64) -> RecordBatch {
    let n = days.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    let time = Date32Array::from(days.to_vec());
    let sym = StringArray::from(vec![Some("SYM01"); n]);
    let close = Float64Array::from(
        (0..n)
            .map(|i| Some(base + i as f64))
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(schema, vec![Arc::new(time), Arc::new(sym), Arc::new(close)]).unwrap()
}

/// A partitioned table directory: `2024/` (days 0-4, close 100-104) and
/// `2025/` (days 100-104, close 200-204), each its own `.meta` dataset.
fn make_table_dir() -> PathBuf {
    let root = temp_dir("part_table");
    create_table(root.join("2024"), &make_batch(&[0, 1, 2, 3, 4], 100.0), true).unwrap();
    create_table(root.join("2025"), &make_batch(&[100, 101, 102, 103, 104], 200.0), true).unwrap();
    root
}

async fn run_query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn count_value(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .flatten()
        })
        .sum()
}

async fn register_and_query(root: &PathBuf, sql: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    register_splayed_table(&ctx, "t", root).unwrap();
    run_query(&ctx, sql).await
}

// ---------------------------------------------------------------------------
// Layer 2 — partitioned table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn partitioned_table_union() {
    let root = make_table_dir();
    let batches = register_and_query(&root, "SELECT * FROM t").await;
    assert_eq!(total_rows(&batches), 10); // 5 + 5
    fs::remove_dir_all(&root).ok();
}

/// A time filter must prune whole partitions (2025 is outside days < 50).
#[tokio::test]
async fn time_filter_prunes_partition() {
    let root = make_table_dir();

    let early = register_and_query(&root, "SELECT close FROM t WHERE time < 50").await;
    assert_eq!(total_rows(&early), 5); // only 2024
    let close = early[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(close.value(0), 100.0);

    let late = register_and_query(&root, "SELECT close FROM t WHERE time >= 50").await;
    assert_eq!(total_rows(&late), 5); // only 2025
    let close = late[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(close.value(0), 200.0);

    fs::remove_dir_all(&root).ok();
}

/// A symbol that exists in only one partition must return 0 rows from the
/// other partition (never leak its rows).
#[tokio::test]
async fn sym_in_one_partition() {
    let root = temp_dir("sym_one_part");
    // 2024 has SYM01, 2025 has SYM02.
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    let mk = |sym: &str, days: &[i32], base: f64| {
        let n = days.len();
        let time = Date32Array::from(days.to_vec());
        let s = StringArray::from(vec![Some(sym.to_string()); n]);
        let c = Float64Array::from((0..n).map(|i| Some(base + i as f64)).collect::<Vec<_>>());
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(time), Arc::new(s), Arc::new(c)],
        )
        .unwrap()
    };
    create_table(root.join("2024"), &mk("SYM01", &[0, 1, 2], 100.0), true).unwrap();
    create_table(root.join("2025"), &mk("SYM02", &[0, 1, 2], 200.0), true).unwrap();

    // sym = SYM02 must return exactly SYM02's rows — 2024 must not leak SYM01 rows.
    let batches = register_and_query(&root, "SELECT sym, close FROM t WHERE sym = 'SYM02'").await;
    assert_eq!(total_rows(&batches), 3);
    let sym = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..batches[0].num_rows() {
        assert_eq!(sym.value(i), "SYM02");
    }

    fs::remove_dir_all(&root).ok();
}

/// A single `.meta` directory is a valid single-partition table (backward
/// compatible with the pre-partition API).
#[tokio::test]
async fn single_meta_dir_is_single_partition() {
    let single = temp_dir("single_part");
    create_table(&single, &make_batch(&[0, 1, 2], 100.0), true).unwrap();

    let provider = SplayedTableProvider::new(&single).unwrap();
    assert_eq!(provider.partitions().len(), 1);

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    let batches = run_query(&ctx, "SELECT COUNT(*) FROM t").await;
    assert_eq!(count_value(&batches), 3);

    fs::remove_dir_all(&single).ok();
}

/// Partitions with mismatched schemas must be rejected at construction.
#[tokio::test]
async fn schema_mismatch_rejected() {
    let root = temp_dir("mismatch");
    // 2024: close; 2025: volume — different field sets.
    create_table(root.join("2024"), &make_batch(&[0, 1], 100.0), true).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("volume", ArrowDT::Int64, true),
    ]));
    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec![Some("SYM01"), Some("SYM01")])),
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
        ],
    )
    .unwrap();
    create_table(root.join("2025"), &rb, true).unwrap();

    assert!(SplayedTableProvider::new(&root).is_err());

    fs::remove_dir_all(&root).ok();
}

/// Table-level statistics must aggregate across partitions.
#[tokio::test]
async fn statistics_aggregate() {
    let root = make_table_dir();
    let provider = SplayedTableProvider::new(&root).unwrap();
    let stats = provider.statistics().unwrap();
    assert_eq!(stats.num_rows, Precision::Exact(10));
    fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// Layer 1 — single dataset provider
// ---------------------------------------------------------------------------

#[tokio::test]
async fn layer1_dataset_provider_direct() {
    let single = temp_dir("layer1");
    create_table(&single, &make_batch(&[0, 1, 2], 100.0), true).unwrap();

    let ctx = SessionContext::new();
    let provider = SplayedDatasetProvider::new(&single).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let batches = run_query(&ctx, "SELECT close FROM t WHERE close >= 101").await;
    assert_eq!(total_rows(&batches), 2);
    fs::remove_dir_all(&single).ok();
}

/// A single dataset sliced into multiple DataFusion output partitions must
/// return identical results (COUNT / filters / ORDER BY) to the serial scan.
#[tokio::test]
async fn layer1_multi_partition_parallel_scan() {
    let single = temp_dir("layer1_par");
    create_table(&single, &make_batch(&[0, 1, 2, 3, 4], 100.0), true).unwrap();

    let ctx = SessionContext::new();
    let provider = SplayedDatasetProvider::new(&single)
        .unwrap()
        .with_scan_parallelism(4);
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // All rows present, in (sym, time) order (ORDER BY forces the merge path).
    let all = run_query(&ctx, "SELECT sym, time, close FROM t ORDER BY sym, time").await;
    assert_eq!(total_rows(&all), 5);
    let close = all
        .iter()
        .flat_map(|b| {
            b.column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(close, vec![100.0, 101.0, 102.0, 103.0, 104.0]);

    // Filters + aggregates still exact.
    let cnt = run_query(&ctx, "SELECT COUNT(*) AS c FROM t WHERE close >= 102").await;
    assert_eq!(count_value(&cnt), 3);

    // LIMIT honored even with multiple partitions.
    let lim = run_query(&ctx, "SELECT close FROM t LIMIT 2").await;
    assert_eq!(total_rows(&lim), 2);

    fs::remove_dir_all(&single).ok();
}

/// Layer 2: per-partition slicing can be enabled table-wide.
#[tokio::test]
async fn layer2_with_scan_parallelism() {
    let root = make_table_dir();
    let provider = SplayedTableProvider::new(&root)
        .unwrap()
        .with_scan_parallelism(2);

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    let batches = run_query(&ctx, "SELECT COUNT(*) AS c FROM t WHERE close > 150").await;
    // 2024 close 100-104 (none > 150), 2025 close 200-204 (all 5) → 5
    assert_eq!(count_value(&batches), 5);

    let ord = run_query(&ctx, "SELECT close FROM t ORDER BY close LIMIT 3").await;
    assert_eq!(
        ord.iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
        vec![100.0, 101.0, 102.0]
    );

    fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// Layer 3 — DataFusion binding
// ---------------------------------------------------------------------------

#[tokio::test]
async fn table_function_read_splayed() {
    let root = make_table_dir();
    let ctx = SessionContext::new();
    ctx.register_udtf("read_splayed", Arc::new(SplayedTableFunction));

    let path = root.display().to_string().replace('\\', "/");
    let batches = run_query(
        &ctx,
        &format!("SELECT COUNT(*) AS c FROM read_splayed('{path}')"),
    )
    .await;
    assert_eq!(count_value(&batches), 10);
    fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn create_external_table_factory() {
    let root = make_table_dir();
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_table_factory("SPLAYED".to_string(), Arc::new(SplayedTableFactory))
        .build();
    let ctx = SessionContext::new_with_state(state);

    let path = root.display().to_string().replace('\\', "/");
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE t STORED AS SPLAYED LOCATION '{path}'"
    ))
    .await
    .unwrap();

    let batches = run_query(&ctx, "SELECT COUNT(*) AS c FROM t").await;
    assert_eq!(count_value(&batches), 10);
    fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn table_function_read_splayed_subset() {
    let root = temp_dir("subset");
    // 2 SYM × 2 天：SYM01 close 100,101；SYM02 close 200,201（父全局行序）。
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    let rb = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1, 0, 1])),
            Arc::new(StringArray::from(vec!["SYM01", "SYM01", "SYM02", "SYM02"])),
            Arc::new(Float64Array::from(vec![100.0, 101.0, 200.0, 201.0])),
        ],
    )
    .unwrap();
    create_table(&root, &rb, true).unwrap();

    // 子集：SYM01 两天 {0,1} + SYM02 只取 day0。
    splayed_core::create_subset(
        &root,
        "hs300",
        &[
            splayed_core::SubsetInput::new("SYM01", vec![(0i64, 2u32)]),
            splayed_core::SubsetInput::new("SYM02", vec![(0i64, 1u32)]),
        ],
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_udtf("read_splayed_subset", Arc::new(SplayedSubsetFunction));

    let path = root.display().to_string().replace('\\', "/");
    let batches = run_query(
        &ctx,
        &format!(
            "SELECT COUNT(*) AS c, SUM(close) AS s FROM read_splayed_subset('{path}', 'hs300')"
        ),
    )
    .await;
    assert_eq!(batches[0].num_rows(), 1); // 聚合单行
    assert_eq!(count_value(&batches), 3); // COUNT(*) = 子集行数
    let sum = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0);
    assert_eq!(sum, 100.0 + 101.0 + 200.0); // 父全局行序

    // 直接 SELECT 子集行。
    let batches = run_query(
        &ctx,
        &format!("SELECT sym, close FROM read_splayed_subset('{path}', 'hs300') ORDER BY close"),
    )
    .await;
    let closes: Vec<f64> = batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(closes, vec![100.0, 101.0, 200.0]);

    fs::remove_dir_all(&root).ok();
}
