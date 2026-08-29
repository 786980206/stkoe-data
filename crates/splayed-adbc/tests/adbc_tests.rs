//! splayed-adbc 集成测试：上层 ADBC 驱动（内部经 DataFusion 执行 SQL →
//! Arrow 结果流），覆盖全表 / 过滤 / 聚合 / 流式迭代 / 分区表。

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray as ArrowStringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use splayed_arrow::create_table;
use splayed_adbc::Connection;

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splayed_adbc_{suffix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// time 0..4 × SYM01/02，close 100..104 / 200..204，vol 1000.. / 2000..（含一个 NULL）
fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
        Field::new("vol", ArrowDT::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Date32Array::from(vec![
                0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
            ])),
            Arc::new(ArrowStringArray::from(vec![
                "SYM01", "SYM01", "SYM01", "SYM01", "SYM01", "SYM02", "SYM02", "SYM02", "SYM02",
                "SYM02",
            ])),
            Arc::new(Float64Array::from(vec![
                100.0, 101.0, 102.0, 103.0, 104.0, 200.0, 201.0, 202.0, 203.0, 204.0,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1000),
                Some(1001),
                None,
                Some(1003),
                Some(1004),
                Some(2000),
                Some(2001),
                Some(2002),
                Some(2003),
                Some(2004),
            ])),
        ],
    )
    .unwrap()
}

fn fold_rows(conn_result: Vec<RecordBatch>) -> usize {
    conn_result.iter().map(|rb| rb.num_rows()).sum()
}

#[test]
fn sql_filter_and_projection() {
    let dir = temp_dir("sql_filter");
    create_table(&dir, &make_batch(), true).unwrap();
    let conn = Connection::open(&dir).unwrap();

    // SQL 由 DataFusion 执行：过滤 + 投影。
    let rows = conn
        .execute("SELECT close FROM splayed WHERE sym = 'SYM02' AND close >= 201")
        .unwrap();
    assert_eq!(fold_rows(rows), 4); // 201..204

    // 聚合（SQL 能力来自 DataFusion，adbc 不复实现解析）。
    let count = conn.execute("SELECT COUNT(*) AS c FROM splayed").unwrap();
    let c = count[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 10);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn streamed_iteration() {
    let dir = temp_dir("stream");
    create_table(&dir, &make_batch(), true).unwrap();
    let conn = Connection::open(&dir).unwrap();

    let stream = conn.statement("SELECT sym, close FROM splayed").execute().unwrap();
    let mut total = 0usize;
    let mut first_sym: Option<String> = None;
    for rb in stream {
        let rb = rb.unwrap();
        total += rb.num_rows();
        if first_sym.is_none() {
            let s = rb
                .column(0)
                .as_any()
                .downcast_ref::<ArrowStringArray>()
                .unwrap();
            first_sym = Some(s.value(0).to_string());
        }
    }
    assert_eq!(total, 10);
    assert_eq!(first_sym.as_deref(), Some("SYM01"));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn null_values_preserved() {
    let dir = temp_dir("nulls");
    create_table(&dir, &make_batch(), true).unwrap();
    let conn = Connection::open(&dir).unwrap();

    // vol 列恰有一个 NULL（SYM01 第 2 天）。
    let rows = conn.execute("SELECT vol FROM splayed WHERE vol IS NULL").unwrap();
    assert_eq!(fold_rows(rows), 1);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn partitioned_table_open() {
    let root = temp_dir("parts");
    // 两个 dataset 目录做分区表（自动探测）。
    create_table(&root.join("2024"), &make_batch(), true).unwrap();
    create_table(&root.join("2025"), &make_batch(), true).unwrap();

    let conn = Connection::open(&root).unwrap();
    let rows = conn.execute("SELECT close FROM splayed").unwrap();
    assert_eq!(fold_rows(rows), 20); // 10 + 10

    std::fs::remove_dir_all(&root).ok();
}

/// `update_meta` 联动：core 重排后 `Connection::refresh()` 使后续 SQL 看到
/// 新布局（旧注册被新 provider 替换）。
#[test]
fn refresh_sees_new_layout_after_update_meta() {
    use splayed_core::update_meta;

    let dir = temp_dir("refresh");
    create_table(&dir, &make_batch(), true).unwrap(); // 2 sym × 5 time = 10 行

    let conn = Connection::open(&dir).unwrap();
    let before = conn.execute("SELECT COUNT(*) AS c FROM splayed").unwrap();
    let c = before[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 10);

    // core 重排布局：只保留 SYM02 的 t0..2（3 行）。
    let syms: Vec<String> = ["SYM02", "SYM02", "SYM02"].map(|s| s.to_string()).to_vec();
    update_meta(&dir, splayed_format::TimeType::Date32, &syms, &[0, 1, 2]).unwrap();

    // 未 refresh 时还是旧视图（缓存 provider）——刷新后为 3 行。
    let stale = conn.execute("SELECT COUNT(*) AS c FROM splayed").unwrap();
    let sc = stale[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(sc, 10); // 旧 provider 缓存

    conn.refresh().unwrap();
    let after = conn.execute("SELECT COUNT(*) AS c FROM splayed").unwrap();
    let ac = after[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(ac, 3);

    std::fs::remove_dir_all(&dir).ok();
}