//! splayed-polars 集成测试：AnonymousScan 惰性扫描（投影裁剪 / 谓词下推 / 兜底）。

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray as ArrowStringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use polars::prelude::*;
use splayed_arrow::create_table;
use splayed_polars::splayed_lazyframe;

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splayed_pl_{suffix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// time 0..2 × SYM01/02：close 100..102 / 200..202，vol 1..6（含一个 NULL）。
fn make_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("time", ArrowDT::Date32, false),
            Field::new("sym", ArrowDT::Utf8, false),
            Field::new("close", ArrowDT::Float64, true),
            Field::new("vol", ArrowDT::Int64, true),
        ])),
        vec![
            Arc::new(arrow_array::Date32Array::from(vec![0, 1, 2, 0, 1, 2])),
            Arc::new(ArrowStringArray::from(vec![
                "SYM01", "SYM01", "SYM01", "SYM02", "SYM02", "SYM02",
            ])),
            Arc::new(Float64Array::from(vec![
                100.0, 101.0, 102.0, 200.0, 201.0, 202.0,
            ])),
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4), Some(5), Some(6)])),
        ],
    )
    .unwrap()
}

fn values_f64(df: &DataFrame) -> Vec<f64> {
    df.column("close")
        .unwrap()
        .f64()
        .unwrap()
        .iter()
        .map(|v| v.unwrap())
        .collect()
}

#[test]
fn lazy_scan_full_collect() {
    let dir = temp_dir("full");
    create_table(&dir, &make_batch(), true).unwrap();
    let lf = splayed_lazyframe(&dir).unwrap();
    // polars 0.45 的 anonymous-scan 在「无显式 select 的完整收集」下存在投影
    // 优化 bug（reader_schema=None 被 unwrap）；显式全列 select 等价且避开。
    let df = lf
        .select([col("time"), col("sym"), col("close"), col("vol")])
        .collect()
        .unwrap();
    assert_eq!(df.shape(), (6, 4));
    assert!(df.get_column_names().iter().any(|n| n.as_str() == "sym"));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn projection_pushdown_selects_columns() {
    let dir = temp_dir("proj");
    create_table(&dir, &make_batch(), true).unwrap();
    let lf = splayed_lazyframe(&dir).unwrap();
    let df = lf.select([col("close")]).collect().unwrap();
    assert_eq!(df.shape(), (6, 1)); // 只读/只返一列
    assert_eq!(values_f64(&df), vec![100.0, 101.0, 102.0, 200.0, 201.0, 202.0]);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn predicate_pushdown_filter() {
    let dir = temp_dir("pred");
    create_table(&dir, &make_batch(), true).unwrap();
    let lf = splayed_lazyframe(&dir).unwrap();

    // close > 150：核心层值过滤（同时被兜底），结果 = SYM02 的 3 行。
    let df = lf
        .clone()
        .filter(col("close").gt(lit(150.0)))
        .select([col("sym"), col("close")])
        .collect()
        .unwrap();
    assert_eq!(df.shape(), (3, 2));
    assert_eq!(values_f64(&df), vec![200.0, 201.0, 202.0]);

    // sym = 'SYM01'：核心层 SymbolSelection 裁剪。
    let df = lf
        .filter(col("sym").eq(lit("SYM01")))
        .select([col("close")])
        .collect()
        .unwrap();
    assert_eq!(values_f64(&df), vec![100.0, 101.0, 102.0]);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn predicate_fallback_for_complex_expr() {
    let dir = temp_dir("fallback");
    create_table(&dir, &make_batch(), true).unwrap();
    let lf = splayed_lazyframe(&dir).unwrap();

    // 非简单「列 op 字面量」（表达式对比）→ 核心不裁剪，兜底过滤保证正确。
    let df = lf
        .filter(col("close").gt(col("close").mean()))
        .select([col("sym"), col("close")])
        .collect()
        .unwrap();
    // mean = (100+101+102+200+201+202)/6 = 151；close > 151 → 200,201,202 → 3 行。
    assert_eq!(df.shape(), (3, 2));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn null_values_survive_conversion() {
    let dir = temp_dir("nulls");
    create_table(&dir, &make_batch(), true).unwrap();
    let lf = splayed_lazyframe(&dir).unwrap();
    let df = lf
        .filter(col("vol").is_null())
        .select([col("sym"), col("vol")])
        .collect()
        .unwrap();
    assert_eq!(df.shape(), (1, 2)); // vol 恰一个 NULL（SYM01 day1）
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn group_by_aggregation_over_lazy_scan() {
    let dir = temp_dir("group");
    create_table(&dir, &make_batch(), true).unwrap();
    let lf = splayed_lazyframe(&dir).unwrap();
    let df = lf
        .group_by([col("sym")])
        .agg([col("close").sum()])
        .sort(["sym"], Default::default())
        .collect()
        .unwrap();
    assert_eq!(df.shape(), (2, 2));
    let sums: Vec<f64> = df
        .column("close")
        .unwrap()
        .f64()
        .unwrap()
        .iter()
        .map(|v| v.unwrap())
        .collect();
    assert_eq!(sums, vec![303.0, 603.0]); // SYM01=303, SYM02=603（升序排列后）
    std::fs::remove_dir_all(&dir).ok();
}

/// 分区表惰性扫描：key=value 分区列进 schema、分区列过滤下推、常量列补回。
#[test]
fn lazy_table_scan_partition_columns() {
    use splayed_polars::splayed_lazyframe_table;

    let root = std::env::temp_dir().join(format!("splayed_pl_tbl_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    create_table(&root.join("year=2024"), &make_batch(), true).unwrap(); // 6 行
    create_table(&root.join("year=2025"), &make_batch(), true).unwrap();

    let lf = splayed_lazyframe_table(&root).unwrap();

    // 分区列过滤 → 只扫 2024；year 投影为常量列。
    let df = lf
        .clone()
        .filter(col("year").eq(lit(2024)))
        .select([col("close"), col("year")])
        .collect()
        .unwrap();
    assert_eq!(df.shape(), (6, 2));
    let years: Vec<i64> = df.column("year").unwrap().i64().unwrap().iter().map(|v| v.unwrap()).collect();
    assert!(years.iter().all(|&y| y == 2024));

    // 字符串字面量同样剪裁（year = '2025'）由 core 层完成；此处用 String 分区列验证。
    let root2 = std::env::temp_dir().join(format!("splayed_pl_tbl2_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root2);
    create_table(&root2.join("env=prod"), &make_batch(), true).unwrap();
    create_table(&root2.join("env=test"), &make_batch(), true).unwrap();
    let lf2 = splayed_lazyframe_table(&root2).unwrap();
    let df2 = lf2
        .filter(col("env").eq(lit("prod")))
        .select([col("close"), col("env")])
        .collect()
        .unwrap();
    assert_eq!(df2.shape(), (6, 2));
    let envs: Vec<&str> = df2
        .column("env")
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|v| v.unwrap())
        .collect();
    assert!(envs.iter().all(|&e| e == "prod"));
    std::fs::remove_dir_all(&root2).ok();

    // count over 全表（两分区）。
    let df3 = lf
        .filter(col("close").gt(lit(150.0)))
        .group_by([col("year")])
        .agg([col("close").count()])
        .sort(["year"], Default::default())
        .collect()
        .unwrap();
    assert_eq!(df3.shape(), (2, 2));

    std::fs::remove_dir_all(&root).ok();
}