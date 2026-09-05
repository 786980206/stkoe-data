//! splayed-polars 集成测试：scan_polars 端到端（含分区表、过滤、NULL）。

use std::path::{Path, PathBuf};

use splayed_format::{Bitmap, Buffer, Column, Data, DataType, FieldSchema, Schema};
use splayed_table::create_table;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("splayed_polars_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// 2026-08 与 2026-09 各 4 行（2 sym × 2 天），time 为 Date32 天序号。
fn month_sample_with_null() -> Data {
    let d = |y: i32, m: u32, dd: u32| splayed_table::days_from_civil(y as i64, m, dd) as i32;
    let rows: [(&str, i32, f64); 8] = [
        ("AAPL", d(2026, 8, 3), 10.0),
        ("AAPL", d(2026, 8, 4), 11.0),
        ("MSFT", d(2026, 8, 3), 20.0),
        ("MSFT", d(2026, 8, 4), 0.0), // NULL
        ("AAPL", d(2026, 9, 1), 12.0),
        ("AAPL", d(2026, 9, 2), 13.0),
        ("MSFT", d(2026, 9, 1), 22.0),
        ("MSFT", d(2026, 9, 2), 23.0),
    ];
    let mut dict: Vec<&str> = rows.iter().map(|r| r.0).collect();
    dict.sort();
    dict.dedup();
    let mut offsets = vec![0u64];
    let mut strings = Vec::new();
    for s in &dict {
        strings.extend_from_slice(s.as_bytes());
        offsets.push(strings.len() as u64);
    }
    let keys: Vec<u32> = rows
        .iter()
        .map(|r| dict.iter().position(|x| *x == r.0).unwrap() as u32)
        .collect();
    let sym_col = Column::from_dict(keys, offsets, strings, None);
    let mut price_bits = Bitmap::ones(8);
    price_bits.set(3, false); // MSFT 08-04 为 NULL
    let time_col = Column {
        data_type: DataType::Date32,
        values: Buffer::from_slice_copy(&rows.iter().map(|r| r.1).collect::<Vec<i32>>()),
        validity: None,
        dict: None,
    };
    let price_col = Column {
        data_type: DataType::Float64,
        values: Buffer::from_slice_copy(&rows.iter().map(|r| r.2).collect::<Vec<f64>>()),
        validity: Some(price_bits),
        dict: None,
    };
    Data::new(
        Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", DataType::Date32),
            FieldSchema::new("price", DataType::Float64),
        ]),
        vec![sym_col, time_col, price_col],
    )
    .unwrap()
}

#[test]
fn scan_polars_month_table_filter_and_nulls() {
    let dir = temp_dir("month");
    let root = dir.join("tbl");
    create_table(&root, &month_sample_with_null(), splayed_table::PartitionScheme::Month).unwrap();

    let lf = splayed_polars::scan_polars(&root).unwrap();
    let df = lf.collect().unwrap();
    assert_eq!(df.height(), 8);
    assert_eq!(df.width(), 3); // sym / time / price

    // 过滤：price > 15 → 20, 22, 23（NULL 行由 polars 排除）
    let lf = splayed_polars::scan_polars(&root).unwrap();
    let df = lf
        .filter(polars::prelude::col("price").gt(polars::prelude::lit(15.0)))
        .collect()
        .unwrap();
    assert_eq!(df.height(), 3);

    // 投影：只取 sym
    let lf = splayed_polars::scan_polars(&root).unwrap();
    let df = lf.select([polars::prelude::col("sym")]).collect().unwrap();
    assert_eq!(df.width(), 1);
    assert_eq!(df.height(), 8);

    // 分区目录存在
    assert!(root.join("month=2026-08").exists());
    assert!(root.join("month=2026-09").exists());
    cleanup(&dir);
}

#[test]
fn scan_polars_none_table() {
    let dir = temp_dir("none");
    let root = dir.join("tbl");
    let _ = std::fs::remove_dir_all(&root);
    let d = |y: i32, m: u32, dd: u32| splayed_table::days_from_civil(y as i64, m, dd) as i32;
    let rows: Vec<(&str, i32, f64)> = vec![
        ("AAPL", d(2026, 8, 3), 10.0),
        ("AAPL", d(2026, 8, 4), 11.0),
        ("MSFT", d(2026, 8, 3), 20.0),
        ("MSFT", d(2026, 8, 4), 0.0),
    ];
    // 复用 month_sample_with_null 的构造逻辑（截取前 4 行的有序版）
    let mut dict: Vec<&str> = rows.iter().map(|r| r.0).collect();
    dict.sort();
    dict.dedup();
    let mut offsets = vec![0u64];
    let mut strings = Vec::new();
    for s in &dict {
        strings.extend_from_slice(s.as_bytes());
        offsets.push(strings.len() as u64);
    }
    let keys: Vec<u32> = rows
        .iter()
        .map(|r| dict.iter().position(|x| *x == r.0).unwrap() as u32)
        .collect();
    let sym_col = Column::from_dict(keys, offsets, strings, None);
    let mut bits = Bitmap::ones(4);
    bits.set(3, false);
    let time_col = Column {
        data_type: DataType::Date32,
        values: Buffer::from_slice_copy(&rows.iter().map(|r| r.1).collect::<Vec<i32>>()),
        validity: None,
        dict: None,
    };
    let price_col = Column {
        data_type: DataType::Float64,
        values: Buffer::from_slice_copy(&rows.iter().map(|r| r.2).collect::<Vec<f64>>()),
        validity: Some(bits),
        dict: None,
    };
    let data = Data::new(
        Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", DataType::Date32),
            FieldSchema::new("price", DataType::Float64),
        ]),
        vec![sym_col, time_col, price_col],
    )
    .unwrap();
    create_table(&root, &data, splayed_table::PartitionScheme::None).unwrap();

    let lf = splayed_polars::scan_polars(&root).unwrap();
    let df = lf.collect().unwrap();
    assert_eq!(df.height(), 4);
    let price = df.column("price").unwrap();
    assert_eq!(price.null_count(), 1);
    cleanup(&dir);
}
