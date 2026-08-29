//! FIELD footer 统计 + 扫描剪裁 + 读取期限值 的集成测试。
//!
//! 覆盖：
//! - footer 写读环回（create_table → FieldReader::stats）；
//! - update 后统计失效；compact（含压缩）后统计仍在；
//! - Scanner::plan 利用 min/max 做整数据集剪裁（filter 与统计不相交 → 0 行）；
//! - IsNull/IsNotNull 按 null_count 剪裁；
//! - ScanRequest.limit 读取期截断（单线程 / 并行 / 全收）。

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use splayed_codec::compact_field;
use splayed_core::{
    FieldReader, Filter, FilterValue, ScanRequest, Scanner, SymbolSelection, TimeRange, TableColumn,
    UpdateItem, create_table, scan_owned, scan_owned_parallel, update_field,
};
use splayed_format::{Compression, DataType as ST, RawValue, TimeType};

fn temp_dir(suffix: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("splayed_core_sl_{suffix}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn f64_col(vals: &[f64]) -> Vec<u8> {
    let mut b = vec![0u8; vals.len() * 8];
    for (i, v) in vals.iter().enumerate() {
        RawValue::from_f64(*v).write_le(&mut b, i * 8);
    }
    b
}

fn i64_col(vals: &[Option<i64>]) -> Vec<u8> {
    let dt = ST::Int64;
    let mut b = vec![0u8; vals.len() * 8];
    for (i, v) in vals.iter().enumerate() {
        match v {
            Some(x) => RawValue::from_i64(*x).write_le(&mut b, i * 8),
            None => b[i * 8..(i + 1) * 8].copy_from_slice(dt.null_bytes()),
        }
    }
    b
}

/// 构造数据集：SYM01/SYM02 × time 0..4（10 行）。
/// close: SYM01=100..104, SYM02=200..204；vol: 51 行有值（SYM02 day0 是 NULL）。
fn build_dataset(dir: &PathBuf) {
    const S5: i64 = 5;
    let syms: Vec<String> = (0..S5 as usize)
        .map(|_| "SYM01".to_string())
        .chain((0..S5 as usize).map(|_| "SYM02".to_string()))
        .collect();
    let time: Vec<i64> = (0..S5).chain(0..S5).collect();

    let mut close = f64_col(&[100.0, 101.0, 102.0, 103.0, 104.0]);
    close.extend_from_slice(&f64_col(&[200.0, 201.0, 202.0, 203.0, 204.0]));
    let mut vol = i64_col(&[Some(1); 5].as_slice());
    vol.extend_from_slice(&i64_col(&[None, Some(2), Some(3), Some(4), Some(5)].as_slice()));

    // create_table 自带 create_meta，并要求目录为空（勿先调 create_meta）。
    create_table(
        dir,
        TimeType::Date32,
        &syms,
        &time,
        &[
            TableColumn {
                name: "close".to_string(),
                data_type: ST::Float64,
                values: close,
            },
            TableColumn {
                name: "vol".to_string(),
                data_type: ST::Int64,
                values: vol,
            },
        ],
        true,
    )
    .unwrap();
}

fn scan_all(dir: &PathBuf, req: &ScanRequest) -> Vec<f64> {
    let dataset = splayed_core::open_dataset(dir).unwrap();
    let scanner = Scanner::new(&dataset);
    let plan = scanner.plan(req).unwrap();
    let mut out = Vec::new();
    let mut batches = scanner.scan(&plan, req).unwrap();
    while let Some(b) = batches.next_batch().unwrap() {
        let data = b.column(2).data();
        for i in 0..b.num_rows() {
            out.push(f64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap()));
        }
    }
    out
}

#[test]
fn footer_stats_roundtrip_and_invalidation() {
    let dir = temp_dir("rt");
    build_dataset(&dir);

    // stats 写出：close min=100 max=204；vol min=1 max=5（跳过 NULL）。
    let close = FieldReader::open(dir.join("close")).unwrap();
    let st = close.stats().expect("close footer stats");
    assert_eq!(f64::from_le_bytes(st.min), 100.0);
    assert_eq!(f64::from_le_bytes(st.max), 204.0);
    let vol = FieldReader::open(dir.join("vol")).unwrap();
    let st = vol.stats().expect("vol footer stats");
    assert_eq!(i64::from_le_bytes(st.min), 1);
    assert_eq!(i64::from_le_bytes(st.max), 5);

    // update 后统计失效（magic 归零）。
    let mut items = Vec::new();
    let mut vals = vec![0u8; 8];
    RawValue::from_f64(420.0).write_le(&mut vals, 0);
    items.push(UpdateItem::new(0, vals.clone()));
    let _ = update_field(dir.join("close"), &items);
    assert!(FieldReader::open(dir.join("close")).unwrap().stats().is_none());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compact_preserves_stats() {
    let dir = temp_dir("compact");
    build_dataset(&dir);
    let path = dir.join("close");
    compact_field(&path, Compression::Zstd).unwrap();
    let reader = FieldReader::open(&path).unwrap();
    let st = reader.stats().expect("compressed footer stats");
    assert_eq!(f64::from_le_bytes(st.min), 100.0);
    assert_eq!(f64::from_le_bytes(st.max), 204.0);
    // 压缩路径读取且不含 footer 污染。
    assert_eq!(reader.read_row(0).unwrap().as_f64(), Some(100.0));
    assert_eq!(reader.read_row(9).unwrap().as_f64(), Some(204.0));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn is_not_null_scan_rows() {
    // vol 有 1 个 NULL；IsNotNull → 9 行（IsNull/IsNotNull 不做计划级剪裁，
    // 由行级过滤保证）。
    let dir = temp_dir("inn");
    build_dataset(&dir);
    let dataset = Arc::new(splayed_core::open_dataset(&dir).unwrap());

    let req = ScanRequest {
        columns: vec!["vol".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![Filter::IsNotNull {
            field: "vol".into(),
        }],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };
    let scanner = Scanner::new(&dataset);
    let plan = scanner.plan(&req).unwrap();
    assert!(plan.total_rows > 0); // 未被误剪
    let mut n = 0usize;
    let mut batches = scanner.scan(&plan, &req).unwrap();
    while let Some(b) = batches.next_batch().unwrap() {
        n += b.num_rows();
    }
    assert_eq!(n, 9);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn stats_prune_empty_scan() {
    let dir = temp_dir("prune");
    build_dataset(&dir);

    // close max=204：filter close > 204 → 整表无匹配 → 0 行（三种入口一致）。
    for (label, req) in [
        (
            "gt204",
            ScanRequest {
                columns: vec!["close".into()],
                symbols: SymbolSelection::All,
                time_range: TimeRange::all(),
                filters: vec![Filter::GreaterThan {
                    field: "close".into(),
                    value: FilterValue::Float64(204.0),
                }],
                batch_size: 65536,
                parallelism: 1,
                limit: None,
            },
        ),
        (
            "lt100",
            ScanRequest {
                columns: vec!["close".into()],
                symbols: SymbolSelection::All,
                time_range: TimeRange::all(),
                filters: vec![Filter::LessThan {
                    field: "close".into(),
                    value: FilterValue::Float64(100.0),
                }],
                batch_size: 65536,
                parallelism: 1,
                limit: None,
            },
        ),
        (
            "isnull_close",
            ScanRequest {
                columns: vec!["close".into()],
                symbols: SymbolSelection::All,
                time_range: TimeRange::all(),
                filters: vec![Filter::IsNull {
                    field: "close".into(),
                }],
                batch_size: 65536,
                parallelism: 1,
                limit: None,
            },
        ),
    ] {
        assert_eq!(scan_all(&dir, &req), Vec::<f64>::new(), "prune {label}");
    }

    // 有命中时不受影响：close >= 200 → SYM02 的 5 行。
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
        limit: None,
    };
    assert_eq!(scan_all(&dir, &req), vec![200.0, 201.0, 202.0, 203.0, 204.0]);

    // 边界相等不剪裁：close > 200（max=204 > 200）→ 4 行。
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![Filter::GreaterThan {
            field: "close".into(),
            value: FilterValue::Float64(200.0),
        }],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };
    assert_eq!(scan_all(&dir, &req), vec![201.0, 202.0, 203.0, 204.0]);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn limit_truncates_all_paths() {
    let dir = temp_dir("limit");
    build_dataset(&dir);
    let dataset = Arc::new(splayed_core::open_dataset(&dir).unwrap());

    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![],
        batch_size: 65536,
        parallelism: 1,
        limit: Some(3),
    };
    let scanner = Scanner::new(&dataset);
    let plan = scanner.plan(&req).unwrap();

    // 单线程流式。
    let mut got = Vec::new();
    let mut batches = scanner.scan(&plan, &req).unwrap();
    while let Some(b) = batches.next_batch().unwrap() {
        assert!(b.num_rows() <= 3);
        got.extend(read_close(&b));
    }
    assert_eq!(got, vec![100.0, 101.0, 102.0]); // (sym,time) 序的前 3

    // owned 流式。
    let mut owned = scan_owned(Arc::clone(&dataset), &plan, &req).unwrap();
    let mut got2 = Vec::new();
    while let Some(b) = owned.next_batch().unwrap() {
        got2.extend(read_close(&b));
    }
    assert_eq!(got2, vec![100.0, 101.0, 102.0]);

    // 并行流式。
    let mut par = scan_owned_parallel(Arc::clone(&dataset), &plan, &req, 4).unwrap();
    let mut got3 = Vec::new();
    while let Some(b) = par.next_batch().unwrap() {
        got3.extend(read_close(&b));
    }
    assert_eq!(got3, vec![100.0, 101.0, 102.0]);

    // 全收集（scan_all_parallel）：limit 之后不产出。
    let req_big = ScanRequest {
        limit: Some(7),
        ..req
    };
    let plan_big = scanner.plan(&req_big).unwrap();
    let all = scanner.scan_all_parallel(&plan_big, &req_big).unwrap();
    let rows: usize = all.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 7);

    std::fs::remove_dir_all(&dir).ok();
}

fn read_close(b: &splayed_core::CoreBatch) -> Vec<f64> {
    let data = b.column(2).data();
    (0..b.num_rows())
        .map(|i| f64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap()))
        .collect()
}

#[test]
fn limit_with_filter_counts_passing_rows() {
    // limit 作用于「通过过滤的行」：close > 200 限 2 行 → 201, 202。
    let dir = temp_dir("limit2");
    build_dataset(&dir);
    let dataset = Arc::new(splayed_core::open_dataset(&dir).unwrap());
    let req = ScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![Filter::GreaterThan {
            field: "close".into(),
            value: FilterValue::Float64(200.0),
        }],
        batch_size: 2,
        parallelism: 1,
        limit: Some(2),
    };
    let scanner = Scanner::new(&dataset);
    let plan = scanner.plan(&req).unwrap();
    let mut got = Vec::new();
    let mut batches = scanner.scan(&plan, &req).unwrap();
    while let Some(b) = batches.next_batch().unwrap() {
        got.extend(read_close(&b));
    }
    assert_eq!(got, vec![201.0, 202.0]);
    std::fs::remove_dir_all(&dir).ok();
}