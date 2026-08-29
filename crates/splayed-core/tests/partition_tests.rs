//! `splayed-core::partition` 集成测试：发现 / schema 合并 / 三层剪裁
//! （TIME / 符号 / 分区统计）/ 按名升序流式合并。

use std::fs;
use std::path::PathBuf;

use splayed_core::{
    Filter, FilterValue, PartitionError, PartitionScanRequest, PartitionedTable, SymbolSelection,
    TableColumn, TimeRange, create_table,
};
use splayed_format::{DataType as ST, RawValue, TimeType};

fn temp_dir(suffix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("splayed_core_pt_{suffix}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// 在 dir 建一个分区：`sym × time 3 点`，close = 起始 + (r*100 + c*10 + t)。
fn make_partition(dir: &std::path::Path, syms: &[&str], base: f64) {
    let mut s: Vec<String> = Vec::new();
    let mut t: Vec<i64> = Vec::new();
    let mut vals: Vec<f64> = Vec::new();
    for (r, sym) in syms.iter().enumerate() {
        for ti in 0..3i64 {
            s.push(sym.to_string());
            t.push(ti);
            vals.push(base + (r as f64) * 100.0 + (ti as f64) * 10.0);
        }
    }
    let mut bytes = vec![0u8; vals.len() * 8];
    for (i, v) in vals.iter().enumerate() {
        RawValue::from_f64(*v).write_le(&mut bytes, i * 8);
    }
    create_table(
        dir,
        TimeType::Date32,
        &s,
        &t,
        &[TableColumn {
            name: "close".to_string(),
            data_type: ST::Float64,
            values: bytes,
        }],
        true,
    )
    .unwrap();
}

fn read_close(plan: &splayed_core::PartitionPlan, req: &PartitionScanRequest, table: &PartitionedTable) -> Vec<f64> {
    let mut batches = table.scan(plan, req).unwrap();
    let mut out = Vec::new();
    while let Some(b) = batches.next_batch().unwrap() {
        let data = b.column(2).data();
        for i in 0..b.num_rows() {
            out.push(f64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap()));
        }
    }
    out
}

#[test]
fn discover_and_merge_schema() {
    let root = temp_dir("discover");
    make_partition(&root.join("2024"), &["SYM01", "SYM02"], 100.0);
    make_partition(&root.join("2025"), &["SYM01", "SYM03"], 500.0);

    let table = PartitionedTable::open(&root).unwrap();
    assert_eq!(table.partition_count(), 2);
    assert_eq!(table.partitions()[0].name, "2024");
    assert_eq!(table.partitions()[1].name, "2025");
    assert_eq!(table.fields(), &[("close".to_string(), ST::Float64)]);
    assert_eq!(table.symbols(), &["SYM01", "SYM02", "SYM03"]);

    // 单 dataset 目录 → 单分区（名 "."）。
    let single = PartitionedTable::open(&root.join("2024")).unwrap();
    assert_eq!(single.partition_count(), 1);
    assert_eq!(single.partitions()[0].name, ".");

    // 空目录 → NoPartition。
    let empty = root.join("empty");
    fs::create_dir(&empty).unwrap();
    assert!(matches!(
        PartitionedTable::open(&empty),
        Err(PartitionError::NoPartition { .. })
    ));

    fs::remove_dir_all(&root).ok();
}

#[test]
fn schema_mismatch_rejected() {
    let root = temp_dir("mismatch");
    make_partition(&root.join("2024"), &["SYM01"], 100.0);
    // 2025 多一个字段 → 报错。
    let dir5 = root.join("2025");
    make_partition(&dir5, &["SYM01"], 200.0);
    let mut bytes = vec![0u8; 3 * 8];
    for (i, v) in [1i64, 2, 3].iter().enumerate() {
        RawValue::from_i64(*v).write_le(&mut bytes, i * 8);
    }
    // 只能手动补一个字段：用 splayed_core::create_field_with_data（需 meta 先行）。
    splayed_core::create_field_with_data(dir5.join("vol"), ST::Int64, &bytes).unwrap();

    let err = PartitionedTable::open(&root).unwrap_err();
    assert!(matches!(err, PartitionError::SchemaMismatch { .. }));

    fs::remove_dir_all(&root).ok();
}

#[test]
fn time_pruning_skips_non_overlapping_partitions() {
    let root = temp_dir("tprune");
    make_partition(&root.join("2024"), &["SYM01"], 100.0); // time 0..2
    make_partition(&root.join("2025"), &["SYM01"], 500.0); // time 0..2（演示为相接区间）

    // 2025 的 time_axis 也是 0..2？改用分开的时间轴：
    fs::remove_dir_all(&root).ok();
    let root = temp_dir("tprune2");
    // 2024: time 0..2；2025: time 10..12（通过不同的 time 值）。
    let mk = |dir: &std::path::Path, base: f64, off: i64| {
        let mut syms = Vec::new();
        let mut t = Vec::new();
        let mut vals = Vec::new();
        for ti in 0..3i64 {
            syms.push("SYM01".to_string());
            t.push(off + ti);
            vals.push(base + ti as f64);
        }
        let mut bytes = vec![0u8; vals.len() * 8];
        for (i, v) in vals.iter().enumerate() {
            RawValue::from_f64(*v).write_le(&mut bytes, i * 8);
        }
        create_table(
            dir,
            TimeType::Date32,
            &syms,
            &t,
            &[TableColumn {
                name: "close".to_string(),
                data_type: ST::Float64,
                values: bytes,
            }],
            true,
        )
        .unwrap();
    };
    mk(&root.join("2024"), 100.0, 0);
    mk(&root.join("2025"), 500.0, 10);

    let table = PartitionedTable::open(&root).unwrap();

    // 只查 time 0..2 → 只扫 2024。
    let req = PartitionScanRequest {
        columns: vec!["close".into()],
        time_range: TimeRange::new(1, 3),
        ..Default::default()
    };
    let plan = table.plan(&req).unwrap();
    assert_eq!(plan.tasks.len(), 1);
    assert_eq!(plan.tasks[0].partition, 0);
    assert_eq!(
        read_close(&plan, &req, &table),
        vec![101.0, 102.0] // 2024 time 1..2
    );

    // 只查 time 10..12 → 只扫 2025。
    let req2 = PartitionScanRequest {
        columns: vec!["close".into()],
        time_range: TimeRange::new(10, 13),
        ..Default::default()
    };
    let plan2 = table.plan(&req2).unwrap();
    assert_eq!(plan2.tasks.len(), 1);
    assert_eq!(plan2.tasks[0].partition, 1);

    // 全时间 → 两个分区，按名升序合并。
    let req3 = PartitionScanRequest {
        columns: vec!["close".into()],
        ..Default::default()
    };
    let plan3 = table.plan(&req3).unwrap();
    assert_eq!(plan3.tasks.len(), 2);
    assert_eq!(
        read_close(&plan3, &req3, &table),
        vec![100.0, 101.0, 102.0, 500.0, 501.0, 502.0]
    );

    fs::remove_dir_all(&root).ok();
}

#[test]
fn symbol_and_stats_pruning() {
    let root = temp_dir("symprune");
    make_partition(&root.join("2024"), &["SYM01"], 100.0); // close 100..120
    make_partition(&root.join("2025"), &["SYM02"], 500.0); // close 500..520

    let table = PartitionedTable::open(&root).unwrap();

    // 符号剪裁：SYM02 只在 2025 → 2024 被跳过。
    let req = PartitionScanRequest {
        columns: vec!["close".into()],
        symbols: SymbolSelection::syms(["SYM02"]),
        ..Default::default()
    };
    let plan = table.plan(&req).unwrap();
    assert_eq!(plan.tasks.len(), 1);
    assert_eq!(plan.tasks[0].partition, 1);

    // 统计剪裁：close > 1000 与任何分区不相交 → 空计划。
    let req2 = PartitionScanRequest {
        columns: vec!["close".into()],
        filters: vec![Filter::GreaterThan {
            field: "close".into(),
            value: FilterValue::Float64(1000.0),
        }],
        ..Default::default()
    };
    let plan2 = table.plan(&req2).unwrap();
    assert!(plan2.tasks.is_empty());

    // 统计剪裁：只与 2025 相交 → 只扫 2025。
    let req3 = PartitionScanRequest {
        columns: vec!["close".into()],
        filters: vec![Filter::GreaterThan {
            field: "close".into(),
            value: FilterValue::Float64(400.0),
        }],
        ..Default::default()
    };
    let plan3 = table.plan(&req3).unwrap();
    assert_eq!(plan3.tasks.len(), 1);
    assert_eq!(plan3.tasks[0].partition, 1);
    // close = 500 + 0*100 + t*10 → 500/510/520（全 > 400）。
    assert_eq!(read_close(&plan3, &req3, &table), vec![500.0, 510.0, 520.0]);

    fs::remove_dir_all(&root).ok();
}

#[test]
fn partition_columns_key_value() {
    use splayed_core::{FilterValue, PartitionColumnKind};

    let root = temp_dir("kv");
    make_partition(&root.join("year=2024"), &["SYM01"], 100.0);
    make_partition(&root.join("year=2025"), &["SYM01"], 500.0);

    let table = PartitionedTable::open(&root).unwrap();
    // 分区列：year（Int64）+ declared 值 + 分区名（仍为目录名）。
    assert_eq!(table.partition_columns().len(), 1);
    let pc = &table.partition_columns()[0];
    assert_eq!(pc.name, "year");
    assert_eq!(pc.kind, PartitionColumnKind::Int64);
    assert_eq!(table.partitions()[0].declared, vec![("year".to_string(), "2024".to_string())]);
    assert_eq!(table.partitions()[1].declared, vec![("year".to_string(), "2025".to_string())]);

    // 分区列过滤：year = 2024 → 只扫 2024 分区。
    let req = PartitionScanRequest {
        columns: vec!["close".into()],
        partition_filters: vec![Filter::Equal {
            field: "year".into(),
            value: FilterValue::Int64(2024),
        }],
        ..Default::default()
    };
    let plan = table.plan(&req).unwrap();
    assert_eq!(plan.tasks.len(), 1);
    assert_eq!(plan.tasks[0].partition, 0);
    assert_eq!(read_close(&plan, &req, &table), vec![100.0, 110.0, 120.0]);

    // year > 2024 → 只扫 2025。
    let req2 = PartitionScanRequest {
        columns: vec!["close".into()],
        partition_filters: vec![Filter::GreaterThan {
            field: "year".into(),
            value: FilterValue::Int64(2024),
        }],
        ..Default::default()
    };
    let plan2 = table.plan(&req2).unwrap();
    assert_eq!(plan2.tasks.len(), 1);
    assert_eq!(plan2.tasks[0].partition, 1);

    // 字符串字面量：year = '2024' 也命中 2024。
    let req3 = PartitionScanRequest {
        columns: vec!["close".into()],
        partition_filters: vec![Filter::Equal {
            field: "year".into(),
            value: FilterValue::String("2024".to_string()),
        }],
        ..Default::default()
    };
    let plan3 = table.plan(&req3).unwrap();
    assert_eq!(plan3.tasks.len(), 1);
    assert_eq!(plan3.tasks[0].partition, 0);

    // year = 9999 → 空计划。
    let req4 = PartitionScanRequest {
        partition_filters: vec![Filter::Equal {
            field: "year".into(),
            value: FilterValue::Int64(9999),
        }],
        ..Default::default()
    };
    assert!(table.plan(&req4).unwrap().tasks.is_empty());

    fs::remove_dir_all(&root).ok();
}

#[test]
fn partition_column_string_kind_and_mixed_naming() {
    use splayed_core::{FilterValue, PartitionColumnKind};

    // 非数字值 → String 分区列。
    let root = temp_dir("kvstr");
    make_partition(&root.join("env=prod"), &["SYM01"], 100.0);
    make_partition(&root.join("env=test"), &["SYM01"], 500.0);
    let table = PartitionedTable::open(&root).unwrap();
    assert_eq!(table.partition_columns()[0].name, "env");
    assert_eq!(table.partition_columns()[0].kind, PartitionColumnKind::String);

    let req = PartitionScanRequest {
        columns: vec!["close".into()],
        partition_filters: vec![Filter::Equal {
            field: "env".into(),
            value: FilterValue::String("prod".to_string()),
        }],
        ..Default::default()
    };
    let plan = table.plan(&req).unwrap();
    assert_eq!(plan.tasks.len(), 1);
    assert_eq!(plan.tasks[0].partition, 0);
    assert_eq!(read_close(&plan, &req, &table), vec![100.0, 110.0, 120.0]);

    // 普通目录名与 key=value 混用 → 报错。
    let root2 = temp_dir("kv_mix");
    make_partition(&root2.join("year=2024"), &["SYM01"], 100.0);
    make_partition(&root2.join("misc"), &["SYM01"], 500.0);
    assert!(matches!(
        PartitionedTable::open(&root2),
        Err(splayed_core::PartitionError::MixedPartitionNaming { .. })
    ));

    fs::remove_dir_all(&root).ok();
    fs::remove_dir_all(&root2).ok();
}