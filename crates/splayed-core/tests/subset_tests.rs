//! `.sub.xxx`（父 `.meta` 网格子集）集成测试：
//! 建表 → create_subset → SubsetReader 读回 → 按父全局行序读字段值；
//! 多不连续区间 / 校验错误 / 父失效（StaleParent）/ 非字段（list_fields 忽略）。

use std::fs;
use std::path::PathBuf;

use splayed_core::{
    FieldReader, SubsetInput, SubsetReader, create_subset, create_table, open_dataset,
    update_meta,
};
use splayed_format::{DataType, RawValue, SymIndexRecord, TimeType};

fn temp_dir(suffix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splayed_subset_{suffix}_{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn native_column(dt: DataType, vals: &[RawValue]) -> Vec<u8> {
    let mut out = vec![0u8; vals.len() * dt.size_of()];
    for (i, v) in vals.iter().enumerate() {
        v.write_le(&mut out, i * dt.size_of());
    }
    out
}

/// 2 SYM × 5 天（0..5）表；close = SYM01:1.0..5.0，SYM02:10.0..50.0。
fn make_table(dir: &PathBuf) {
    let syms: Vec<String> = ["SYM01", "SYM02"]
        .iter()
        .flat_map(|s| std::iter::repeat_n(s.to_string(), 5))
        .collect();
    let times: Vec<i64> = (0..5)
        .chain(0..5)
        .collect();
    let close = native_column(
        DataType::Float64,
        &[
            RawValue::from_f64(1.0), RawValue::from_f64(2.0), RawValue::from_f64(3.0),
            RawValue::from_f64(4.0), RawValue::from_f64(5.0),
            RawValue::from_f64(10.0), RawValue::from_f64(20.0), RawValue::from_f64(30.0),
            RawValue::from_f64(40.0), RawValue::from_f64(50.0),
        ],
    );
    create_table(
        dir,
        TimeType::Date32,
        &syms,
        &times,
        &[splayed_core::TableColumn {
            name: "close".into(),
            data_type: DataType::Float64,
            values: close,
        }],
        true,
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// 基本读回：SYM01 选中 days {0,1,3,4}（两段：[0,2) ∪ [3,5)）
// ---------------------------------------------------------------------------

#[test]
fn create_subset_basic_readback() {
    let dir = temp_dir("basic");
    make_table(&dir);

    let inputs = [
        SubsetInput::new("SYM01", vec![(0, 2), (3, 2)]),
        SubsetInput::new("SYM02", vec![(1, 1)]),
    ];
    create_subset(&dir, "hs300", &inputs).unwrap();

    let sub = SubsetReader::open(&dir, "hs300").unwrap();
    assert_eq!(sub.symbols(), &["SYM01", "SYM02"]);
    assert!(sub.contains("SYM01"));
    assert!(!sub.contains("SYM99"));
    assert_eq!(sub.total_rows(), 4 + 1);

    // SYM01：两段不连续区间（day 2 缺席）。
    assert_eq!(
        sub.ranges("SYM01").unwrap(),
        &[
            SymIndexRecord { time_start: 0, time_count: 2, row_start: 0 },
            SymIndexRecord { time_start: 3, time_count: 2, row_start: 3 },
        ]
    );
    // SYM02：一段（day 1，全局行 6）。
    assert_eq!(
        sub.ranges("SYM02").unwrap(),
        &[SymIndexRecord { time_start: 1, time_count: 1, row_start: 6 }]
    );

    // 读 close 在子集内的值（父全局行序拼接）。
    let ds = open_dataset(&dir).unwrap();
    let reader = FieldReader::open(ds.field_path("close")).unwrap();
    let bytes = sub.read_field_values(&reader).unwrap();
    let got: Vec<f64> = bytes
        .chunks_exact(8)
        .map(|c| f64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(got, vec![1.0, 2.0, 4.0, 5.0, 20.0]);
    drop(reader);

    // iter_entries：按父全局行序产出 (sym, time, row)。
    let entries: Vec<(String, i64, u32)> = sub
        .iter_entries(&ds.meta)
        .map(|(s, t, r)| (s.to_string(), t, r))
        .collect();
    assert_eq!(
        entries,
        vec![
            ("SYM01".into(), 0, 0),
            ("SYM01".into(), 1, 1),
            ("SYM01".into(), 3, 3),
            ("SYM01".into(), 4, 4),
            ("SYM02".into(), 1, 6),
        ]
    );

    // 文件确已落盘：`.sub.hs300` 存在。
    assert!(dir.join(".sub.hs300").is_file());
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 相邻段自动合并成一条区间
// ---------------------------------------------------------------------------

#[test]
fn create_subset_merges_adjacent_segments() {
    let dir = temp_dir("merge");
    make_table(&dir);

    // days 0,1（(0,2)）与 days 2,3（(2,2)）相邻 → 合并为 [0,4)。
    let inputs = [SubsetInput::new("SYM01", vec![(0, 2), (2, 2)])];
    create_subset(&dir, "m", &inputs).unwrap();

    let sub = SubsetReader::open(&dir, "m").unwrap();
    assert_eq!(
        sub.ranges("SYM01").unwrap(),
        &[SymIndexRecord { time_start: 0, time_count: 4, row_start: 0 }]
    );
    assert_eq!(sub.total_rows(), 4);
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 校验错误路径
// ---------------------------------------------------------------------------

#[test]
fn create_subset_errors() {
    let dir = temp_dir("errors");
    make_table(&dir);

    // 符号不在父 meta
    let e = create_subset(
        &dir,
        "bad1",
        &[SubsetInput::new("SYM99", vec![(0, 1)])],
    )
    .unwrap_err();
    assert!(matches!(e, splayed_core::SubsetError::SymNotFound(_)));

    // TIME 不在父 TIME AXIS
    let e = create_subset(
        &dir,
        "bad2",
        &[SubsetInput::new("SYM01", vec![(7, 1)])],
    )
    .unwrap_err();
    assert!(matches!(e, splayed_core::SubsetError::TimeNotFound(_)));

    // 段超出该 SYM 的连续时间块（SYM01 块为 days 0..5，(4,2) 越过）
    let e = create_subset(
        &dir,
        "bad3",
        &[SubsetInput::new("SYM01", vec![(4, 2)])],
    )
    .unwrap_err();
    assert!(matches!(e, splayed_core::SubsetError::SegmentOutOfRange { .. }));

    // 空段
    let e = create_subset(
        &dir,
        "bad4",
        &[SubsetInput::new("SYM01", vec![(0, 0)])],
    )
    .unwrap_err();
    assert!(matches!(e, splayed_core::SubsetError::EmptyRange));

    // 非法名：空 / 以 . 开头 / 路径分隔符
    for bad in [".hidden", "a/b", "", ".."] {
        let e = create_subset(&dir, bad, &[SubsetInput::new("SYM01", vec![(0, 1)])])
            .unwrap_err();
        assert!(
            matches!(e, splayed_core::SubsetError::BadName(_)),
            "name {bad:?} should be rejected"
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 父失效：update_meta 重排 → generation 递增 → StaleParent
// ---------------------------------------------------------------------------

#[test]
fn subset_stale_parent_detected() {
    let dir = temp_dir("stale");
    make_table(&dir);
    create_subset(&dir, "hs300", &[SubsetInput::new("SYM01", vec![(0, 5)])]).unwrap();

    let before = open_dataset(&dir).unwrap();
    let r = SubsetReader::open_with_parent(&dir, "hs300", &before.meta);
    assert!(r.is_ok(), "未重排前应通过");
    drop(before);

    // 重排：加一天，generation 递增。
    let syms: Vec<String> = ["SYM01", "SYM02"]
        .iter()
        .flat_map(|s| std::iter::repeat_n(s.to_string(), 6))
        .collect();
    let times: Vec<i64> = (0..6).chain(0..6).collect();
    update_meta(&dir, TimeType::Date32, &syms, &times).unwrap();

    let after = open_dataset(&dir).unwrap();
    let err = SubsetReader::open_with_parent(&dir, "hs300", &after.meta).unwrap_err();
    assert!(matches!(err, splayed_core::SubsetError::StaleParent { .. }));

    // 无父校验的裸 open 仍可读（文件未动）。
    let sub = SubsetReader::open(&dir, "hs300").unwrap();
    assert_eq!(sub.total_rows(), 5);
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// `.sub.xxx` 不是字段：list_fields 忽略
// ---------------------------------------------------------------------------

#[test]
fn subset_not_listed_as_field() {
    let dir = temp_dir("notfield");
    make_table(&dir);
    create_subset(&dir, "hs300", &[SubsetInput::new("SYM01", vec![(0, 2)])]).unwrap();

    let ds = open_dataset(&dir).unwrap();
    let fields = ds.list_fields().unwrap();
    assert!(fields.contains(&"close".to_string()));
    assert!(
        !fields.iter().any(|f| f.starts_with('.')),
        "点文件（含 .sub.xxx）不应被列为字段，got {fields:?}"
    );
    let _ = fs::remove_dir_all(&dir);
}
