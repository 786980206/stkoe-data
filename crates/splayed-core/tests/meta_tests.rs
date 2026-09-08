//! META 构建集成测试：连续子区间原则
//! （sym time 交织连续 → 成功；sym 内跳空 / 重复 / 乱序 → NonContiguousTime）。

use splayed_core::{CoreError, MetaBuilder};
use splayed_format::{
    Buffer, Column, DataView, DataType, FieldSchema, MetaHeader, Schema, SymIndexRecord,
    META_HEADER_SIZE, SYM_INDEX_RECORD_SIZE,
};

/// 构造 MetaBuilder 输入视图：sym（字典编码）+ time（TimestampUs）。
/// `store` 持有底层 Column（即 Buffer）的生命周期。
fn meta_view<'a>(syms: &[&str], times: &[i64], store: &'a mut Vec<Column>) -> DataView<'a> {
    assert_eq!(syms.len(), times.len());
    // 按首次出现顺序建字典
    let mut unique: Vec<&str> = Vec::new();
    let mut ids: std::collections::HashMap<&str, u32> = std::collections::HashMap::new();
    let mut keys = Vec::with_capacity(syms.len());
    for s in syms {
        let n = unique.len() as u32;
        keys.push(*ids.entry(*s).or_insert_with(|| {
            unique.push(s);
            n
        }));
    }
    let mut offsets = vec![0u64];
    let mut strings = Vec::new();
    for s in &unique {
        strings.extend_from_slice(s.as_bytes());
        offsets.push(strings.len() as u64);
    }
    store.push(Column::from_dict(keys, offsets, strings, None));
    store.push(Column {
        data_type: DataType::TimestampUs,
        values: Buffer::from_slice_copy(times),
        validity: None,
        dict: None,
    });
    let schema = Schema::new(vec![
        FieldSchema::new("sym", DataType::Utf8),
        FieldSchema::new("time", DataType::TimestampUs),
    ]);
    let n = store.len();
    DataView::new(schema, vec![store[n - 2].as_view(), store[n - 1].as_view()])
        .expect("two-column view is valid by construction")
}

#[test]
fn interleaved_contiguous_syms_build() {
    // A: 1,2,3   B: 2,3,4   C: 10 → axis [1,2,3,4,10]
    // 不同 SYM 时间范围不同是合法的：各自都是轴的连续子区间
    let syms = ["A", "A", "A", "B", "B", "B", "C"];
    let times = [1i64, 2, 3, 2, 3, 4, 10];
    let mut store = Vec::new();
    let view = meta_view(&syms, &times, &mut store);
    let bytes = MetaBuilder::build(&view).unwrap();
    // header + AXIS(5×8B) + DICT OFFSETS(4×8B) + STRING DATA("ABC") + SYM INDEX(3×12B)
    assert_eq!(bytes.len(), META_HEADER_SIZE + 5 * 8 + 4 * 8 + 3 + 3 * SYM_INDEX_RECORD_SIZE);

    let header = MetaHeader::from_bytes(&bytes).unwrap();
    assert_eq!(header.time_count, 5);
    assert_eq!(header.sym_count, 3);
    assert_eq!(header.row_count, 7);
    // SYM INDEX 逐 record 校验：row_start = Σ 前序 time_count，span == 数据行数
    let base = header.sym_index_offset as usize;
    let rec = |i: usize| {
        SymIndexRecord::from_bytes(&bytes[base + i * SYM_INDEX_RECORD_SIZE..]).unwrap()
    };
    let a = rec(0);
    assert_eq!((a.time_start, a.time_count, a.row_start), (0, 3, 0));
    let b = rec(1);
    assert_eq!((b.time_start, b.time_count, b.row_start), (1, 3, 3));
    let c = rec(2);
    assert_eq!((c.time_start, c.time_count, c.row_start), (4, 1, 6));
}

#[test]
fn gap_inside_sym_rejected() {
    // A 的 time {1,3} 在轴 [1,2,3] 上跳空（2 由 B 贡献）→ 拒绝，
    // 否则轴跨度(3)会被当成 A 的行容量(2)，网格与数据错位
    let syms = ["A", "A", "B"];
    let times = [1i64, 3, 2];
    let mut store = Vec::new();
    let view = meta_view(&syms, &times, &mut store);
    assert!(matches!(
        MetaBuilder::build(&view),
        Err(CoreError::NonContiguousTime(s)) if s == "A"
    ));
}

#[test]
fn duplicate_time_inside_sym_rejected() {
    let syms = ["A", "A", "A"];
    let times = [1i64, 1, 2];
    let mut store = Vec::new();
    let view = meta_view(&syms, &times, &mut store);
    assert!(matches!(
        MetaBuilder::build(&view),
        Err(CoreError::NonContiguousTime(s)) if s == "A"
    ));
}

#[test]
fn unsorted_time_inside_sym_rejected() {
    let syms = ["A", "A", "A"];
    let times = [3i64, 1, 2];
    let mut store = Vec::new();
    let view = meta_view(&syms, &times, &mut store);
    assert!(matches!(
        MetaBuilder::build(&view),
        Err(CoreError::NonContiguousTime(s)) if s == "A"
    ));
}

#[test]
fn single_symbol_single_timestamp_boundary() {
    let syms = ["AAPL"];
    let times = [1000i64];
    let mut store = Vec::new();
    let view = meta_view(&syms, &times, &mut store);
    let bytes = MetaBuilder::build(&view).unwrap();
    let header = MetaHeader::from_bytes(&bytes).unwrap();
    assert_eq!(header.sym_count, 1);
    assert_eq!(header.time_count, 1);
    assert_eq!(header.row_count, 1);

    let temp_dir = std::env::temp_dir().join(format!("splayed_meta_single_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();
    let meta_path = temp_dir.join(".meta");
    splayed_core::create_meta_file(&meta_path, &view).unwrap();

    let meta = splayed_core::MetaHandle::open(&meta_path).unwrap();
    assert_eq!(meta.sym_str(0).unwrap(), "AAPL");
    assert_eq!(meta.sym_id_of("AAPL").unwrap(), Some(0));
    assert_eq!(meta.sym_id_of("GOOG").unwrap(), None);

    // 读取第 0 行
    let r_view = meta.read_index_handle(0, 1).unwrap();
    assert_eq!(r_view.length(), 1);
    assert_eq!(r_view.column("sym").unwrap().string_at(0), Some("AAPL"));

    // 读超出范围
    assert!(meta.read_index_handle(0, 2).is_err());
    assert!(meta.read_index_handle(1, 1).is_err());

    meta.close().unwrap();
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn read_index_multi_sym_crossing_segments() {
    // 构造跨多个标的的数据：A 有 3 行，B 有 4 行，C 有 2 行
    let syms = ["A", "A", "A", "B", "B", "B", "B", "C", "C"];
    let times = [10i64, 11, 12, 10, 11, 12, 13, 11, 12];
    let mut store = Vec::new();
    let view = meta_view(&syms, &times, &mut store);

    let temp_dir = std::env::temp_dir().join(format!("splayed_meta_crossing_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();
    let meta_path = temp_dir.join(".meta");
    splayed_core::create_meta_file(&meta_path, &view).unwrap();

    let meta = splayed_core::MetaHandle::open(&meta_path).unwrap();
    assert_eq!(meta.header().row_count, 9);

    // 1. 跨越 A 尾部到 B 头部：从第 2 行读到第 5 行 (offset 2, length 4)
    // 覆盖 A[2..3] (1 行) 和 B[0..3] (3 行)
    let read_view = meta.read_index_handle(2, 4).unwrap();
    assert_eq!(read_view.length(), 4);
    let sym_col = read_view.column("sym").unwrap();
    assert_eq!(sym_col.string_at(0), Some("A"));
    assert_eq!(sym_col.string_at(1), Some("B"));
    assert_eq!(sym_col.string_at(2), Some("B"));
    assert_eq!(sym_col.string_at(3), Some("B"));

    // 2. 跨越所有标的：从第 1 行读到第 8 行 (offset 1, length 7)
    // 覆盖 A 的后 2 行，B 的全部 4 行，C 的前 1 行
    let read_all = meta.read_index_handle(1, 7).unwrap();
    assert_eq!(read_all.length(), 7);
    let sym_col2 = read_all.column("sym").unwrap();
    assert_eq!(sym_col2.string_at(0), Some("A"));
    assert_eq!(sym_col2.string_at(1), Some("A"));
    assert_eq!(sym_col2.string_at(2), Some("B"));
    assert_eq!(sym_col2.string_at(5), Some("B"));
    assert_eq!(sym_col2.string_at(6), Some("C"));

    // 3. 读取 length = 0 边界
    let empty_view = meta.read_index_handle(0, 0).unwrap();
    assert_eq!(empty_view.length(), 0);

    meta.close().unwrap();
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn locate_index_edge_cases() {
    let syms = ["A", "A", "B", "B", "C"];
    let times = [10i64, 20, 10, 20, 30];
    let mut store = Vec::new();
    let view = meta_view(&syms, &times, &mut store);

    let temp_dir = std::env::temp_dir().join(format!("splayed_meta_locate_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();
    let meta_path = temp_dir.join(".meta");
    splayed_core::create_meta_file(&meta_path, &view).unwrap();

    let meta = splayed_core::MetaHandle::open(&meta_path).unwrap();

    // 1. 空查询
    let ranges = meta.locate_index_handle(&[]).unwrap();
    assert!(ranges.is_empty());

    // 2. 正常查询所有点
    let pairs: Vec<(String, i64)> = vec![
        ("A".into(), 10),
        ("A".into(), 20),
        ("B".into(), 10),
        ("B".into(), 20),
        ("C".into(), 30),
    ];
    let ranges = meta.locate_index_handle(&pairs).unwrap();
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].offset, 0);
    assert_eq!(ranges[0].length, 5);

    // 3. 不存在的 sym（小于首个标的）
    let bad_sym_lo = [("0000".into(), 10i64)];
    assert!(meta.locate_index_handle(&bad_sym_lo).is_err());

    // 4. 不存在的 sym（中间缺失）
    let bad_sym_mid = [("B_MISSED".into(), 10i64)];
    assert!(meta.locate_index_handle(&bad_sym_mid).is_err());

    // 5. 不存在的 sym（大于末尾标的）
    let bad_sym_hi = [("Z".into(), 10i64)];
    assert!(meta.locate_index_handle(&bad_sym_hi).is_err());

    // 6. 存在的 sym，但时间不在轴上
    let bad_time = [("A".into(), 15i64)];
    assert!(meta.locate_index_handle(&bad_time).is_err());

    // 7. 乱序输入应该被拒绝
    let unsorted = [("B".into(), 10i64), ("A".into(), 10i64)];
    assert!(meta.locate_index_handle(&unsorted).is_err());

    // 8. 重复输入应该被拒绝
    let dups = [("A".into(), 10i64), ("A".into(), 10i64)];
    assert!(meta.locate_index_handle(&dups).is_err());

    meta.close().unwrap();
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn scan_index_boundary_conditions() {
    let syms = ["MSFT", "MSFT", "MSFT", "TSLA", "TSLA"];
    let times = [100i64, 200, 300, 200, 300];
    let mut store = Vec::new();
    let view = meta_view(&syms, &times, &mut store);

    let temp_dir = std::env::temp_dir().join(format!("splayed_meta_scan_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();
    let meta_path = temp_dir.join(".meta");
    splayed_core::create_meta_file(&meta_path, &view).unwrap();

    let meta = splayed_core::MetaHandle::open(&meta_path).unwrap();

    // 1. 扫描整个范围（无谓词）
    let req_all = splayed_core::ScanRequest::default();
    let mut scanner = meta.scan_index_handle(&req_all).unwrap();
    let mut total_rows = 0;
    while let Some(r) = scanner.next().unwrap() {
        total_rows += r.length;
    }
    assert_eq!(total_rows, 5);

    // 2. 匹配单一 sym
    let req_sym = splayed_core::ScanRequest {
        ranges: vec![],
        projection: vec![],
        predicate: Some(splayed_core::Predicate::cmp("sym", splayed_core::CmpOp::Eq, splayed_core::Scalar::Str("TSLA".into()))),
        limit: None,
    };
    let mut scanner = meta.scan_index_handle(&req_sym).unwrap();
    let r = scanner.next().unwrap().unwrap();
    assert_eq!(r.offset, 3);
    assert_eq!(r.length, 2);
    assert_eq!(scanner.next().unwrap(), None);

    // 3. 时间窗口完全在数据左侧（命中 0 行）
    let req_time_before = splayed_core::ScanRequest {
        ranges: vec![],
        projection: vec![],
        predicate: Some(splayed_core::Predicate::cmp("time", splayed_core::CmpOp::Lt, splayed_core::Scalar::Int(50))),
        limit: None,
    };
    let mut scanner = meta.scan_index_handle(&req_time_before).unwrap();
    assert_eq!(scanner.next().unwrap(), None);

    // 4. 时间窗口完全在数据右侧（命中 0 行）
    let req_time_after = splayed_core::ScanRequest {
        ranges: vec![],
        projection: vec![],
        predicate: Some(splayed_core::Predicate::cmp("time", splayed_core::CmpOp::Gt, splayed_core::Scalar::Int(500))),
        limit: None,
    };
    let mut scanner = meta.scan_index_handle(&req_time_after).unwrap();
    assert_eq!(scanner.next().unwrap(), None);

    // 5. limit = 1 早停
    let req_limit = splayed_core::ScanRequest {
        ranges: vec![],
        projection: vec![],
        predicate: None,
        limit: Some(1),
    };
    let mut scanner = meta.scan_index_handle(&req_limit).unwrap();
    let r = scanner.next().unwrap().unwrap();
    assert_eq!(r.length, 1);
    assert_eq!(scanner.next().unwrap(), None);

    meta.close().unwrap();
    let _ = std::fs::remove_dir_all(&temp_dir);
}
