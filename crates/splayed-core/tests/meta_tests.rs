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
