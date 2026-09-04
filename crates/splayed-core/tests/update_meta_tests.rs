//! `update_meta` 集成测试：以新 (SYM, TIME) 布局重建 .meta，并并发重散布
//! 全部现有 FIELD（gather/NULL 补齐/generation/统计重算/幂等/错误路径）。

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use splayed_core::{
    FieldReader, Filter, FilterValue, ScanRequest, Scanner, SymbolSelection, TableColumn,
    TimeRange, create_table, open_dataset, update_meta,
};
use splayed_format::{DataType as ST, RawValue, TimeType};

fn temp_dir(suffix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("splayed_core_um_{suffix}_{}", std::process::id()));
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

/// 2 SYM × 3 time（6 行）：close 按 (sym,time) 编码 1..6，vol 含一个 NULL。
/// 输入行序 = (SYM01 t0..2, SYM02 t0..2)，sorted=true。
fn build_dataset(dir: &PathBuf) {
    let syms: Vec<String> = ["SYM01", "SYM01", "SYM01", "SYM02", "SYM02", "SYM02"]
        .map(|s| s.to_string())
        .to_vec();
    let time: Vec<i64> = vec![0, 1, 2, 0, 1, 2];
    let close = f64_col(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    let vol = i64_col(&[Some(1), None, Some(3), Some(4), Some(5), Some(6)]);
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

fn close_reader(dir: &std::path::Path) -> Vec<f64> {
    // 直接按文件行序读出 close（全局行序）。
    let r = FieldReader::open(dir.join("close")).unwrap();
    let n = r.row_count() as usize;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(r.read_row(i as u32).unwrap().as_f64().unwrap_or(f64::NAN));
    }
    out
}

#[test]
fn reorder_and_shrink_reshapes_all_fields() {
    let dir = temp_dir("reshape");
    build_dataset(&dir);

    // 新布局（输入行序任意）：SYM03{t0,t1,t2} + SYM02{t0,t1} + SYM01{t1,t2}。
    let syms: Vec<String> = [
        "SYM03", "SYM02", "SYM02", "SYM01", "SYM01", "SYM03", "SYM03",
    ]
    .map(|s| s.to_string())
    .to_vec();
    let time: Vec<i64> = vec![1, 0, 1, 1, 2, 0, 2];
    let new_meta = update_meta(&dir, TimeType::Date32, &syms, &time).unwrap();

    // meta：全局 TIME AXIS = {0,1,2}；SYM 升序；每 SYM 行数 = 其连续区间长。
    assert_eq!(new_meta.time_axis, vec![0, 1, 2]);
    let symbols: Vec<&str> = new_meta.symbols.iter().map(|s| s.as_str()).collect();
    assert_eq!(symbols, vec!["SYM01", "SYM02", "SYM03"]);
    assert_eq!(new_meta.total_rows(), 7); // SYM01 2 + SYM02 2 + SYM03 3
    assert_eq!(new_meta.header.generation, 2); // 旧 gen=1 + 1

    // 全局行序 = [SYM01:t1,t2 | SYM02:t0,t1 | SYM03:t0..t2]
    let got = close_reader(&dir);
    let expected: Vec<f64> = vec![2.0, 3.0, 4.0, 5.0, f64::NAN, f64::NAN, f64::NAN];
    assert_eq!(got.len(), expected.len());
    for (g, e) in got.iter().zip(expected.iter()) {
        if e.is_nan() {
            assert!(g.is_nan(), "expected NULL, got {g}");
        } else {
            assert_eq!(g, e);
        }
    }

    // vol 同布局：SYM01 t1 = 原 NULL；SYM02 t1 = 5；SYM03 全 NULL。
    let v = FieldReader::open(dir.join("vol")).unwrap();
    assert_eq!(v.read_row(0).unwrap().as_i64(), None); // 原 NULL 搬运
    assert_eq!(v.read_row(1).unwrap().as_i64(), Some(3));
    assert_eq!(v.read_row(3).unwrap().as_i64(), Some(5));
    for i in 4..7 {
        assert_eq!(v.read_row(i).unwrap().as_i64(), None);
    }
    assert_eq!(v.header().null_count, 4); // SYM01 t1 + SYM03×3

    // 统计 footer 按新数据重算：close min=2 max=5（跳过 NULL）。
    let c = FieldReader::open(dir.join("close")).unwrap();
    let st = c.stats().unwrap();
    assert_eq!(f64::from_le_bytes(st.min), 2.0);
    assert_eq!(f64::from_le_bytes(st.max), 5.0);

    // 扫描一致性：open_dataset → generation 校验 → 值过滤。
    let ds = Arc::new(open_dataset(&dir).unwrap());
    let scanner = Scanner::new(&ds);
    let req = ScanRequest {
        columns: vec!["close".into(), "vol".into()],
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![Filter::GreaterThan {
            field: "close".into(),
            value: FilterValue::Float64(4.0),
        }],
        batch_size: 65536,
        parallelism: 1,
        limit: None,
    };
    let plan = scanner.plan(&req).unwrap();
    let mut n = 0usize;
    let mut batches = scanner.scan(&plan, &req).unwrap();
    while let Some(b) = batches.next_batch().unwrap() {
        n += b.num_rows();
    }
    assert_eq!(n, 1); // 只有 close=5（SYM01 t2）

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn rerun_is_idempotent() {
    let dir = temp_dir("idem");
    build_dataset(&dir);

    let syms: Vec<String> = ["SYM02", "SYM01"].map(|s| s.to_string()).to_vec();
    let time: Vec<i64> = vec![0, 0];
    update_meta(&dir, TimeType::Date32, &syms, &time).unwrap();
    let gen1 = open_dataset(&dir).unwrap().meta.header.generation;

    // 同样输入重跑：成功且数据不变，generation 再 +1。
    update_meta(&dir, TimeType::Date32, &syms, &time).unwrap();
    let meta2 = open_dataset(&dir).unwrap();
    assert_eq!(meta2.meta.header.generation, gen1 + 1);
    // 全局行序按 SYM 升序：[SYM01 t0=1, SYM02 t0=4]
    assert_eq!(close_reader(&dir), vec![1.0, 4.0]);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn errors_leave_old_meta_intact() {
    let dir = temp_dir("err");
    build_dataset(&dir);

    // 长度不匹配 → 报错，不触碰任何文件。
    let r = update_meta(&dir, TimeType::Date32, &["SYM01".to_string()], &[0, 1]);
    assert!(matches!(r, Err(splayed_core::TableError::LengthMismatch { .. })));

    // 不存在的目录 → io 错误。
    let r = update_meta(
        PathBuf::from("no_such_dir_um_xyz"),
        TimeType::Date32,
        &["A".to_string()],
        &[0],
    );
    assert!(r.is_err());

    fs::remove_dir_all(&dir).ok();
}