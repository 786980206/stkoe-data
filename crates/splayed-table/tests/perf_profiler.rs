//! 性能分析：各阶段独立计时，64K 行数据量，3 次取中位数。

use std::path::{Path, PathBuf};
use std::time::Instant;

use splayed_format::{Buffer, Column, Data, DataType, FieldSchema, Schema};
use splayed_table::create_table;

const SYMS: usize = 256;
const ROWS_PER_SYM: usize = 250;
const TOTAL: usize = SYMS * ROWS_PER_SYM; // 64K

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("splayed_perf_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn make_data() -> Data {
    let mut dict: Vec<String> = (0..SYMS).map(|i| format!("SYM{:04}", i)).collect();
    dict.sort();
    let mut offsets = vec![0u64];
    let mut strings = Vec::new();
    for s in &dict {
        strings.extend_from_slice(s.as_bytes());
        offsets.push(strings.len() as u64);
    }
    let mut keys = Vec::with_capacity(TOTAL);
    let mut times = Vec::with_capacity(TOTAL);
    let mut prices = Vec::with_capacity(TOTAL);
    let mut vols = Vec::with_capacity(TOTAL);
    for i in 0..SYMS {
        let key = dict.iter().position(|s| *s == format!("SYM{:04}", i)).unwrap() as u32;
        for j in 0..ROWS_PER_SYM {
            keys.push(key);
            times.push((18_000 + j) as i32);
            prices.push(100.0 + ((i * 7 + j * 13) % 97) as f64 * 0.1);
            vols.push(1000 + ((i * 3 + j) % 501) as i64);
        }
    }
    let sym_col = Column::from_dict(keys, offsets, strings, None);
    Data::new(
        Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", DataType::Date32),
            FieldSchema::new("price", DataType::Float64),
            FieldSchema::new("volume", DataType::Int64),
        ]),
        vec![
            sym_col,
            Column { data_type: DataType::Date32, values: Buffer::from_slice_copy(&times), validity: None, dict: None },
            Column { data_type: DataType::Float64, values: Buffer::from_slice_copy(&prices), validity: None, dict: None },
            Column { data_type: DataType::Int64, values: Buffer::from_slice_copy(&vols), validity: None, dict: None },
        ],
    )
    .unwrap()
}

#[test]
fn perf_profiler() {
    let data = make_data();
    let root = temp_dir("profiler");
    let mut times: Vec<(&str, std::time::Duration)> = Vec::new();

    // ---- 1. create_table（端到端写入）----
    let t0 = Instant::now();
    create_table(&root, &data, splayed_table::PartitionScheme::None).unwrap();
    times.push(("create_table (total)", t0.elapsed()));

    // 文件大小
    let mut total_bytes = 0u64;
    for entry in walkdir(&root) {
        let sz = entry.metadata().map(|m| m.len()).unwrap_or(0);
        total_bytes += sz;
    }
    times.push(("  files on disk (bytes)", std::time::Duration::from_secs(total_bytes)));

    // ---- 2. open_dataset ----
    let t1 = Instant::now();
    let ds = splayed_core::open_dataset(&root, splayed_core::Mode::Read).unwrap();
    times.push(("open_dataset", t1.elapsed()));

    // ---- 3. read_dataset（全量）----
    let t2 = Instant::now();
    let view = ds.read_dataset(0, TOTAL as u64, Some(&["price", "volume"])).unwrap();
    times.push(("read_dataset (64K rows)", t2.elapsed()));
    assert_eq!(view.length(), TOTAL);

    // ---- 4. read_dataset 局部（1000 行）----
    let t3 = Instant::now();
    let _view_small = ds.read_dataset(1000, 1000, Some(&["price"])).unwrap();
    times.push(("read_dataset (1000 rows)", t3.elapsed()));

    // ---- 5. scan_dataset 无谓词 ----
    let t4 = Instant::now();
    let mut scanner = ds.scan_dataset(&splayed_core::ScanRequest::default()).unwrap();
    let mut ranges = 0;
    while scanner.next().unwrap().is_some() { ranges += 1; }
    assert!(ranges > 0, "scan must produce ranges");
    scanner.close().unwrap();
    times.push(("scan_dataset (no filter)", t4.elapsed()));

    // ---- 6. scan_dataset 谓词 ----
    let t5 = Instant::now();
    let pred = splayed_core::Predicate::cmp(
        "price", splayed_core::CmpOp::Gt, splayed_core::Scalar::Float(105.0),
    );
    let mut scanner = ds.scan_dataset(&splayed_core::ScanRequest {
        ranges: vec![],
        projection: vec![],
        predicate: Some(pred),
        limit: None,
    }).unwrap();
    let mut pred_rows = 0u64;
    while let Some(r) = scanner.next().unwrap() { pred_rows += r.length; }
    times.push(("scan_dataset (price>15)", t5.elapsed()));
    assert!(pred_rows > 0);

    // ---- 7. close_dataset ----
    let t6 = Instant::now();
    ds.close_dataset().unwrap();
    times.push(("close_dataset", t6.elapsed()));

    // ---- 8. 按月分区 create_table ----
    let root2 = temp_dir("profiler_month");
    let t7 = Instant::now();
    create_table(&root2, &data, splayed_table::PartitionScheme::Month).unwrap();
    times.push(("create_table (month, 12 partitions)", t7.elapsed()));
    let _ = std::fs::remove_dir_all(&root2);

    // ---- 9. rename_table_field ----
    let t8 = Instant::now();
    let mut ds2 = splayed_core::open_dataset(&root, splayed_core::Mode::Write).unwrap();
    ds2.rename_dataset_field("price", "price_renamed").unwrap();
    times.push(("rename_dataset_field", t8.elapsed()));

    // ---- 10. compress ----
    let t9 = Instant::now();
    ds2.rename_dataset_field("price_renamed", "price").unwrap();
    ds2.compress_dataset_field("price").unwrap();
    times.push(("compress_dataset_field (64K rows)", t9.elapsed()));

    // ---- 11. decompress ----
    let t10 = Instant::now();
    ds2.decompress_dataset_field("price").unwrap();
    times.push(("decompress_dataset_field", t10.elapsed()));

    ds2.close_dataset().unwrap();
    let _ = std::fs::remove_dir_all(&root);

    // 输出
    eprintln!("\n==== 性能分析: {} sym × {} rows = {} rows × 4 cols ====", SYMS, ROWS_PER_SYM, TOTAL);
    for (name, dur) in &times {
        if name.contains("bytes") {
            eprintln!("  {:<45} {} bytes", name, dur.as_secs());
        } else {
            eprintln!("  {:<45} {:?}", name, dur);
        }
    }
}

fn walkdir(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                out.extend(walkdir(&entry.path()));
            } else {
                out.push(entry.path());
            }
        }
    }
    out
}
