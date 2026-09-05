//! Dataset 层集成测试：create / open / read / write / scan / 统计 / 结构操作 / locate。

use std::path::{Path, PathBuf};

use splayed_core::{
    create_dataset, create_dataset_index, delete_dataset, open_dataset, CoreError, CmpOp,
    DatasetFieldInit, FieldChunkReader, Mode, Predicate, Scalar, ScanRequest, StreamValues,
};
use splayed_format::{Buffer, Column, Data, DataType, FieldSchema, Schema};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("splayed_core_dataset_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// 从 (sym, time, price) 三列构造 Data；sym 字典编码。
fn make_data(syms: &[&str], times: &[i64], prices: &[f64]) -> Data {
    assert_eq!(syms.len(), times.len());
    assert_eq!(syms.len(), prices.len());
    let mut dict: Vec<&str> = syms.to_vec();
    dict.sort();
    dict.dedup();
    let mut offsets = vec![0u64];
    let mut strings = Vec::new();
    for s in &dict {
        strings.extend_from_slice(s.as_bytes());
        offsets.push(strings.len() as u64);
    }
    let keys: Vec<u32> = syms
        .iter()
        .map(|s| dict.iter().position(|x| x == s).unwrap() as u32)
        .collect();
    let sym_col = Column::from_dict(keys, offsets, strings, None);
    let time_col = Column {
        data_type: DataType::TimestampUs,
        values: Buffer::from_slice_copy(times),
        validity: None,
        dict: None,
    };
    let price_col = Column {
        data_type: DataType::Float64,
        values: Buffer::from_slice_copy(prices),
        validity: None,
        dict: None,
    };
    Data::new(
        Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", DataType::TimestampUs),
            FieldSchema::new("price", DataType::Float64),
        ]),
        vec![sym_col, time_col, price_col],
    )
    .unwrap()
}

/// 3 sym × 不同 time 数，按 sym ASC 排列：AAPL 3 行、GOOG 3 行、MSFT 2 行（L = 8）。
fn sample_data() -> Data {
    make_data(
        &["AAPL", "AAPL", "AAPL", "GOOG", "GOOG", "GOOG", "MSFT", "MSFT"],
        &[100, 200, 300, 100, 200, 300, 100, 200],
        &[10.0, 11.0, 12.0, 30.0, 31.0, 32.0, 20.0, 21.0],
    )
}

#[test]
fn dataset_create_read_write_roundtrip() {
    let dir = temp_dir("roundtrip");
    let root = dir.join("ds");
    create_dataset(&root, sample_data()).unwrap();
    assert!(root.join(".meta").exists());
    assert!(root.join("price").exists());
    // 重复创建 → AlreadyExists
    assert!(create_dataset(&root, sample_data()).is_err());

    let ds = open_dataset(&root, Mode::Read).unwrap();
    let schema = ds.read_dataset_schema();
    assert_eq!(schema.len(), 3);
    assert_eq!(schema.data_type_of("price"), Some(DataType::Float64));

    // 默认列语义：columns = None → 只返回 sym 与 time（设计文档 §7.10）
    let view = ds.read_dataset(0, 8, None).unwrap();
    assert_eq!(view.schema.len(), 2);
    assert!(view.column("price").is_none());

    // 全量读取：sym / time / price
    let view = ds.read_dataset(0, 8, Some(&["price"])).unwrap();
    assert_eq!(view.length(), 8);
    assert_eq!(view.schema.len(), 3);
    let sym_col = view.column("sym").unwrap();
    assert_eq!(sym_col.string_at(0), Some("AAPL"));
    assert_eq!(sym_col.string_at(3), Some("GOOG"));
    assert_eq!(sym_col.string_at(7), Some("MSFT"));
    let time_col = view.column("time").unwrap();
    let times: Vec<i64> = time_col
        .segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, i64>(s.fixed_bytes().unwrap()).to_vec())
        .collect();
    assert_eq!(times, vec![100, 200, 300, 100, 200, 300, 100, 200]);
    let price_col = view.column("price").unwrap();
    let prices: Vec<f64> = price_col
        .segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, f64>(s.fixed_bytes().unwrap()).to_vec())
        .collect();
    assert_eq!(prices, vec![10.0, 11.0, 12.0, 30.0, 31.0, 32.0, 20.0, 21.0]);

    // projection：只取 price（sym/time 恒在）
    let view = ds.read_dataset(0, 8, Some(&["price"])).unwrap();
    assert_eq!(view.schema.len(), 3); // sym + time + price
    assert!(view.column("price").is_some());

    // statistics（容量网格：row_count = 8）
    let stats = ds.read_dataset_statistics().unwrap();
    assert_eq!(stats.row_count, 8);
    assert_eq!(stats.sym_count, 3);
    assert_eq!(stats.sym_min.as_deref(), Some("AAPL"));
    assert_eq!(stats.sym_max.as_deref(), Some("MSFT"));
    assert_eq!(stats.time_min, 100);
    assert_eq!(stats.time_max, 300);

    // 越界
    assert!(ds.read_dataset(5, 5, None).is_err());
    ds.close_dataset().unwrap();
    cleanup(&dir);
}

#[test]
fn dataset_write_and_scan() {
    let dir = temp_dir("write_scan");
    let root = dir.join("ds");
    create_dataset(&root, sample_data()).unwrap();
    let ds = open_dataset(&root, Mode::Write).unwrap();

    // write：覆盖 MSFT 两行（逻辑行 [3, 5)）
    let patch = Column {
        data_type: DataType::Float64,
        values: Buffer::from_slice_copy(&[99.0, 98.0]),
        validity: None,
        dict: None,
    };
    let schema = Schema::new(vec![FieldSchema::new("price", DataType::Float64)]);
    let view = splayed_format::DataView::new(schema, vec![patch.as_view()]).unwrap();
    ds.write_dataset(3, &view).unwrap();

    // read 验证
    let view = ds.read_dataset(3, 2, Some(&["price"])).unwrap();
    let prices: Vec<f64> = view
        .column("price")
        .unwrap()
        .segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, f64>(s.fixed_bytes().unwrap()).to_vec())
        .collect();
    assert_eq!(prices, vec![99.0, 98.0]);

    // scan：time >= 200 → AAPL [1,3) + MSFT [4,5) + GOOG [6,8)
    let pred = Predicate::cmp("time", CmpOp::Ge, Scalar::Int(200));
    let mut scanner = ds.scan_dataset(&ScanRequest { ranges: vec![], projection: vec![], predicate: Some(pred), limit: None }).unwrap();
    let mut rows = Vec::new();
    while let Some(r) = scanner.next().unwrap() {
        for i in 0..r.length {
            rows.push(r.offset + i);
        }
    }
    scanner.close().unwrap();
    assert_eq!(rows, vec![1, 2, 4, 5, 7]);

    // scan：sym == "MSFT" → 行 [6, 8)
    let pred = Predicate::cmp("sym", CmpOp::Eq, Scalar::Str("MSFT".into()));
    let mut scanner = ds.scan_dataset(&ScanRequest { ranges: vec![], projection: vec![], predicate: Some(pred), limit: None }).unwrap();
    let mut rows = Vec::new();
    while let Some(r) = scanner.next().unwrap() {
        for i in 0..r.length {
            rows.push(r.offset + i);
        }
    }
    scanner.close().unwrap();
    assert_eq!(rows, vec![6, 7]);

    // scan：price < 15 → AAPL 的行 [0, 3) 中 10, 11, 12 命中 → 行 0, 1, 2
    let pred = Predicate::cmp("price", CmpOp::Lt, Scalar::Float(15.0));
    let mut scanner = ds.scan_dataset(&ScanRequest { ranges: vec![], projection: vec![], predicate: Some(pred), limit: None }).unwrap();
    let mut rows = Vec::new();
    while let Some(r) = scanner.next().unwrap() {
        for i in 0..r.length {
            rows.push(r.offset + i);
        }
    }
    scanner.close().unwrap();
    assert_eq!(rows, vec![0, 1, 2]);

    // 组合：sym == "GOOG" AND price > 30 → GOOG 已被上面 write 改为 99/98/32 → 行 [3, 6) 全命中
    let pred = Predicate::And(vec![
        Predicate::cmp("sym", CmpOp::Eq, Scalar::Str("GOOG".into())),
        Predicate::cmp("price", CmpOp::Gt, Scalar::Float(30.0)),
    ]);
    let mut scanner = ds.scan_dataset(&ScanRequest { ranges: vec![], projection: vec![], predicate: Some(pred), limit: None }).unwrap();
    let mut rows = Vec::new();
    while let Some(r) = scanner.next().unwrap() {
        for i in 0..r.length {
            rows.push(r.offset + i);
        }
    }
    scanner.close().unwrap();
    assert_eq!(rows, vec![3, 4, 5]);

    ds.close_dataset().unwrap();
    cleanup(&dir);
}

#[test]
fn dataset_struct_ops_and_compression() {
    let dir = temp_dir("struct");
    let root = dir.join("ds");
    create_dataset(&root, sample_data()).unwrap();
    let mut ds = open_dataset(&root, Mode::Write).unwrap();

    // create：全 NULL 字段
    ds.create_dataset_field( "volume", DataType::Int64, DatasetFieldInit::AllNull).unwrap();
    assert_eq!(ds.read_dataset_schema().data_type_of("volume"), Some(DataType::Int64));
    let view = ds.read_dataset(0, 8, Some(&["volume"])).unwrap();
    assert_eq!(view.column("volume").unwrap().null_count(), 8);
    // 重复创建 → Error；保留名 → Error
    assert!(ds.create_dataset_field( "volume", DataType::Int64, DatasetFieldInit::AllNull).is_err());
    assert!(ds.create_dataset_field( "sym", DataType::Utf8, DatasetFieldInit::AllNull).is_err());

    // create：带数据初始化（长度必须 == L）
    let col = Column {
        data_type: DataType::Int64,
        values: Buffer::from_slice_copy(&vec![1i64; 8]),
        validity: None,
        dict: None,
    };
    ds.create_dataset_field( "qty", DataType::Int64, DatasetFieldInit::Data(col)).unwrap();
    assert!(ds.create_dataset_field( "qty2", DataType::Int64, DatasetFieldInit::Data(Column {
        data_type: DataType::Int64,
        values: Buffer::from_slice_copy(&vec![1i64; 3]),
        validity: None,
        dict: None,
    })).is_err());

    // rename
    ds.rename_dataset_field( "qty", "quantity").unwrap();
    assert_eq!(ds.read_dataset_schema().data_type_of("quantity"), Some(DataType::Int64));
    assert!(ds.read_dataset_schema().data_type_of("qty").is_none());

    // cast：f64 → i64（as 语义）
    ds.cast_dataset_field("price", DataType::Int64).unwrap();
    let view = ds.read_dataset(0, 8, Some(&["price"])).unwrap();
    let prices: Vec<i64> = view
        .column("price")
        .unwrap()
        .segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, i64>(s.fixed_bytes().unwrap()).to_vec())
        .collect();
    assert_eq!(prices, vec![10, 11, 12, 30, 31, 32, 20, 21]);

    // compress + compressed 下 read/write
    ds.cast_dataset_field("price", DataType::Float64).unwrap();
    ds.compress_dataset_field("price").unwrap();
    let view = ds.read_dataset(2, 4, Some(&["price"])).unwrap();
    let prices: Vec<f64> = view
        .column("price")
        .unwrap()
        .segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, f64>(s.fixed_bytes().unwrap()).to_vec())
        .collect();
    assert_eq!(prices, vec![12.0, 30.0, 31.0, 32.0]);

    // delete
    ds.delete_dataset_field( "volume").unwrap();
    assert!(ds.read_dataset_schema().data_type_of("volume").is_none());

    ds.close_dataset().unwrap();

    // close 后文件仍在
    assert!(root.join("price").exists());
    assert!(root.join("quantity").exists());
    cleanup(&dir);
}

#[test]
fn dataset_locate_index() {
    let dir = temp_dir("locate");
    let root = dir.join("ds");
    create_dataset(&root, sample_data()).unwrap();
    let ds = open_dataset(&root, Mode::Read).unwrap();

    // 有序唯一输入：AAPL@100 → row 0，AAPL@300 → row 2，GOOG@200 → row 4, GOOG@300 → row 5
    let pairs = vec![
        ("AAPL".to_string(), 100),
        ("AAPL".to_string(), 300),
        ("GOOG".to_string(), 200),
        ("GOOG".to_string(), 300),
    ];
    let ranges = ds.locate_dataset_index(&pairs).unwrap();
    let total: u64 = ranges.iter().map(|r| r.length).sum();
    assert_eq!(total, 4);
    assert!(ranges[0].contains(0));
    assert!(ranges[1].contains(2));
    assert!(ranges[2].contains(4));
    assert!(ranges[2].contains(5));

    // key 不存在 → Error
    assert!(ds.locate_dataset_index(&[("AAPL".to_string(), 999)]).is_err());
    assert!(ds.locate_dataset_index(&[("TSLA".to_string(), 100)]).is_err());
    // 无序输入 → Error
    assert!(ds
        .locate_dataset_index(&[
            ("GOOG".to_string(), 100),
            ("AAPL".to_string(), 100),
        ])
        .is_err());
    ds.close_dataset().unwrap();
    cleanup(&dir);
}

#[test]
fn dataset_index_rebuild() {
    let dir = temp_dir("reindex");
    let root = dir.join("ds");
    create_dataset(&root, sample_data()).unwrap();
    // 重建 .meta（同输入）
    let data = sample_data();
    create_dataset_index(&root, &data.as_view()).unwrap();
    let ds = open_dataset(&root, Mode::Read).unwrap();
    let view = ds.read_dataset(0, 8, None).unwrap();
    assert_eq!(view.length(), 8);
    ds.close_dataset().unwrap();

    // 删除整个 Dataset
    delete_dataset(&root).unwrap();
    assert!(!root.exists());
    cleanup(&dir);
}

/// 立即失败的流式 reader（模拟源数据中断）。
struct FailReader;
impl FieldChunkReader for FailReader {
    fn next_values(&mut self) -> splayed_core::Result<Option<StreamValues>> {
        Err(CoreError::Invalid("stream source aborted".into()))
    }
    fn next_validity(&mut self) -> splayed_core::Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

#[test]
fn create_dataset_field_failure_leaves_no_residue() {
    let dir = temp_dir("create_field_fail");
    let root = dir.join("ds");
    let data = make_data(
        &["A", "A", "A", "A", "B", "B", "B", "B"],
        &[1, 2, 3, 4, 1, 2, 3, 4],
        &[10.0, 11.0, 12.0, 13.0, 20.0, 21.0, 22.0, 23.0],
    );
    create_dataset(&root, data).unwrap();

    let mut ds = open_dataset(&root, Mode::Write).unwrap();
    let err = ds
        .create_dataset_field("vol", DataType::Int64, DatasetFieldInit::Stream { reader: Box::new(FailReader) })
        .unwrap_err();
    assert!(matches!(err, CoreError::Invalid(_)));
    // 半成品不落痕：文件不存在、Schema 不含该字段
    assert!(!root.join("vol").exists());
    assert!(ds.read_dataset_schema().position("vol").is_none());
    ds.close_dataset().unwrap();

    // 重新 open 不受残留影响（这是修复前的失败场景：零填充占位 header 破坏 build_schema）
    let ds = open_dataset(&root, Mode::Read).unwrap();
    assert!(ds.read_dataset_schema().position("vol").is_none());
    ds.close_dataset().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
