//! Dataset 层集成测试：create / open / read / write / scan / 统计 / 结构操作 / locate。

use std::path::{Path, PathBuf};

use splayed_core::{
    close_field_handle, create_dataset, create_dataset_data, create_dataset_index, delete_dataset, open_dataset,
    open_field_file, CoreError, CmpOp, DatasetFieldInit, FieldChunkReader, Mode, Predicate,
    Scalar, ScanRequest, StreamValues,
};
use splayed_format::{Bitmap, Buffer, Column, Data, DataType, FieldSchema, Schema};

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
    create_dataset_data(&root, sample_data(), splayed_core::CreateDatasetOptions::default()).unwrap();
    assert!(root.join(".meta").exists());
    assert!(root.join("price").exists());
    // 重复创建 → AlreadyExists
    assert!(create_dataset_data(&root, sample_data(), splayed_core::CreateDatasetOptions::default()).is_err());

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
    create_dataset_data(&root, sample_data(), splayed_core::CreateDatasetOptions::default()).unwrap();
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
    create_dataset_data(&root, sample_data(), splayed_core::CreateDatasetOptions::default()).unwrap();
    let mut ds = open_dataset(&root, Mode::Write).unwrap();

    // create：全 NULL 字段
    ds.create_dataset_field( "volume", DataType::Int64, DatasetFieldInit::AllNull, splayed_core::CreateFieldOptions::default()).unwrap();
    // 验证底层物理文件仅 64 字节
    assert_eq!(std::fs::metadata(root.join("volume")).unwrap().len(), 64);
    assert_eq!(ds.read_dataset_schema().data_type_of("volume"), Some(DataType::Int64));
    let view = ds.read_dataset(0, 8, Some(&["volume"])).unwrap();
    assert_eq!(view.column("volume").unwrap().null_count(), 8);
    // 重复创建 → Error；保留名 → Error
    assert!(ds.create_dataset_field( "volume", DataType::Int64, DatasetFieldInit::AllNull, splayed_core::CreateFieldOptions::default()).is_err());
    assert!(ds.create_dataset_field( "sym", DataType::Utf8, DatasetFieldInit::AllNull, splayed_core::CreateFieldOptions::default()).is_err());

    // create：带数据初始化（长度必须 == L）
    let col = Column {
        data_type: DataType::Int64,
        values: Buffer::from_slice_copy(&vec![1i64; 8]),
        validity: None,
        dict: None,
    };
    ds.create_dataset_field("qty", DataType::Int64, DatasetFieldInit::Data(col), splayed_core::CreateFieldOptions::default()).unwrap();
    assert!(ds.create_dataset_field("qty2", DataType::Int64, DatasetFieldInit::Data(Column {
        data_type: DataType::Int64,
        values: Buffer::from_slice_copy(&vec![1i64; 3]),
        validity: None,
        dict: None,
    }), splayed_core::CreateFieldOptions::default()).is_err());

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
    create_dataset_data(&root, sample_data(), splayed_core::CreateDatasetOptions::default()).unwrap();
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
    create_dataset_data(&root, sample_data(), splayed_core::CreateDatasetOptions::default()).unwrap();
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
    create_dataset_data(&root, data, splayed_core::CreateDatasetOptions::default()).unwrap();

    let mut ds = open_dataset(&root, Mode::Write).unwrap();
    let err = ds
        .create_dataset_field(
            "vol",
            DataType::Int64,
            DatasetFieldInit::Stream { reader: Box::new(FailReader) },
            splayed_core::CreateFieldOptions::default(),
        )
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

/// 两字段数据（price + volume），确保 create_dataset 命中并行分支（字段数 ≥ 2）。
fn two_field_data() -> Data {
    let mut data = make_data(
        &["A", "A", "B", "B"],
        &[1, 2, 1, 2],
        &[10.0, 11.0, 20.0, 21.0],
    );
    let vol: Vec<i64> = vec![1, 2, 3, 4];
    data.schema.fields.push(FieldSchema::new("volume", DataType::Int64));
    data.columns.push(Column {
        data_type: DataType::Int64,
        values: Buffer::from_slice_copy(&vol),
        validity: None,
        dict: None,
    });
    data
}

#[test]
fn create_dataset_parallel_options() {
    let dir = temp_dir("create_par");
    let data = two_field_data();

    // 串行（max_parallelism = 1）
    let root1 = dir.join("serial");
    create_dataset_data(&root1, data.clone(), splayed_core::CreateDatasetOptions { max_parallelism: 1, ..Default::default() }).unwrap();
    // 并行（max_parallelism = 8 > 字段数 → P = 2）
    let root2 = dir.join("par");
    create_dataset_data(&root2, data, splayed_core::CreateDatasetOptions { max_parallelism: 8, ..Default::default() }).unwrap();

    for root in [&root1, &root2] {
        let ds = open_dataset(&root, Mode::Read).unwrap();
        assert_eq!(ds.read_dataset_schema().fields.len(), 4); // sym / time / price / volume
        let view = ds.read_dataset(0, 4, Some(&["price", "volume"])).unwrap();
        assert_eq!(view.length(), 4);
        assert_eq!(view.column("volume").unwrap().length(), 4);
        ds.close_dataset().unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ------------------------------------------------- 并行路径（write / scan / read 组装）

/// 大体量数据（1 sym × n 行；price 伪随机 f64、volume 每 97 行一个 NULL 的 i64），
/// 字节量足以越过 write_dataset / scan_dataset 的并行门槛。
fn large_two_field_data(n: usize) -> Data {
    let sym_col = Column::from_dict(vec![0u32; n], vec![0, 1], b"A".to_vec(), None);
    let time_col = Column {
        data_type: DataType::TimestampUs,
        values: Buffer::from_slice_copy(
            &(0..n).map(|i| 1_000_000 + i as i64).collect::<Vec<i64>>(),
        ),
        validity: None,
        dict: None,
    };
    let price_col = Column {
        data_type: DataType::Float64,
        values: Buffer::from_slice_copy(
            &(0..n)
                .map(|i| ((i.wrapping_mul(2654435761)) % 1000) as f64)
                .collect::<Vec<f64>>(),
        ),
        validity: None,
        dict: None,
    };
    let vol_bits = {
        let mut bytes = vec![0u8; n.div_ceil(8)];
        for i in 0..n {
            if i % 97 != 0 {
                bytes[i / 8] |= 1 << (i % 8);
            }
        }
        bytes
    };
    let volume_col = Column {
        data_type: DataType::Int64,
        values: Buffer::from_slice_copy(
            &(0..n).map(|i| (i % 7) as i64).collect::<Vec<i64>>(),
        ),
        validity: Some(Bitmap::from_bytes(vol_bits, n)),
        dict: None,
    };
    Data::new(
        Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", DataType::TimestampUs),
            FieldSchema::new("price", DataType::Float64),
            FieldSchema::new("volume", DataType::Int64),
        ]),
        vec![sym_col, time_col, price_col, volume_col],
    )
    .unwrap()
}

fn vol_validity_bytes(n: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; n.div_ceil(8)];
    for i in 0..n {
        if i % 97 != 0 {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    bytes
}

#[test]
fn write_dataset_parallel_large_matches_expected() {
    let n = 100_000; // 2 字段 × 8B × 100K = 1.5 MiB ≥ WRITE_PARALLEL_MIN_BYTES（1 MiB）
    let dir = temp_dir("write_par");
    let root = dir.join("ds");
    create_dataset_data(&root, large_two_field_data(n), splayed_core::CreateDatasetOptions::default())
        .unwrap();
    let ds = open_dataset(&root, Mode::Write).unwrap();
    ds.set_max_parallelism(4); // 强制走并行分支

    // 全量覆盖：price 翻负、volume 全 42（沿用原 validity 位型，NULL 位置不变）
    let price_col = Column {
        data_type: DataType::Float64,
        values: Buffer::from_slice_copy(
            &(0..n).map(|i| -(i as f64) * 0.25).collect::<Vec<f64>>(),
        ),
        validity: None,
        dict: None,
    };
    let volume_col = Column {
        data_type: DataType::Int64,
        values: Buffer::from_slice_copy(&vec![42i64; n]),
        validity: Some(Bitmap::from_bytes(vol_validity_bytes(n), n)),
        dict: None,
    };
    let patch = splayed_format::DataView::new(
        Schema::new(vec![
            FieldSchema::new("price", DataType::Float64),
            FieldSchema::new("volume", DataType::Int64),
        ]),
        vec![price_col.as_view(), volume_col.as_view()],
    )
    .unwrap();
    ds.write_dataset(0, &patch).unwrap();

    // 重复列名显式拒绝（并行分桶依赖字段唯一）
    let dup = splayed_format::DataView::new(
        Schema::new(vec![
            FieldSchema::new("price", DataType::Float64),
            FieldSchema::new("price", DataType::Float64),
        ]),
        vec![price_col.as_view(), price_col.as_view()],
    )
    .unwrap();
    assert!(matches!(
        ds.write_dataset(0, &dup),
        Err(CoreError::Invalid(_))
    ));

    ds.close_dataset().unwrap();

    // 重开验证：两字段均被并行写入正确值，NULL 数不变，sym/time 未受影响
    let ds = open_dataset(&root, Mode::Read).unwrap();
    let view = ds.read_dataset(0, n as u64, Some(&["volume", "price"])).unwrap();
    assert_eq!(
        view.schema.fields.iter().map(|f| f.name.as_ref()).collect::<Vec<_>>(),
        vec!["sym", "time", "volume", "price"] // 请求序组装
    );
    let prices: Vec<f64> = view
        .column("price")
        .unwrap()
        .segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, f64>(s.fixed_bytes().unwrap()).to_vec())
        .collect();
    assert_eq!(prices[0], 0.0);
    assert_eq!(prices[n / 2], -(n as f64 / 2.0) * 0.25);
    assert_eq!(prices[n - 1], -((n - 1) as f64) * 0.25);
    let vol = view.column("volume").unwrap();
    assert_eq!(vol.null_count(), (0..n).step_by(97).count());
    let volumes: Vec<i64> = vol
        .segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, i64>(s.fixed_bytes().unwrap()).to_vec())
        .collect();
    assert!(volumes.iter().all(|&v| v == 42));
    ds.close_dataset().unwrap();
    cleanup(&dir);
}

#[test]
fn scan_dataset_parallel_multi_field_matches_brute_force() {
    let n = 128_000; // ≥ SCAN_PARALLEL_MIN_ROWS（64K），双字段谓词 → 并行扫描分支
    let dir = temp_dir("scan_par");
    let root = dir.join("ds");
    create_dataset_data(&root, large_two_field_data(n), splayed_core::CreateDatasetOptions::default())
        .unwrap();
    let ds = open_dataset(&root, Mode::Read).unwrap();
    ds.set_max_parallelism(4);

    let pred = Predicate::And(vec![
        Predicate::cmp("price", CmpOp::Gt, Scalar::Float(500.0)),
        Predicate::cmp("volume", CmpOp::Lt, Scalar::Int(3)),
    ]);
    let mut scanner = ds
        .scan_dataset(&ScanRequest { ranges: vec![], projection: vec![], predicate: Some(pred), limit: None })
        .unwrap();
    let mut got = Vec::new();
    while let Some(r) = scanner.next().unwrap() {
        got.push((r.offset, r.length));
    }
    scanner.close().unwrap();

    // 暴力期望：price > 500 ∧ volume 有效且 < 3（NULL 不命中）
    let mut hits = Vec::new();
    for i in 0..n {
        let price_ok = ((i.wrapping_mul(2654435761)) % 1000) as f64 > 500.0;
        let vol_ok = i % 97 != 0 && (i % 7) < 3;
        if price_ok && vol_ok {
            hits.push(i as u64);
        }
    }
    // 有序不重叠 + 行集精确一致
    assert!(got.windows(2).all(|w| w[0].0 + w[0].1 <= w[1].0));
    let got_rows: Vec<u64> = got.iter().flat_map(|&(o, l)| o..o + l).collect();
    assert_eq!(got_rows, hits);

    // 单字段谓词保持串行快路径语义不变
    let pred = Predicate::cmp("volume", CmpOp::Lt, Scalar::Int(2));
    let mut scanner = ds
        .scan_dataset(&ScanRequest { ranges: vec![], projection: vec![], predicate: Some(pred), limit: None })
        .unwrap();
    let mut rows = 0u64;
    while let Some(r) = scanner.next().unwrap() {
        rows += r.length;
    }
    scanner.close().unwrap();
    assert_eq!(rows, (0..n).filter(|&i| i % 97 != 0 && i % 7 < 2).count() as u64);

    ds.close_dataset().unwrap();
    cleanup(&dir);
}

#[test]
fn read_dataset_projection_request_order_and_validation() {
    let dir = temp_dir("read_order");
    let root = dir.join("ds");
    create_dataset_data(&root, two_field_data(), splayed_core::CreateDatasetOptions::default()).unwrap();
    let ds = open_dataset(&root, Mode::Read).unwrap();

    // 输出按 projection 请求序（schema 序为 price, volume）
    let view = ds.read_dataset(0, 4, Some(&["volume", "price"])).unwrap();
    assert_eq!(
        view.schema.fields.iter().map(|f| f.name.as_ref()).collect::<Vec<_>>(),
        vec!["sym", "time", "volume", "price"]
    );
    // 保留名 / 重复请求被跳过去重，sym/time 恒在最前
    let view = ds.read_dataset(0, 4, Some(&["price", "sym", "price"])).unwrap();
    assert_eq!(
        view.schema.fields.iter().map(|f| f.name.as_ref()).collect::<Vec<_>>(),
        vec!["sym", "time", "price"]
    );
    assert!(matches!(
        ds.read_dataset(0, 4, Some(&["nope"])),
        Err(CoreError::Invalid(_))
    ));
    ds.close_dataset().unwrap();
    cleanup(&dir);
}

/// 创建即压缩贯通 Dataset 层：compression + chunk_syms → 字段以 sym 对齐 chunk
/// 直接创建（is_chunked），读回值不变。
#[test]
fn create_dataset_with_compression() {
    let dir = temp_dir("ds_compress");
    let root = dir.join("ds");
    create_dataset_data(
        &root,
        two_field_data(),
        splayed_core::CreateDatasetOptions {
            compression: splayed_format::Compression::Zstd,
            chunk_target_rows: 1,
            ..Default::default()
        },
    )
    .unwrap();

    // 字段文件为 chunked 物理表示（sym 对齐：2 sym → 2 chunk）
    let fh = open_field_file(&root.join("price"), Mode::Read).unwrap();
    assert!(fh.is_chunked());
    close_field_handle(fh).unwrap();

    // 读回值一致
    let ds = open_dataset(&root, Mode::Read).unwrap();
    let view = ds.read_dataset(0, 4, Some(&["price"])).unwrap();
    let prices: Vec<f64> = view
        .column("price")
        .unwrap()
        .segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, f64>(s.fixed_bytes().unwrap()).to_vec())
        .collect();
    assert_eq!(prices, vec![10.0, 11.0, 20.0, 21.0]);
    ds.close_dataset().unwrap();
    cleanup(&dir);
}

/// sym IN（Or 组合）下推 META 扫描：只命中目标 sym 的行（R3/R4 场景的过滤语义）。
#[test]
fn scan_dataset_symbol_in_or_pushdown() {
    let dir = temp_dir("sym_in");
    let root = dir.join("ds");
    let data = two_field_data(); // A@1,A@2,B@1,B@2
    create_dataset_data(&root, data, splayed_core::CreateDatasetOptions::default()).unwrap();
    let ds = open_dataset(&root, Mode::Read).unwrap();

    // sym IN ("B") —— 单元素 Or 等价 IN
    let pred = Predicate::Or(vec![Predicate::cmp("sym", CmpOp::Eq, Scalar::Str("B".into()))]);
    let mut scanner = ds
        .scan_dataset(&ScanRequest { ranges: vec![], projection: vec![], predicate: Some(pred), limit: None })
        .unwrap();
    let mut rows = Vec::new();
    while let Some(r) = scanner.next().unwrap() {
        for i in 0..r.length {
            rows.push(r.offset + i);
        }
    }
    scanner.close().unwrap();
    assert_eq!(rows, vec![2, 3]); // B 的行（输入序 A@1,A@2,B@1,B@2）

    // sym IN ("A","B") 之外再叠加 time 条件：And(Or(sym), time Ge)
    let pred = Predicate::And(vec![
        Predicate::Or(vec![
            Predicate::cmp("sym", CmpOp::Eq, Scalar::Str("A".into())),
            Predicate::cmp("sym", CmpOp::Eq, Scalar::Str("B".into())),
        ]),
        Predicate::cmp("time", CmpOp::Ge, Scalar::Int(2)),
    ]);
    let mut scanner = ds
        .scan_dataset(&ScanRequest { ranges: vec![], projection: vec![], predicate: Some(pred), limit: None })
        .unwrap();
    let mut rows = Vec::new();
    while let Some(r) = scanner.next().unwrap() {
        for i in 0..r.length {
            rows.push(r.offset + i);
        }
    }
    scanner.close().unwrap();
    // A@2 与 B@2（time >= 2 的行）
    assert_eq!(rows, vec![1, 3]);
    ds.close_dataset().unwrap();
    cleanup(&dir);
}

#[test]
fn dataset_read_write_boundaries_and_zero_length() {
    let dir = temp_dir("boundaries");
    let root = dir.join("ds");
    create_dataset_data(&root, sample_data(), splayed_core::CreateDatasetOptions::default()).unwrap();
    let ds = open_dataset(&root, Mode::Write).unwrap();

    // 1. 读取 length = 0：应成功返回空 DataView，且 schema 保持 (None 只读 sym, time)
    let view_zero = ds.read_dataset(0, 0, None).unwrap();
    assert_eq!(view_zero.length(), 0);
    assert_eq!(view_zero.schema.len(), 2);

    let view_zero_proj = ds.read_dataset(3, 0, Some(&["price"])).unwrap();
    assert_eq!(view_zero_proj.length(), 0);
    assert_eq!(view_zero_proj.schema.len(), 3);

    // 2. 越界读取检查
    assert!(ds.read_dataset(0, 9, None).is_err());
    assert!(ds.read_dataset(8, 1, None).is_err());
    assert!(ds.read_dataset(u64::MAX, 1, None).is_err());

    // 3. 包含保留名字段作为 projection（sym, time 被内部识别并安全处理）
    let view_res = ds.read_dataset(2, 3, Some(&["sym", "time", "price"])).unwrap();
    assert_eq!(view_res.length(), 3);
    assert_eq!(view_res.column("sym").unwrap().length(), 3);
    assert_eq!(view_res.column("time").unwrap().length(), 3);
    assert_eq!(view_res.column("price").unwrap().length(), 3);

    // 4. 重复指定列名投影（去重返回）
    let view_dup = ds.read_dataset(0, 2, Some(&["price", "price"])).unwrap();
    assert_eq!(view_dup.schema.len(), 3); // sym, time, price（不重复出现 price）

    // 5. 写入越界检查
    let patch_col = Column {
        data_type: DataType::Float64,
        values: Buffer::from_slice_copy(&[99.0f64, 100.0f64]),
        validity: None,
        dict: None,
    };
    let patch_data = Data::new(
        Schema::new(vec![FieldSchema::new("price", DataType::Float64)]),
        vec![patch_col],
    )
    .unwrap();
    // offset 7 写入 2 行（7+2 = 9 > 8），应报错
    assert!(ds.write_dataset(7, &patch_data.as_view()).is_err());

    // 正好写入末尾：offset 6 写入 2 行（6+2 = 8 == 8），应成功
    ds.write_dataset(6, &patch_data.as_view()).unwrap();
    let read_tail = ds.read_dataset(6, 2, Some(&["price"])).unwrap();
    assert_eq!(
        bytemuck::cast_slice::<u8, f64>(read_tail.column("price").unwrap().segments()[0].fixed_bytes().unwrap()),
        &[99.0, 100.0]
    );

    ds.close_dataset().unwrap();
    cleanup(&dir);
}

#[test]
fn dataset_skeleton_create_and_schema_read() {
    let dir = temp_dir("ds_skeleton");
    let root = dir.join("ds");
    let schema = Schema::new(vec![
        FieldSchema::new("sym", DataType::Utf8),
        FieldSchema::new("time", DataType::TimestampUs),
        FieldSchema::new("price", DataType::Float64),
        FieldSchema::new("factor.ret20", DataType::Float32),
    ]);
    let ds = create_dataset(&root, &schema).unwrap();
    assert_eq!(ds.read_dataset_schema().len(), 4);
    assert!(root.join(".meta").exists());
    assert_eq!(std::fs::metadata(root.join("price")).unwrap().len(), 64);
    assert_eq!(std::fs::metadata(root.join("factor/ret20")).unwrap().len(), 64);
    ds.close_dataset().unwrap();
    cleanup(&dir);
}
