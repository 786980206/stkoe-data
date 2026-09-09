//! splayed-arrow 集成测试：类型映射、NULL 保留、字典列、端到端 Table → Arrow。

use splayed_arrow::{
    column_to_array, data_to_record_batch, data_view_to_record_batch, from_arrow_type,
    read_table_as_arrow, record_batch_to_data, to_arrow_type,
};
use splayed_core::Mode;
use arrow_array::Array as _;
use std::path::Path;
use splayed_core::{open_dataset as open_dataset_inner};
use splayed_format::{Bitmap, Buffer, Column, Data, DataType, FieldSchema, Schema};

fn open_dataset(root: &Path, mode: splayed_core::Mode) -> splayed_core::Result<splayed_core::DatasetHandle> {
    open_dataset_inner(root, mode)
}
fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}
use splayed_table::{
    create_table_data, open_table, scan_table, TableOptions, TableScanRequest,
};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("splayed_arrow_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn sample_data() -> Data {
    // sym 字典列 + 定宽列（含 NULL）
    let mut bits = Bitmap::ones(4);
    bits.set(2, false); // price 第 3 行 NULL
    let sym_col = Column::from_dict(
        vec![0, 0, 1, 1],
        vec![0, 4, 8],
        b"AAPLMSFT".to_vec(),
        None,
    );
    let time_col = Column {
        data_type: DataType::Date32,
        values: Buffer::from_slice_copy(&[100i32, 101, 100, 101]),
        validity: None,
        dict: None,
    };
    let price_col = Column {
        data_type: DataType::Float64,
        values: Buffer::from_slice_copy(&[10.0f64, 11.0, 0.0, 21.0]),
        validity: Some(bits),
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
fn type_mapping_roundtrip() {
    // 正向：全部定宽类型 + Utf8
    for (dt, expected) in [
        (DataType::Bool, arrow_schema::DataType::Boolean),
        (DataType::Int64, arrow_schema::DataType::Int64),
        (DataType::UInt8, arrow_schema::DataType::UInt8),
        (DataType::Float64, arrow_schema::DataType::Float64),
        (DataType::Date32, arrow_schema::DataType::Date32),
        (
            DataType::TimestampUs,
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        ),
        (
            DataType::Utf8,
            arrow_schema::DataType::Dictionary(
                Box::new(arrow_schema::DataType::Int32),
                Box::new(arrow_schema::DataType::Utf8),
            ),
        ),
    ] {
        assert_eq!(to_arrow_type(dt), expected);
        assert_eq!(from_arrow_type(&expected).unwrap(), dt);
    }
    // 不支持的 Arrow 类型 → Error
    assert!(from_arrow_type(&arrow_schema::DataType::Binary).is_err());
}

#[test]
fn data_to_arrow_preserves_values_and_nulls() {
    let data = sample_data();
    let batch = data_to_record_batch(&data).unwrap();
    assert_eq!(batch.num_rows(), 4);
    assert_eq!(batch.num_columns(), 3);

    // NULL：price 第 3 行（index 2）为 null
    let price = batch
        .column(2)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    assert!(price.is_null(2));
    assert_eq!(price.value(0), 10.0);

    // 字典列：sym 值保持
    let sym = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>()
        .unwrap();
    let keys = sym.keys();
    assert!(!keys.is_null(0));
    assert_eq!(keys.value(0), keys.value(1)); // AAPL AAPL

    // 反向：RecordBatch → Data → 值一致
    let back = record_batch_to_data(&batch).unwrap();
    assert_eq!(back.length(), 4);
    assert_eq!(back.schema.data_type_of("price"), Some(DataType::Float64));
    let price_back = back.column("price").unwrap();
    assert_eq!(price_back.null_count(), 1);
    assert!(!price_back.validity.as_ref().unwrap().is_valid(2));
    assert_eq!(
        f64::from_le_bytes(price_back.values.as_slice()[0..8].try_into().unwrap()),
        10.0
    );
}

#[test]
fn multi_segment_view_to_batch() {
    // 同一列的两个段（模拟跨 chunk / 跨分区拼接）
    let a = Buffer::from_vec(bytemuck::cast_slice::<f64, u8>(&[1.0, 2.0]).to_vec());
    let b = Buffer::from_vec(bytemuck::cast_slice::<f64, u8>(&[3.0]).to_vec());
    let view = splayed_format::ColumnView::new(
        DataType::Float64,
        vec![
            splayed_format::ColumnSegment::new(
                DataType::Float64,
                splayed_format::BufferView::from_buffer(&a),
                None,
                2,
            )
            .unwrap(),
            splayed_format::ColumnSegment::new(
                DataType::Float64,
                splayed_format::BufferView::from_buffer(&b),
                None,
                1,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let arr = splayed_arrow::column_view_to_array(&view).unwrap();
    let arr = arr
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    assert_eq!(arr.values(), &[1.0, 2.0, 3.0]);
    let _ = data_view_to_record_batch; // API 存在性
}

#[test]
fn table_scan_to_arrow_end_to_end() {
    let dir = temp_dir("e2e");
    let root = dir.join("tbl");
    let data = sample_data();
    create_table_data(&root, data, splayed_table::PartitionScheme::None, splayed_table::TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();
    // 流式 Reader：逐批转换，不物化整个结果集
    let scanner = scan_table(&table, TableScanRequest::default()).unwrap();
    let mut reader = read_table_as_arrow(&table, scanner, Some(2));
    let mut batches = Vec::new();
    while let Some(b) = reader.next().unwrap() {
        batches.push(b);
    }
    reader.close().unwrap();
    assert!(!batches.is_empty());
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4);

    // 经 parquet 写读对照（能力对齐验证：Arrow 生态互通）
    let file = std::fs::File::create(dir.join("out.parquet")).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        file,
        batches[0].schema(),
        None,
    )
    .unwrap();
    for b in &batches {
        writer.write(b).unwrap();
    }
    writer.close().unwrap();
    let file = std::fs::File::open(dir.join("out.parquet")).unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let total: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(total, 4);

    // core 读回验证
    let ds = open_dataset(&root, Mode::Read).unwrap();
    assert_eq!(ds.read_dataset_statistics().unwrap().row_count, 4);
    ds.close_dataset().unwrap();
    cleanup(&dir);
}

#[test]
fn multi_segment_view_to_batch_and_table_e2e() {
    // 多段（跨分区 batch 聚合）→ 单 Arrow 批
    let dir = temp_dir("e2e_multi");
    let root = dir.join("tbl");
    create_table_data(&root, sample_data(), splayed_table::PartitionScheme::None, splayed_table::TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();
    let scanner = scan_table(&table, TableScanRequest::default()).unwrap();
    let mut reader = read_table_as_arrow(&table, scanner, Some(2));
    let mut batches = Vec::new();
    while let Some(b) = reader.next().unwrap() {
        batches.push(b);
    }
    reader.close().unwrap();
    // 单 Dataset 全表扫描 = 单一连续 range；batch_size=2 精确切分（截断头 + pending
    // 剩余 range），每批恰好 2 行、不超发
    assert_eq!(batches.len(), 2);
    assert!(batches.iter().all(|b| b.num_rows() == 2));
    for b in &batches {
        assert_eq!(b.num_columns(), 3);
    }
    drop(table);
    cleanup(&dir);
}

#[test]
fn column_to_array_direct() {
    let col = Column::zeroed(DataType::TimestampUs, 3, false);
    let arr = column_to_array(&col).unwrap();
    let arr = arr
        .as_any()
        .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr.values(), &[0i64; 3]);
}

/// 字典 key 超出 i32 值域 → checked cast 报错（不是 reinterpret——u32::MAX 无法
/// 表示为 i32，文档 §2 明确语义）。
#[test]
fn dict_key_overflow_is_checked() {
    let col = Column::from_dict(
        vec![0x8000_0000, 0], // 2^31 > i32::MAX
        vec![0, 1, 2],
        b"ab".to_vec(),
        None,
    );
    let err = column_to_array(&col).unwrap_err();
    assert!(matches!(err, splayed_arrow::ArrowConvError::Unsupported(ref m) if m.contains("exceeds i32")));
}

/// 流式 Reader：全局 limit 经 Scanner 传导，总行数不超过 limit。
#[test]
fn arrow_reader_limit_early_termination() {
    let dir = temp_dir("limit");
    let root = dir.join("tbl");
    create_table_data(&root, sample_data(), splayed_table::PartitionScheme::None, splayed_table::TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();
    let req = TableScanRequest { limit: Some(3), ..Default::default() };
    let scanner = scan_table(&table, req).unwrap();
    let mut reader = read_table_as_arrow(&table, scanner, Some(2));
    let mut total = 0usize;
    let mut lens = Vec::new();
    while let Some(b) = reader.next().unwrap() {
        lens.push(b.num_rows());
        total += b.num_rows();
    }
    reader.close().unwrap();
    assert_eq!(total, 3);
    assert!(lens.iter().all(|&l| l <= 2));
    cleanup(&dir);
}

#[test]
fn arrow_empty_data_view_and_batch_roundtrip() {
    // 1. 空 DataView 转 RecordBatch
    let schema = Schema::new(vec![
        FieldSchema::new("id", DataType::Int32),
        FieldSchema::new("val", DataType::Float64),
    ]);
    let col1 = Column::zeroed(DataType::Int32, 0, false);
    let col2 = Column::zeroed(DataType::Float64, 0, false);
    let data_view = splayed_format::DataView::new(schema, vec![col1.as_view(), col2.as_view()]).unwrap();

    let batch = data_view_to_record_batch(&data_view).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 2);

    // 2. 空 RecordBatch 转 Data
    let data_back = record_batch_to_data(&batch).unwrap();
    assert_eq!(data_back.length(), 0);
    assert_eq!(data_back.schema.len(), 2);

    // 3. 全 NULL 数组转 Arrow
    let null_col = Column::zeroed(DataType::Float64, 5, true);
    assert_eq!(null_col.null_count(), 5);
    let arr = column_to_array(&null_col).unwrap();
    assert_eq!(arr.len(), 5);
    assert_eq!(arr.null_count(), 5);
    assert_eq!(arr.logical_null_count(), 5);
}
