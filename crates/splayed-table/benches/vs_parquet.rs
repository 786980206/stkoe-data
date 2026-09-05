//! V2.0 Benchmark：splayed（Table 层端到端）vs Parquet（arrow-rs parquet crate）。
//!
//! 数据模型对齐设计文档 §13 的思路：SYM × TIME × FIELD 金融时序列。
//! 三条路径：写入（全量物化落盘）、点读（按行范围读回）、谓词扫描（定位+过滤）。
//!
//! 运行：`cargo bench -p splayed-table`

use std::path::PathBuf;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use splayed_format::{Buffer, Column, Data, DataType, FieldSchema, Schema};
use splayed_table::{
    create_table, open_table, query_table, TableOptions, TableScanRequest,
};

const SYMS: usize = 256;
const ROWS_PER_SYM: usize = 250; // 容量网格：每 sym 的行容量
const TOTAL: usize = SYMS * ROWS_PER_SYM;

fn temp_root(tag: &str) -> PathBuf {
    // criterion 多次迭代：根目录由迭代内自建自删，这里只分配唯一前缀
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "splayed_bench_{}_{}_{}",
        tag,
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// 构造 (sym, time, price, volume) 数据：sym ASC, time ASC。
/// time = Date32 天序号；数据分布有局部性（便于编码/压缩有实际意义）。
fn bench_data() -> Data {
    let mut dict: Vec<String> = (0..SYMS).map(|i| format!("SYM{:04}", i)).collect();
    dict.sort();
    let mut offsets = vec![0u64];
    let mut strings = Vec::new();
    for s in &dict {
        strings.extend_from_slice(s.as_bytes());
        offsets.push(strings.len() as u64);
    }
    let mut sym_col_keys: Vec<u32> = Vec::with_capacity(TOTAL);
    let mut times: Vec<i32> = Vec::with_capacity(TOTAL);
    let mut prices: Vec<f64> = Vec::with_capacity(TOTAL);
    let mut volumes: Vec<i64> = Vec::with_capacity(TOTAL);
    for i in 0..SYMS {
        let key = dict.iter().position(|s| *s == format!("SYM{:04}", i)).unwrap() as u32;
        for j in 0..ROWS_PER_SYM {
            sym_col_keys.push(key);
            times.push((18_000 + j) as i32);
            // 价格：围绕基准的窄幅波动（现实分布）
            prices.push(100.0 + ((i * 7 + j * 13) % 97) as f64 * 0.1);
            volumes.push(1000 + ((i * 3 + j) % 501) as i64);
        }
    }
    let sym_col = Column::from_dict(sym_col_keys, offsets, strings, None);
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
            Column { data_type: DataType::Int64, values: Buffer::from_slice_copy(&volumes), validity: None, dict: None },
        ],
    )
    .unwrap()
}

/// Parquet 对照：相同的 4 列写入 Arrow RecordBatch → parquet 文件。
fn parquet_roundtrip_setup(root: &PathBuf, data: &Data) {
    std::fs::create_dir_all(root).unwrap();
    use arrow_array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray, RecordBatch};
    use arrow_schema::{DataType as ArrowDt, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    let rows = data.length();
    let mut syms = Vec::with_capacity(rows);
    let mut times = Vec::with_capacity(rows);
    let mut prices = Vec::with_capacity(rows);
    let mut volumes = Vec::with_capacity(rows);
    for i in 0..rows {
        syms.push(data.column("sym").unwrap().as_view().string_at(i).unwrap().to_owned());
        times.push(i32::from_le_bytes(
            data.column("time").unwrap().values.as_slice()[i * 4..i * 4 + 4].try_into().unwrap(),
        ));
        prices.push(f64::from_le_bytes(
            data.column("price").unwrap().values.as_slice()[i * 8..i * 8 + 8].try_into().unwrap(),
        ));
        volumes.push(i64::from_le_bytes(
            data.column("volume").unwrap().values.as_slice()[i * 8..i * 8 + 8].try_into().unwrap(),
        ));
    }
    let schema = ArrowSchema::new(vec![
        ArrowField::new("sym", ArrowDt::Utf8, false),
        ArrowField::new("time", ArrowDt::Int32, false),
        ArrowField::new("price", ArrowDt::Float64, false),
        ArrowField::new("volume", ArrowDt::Int64, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(syms)) as ArrayRef,
            Arc::new(Int32Array::from(times)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Int64Array::from(volumes)),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(root.join("bench.parquet")).unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn bench_write(c: &mut Criterion) {
    let data = bench_data();
    let mut group = c.benchmark_group("write");
    group.throughput(criterion::Throughput::Elements(TOTAL as u64));

    group.bench_function("splayed_table_month", |b| {
        b.iter(|| {
            let root = temp_root("w_splayed");
            create_table(&root, &data, splayed_table::PartitionScheme::Month).unwrap();
            black_box(&root);
            let _ = std::fs::remove_dir_all(&root);
        })
    });

    group.bench_function("parquet", |b| {
        b.iter(|| {
            let root = temp_root("w_parquet");
            parquet_roundtrip_setup(&root, &data);
            black_box(&root);
            let _ = std::fs::remove_dir_all(&root);
        })
    });
    group.finish();
}

fn bench_read_scan(c: &mut Criterion) {
    let data = bench_data();
    // 预建 splayed 表与 parquet 文件
    let splayed_root = temp_root("r_splayed");
    create_table(&splayed_root, &data, splayed_table::PartitionScheme::Month).unwrap();
    parquet_roundtrip_setup(&splayed_root, &data);

    let mut group = c.benchmark_group("read");
    group.throughput(criterion::Throughput::Elements(TOTAL as u64));

    // 全表读取（Table 层端到端 → DataView）；open_table 在循环外
    {
        let table = open_table(&splayed_root, splayed_core::Mode::Read, TableOptions::default()).unwrap();
        group.bench_function("splayed_table_full_scan", |b| {
            b.iter(|| {
                let req = TableScanRequest::default();
                let mut reader = query_table(&table, req, Some(4096)).unwrap();
                let mut rows = 0usize;
                while let Some(view) = reader.next().unwrap() {
                    rows += view.length();
                    black_box(view.column("price").unwrap().length());
                }
                reader.close().unwrap();
                assert_eq!(rows, TOTAL);
            })
        });
        drop(table);
    }

    // 谓词扫描：price > 15（行级过滤，覆盖全部数据集）；open_table 在循环外
    {
        let table = open_table(&splayed_root, splayed_core::Mode::Read, TableOptions::default()).unwrap();
        group.bench_function("splayed_table_predicated_scan", |b| {
            b.iter(|| {
            let req = TableScanRequest {
                predicate: Some(splayed_core::Predicate::cmp(
                    "price",
                    splayed_core::CmpOp::Gt,
                    splayed_core::Scalar::Float(15.0),
                )),
                ..Default::default()
            };
            let mut reader = query_table(&table, req, Some(4096)).unwrap();
            let mut rows = 0usize;
            while let Some(view) = reader.next().unwrap() {
                rows += view.length();
            }
            reader.close().unwrap();
            black_box(rows);
            })
        });
        drop(table);
    }

    // parquet 全表读取（对照）
    group.bench_function("parquet_full_scan", |b| {
        b.iter(|| {
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            let file = std::fs::File::open(splayed_root.join("bench.parquet")).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let reader = builder.build().unwrap();
            let mut rows = 0usize;
            for batch in reader {
                rows += batch.unwrap().num_rows();
            }
            assert_eq!(rows, TOTAL);
        })
    });

    // 谓词扫描对照：parquet 谓词下推（row group statistics + 行级 filter）
    group.bench_function("parquet_predicated_scan", |b| {
        b.iter(|| {
            use arrow_array::{Float64Array, RecordBatch};
            use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
            use parquet::arrow::ProjectionMask;
            let file = std::fs::File::open(splayed_root.join("bench.parquet")).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let price_idx = builder.schema().fields().iter().position(|f| f.name() == "price").unwrap();
            let mask = ProjectionMask::roots(builder.parquet_schema(), [price_idx]);
            let pred = ArrowPredicateFn::new(mask, |batch: RecordBatch| {
                let arr = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .clone();
                let mask = arr
                    .values()
                    .iter()
                    .map(|&v| v > 15.0)
                    .collect::<Vec<bool>>();
                let out = arrow_array::BooleanArray::from(mask);
                Ok(out)
            });
            let reader = builder
                .with_row_filter(RowFilter::new(vec![Box::new(pred)]))
                .build()
                .unwrap();
            let mut rows = 0usize;
            for batch in reader {
                rows += batch.unwrap().num_rows();
            }
            black_box(rows);
        })
    });

    group.finish();
    let _ = std::fs::remove_dir_all(&splayed_root);
}

criterion_group!(benches, bench_write, bench_read_scan);
criterion_main!(benches);
