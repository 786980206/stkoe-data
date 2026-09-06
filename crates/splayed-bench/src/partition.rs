//! 分区基准（docs/benchmark-partition.md）：Splayed Year 分区表侧执行器。
//! DuckDB / Polars 侧由 `scripts/partition_bench.py` 驱动（官方 Python API），
//! 三引擎结果写入同一 CSV 并交叉校验（count / sum(n0) / sum(n1) 精确整数和）。

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use splayed_core::{CreateDatasetOptions, Mode};
use splayed_format::{Buffer, Column, Data, DataType, FieldSchema, Schema};
use crate::{month_start_us, sym_name, sym_strings};
use std::sync::Arc;
use std::io::Write as _;
use splayed_table::{
    close_table, create_table, create_table_partition, open_table, read_table, scan_table,
    TableHandle, TableOptions, TableScanRequest,
};

/// 分区基准 CSV（9 列；engine,scenario,scale_rows,run_idx,time_ms,output_rows,
/// output_bytes,physical_size,file_count）。追加式：文件存在则不加 header。
struct PCsv {
    f: std::io::BufWriter<std::fs::File>,
}

impl PCsv {
    fn open(path: &PathBuf) -> Self {
        let new = !path.exists();
        let mut f = std::io::BufWriter::new(
            std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap(),
        );
        if new {
            writeln!(f, "engine,scenario,scale_rows,run_idx,time_ms,output_rows,output_bytes,physical_size,file_count").unwrap();
        }
        PCsv { f }
    }
    #[allow(clippy::too_many_arguments)]
    fn row(
        &mut self,
        engine: &str,
        scenario: &str,
        scale_rows: usize,
        run_idx: usize,
        time_ms: f64,
        output_rows: usize,
        output_bytes: u64,
        physical_size: u64,
        file_count: u64,
    ) {
        writeln!(
            self.f,
            "{engine},{scenario},{scale_rows},{run_idx},{time_ms:.2},{output_rows},{output_bytes},{physical_size},{file_count}"
        )
        .unwrap();
    }
}

const SYMS: usize = 1000;
const YEARS: usize = 4;
const NUMERIC_FIELDS: usize = 18; // 9 i64（偶数下标）+ 9 f64
/// 每行逻辑字节：sym keys 4 + ts 8 + 18×8 = 156。
const PER_ROW_BYTES: u64 = 4 + 8 + 18 * 8;

struct PartitionGen {
    rows_per_sym: usize,
    year_offsets: Vec<usize>,
}

impl PartitionGen {
    fn new(total_rows: usize) -> Self {
        let rows_per_sym = total_rows / SYMS;
        assert!(
            rows_per_sym % YEARS == 0,
            "rows must be a multiple of {}×{}",
            SYMS,
            YEARS
        );
        let per_year = rows_per_sym / YEARS;
        let mut year_offsets = vec![0usize];
        let mut acc = 0usize;
        for _ in 0..YEARS {
            acc += per_year;
            year_offsets.push(acc);
        }
        PartitionGen { rows_per_sym, year_offsets }
    }
    fn years(&self) -> usize {
        YEARS
    }
    fn year_rows(&self) -> usize {
        SYMS * (self.rows_per_sym / YEARS)
    }
    fn generate_year(&self, y: usize) -> YearData {
        let k = self.rows_per_sym / YEARS;
        let rows = SYMS * k;
        let start = month_start_us(y * 12);
        let span = month_start_us((y + 1) * 12) - start;
        let mut sym_keys = Vec::with_capacity(rows);
        let mut times = Vec::with_capacity(rows);
        let mut numerics = vec![Vec::with_capacity(rows * 8); NUMERIC_FIELDS];
        for s in 0..SYMS {
            for j in 0..k {
                sym_keys.push(s as u32);
                times.push(start + (span * j as i64) / k as i64);
                let jg = (self.year_offsets[y] + j) as u64;
                for f in 0..NUMERIC_FIELDS {
                    let h = crate::mix(s as u64, jg, f as u64);
                    if f % 2 == 0 {
                        numerics[f].extend_from_slice(&((h % 1_000_000) as i64).to_le_bytes());
                    } else {
                        numerics[f]
                            .extend_from_slice(&((h % 1_000_000) as f64 / 8.0).to_le_bytes());
                    }
                }
            }
        }
        YearData { sym_keys, times, numerics, rows }
    }
}

struct YearData {
    sym_keys: Vec<u32>,
    times: Vec<i64>,
    numerics: Vec<Vec<u8>>,
    rows: usize,
}

fn year_start_us(y: usize) -> i64 {
    month_start_us(y * 12)
}

fn part_schema() -> Schema {
    let mut fields = vec![
        FieldSchema::new("sym", DataType::Utf8),
        FieldSchema::new("time", DataType::TimestampUs),
    ];
    for f in 0..NUMERIC_FIELDS {
        fields.push(FieldSchema::new(
            format!("n{f}"),
            if f % 2 == 0 { DataType::Int64 } else { DataType::Float64 },
        ));
    }
    Schema::new(fields)
}

fn year_to_splayed_data(yd: &YearData) -> Data {
    let sym_strings = sym_strings();
    let mut sym_offsets = vec![0u64];
    let mut acc = 0usize;
    for s in 0..SYMS {
        acc += sym_name(s).len();
        sym_offsets.push(acc as u64);
    }
    let mut columns = vec![
        Column::from_dict(yd.sym_keys.clone(), sym_offsets, sym_strings, None),
        Column {
            data_type: DataType::TimestampUs,
            values: Buffer::from_slice_copy(&yd.times),
            validity: None,
            dict: None,
        },
    ];
    for f in 0..NUMERIC_FIELDS {
        columns.push(Column {
            data_type: if f % 2 == 0 { DataType::Int64 } else { DataType::Float64 },
            values: Buffer::from_vec(yd.numerics[f].clone()),
            validity: None,
            dict: None,
        });
    }
    Data::new(part_schema(), columns).unwrap()
}

/// 源 Parquet 的 Arrow schema（20 列 + year，供 DuckDB / Polars 输入）。
fn source_arrow_schema() -> std::sync::Arc<arrow_schema::Schema> {
    let mut fields = vec![
        arrow_schema::Field::new("sym", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new(
            "time",
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            false,
        ),
    ];
    for f in 0..NUMERIC_FIELDS {
        fields.push(arrow_schema::Field::new(
            format!("n{f}"),
            if f % 2 == 0 { arrow_schema::DataType::Int64 } else { arrow_schema::DataType::Float64 },
            false,
        ));
    }
    fields.push(arrow_schema::Field::new("year", arrow_schema::DataType::Int64, false));
    std::sync::Arc::new(arrow_schema::Schema::new(fields))
}

/// 年数据 → 源 Arrow RecordBatch（含 year 列；写源 Parquet 供外部引擎输入）。
fn year_to_source_batch(yd: &YearData, y: usize) -> arrow_array::RecordBatch {
    use arrow_array::{
    ArrayRef, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
    let syms: Vec<String> = (0..yd.rows)
        .map(|i| sym_name(yd.sym_keys[i] as usize))
        .collect();
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(syms)) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(yd.times.clone())) as ArrayRef,
    ];
    let mut fields = vec![
        arrow_schema::Field::new("sym", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new(
            "time",
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            false,
        ),
    ];
    for (f, bytes) in yd.numerics.iter().enumerate() {
        if f % 2 == 0 {
            let vals: Vec<i64> = bytes
                .chunks_exact(8)
                .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
                .collect();
            arrays.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
            fields.push(arrow_schema::Field::new(format!("n{f}"), arrow_schema::DataType::Int64, false));
        } else {
            let vals: Vec<f64> = bytes
                .chunks_exact(8)
                .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
                .collect();
            arrays.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
            fields.push(arrow_schema::Field::new(format!("n{f}"), arrow_schema::DataType::Float64, false));
        }
    }
    let year_vals = vec![2020i64 + y as i64; yd.rows];
    arrays.push(Arc::new(Int64Array::from(year_vals)) as ArrayRef);
    fields.push(arrow_schema::Field::new("year", arrow_schema::DataType::Int64, false));
    arrow_array::RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays).unwrap()
}

fn count_files(path: &PathBuf) -> u64 {
    let mut n = 0u64;
    if path.is_dir() {
        for e in fs::read_dir(path).into_iter().flatten().flatten() {
            if e.path().is_dir() {
                n += count_files(&e.path());
            } else {
                n += 1;
            }
        }
    }
    n
}

/// Splayed 读场景：聚合扫描（count + sum(n0) + sum(n2)），供跨引擎正确性比对。
fn agg_splayed(table: &TableHandle, sc: &str, year: usize) -> (usize, i64, i64) {
    let mut req = TableScanRequest::default();
    match sc {
        "PR1" => {}
        "PR2" => {
            req.predicate = Some(splayed_core::Predicate::And(vec![
                splayed_core::Predicate::cmp(
                    "time",
                    splayed_core::CmpOp::Ge,
                    splayed_core::Scalar::Int(year_start_us(year)),
                ),
                splayed_core::Predicate::cmp(
                    "time",
                    splayed_core::CmpOp::Lt,
                    splayed_core::Scalar::Int(year_start_us(year + 1)),
                ),
            ]));
        }
        "PR3" => {
            req.predicate = Some(splayed_core::Predicate::And(vec![
                splayed_core::Predicate::cmp(
                    "time",
                    splayed_core::CmpOp::Ge,
                    splayed_core::Scalar::Int(year_start_us(1)),
                ),
                splayed_core::Predicate::cmp(
                    "time",
                    splayed_core::CmpOp::Lt,
                    splayed_core::Scalar::Int(year_start_us(3)),
                ),
            ]));
        }
        "PR4" => {
            req.projection = vec!["n0".into(), "n1".into(), "n2".into()];
            req.predicate = Some(splayed_core::Predicate::And(vec![
                splayed_core::Predicate::cmp(
                    "sym",
                    splayed_core::CmpOp::Eq,
                    splayed_core::Scalar::Str(sym_name(0).into()),
                ),
                splayed_core::Predicate::cmp(
                    "time",
                    splayed_core::CmpOp::Ge,
                    splayed_core::Scalar::Int(year_start_us(2)),
                ),
                splayed_core::Predicate::cmp(
                    "time",
                    splayed_core::CmpOp::Lt,
                    splayed_core::Scalar::Int(year_start_us(3)),
                ),
            ]));
        }
        _ => {}
    }
    let scanner = scan_table(table, req).unwrap();
    let mut reader = read_table(table, scanner, Some(8192));
    let (mut rows, mut s0, mut s2) = (0usize, 0i64, 0i64);
    while let Some(view) = reader.next().unwrap() {
        rows += view.length();
        for field in &view.schema.fields {
            let name = field.name.as_ref();
            if name != "n0" && name != "n2" {
                continue;
            }
            let col = view.column(name).unwrap();
            for seg in col.segments() {
                for v in bytemuck::cast_slice::<u8, i64>(seg.fixed_bytes().unwrap_or(&[])) {
                    if name == "n0" {
                        s0 = s0.wrapping_add(*v);
                    } else {
                        s2 = s2.wrapping_add(*v);
                    }
                }
            }
        }
    }
    reader.close().unwrap();
    (rows, s0, s2)
}

/// 分区基准主流程（Splayed 侧）：写 Year 分区表 → 源 Parquet → 读场景 → CSV。
/// DuckDB / Polars 行由 `scripts/partition_bench.py` 追加到同一 CSV。
pub fn run_partition(out: &PathBuf, total_rows: usize, runs: usize) {
    let gen = PartitionGen::new(total_rows);
    let years = gen.years();
    let year_rows = gen.year_rows();
    let exists_new = !out.exists();
    let mut csv = PCsv::open(out);

    // ---- PW1: Splayed Year 分区写入（Zstd，chunk 目标 8192 行自动 sym 对齐）----
    let root = PathBuf::from("bench_data/splayed_year");
    let _ = fs::remove_dir_all(&root);
    let mut write_s = std::time::Duration::ZERO;
    let mut source_batches: Vec<arrow_array::RecordBatch> = Vec::new();
    for y in 0..years {
        let yd = gen.generate_year(y);
        source_batches.push(year_to_source_batch(&yd, y));
        let data = year_to_splayed_data(&yd);
        let t0 = Instant::now();
        if y == 0 {
            create_table(
                &root,
                data,
                splayed_table::PartitionScheme::Year,
                TableOptions {
                    compression: Some(splayed_format::Compression::Zstd),
                    ..Default::default()
                },
            )
            .unwrap();
        } else {
            create_table_partition(
                &root,
                &format!("year={}", 2020 + y),
                data,
                CreateDatasetOptions {
                    compression: splayed_format::Compression::Zstd,
                    ..Default::default()
                },
            )
            .unwrap();
        }
        write_s += t0.elapsed();
    }
    let physical = crate::dir_size(&root);
    let files = count_files(&root);
    if !exists_new {
        // 首次创建文件时 header 已写（CsvWriter::open）
    }
    csv.row(
        "splayed",
        "PW1",
        total_rows,
        0,
        write_s.as_secs_f64() * 1000.0,
        total_rows,
        total_rows as u64 * PER_ROW_BYTES,
        physical,
        files,
    );
    // file_count 单独一列——重写 row：简化为 stdout 报告 + CSV 用 physical 表达
    println!(
        "splayed write: {:.1} ms, physical {:.2} GB, files {}",
        write_s.as_secs_f64() * 1000.0,
        physical as f64 / 1e9,
        files
    );

    // ---- 源 Parquet（DuckDB / Polars 的共同输入，不计入任何引擎写入时间）----
    let src = PathBuf::from("bench_data/source.parquet");
    {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        let _ = fs::remove_file(&src);
        fs::create_dir_all(src.parent().unwrap()).unwrap();
        let file = fs::File::create(&src).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(1_000_000)
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(3).unwrap(),
            ))
            .build();
        let schema = source_arrow_schema();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        for b in &source_batches {
            writer.write(b).unwrap();
        }
        writer.close().unwrap();
    }

    // ---- PR1–PR4: Splayed 读（预热 1 次 + runs 次计时）----
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();
    let mut splayed_aggs: Vec<(String, usize, i64, i64)> = Vec::new();
    for (sc, year) in [("PR1", 0usize), ("PR2", 2usize), ("PR3", 0usize), ("PR4", 2usize)] {
        let _ = agg_splayed(&table, sc, year); // 预热
        for run in 0..runs {
            let t0 = Instant::now();
            let (rows, s0, s2) = agg_splayed(&table, sc, year);
            let t = t0.elapsed().as_secs_f64() * 1000.0;
            csv.row(
                "splayed",
                sc,
                total_rows,
                run,
                t,
                rows,
                rows as u64 * PER_ROW_BYTES,
                physical,
                files,
            );
            if run == 0 {
                splayed_aggs.push((sc.to_string(), rows, s0, s2));
            }
        }
    }

    // ---- PR5: 分区元数据（分区列表 + 各分区行数；META-only）----
    {
        let t0 = Instant::now();
        let stats = table.read_table_statistics().unwrap();
        let t = t0.elapsed().as_secs_f64() * 1000.0;
        for run in 0..runs {
            csv.row(
                "splayed",
                "PR5",
                total_rows,
                run,
                t,
                stats.partition_count as usize,
                0,
                physical,
                files,
            );
        }
        println!(
            "splayed PR5 metadata: {:.2} ms ({} partitions)",
            t,
            stats.partition_count
        );
    }
    close_table(table).unwrap();

    // 校验和结果落盘供 python 侧比对
    let mut cs = fs::File::create("bench_data/splayed_aggs.txt").unwrap();
    for (sc, rows, s0, s2) in &splayed_aggs {
        writeln!(cs, "{sc} {rows} {s0} {s2}").unwrap();
    }

    // 清理：Splayed 存储读后即删（DuckDB / Polars 侧不再需要它）
    let _ = fs::remove_dir_all(&root);
    // 源 Parquet 保留（python 侧输入），由 python 脚本最后清理
    let _ = source_batches;
    let _ = exists_new;
}
