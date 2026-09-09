//! Splayed vs Parquet 基准测试执行器（docs/benchmark.md）。
//!
//! 场景：W1（全量写入）+ R1–R6（扫描 / 投影 / 选择性过滤）。
//! 数据：`sym`（1000 字典）+ `time`（i64 µs，2020–2023）+ `description`
//!（5000 短语字典）+ 47 个数值列（24 i64 + 23 f64），按 (sym ASC, time ASC) 排序，
//! 固定种子可重现。Warm-only（Windows 无 drop_caches）。

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType as ArrowDt, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use splayed_core::{CreateDatasetOptions, Mode};
use splayed_format::{Buffer, Column, Data, DataType, FieldSchema, Schema};
use splayed_table::{
    close_table, create_table_partition, init_table, open_table, read_table, scan_table,
    TableHandle, TableOptions, TableScanRequest,
};

const SYMS: usize = 1000;
const DESCRIPTION_PHRASES: usize = 5000;
const NUM_FIELDS: usize = 47; // 24 i64 + 23 f64
const PARQUET_ROW_GROUP: usize = 1_000_000;
const READ_BATCH: usize = 8192;

fn sym_name(s: usize) -> String {
    format!("SYM{s:04}")
}

fn phrase(p: usize) -> String {
    format!("desc{p:05}")
}

fn days_in_month(y: usize, m: usize) -> u64 {
    match m + 1 {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// 月份起点（unix µs）：2020-01 起第 m 个月（2020-01-01 UTC = 1577836800 s）。
fn month_start_us(m: usize) -> i64 {
    let mut days = 0i64;
    let (mut cy, mut cm) = (2020usize, 0usize);
    for _ in 0..m {
        days += days_in_month(cy, cm) as i64;
        cm += 1;
        if cm == 12 {
            cm = 0;
            cy += 1;
        }
    }
    (1577836800 + days * 86_400) * 1_000_000
}

fn month_name(m: usize) -> String {
    format!("month={}-{:02}", 2020 + m / 12, m % 12 + 1)
}

fn month_span_us(m: usize) -> i64 {
    days_in_month(2020 + m / 12, m % 12) as i64 * 86_400 * 1_000_000
}

/// 确定性混淆哈希（splitmix 风格，固定种子可重现）。
fn mix(s: u64, j: u64, f: u64) -> u64 {
    let mut x = s
        .wrapping_mul(0x9E3779B97F4A7C15)
        ^ j.wrapping_mul(0xBF58476D1CE4E5B9)
        ^ f.wrapping_mul(0x94D049BB133111EB);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

/// 每月每个 sym 的行数分布：rows_per_sym 均匀拆到 48 个月（余数给前几个月）。
fn rows_per_month(rows_per_sym: usize) -> Vec<usize> {
    let (mut base, mut rem) = (rows_per_sym / 48, rows_per_sym % 48);
    if rem > 0 {
        base += 1;
        rem = 48 - rem;
        let mut out = vec![base; 48];
        for k in out.iter_mut().rev().take(rem) {
            *k -= 1;
        }
        out
    } else {
        vec![base; 48]
    }
}

struct MonthData {
    sym_keys: Vec<u32>,
    times: Vec<i64>,
    desc_keys: Vec<u32>,
    numerics: Vec<Vec<u8>>,
    rows: usize,
}

fn generate_month(m: usize, rows_per_sym: usize, month_offsets: &[usize]) -> MonthData {
    let k = rows_per_month(rows_per_sym)[m];
    let rows = SYMS * k;
    let start = month_start_us(m);
    let span = month_span_us(m);
    let mut sym_keys = Vec::with_capacity(rows);
    let mut times = Vec::with_capacity(rows);
    let mut desc_keys = Vec::with_capacity(rows);
    let mut numerics: Vec<Vec<u8>> = vec![Vec::with_capacity(rows * 8); NUM_FIELDS];
    for s in 0..SYMS {
        for j in 0..k {
            sym_keys.push(s as u32);
            times.push(start + (span * j as i64) / k as i64);
            let jg = (month_offsets[m] + j) as u64;
            desc_keys.push(((s as u64 * 13 + jg) % DESCRIPTION_PHRASES as u64) as u32);
            for f in 0..NUM_FIELDS {
                let h = mix(s as u64, jg, f as u64);
                if f % 2 == 0 {
                    numerics[f].extend_from_slice(&(h as i64).to_le_bytes());
                } else {
                    numerics[f].extend_from_slice(&((h % 1_000_000) as f64 / 8.0).to_le_bytes());
                }
            }
        }
    }
    MonthData { sym_keys, times, desc_keys, numerics, rows }
}

fn bench_schema() -> Schema {
    let mut fields = vec![
        FieldSchema::new("sym", DataType::Utf8),
        FieldSchema::new("time", DataType::TimestampUs),
        FieldSchema::new("description", DataType::Utf8),
    ];
    for f in 0..NUM_FIELDS {
        fields.push(FieldSchema::new(
            format!("n{f}"),
            if f % 2 == 0 { DataType::Int64 } else { DataType::Float64 },
        ));
    }
    Schema::new(fields)
}

fn sym_strings() -> Vec<u8> {
    (0..SYMS).flat_map(|s| sym_name(s).into_bytes()).collect()
}

fn desc_dict() -> (Vec<u8>, Vec<u64>) {
    let mut strings = Vec::new();
    let mut offsets = vec![0u64];
    for p in 0..DESCRIPTION_PHRASES {
        strings.extend_from_slice(phrase(p).as_bytes());
        offsets.push(strings.len() as u64);
    }
    (strings, offsets)
}

/// 月数据 → 拥有型 Data（splayed 建表输入）。
fn month_to_splayed_data(md: &MonthData) -> Data {
    let (desc_strings, desc_offsets) = desc_dict();
    let sym_strings = sym_strings();
    // sym 字典 offsets 按实际字符串长度累计（SYM%04d = 7 字节/项）
    let mut sym_offsets = vec![0u64];
    let mut sym_acc = 0usize;
    for s in 0..SYMS {
        sym_acc += sym_name(s).len();
        sym_offsets.push(sym_acc as u64);
    }
    let mut columns = vec![
        Column::from_dict(md.sym_keys.clone(), sym_offsets, sym_strings, None),
        Column {
            data_type: DataType::TimestampUs,
            values: Buffer::from_slice_copy(&md.times),
            validity: None,
            dict: None,
        },
        Column::from_dict(md.desc_keys.clone(), desc_offsets, desc_strings, None),
    ];
    for f in 0..NUM_FIELDS {
        columns.push(Column {
            data_type: if f % 2 == 0 { DataType::Int64 } else { DataType::Float64 },
            values: Buffer::from_vec(md.numerics[f].clone()),
            validity: None,
            dict: None,
        });
    }
    Data::new(bench_schema(), columns).unwrap()
}

/// 月数据 → Arrow RecordBatch（parquet 写入输入）。
fn month_to_record_batch(md: &MonthData) -> RecordBatch {
    let syms: Vec<String> = (0..md.rows)
        .map(|i| sym_name(md.sym_keys[i] as usize))
        .collect();
    let descs: Vec<String> = (0..md.rows)
        .map(|i| phrase(md.desc_keys[i] as usize))
        .collect();
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(syms)) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(md.times.clone())) as ArrayRef,
        Arc::new(StringArray::from(descs)) as ArrayRef,
    ];
    let mut fields = vec![
        arrow_schema::Field::new("sym", ArrowDt::Utf8, false),
        arrow_schema::Field::new("time", ArrowDt::Timestamp(TimeUnit::Microsecond, None), false),
        arrow_schema::Field::new("description", ArrowDt::Utf8, false),
    ];
    for (f, bytes) in md.numerics.iter().enumerate() {
        if f % 2 == 0 {
            let vals: Vec<i64> = bytes
                .chunks_exact(8)
                .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
                .collect();
            arrays.push(Arc::new(Int64Array::from(vals)) as ArrayRef);
            fields.push(arrow_schema::Field::new(format!("n{f}"), ArrowDt::Int64, false));
        } else {
            let vals: Vec<f64> = bytes
                .chunks_exact(8)
                .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
                .collect();
            arrays.push(Arc::new(Float64Array::from(vals)) as ArrayRef);
            fields.push(arrow_schema::Field::new(format!("n{f}"), ArrowDt::Float64, false));
        }
    }
    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays).unwrap()
}

// ---------------------------------------------------------------- 场景

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum Scenario {
    W1,
    R1,
    R2,
    R3,
    R4,
    R5,
    R6,
}

impl Scenario {
    fn name(self) -> &'static str {
        match self {
            Scenario::W1 => "W1",
            Scenario::R1 => "R1",
            Scenario::R2 => "R2",
            Scenario::R3 => "R3",
            Scenario::R4 => "R4",
            Scenario::R5 => "R5",
            Scenario::R6 => "R6",
        }
    }
}

/// 场景投影（值字段；splayed 读回额外恒含 sym/time）。
fn scenario_projection(sc: Scenario) -> Vec<String> {
    match sc {
        Scenario::R2 | Scenario::R6 => vec!["n0".into()],
        Scenario::R3 | Scenario::R4 | Scenario::R5 => {
            vec!["description".into(), "n0".into(), "n1".into(), "n2".into()]
        }
        _ => vec![],
    }
}

/// 场景定义的逻辑投影字节/行（两引擎一致）：
/// R1 = sym keys 4 + time 8 + desc keys 4 + 47×8 = 392；R2/R6 = 8；R3/R4/R5 = 32。
fn scenario_bytes_per_row(sc: Scenario) -> u64 {
    match sc {
        Scenario::W1 => 4 + 8 + 4 + NUM_FIELDS as u64 * 8,
        Scenario::R1 => 4 + 8 + 4 + NUM_FIELDS as u64 * 8,
        Scenario::R2 | Scenario::R6 => 8,
        Scenario::R3 | Scenario::R4 | Scenario::R5 => 4 + 4 + 3 * 8,
    }
}

/// sym IN 集合（1% = 10 个；0.1% = 1 个——1000 sym 下的下限）。
fn sym_in_set(sel: f64) -> Vec<usize> {
    let count = ((SYMS as f64 * sel).ceil() as usize).max(1);
    (0..count).map(|i| i * (SYMS / count)).collect()
}

/// time sel 连续窗口（从总范围起点取）。
fn time_window(sel: f64, total_span: i64, t0: i64) -> (i64, i64) {
    (t0, t0 + (total_span as f64 * sel) as i64)
}

/// 场景谓词：And(Or(sym IN), time BETWEEN)。
fn scenario_predicate(sc: Scenario, t0: i64, span: i64) -> Option<splayed_core::Predicate> {
    use splayed_core::{CmpOp, Predicate, Scalar};
    let sel_sym = match sc {
        Scenario::R3 | Scenario::R4 => Some(0.01),
        Scenario::R6 => Some(0.001),
        _ => None,
    };
    let sel_time = match sc {
        Scenario::R3 | Scenario::R5 => Some(0.01),
        Scenario::R6 => Some(0.0001),
        _ => None,
    };
    let mut parts: Vec<Predicate> = Vec::new();
    if let Some(sel) = sel_sym {
        let syms: Vec<Predicate> = sym_in_set(sel)
            .into_iter()
            .map(|s| Predicate::cmp("sym", CmpOp::Eq, Scalar::Str(sym_name(s).into())))
            .collect();
        parts.push(Predicate::Or(syms));
    }
    if let Some(sel) = sel_time {
        let (lo, hi) = time_window(sel, span, t0);
        parts.push(Predicate::cmp("time", CmpOp::Ge, Scalar::Int(lo)));
        parts.push(Predicate::cmp("time", CmpOp::Lt, Scalar::Int(hi)));
    }
    match parts.len() {
        0 => None,
        _ => Some(Predicate::And(parts)),
    }
}

// ---------------------------------------------------------------- 校验和

/// 按列独立累加的校验和（FNV-1a；与分批结构无关——两引擎分批不同也能比较）。
#[derive(Default)]
struct Checksum {
    rows: u64,
    cols: HashMap<String, u64>,
    bytes: HashMap<String, u64>,
}

impl Checksum {
    fn new() -> Self {
        Checksum::default()
    }
    fn feed(&mut self, name: &str, data: &[u8]) {
        self.rows += 0; // 行数由调用方维护
        let h = self.cols.entry(name.to_string()).or_insert(0xcbf2_9ce4_8422_2325);
        let bl = self.bytes.entry(name.to_string()).or_insert(0);
        *bl += data.len() as u64;
        for &b in data {
            *h ^= b as u64;
            *h = h.wrapping_mul(0x100000001b3);
        }
    }
}

fn feed_col_values(cs: &mut Checksum, name: &str, col: &ArrayRef) {
    if name == "description" {
        return; // Utf8 字段不 roundtrip（已知限制，见 design-boundary）——校验跳过
    }
    if let Some(d) = col
        .as_any()
        .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>()
    {
        let vals = d
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dict values utf8");
        for k in d.keys().iter() {
            match k {
                Some(idx) => cs.feed(name, vals.value(idx as usize).as_bytes()),
                None => cs.feed(name, b"\x00NULL"),
            }
            cs.feed(name, b"|");
        }
    } else if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        for i in 0..s.len() {
            if s.is_null(i) {
                cs.feed(name, b"\x00NULL");
            } else {
                cs.feed(name, s.value(i).as_bytes());
            }
            cs.feed(name, b"|");
        }
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        cs.feed(name, bytemuck::cast_slice::<i64, u8>(a.values()));
    } else if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        cs.feed(name, bytemuck::cast_slice::<f64, u8>(a.values()));
    } else if let Some(a) = col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        cs.feed(name, bytemuck::cast_slice::<i64, u8>(a.values()));
    } else {
        panic!("unsupported column type for checksum: {:?}", col.data_type());
    }
}

/// RecordBatch → 增量校验和（行序敏感；字符串按行解析）。
fn feed_batch(cs: &mut Checksum, batch: &RecordBatch) {
    cs.rows += batch.num_rows() as u64;
    for (i, col) in batch.columns().iter().enumerate() {
        feed_col_values(cs, batch.schema().field(i).name(), col);
    }
}

fn feed_col_view(cs: &mut Checksum, name: &str, col: &splayed_format::ColumnView<'_>) {
    if name == "description" {
        return; // 同上：Utf8 字段校验跳过
    }
    for seg in col.segments() {
        match seg.values() {
            splayed_format::ColumnValues::Fixed(v) => cs.feed(name, v.as_slice()),
            splayed_format::ColumnValues::Dict { keys, dict_offsets, dict_strings } => {
                for i in 0..keys.len() / 4 {
                    let k = u32::from_le_bytes(
                        keys.as_slice()[i * 4..i * 4 + 4].try_into().unwrap(),
                    ) as usize;
                    let lo = u64::from_le_bytes(
                        dict_offsets.as_slice()[k * 8..k * 8 + 8].try_into().unwrap(),
                    ) as usize;
                    let hi = u64::from_le_bytes(
                        dict_offsets.as_slice()[k * 8 + 8..k * 8 + 16].try_into().unwrap(),
                    ) as usize;
                    cs.feed(name, &dict_strings.as_slice()[lo..hi]);
                    cs.feed(name, b"|");
                }
            }
            splayed_format::ColumnValues::RepeatDict { dict_offsets, dict_strings, dict_index } => {
                let lo = u64::from_le_bytes(
                    dict_offsets.as_slice()[*dict_index as usize * 8..*dict_index as usize * 8 + 8]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let hi = u64::from_le_bytes(
                    dict_offsets.as_slice()
                        [*dict_index as usize * 8 + 8..*dict_index as usize * 8 + 16]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let sv = &dict_strings.as_slice()[lo..hi];
                for _ in 0..seg.rows() {
                    cs.feed(name, sv);
                    cs.feed(name, b"|");
                }
            }
        }
    }
}

fn feed_data_view(cs: &mut Checksum, view: &splayed_format::DataView<'_>) {
    cs.rows += view.length() as u64;
    for field in &view.schema.fields {
        let col = view.column(&field.name).expect("schema iteration guarantees");
        feed_col_view(cs, field.name.as_ref(), col);
    }
}

// ---------------------------------------------------------------- CSV

struct CsvWriter {
    w: std::io::BufWriter<std::fs::File>,
}

impl CsvWriter {
    fn new(path: &Path) -> Self {
        let mut w = std::io::BufWriter::new(std::fs::File::create(path).unwrap());
        writeln!(w, "engine,variant,scenario,scale_rows,run_idx,time_ms,output_rows,output_bytes,physical_size,compression_ratio").unwrap();
        CsvWriter { w }
    }
    #[allow(clippy::too_many_arguments)]
    fn row(
        &mut self,
        engine: &str,
        variant: &str,
        scenario: &str,
        scale_rows: usize,
        run_idx: usize,
        time_ms: f64,
        output_rows: usize,
        output_bytes: u64,
        physical_size: u64,
        uncompressed: u64,
    ) {
        let ratio = if uncompressed > 0 {
            physical_size as f64 / uncompressed as f64
        } else {
            0.0
        };
        writeln!(
            self.w,
            "{engine},{variant},{scenario},{scale_rows},{run_idx},{time_ms:.2},{output_rows},{output_bytes},{physical_size},{ratio:.4}"
        )
        .unwrap();
    }
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if path.is_dir() {
        for e in fs::read_dir(path).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                total += dir_size(&p);
            } else {
                total += e.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    } else if let Ok(m) = fs::metadata(path) {
        total += m.len();
    }
    total
}

// ---------------------------------------------------------------- 引擎写入

/// 按月惰性生成器（避免全量物化）。
struct MonthGenerator {
    rows_per_sym: usize,
    month_offsets: Vec<usize>,
}

impl MonthGenerator {
    fn new(total_rows: usize) -> Self {
        let rows_per_sym = total_rows / SYMS;
        assert!(rows_per_sym * SYMS == total_rows, "rows must be a multiple of {SYMS}");
        let mut month_offsets = vec![0usize];
        let mut acc = 0usize;
        for k in rows_per_month(rows_per_sym) {
            acc += k;
            month_offsets.push(acc);
        }
        MonthGenerator { rows_per_sym, month_offsets }
    }
    fn months(&self) -> usize {
        48
    }
    fn generate(&self, m: usize) -> MonthData {
        generate_month(m, self.rows_per_sym, &self.month_offsets)
    }
}

/// 逐月生成（不计入写入时间）+ 逐分区写入（计时：累计各分区 create 耗时）。
/// 首月走 create_table（创建根目录），后续月份走 create_table_partition。
fn write_splayed(
    root: &Path,
    gen: &MonthGenerator,
    months: usize,
    max_parallelism: usize,
) -> (f64, u64) {
    let _ = fs::remove_dir_all(root);
    let mut write_time = std::time::Duration::ZERO;
    for m in 0..months {
        let md = gen.generate(m);
        let data = month_to_splayed_data(&md);
        let t0 = Instant::now();
        if m == 0 {
            init_table(
                root,
                splayed_table::PartitionScheme::Month,
                &data.as_view(),
                Some(TableOptions {
                    max_parallelism: Some(max_parallelism),
                    compression: Some(splayed_format::Compression::Zstd),
                    ..Default::default()
                }),
            )
            .unwrap();
        } else {
            create_table_partition(
                root,
                &month_name(m),
                data,
                CreateDatasetOptions {
                    max_parallelism,
                    compression: splayed_format::Compression::Zstd,
                    ..Default::default()
                },
            )
            .unwrap();
        }
        write_time += t0.elapsed();
    }
    let physical = dir_size(root);
    (write_time.as_secs_f64(), physical)
}

/// Parquet 单文件写入（单线程 writer；行组 1M；ZSTD level 3）。
fn write_parquet(pq: &Path, gen: &MonthGenerator, months: usize) -> (f64, u64) {
    let _ = fs::remove_file(pq);
    fs::create_dir_all(pq.parent().unwrap()).unwrap();
    let t0 = Instant::now();
    let file = fs::File::create(pq).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(PARQUET_ROW_GROUP)
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3).unwrap(),
        ))
        .build();
    let mut writer = ArrowWriter::try_new(file, pq_file_schema(), Some(props)).unwrap();
    for m in 0..months {
        let md = gen.generate(m);
        let batch = month_to_record_batch(&md);
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    let write_s = t0.elapsed().as_secs_f64();
    let physical = fs::metadata(pq).unwrap().len();
    (write_s, physical)
}

fn pq_file_schema() -> std::sync::Arc<arrow_schema::Schema> {
    let mut fields = vec![
        arrow_schema::Field::new("sym", ArrowDt::Utf8, false),
        arrow_schema::Field::new("time", ArrowDt::Timestamp(TimeUnit::Microsecond, None), false),
        arrow_schema::Field::new("description", ArrowDt::Utf8, false),
    ];
    for f in 0..NUM_FIELDS {
        fields.push(arrow_schema::Field::new(
            format!("n{f}"),
            if f % 2 == 0 { ArrowDt::Int64 } else { ArrowDt::Float64 },
            false,
        ));
    }
    std::sync::Arc::new(arrow_schema::Schema::new(fields))
}

// ---------------------------------------------------------------- 场景读

/// 场景请求（投影 + 谓词；limit 无）。
fn scenario_request(sc: Scenario, span: i64) -> TableScanRequest {
    let t0 = month_start_us(0);
    TableScanRequest {
        projection: scenario_projection(sc),
        predicate: scenario_predicate(sc, t0, span),
        limit: None,
        ..Default::default()
    }
}

/// Splayed 读场景一次执行（TableReader 流式消费）。
/// `with_checksum = false` 时只计数行（校验不计入性能时间，见 docs/benchmark.md §7）。
fn read_splayed_once(
    table: &TableHandle,
    sc: Scenario,
    span: i64,
    with_checksum: bool,
) -> (f64, usize, u64, Option<Checksum>) {
    let start = Instant::now();
    let scanner = scan_table(table, scenario_request(sc, span)).unwrap();
    let mut reader = read_table(table, scanner, Some(READ_BATCH));
    let mut cs = Checksum::new();
    let mut rows = 0usize;
    while let Some(view) = reader.next().unwrap() {
        rows += view.length();
        if with_checksum {
            feed_data_view(&mut cs, &view);
        }
    }
    reader.close().unwrap();
    let elapsed = start.elapsed().as_secs_f64();
    let bytes = rows as u64 * scenario_bytes_per_row(sc);
    let cs_out = if with_checksum { Some(cs) } else { None };
    (elapsed, rows, bytes, cs_out)
}

/// Parquet 读场景一次执行（ProjectionMask + RowFilter）。
fn read_parquet_once(
    pq: &Path,
    sc: Scenario,
    span: i64,
    with_checksum: bool,
) -> (f64, usize, u64, Option<Checksum>) {
    use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
    use parquet::arrow::ProjectionMask;

    let file = fs::File::open(pq).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.schema().clone();
    let col = |name: &str| schema.fields().iter().position(|f| f.name() == name).unwrap();

    let builder = match scenario_projection(sc).len() {
        0 => builder,
        1 => {
            let mask = ProjectionMask::leaves(builder.parquet_schema(), [col("n0")]);
            builder.with_projection(mask)
        }
        _ => {
            let mask = ProjectionMask::leaves(
                builder.parquet_schema(),
                [col("sym"), col("description"), col("n0"), col("n1"), col("n2")],
            );
            builder.with_projection(mask)
        }
    };

    let mut preds: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> = Vec::new();
    let sym_sel = match sc {
        Scenario::R3 | Scenario::R4 => Some(0.01),
        Scenario::R6 => Some(0.001),
        _ => None,
    };
    if let Some(sel) = sym_sel {
        let syms: Arc<HashSet<String>> = Arc::new(sym_in_set(sel).into_iter().map(sym_name).collect());
        let mask = ProjectionMask::leaves(builder.parquet_schema(), [col("sym")]);
        preds.push(Box::new(ArrowPredicateFn::new(
            mask,
            move |batch: RecordBatch| {
                let arr = batch
                    .column_by_name("sym")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                Ok(arrow_array::BooleanArray::from_iter(
                    (0..arr.len()).map(|i| Some(syms.contains(arr.value(i)))),
                ))
            },
        )));
    }
    let time_sel = match sc {
        Scenario::R3 | Scenario::R5 => Some(0.01),
        Scenario::R6 => Some(0.0001),
        _ => None,
    };
    if let Some(sel) = time_sel {
        let (t_lo, t_hi) = time_window(sel, span, month_start_us(0));
        let mask = ProjectionMask::leaves(builder.parquet_schema(), [col("time")]);
        preds.push(Box::new(ArrowPredicateFn::new(
            mask,
            move |batch: RecordBatch| {
                let arr = batch
                    .column_by_name("time")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                Ok(arrow_array::BooleanArray::from_iter(
                    (0..arr.len()).map(|i| Some(arr.value(i) >= t_lo && arr.value(i) < t_hi)),
                ))
            },
        )));
    }
    let builder = if preds.is_empty() {
        builder
    } else {
        builder.with_row_filter(RowFilter::new(preds))
    };

    let start = Instant::now();
    let reader = builder.build().unwrap();
    let mut cs = Checksum::new();
    let mut rows = 0usize;
    for batch in reader {
        let batch = batch.unwrap();
        rows += batch.num_rows();
        if with_checksum {
            feed_batch(&mut cs, &batch);
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let bytes = rows as u64 * scenario_bytes_per_row(sc);
    let cs_out = if with_checksum { Some(cs) } else { None };
    (elapsed, rows, bytes, cs_out)
}

// ---------------------------------------------------------------- 主流程

mod partition;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s == "--mode") .unwrap_or(false) && args.get(2).map(|s| s == "partition").unwrap_or(false) {
        // partition 模式：--mode partition --scale <rows> --runs <n> --out <csv>
        let mut scale = 0usize;
        let mut runs = 3usize;
        let mut out = PathBuf::from("results/partition_bench.csv");
        let mut i = 3usize;
        while i < args.len() {
            match args[i].as_str() {
                "--scale" => { i += 1; scale = args[i].parse().unwrap(); }
                "--runs" => { i += 1; runs = args[i].parse().unwrap(); }
                "--out" => { i += 1; out = PathBuf::from(&args[i]); }
                other => panic!("unknown arg {other}"),
            }
            i += 1;
        }
        partition::run_partition(&out, scale, runs);
        return;
    }
    let mut scales: Vec<usize> = vec![1, 5, 10, 20];
    let mut runs = 3usize;
    let mut out = PathBuf::from("results/bench.csv");
    let mut i = 1usize;
    while i < args.len() {
        match args[i].as_str() {
            "--scales" => {
                i += 1;
                scales = args[i].split(',').map(|s| s.trim().parse().unwrap()).collect();
            }
            "--runs" => {
                i += 1;
                runs = args[i].parse().unwrap();
            }
            "--out" => {
                i += 1;
                out = PathBuf::from(&args[i]);
            }
            other => panic!("unknown arg {other}"),
        }
        i += 1;
    }
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let mut csv = CsvWriter::new(&out);

    for millions in scales {
        println!("=== scale {millions}M rows ===");
        run_scale(&mut csv, millions * 1_000_000, runs);
    }
    println!("done");
}

fn run_scale(csv: &mut CsvWriter, total_rows: usize, runs: usize) {
    let gen = MonthGenerator::new(total_rows);
    let months = gen.months();
    let uncompressed = total_rows as u64 * (4 + 8 + 4 + NUM_FIELDS as u64 * 8);
    let mut store_checksums: Vec<(String, Vec<(String, Checksum)>)> = Vec::new();

    // ---- W1 + R1–R6：splayed 1t / 4t（1t 存储读后即删，省磁盘） ----
    for threads in [1usize, 4usize] {
        let root = PathBuf::from(format!("bench_data/splayed_{threads}t"));
        let (write_s, physical) = write_splayed(&root, &gen, months, threads);
        println!(
            "splayed {threads}t write: {:.1} ms, physical {:.2} GB",
            write_s * 1000.0,
            physical as f64 / 1e9
        );
        csv.row(
            "splayed", &format!("{threads}t"), "W1", total_rows, 0,
            write_s * 1000.0, total_rows, uncompressed, physical, uncompressed,
        );

        let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();
        let span = month_start_us(months) - month_start_us(0);
        let mut checksums: Vec<(String, Checksum)> = Vec::new();
        for sc in [Scenario::R1, Scenario::R2, Scenario::R3, Scenario::R4, Scenario::R5, Scenario::R6] {
            // 独立免计时校验 pass（校验不计入性能时间）
            let (_, _, _, cs_out) = read_splayed_once(&table, sc, span, true);
            checksums.push((sc.name().to_string(), cs_out.expect("checksum pass")));
            // 预热（不计时、不校验）
            let _ = read_splayed_once(&table, sc, span, false);
            // 正式计时（3–5 次取中位数）
            for run in 0..runs {
                let (t, rows, bytes, _) = read_splayed_once(&table, sc, span, false);
                csv.row(
                    "splayed", &format!("{threads}t"), sc.name(), total_rows, run,
                    t * 1000.0, rows, bytes, physical, uncompressed,
                );
            }
        }
        close_table(table).unwrap();
        store_checksums.push((format!("splayed-{threads}t"), checksums));
        // 读后即删（校验和已在内存，无需存储共存）——20M 规模下峰值磁盘 = 单存储
        let _ = fs::remove_dir_all(&root);
    }

    // ---- W1 + R1–R6：parquet 单文件（单线程 writer） ----
    let pq = PathBuf::from("bench_data/parquet.parquet");
    let (write_s, physical) = write_parquet(&pq, &gen, months);
    println!(
        "parquet write: {:.1} ms, physical {:.2} GB",
        write_s * 1000.0,
        physical as f64 / 1e9
    );
    csv.row(
        "parquet", "1t", "W1", total_rows, 0,
        write_s * 1000.0, total_rows, uncompressed, physical, uncompressed,
    );

    let mut pq_checksums: Vec<(String, Checksum)> = Vec::new();
    for sc in [Scenario::R1, Scenario::R2, Scenario::R3, Scenario::R4, Scenario::R5, Scenario::R6] {
        let (_, _, _, cs_out) = read_parquet_once(&pq, sc, span_for(months), true); // 校验 pass
        pq_checksums.push((sc.name().to_string(), cs_out.expect("checksum pass")));
        let _ = read_parquet_once(&pq, sc, span_for(months), false); // 预热
        for run in 0..runs {
            let (t, rows, bytes, _) = read_parquet_once(&pq, sc, span_for(months), false);
            csv.row(
                "parquet", "1t", sc.name(), total_rows, run,
                t * 1000.0, rows, bytes, physical, uncompressed,
            );
        }
    }
    let _ = fs::remove_file(&pq);

    // 正确性校验：splayed-4t vs parquet 校验和一致（行序 / 列值 / NULL）
    let splayed_cs = &store_checksums[1].1;
    for (name, cs) in splayed_cs {
        let pq_cs = pq_checksums.iter().find(|(n, _)| n == name).unwrap();
        assert_eq!(cs.rows, pq_cs.1.rows, "{name} rows mismatch");
        // 结构性差异：splayed 读恒返回 sym/time（API 契约），parquet 投影仅返回
        // 选中列——校验「共同列」的逐列校验和一致即可
        for (cname, sp_h) in &cs.cols {
            if let Some(pq_h) = pq_cs.1.cols.get(cname) {
                assert_eq!(sp_h, pq_h, "{name} column '{cname}' checksum mismatch");
            }
        }
    }
    println!("correctness: splayed == parquet (all scenarios)");
}

fn span_for(months: usize) -> i64 {
    month_start_us(months) - month_start_us(0)
}
