//! Splayed V1 CLI tool — create datasets and run SQL queries.
//!
//! This binary provides subcommands that the Python example scripts call
//! via subprocess to demonstrate the full Splayed workflow:
//!
//! 1. `init`   — create a sample stock dataset (SYM×TIME×FIELD)
//! 2. `update` — update field values (in-place write)
//! 3. `compact`— compress a field with ZSTD or LZ4
//! 4. `read`   — read a field's raw values back
//! 5. `sql`    — run a SQL query via DataFusion TableProvider
//!
//! Usage:
//!   splayed init  <dataset_dir>
//!   splayed update <dataset_dir> --field close --sym AAPL --time 0 --value 999.99
//!   splayed compact <dataset_dir> --field close --algo zstd
//!   splayed read   <dataset_dir> --field close
//!   splayed sql     <dataset_dir> --query "SELECT * FROM splayed"

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{
    Array, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use clap::{Parser, Subcommand};
use datafusion::prelude::SessionContext;
use splayed_arrow::create_table;
use splayed_core::{
    compact_field as core_compact, open_dataset,
    update_field, FieldReader, UpdateItem,
};
use splayed_datafusion::register_splayed_table;
use splayed_format::{Compression, DataType, RawValue};

#[derive(Parser)]
#[command(name = "splayed", about = "Splayed V1 CLI — columnar storage engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a sample stock dataset with 3 symbols × 5 days.
    Init {
        dataset_dir: PathBuf,
    },
    /// Update a single cell value (in-place).
    Update {
        dataset_dir: PathBuf,
        #[arg(long)]
        field: String,
        #[arg(long)]
        sym: String,
        #[arg(long)]
        time: i64,
        #[arg(long)]
        value: f64,
    },
    /// Compress a field (ZSTD or LZ4). After compaction the field is read-only.
    Compact {
        dataset_dir: PathBuf,
        #[arg(long)]
        field: String,
        #[arg(long, default_value = "zstd")]
        algo: String,
    },
    /// Read a field's raw values and print to stdout.
    Read {
        dataset_dir: PathBuf,
        #[arg(long)]
        field: String,
        #[arg(long)]
        sym: Option<String>,
    },
    /// Run a SQL query via DataFusion TableProvider.
    Sql {
        dataset_dir: PathBuf,
        #[arg(long, default_value = "SELECT * FROM splayed LIMIT 20")]
        query: String,
    },
    /// Export the full dataset as a CSV file (for DuckDB / pandas / Excel).
    Export {
        dataset_dir: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Export the full dataset as Arrow IPC (for DuckDB read_arrow / Arrow ecosystem).
    ExportArrow {
        dataset_dir: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

fn make_sample_batch() -> RecordBatch {
    // 3 symbols × 5 days each = 15 rows
    // AAPL: days 0-4  close = 150..154  volume = 1000..4000
    // GOOG: days 0-4  close = 280..284  volume = 2000..6000
    // MSFT: days 0-4  close = 380..384  volume = 3000..7000
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
        Field::new("volume", ArrowDT::Int64, true),
    ]));

    let time = Date32Array::from(vec![0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4]);
    let sym = StringArray::from(vec![
        Some("AAPL"), Some("AAPL"), Some("AAPL"), Some("AAPL"), Some("AAPL"),
        Some("GOOG"), Some("GOOG"), Some("GOOG"), Some("GOOG"), Some("GOOG"),
        Some("MSFT"), Some("MSFT"), Some("MSFT"), Some("MSFT"), Some("MSFT"),
    ]);
    let close = Float64Array::from(vec![
        Some(150.0), Some(151.0), Some(152.0), Some(153.0), Some(154.0),
        Some(280.0), Some(281.0), Some(282.0), Some(283.0), Some(284.0),
        Some(380.0), Some(381.0), Some(382.0), Some(383.0), Some(384.0),
    ]);
    let volume = Int64Array::from(vec![
        Some(1000), Some(2000), Some(3000), Some(4000), Some(5000),
        Some(2000), Some(3000), Some(4000), Some(5000), Some(6000),
        Some(3000), Some(4000), Some(5000), Some(6000), Some(7000),
    ]);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(time),
            Arc::new(sym),
            Arc::new(close),
            Arc::new(volume),
        ],
    )
    .unwrap()
}

fn cmd_init(dir: &PathBuf) {
    // Only overwrite if it's an existing Splayed dataset (has .meta).
    let meta_path = dir.join(".meta");
    if meta_path.exists() {
        std::fs::remove_dir_all(dir).expect("removing existing dataset");
    } else if dir.exists() {
        eprintln!("✗ Directory exists and is not a Splayed dataset: {}", dir.display());
        std::process::exit(1);
    }
    create_table(dir, &make_sample_batch(), true).unwrap();
    println!("✓ Created dataset: {}", dir.display());
    let ds = open_dataset(dir).unwrap();
    println!("  Symbols: {:?}", ds.meta.symbols);
    println!("  Fields: {:?}", ds.list_fields().unwrap());
    println!("  Total rows: {}", ds.meta.total_rows());
}

fn cmd_update(dir: &PathBuf, field: &str, sym: &str, time_val: i64, value: f64) {
    let ds = open_dataset(dir).unwrap();
    let row = ds.meta.global_row(sym, time_val).unwrap_or_else(|| {
        eprintln!("✗ Symbol '{sym}' has no time point {time_val}");
        std::process::exit(1);
    });
    let field_path = ds.field_path(field);
    // Read the field's DataType to determine the correct value width.
    let reader = FieldReader::open(&field_path).expect("open field for type check");
    let field_dt = reader.data_type();
    let sz = field_dt.size_of();
    let mut value_bytes = vec![0u8; sz];
    // Encode the value based on the field type.
    let raw = match field_dt {
        DataType::Float64 => RawValue::from_f64(value),
        DataType::Float32 => RawValue::from_f32(value as f32),
        DataType::Int64 => RawValue::from_i64(value as i64),
        DataType::Int32 => RawValue::from_i32(value as i32),
        _ => {
            eprintln!("✗ Cannot update field of type {field_dt:?} with f64 value");
            std::process::exit(1);
        }
    };
    raw.write_le(&mut value_bytes, 0);
    // Drop the reader before writing (Windows mmap safety).
    drop(reader);
    update_field(&field_path, &[UpdateItem::new(row, value_bytes)]).unwrap();
    println!("✓ Updated {field}[{sym}, time={time_val}] = {value}");
}

fn cmd_compact(dir: &PathBuf, field: &str, algo: &str) {
    let ds = open_dataset(dir).unwrap();
    let field_path = ds.field_path(field);
    let comp = match algo.to_lowercase().as_str() {
        "zstd" => Compression::Zstd,
        "lz4" => Compression::Lz4,
        _ => panic!("Unknown algorithm: {algo} (use zstd or lz4)"),
    };
    core_compact(&field_path, comp).unwrap();
    println!("✓ Compacted {field} with {algo}");
}

fn cmd_read(dir: &PathBuf, field: &str, sym_filter: Option<&str>) {
    let ds = open_dataset(dir).unwrap();
    let field_path = ds.field_path(field);
    let reader = FieldReader::open(&field_path).unwrap();

    let dt = reader.data_type();
    let total = reader.row_count();
    println!("Field '{field}': type={dt:?}, rows={total}");

    let symbols = match sym_filter {
        Some(s) => vec![s.to_string()],
        None => ds.meta.symbols.clone(),
    };

    for sym_name in &symbols {
        let sym_idx = match ds.meta.find_symbol(sym_name) {
            Some(idx) => idx,
            None => {
                eprintln!("✗ Symbol '{sym_name}' not found in dataset");
                eprintln!("  Available symbols: {:?}", ds.meta.symbols);
                std::process::exit(1);
            }
        };
        let sym_rec = &ds.meta.sym_index[sym_idx];
        let row_start = sym_rec.row_start as usize;
        let row_count = sym_rec.time_count as usize;
        let time_start = sym_rec.time_start as usize;
        print!("  {sym_name:6}: ");
        for i in 0..row_count {
            let row = (row_start + i) as u32;
            // `i` is the LOCAL row index within this symbol's range (0..row_count),
            // and `time_start` is this symbol's start index into the global TIME
            // AXIS. So `time_start + i` is the correct global time axis index for
            // this row's time label.
            let t = ds.meta.time_axis[time_start + i];
            match dt {
                DataType::Float64 => {
                    let v = reader.read_row(row).unwrap();
                    match v.as_f64() {
                        Some(f) => print!("[t={t}: {f:.1}] "),
                        None => print!("[t={t}: NULL] "),
                    }
                }
                DataType::Int64 => {
                    let v = reader.read_row(row).unwrap();
                    match v.as_i64() {
                        Some(v) => print!("[t={t}: {v}] "),
                        None => print!("[t={t}: NULL] "),
                    }
                }
                _ => print!("[t={t}: ?] "),
            }
        }
        println!();
    }
}

async fn cmd_sql(dir: &PathBuf, query: &str) {
    let ctx = SessionContext::new();
    // Layer 3: auto-detect single dataset vs partitioned table.
    register_splayed_table(&ctx, "splayed", dir).unwrap();

    println!("SQL: {query}");
    let batches = ctx.sql(query).await.unwrap().collect().await.unwrap();

    for batch in &batches {
        print_record_batch(batch);
    }
}

/// Print a RecordBatch as a simple table.
fn print_record_batch(batch: &RecordBatch) {
    let schema = batch.schema();

    // Column headers
    let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let sep: String = headers.iter().map(|h| "-".repeat(h.len().max(8) + 2)).collect::<Vec<_>>().join("+");
    let header_line: String = headers
        .iter()
        .map(|h| format!(" {:<w$} ", h, w = h.len().max(8)))
        .collect::<Vec<_>>()
        .join("|");
    println!("{header_line}");
    println!("{sep}");

    // Rows
    for row_idx in 0..batch.num_rows() {
        let cells: Vec<String> = (0..batch.num_columns())
            .map(|col_idx| {
                let col = batch.column(col_idx);
                let dt = schema.field(col_idx).data_type();
                use arrow_schema::DataType;
                match dt {
                    DataType::Utf8 => {
                        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                        if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                    }
                    DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        if arr.is_null(row_idx) { "NULL".to_string() } else { format!("{:.2}", arr.value(row_idx)) }
                    }
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        if arr.is_null(row_idx) { "NULL".to_string() } else { arr.value(row_idx).to_string() }
                    }
                    DataType::Date32 => {
                        let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                        arr.value(row_idx).to_string()
                    }
                    _ => "?".to_string(),
                }
            })
            .collect();
        let line: String = cells
            .iter()
            .enumerate()
            .map(|(i, c)| format!(" {:<w$} ", c, w = headers.get(i).map_or(8, |h| h.len().max(8))))
            .collect::<Vec<_>>()
            .join("|");
        println!("{line}");
    }
    println!();
}

/// Export the full dataset to a CSV file via DataFusion + arrow-csv.
async fn cmd_export(dir: &PathBuf, output: &PathBuf) {
    let ctx = SessionContext::new();
    // Layer 3: auto-detect single dataset vs partitioned table.
    register_splayed_table(&ctx, "splayed", dir).unwrap();

    let batches = ctx.sql("SELECT * FROM splayed").await.unwrap().collect().await.unwrap();

    // Write CSV
    use arrow_csv::WriterBuilder;
    let file = std::fs::File::create(output).unwrap();
    let mut writer = WriterBuilder::new().with_header(true).build(file);
    for batch in &batches {
        writer.write(batch).unwrap();
    }
    println!("✓ Exported {} rows to {}", batches.iter().map(|b| b.num_rows()).sum::<usize>(), output.display());
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Init { dataset_dir } => cmd_init(&dataset_dir),
        Commands::Update {
            dataset_dir,
            field,
            sym,
            time,
            value,
        } => cmd_update(&dataset_dir, &field, &sym, time, value),
        Commands::Compact {
            dataset_dir,
            field,
            algo,
        } => cmd_compact(&dataset_dir, &field, &algo),
        Commands::Read {
            dataset_dir,
            field,
            sym,
        } => cmd_read(&dataset_dir, &field, sym.as_deref()),
        Commands::Sql { dataset_dir, query } => cmd_sql(&dataset_dir, &query).await,
        Commands::Export { dataset_dir, output } => cmd_export(&dataset_dir, &output).await,
        Commands::ExportArrow { dataset_dir, output } => cmd_export_arrow(&dataset_dir, &output),
    }
}

/// Export the dataset to Arrow IPC format (Phase 9: Splayed → Arrow → DuckDB).
fn cmd_export_arrow(dir: &PathBuf, output: &PathBuf) {
    let rows = splayed_duckdb::export_to_arrow_ipc(dir, output).unwrap_or_else(|e| {
        eprintln!("✗ Export failed: {e}");
        std::process::exit(1);
    });
    println!("✓ Exported {rows} rows to Arrow IPC: {}", output.display());
}
