# Splayed V1

A specialized **columnar storage engine** for `SYM × TIME × FIELD` financial time-series data.

Designed for **extremely low I/O, O(1) row location, mmap/zero-copy reads, and high-throughput pre-allocated writes**, with Arrow as the exchange layer connecting to DataFusion and DuckDB.

---

## Design Principles

> **META decides *where* to read. FIELD decides *what* to read.**

| Principle | Decision |
|---|---|
| Data model | `SYM × TIME × FIELD` |
| Layout | Splayed — one FIELD per file |
| SYM | Contiguous rows per symbol, mmap-friendly |
| TIME | Global deduplicated axis; each SYM references a `[time_start, time_start+time_count)` interval |
| FIELD | Fixed-width types only; no STRING, no Block Index, no NULL bitmap |
| NULL | Type-internal sentinel bit pattern (canonical NaN for floats, `INT32_MIN` for ints) |
| Write model | Full pre-allocation + in-place update (`create_field` → `update_field`) |
| Compression | `NONE` = fastest mmap path; `ZSTD`/`LZ4` for cold data |
| Arrow | Exchange layer only, not the storage format |

---

## Architecture

```text
                     User / SQL
                        |
              +---------+---------+
              |                   |
        DataFusion            DuckDB
              |                   |
        Arrow Adapter     DuckDB Extension
              |                   |
              +---+---+---+---+---+
                  |   Scanner    |
                  +---+---+---+--+
                      |       |
                   META     FIELDs
                   (index)  (values)
                      |       |
                      v       v
                   mmap / filesystem
```

### Crate Structure

```text
splayed/
├── splayed-format/      # Binary format: META + FIELD headers, types, NULL encoding (no deps)
├── splayed-codec/       # Encoding (PLAIN) + compression (NONE/ZSTD/LZ4)
├── splayed-core/        # Reader (mmap), field writer, dataset, scanner (no Arrow)
├── splayed-arrow/       # Arrow exchange: create_meta, create_table, update_table, ColumnView→Arrow
└── splayed-datafusion/  # DataFusion TableProvider: SQL queries with pushdown
```

Dependency direction: `format ← codec ← core → arrow → datafusion`

---

## Quick Start

### Build & Test

```bash
cargo build
cargo test
```

### Example: Create a Dataset and Read It Back

```rust
use arrow_array::{Date32Array, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use std::sync::Arc;
use splayed_arrow::create_table;
use splayed_core::{open_dataset, FieldReader};

// Build an Arrow RecordBatch: TIME + SYM + close
let schema = Arc::new(Schema::new(vec![
    Field::new("time", ArrowDT::Date32, false),
    Field::new("sym", ArrowDT::Utf8, false),
    Field::new("close", ArrowDT::Float64, true),
]));
let batch = RecordBatch::try_new(schema, vec![
    Arc::new(Date32Array::from(vec![0, 1, 2])),       // days since epoch
    Arc::new(StringArray::from(vec![Some("AAPL"); 3])),
    Arc::new(Float64Array::from(vec![Some(100.0), Some(101.0), Some(102.0)])),
]).unwrap();

// One-shot: create .meta + close field + fill values
create_table("my_dataset", &batch, true).unwrap();

// Read back
let dataset = open_dataset("my_dataset").unwrap();
let reader = FieldReader::open("my_dataset/close").unwrap();
assert_eq!(reader.read_row(0).unwrap().as_f64(), Some(100.0));
```

### Example: SQL Queries via DataFusion

```rust
use datafusion::prelude::SessionContext;
use splayed_arrow::create_table;
use splayed_datafusion::SplayedTableProvider;
// ... create_table as above ...

let ctx = SessionContext::new();
ctx.register_table("splayed", Arc::new(SplayedTableProvider::new("my_dataset").unwrap()))
    .unwrap();

// SQL with pushdown: SYM filter → SymbolSelection, TIME filter → TimeRange
let batches = ctx.sql("SELECT close FROM splayed WHERE sym = 'AAPL' AND time >= 1")
    .await.unwrap().collect().await.unwrap();
```

---

## Function API

Seven writer functions (see `plan.md` §8.4 for full specification):

| Function | Crate | Description |
|---|---|---|
| `create_meta(folder, data, sorted)` | splayed-arrow | Build `.meta` from Arrow RecordBatch (TIME + SYM) |
| `create_table(folder, data, sorted)` | splayed-arrow | One-shot: create_meta + create_field + update_field |
| `create_field(field_path, data_type)` | splayed-core | Pre-allocate FIELD file (all NULL) |
| `update_field(field_path, update_info[])` | splayed-core | In-place update at absolute row offsets |
| `delete_field(field_path)` | splayed-core | Delete a FIELD file (idempotent) |
| `compact_field(field_path, compression)` | splayed-codec | Compress FIELD (NONE → ZSTD), mark read-only |
| `update_table(folder, data, sorted)` | splayed-arrow | Update existing fields from Arrow RecordBatch |

### On-disk Layout

```text
dataset/
├── .meta          # Index: TIME AXIS + SYM DICT + SYM INDEX
├── close          # FIELD: [64B header] [data...]
├── open
├── volume
└── ...
```

### META Header (64 bytes)

| Offset | Field | Type |
|---:|---|---|
| 0 | `magic` | u64 (`SPLAYMTA`) |
| 8 | `version` | u16 |
| 12 | `time_type` | u8 (0=DATE32, 1=TIMESTAMP_US) |
| 16 | `generation` | u64 |
| 24 | `time_count` | u32 |
| 28 | `sym_count` | u32 |
| 32 | `sym_dict_offset` | u64 |
| 40 | `sym_index_offset` | u64 |
| 48 | `file_size` | u64 |

### SYM INDEX Record (12 bytes)

| Offset | Field | Type | Description |
|---:|---|---|---|
| 0 | `time_start` | u32 | Start index in global TIME AXIS |
| 4 | `time_count` | u32 | Time points = row capacity (full pre-declaration) |
| 8 | `row_start` | u32 | Start row in FIELD files = cumulative sum of prior `time_count` |

### FIELD Header (64 bytes)

| Offset | Field | Type |
|---:|---|---|
| 0 | `magic` | u64 (`SPLAYFLD`) |
| 12 | `data_type` | u8 |
| 13 | `encoding` | u8 |
| 14 | `compression` | u8 |
| 16 | `generation` | u64 |
| 24 | `row_count` | u32 |
| 28 | `null_count` | u32 |
| 40 | `data_length` | u64 |

### Data Types

| ID | Type | Size | NULL pattern |
|---:|---|---:|---|
| 0 | `BOOL` | 1 B | `0x02` |
| 1 | `INT32` | 4 B | `0x80000000` |
| 2 | `INT64` | 8 B | `0x8000000000000000` |
| 3 | `FLOAT32` | 4 B | `0x7FC00000` (canonical NaN) |
| 4 | `FLOAT64` | 8 B | `0x7FF8000000000000` (canonical NaN) |
| 5 | `DATE32` | 4 B | `0x80000000` |
| 6 | `TIMESTAMP_US` | 8 B | `0x8000000000000000` |

---

## Development Phases

| Phase | Status | Description |
|---:|---|---|
| 1 | ✅ Done | Format: META + FIELD binary read/write, types, NULL encoding |
| 2 | ✅ Done | Reader: mmap, SYM/TIME lookup, row range, column read |
| 3 | ✅ Done | Writer: all 7 function interfaces, generation, crash recovery |
| 4 | ✅ Done | Scanner: projection/predicate/filter pushdown, batch API, ColumnView |
| 5 | ✅ Done | Performance: parallel scan, SIMD filter, inline prefetch |
| 6 | ✅ Done | Compression: ZSTD, LZ4, DELTA, RLE, BITPACK |
| 7 | ✅ Done | Arrow: ColumnView→Arrow, NULL/NaN semantics, type mapping |
| 8 | ✅ Done | DataFusion: TableProvider, ExecutionPlan, pushdown |
| 9 | ✅ Done | DuckDB: Arrow IPC bridge (Splayed→Arrow→DuckDB) |

---

## License

MIT
