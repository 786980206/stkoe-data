//! Integration tests: create_table → read → update_table → compact_field
//! → delete_field, plus error-path tests.

use std::fs;
use std::sync::Arc;

use arrow_array::{
    Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use splayed_arrow::{create_meta, create_table, update_table};
use splayed_core::{
    compact_field, delete_field, open_dataset, FieldReader, UpdateError, UpdateItem,
};
use splayed_format::{Compression, DataType, RawValue};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
        Field::new("volume", ArrowDT::Int64, true),
    ]));

    // SYM01: days 0,1,2   close = 100.0, 101.0, 102.0   vol = 1000, 2000, 3000
    // SYM02: days 0,1,2   close = 200.0, NULL, 202.0     vol = NULL, 5000, 6000
    let time = Date32Array::from(vec![0, 1, 2, 0, 1, 2]);
    let sym = StringArray::from(vec![
        Some("SYM01"), Some("SYM01"), Some("SYM01"),
        Some("SYM02"), Some("SYM02"), Some("SYM02"),
    ]);
    let close = Float64Array::from(vec![
        Some(100.0), Some(101.0), Some(102.0),
        Some(200.0), None, Some(202.0),
    ]);
    let volume = Int64Array::from(vec![
        Some(1000), Some(2000), Some(3000),
        None, Some(5000), Some(6000),
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

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("splayed_test_{suffix}_{}", std::process::id()))
}

// ---------------------------------------------------------------------------
// Core roundtrip tests
// ---------------------------------------------------------------------------

#[test]
fn create_table_and_read_back() {
    let dir = temp_dir("crt");
    let _ = fs::remove_dir_all(&dir);

    let batch = make_batch();
    create_table(&dir, &batch, true).expect("create_table failed");

    // Verify .meta and field files exist.
    assert!(dir.join(".meta").exists(), ".meta file should exist");
    assert!(dir.join("close").exists(), "close field should exist");
    assert!(dir.join("volume").exists(), "volume field should exist");

    // Open dataset and verify structure.
    let dataset = open_dataset(&dir).expect("open_dataset failed");
    assert_eq!(dataset.meta.symbols, vec!["SYM01", "SYM02"]);
    assert_eq!(dataset.meta.sym_index.len(), 2);
    assert_eq!(dataset.meta.sym_index[0].time_count, 3);
    assert_eq!(dataset.meta.sym_index[0].row_start, 0);
    assert_eq!(dataset.meta.sym_index[1].time_count, 3);
    assert_eq!(dataset.meta.sym_index[1].row_start, 3);
    assert_eq!(dataset.meta.total_rows(), 6);

    // Read close field.
    let reader = FieldReader::open(dir.join("close")).expect("open close reader failed");
    assert_eq!(reader.data_type(), DataType::Float64);
    assert_eq!(reader.row_count(), 6);

    assert_eq!(reader.read_row(0).unwrap().as_f64(), Some(100.0));
    assert_eq!(reader.read_row(1).unwrap().as_f64(), Some(101.0));
    assert_eq!(reader.read_row(2).unwrap().as_f64(), Some(102.0));
    assert_eq!(reader.read_row(3).unwrap().as_f64(), Some(200.0));
    assert!(reader.read_row(4).unwrap().is_null());
    assert_eq!(reader.read_row(5).unwrap().as_f64(), Some(202.0));

    // Read volume field.
    let reader = FieldReader::open(dir.join("volume")).expect("open volume reader failed");
    assert_eq!(reader.data_type(), DataType::Int64);
    assert_eq!(reader.read_row(0).unwrap().as_i64(), Some(1000));
    assert_eq!(reader.read_row(1).unwrap().as_i64(), Some(2000));
    assert_eq!(reader.read_row(2).unwrap().as_i64(), Some(3000));
    assert!(reader.read_row(3).unwrap().is_null());
    assert_eq!(reader.read_row(4).unwrap().as_i64(), Some(5000));
    assert_eq!(reader.read_row(5).unwrap().as_i64(), Some(6000));

    let fields = dataset.list_fields().unwrap();
    assert_eq!(fields, vec!["close", "volume"]);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn update_table_modifies_values() {
    let dir = temp_dir("upd");
    let _ = fs::remove_dir_all(&dir);

    let batch = make_batch();
    create_table(&dir, &batch, true).expect("create_table failed");

    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    let update_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1, 2])),
            Arc::new(StringArray::from(vec![Some("SYM01"), Some("SYM01"), Some("SYM01")])),
            Arc::new(Float64Array::from(vec![Some(500.0), Some(501.0), Some(502.0)])),
        ],
    )
    .unwrap();

    update_table(&dir, &update_batch, true).expect("update_table failed");

    let reader = FieldReader::open(dir.join("close")).expect("open reader failed");
    assert_eq!(reader.read_row(0).unwrap().as_f64(), Some(500.0));
    assert_eq!(reader.read_row(1).unwrap().as_f64(), Some(501.0));
    assert_eq!(reader.read_row(2).unwrap().as_f64(), Some(502.0));
    // SYM02 unchanged.
    assert_eq!(reader.read_row(3).unwrap().as_f64(), Some(200.0));
    assert!(reader.read_row(4).unwrap().is_null());
    assert_eq!(reader.read_row(5).unwrap().as_f64(), Some(202.0));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn read_range_raw_is_zero_copy_slice() {
    let dir = temp_dir("range");
    let _ = fs::remove_dir_all(&dir);

    let batch = make_batch();
    create_table(&dir, &batch, true).expect("create_table failed");

    let reader = FieldReader::open(dir.join("close")).expect("open reader failed");
    let slice = reader.read_range_raw(0, 3).expect("read_range_raw failed");
    assert_eq!(slice.len(), 3 * 8);

    let v0 = RawValue::read_le(slice, 0, DataType::Float64);
    assert_eq!(v0.as_f64(), Some(100.0));
    let v1 = RawValue::read_le(slice, 8, DataType::Float64);
    assert_eq!(v1.as_f64(), Some(101.0));
    let v2 = RawValue::read_le(slice, 16, DataType::Float64);
    assert_eq!(v2.as_f64(), Some(102.0));

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// compact_field test
// ---------------------------------------------------------------------------

#[test]
fn compact_field_then_decompress() {
    let dir = temp_dir("compact");
    let _ = fs::remove_dir_all(&dir);

    let batch = make_batch();
    create_table(&dir, &batch, true).expect("create_table failed");

    let close_path = dir.join("close");

    // Verify it's writable (NONE) before compaction.
    {
        let reader = FieldReader::open(&close_path).unwrap();
        assert_eq!(reader.header().compression(), Ok(Compression::None));
        assert_eq!(reader.read_row(0).unwrap().as_f64(), Some(100.0));
    } // reader (mmap) dropped here — file can be written on Windows.

    // Compact with ZSTD.
    compact_field(&close_path, Compression::Zstd).expect("compact_field failed");

    // After compaction, header compression should be Zstd.
    // We can't use FieldReader's read_row on compressed data (it assumes NONE),
    // but we can decompress via the codec API.
    use splayed_core::decompress_field_data;
    let decompressed = decompress_field_data(&close_path).expect("decompress failed");

    // Verify values are intact after decompression.
    let v0 = RawValue::read_le(&decompressed, 0, DataType::Float64);
    assert_eq!(v0.as_f64(), Some(100.0));
    let v5 = RawValue::read_le(&decompressed, 5 * 8, DataType::Float64);
    assert_eq!(v5.as_f64(), Some(202.0));

    // Verify file structure (header + compressed payload).
    let compressed_size = fs::metadata(&close_path).unwrap().len();
    // Header(64) + 8 (uncompressed_len prefix) + compressed payload.
    assert!(compressed_size > HEADER_SIZE_BYTES);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn compact_field_lz4_then_decompress() {
    let dir = temp_dir("compact_lz4");
    let _ = fs::remove_dir_all(&dir);

    let batch = make_batch();
    create_table(&dir, &batch, true).expect("create_table failed");

    let close_path = dir.join("close");

    // Drop any reader before compacting (Windows mmap).
    {
        let _reader = FieldReader::open(&close_path).unwrap();
    }

    // Compact with LZ4.
    compact_field(&close_path, Compression::Lz4).expect("compact_field LZ4 failed");

    // Decompress and verify values.
    use splayed_core::decompress_field_data;
    let decompressed = decompress_field_data(&close_path).expect("decompress LZ4 failed");

    let v0 = RawValue::read_le(&decompressed, 0, DataType::Float64);
    assert_eq!(v0.as_f64(), Some(100.0));
    let v5 = RawValue::read_le(&decompressed, 5 * 8, DataType::Float64);
    assert_eq!(v5.as_f64(), Some(202.0));

    fs::remove_dir_all(&dir).ok();
}

const HEADER_SIZE_BYTES: u64 = 64;

// ---------------------------------------------------------------------------
// delete_field test
// ---------------------------------------------------------------------------

#[test]
fn delete_field_removes_file() {
    let dir = temp_dir("delete");
    let _ = fs::remove_dir_all(&dir);

    let batch = make_batch();
    create_table(&dir, &batch, true).expect("create_table failed");

    assert!(dir.join("close").exists());
    assert!(dir.join("volume").exists());

    // Delete close.
    delete_field(dir.join("close")).expect("delete_field failed");
    assert!(!dir.join("close").exists(), "close should be deleted");
    assert!(dir.join("volume").exists(), "volume should still exist");

    // Delete again — should be no-op (not an error).
    delete_field(dir.join("close")).expect("delete_field idempotent");

    // Delete non-existent file — no-op.
    delete_field(dir.join("nonexistent")).expect("delete nonexistent no-op");

    // Verify remaining fields.
    let dataset = open_dataset(&dir).expect("open_dataset failed");
    let fields = dataset.list_fields().unwrap();
    assert_eq!(fields, vec!["volume"]);

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// create_meta standalone test
// ---------------------------------------------------------------------------

#[test]
fn create_meta_standalone() {
    let dir = temp_dir("metaonly");
    let _ = fs::remove_dir_all(&dir);

    // Batch with only TIME + SYM (no FIELD columns).
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![0, 1, 2, 0, 1])),
            Arc::new(StringArray::from(vec![
                Some("AAPL"), Some("AAPL"), Some("AAPL"),
                Some("MSFT"), Some("MSFT"),
            ])),
        ],
    )
    .unwrap();

    let meta = create_meta(&dir, &batch, true).expect("create_meta failed");

    assert!(dir.join(".meta").exists());
    assert_eq!(meta.symbols, vec!["AAPL", "MSFT"]);
    assert_eq!(meta.time_axis, vec![0, 1, 2]);
    // AAPL: days 0,1,2 → time_start=0, time_count=3
    assert_eq!(meta.sym_index[0].time_start, 0);
    assert_eq!(meta.sym_index[0].time_count, 3);
    // MSFT: days 0,1 → time_start=0, time_count=2
    assert_eq!(meta.sym_index[1].time_start, 0);
    assert_eq!(meta.sym_index[1].time_count, 2);
    assert_eq!(meta.total_rows(), 5);

    // Now we can create_field on this dataset.
    use splayed_core::create_field;
    create_field(dir.join("price"), DataType::Float64).expect("create_field after create_meta");
    assert!(dir.join("price").exists());

    // Verify it's all NULL.
    let reader = FieldReader::open(dir.join("price")).unwrap();
    assert_eq!(reader.row_count(), 5);
    for i in 0..5 {
        assert!(reader.read_row(i).unwrap().is_null(), "row {i} should be NULL");
    }

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Error path tests
// ---------------------------------------------------------------------------

#[test]
fn update_field_out_of_range_rejected() {
    let dir = temp_dir("oor");
    let _ = fs::remove_dir_all(&dir);

    let batch = make_batch();
    create_table(&dir, &batch, true).expect("create_table failed");

    let close_path = dir.join("close");
    // total_rows = 6 (3 SYM01 + 3 SYM02)

    // Try writing past the end: start_row=5, 2 values → [5,7) exceeds 6.
    let item = UpdateItem::new(5, vec![0u8; 16]); // 2 Float64 values
    let result = splayed_core::update_field(&close_path, &[item]);
    assert!(matches!(result, Err(UpdateError::OutOfRange { .. })));

    // Valid: start_row=5, 1 value → [5,6) is within bounds.
    let item = UpdateItem::new(5, vec![0u8; 8]); // 1 Float64
    let result = splayed_core::update_field(&close_path, &[item]);
    assert!(result.is_ok());

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn update_after_compact_rejected() {
    let dir = temp_dir("ro_after_compact");
    let _ = fs::remove_dir_all(&dir);

    let batch = make_batch();
    create_table(&dir, &batch, true).expect("create_table failed");

    let close_path = dir.join("close");

    // Compact → read-only.
    compact_field(&close_path, Compression::Zstd).expect("compact_field failed");

    // Attempt to update should be rejected.
    let item = UpdateItem::new(0, vec![0u8; 8]);
    let result = splayed_core::update_field(&close_path, &[item]);
    assert!(
        matches!(result, Err(UpdateError::ReadOnlyAfterCompress)),
        "expected ReadOnlyAfterCompress, got: {result:?}"
    );

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn read_row_out_of_range_rejected() {
    let dir = temp_dir("read_oor");
    let _ = fs::remove_dir_all(&dir);

    let batch = make_batch();
    create_table(&dir, &batch, true).expect("create_table failed");

    let reader = FieldReader::open(dir.join("close")).unwrap();
    // row_count = 6, row 6 is out of range.
    let result = reader.read_row(6);
    assert!(result.is_err());

    // read_range_raw with too many rows.
    let result = reader.read_range_raw(0, 7);
    assert!(result.is_err());

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn unsorted_input_produces_correct_meta() {
    let dir = temp_dir("unsorted");
    let _ = fs::remove_dir_all(&dir);

    // Deliberately unsorted input.
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![2, 0, 1, 0])),  // unsorted time
            Arc::new(StringArray::from(vec![Some("MSFT"), Some("AAPL"), Some("AAPL"), Some("MSFT")])),  // unsorted sym
            Arc::new(Float64Array::from(vec![Some(302.0), Some(100.0), Some(101.0), Some(300.0)])),
        ],
    )
    .unwrap();

    // sorted=false → builder will sort internally.
    create_table(&dir, &batch, false).expect("create_table with unsorted input");

    let dataset = open_dataset(&dir).expect("open_dataset failed");
    // Symbols should be sorted.
    assert_eq!(dataset.meta.symbols, vec!["AAPL", "MSFT"]);

    // AAPL: days 0,1 → time_start=0, time_count=2
    assert_eq!(dataset.meta.sym_index[0].time_start, 0);
    assert_eq!(dataset.meta.sym_index[0].time_count, 2);
    // MSFT: days 0,2 → time_start=0, time_count=3 (interval [0,2])
    assert_eq!(dataset.meta.sym_index[1].time_start, 0);
    assert_eq!(dataset.meta.sym_index[1].time_count, 3);

    // Verify values landed in correct rows.
    // AAPL row 0 = day 0 = 100.0, AAPL row 1 = day 1 = 101.0
    // MSFT row 2 = day 0 = 300.0, MSFT row 3 = day 1 = NULL, MSFT row 4 = day 2 = 302.0
    let reader = FieldReader::open(dir.join("close")).unwrap();
    assert_eq!(reader.read_row(0).unwrap().as_f64(), Some(100.0)); // AAPL day 0
    assert_eq!(reader.read_row(1).unwrap().as_f64(), Some(101.0)); // AAPL day 1
    assert_eq!(reader.read_row(2).unwrap().as_f64(), Some(300.0)); // MSFT day 0
    assert!(reader.read_row(3).unwrap().is_null());                 // MSFT day 1 (NULL)
    assert_eq!(reader.read_row(4).unwrap().as_f64(), Some(302.0)); // MSFT day 2

    fs::remove_dir_all(&dir).ok();
}
