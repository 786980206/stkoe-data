//! CoreBatch → Arrow adapter tests: zero-copy pointer guarantee, NULL validity
//! behavior, and the extended fixed-width types.

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int16Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType as ArrowDT, Field};
use splayed_arrow::{corebatch_into_record_batch, create_table};
use splayed_core::{
    Buffer, CoreBatch, CoreColumn, CoreField, CoreSchema, CoreStringDict, CoreType, Scanner,
    ScanRequest,
};

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splayed_cb_{suffix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn le<T: Copy>(vals: &[T]) -> Vec<u8> {
    // Only used for fixed-size Copy types; byte width comes from size_of.
    unsafe {
        std::slice::from_raw_parts(vals.as_ptr() as *const u8, std::mem::size_of_val(vals))
            .to_vec()
    }
}

#[test]
fn into_record_batch_transfers_buffers_zero_copy() {
    let schema = Arc::new(CoreSchema::new(vec![
        CoreField::new("time", CoreType::Date32, false),
        CoreField::new("sym", CoreType::Utf8, false),
        CoreField::new("close", CoreType::Float64, true),
    ]));
    let time = CoreColumn::primitive(
        CoreType::Date32,
        Buffer::from_vec(le(&[0i32, 1, 2])),
        None,
    );
    let sym = CoreColumn::dictionary(
        Buffer::from_vec(le(&[0u32, 0u32, 0u32])),
        Arc::new(CoreStringDict::new(vec!["SYM01".to_string()], true)),
    );
    let data = le(&[100.0f64, 101.0, 102.0]);
    let data_ptr = data.as_ptr() as usize;
    let close = CoreColumn::primitive(CoreType::Float64, Buffer::from_vec(data), None);
    let batch = CoreBatch::new(schema, vec![time, sym, close], 3);

    let rb = corebatch_into_record_batch(
        batch,
        &[2],
        &[Field::new("close", ArrowDT::Float64, true)],
        None,
    )
    .unwrap();
    let arr = rb
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    // The Arrow buffer must BE the original Vec — no copying.
    assert_eq!(arr.values().as_ptr() as usize, data_ptr, "data was copied");
    assert_eq!(arr.value(0), 100.0);
    assert_eq!(arr.value(2), 102.0);
}

#[test]
fn sym_dictionary_unwraps_to_utf8_with_schema_match() {
    let dir = temp_dir("dict");
    // schema via create_table: time, sym (Utf8), close
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Date32Array::from(vec![0, 1])),
            Arc::new(arrow_array::StringArray::from(vec!["SYM01", "SYM02"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        ],
    )
    .unwrap();
    create_table(&dir, &batch, true).unwrap();

    let dataset = splayed_core::open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);
    let req = ScanRequest::new(vec!["close".to_string()]);
    let plan = scanner.plan(&req).unwrap();
    let mut it = scanner.scan(&plan, &req).unwrap();
    let cb = it.next_batch().unwrap().unwrap();

    let rb = corebatch_into_record_batch(
        cb,
        &[0, 1, 2],
        &[
            Field::new("time", ArrowDT::Date32, false),
            Field::new("sym", ArrowDT::Utf8, false),
            Field::new("close", ArrowDT::Float64, true),
        ],
        None,
    )
    .unwrap();
    let sym = rb
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(sym.value(0), "SYM01");
    assert_eq!(sym.value(1), "SYM02");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn extended_types_roundtrip_through_arrow() {
    let dir = temp_dir("ext_types");
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            Field::new("time", ArrowDT::Date32, false),
            Field::new("sym", ArrowDT::Utf8, false),
            Field::new("i16", ArrowDT::Int16, true),
            Field::new("u32", ArrowDT::UInt32, true),
            Field::new("date64", ArrowDT::Date64, true),
            Field::new("count", ArrowDT::UInt64, false),
        ])),
        vec![
            Arc::new(arrow_array::Date32Array::from(vec![0, 1])),
            Arc::new(arrow_array::StringArray::from(vec!["SYM01", "SYM01"])),
            Arc::new(arrow_array::Int16Array::from(vec![Some(7), None])),
            Arc::new(arrow_array::UInt32Array::from(vec![Some(10), Some(20)])),
            Arc::new(arrow_array::Date64Array::from(vec![Some(86400000), Some(172800000)])),
            Arc::new(arrow_array::UInt64Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    create_table(&dir, &batch, true).unwrap();

    let dataset = splayed_core::open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);
    let req = ScanRequest::new(vec![
        "i16".to_string(),
        "u32".to_string(),
        "date64".to_string(),
        "count".to_string(),
    ]);
    let plan = scanner.plan(&req).unwrap();
    let mut it = scanner.scan(&plan, &req).unwrap();
    let cb = it.next_batch().unwrap().unwrap();

    let rb = corebatch_into_record_batch(
        cb,
        &[2, 3, 4, 5],
        &[
            Field::new("i16", ArrowDT::Int16, true),
            Field::new("u32", ArrowDT::UInt32, true),
            Field::new("date64", ArrowDT::Date64, true),
            Field::new("count", ArrowDT::UInt64, false),
        ],
        None,
    )
    .unwrap();

    let i16 = rb.column(0).as_any().downcast_ref::<Int16Array>().unwrap();
    assert!(i16.is_null(1));
    assert_eq!(i16.value(0), 7);

    let u32 = rb.column(1).as_any().downcast_ref::<UInt32Array>().unwrap();
    assert_eq!(u32.value(1), 20);

    let count = rb
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .unwrap();
    assert_eq!(count.value(0), 100);
    assert_eq!(count.value(1), 200);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn null_column_validity_and_zero_null_fast_path() {
    let dir = temp_dir("validity");
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            Field::new("time", ArrowDT::Date32, false),
            Field::new("sym", ArrowDT::Utf8, false),
            Field::new("a", ArrowDT::Int32, true), // NULL present
            Field::new("b", ArrowDT::Int32, true), // no NULLs
        ])),
        vec![
            Arc::new(arrow_array::Date32Array::from(vec![0, 1, 2])),
            Arc::new(arrow_array::StringArray::from(vec!["SYM01", "SYM01", "SYM01"])),
            Arc::new(arrow_array::Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(arrow_array::Int32Array::from(vec![Some(10), Some(11), Some(12)])),
        ],
    )
    .unwrap();
    create_table(&dir, &batch, true).unwrap();

    let dataset = splayed_core::open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);
    let req = ScanRequest::new(vec!["a".to_string(), "b".to_string()]);
    let plan = scanner.plan(&req).unwrap();
    let mut it = scanner.scan(&plan, &req).unwrap();
    let cb = it.next_batch().unwrap().unwrap();

    // Column a has NULLs → bitmap present, exactly one null at row 1.
    let a = cb.column(2);
    let va = a.validity().expect("a should have a validity bitmap");
    assert!(va.is_valid(0) && !va.is_valid(1) && va.is_valid(2));

    // Column b has no NULLs → header null_count == 0 → no bitmap (fast path).
    let b = cb.column(3);
    assert!(b.validity().is_none(), "zero-null column must skip the bitmap");

    std::fs::remove_dir_all(&dir).ok();
}