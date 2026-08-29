//! splayed-adbc 集成测试：CoreBatch 流 / 零拷贝 Arrow 流 / 过滤 / limit / 并行。

use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray as ArrowStringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use splayed_arrow::create_table;
use splayed_adbc::Connection;

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splayed_adbc_{suffix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// time 0..4 × SYM01/02，close 100..104 / 200..204，vol 1000.. / 2000..
fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", ArrowDT::Date32, false),
        Field::new("sym", ArrowDT::Utf8, false),
        Field::new("close", ArrowDT::Float64, true),
        Field::new("vol", ArrowDT::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Date32Array::from(vec![
                0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
            ])),
            Arc::new(ArrowStringArray::from(vec![
                "SYM01", "SYM01", "SYM01", "SYM01", "SYM01", "SYM02", "SYM02", "SYM02", "SYM02",
                "SYM02",
            ])),
            Arc::new(Float64Array::from(vec![
                100.0, 101.0, 102.0, 103.0, 104.0, 200.0, 201.0, 202.0, 203.0, 204.0,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1000),
                Some(1001),
                None,
                Some(1003),
                Some(1004),
                Some(2000),
                Some(2001),
                Some(2002),
                Some(2003),
                Some(2004),
            ])),
        ],
    )
    .unwrap()
}

#[test]
fn core_stream_full_and_filtered() {
    let dir = temp_dir("core");
    create_table(&dir, &make_batch(), true).unwrap();
    let conn = Connection::open(&dir).unwrap();

    // Full scan: 10 rows.
    let full = conn.statement().execute().unwrap();
    let rows: usize = full.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, 10);

    // Filter close > 150: SYM02 (200..204) → 5 rows.
    let filtered = conn
        .statement()
        .select_all()
        .filter(splayed_core::Filter::GreaterThan {
            field: "close".into(),
            value: splayed_core::FilterValue::Float64(150.0),
        })
        .execute()
        .unwrap();
    let rows: usize = filtered.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(rows, 5);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn arrow_stream_values_and_nulls() {
    let dir = temp_dir("arrow");
    create_table(&dir, &make_batch(), true).unwrap();
    let conn = Connection::open(&dir).unwrap();

    let stream = conn
        .statement()
        .select_all()
        .execute_arrow()
        .unwrap();

    let mut total = 0usize;
    let mut null_vols = 0usize;
    for rb in stream {
        let rb = rb.unwrap();
        total += rb.num_rows();
        // vol column index 3; row 2 of SYM01 is NULL (global row 2).
        let vol = rb
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..vol.len() {
            if vol.is_null(i) {
                null_vols += 1;
            }
        }
        let sym = rb
            .column(1)
            .as_any()
            .downcast_ref::<ArrowStringArray>()
            .unwrap();
        assert_eq!(sym.value(0), "SYM01");
    }
    assert_eq!(total, 10);
    assert_eq!(null_vols, 1); // exactly the one NULL cell

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn limit_truncates_in_batch() {
    let dir = temp_dir("limit");
    create_table(&dir, &make_batch(), true).unwrap();
    let conn = Connection::open(&dir).unwrap();

    let stream = conn.statement().select_all().limit(3).execute_arrow().unwrap();
    let total: usize = stream.filter_map(|rb| rb.ok().map(|b| b.num_rows())).sum();
    assert_eq!(total, 3);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn parallelism_keeps_order_and_count() {
    let dir = temp_dir("par");
    create_table(&dir, &make_batch(), true).unwrap();
    let conn = Connection::open(&dir).unwrap();

    let serial: Vec<i64> = conn
        .statement()
        .select_all()
        .execute_arrow()
        .unwrap()
        .filter_map(|rb| rb.ok())
        .flat_map(|rb| {
            rb.column(0)
                .as_any()
                .downcast_ref::<arrow_array::Date32Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap() as i64)
                .collect::<Vec<_>>()
        })
        .collect();

    let parallel: Vec<i64> = conn
        .statement()
        .select_all()
        .parallelism(4)
        .execute_arrow()
        .unwrap()
        .filter_map(|rb| rb.ok())
        .flat_map(|rb| {
            rb.column(0)
                .as_any()
                .downcast_ref::<arrow_array::Date32Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap() as i64)
                .collect::<Vec<_>>()
        })
        .collect();

    assert_eq!(serial, parallel);

    std::fs::remove_dir_all(&dir).ok();
}