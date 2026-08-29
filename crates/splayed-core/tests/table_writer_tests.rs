//! Native (non-Arrow) table-writer integration tests: `splayed_core::create_meta`
//! / `create_table` / `update_table` + `create_field_with_data`.
//!
//! These prove the exchange-layer contract: raw byte columns in input-row order
//! are scattered to global rows by the core engine and written in one pass.

use std::fs;
use std::path::PathBuf;

use splayed_core::{
    create_meta, create_table, update_table, FieldReader, Scanner, ScanRequest, TableColumn,
    TableError,
};
use splayed_format::{DataType, RawValue, TimeType};

fn temp_dir(suffix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("splayed_core_tw_{suffix}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// Build a native column's raw LE bytes from `RawValue`s.
fn native_column(cols: &[(&str, DataType, Vec<RawValue>)]) -> Vec<TableColumn> {
    let n = cols[0].2.len();
    cols.iter()
        .map(|(name, ty, vals)| {
            let elem_sz = ty.size_of();
            let mut values = vec![0u8; n * elem_sz];
            for (i, v) in vals.iter().enumerate() {
                v.write_le(&mut values, i * elem_sz);
            }
            TableColumn {
                name: name.to_string(),
                data_type: *ty,
                values,
            }
        })
        .collect()
}

fn syms_of(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// Read a single row's value back through the reader.
fn read_row(dir: &PathBuf, field: &str, row: u32) -> RawValue {
    let reader = FieldReader::open(dir.join(field)).unwrap();
    reader.read_row(row).unwrap()
}

// ---------------------------------------------------------------------------
// create_meta / create_field_with_data
// ---------------------------------------------------------------------------

#[test]
fn create_meta_then_field_with_data() {
    let dir = temp_dir("meta_field");
    let meta = create_meta(&dir, TimeType::Date32, &syms_of(&["SYM01", "SYM01", "SYM02"]), &[0, 1, 0])
        .expect("create_meta failed");
    assert_eq!(meta.total_rows(), 3); // SYM01: time 0-1 (2 rows) + SYM02: time 0 (1 row)

    // One-pass field write covering exactly total_rows.
    let mut vals = vec![0u8; 3 * 8];
    RawValue::from_f64(100.0).write_le(&mut vals, 0);
    RawValue::from_f64(101.0).write_le(&mut vals, 8);
    RawValue::from_f64(200.0).write_le(&mut vals, 16);

    splayed_core::create_field_with_data(dir.join("close"), DataType::Float64, &vals)
        .expect("create_field_with_data failed");

    assert_eq!(read_row(&dir, "close", 0).as_f64(), Some(100.0));
    assert_eq!(read_row(&dir, "close", 1).as_f64(), Some(101.0));
    assert_eq!(read_row(&dir, "close", 2).as_f64(), Some(200.0));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn create_field_with_data_length_mismatch() {
    let dir = temp_dir("len_mismatch");
    create_meta(&dir, TimeType::Date32, &syms_of(&["SYM01"]), &[0]).unwrap();

    // 2 values but meta declares 1 row → must error.
    let mut vals = vec![0u8; 16];
    RawValue::from_f64(1.0).write_le(&mut vals, 0);
    RawValue::from_f64(2.0).write_le(&mut vals, 8);
    let err = splayed_core::create_field_with_data(dir.join("close"), DataType::Float64, &vals);
    assert!(matches!(err, Err(splayed_core::CreateFieldError::LengthMismatch { .. })));

    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// create_table (one-pass) + scanner read-back
// ---------------------------------------------------------------------------

#[test]
fn create_table_writes_fields_in_one_pass() {
    let dir = temp_dir("create_table");
    let columns = native_column(&[
        ("close", DataType::Float64, vec![
            RawValue::from_f64(100.0),
            RawValue::from_f64(101.0),
            RawValue::from_f64(102.0), // SYM01, day 2
            RawValue::from_f64(200.0),
            RawValue::from_f64(201.0), // SYM02, days 0-1
        ]),
        ("volume", DataType::Int64, vec![
            RawValue::from_i64(1000),
            RawValue::from_i64(1001),
            RawValue::from_i64(1002),
            RawValue::from_i64(2000),
            RawValue::from_i64(2001),
        ]),
    ]);

    // Input order deliberately NOT (sym,time)-sorted → engine must scatter.
    // Input rows: (SYM01,0)→g0, (SYM02,0)→g3, (SYM01,1)→g1, (SYM02,1)→g4, (SYM01,2)→g2
    let meta = create_table(
        &dir,
        TimeType::Date32,
        &syms_of(&["SYM01", "SYM02", "SYM01", "SYM02", "SYM01"]),
        &[0, 0, 1, 1, 2],
        &columns,
        false,
    )
    .expect("create_table failed");
    assert_eq!(meta.total_rows(), 5);

    // Global row layout: SYM01 (rows 0-2: time 0,1,2), SYM02 (rows 3-4: time 0,1).
    // close input = [100(i0), 101(i1), 102(i2), 200(i3), 201(i4)]
    assert_eq!(read_row(&dir, "close", 0).as_f64(), Some(100.0)); // g0 ← i0
    assert_eq!(read_row(&dir, "close", 1).as_f64(), Some(102.0)); // g1 ← i2
    assert_eq!(read_row(&dir, "close", 2).as_f64(), Some(201.0)); // g2 ← i4
    assert_eq!(read_row(&dir, "close", 3).as_f64(), Some(101.0)); // g3 ← i1
    assert_eq!(read_row(&dir, "close", 4).as_f64(), Some(200.0)); // g4 ← i3
    // volume input = [1000(i0), 1001(i1), 1002(i2), 2000(i3), 2001(i4)]
    assert_eq!(read_row(&dir, "volume", 2).as_i64(), Some(2001)); // g2 ← i4
    assert_eq!(read_row(&dir, "volume", 4).as_i64(), Some(2000)); // g4 ← i3

    // Scanner read-back (native, no Arrow).
    let dataset = splayed_core::open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);
    let request = ScanRequest::new(vec!["close".to_string(), "volume".to_string()]);
    let plan = scanner.plan(&request).unwrap();
    let mut batches = scanner.scan(&plan, &request).unwrap();
    let batch = batches.next_batch().unwrap().expect("one batch");
    assert_eq!(batch.row_count, 5);
    let close_view = batch.column_view("close").unwrap();
    assert_eq!(close_view.get(0).unwrap().as_f64(), Some(100.0));
    assert_eq!(close_view.get(4).unwrap().as_f64(), Some(200.0));

    fs::remove_dir_all(&dir).ok();
}

/// `sorted=true` on genuinely sorted input takes the fast path (window-cursor
/// scatter) and must produce the same layout as the general path.
#[test]
fn create_table_sorted_fast_path() {
    let dir = temp_dir("sorted_fast");
    let columns = native_column(&[
        ("close", DataType::Float64, vec![
            RawValue::from_f64(100.0),
            RawValue::from_f64(101.0),
            RawValue::from_f64(200.0),
            RawValue::from_f64(201.0),
        ]),
        ("volume", DataType::Int64, vec![
            RawValue::from_i64(1000),
            RawValue::from_i64(1001),
            RawValue::from_i64(2000),
            RawValue::from_i64(2001),
        ]),
    ]);
    // Already (SYM, TIME) ascending: SYM01 t0,t1 then SYM02 t0,t1.
    let meta = create_table(
        &dir,
        TimeType::Date32,
        &syms_of(&["SYM01", "SYM01", "SYM02", "SYM02"]),
        &[0, 1, 0, 1],
        &columns,
        true,
    )
    .expect("sorted create_table failed");
    assert_eq!(meta.total_rows(), 4);

    // Global rows are identity here (sorted input): g0..g3 map 1:1.
    assert_eq!(read_row(&dir, "close", 0).as_f64(), Some(100.0));
    assert_eq!(read_row(&dir, "close", 1).as_f64(), Some(101.0));
    assert_eq!(read_row(&dir, "close", 2).as_f64(), Some(200.0));
    assert_eq!(read_row(&dir, "close", 3).as_f64(), Some(201.0));
    assert_eq!(read_row(&dir, "volume", 3).as_i64(), Some(2001));

    fs::remove_dir_all(&dir).ok();
}

/// `sorted=true` with a lie (actually unsorted input) must fall back to the
/// general scatter — results stay correct, never corrupted.
#[test]
fn create_table_sorted_hint_unsorted_input_falls_back() {
    let dir = temp_dir("sorted_lie");
    let columns = native_column(&[("close", DataType::Float64, vec![
        RawValue::from_f64(100.0),
        RawValue::from_f64(101.0),
        RawValue::from_f64(200.0),
    ])]);
    // Claim sorted=true but row 2 is (SYM02, t0) after (SYM01, t1): not sorted.
    let meta = create_table(
        &dir,
        TimeType::Date32,
        &syms_of(&["SYM01", "SYM01", "SYM02"]),
        &[0, 1, 0],
        &columns,
        true,
    )
    .expect("lied sorted input still works");
    assert_eq!(meta.total_rows(), 3);
    // Correct global layout regardless of the false hint.
    assert_eq!(read_row(&dir, "close", 0).as_f64(), Some(100.0)); // SYM01 t0
    assert_eq!(read_row(&dir, "close", 1).as_f64(), Some(101.0)); // SYM01 t1
    assert_eq!(read_row(&dir, "close", 2).as_f64(), Some(200.0)); // SYM02 t0

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn create_table_nonempty_dir_errors() {
    let dir = temp_dir("nonempty");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("stray.txt"), "x").unwrap();

    let columns = native_column(&[("close", DataType::Float64, vec![RawValue::from_f64(1.0)])]);
    let err = create_table(
        &dir,
        TimeType::Date32,
        &syms_of(&["SYM01"]),
        &[0],
        &columns,
        false,
    );
    assert!(matches!(err, Err(TableError::DirNotEmpty)));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn create_table_length_mismatch_errors() {
    let dir = temp_dir("tw_len");
    let columns = native_column(&[(
        "close",
        DataType::Float64,
        vec![RawValue::from_f64(1.0), RawValue::from_f64(2.0)], // 2 vals vs 1 row
    )]);
    let err = create_table(&dir, TimeType::Date32, &syms_of(&["SYM01"]), &[0], &columns, false);
    assert!(matches!(
        err,
        Err(TableError::LengthMismatch { field, .. }) if field == "close"
    ));
    fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// update_table
// ---------------------------------------------------------------------------

#[test]
fn update_table_modifies_existing_cells() {
    let dir = temp_dir("upd");
    let columns = native_column(&[("close", DataType::Float64, vec![RawValue::from_f64(100.0)])]);
    create_table(&dir, TimeType::Date32, &syms_of(&["SYM01"]), &[0], &columns, false).unwrap();

    // Update SYM01@0 close → 999.
    let upd = native_column(&[("close", DataType::Float64, vec![RawValue::from_f64(999.0)])]);
    update_table(&dir, &syms_of(&["SYM01"]), &[0], &upd, false).expect("update_table failed");
    assert_eq!(read_row(&dir, "close", 0).as_f64(), Some(999.0));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn update_table_unknown_sym_time_errors() {
    let dir = temp_dir("upd_unknown");
    let columns = native_column(&[("close", DataType::Float64, vec![RawValue::from_f64(100.0)])]);
    create_table(&dir, TimeType::Date32, &syms_of(&["SYM01"]), &[0], &columns, false).unwrap();

    // SYM01@99 doesn't exist in META → error (no silent expansion).
    let upd = native_column(&[("close", DataType::Float64, vec![RawValue::from_f64(1.0)])]);
    let err = update_table(&dir, &syms_of(&["SYM01"]), &[99], &upd, false);
    assert!(matches!(err, Err(TableError::SymTimeNotFound { row: 0 })));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn update_table_missing_field_errors_by_default() {
    let dir = temp_dir("upd_field");
    let columns = native_column(&[("close", DataType::Float64, vec![RawValue::from_f64(100.0)])]);
    create_table(&dir, TimeType::Date32, &syms_of(&["SYM01"]), &[0], &columns, false).unwrap();

    // Default: field missing → error (protects against typos).
    let upd = native_column(&[("volume", DataType::Int64, vec![RawValue::from_i64(7)])]);
    let err = update_table(&dir, &syms_of(&["SYM01"]), &[0], &upd, false);
    assert!(matches!(err, Err(TableError::FieldNotFound(name)) if name == "volume"));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn update_table_auto_creates_missing_field() {
    let dir = temp_dir("upd_auto");
    let columns = native_column(&[("close", DataType::Float64, vec![
        RawValue::from_f64(100.0),
        RawValue::from_f64(101.0),
    ])]);
    // SYM01 with times 0,1 (2 global rows).
    create_table(&dir, TimeType::Date32, &syms_of(&["SYM01", "SYM01"]), &[0, 1], &columns, true)
        .unwrap();

    // New field "volume": only the row for SYM01@0 is filled; SYM01@1 stays NULL.
    let upd = native_column(&[("volume", DataType::Int64, vec![RawValue::from_i64(999)])]);
    update_table(&dir, &syms_of(&["SYM01"]), &[0], &upd, true).expect("auto-create failed");

    let reader = FieldReader::open(dir.join("volume")).unwrap();
    assert_eq!(reader.read_row(0).unwrap().as_i64(), Some(999));
    assert!(reader.read_row(1).unwrap().is_null());

    // Existing field still updated fine alongside the new one.
    let upd2 = native_column(&[
        ("volume", DataType::Int64, vec![RawValue::from_i64(1000)]),
        ("close", DataType::Float64, vec![RawValue::from_f64(50.0)]),
    ]);
    update_table(&dir, &syms_of(&["SYM01"]), &[1], &upd2, true).expect("update failed");
    let reader = FieldReader::open(dir.join("volume")).unwrap();
    assert_eq!(reader.read_row(0).unwrap().as_i64(), Some(999));
    assert_eq!(reader.read_row(1).unwrap().as_i64(), Some(1000));
    assert_eq!(read_row(&dir, "close", 1).as_f64(), Some(50.0));

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn update_table_type_mismatch_errors() {
    let dir = temp_dir("upd_type");
    let columns = native_column(&[("close", DataType::Float64, vec![RawValue::from_f64(100.0)])]);
    create_table(&dir, TimeType::Date32, &syms_of(&["SYM01"]), &[0], &columns, false).unwrap();

    // Same field name but Int64 → rejected.
    let upd = native_column(&[("close", DataType::Int64, vec![RawValue::from_i64(7)])]);
    let err = update_table(&dir, &syms_of(&["SYM01"]), &[0], &upd, false);
    assert!(matches!(err, Err(TableError::TypeMismatch { field, .. }) if field == "close"));

    fs::remove_dir_all(&dir).ok();
}