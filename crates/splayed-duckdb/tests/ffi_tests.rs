//! C ABI（ffi 模块）集成测试：从 Rust 测试内直接调用 `extern "C"` 函数，
//! 验证句柄生命周期、schema、扫描列视图、有效性位图与字典取值契约。

use std::ffi::{c_char, c_void, CString};
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray as ArrowStringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use splayed_arrow::create_table;
use splayed_duckdb::ffi::{
    SPLAYED_TYPE_STRING_DICT, SplayedBatchFFI, splayed_dataset_close, splayed_dataset_open,
    splayed_dataset_schema_count, splayed_dataset_schema_field, splayed_last_error,
    splayed_scan_close, splayed_scan_dict_value, splayed_scan_next, splayed_scan_open,
};

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splayed_ffi_{suffix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn make_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("time", ArrowDT::Date32, false),
            Field::new("sym", ArrowDT::Utf8, false),
            Field::new("close", ArrowDT::Float64, true),
            Field::new("vol", ArrowDT::Int64, true),
        ])),
        vec![
            Arc::new(arrow_array::Date32Array::from(vec![0, 1, 2, 0, 1, 2])),
            Arc::new(ArrowStringArray::from(vec![
                "SYM01", "SYM01", "SYM01", "SYM02", "SYM02", "SYM02",
            ])),
            Arc::new(Float64Array::from(vec![
                100.0, 101.0, 102.0, 200.0, 201.0, 202.0,
            ])),
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4), Some(5), Some(6)])),
        ],
    )
    .unwrap()
}

fn cpath(p: &std::path::Path) -> CString {
    CString::new(p.to_string_lossy().as_bytes()).unwrap()
}

unsafe fn err_str() -> String {
    let ptr = splayed_last_error();
    if ptr.is_null() {
        return String::new();
    }
    std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
}

#[test]
fn open_schema_and_scan_via_c_abi() {
    let dir = temp_dir("abi");
    create_table(&dir, &make_batch(), true).unwrap();

    let dir_c = cpath(&dir);
    let handle = unsafe { splayed_dataset_open(dir_c.as_ptr(), dir_c.as_bytes().len() as u32) };
    assert!(!handle.is_null(), "open failed: {}", unsafe { err_str() });

    // Schema: time + sym + close + vol = 4.
    let n = unsafe { splayed_dataset_schema_count(handle) };
    assert_eq!(n, 4);

    let mut name: *const c_char = std::ptr::null();
    let mut name_len: u32 = 0;
    let mut ty: u8 = 0;
    let mut nullable: u8 = 1;
    let rc = unsafe {
        splayed_dataset_schema_field(handle, 1, &mut name, &mut name_len, &mut ty, &mut nullable)
    };
    assert_eq!(rc, 0);
    assert_eq!(
        unsafe { std::slice::from_raw_parts(name as *const u8, name_len as usize) },
        b"sym"
    );
    assert_eq!(ty, SPLAYED_TYPE_STRING_DICT);
    assert_eq!(nullable, 0);

    // Scan close + vol (batch columns = time, sym, close, vol).
    let cols: Vec<CString> = ["close", "vol"]
        .iter()
        .map(|s| CString::new(*s).unwrap())
        .collect();
    let col_ptrs: Vec<*const c_char> = cols.iter().map(|c| c.as_ptr()).collect();
    let col_lens: Vec<u32> = cols.iter().map(|c| c.as_bytes().len() as u32).collect();
    let scan = unsafe {
        splayed_scan_open(
            handle,
            col_ptrs.as_ptr(),
            col_lens.as_ptr(),
            cols.len() as i32,
            4, // batch_rows
            2, // parallelism
        )
    };
    assert!(!scan.is_null(), "scan open failed: {}", unsafe { err_str() });

    let mut batch = SplayedBatchFFI {
        row_count: 0,
        column_count: 0,
        system_columns: 0,
        columns: std::ptr::null(),
    };
    let mut total = 0usize;
    let mut null_vols = 0usize;
    let mut first_close_seen = false;
    let mut first_close = 0.0f64;
    loop {
        let rc = unsafe { splayed_scan_next(scan, &mut batch) };
        assert!(rc >= 0, "scan_next: {}", unsafe { err_str() });
        if rc == 0 {
            break;
        }
        assert_eq!(batch.column_count, 4);
        assert_eq!(batch.system_columns, 2);
        total += batch.row_count as usize;
        let views =
            unsafe { std::slice::from_raw_parts(batch.columns, batch.column_count as usize) };
        // close = column 2（Float64 LE）；vol = column 3（Int64 LE，含 NULL）。
        let close = &views[2];
        assert_eq!(close.data_len as usize, batch.row_count as usize * 8);
        let v = unsafe { std::ptr::read_unaligned(close.data as *const f64) };
        if !first_close_seen {
            first_close = v;
            first_close_seen = true;
        }
        let vol = &views[3];
        if !vol.validity.is_null() {
            let bits =
                unsafe { std::slice::from_raw_parts(vol.validity, vol.validity_len as usize) };
            for i in 0..batch.row_count as usize {
                if (bits[i / 8] >> (i % 8)) & 1 == 0 {
                    null_vols += 1;
                }
            }
        }
    }
    assert_eq!(total, 6);
    assert_eq!(null_vols, 1); // vol 恰一个 NULL
    assert_eq!(first_close, 100.0);

    // 字典列（sym）取值：第 1 列、0 号字符串 = "SYM01"。
    let mut sptr: *const c_char = std::ptr::null();
    let mut slen: u32 = 0;
    let rc = unsafe { splayed_scan_dict_value(scan, 1, 0, &mut sptr, &mut slen) };
    assert_eq!(rc, 0);
    assert_eq!(
        unsafe { std::slice::from_raw_parts(sptr as *const u8, slen as usize) },
        b"SYM01"
    );

    unsafe { splayed_scan_close(scan) };
    unsafe { splayed_dataset_close(handle) };

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn open_missing_dataset_reports_error() {
    let p = cpath(std::path::Path::new("no_such_dir_xyz"));
    let handle = unsafe { splayed_dataset_open(p.as_ptr(), p.as_bytes().len() as u32) };
    assert!(handle.is_null());
    assert!(!unsafe { err_str() }.is_empty());
}

// 保证 ffi 结构体在测试中也被引用（repr(C) 布局在集成层验证）。
#[allow(dead_code)]
fn _layout_reference() {
    let _ = std::mem::size_of::<splayed_duckdb::ffi::SplayedColumnFFI>();
    let _: *mut c_void = std::ptr::null_mut();
}