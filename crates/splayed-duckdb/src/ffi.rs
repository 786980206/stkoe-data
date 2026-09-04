//! DuckDB 扩展的 C ABI（`splayed-duckdb` → 扩展壳）。
//!
//! 这是 Rust 集成层对 DuckDB 扩展壳（C++ 工程）暴露的契约：**零 Arrow**、
//! 句柄即裸指针（`Box::into_raw`，谁创建谁释放——每个对象都有 `*_close`）、
//! 错误通过返回码 + 线程本地错误消息（`splayed_last_error`，Rust 持有、
//! 直到下一次 splayed 调用前有效）。
//!
//! 批次视图（`splayed_scan_next` 返回的列视图）**借用 Rust 侧的当前批**，
//! 仅在**下一次** `splayed_scan_next` / `splayed_scan_close` 之前有效——
//! 扩展壳应在本批次消费完（写入 DataChunk）后再取下一批。
//!
//! 列布局：`[0]=time`（Date32/TimestampUs 原生类型）、`[1]=sym`（字典列，
//! `type_id=SPLAYED_TYPE_STRING_DICT`，字符串经 `splayed_scan_dict_value`
//! 取回）、`[2..]=FIELD 列`（定长小端列，可与 DuckDB `Vector` 零拷贝借用）。
//!
//! 类型 id 复用 `splayed_format::DataType`（0..=13），字典字符串列用 200。

use std::cell::RefCell;
use std::ffi::{c_char, c_void, CString};
use std::sync::Arc;

use splayed_core::{
    CoreBatch, CoreColumnKind, Dataset, ParallelScanBatches, ScanRequest, Scanner,
    SymbolSelection, TimeRange, open_dataset, scan_owned_parallel,
};

/// 字典字符串列（SYM）的 type_id。
pub const SPLAYED_TYPE_STRING_DICT: u8 = 200;

/// `splayed_format::DataType` 的磁盘 id（repr(u8) 直接取值）。
fn disk_type_id(dt: splayed_format::DataType) -> u8 {
    dt as u8
}

/// TIME 列在磁盘上的类型 id。
fn time_type_id(tt: splayed_format::TimeType) -> u8 {
    match tt {
        splayed_format::TimeType::Date32 => disk_type_id(splayed_format::DataType::Date32),
        splayed_format::TimeType::TimestampUs => disk_type_id(splayed_format::DataType::TimestampUs),
    }
}

// ---------------------------------------------------------------------------
// FFI 结构体定义（repr(C)，与扩展壳共享）
// ---------------------------------------------------------------------------

/// 一个列的视图（引用 Rust 侧当前批次的缓冲）。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SplayedColumnFFI {
    /// 0 = 定长原始列；1 = 字典列（SYM）。
    pub kind: u8,
    /// 定长列：`splayed_format::DataType` id（0..=13）；字典列：200。
    pub type_id: u8,
    /// 定长列：值缓冲（LE，连续）；字典列：u32 索引缓冲。
    pub data: *const u8,
    pub data_len: u32,
    /// validity 位图（LSB-first，bit 置位 = 有效）；无 NULL 时为 null/0。
    pub validity: *const u8,
    pub validity_len: u32,
    /// 字典列：字典字符串数；定长列：0。
    pub dict_count: i32,
}

/// 一批数据的扁平视图。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SplayedBatchFFI {
    pub row_count: i32,
    pub column_count: i32,
    /// 系统列数（时间 + SYM = 2），其后为 FIELD 列。
    pub system_columns: i32,
    /// `column_count` 个列的视图数组（Rust 持有，至下一次调用前有效）。
    pub columns: *const SplayedColumnFFI,
}

// ---------------------------------------------------------------------------
// 错误处理（线程本地，Rust 持有；C++ 只读不释放）
// ---------------------------------------------------------------------------

thread_local! {
    static LAST_ERR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_err(msg: impl AsRef<str>) {
    let msg = msg.as_ref();
    LAST_ERR.with(|c| {
        *c.borrow_mut() = Some(CString::new(msg).unwrap_or_default());
    });
}

/// 取最近一次错误的 UTF-8 消息（Rust 持有，至下一次 splayed 调用前有效）。
#[no_mangle]
pub extern "C" fn splayed_last_error() -> *const c_char {
    LAST_ERR.with(|c| {
        let mut c = c.borrow_mut();
        c.get_or_insert_with(|| CString::new("").unwrap()).as_ptr()
    })
}

// ---------------------------------------------------------------------------
// 连接（数据集）
// ---------------------------------------------------------------------------

type DatasetHandle = Arc<Dataset>;

/// 打开一个 dataset（`dir` 必须直接含 `.meta`）。
///
/// # Safety
/// `dir` 必须指向 `dir_len` 字节的合法可读内存（UTF-8 路径字节）；返回句柄
/// 所有权归调用方，须经 `splayed_dataset_close` 释放。
#[no_mangle]
pub unsafe extern "C" fn splayed_dataset_open(dir: *const c_char, dir_len: u32) -> *mut c_void {
    let path = unsafe { c_bytes_to_string(dir, dir_len) };
    match open_dataset(&path) {
        Ok(ds) => Box::into_raw(Box::new(Arc::new(ds))) as *mut c_void,
        Err(e) => {
            set_err(format!("open_dataset {path:?}: {e}"));
            std::ptr::null_mut()
        }
    }
}

/// 关闭数据集句柄。
///
/// # Safety
/// `handle` 必须来自 `splayed_dataset_open`，且只能关闭一次（之后不得再使用）。
#[no_mangle]
pub unsafe extern "C" fn splayed_dataset_close(handle: *mut c_void) {
    if !handle.is_null() {
        let _ = unsafe { Box::from_raw(handle as *mut DatasetHandle) };
    }
}

/// 列数（time + sym + fields）。
///
/// # Safety
/// `handle` 必须为 `splayed_dataset_open` 返回且尚未关闭的有效句柄。
#[no_mangle]
pub unsafe extern "C" fn splayed_dataset_schema_count(handle: *mut c_void) -> i32 {
    let ds = unsafe { &*(handle as *const DatasetHandle) };
    let n = 2 + ds.list_fields().map(|f| f.len()).unwrap_or(0);
    n as i32
}

/// 第 i 个列的元数据（name / type_id / nullable）。
///
/// # Safety
/// `handle` 须为有效数据集句柄；`name_out/name_len_out/type_out/nullable_out`
/// 须指向可写的合法内存。
#[no_mangle]
pub unsafe extern "C" fn splayed_dataset_schema_field(
    handle: *mut c_void,
    index: i32,
    name_out: *mut *const c_char,
    name_len_out: *mut u32,
    type_out: *mut u8,
    nullable_out: *mut u8,
) -> i32 {
    let ds = unsafe { &*(handle as *const DatasetHandle) };
    let fields = ds.list_fields().unwrap_or_default();
    let (name, ty, nullable): (String, u8, bool) = if index < 0 {
        return -1;
    } else if index == 0 {
        // TIME
        ("time".to_string(), time_type_id(ds.meta.time_type()), false)
    } else if index == 1 {
        ("sym".to_string(), SPLAYED_TYPE_STRING_DICT, false)
    } else {
        let field_idx = (index - 2) as usize;
        let Some(name) = fields.get(field_idx) else {
            return -1;
        };
        let Ok(reader) = splayed_core::FieldReader::open(ds.field_path(name)) else {
            return -1;
        };
        (name.clone(), disk_type_id(reader.data_type()), true)
    };

    // 名字以 NUL 结尾的 CString 存进 TLS 缓存（常驻，避免释放问题）。
    static FIELD_NAMES: std::sync::OnceLock<std::sync::Mutex<Vec<CString>>> =
        std::sync::OnceLock::new();
    let names = FIELD_NAMES.get_or_init(|| std::sync::Mutex::new(Vec::new()));
    let mut names = names.lock().unwrap();
    names.push(CString::new(name).unwrap_or_default());
    let ptr = names.last().unwrap().as_ptr();

    unsafe {
        *name_out = ptr;
        *name_len_out = names.last().unwrap().to_bytes().len() as u32;
        *type_out = ty;
        *nullable_out = nullable as u8;
    }
    0
}

// ---------------------------------------------------------------------------
// 扫描
// ---------------------------------------------------------------------------

/// 扫描状态：持有并行流 + 当前批 + 该批的列视图（供 C++ 一次性消费）。
struct ScanState {
    stream: ParallelScanBatches,
    current: Option<CoreBatch>,
    views: Vec<SplayedColumnFFI>,
}

/// 打开一次扫描：`cols`/`col_lens` 为要读取的 FIELD 列名（长度数组）。
///
/// 返回扫描句柄；失败返回 NULL（`splayed_last_error`）。
///
/// # Safety
/// `dataset` 须为有效数据集句柄；`cols`/`col_lens` 须各指向 `col_count` 个
/// 元素的合法数组；返回句柄所有权归调用方，须经 `splayed_scan_close` 释放。
#[no_mangle]
pub unsafe extern "C" fn splayed_scan_open(
    dataset: *mut c_void,
    cols: *const *const c_char,
    col_lens: *const u32,
    col_count: i32,
    batch_rows: u32,
    parallelism: i32,
) -> *mut c_void {
    let ds = unsafe { &*(dataset as *const DatasetHandle) };
    let mut columns = Vec::with_capacity(col_count.max(0) as usize);
    for i in 0..col_count.max(0) {
        let name = unsafe { c_bytes_to_string(*cols.offset(i as isize), *col_lens.offset(i as isize)) };
        columns.push(name);
    }

    let req = ScanRequest {
        columns,
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: Vec::new(),
        batch_size: batch_rows.max(1) as usize,
        parallelism: 1,
        limit: None,
    };
    let scanner = Scanner::new(ds);
    let plan = match scanner.plan(&req) {
        Ok(p) => p,
        Err(e) => {
            set_err(format!("scan plan: {e}"));
            return std::ptr::null_mut();
        }
    };
    let stream = match scan_owned_parallel(Arc::clone(ds), &plan, &req, parallelism.max(1) as usize)
    {
        Ok(s) => s,
        Err(e) => {
            set_err(format!("scan open: {e}"));
            return std::ptr::null_mut();
        }
    };

    Box::into_raw(Box::new(ScanState {
        stream,
        current: None,
        views: Vec::new(),
    })) as *mut c_void
}

/// 取下一批：返回 1 = 有批（`*out` 填好），0 = 结束，负值 = 错误。
///
/// # Safety
/// `scan` 须为 `splayed_scan_open` 返回且未关闭的有效句柄；`out` 须指向可写内存。
#[no_mangle]
pub unsafe extern "C" fn splayed_scan_next(
    scan: *mut c_void,
    out: *mut SplayedBatchFFI,
) -> i32 {
    let state = unsafe { &mut *(scan as *mut ScanState) };
    match state.stream.next_batch() {
        Ok(Some(batch)) => {
            let row_count = batch.num_rows();
            let views = build_views(&batch);
            state.current = Some(batch);
            state.views = views;
            let sys = 2i32.min(state.views.len() as i32);
            unsafe {
                *out = SplayedBatchFFI {
                    row_count: row_count as i32,
                    column_count: state.views.len() as i32,
                    system_columns: sys,
                    columns: state.views.as_ptr(),
                };
            }
            1
        }
        Ok(None) => 0,
        Err(e) => {
            set_err(format!("scan next: {e}"));
            -1
        }
    }
}

/// 取字典列（SYM）第 `idx` 个字符串（Rust 持有，至下一次调用前有效）。
///
/// # Safety
/// `scan` 须为有效扫描句柄且最近一次 `splayed_scan_next` 返回 1；
/// `out_ptr/out_len` 须指向可写内存。
#[no_mangle]
pub unsafe extern "C" fn splayed_scan_dict_value(
    scan: *mut c_void,
    column: i32,
    idx: i32,
    out_ptr: *mut *const c_char,
    out_len: *mut u32,
) -> i32 {
    let state = unsafe { &*(scan as *const ScanState) };
    let Some(batch) = state.current.as_ref() else {
        return -1;
    };
    if column < 0 || column >= batch.num_columns() as i32 {
        return -1;
    }
    let col = batch.column(column as usize);
    let Some(dict) = col.dictionary_dict() else {
        return -1;
    };
    let Some(value) = dict.values.get(idx as usize) else {
        return -1;
    };
    // 字符串以 NUL 结尾存进 TLS 缓存（常驻）。
    thread_local! {
        static DICT_CMDS: RefCell<Vec<CString>> = const { RefCell::new(Vec::new()) };
    }
    DICT_CMDS.with(|cmds| {
        let mut cmds = cmds.borrow_mut();
        cmds.push(CString::new(value.as_str()).unwrap_or_default());
        unsafe {
            *out_ptr = cmds.last().unwrap().as_ptr();
            *out_len = cmds.last().unwrap().to_bytes().len() as u32;
        }
        0
    })
}

/// 关闭扫描句柄。
///
/// # Safety
/// `scan` 须为 `splayed_scan_open` 返回且未关闭的有效句柄；只可关闭一次。
#[no_mangle]
pub unsafe extern "C" fn splayed_scan_close(scan: *mut c_void) {
    if !scan.is_null() {
        let _ = unsafe { Box::from_raw(scan as *mut ScanState) };
    }
}

// ---------------------------------------------------------------------------
// 内部辅助
// ---------------------------------------------------------------------------

/// `(ptr,len)` → String（FFI 不可能给我们缺 NUL 的 UTF-8 保证，用 lossy）。
unsafe fn c_bytes_to_string(ptr: *const c_char, len: u32) -> String {
    if ptr.is_null() {
        return String::new();
    }
    let slice = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    String::from_utf8_lossy(slice).into_owned()
}

/// 把当前批转成 C++ 可消费的列视图（借用批次缓冲，随下一批失效）。
fn build_views(batch: &CoreBatch) -> Vec<SplayedColumnFFI> {
    (0..batch.num_columns())
        .map(|i| {
            let col = batch.column(i);
            match &col.kind {
                CoreColumnKind::Primitive { ty, data } => {
                    let data = data.as_slice();
                    let (validity, validity_len) = match &col.nulls {
                        Some(bm) => (bm.as_bytes().as_ptr(), bm.as_bytes().len() as u32),
                        None => (std::ptr::null(), 0u32),
                    };
                    SplayedColumnFFI {
                        kind: 0,
                        type_id: splayed_core::CoreType::to_disk(ty)
                            .map(disk_type_id)
                            .unwrap_or(255),
                        data: data.as_ptr(),
                        data_len: data.len() as u32,
                        validity,
                        validity_len,
                        dict_count: 0,
                    }
                }
                CoreColumnKind::Dictionary { indices, values } => {
                    let data = indices.as_slice();
                    SplayedColumnFFI {
                        kind: 1,
                        type_id: SPLAYED_TYPE_STRING_DICT,
                        data: data.as_ptr(),
                        data_len: data.len() as u32,
                        validity: std::ptr::null(),
                        validity_len: 0,
                        dict_count: values.values.len() as i32,
                    }
                }
                CoreColumnKind::Varlen { .. } => SplayedColumnFFI {
                    kind: 0,
                    type_id: 255,
                    data: std::ptr::null(),
                    data_len: 0,
                    validity: std::ptr::null(),
                    validity_len: 0,
                    dict_count: 0,
                },
            }
        })
        .collect()
}