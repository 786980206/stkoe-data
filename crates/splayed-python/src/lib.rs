//! Splayed V1 Python 绑定（pyo3），基于 `splayed-arrow`，以 **pyarrow** 作为表格
//! 交换层（Arrow C data interface，零拷贝）。
//!
//! - 写：`create_meta` / `create_table[_with_options]` / `update_table[_with_options]`
//!   / `update_meta`；分区写（`create_partitioned_table` / `append_partition` /
//!   `update_partition_table` / `update_partition_meta` / `drop_partition`）；子集
//!   `create_subset`。
//! - 读：`scan_dataset` / `scan_partitioned`（→ `list[pyarrow.RecordBatch]`）/
//!   `read_subset`（→ `pyarrow.RecordBatch`）。
//!
//! 写接口的 `data` 参数为 `pyarrow.RecordBatch`（须含 TIME + SYM + 字段列）；读
//! 接口返回 `pyarrow.RecordBatch`，可直接 `df = t.to_pandas()`。
//!
//! 依赖：Python 3.x + `pyarrow`；模块名 `splayed`（`import splayed`）。
//! 构建：`cargo build -p splayed-python --release --features extension-module`，
//! 产物 `target/release/splayed.dll` → 复制为 `splayed.pyd` 后置于 `sys.path`。

use arrow_array::RecordBatch;
use arrow::pyarrow::{PyArrowType, ToPyArrow};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use splayed_core::{
    FieldWriteOptions, PartitionScanRequest, ScanRequest, SubsetInput as CoreSubsetInput,
    SymbolSelection, TimeRange, open_dataset, PartitionedTable,
};
use splayed_format::{Compression, Encoding, TimeType};

pyo3::create_exception!(splayed, SplayedError, pyo3::exceptions::PyException);

/// 所有 splayed 侧错误 → `SplayedError`（保留 Display 信息）。
fn py_err<E: std::fmt::Display>(e: E) -> PyErr {
    SplayedError::new_err(e.to_string())
}

fn parse_encoding(s: &str) -> PyResult<Encoding> {
    match s.to_ascii_lowercase().as_str() {
        "plain" => Ok(Encoding::Plain),
        "delta" => Ok(Encoding::Delta),
        "rle" => Ok(Encoding::Rle),
        "bitpack" => Ok(Encoding::Bitpack),
        other => Err(PyValueError::new_err(format!(
            "unknown encoding '{other}' (plain|delta|rle|bitpack)"
        ))),
    }
}

fn parse_compression(s: &str) -> PyResult<Compression> {
    match s.to_ascii_lowercase().as_str() {
        "none" => Ok(Compression::None),
        "zstd" => Ok(Compression::Zstd),
        "lz4" => Ok(Compression::Lz4),
        other => Err(PyValueError::new_err(format!(
            "unknown compression '{other}' (none|zstd|lz4)"
        ))),
    }
}

fn parse_time_type(s: &str) -> PyResult<TimeType> {
    match s.to_ascii_lowercase().as_str() {
        "date32" => Ok(TimeType::Date32),
        "timestamp_us" => Ok(TimeType::TimestampUs),
        other => Err(PyValueError::new_err(format!(
            "unknown time_type '{other}' (date32|timestamp_us)"
        ))),
    }
}

fn write_options(encoding: &str, compression: &str) -> PyResult<FieldWriteOptions> {
    Ok(FieldWriteOptions {
        encoding: parse_encoding(encoding)?,
        compression: parse_compression(compression)?,
    })
}

fn to_pyarrow_list<'py>(py: Python<'py>, batches: Vec<RecordBatch>) -> PyResult<Bound<'py, PyAny>> {
    let list = PyList::empty(py);
    for rb in batches {
        list.append(rb.to_pyarrow(py)?)?;
    }
    Ok(list.into_any())
}

/// `columns=None` → 该 dataset / 分区表的**全部字段**（否则只有 time/sym 两列）。
fn resolve_columns(
    dir: &str,
    columns: Option<Vec<String>>,
    partitioned: bool,
) -> PyResult<Vec<String>> {
    if let Some(cols) = columns {
        return Ok(cols);
    }
    if partitioned {
        let table = PartitionedTable::open(dir).map_err(py_err)?;
        Ok(table.fields().iter().map(|(n, _)| n.clone()).collect())
    } else {
        let ds = open_dataset(dir).map_err(py_err)?;
        ds.list_fields().map_err(py_err)
    }
}

// ---------------------------------------------------------------------------
// 写：单 dataset
// ---------------------------------------------------------------------------

/// 建 `.meta`（只含 TIME + SYM 两列即可）。
#[pyfunction]
#[pyo3(signature = (dir, data, sorted=true))]
fn create_meta(dir: String, data: PyArrowType<RecordBatch>, sorted: bool) -> PyResult<()> {
    splayed_arrow::create_meta(&dir, &data.0, sorted).map_err(py_err)?;
    Ok(())
}

/// 一次性建 `.meta` + 全部字段（`data` 含 TIME + SYM + 字段列）。
#[pyfunction]
#[pyo3(signature = (dir, data, sorted=true))]
fn create_table(dir: String, data: PyArrowType<RecordBatch>, sorted: bool) -> PyResult<()> {
    splayed_arrow::create_table(&dir, &data.0, sorted).map_err(py_err)?;
    Ok(())
}

/// [`create_table`] + 新字段编码/压缩（非默认时写后只读）。
#[pyfunction]
#[pyo3(signature = (dir, data, sorted=true, encoding="plain", compression="none"))]
fn create_table_with_options(
    dir: String,
    data: PyArrowType<RecordBatch>,
    sorted: bool,
    encoding: &str,
    compression: &str,
) -> PyResult<()> {
    let opts = write_options(encoding, compression)?;
    splayed_arrow::create_table_with_options(&dir, &data.0, sorted, opts).map_err(py_err)?;
    Ok(())
}

/// 原地更新既有字段（`create_missing_fields=True` 时自动建新字段）。
#[pyfunction]
#[pyo3(signature = (dir, data, create_missing_fields=false))]
fn update_table(
    dir: String,
    data: PyArrowType<RecordBatch>,
    create_missing_fields: bool,
) -> PyResult<()> {
    // `sorted` 在 arrow 侧是占位参数（update_table 内部总是校验/排序）。
    splayed_arrow::update_table(&dir, &data.0, false, create_missing_fields).map_err(py_err)?;
    Ok(())
}

/// [`update_table`] + 新字段编码/压缩（已存在字段仍原地更新）。
#[pyfunction]
#[pyo3(signature = (dir, data, create_missing_fields=false, encoding="plain", compression="none"))]
fn update_table_with_options(
    dir: String,
    data: PyArrowType<RecordBatch>,
    create_missing_fields: bool,
    encoding: &str,
    compression: &str,
) -> PyResult<()> {
    let opts = write_options(encoding, compression)?;
    splayed_arrow::update_table_with_options(&dir, &data.0, create_missing_fields, opts)
        .map_err(py_err)?;
    Ok(())
}

/// 用新的 (SYM, TIME) 布局重写 `.meta` 并重散布全部字段（只取 TIME + SYM 列）。
#[pyfunction]
fn update_meta(dir: String, data: PyArrowType<RecordBatch>) -> PyResult<()> {
    splayed_arrow::update_meta(&dir, &data.0, false).map_err(py_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// 写：分区表
// ---------------------------------------------------------------------------

/// 一个分区的写入输入（建表 / 追加 / 布局重排共用）。
#[pyclass(from_py_object)]
struct PartitionWriteInput {
    /// 分区名 = 目录名（可含 `key=value`，如 `"month=2026-07"`）。
    #[pyo3(get)]
    name: String,
    /// 该分区数据（TIME + SYM + 全表字段列）。
    data: PyArrowType<RecordBatch>,
    /// (SYM, TIME) 升序性能提示。
    #[pyo3(get)]
    sorted: bool,
}

// `from_py_object` 需要 `Clone`；RecordBatch 本身 Clone（Arc 缓冲），可手动实现。
impl Clone for PartitionWriteInput {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            data: PyArrowType(self.data.0.clone()),
            sorted: self.sorted,
        }
    }
}

#[pymethods]
impl PartitionWriteInput {
    #[new]
    #[pyo3(signature = (name, data, sorted=true))]
    fn new(name: String, data: PyArrowType<RecordBatch>, sorted: bool) -> Self {
        Self {
            name,
            data,
            sorted,
        }
    }
}

/// 一个符号在子集内的输入：符号名 + 若干连续 TIME 段 `(time_value, count)`。
#[pyclass(from_py_object)]
#[derive(Clone)]
struct SubsetInput {
    #[pyo3(get)]
    sym: String,
    #[pyo3(get)]
    segments: Vec<(i64, u32)>,
}

#[pymethods]
impl SubsetInput {
    #[new]
    fn new(sym: String, segments: Vec<(i64, u32)>) -> Self {
        Self { sym, segments }
    }
}

fn convert_inputs(inputs: Vec<PartitionWriteInput>) -> Vec<splayed_arrow::PartitionWriteInputArrow> {
    inputs
        .into_iter()
        .map(|p| {
            splayed_arrow::PartitionWriteInputArrow::new(p.name, p.data.0, p.sorted)
        })
        .collect()
}

/// 一次建出整个分区表（root + N 个分区）。
#[pyfunction]
#[pyo3(signature = (root, time_type, partitions, encoding="plain", compression="none"))]
fn create_partitioned_table(
    root: String,
    time_type: String,
    partitions: Vec<PartitionWriteInput>,
    encoding: &str,
    compression: &str,
) -> PyResult<()> {
    let tt = parse_time_type(&time_type)?;
    let opts = write_options(encoding, compression)?;
    let inputs = convert_inputs(partitions);
    splayed_arrow::create_partitioned_table(&root, tt, &inputs, opts).map_err(py_err)?;
    Ok(())
}

/// 追加一个分区（校验与既有分区 schema / 命名风格 / 分区列一致）。
#[pyfunction]
#[pyo3(signature = (root, partition, encoding="plain", compression="none"))]
fn append_partition(
    root: String,
    partition: PartitionWriteInput,
    encoding: &str,
    compression: &str,
) -> PyResult<()> {
    let opts = write_options(encoding, compression)?;
    let input = splayed_arrow::PartitionWriteInputArrow::new(
        partition.name,
        partition.data.0,
        partition.sorted,
    );
    splayed_arrow::append_partition(&root, &input, opts).map_err(py_err)?;
    Ok(())
}

/// 表级格子写入（跨分区路由）。`target_partition=None` 时每行路由到唯一分区。
#[pyfunction]
#[pyo3(signature = (root, data, create_missing_fields=false, target_partition=None, encoding="plain", compression="none"))]
fn update_partition_table(
    root: String,
    data: PyArrowType<RecordBatch>,
    create_missing_fields: bool,
    target_partition: Option<String>,
    encoding: &str,
    compression: &str,
) -> PyResult<()> {
    let opts = write_options(encoding, compression)?;
    let target = target_partition.as_deref();
    splayed_arrow::update_partition_table(&root, &data.0, create_missing_fields, target, opts)
        .map_err(py_err)?;
    Ok(())
}

/// 表级布局重排：输入里的分区 `update_meta`、新增分区 `create_table`、缺席分区删除。
#[pyfunction]
#[pyo3(signature = (root, partitions, encoding="plain", compression="none"))]
fn update_partition_meta(
    root: String,
    partitions: Vec<PartitionWriteInput>,
    encoding: &str,
    compression: &str,
) -> PyResult<()> {
    let opts = write_options(encoding, compression)?;
    let inputs = convert_inputs(partitions);
    splayed_arrow::update_partition_meta(&root, &inputs, opts).map_err(py_err)?;
    Ok(())
}

/// 删除一个分区（目录整体删除；分区不存在报错）。
#[pyfunction]
fn drop_partition(root: String, name: String) -> PyResult<()> {
    splayed_arrow::drop_partition(&root, &name).map_err(py_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// 写：子集
// ---------------------------------------------------------------------------

/// 创建 `.sub.{name}`：父 dataset 目录下的 `SYM × TIME` 子集（每 SYM 可多段）。
#[pyfunction]
fn create_subset(dir: String, name: String, inputs: Vec<SubsetInput>) -> PyResult<()> {
    let core_inputs: Vec<CoreSubsetInput> = inputs
        .into_iter()
        .map(|i| CoreSubsetInput::new(i.sym, i.segments))
        .collect();
    splayed_core::create_subset(&dir, &name, &core_inputs).map_err(py_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// 读
// ---------------------------------------------------------------------------

/// 扫描单 dataset → `list[pyarrow.RecordBatch]`（列 = time/sym/...columns）。
#[pyfunction]
#[allow(clippy::too_many_arguments)] // Python 签名需要 dir/columns/symbols/time_range/batch/parallelism/limit + py
#[pyo3(signature = (dir, columns=None, symbols=None, time_range=None, batch_size=65536, parallelism=1, limit=None))]
fn scan_dataset<'py>(
    dir: String,
    columns: Option<Vec<String>>,
    symbols: Option<Vec<String>>,
    time_range: Option<(i64, i64)>,
    batch_size: usize,
    parallelism: usize,
    limit: Option<usize>,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let columns = resolve_columns(&dir, columns, false)?;
    let mut req = ScanRequest::new(columns);
    req.symbols = match symbols {
        None => SymbolSelection::All,
        Some(syms) => SymbolSelection::Symbols(syms),
    };
    req.time_range = match time_range {
        None => TimeRange::all(),
        Some((start, end)) => TimeRange::new(start, end),
    };
    req.batch_size = batch_size;
    req.parallelism = parallelism;
    req.limit = limit;
    let batches = splayed_arrow::scan_dataset(&dir, &req).map_err(py_err)?;
    to_pyarrow_list(py, batches)
}

/// 扫描分区表 → `list[pyarrow.RecordBatch]`（按分区名升序合并）。
#[pyfunction]
#[pyo3(signature = (dir, columns=None, symbols=None, time_range=None, batch_size=65536, parallelism=1))]
fn scan_partitioned<'py>(
    dir: String,
    columns: Option<Vec<String>>,
    symbols: Option<Vec<String>>,
    time_range: Option<(i64, i64)>,
    batch_size: usize,
    parallelism: usize,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let columns = resolve_columns(&dir, columns, true)?;
    let req = PartitionScanRequest {
        columns,
        symbols: match symbols {
            None => SymbolSelection::All,
            Some(syms) => SymbolSelection::Symbols(syms),
        },
        time_range: match time_range {
            None => TimeRange::all(),
            Some((start, end)) => TimeRange::new(start, end),
        },
        batch_size,
        parallelism,
        ..Default::default()
    };
    let batches = splayed_arrow::scan_partitioned(&dir, &req).map_err(py_err)?;
    to_pyarrow_list(py, batches)
}

/// 读取子集 `.sub.{name}` → `pyarrow.RecordBatch`（父全局行序，列 = time/sym/...columns）。
#[pyfunction]
#[pyo3(signature = (dir, name, columns=None))]
fn read_subset<'py>(
    dir: String,
    name: String,
    columns: Option<Vec<String>>,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let rb = splayed_arrow::read_subset(&dir, &name, &columns.unwrap_or_default())
        .map_err(py_err)?;
    rb.to_pyarrow(py)
}

/// `splayed` Python 扩展模块入口。
#[pymodule]
fn splayed(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("SplayedError", m.py().get_type::<SplayedError>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PartitionWriteInput>()?;
    m.add_class::<SubsetInput>()?;
    m.add_function(wrap_pyfunction!(create_meta, m)?)?;
    m.add_function(wrap_pyfunction!(create_table, m)?)?;
    m.add_function(wrap_pyfunction!(create_table_with_options, m)?)?;
    m.add_function(wrap_pyfunction!(update_table, m)?)?;
    m.add_function(wrap_pyfunction!(update_table_with_options, m)?)?;
    m.add_function(wrap_pyfunction!(update_meta, m)?)?;
    m.add_function(wrap_pyfunction!(create_partitioned_table, m)?)?;
    m.add_function(wrap_pyfunction!(append_partition, m)?)?;
    m.add_function(wrap_pyfunction!(update_partition_table, m)?)?;
    m.add_function(wrap_pyfunction!(update_partition_meta, m)?)?;
    m.add_function(wrap_pyfunction!(drop_partition, m)?)?;
    m.add_function(wrap_pyfunction!(create_subset, m)?)?;
    m.add_function(wrap_pyfunction!(scan_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(scan_partitioned, m)?)?;
    m.add_function(wrap_pyfunction!(read_subset, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
