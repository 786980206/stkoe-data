use std::sync::Arc;

use arrow::pyarrow::{FromPyArrow, IntoPyArrow};
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyType};

use splayed_arrow::{
    TableArrowBatchReader, TableArrowReader as CoreArrowReader,
    TableArrowWriter as CoreArrowWriter,
};
use splayed_format::DataType;
use splayed_table::{PartitionScheme, TableOptions, TableScanRequest};

fn to_py_err(e: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn parse_scheme(s: &str) -> PyResult<PartitionScheme> {
    match s.to_lowercase().as_str() {
        "none" => Ok(PartitionScheme::None),
        "year" => Ok(PartitionScheme::Year),
        "month" => Ok(PartitionScheme::Month),
        "date" | "day" => Ok(PartitionScheme::Date),
        other => Err(PyValueError::new_err(format!(
            "invalid partition scheme: '{other}'. Expected 'none', 'year', 'month', or 'date'"
        ))),
    }
}

fn parse_compression(s: Option<&str>) -> PyResult<splayed_format::Compression> {
    match s.unwrap_or("zstd").to_ascii_lowercase().as_str() {
        "none" | "uncompressed" => Ok(splayed_format::Compression::None),
        "zstd" => Ok(splayed_format::Compression::Zstd),
        "lz4" => Ok(splayed_format::Compression::Lz4),
        other => Err(PyValueError::new_err(format!(
            "invalid compression: '{other}'. Expected 'zstd', 'lz4', or 'none'"
        ))),
    }
}

fn parse_data_type(s: &str) -> PyResult<DataType> {
    match s.to_lowercase().as_str() {
        "bool" | "boolean" => Ok(DataType::Bool),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "float32" | "float" => Ok(DataType::Float32),
        "float64" | "double" => Ok(DataType::Float64),
        "date32" => Ok(DataType::Date32),
        "date64" => Ok(DataType::Date64),
        "timestamp" | "timestamp_us" => Ok(DataType::TimestampUs),
        "utf8" | "string" => Ok(DataType::Utf8),
        other => Err(PyValueError::new_err(format!("unsupported data type '{other}'"))),
    }
}

fn extract_record_batch(obj: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    // 尝试直接提取为 RecordBatch
    if let Ok(batch) = RecordBatch::from_pyarrow_bound(obj) {
        return Ok(batch);
    }
    // 若传入的是 pa.Table，尝试调用 to_batches()
    if let Ok(to_batches) = obj.getattr("to_batches") {
        let batches: Bound<'_, PyList> = to_batches.call0()?.extract()?;
        if batches.is_empty() {
            let schema_obj = obj.getattr("schema")?;
            let schema = ArrowSchema::from_pyarrow_bound(&schema_obj)?;
            return Ok(RecordBatch::new_empty(Arc::new(schema)));
        }
        if batches.len() == 1 {
            let first = batches.get_item(0)?;
            return RecordBatch::from_pyarrow_bound(&first);
        }
        let mut rbs = Vec::with_capacity(batches.len());
        for item in batches.iter() {
            rbs.push(RecordBatch::from_pyarrow_bound(&item)?);
        }
        let schema = rbs[0].schema();
        let concatenated = arrow::compute::concat_batches(&schema, &rbs).map_err(to_py_err)?;
        return Ok(concatenated);
    }
    Err(PyValueError::new_err(
        "expected a pyarrow.RecordBatch or pyarrow.Table",
    ))
}

#[pyclass(name = "PartitionScheme", unsendable)]
pub struct PyPartitionScheme;

#[pymethods]
impl PyPartitionScheme {
    #[classattr]
    const NONE: &'static str = "none";
    #[classattr]
    const YEAR: &'static str = "year";
    #[classattr]
    const MONTH: &'static str = "month";
    #[classattr]
    const DAY: &'static str = "day";
}

/// 流式批次读取器
#[pyclass(name = "TableBatchReader", unsendable)]
pub struct PyTableBatchReader {
    inner: Option<TableArrowBatchReader<'static>>,
}

#[pymethods]
impl PyTableBatchReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        let reader = self
            .inner
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("batch reader already closed"))?;
        match reader.next().map_err(to_py_err)? {
            None => Ok(None),
            Some(batch) => Ok(Some(batch.into_pyarrow(py)?)),
        }
    }

    fn next(&mut self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        self.__next__(py)
    }

    fn read_all(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        let mut batches = Vec::new();
        while let Some(batch_obj) = self.__next__(py)? {
            batches.push(batch_obj);
        }
        let pa = py.import("pyarrow")?;
        let table = pa.getattr("Table")?.call_method1("from_batches", (batches,))?;
        Ok(table.into())
    }

    fn close(&mut self) -> PyResult<()> {
        if let Some(reader) = self.inner.take() {
            reader.close().map_err(to_py_err)?;
        }
        Ok(())
    }
}

/// 只读表对象
#[pyclass(name = "TableReader", unsendable)]
pub struct PyTableReader {
    inner: Arc<CoreArrowReader>,
}

#[pymethods]
impl PyTableReader {
    #[classmethod]
    #[pyo3(signature = (path, max_parallelism=None))]
    fn open(_cls: &Bound<'_, PyType>, path: &str, max_parallelism: Option<usize>) -> PyResult<Self> {
        let mut options = TableOptions::default();
        options.max_parallelism = max_parallelism;
        let reader = CoreArrowReader::open_with_options(std::path::Path::new(path), options)
            .map_err(to_py_err)?;
        Ok(PyTableReader {
            inner: Arc::new(reader),
        })
    }

    #[pyo3(signature = (symbols=None, start_time=None, end_time=None, columns=None, limit=None, batch_size=None))]
    fn read(
        &self,
        symbols: Option<Vec<String>>,
        start_time: Option<i64>,
        end_time: Option<i64>,
        columns: Option<Vec<String>>,
        limit: Option<u64>,
        batch_size: Option<usize>,
    ) -> PyResult<PyTableBatchReader> {
        let mut req = TableScanRequest::default();
        if let Some(syms) = symbols {
            if let Some(first) = syms.into_iter().next() {
                req.sym = Some(first);
            }
        }
        if let (Some(s), Some(e)) = (start_time, end_time) {
            req.time = Some((s, e));
        }
        if let Some(cols) = columns {
            req.projection = cols;
        }
        req.limit = limit;

        let reader_ref: &'static CoreArrowReader = unsafe {
            let ptr: *const CoreArrowReader = &*self.inner;
            &*ptr
        };
        let batch_reader = reader_ref.read(req, batch_size).map_err(to_py_err)?;
        Ok(PyTableBatchReader {
            inner: Some(batch_reader),
        })
    }

    #[pyo3(signature = (symbols=None, start_time=None, end_time=None, columns=None, limit=None, max_parallelism=None))]
    fn read_all(
        &self,
        py: Python<'_>,
        symbols: Option<Vec<String>>,
        start_time: Option<i64>,
        end_time: Option<i64>,
        columns: Option<Vec<String>>,
        limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> PyResult<PyObject> {
        let mut req = TableScanRequest::default();
        if let Some(syms) = symbols {
            if let Some(first) = syms.into_iter().next() {
                req.sym = Some(first);
            }
        }
        if let (Some(s), Some(e)) = (start_time, end_time) {
            req.time = Some((s, e));
        }
        if let Some(cols) = columns {
            req.projection = cols;
        }
        req.limit = limit;
        req.max_parallelism = max_parallelism;

        let batches = self.inner.read_all_parallel(req).map_err(to_py_err)?;

        let py_batches = batches
            .into_iter()
            .map(|b| b.into_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()?;

        let pa = py.import("pyarrow")?;
        let table = pa.getattr("Table")?.call_method1("from_batches", (py_batches,))?;
        Ok(table.into())
    }

    #[pyo3(signature = (symbols=None, start_time=None, end_time=None, columns=None, limit=None, max_parallelism=None))]
    fn read_batches(
        &self,
        py: Python<'_>,
        symbols: Option<Vec<String>>,
        start_time: Option<i64>,
        end_time: Option<i64>,
        columns: Option<Vec<String>>,
        limit: Option<u64>,
        max_parallelism: Option<usize>,
    ) -> PyResult<Vec<PyObject>> {
        let mut req = TableScanRequest::default();
        if let Some(syms) = symbols {
            if let Some(first) = syms.into_iter().next() {
                req.sym = Some(first);
            }
        }
        if let (Some(s), Some(e)) = (start_time, end_time) {
            req.time = Some((s, e));
        }
        if let Some(cols) = columns {
            req.projection = cols;
        }
        req.limit = limit;
        req.max_parallelism = max_parallelism;

        let batches = self.inner.read_all_parallel(req).map_err(to_py_err)?;

        batches
            .into_iter()
            .map(|b| b.into_pyarrow(py))
            .collect()
    }

    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        let schema = self.inner.schema().map_err(to_py_err)?;
        schema.as_ref().clone().into_pyarrow(py)
    }

    fn statistics(&self, py: Python<'_>) -> PyResult<PyObject> {
        let stats = self.inner.statistics().map_err(to_py_err)?;
        let dict = PyDict::new(py);
        dict.set_item("row_count", stats.row_count)?;
        dict.set_item("partition_count", stats.partition_count)?;
        dict.set_item("time_min", stats.time_min)?;
        dict.set_item("time_max", stats.time_max)?;
        if let Some(min) = stats.sym_min {
            dict.set_item("sym_min", min)?;
        }
        if let Some(max) = stats.sym_max {
            dict.set_item("sym_max", max)?;
        }
        Ok(dict.into())
    }

    fn metadata(&self, py: Python<'_>) -> PyResult<PyObject> {
        let meta = self.inner.metadata().map_err(to_py_err)?;
        let dict = PyDict::new(py);
        dict.set_item("scheme", meta.partitioning.scheme.to_lowercase())?;
        dict.set_item("partition_count", meta.partitions.len())?;
        let py_partitions = pyo3::types::PyList::empty(py);
        for p in &meta.partitions {
            let p_dict = PyDict::new(py);
            p_dict.set_item("name", &p.name)?;
            p_dict.set_item("time_min", p.time_min)?;
            p_dict.set_item("time_max", p.time_max)?;
            py_partitions.append(p_dict)?;
        }
        dict.set_item("partitions", py_partitions)?;
        dict.set_item("projection_pushdown", meta.capabilities.projection_pushdown)?;
        dict.set_item("predicate_pushdown", meta.capabilities.predicate_pushdown)?;
        dict.set_item("limit_pushdown", meta.capabilities.limit_pushdown)?;
        Ok(dict.into())
    }

    fn path(&self) -> String {
        self.inner.path().to_string_lossy().to_string()
    }

    fn scheme(&self) -> String {
        self.inner.scheme().as_str().to_string()
    }

    fn close(&self) -> PyResult<()> {
        Ok(())
    }
}

/// 可写表对象
#[pyclass(name = "TableWriter", unsendable)]
pub struct PyTableWriter {
    inner: Option<CoreArrowWriter>,
}

impl Drop for PyTableWriter {
    fn drop(&mut self) {
        if let Some(w) = self.inner.take() {
            let _ = w.close();
        }
    }
}

impl PyTableWriter {
    fn inner_writer(&self) -> PyResult<&CoreArrowWriter> {
        self.inner
            .as_ref()
            .ok_or_else(|| PyIOError::new_err("table writer has been closed or removed"))
    }
}

#[pymethods]
impl PyTableWriter {
    #[classmethod]
    #[pyo3(signature = (path, schema, scheme="none", initial_partition=None, compression=None, max_parallelism=None))]
    fn create(
        _cls: &Bound<'_, PyType>,
        path: &str,
        schema: &Bound<'_, PyAny>,
        scheme: &str,
        initial_partition: Option<&str>,
        compression: Option<&str>,
        max_parallelism: Option<usize>,
    ) -> PyResult<Self> {
        let arrow_schema = ArrowSchema::from_pyarrow_bound(schema)?;
        let partition_scheme = parse_scheme(scheme)?;
        let comp = parse_compression(compression)?;
        let mut options = TableOptions::default();
        options.max_parallelism = max_parallelism;
        options.compression = Some(comp);
        let writer = CoreArrowWriter::create(
            std::path::Path::new(path),
            &arrow_schema,
            partition_scheme,
            initial_partition,
            Some(options),
        )
        .map_err(to_py_err)?;
        Ok(PyTableWriter { inner: Some(writer) })
    }

    #[classmethod]
    #[pyo3(signature = (path, batch, scheme="none", compression=None, max_parallelism=None))]
    fn init(
        _cls: &Bound<'_, PyType>,
        path: &str,
        batch: &Bound<'_, PyAny>,
        scheme: &str,
        compression: Option<&str>,
        max_parallelism: Option<usize>,
    ) -> PyResult<Self> {
        let rb = extract_record_batch(batch)?;
        let partition_scheme = parse_scheme(scheme)?;
        let comp = parse_compression(compression)?;
        let mut options = TableOptions::default();
        options.max_parallelism = max_parallelism;
        options.compression = Some(comp);
        let writer = CoreArrowWriter::init(
            std::path::Path::new(path),
            partition_scheme,
            &rb,
            Some(options),
        )
        .map_err(to_py_err)?;
        Ok(PyTableWriter { inner: Some(writer) })
    }

    #[classmethod]
    #[pyo3(signature = (path, max_parallelism=None))]
    fn open(_cls: &Bound<'_, PyType>, path: &str, max_parallelism: Option<usize>) -> PyResult<Self> {
        let mut options = TableOptions::default();
        options.max_parallelism = max_parallelism;
        let writer = CoreArrowWriter::open_with_options(std::path::Path::new(path), options)
            .map_err(to_py_err)?;
        Ok(PyTableWriter { inner: Some(writer) })
    }

    fn write(&self, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = extract_record_batch(data)?;
        self.inner_writer()?.write(&batch).map_err(to_py_err)
    }

    fn update(&self, data: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = extract_record_batch(data)?;
        self.inner_writer()?.update(&batch).map_err(to_py_err)
    }

    fn delete_partition(&self, partition_name: &str) -> PyResult<()> {
        self.inner_writer()?.delete_partition(partition_name).map_err(to_py_err)
    }

    #[pyo3(signature = (field_name, data_type, opts=None))]
    fn init_field(
        &self,
        field_name: &str,
        data_type: &str,
        opts: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = opts;
        let dt = parse_data_type(data_type)?;
        self.inner_writer()?
            .init_field(field_name, dt, None)
            .map_err(to_py_err)
    }

    fn delete_field(&self, name: &str) -> PyResult<()> {
        self.inner_writer()?.delete_field(name).map_err(to_py_err)
    }

    fn rename_field(&self, name: &str, new_name: &str) -> PyResult<()> {
        self.inner_writer()?.rename_field(name, new_name).map_err(to_py_err)
    }

    fn cast_field(&self, name: &str, target_type: &str) -> PyResult<()> {
        let dt = parse_data_type(target_type)?;
        self.inner_writer()?.cast_field(name, dt).map_err(to_py_err)
    }

    fn compress_field(&self, name: &str) -> PyResult<()> {
        self.inner_writer()?.compress_field(name).map_err(to_py_err)
    }

    fn decompress_field(&self, name: &str) -> PyResult<()> {
        self.inner_writer()?.decompress_field(name).map_err(to_py_err)
    }

    fn fix(&self) -> PyResult<()> {
        self.inner_writer()?.fix().map_err(to_py_err)
    }

    fn as_reader(&self) -> PyResult<PyTableReader> {
        let reader = self.inner_writer()?.as_reader().map_err(to_py_err)?;
        Ok(PyTableReader {
            inner: Arc::new(reader),
        })
    }

    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        let schema = self.inner_writer()?.schema().map_err(to_py_err)?;
        schema.as_ref().clone().into_pyarrow(py)
    }

    fn statistics(&self, py: Python<'_>) -> PyResult<PyObject> {
        let stats = self.inner_writer()?.statistics().map_err(to_py_err)?;
        let dict = PyDict::new(py);
        dict.set_item("row_count", stats.row_count)?;
        dict.set_item("partition_count", stats.partition_count)?;
        dict.set_item("time_min", stats.time_min)?;
        dict.set_item("time_max", stats.time_max)?;
        if let Some(min) = stats.sym_min {
            dict.set_item("sym_min", min)?;
        }
        if let Some(max) = stats.sym_max {
            dict.set_item("sym_max", max)?;
        }
        Ok(dict.into())
    }

    fn metadata(&self, py: Python<'_>) -> PyResult<PyObject> {
        let meta = self.inner_writer()?.metadata().map_err(to_py_err)?;
        let dict = PyDict::new(py);
        dict.set_item("scheme", meta.partitioning.scheme.to_lowercase())?;
        dict.set_item("partition_count", meta.partitions.len())?;
        let py_partitions = pyo3::types::PyList::empty(py);
        for p in &meta.partitions {
            let p_dict = PyDict::new(py);
            p_dict.set_item("name", &p.name)?;
            p_dict.set_item("time_min", p.time_min)?;
            p_dict.set_item("time_max", p.time_max)?;
            py_partitions.append(p_dict)?;
        }
        dict.set_item("partitions", py_partitions)?;
        dict.set_item("projection_pushdown", meta.capabilities.projection_pushdown)?;
        dict.set_item("predicate_pushdown", meta.capabilities.predicate_pushdown)?;
        dict.set_item("limit_pushdown", meta.capabilities.limit_pushdown)?;
        Ok(dict.into())
    }

    fn path(&self) -> PyResult<String> {
        Ok(self.inner_writer()?.path().to_string_lossy().to_string())
    }

    fn scheme(&self) -> PyResult<String> {
        Ok(self.inner_writer()?.scheme().as_str().to_string())
    }

    fn close(&mut self) -> PyResult<()> {
        if let Some(w) = self.inner.take() {
            w.close().map_err(to_py_err)?;
        }
        Ok(())
    }

    fn remove(&mut self) -> PyResult<()> {
        if let Some(w) = self.inner.take() {
            w.remove().map_err(to_py_err)?;
        }
        Ok(())
    }
}

#[pymodule]
fn _splayed(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPartitionScheme>()?;
    m.add_class::<PyTableBatchReader>()?;
    m.add_class::<PyTableReader>()?;
    m.add_class::<PyTableWriter>()?;
    Ok(())
}
