//! ADBC 风格的引擎无关连接组件（`splayed-adbc`）。
//!
//! 面向任意外部数据引擎的统一 Splayed 读取口：定义 `Connection` / `Statement` /
//! 流式结果（`CoreBatchStream` 与零拷贝 `ArrowStream`），不依赖 DataFusion、
//! DuckDB，也不依赖任何 async runtime。数据引擎侧（DataFusion / DuckDB /
//! Velox / Flink 等）经由统一接口接入，`splayed-arrow::corebatch_into_record_batch`
//! 是它们共享的零拷贝 Arrow 桥。
//!
//! 说明：这是 ADBC *风格* 的原生查询 API（投影/过滤/limit/并行度），SQL 解析与
//! ADBC C ABI / `adbc.h` FFI 绑定留待后续（届时在本接口上加薄层即可）。

use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit as ArrowTimeUnit,
};

use splayed_core::{
    CoreBatch, Dataset, Filter, ScanRequest, Scanner, SymbolSelection, TimeRange, open_dataset,
    scan_owned_parallel,
};
use splayed_format::TimeType;

/// 统一错误类型。
#[derive(Debug)]
pub enum AdbcError {
    OpenDataset(String),
    Scan(String),
    Arrow(String),
    Io(std::io::Error),
}

impl std::fmt::Display for AdbcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenDataset(s) => write!(f, "open dataset failed: {s}"),
            Self::Scan(s) => write!(f, "scan failed: {s}"),
            Self::Arrow(s) => write!(f, "arrow conversion failed: {s}"),
            Self::Io(e) => write!(f, "io error: {e}"),
        }
    }
}
impl std::error::Error for AdbcError {}

/// 连接：持有打开的数据集，产出 `Statement`。
#[derive(Debug, Clone)]
pub struct Connection {
    dataset: Arc<Dataset>,
}

impl Connection {
    /// 打开一个 dataset 目录（含 `.meta`）。
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, AdbcError> {
        let dataset =
            Arc::new(open_dataset(dir).map_err(|e| AdbcError::OpenDataset(e.to_string()))?);
        Ok(Self { dataset })
    }

    pub fn dataset(&self) -> &Arc<Dataset> {
        &self.dataset
    }

    /// 逻辑 schema：`time, sym, <fields...>`（与 DataFusion/DuckDB 适配层一致）。
    pub fn arrow_schema(&self) -> SchemaRef {
        build_arrow_schema(&self.dataset)
    }

    /// 创建一个查询语句（默认全字段、无过滤、全符号、全时间）。
    pub fn statement(&self) -> Statement {
        Statement {
            dataset: Arc::clone(&self.dataset),
            columns: self.dataset.list_fields().unwrap_or_default(),
            filters: Vec::new(),
            symbols: SymbolSelection::All,
            time_range: TimeRange::all(),
            limit: None,
            batch_size: 65536,
            parallelism: 1,
        }
    }
}

/// 一次扫描请求（投影 / 过滤 / 符号 / 时间 / limit / 并行度），engine-agnostic。
#[derive(Debug, Clone)]
pub struct Statement {
    dataset: Arc<Dataset>,
    columns: Vec<String>,
    filters: Vec<Filter>,
    symbols: SymbolSelection,
    time_range: TimeRange,
    limit: Option<usize>,
    batch_size: usize,
    parallelism: usize,
}

impl Statement {
    /// 投影 FIELD 列（空 = 仅 time/sym）。
    pub fn select(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    /// 投影全部 FIELD 列（等效于 `SELECT time, sym, <all fields>`）。
    pub fn select_all(mut self) -> Self {
        self.columns = self.dataset.list_fields().unwrap_or_default();
        self
    }

    /// 追加一个值级过滤（AND 语义，多过滤全部生效）。
    pub fn filter(mut self, f: Filter) -> Self {
        self.filters.push(f);
        self
    }

    /// 符号选择。
    pub fn symbols(mut self, syms: SymbolSelection) -> Self {
        self.symbols = syms;
        self
    }

    /// 时间范围（半开 `[start, end)`）。
    pub fn time_range(mut self, tr: TimeRange) -> Self {
        self.time_range = tr;
        self
    }

    /// LIMIT 提前截断。
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// 批大小。
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    /// 并行扫描线程数（有序流式，`split_ranges` 行均衡切片）。
    pub fn parallelism(mut self, n: usize) -> Self {
        self.parallelism = n.max(1);
        self
    }

    /// 执行：产出引擎无关的 `CoreBatch` 流。
    pub fn execute(&self) -> Result<CoreBatchStream, AdbcError> {
        let req = ScanRequest {
            columns: self.columns.clone(),
            symbols: self.symbols.clone(),
            time_range: self.time_range,
            filters: self.filters.clone(),
            batch_size: self.batch_size,
            parallelism: 1,
        };
        let scanner = Scanner::new(&self.dataset);
        let plan = scanner
            .plan(&req)
            .map_err(|e| AdbcError::Scan(e.to_string()))?;
        let inner = scan_owned_parallel(Arc::clone(&self.dataset), &plan, &req, self.parallelism)
            .map_err(|e| AdbcError::Scan(e.to_string()))?;
        Ok(CoreBatchStream {
            inner,
            remaining: self.limit,
        })
    }

    /// 执行：产出零拷贝 Arrow `RecordBatch` 流。
    pub fn execute_arrow(&self) -> Result<ArrowStream, AdbcError> {
        let inner = self.execute()?;
        let sym_dict = Arc::new(arrow::array::StringArray::from_iter_values(
            self.dataset.meta.symbols.iter().map(|s| s.as_str()),
        ));
        let schema = build_arrow_schema(&self.dataset);
        Ok(ArrowStream {
            inner,
            sym_dict,
            schema,
        })
    }
}

/// 引擎无关的 CoreBatch 结果流（同步迭代）。
pub struct CoreBatchStream {
    inner: splayed_core::ParallelScanBatches,
    remaining: Option<usize>,
}

impl CoreBatchStream {
    pub fn next_batch(&mut self) -> Result<Option<CoreBatch>, AdbcError> {
        if let Some(n) = self.remaining {
            if n == 0 {
                return Ok(None);
            }
        }
        let Some(batch) = self
            .inner
            .next_batch()
            .map_err(|e| AdbcError::Scan(e.to_string()))?
        else {
            return Ok(None);
        };
        if let Some(n) = self.remaining {
            if batch.num_rows() >= n {
                let sliced = batch.slice(0, n);
                self.remaining = Some(0);
                return Ok(Some(sliced));
            }
            self.remaining = Some(n - batch.num_rows());
        }
        Ok(Some(batch))
    }
}

impl Iterator for CoreBatchStream {
    type Item = Result<CoreBatch, AdbcError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

/// 零拷贝 Arrow RecordBatch 结果流。
pub struct ArrowStream {
    inner: CoreBatchStream,
    sym_dict: Arc<arrow::array::StringArray>,
    schema: SchemaRef,
}

impl ArrowStream {
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>, AdbcError> {
        let Some(cb) = self.inner.next_batch()? else {
            return Ok(None);
        };
        let indices: Vec<usize> = (0..self.schema.fields().len()).collect();
        let fields: Vec<Field> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let rb = splayed_arrow::corebatch_into_record_batch(
            cb,
            &indices,
            &fields,
            Some(Arc::clone(&self.sym_dict)),
        )
        .map_err(|e| AdbcError::Arrow(e.to_string()))?;
        Ok(Some(rb))
    }
}

impl Iterator for ArrowStream {
    type Item = Result<RecordBatch, AdbcError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

/// 逻辑 schema：`time, sym, <fields...>`（与 DataFusion/DuckDB 适配层一致）。
pub fn build_arrow_schema(dataset: &Dataset) -> SchemaRef {
    let time_arrow = match dataset.meta.time_type() {
        TimeType::Date32 => ArrowDataType::Date32,
        TimeType::TimestampUs => ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None),
    };
    let mut fields = vec![
        Field::new("time", time_arrow, false),
        Field::new("sym", ArrowDataType::Utf8, false),
    ];
    if let Ok(names) = dataset.list_fields() {
        for name in names {
            if let Ok(reader) = splayed_core::FieldReader::open(dataset.field_path(&name)) {
                fields.push(Field::new(
                    &name,
                    splayed_arrow::splayed_to_arrow_type(reader.data_type()),
                    true,
                ));
            }
        }
    }
    Arc::new(Schema::new(fields))
}