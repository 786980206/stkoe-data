//! Layer 1 — a single Splayed dataset folder as a `TableProvider`.
//!
//! One directory containing `.meta` + FIELD files is the Splayed analogue of a
//! single Parquet file: a self-contained, queryable unit. A partitioned table
//! (Layer 2) is built out of many of these.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef, TimeUnit as ArrowTimeUnit,
};
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{
    ColumnStatistics, DataFusionError, Result as DFResult, ScalarValue, Statistics,
};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::catalog::TableProvider;

use splayed_core::{
    open_dataset, Dataset, FieldReader, FieldStats, ScanRequest, Scanner, SymbolSelection,
    TimeRange,
};
use splayed_format::{MetaFile, TimeType};

/// 单字段统计：`(字段名, Arrow 类型, null_count, 文件大小, 可选 min/max)`。
type FieldStat = (String, ArrowDataType, u32, u64, Option<(ScalarValue, ScalarValue)>);

/// `init()` 返回：`(Dataset, ArrowSchema, 字段集合, 统计, 错误字符串)`。
type InitState = (Arc<Dataset>, ArrowSchemaRef, HashSet<String>, Option<Statistics>, String);

use crate::exec::SplayedScanExec;
use crate::filter::{classify, parse_filters};

/// A single Splayed dataset folder ("one partition") as a `TableProvider`.
#[derive(Debug, Clone)]
pub struct SplayedDatasetProvider {
    dataset: Arc<Dataset>,
    schema: ArrowSchemaRef,
    symbols: HashSet<String>,
    stats: Option<Statistics>,
    definition: String,
    /// How many DataFusion output partitions this single dataset is split into
    /// for parallel scanning (default 1 = serial).
    scan_parallelism: usize,
}

impl SplayedDatasetProvider {
    /// Open a dataset directory containing `.meta` + FIELD files.
    pub fn new(dir: impl Into<PathBuf>) -> DFResult<Self> {
        let dir = dir.into();
        let (dataset, schema, symbols, stats, definition) = Self::init(&dir)?;
        Ok(Self {
            dataset,
            schema,
            symbols,
            stats,
            definition,
            scan_parallelism: 1,
        })
    }

    /// 重新加载磁盘状态（例如 `splayed_core::update_meta` 之后）。调用方须先
    /// 结束所有正在进行中的查询（本 provider 持有的旧 `Dataset` 会被替换）。
    pub fn reload(&mut self) -> DFResult<()> {
        let dir = self.dataset_dir().to_path_buf();
        let (dataset, schema, symbols, stats, definition) = Self::init(&dir)?;
        self.dataset = dataset;
        self.schema = schema;
        self.symbols = symbols;
        self.stats = stats;
        self.definition = definition;
        Ok(())
    }

    /// 当前数据集目录（供 reload / 上层重建使用）。
    pub fn dataset_dir(&self) -> &std::path::Path {
        &self.dataset.dir
    }

    /// 打开目录并构建 provider 状态（new / reload 共用）。
    fn init(
        dir: &std::path::Path,
    ) -> DFResult<InitState> {
        let dataset = Arc::new(open_dataset(dir).map_err(|e| {
            DataFusionError::Execution(format!("failed to open splayed dataset: {e}"))
        })?);
        let meta = &dataset.meta;

        // Open every FIELD once: type → schema, null_count + size → statistics.
        let field_names = dataset
            .list_fields()
            .map_err(|e| DataFusionError::Execution(format!("list_fields failed: {e}")))?;

        let mut fields: Vec<FieldStat> = Vec::with_capacity(field_names.len());
        for name in &field_names {
            let path = dataset.field_path(name);
            let reader = FieldReader::open(&path).map_err(|e| {
                DataFusionError::Execution(format!("open field '{name}' failed: {e}"))
            })?;
            let arrow_ty = splayed_arrow::splayed_to_arrow_type(reader.data_type());
            let null_count = reader.header().null_count;
            let size = std::fs::metadata(&path)
                .map_err(|e| {
                    DataFusionError::Execution(format!("stat field '{name}' failed: {e}"))
                })?
                .len();
            // FIELD footer 的 min/max 统计（无 footer 或已失效 → None）。
            let stats_pair = reader
                .stats()
                .and_then(|st| field_stats_scalars(&arrow_ty, &st));
            fields.push((name.clone(), arrow_ty, null_count, size, stats_pair));
        }

        let schema = Arc::new(build_schema(meta, &fields));
        let symbols: HashSet<String> = meta.symbols.iter().cloned().collect();
        let stats = compute_statistics(meta, &fields);
        let definition = format!(
            "CREATE EXTERNAL TABLE IF NOT EXISTS splayed STORED AS SPLAYED LOCATION '{}'",
            dir.display()
        );

        Ok((dataset, schema, symbols, Some(stats), definition))
    }

    /// Builder: split this dataset's scan into `n` DataFusion output partitions
    /// (row-balanced, order-preserving) for parallel execution by DataFusion's
    /// multi-threaded runtime. Default 1 (single partition, serial).
    pub fn with_scan_parallelism(mut self, n: usize) -> Self {
        self.scan_parallelism = n;
        self
    }

    /// Runtime switch for the same setting (used by the partitioned-table layer).
    pub fn set_scan_parallelism(&mut self, n: usize) {
        self.scan_parallelism = n;
    }

    /// The underlying dataset (used by the partitioned-table layer).
    pub fn dataset(&self) -> &Arc<Dataset> {
        &self.dataset
    }

    /// The symbol set of this dataset (used by the partitioned-table layer).
    pub fn symbols(&self) -> &HashSet<String> {
        &self.symbols
    }
}

fn build_schema(
    meta: &MetaFile,
    fields: &[FieldStat],
) -> ArrowSchema {
    let time_arrow = match meta.time_type() {
        TimeType::Date32 => ArrowDataType::Date32,
        TimeType::TimestampUs => ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None),
    };
    let mut cols = vec![
        ArrowField::new("time", time_arrow, false),
        ArrowField::new("sym", ArrowDataType::Utf8, false),
    ];
    for (name, ty, _, _, _) in fields {
        cols.push(ArrowField::new(name, ty.clone(), true));
    }
    ArrowSchema::new(cols)
}

/// FIELD footer 的原始槽位 → DataFusion `ScalarValue`（宽度按类型）。
fn field_stats_scalars(aty: &ArrowDataType, st: &FieldStats) -> Option<(ScalarValue, ScalarValue)> {
    let w = match aty {
        ArrowDataType::Boolean | ArrowDataType::Int8 | ArrowDataType::UInt8 => 1,
        ArrowDataType::Int16 | ArrowDataType::UInt16 => 2,
        ArrowDataType::Int32
        | ArrowDataType::UInt32
        | ArrowDataType::Float32
        | ArrowDataType::Date32 => 4,
        ArrowDataType::Int64
        | ArrowDataType::UInt64
        | ArrowDataType::Float64
        | ArrowDataType::Date64
        | ArrowDataType::Timestamp(_, _) => 8,
        _ => return None,
    };
    let one = |b: &[u8]| -> ScalarValue {
        match aty {
            ArrowDataType::Boolean => ScalarValue::Boolean(Some(b[0] != 0)),
            ArrowDataType::Int8 => ScalarValue::Int8(Some(i8::from_le_bytes([b[0]]))),
            ArrowDataType::Int16 => {
                ScalarValue::Int16(Some(i16::from_le_bytes(b[..2].try_into().unwrap())))
            }
            ArrowDataType::Int32 => {
                ScalarValue::Int32(Some(i32::from_le_bytes(b[..4].try_into().unwrap())))
            }
            ArrowDataType::Int64 => {
                ScalarValue::Int64(Some(i64::from_le_bytes(b[..8].try_into().unwrap())))
            }
            ArrowDataType::UInt8 => ScalarValue::UInt8(Some(b[0])),
            ArrowDataType::UInt16 => {
                ScalarValue::UInt16(Some(u16::from_le_bytes(b[..2].try_into().unwrap())))
            }
            ArrowDataType::UInt32 => {
                ScalarValue::UInt32(Some(u32::from_le_bytes(b[..4].try_into().unwrap())))
            }
            ArrowDataType::UInt64 => {
                ScalarValue::UInt64(Some(u64::from_le_bytes(b[..8].try_into().unwrap())))
            }
            ArrowDataType::Float32 => {
                ScalarValue::Float32(Some(f32::from_le_bytes(b[..4].try_into().unwrap())))
            }
            ArrowDataType::Float64 => {
                ScalarValue::Float64(Some(f64::from_le_bytes(b[..8].try_into().unwrap())))
            }
            ArrowDataType::Date32 => {
                ScalarValue::Date32(Some(i32::from_le_bytes(b[..4].try_into().unwrap())))
            }
            ArrowDataType::Date64 => {
                ScalarValue::Date64(Some(i64::from_le_bytes(b[..8].try_into().unwrap())))
            }
            ArrowDataType::Timestamp(_, _) => ScalarValue::TimestampMicrosecond(
                Some(i64::from_le_bytes(b[..8].try_into().unwrap())),
                None,
            ),
            _ => unreachable!(),
        }
    };
    let w = w.min(8);
    Some((one(&st.min[..w]), one(&st.max[..w])))
}

fn compute_statistics(
    meta: &MetaFile,
    fields: &[FieldStat],
) -> Statistics {
    let total_rows = meta.total_rows() as usize;
    let time_elem = meta.time_elem_size();
    let (tmin, tmax) = match (meta.time_axis.first(), meta.time_axis.last()) {
        (Some(&a), Some(&b)) => {
            let mk = |v: i64| match meta.time_type() {
                TimeType::Date32 => ScalarValue::Date32(Some(v as i32)),
                TimeType::TimestampUs => ScalarValue::TimestampMicrosecond(Some(v), None),
            };
            (Precision::Exact(mk(a)), Precision::Exact(mk(b)))
        }
        _ => (Precision::Absent, Precision::Absent),
    };

    let mut column_statistics = Vec::with_capacity(fields.len() + 2);

    // time
    column_statistics.push(ColumnStatistics {
        null_count: Precision::Exact(0),
        max_value: tmax,
        min_value: tmin,
        sum_value: Precision::Absent,
        distinct_count: Precision::Exact(meta.time_axis.len()),
        byte_size: Precision::Exact(total_rows * time_elem),
    });

    // sym (variable-width — byte size unknown)
    column_statistics.push(ColumnStatistics {
        null_count: Precision::Exact(0),
        max_value: Precision::Absent,
        min_value: Precision::Absent,
        sum_value: Precision::Absent,
        distinct_count: Precision::Exact(meta.symbols.len()),
        byte_size: Precision::Absent,
    });

    // FIELD columns (fixed-width only per plan §5.3)
    for (_, ty, null_count, _, stats_pair) in fields {
        let byte_size = fixed_width_bytes(ty).map(|w| Precision::Exact(total_rows * w));
        column_statistics.push(ColumnStatistics {
            null_count: Precision::Exact(*null_count as usize),
            max_value: stats_pair
                .as_ref()
                .map(|(_, mx)| Precision::Exact(mx.clone()))
                .unwrap_or(Precision::Absent),
            min_value: stats_pair
                .as_ref()
                .map(|(mn, _)| Precision::Exact(mn.clone()))
                .unwrap_or(Precision::Absent),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: byte_size.unwrap_or(Precision::Absent),
        });
    }

    let field_bytes: u64 = fields.iter().map(|(_, _, _, s, _)| *s).sum();
    Statistics {
        num_rows: Precision::Exact(total_rows),
        total_byte_size: Precision::Exact(field_bytes as usize),
        column_statistics,
    }
}

fn fixed_width_bytes(ty: &ArrowDataType) -> Option<usize> {
    match ty {
        ArrowDataType::Boolean | ArrowDataType::Int8 | ArrowDataType::UInt8 => Some(1),
        ArrowDataType::Int16 | ArrowDataType::UInt16 => Some(2),
        ArrowDataType::Int32
        | ArrowDataType::UInt32
        | ArrowDataType::Float32
        | ArrowDataType::Date32 => Some(4),
        ArrowDataType::Int64
        | ArrowDataType::UInt64
        | ArrowDataType::Float64
        | ArrowDataType::Timestamp(_, _)
        | ArrowDataType::Date64 => Some(8),
        _ => None,
    }
}

#[async_trait::async_trait]
impl TableProvider for SplayedDatasetProvider {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        Some(&self.definition)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.stats.clone()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(classify(filters, &self.schema, &self.symbols))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.scan_impl(state, projection, filters, limit, None).await
    }
}

/// 分区表层扩展：带符号覆盖的扫描（core 分区计划已把每个分区的符号剪成
/// 有效子集，透传避免分区内缺符号报错）。
impl SplayedDatasetProvider {
    pub(crate) async fn scan_with_symbols(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        symbols: &SymbolSelection,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.scan_impl(state, projection, filters, limit, Some(symbols))
            .await
    }

    async fn scan_impl(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        symbol_override: Option<&SymbolSelection>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_indices: Vec<usize> = match projection {
            Some(indices) => indices.clone(),
            None => (0..self.schema.fields().len()).collect(),
        };

        // DataFusion columns 0=time, 1=sym, 2+=fields.
        let field_names: Vec<String> = projected_indices
            .iter()
            .copied()
            .filter(|i| *i >= 2)
            .map(|i| self.schema.field(i).name().clone())
            .collect();

        let pd = parse_filters(filters, &self.schema, &self.symbols);

        // Ensure pushed-down value-filter columns are read too.
        let mut scan_columns = field_names.clone();
        for f in &pd.value_filters {
            let filter_field = f.field_name();
            if !scan_columns.iter().any(|c| c == filter_field) {
                scan_columns.push(filter_field.to_string());
            }
        }

        // Small LIMITs read fewer rows per batch; the producer still streams
        // until the limit is reached, so this never under-produces.
        let default_batch = 65536usize;
        let batch_size = limit.map(|l| l.min(default_batch)).unwrap_or(default_batch);

        // 符号选择：分区层的有效子集优先（分区表）；否则按解析结果。
        let symbols = symbol_override
            .cloned()
            .unwrap_or(pd.symbols.unwrap_or(SymbolSelection::All));

        let req = ScanRequest {
            columns: scan_columns,
            symbols,
            time_range: pd.time_range.unwrap_or(TimeRange::all()),
            filters: pd.value_filters,
            batch_size,
            parallelism: 1,
            limit,
        };

        let fields: Vec<ArrowField> = projected_indices
            .iter()
            .map(|&i| self.schema.field(i).clone())
            .collect();
        let output_schema = Arc::new(ArrowSchema::new(fields));

        // Partition count: row-balanced slices of the resolved ranges (capped
        // by the number of ranges; >= 1 so the plan always has an output).
        let scanner = Scanner::new(&self.dataset);
        let plan0 = scanner
            .plan(&req)
            .map_err(|e| DataFusionError::Execution(format!("scan plan failed: {e}")))?;
        let partitions = self.scan_parallelism.min(plan0.ranges.len()).max(1);

        Ok(Arc::new(SplayedScanExec::new(
            Arc::clone(&self.dataset),
            output_schema,
            projected_indices,
            req,
            limit,
            partitions,
        )))
    }
}
