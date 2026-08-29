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

use splayed_core::{open_dataset, Dataset, FieldReader, ScanRequest, Scanner, SymbolSelection, TimeRange};
use splayed_format::{MetaFile, TimeType};

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
        let dataset = Arc::new(open_dataset(&dir).map_err(|e| {
            DataFusionError::Execution(format!("failed to open splayed dataset: {e}"))
        })?);
        let meta = &dataset.meta;

        // Open every FIELD once: type → schema, null_count + size → statistics.
        let field_names = dataset
            .list_fields()
            .map_err(|e| DataFusionError::Execution(format!("list_fields failed: {e}")))?;

        let mut fields: Vec<(String, ArrowDataType, u32, u64)> = Vec::with_capacity(field_names.len());
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
            fields.push((name.clone(), arrow_ty, null_count, size));
        }

        let schema = Arc::new(build_schema(meta, &fields));
        let symbols: HashSet<String> = meta.symbols.iter().cloned().collect();
        let stats = compute_statistics(meta, &fields);
        let definition = format!(
            "CREATE EXTERNAL TABLE IF NOT EXISTS splayed STORED AS SPLAYED LOCATION '{}'",
            dir.display()
        );

        Ok(Self {
            dataset,
            schema,
            symbols,
            stats: Some(stats),
            definition,
            scan_parallelism: 1,
        })
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

fn build_schema(meta: &MetaFile, fields: &[(String, ArrowDataType, u32, u64)]) -> ArrowSchema {
    let time_arrow = match meta.time_type() {
        TimeType::Date32 => ArrowDataType::Date32,
        TimeType::TimestampUs => ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None),
    };
    let mut cols = vec![
        ArrowField::new("time", time_arrow, false),
        ArrowField::new("sym", ArrowDataType::Utf8, false),
    ];
    for (name, ty, _, _) in fields {
        cols.push(ArrowField::new(name, ty.clone(), true));
    }
    ArrowSchema::new(cols)
}

fn compute_statistics(
    meta: &MetaFile,
    fields: &[(String, ArrowDataType, u32, u64)],
) -> Statistics {
    let total_rows = meta.total_rows() as usize;
    let time_elem = meta.time_elem_size() as usize;
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
    for (_, ty, null_count, _) in fields {
        let byte_size = fixed_width_bytes(ty).map(|w| Precision::Exact(total_rows * w));
        column_statistics.push(ColumnStatistics {
            null_count: Precision::Exact(*null_count as usize),
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: byte_size.unwrap_or(Precision::Absent),
        });
    }

    let field_bytes: u64 = fields.iter().map(|(_, _, _, s)| *s).sum();
    Statistics {
        num_rows: Precision::Exact(total_rows),
        total_byte_size: Precision::Exact(field_bytes as usize),
        column_statistics,
    }
}

fn fixed_width_bytes(ty: &ArrowDataType) -> Option<usize> {
    match ty {
        ArrowDataType::Boolean => Some(1),
        ArrowDataType::Int32 | ArrowDataType::Float32 | ArrowDataType::Date32 => Some(4),
        ArrowDataType::Int64 | ArrowDataType::Float64 | ArrowDataType::Timestamp(_, _) => Some(8),
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
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

        let req = ScanRequest {
            columns: scan_columns,
            symbols: pd.symbols.unwrap_or(SymbolSelection::All),
            time_range: pd.time_range.unwrap_or(TimeRange::all()),
            filters: pd.value_filters,
            batch_size,
            parallelism: 1,
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
