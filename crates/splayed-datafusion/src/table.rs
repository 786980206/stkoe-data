//! Layer 2 — a partitioned Splayed table as a `TableProvider`.
//!
//! 分区管理（发现 / schema 合并校验 / TIME / 符号 / 分区统计剪裁）在
//! **`splayed_core::partition`（引擎无关）**——DataFusion / DuckDB 共用同一套
//! 实现；本层只是把 core 分区层的剪裁结果接回 DataFusion 执行计划。

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue, Statistics};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;

use splayed_core::{
    PartitionScanRequest, PartitionedTable, SymbolSelection, TimeRange,
};

use crate::dataset::SplayedDatasetProvider;
use crate::exec::SplayedTableScanExec;
use crate::filter::{classify, parse_filters};

/// A partitioned Splayed table ("Hive partition table" analogue).
#[derive(Debug)]
pub struct SplayedTableProvider {
    dir: PathBuf,
    /// core 分区层（引擎无关：发现/校验/剪裁）。
    table: PartitionedTable,
    /// 每分区一个 dataset provider（schema / statistics / 执行扫描）。
    partitions: Vec<Arc<SplayedDatasetProvider>>,
    schema: ArrowSchemaRef,
    /// Union of symbol sets across all partitions.
    known_symbols: HashSet<String>,
    stats: Option<Statistics>,
}

impl SplayedTableProvider {
    /// Auto-detect the layout of `dir`（走 `splayed_core::partition`）:
    /// - `dir/.meta` exists → single-partition table (backward compatible);
    /// - otherwise → every child directory containing `.meta` is a partition.
    pub fn new(dir: impl Into<PathBuf>) -> DFResult<Self> {
        let dir = dir.into();
        let table = PartitionedTable::open(&dir).map_err(|e| {
            DataFusionError::Execution(format!("SplayedTable open failed: {e}"))
        })?;

        let mut partitions = Vec::with_capacity(table.partition_count());
        let mut schemas: Vec<ArrowSchema> = Vec::with_capacity(table.partition_count());
        for p in table.partitions() {
            let provider = SplayedDatasetProvider::new(&p.path)?;
            schemas.push(provider.schema().as_ref().clone());
            partitions.push(Arc::new(provider));
        }

        // Hive-style: all partitions must share the same schema（core 层已校验，
        // 这里再转一遍 Arrow schema 供执行用）。
        let base = &schemas[0];
        for (i, s) in schemas[1..].iter().enumerate() {
            if !same_schema(base, s) {
                return Err(DataFusionError::Execution(format!(
                    "SplayedTable: partition schema mismatch — partition {} does not match {}",
                    i + 1,
                    0
                )));
            }
        }

        let schema = Arc::new(schemas.remove(0));
        let known_symbols: HashSet<String> = partitions
            .iter()
            .flat_map(|p| p.symbols().iter().cloned())
            .collect();
        let stats = aggregate_statistics(&partitions);

        Ok(Self {
            dir,
            table,
            partitions,
            schema,
            known_symbols,
            stats: Some(stats),
        })
    }

    /// The individual partition providers.
    pub fn partitions(&self) -> &[Arc<SplayedDatasetProvider>] {
        &self.partitions
    }

    /// The engine-agnostic partition layer (useful for reload / debugging).
    pub fn partition_table(&self) -> &PartitionedTable {
        &self.table
    }

    /// Builder: how many output partitions each partition's dataset is split
    /// into (see [`SplayedDatasetProvider::with_scan_parallelism`]). Default 1.
    pub fn with_scan_parallelism(mut self, n: usize) -> Self {
        self.set_scan_parallelism(n);
        self
    }

    /// Runtime switch for the same setting, applied to every partition.
    pub fn set_scan_parallelism(&mut self, n: usize) {
        for p in &mut self.partitions {
            Arc::make_mut(p).set_scan_parallelism(n);
        }
    }
}

fn same_schema(a: &ArrowSchema, b: &ArrowSchema) -> bool {
    a.fields().len() == b.fields().len()
        && a.fields().iter().zip(b.fields().iter()).all(|(x, y)| {
            x.name() == y.name() && x.data_type() == y.data_type()
        })
}

/// Aggregate per-partition statistics into table-level statistics.
fn aggregate_statistics(partitions: &[Arc<SplayedDatasetProvider>]) -> Statistics {
    let mut it = partitions.iter().filter_map(|p| p.statistics());
    let Some(mut agg) = it.next() else {
        return Statistics::default();
    };

    for s in it {
        merge_precision_sum(&mut agg.num_rows, s.num_rows);
        merge_precision_sum(&mut agg.total_byte_size, s.total_byte_size);
        if agg.column_statistics.len() != s.column_statistics.len() {
            return Statistics::default();
        }
        for (c1, c2) in agg.column_statistics.iter_mut().zip(s.column_statistics.iter()) {
            merge_precision_sum(&mut c1.null_count, c2.null_count);
            merge_precision_sum(&mut c1.distinct_count, c2.distinct_count);
            merge_precision_sum(&mut c1.byte_size, c2.byte_size);
            c1.min_value = min_precision(c1.min_value.clone(), c2.min_value.clone());
            c1.max_value = max_precision(c1.max_value.clone(), c2.max_value.clone());
        }
    }
    agg
}

fn merge_precision_sum(a: &mut Precision<usize>, b: Precision<usize>) {
    match (&*a, &b) {
        (Precision::Exact(x), Precision::Exact(y)) => {
            *a = Precision::Exact(x.saturating_add(*y));
        }
        _ => *a = Precision::Absent,
    }
}

fn scalar_i64(s: &ScalarValue) -> Option<i64> {
    match s {
        ScalarValue::Date32(Some(v)) => Some(*v as i64),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v),
        _ => None,
    }
}

fn min_precision(a: Precision<ScalarValue>, b: Precision<ScalarValue>) -> Precision<ScalarValue> {
    match (a, b) {
        (Precision::Exact(x), Precision::Exact(y)) => match (scalar_i64(&x), scalar_i64(&y)) {
            (Some(xv), Some(yv)) => Precision::Exact(if xv <= yv { x } else { y }),
            _ => Precision::Absent,
        },
        _ => Precision::Absent,
    }
}

fn max_precision(a: Precision<ScalarValue>, b: Precision<ScalarValue>) -> Precision<ScalarValue> {
    match (a, b) {
        (Precision::Exact(x), Precision::Exact(y)) => match (scalar_i64(&x), scalar_i64(&y)) {
            (Some(xv), Some(yv)) => Precision::Exact(if xv >= yv { x } else { y }),
            _ => Precision::Absent,
        },
        _ => Precision::Absent,
    }
}

#[async_trait::async_trait]
impl TableProvider for SplayedTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<Statistics> {
        self.stats.clone()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(classify(filters, &self.schema, &self.known_symbols))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_indices: Vec<usize> = match projection {
            Some(indices) => indices.clone(),
            None => (0..self.schema.fields().len()).collect(),
        };
        let fields: Vec<ArrowField> = projected_indices
            .iter()
            .map(|&i| self.schema.field(i).clone())
            .collect();
        let output_schema = Arc::new(ArrowSchema::new(fields));

        // 分区剪裁（core 层：TIME → 符号 → 分区统计）→ 只执行命中的分区。
        let pd = parse_filters(filters, &self.schema, &self.known_symbols);
        let preq = PartitionScanRequest {
            columns: Vec::new(),
            symbols: SymbolSelection::All, // 符号级剪裁仍由 dataset 扫描处理
            time_range: pd.time_range.unwrap_or_else(TimeRange::all),
            filters: pd.value_filters.clone(),
            batch_size: 65536,
            parallelism: 1,
        };
        let pplan = self.table.plan(&preq).map_err(|e| {
            DataFusionError::Execution(format!("SplayedTable plan failed: {e}"))
        })?;

        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(pplan.tasks.len());
        for task in &pplan.tasks {
            plans.push(
                self.partitions[task.partition]
                    .scan(state, projection, filters, limit)
                    .await?,
            );
        }

        if plans.is_empty() {
            // No partition matches — empty scan (e.g. COUNT(*) → 0).
            return Ok(Arc::new(EmptyExec::new(Arc::clone(&output_schema))));
        }

        Ok(Arc::new(SplayedTableScanExec::new(
            self.dir.clone(),
            output_schema,
            plans,
        )))
    }
}