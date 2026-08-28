//! Layer 2 — a partitioned Splayed table as a `TableProvider`.
//!
//! The Splayed analogue of a Hive partitioned table: a directory whose direct
//! children each contain their own `.meta` + FIELD files (one "partition" per
//! sub-directory, e.g. `2024/`, `2025/`). A directory that directly contains
//! `.meta` is treated as a single-partition table (backward compatible).

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue, Statistics};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;

use splayed_format::META_FILE_NAME;

use crate::dataset::SplayedDatasetProvider;
use crate::exec::SplayedTableScanExec;
use crate::filter::{classify, parse_filters};

/// A partitioned Splayed table ("Hive partition table" analogue).
#[derive(Debug)]
pub struct SplayedTableProvider {
    dir: PathBuf,
    partitions: Vec<Arc<SplayedDatasetProvider>>,
    schema: ArrowSchemaRef,
    /// Union of symbol sets across all partitions.
    known_symbols: HashSet<String>,
    /// Per-partition `[time_min, time_max]` (inclusive) for partition pruning.
    time_ranges: Vec<(i64, i64)>,
    stats: Option<Statistics>,
}

impl SplayedTableProvider {
    /// Auto-detect the layout of `dir`:
    /// - `dir/.meta` exists → single-partition table (backward compatible);
    /// - otherwise → every child directory containing `.meta` is a partition.
    pub fn new(dir: impl Into<PathBuf>) -> DFResult<Self> {
        let dir = dir.into();
        let partition_dirs = discover_partitions(&dir)?;

        let mut partitions = Vec::with_capacity(partition_dirs.len());
        let mut schemas: Vec<ArrowSchema> = Vec::with_capacity(partition_dirs.len());
        for pd in &partition_dirs {
            let provider = SplayedDatasetProvider::new(pd)?;
            schemas.push(provider.schema().as_ref().clone());
            partitions.push(Arc::new(provider));
        }

        // Hive-style: all partitions must share the same schema.
        let base = &schemas[0];
        for (i, s) in schemas[1..].iter().enumerate() {
            if !same_schema(base, s) {
                return Err(DataFusionError::Execution(format!(
                    "SplayedTable: partition schema mismatch — {} does not match {}",
                    partition_dirs[i + 1].display(),
                    partition_dirs[0].display()
                )));
            }
        }

        let schema = Arc::new(schemas.remove(0));
        let known_symbols: HashSet<String> = partitions
            .iter()
            .flat_map(|p| p.symbols().iter().cloned())
            .collect();
        let time_ranges = partitions
            .iter()
            .map(|p| {
                let axis = &p.dataset().meta.time_axis;
                (
                    axis.first().copied().unwrap_or(i64::MIN),
                    axis.last().copied().unwrap_or(i64::MAX),
                )
            })
            .collect();
        let stats = aggregate_statistics(&partitions);

        Ok(Self {
            dir,
            partitions,
            schema,
            known_symbols,
            time_ranges,
            stats: Some(stats),
        })
    }

    /// The individual partition providers.
    pub fn partitions(&self) -> &[Arc<SplayedDatasetProvider>] {
        &self.partitions
    }
}

/// Find the dataset directories that make up this table.
fn discover_partitions(dir: &Path) -> DFResult<Vec<PathBuf>> {
    if dir.join(META_FILE_NAME).exists() {
        // Single dataset folder = single-partition table.
        return Ok(vec![dir.to_path_buf()]);
    }

    let mut out: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(dir)
        .map_err(|e| DataFusionError::Execution(format!("read_dir {} failed: {e}", dir.display())))?
    {
        let entry = entry.map_err(|e| {
            DataFusionError::Execution(format!("read_dir entry failed: {e}"))
        })?;
        let p = entry.path();
        if p.is_dir() && p.join(META_FILE_NAME).exists() {
            out.push(p);
        }
    }

    if out.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "SplayedTable: no partition (directory containing .meta) found under {}",
            dir.display()
        )));
    }

    out.sort();
    Ok(out)
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

        // Partition pruning: skip partitions whose time range cannot overlap.
        let pd = parse_filters(filters, &self.schema, &self.known_symbols);
        let time_range = pd.time_range;

        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(self.partitions.len());
        for (i, part) in self.partitions.iter().enumerate() {
            if let Some(tr) = &time_range {
                let (pmin, pmax) = self.time_ranges[i];
                // Half-open [start, end) overlap check.
                if tr.end <= pmin || tr.start >= pmax {
                    continue; // pruned
                }
            }
            plans.push(part.scan(state, projection, filters, limit).await?);
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
