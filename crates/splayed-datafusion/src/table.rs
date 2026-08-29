//! Layer 2 — a partitioned Splayed table as a `TableProvider`.
//!
//! 分区管理（发现 / schema 合并校验 / TIME / 符号 / 分区统计剪裁）在
//! **`splayed_core::partition`（引擎无关）**——DataFusion / DuckDB 共用同一套
//! 实现；本层只是把 core 分区层的剪裁结果接回 DataFusion 执行计划。

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue, Statistics};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::{Literal, col};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use splayed_core::{
    Filter, PartitionColumnKind, PartitionScanRequest, PartitionedTable, SymbolSelection,
    TimeRange,
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

        // 表 schema = 数据集 schema + 声明式分区列（追加在末尾）。
        let schema = Arc::new(with_partition_columns(schema.as_ref(), &table));

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

/// dataset schema + 声明式分区列（追加在末尾）。
fn with_partition_columns(schema: &ArrowSchema, table: &PartitionedTable) -> ArrowSchema {
    let mut fields: Vec<ArrowField> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    for pc in table.partition_columns() {
        let ty = match pc.kind {
            PartitionColumnKind::Int64 => ArrowDataType::Int64,
            PartitionColumnKind::String => ArrowDataType::Utf8,
        };
        fields.push(ArrowField::new(pc.name.clone(), ty, false));
    }
    ArrowSchema::new(fields)
}

/// 单表达式（引用分区列）→ core `Filter`（列 op 字面量；剥恒等 cast）。
fn partition_filter_from_expr(expr: &Expr, part_cols: &HashSet<String>) -> Option<Filter> {
    use datafusion::logical_expr::{BinaryExpr, Operator};
    use splayed_core::FilterValue;

    // 剥除 cast 包裹（DF55：Cast(Box<Expr> + 目标类型) 的元组变体）。
    let mut e = expr;
    while let Expr::Cast(cast) = e {
        e = cast.expr.as_ref();
    }
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = e else {
        return None;
    };
    let (field, lit) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) => (c.name.to_string(), v),
        (Expr::Literal(v, _), Expr::Column(c)) => (c.name.to_string(), v),
        _ => return None,
    };
    if !part_cols.contains(field.as_str()) {
        return None;
    }
    let fv = match lit {
        ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) => {
            FilterValue::String(s.clone().unwrap_or_default())
        }
        ScalarValue::Int8(v) => FilterValue::Int64(v.unwrap_or_default() as i64),
        ScalarValue::Int16(v) => FilterValue::Int64(v.unwrap_or_default() as i64),
        ScalarValue::Int32(v) => FilterValue::Int64(v.unwrap_or_default() as i64),
        ScalarValue::Int64(v) => FilterValue::Int64(v.unwrap_or_default()),
        ScalarValue::UInt32(v) => FilterValue::Int64(v.unwrap_or_default() as i64),
        ScalarValue::UInt64(v) => FilterValue::Int64(v.unwrap_or_default() as i64),
        _ => return None,
    };
    Some(match op {
        Operator::Eq => Filter::Equal { field, value: fv },
        Operator::NotEq => Filter::NotEqual { field, value: fv },
        Operator::Gt => Filter::GreaterThan { field, value: fv },
        Operator::GtEq => Filter::GreaterOrEqual { field, value: fv },
        Operator::Lt => Filter::LessThan { field, value: fv },
        Operator::LtEq => Filter::LessOrEqual { field, value: fv },
        _ => return None,
    })
}

/// 某分区某分区列的值 → ScalarValue（表投影的常量列）。
fn partition_scalar(kind: PartitionColumnKind, value: &str) -> ScalarValue {
    match kind {
        PartitionColumnKind::Int64 => {
            ScalarValue::Int64(value.parse::<i64>().ok())
        }
        PartitionColumnKind::String => ScalarValue::Utf8(Some(value.to_string())),
    }
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

        let dataset_cols = self.table.fields().len() + 2; // time + sym + fields
        let has_part_cols = !self.table.partition_columns().is_empty();

        // 分流：引用分区列的表达式 → 分区列剪裁（core plan）；其余 → dataset。
        let part_cols: HashSet<String> = self
            .table
            .partition_columns()
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let mut dataset_exprs: Vec<Expr> = Vec::new();
        let mut partition_filters = Vec::new();
        for expr in filters {
            let mut hits = HashSet::new();
            let _ = expr.apply(|e| {
                if let Expr::Column(c) = e {
                    if part_cols.contains(c.name.as_str()) {
                        hits.insert(c.name.to_string());
                    }
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            });
            if !hits.is_empty() {
                if let Some(f) = partition_filter_from_expr(expr, &part_cols) {
                    partition_filters.push(f);
                }
                continue; // 分区列表达式不传给 dataset
            }
            dataset_exprs.push(expr.clone());
        }

        // 分区剪裁（core 层：TIME → 符号 → 分区统计 → 分区列）。
        let pd = parse_filters(filters, &self.schema, &self.known_symbols);
        // 值过滤只取「非分区列」部分（分区列已进 partition_filters，且其
        // 字段在 dataset 中不存在，不能进 partition_may_match 的 footer 检查）。
        let dataset_filters: Vec<_> = pd
            .value_filters
            .iter()
            .filter(|f| !part_cols.contains(f.field_name()))
            .cloned()
            .collect();
        let preq = PartitionScanRequest {
            columns: Vec::new(),
            symbols: SymbolSelection::All, // 符号级剪裁由 dataset 扫描处理
            time_range: pd.time_range.unwrap_or_else(TimeRange::all),
            filters: dataset_filters.clone(),
            partition_filters,
            batch_size: 65536,
            parallelism: 1,
        };
        let pplan = self.table.plan(&preq).map_err(|e| {
            DataFusionError::Execution(format!("SplayedTable plan failed: {e}"))
        })?;

        // dataset 投影 = 去掉分区列下标（分区列由常量投影补回）。
        let dataset_projection: Vec<usize> = projected_indices
            .iter()
            .copied()
            .filter(|&i| i < dataset_cols)
            .collect();

        let mut plans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(pplan.tasks.len());
        for task in &pplan.tasks {
            let child = self.partitions[task.partition]
                .scan(state, Some(&dataset_projection), &dataset_exprs, limit)
                .await?;

            if !has_part_cols {
                plans.push(child);
                continue;
            }

            // ProjectionExec：数据集列按名取 + 分区列按分区 declared 值做常量列。
            let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::new();
            for &idx in &projected_indices {
                if idx >= dataset_cols {
                    let pos = idx - dataset_cols;
                    let pc = &self.table.partition_columns()[pos];
                    let value = self
                        .table
                        .partitions()[task.partition]
                        .declared
                        .iter()
                        .find(|(k, _)| k == &pc.name)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default();
                    exprs.push((
                        Arc::new(Literal::new(partition_scalar(pc.kind, &value))),
                        pc.name.clone(),
                    ));
                } else {
                    let name = self.schema.field(idx).name().clone();
                    exprs.push((col(&name, &child.schema())?, name));
                }
            }
            plans.push(Arc::new(ProjectionExec::try_new(exprs, child)?));
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