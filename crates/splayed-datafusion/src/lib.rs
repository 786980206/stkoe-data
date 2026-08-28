//! Splayed V1 DataFusion adapter — `TableProvider` implementation.
//!
//! See `plan.md` §8 (DataFusion 集成) and §10.3.
//!
//! Provides `SplayedTableProvider` which implements DataFusion's
//! `TableProvider` trait, enabling SQL queries like:
//! ```sql
//! SELECT close FROM splayed_table WHERE sym = 'AAPL' AND time >= 100
//! ```
//!
//! Pushdown support:
//! - Projection pushdown: only opens requested FIELD files.
//! - Predicate pushdown: SYM → SymbolSelection, TIME → TimeRange.
//!   Value filters (e.g. `close > 100`) are pushed to the Scanner.
//!
//! Note: DataFusion re-exports its own Arrow crates as `datafusion::arrow::*`.
//! We use those re-exports to guarantee version compatibility.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array as ArrowArray, RecordBatch as DFRecordBatch, NullArray as DFNullArray,
    builder::{
        Date32Builder as DFDate32Builder,
        TimestampMicrosecondBuilder as DFTimestampMicrosecondBuilder,
    },
};
use datafusion::arrow::datatypes::{
    DataType as DFDataType, Field as DFField, Schema as DFSchema, SchemaRef as DFSchemaRef,
    TimeUnit as DFTimeUnit,
};
use datafusion::catalog::Session;
use datafusion::common::{tree_node::TreeNodeRecursion, DataFusionError, Result as DFResult};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

use splayed_arrow::column_view_to_arrow;
use splayed_core::{
    open_dataset, Dataset, Filter as SplayedFilter, FilterValue, ScanRequest, Scanner,
    SymbolSelection, TimeRange,
};
use splayed_format::{DataType as SplayedDataType, TimeType};

/// A DataFusion `TableProvider` backed by a Splayed dataset directory.
#[derive(Debug)]
pub struct SplayedTableProvider {
    dataset_dir: PathBuf,
    schema: DFSchemaRef,
}

impl SplayedTableProvider {
    /// Create a new provider from a dataset directory path.
    pub fn new(dataset_dir: impl Into<PathBuf>) -> DFResult<Self> {
        let dir = dataset_dir.into();
        let dataset = open_dataset(&dir).map_err(|e| {
            DataFusionError::Execution(format!("failed to open splayed dataset: {e}"))
        })?;
        let schema = Arc::new(build_schema(&dataset)?);
        Ok(Self {
            dataset_dir: dir,
            schema,
        })
    }
}

fn build_schema(dataset: &Dataset) -> DFResult<DFSchema> {
    let meta = &dataset.meta;
    let time_type = meta.time_type();
    let mut fields = Vec::new();

    let time_arrow_type = match time_type {
        TimeType::Date32 => DFDataType::Date32,
        TimeType::TimestampUs => DFDataType::Timestamp(DFTimeUnit::Microsecond, None),
    };
    fields.push(DFField::new("time", time_arrow_type, true));
    fields.push(DFField::new("sym", DFDataType::Utf8, true));

    let field_names = dataset
        .list_fields()
        .map_err(|e| DataFusionError::Execution(format!("list_fields failed: {e}")))?;

    for name in field_names {
        let path = dataset.field_path(&name);
        let reader = splayed_core::FieldReader::open(&path).map_err(|e| {
            DataFusionError::Execution(format!("open field '{name}' failed: {e}"))
        })?;
        // Use the shared type mapper from splayed-arrow (avoids D1 duplication).
        let arrow_ty = splayed_arrow::splayed_to_arrow_type(reader.data_type());
        fields.push(DFField::new(&name, arrow_ty, true));
    }

    Ok(DFSchema::new(fields))
}

#[async_trait::async_trait]
impl TableProvider for SplayedTableProvider {
    fn schema(&self) -> DFSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // All binary comparisons on known columns are pushdownable:
        // - SYM equality → SymbolSelection
        // - TIME comparison → TimeRange
        // - Value comparison → SplayedFilter
        Ok(filters
            .iter()
            .map(|f| {
                if is_pushdownable(f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Resolve which columns (by full-schema index) are projected.
        let projected_indices: Vec<usize> = match projection {
            Some(indices) => indices.clone(),
            None => (0..self.schema.fields().len()).collect(),
        };

        // Determine which FIELD files to read (indices >= 2).
        let field_names: Vec<String> = projected_indices
            .iter()
            .copied()
            .filter(|i| *i >= 2)
            .map(|i| self.schema.field(i).name().clone())
            .collect();

        let include_time = projected_indices.contains(&0);
        let include_sym = projected_indices.contains(&1);

        let (symbol_sel, time_range, value_filter) = parse_filters(filters, &self.schema);

        // Ensure filter columns are read too.
        let mut scan_columns = field_names.clone();
        if let Some(ref filter) = value_filter {
            let filter_field = filter.field_name();
            if !scan_columns.iter().any(|c| c == filter_field) {
                scan_columns.push(filter_field.to_string());
            }
        }

        let req = ScanRequest {
            columns: scan_columns,
            symbols: symbol_sel.unwrap_or(SymbolSelection::All),
            time_range: time_range.unwrap_or(TimeRange::all()),
            filter: value_filter,
            batch_size: 65536,
            parallelism: 1,
        };

        // Build the output schema from projected indices.
        let output_fields: Vec<DFField> = projected_indices
            .iter()
            .map(|&i| self.schema.field(i).clone())
            .collect();
        let output_schema = Arc::new(DFSchema::new(output_fields));

        Ok(Arc::new(SplayedScanPlanExec::new(
            Arc::clone(&self.schema),
            output_schema,
            self.dataset_dir.clone(),
            req,
            projected_indices,
            include_time,
            include_sym,
        )))
    }
}

// ---------------------------------------------------------------------------
// Filter parsing
// ---------------------------------------------------------------------------

fn is_pushdownable(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(bin) => {
            let is_column = |e: &Expr| -> bool { matches!(e, Expr::Column(_)) };
            let is_literal = |e: &Expr| -> bool { matches!(e, Expr::Literal(..)) };
            // Any Column <op> Literal (or swapped) is pushdownable:
            // SYM → SymbolSelection, TIME → TimeRange, FIELD → SplayedFilter
            (is_column(&bin.left) && is_literal(&bin.right))
                || (is_literal(&bin.left) && is_column(&bin.right))
        }
        _ => false,
    }
}

fn parse_filters(
    filters: &[Expr],
    schema: &DFSchema,
) -> (Option<SymbolSelection>, Option<TimeRange>, Option<SplayedFilter>) {
    let mut symbols: Vec<String> = Vec::new();
    let mut time_start = i64::MIN;
    let mut time_end = i64::MAX;
    let mut value_filter: Option<SplayedFilter> = None;

    for expr in filters {
        match expr {
            Expr::BinaryExpr(bin) => {
                let (col_name, lit_val, swapped) = match (&bin.left, &bin.right) {
                    (boxed_l, boxed_r) => match (boxed_l.as_ref(), boxed_r.as_ref()) {
                        (Expr::Column(c), Expr::Literal(v, _)) => {
                            (c.name.clone(), v.clone(), false)
                        }
                        (Expr::Literal(v, _), Expr::Column(c)) => {
                            (c.name.clone(), v.clone(), true)
                        }
                        _ => continue,
                    },
                };

                match col_name.as_str() {
                    "sym" => {
                        if let Some(s) = scalar_to_string(&lit_val) {
                            symbols.push(s);
                        }
                    }
                    "time" => {
                        if let Some(t) = extract_int_from_scalar(&lit_val) {
                            let op = if swapped { invert_op(&bin.op) } else { bin.op.clone() };
                            match op {
                                Operator::Eq => {
                                    time_start = t;
                                    time_end = t + 1;
                                }
                                Operator::Gt => time_start = t + 1,
                                Operator::GtEq => time_start = t,
                                Operator::Lt => time_end = t,
                                Operator::LtEq => time_end = t + 1,
                                _ => {}
                            }
                        }
                    }
                    other => {
                        if value_filter.is_none() {
                            let op = if swapped { invert_op(&bin.op) } else { bin.op.clone() };
                            if let Some(f) = build_value_filter(other, &op, &lit_val, schema) {
                                value_filter = Some(f);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let symbol_sel = if symbols.is_empty() {
        None
    } else {
        Some(SymbolSelection::syms(symbols))
    };

    let time_range = if time_start == i64::MIN && time_end == i64::MAX {
        None
    } else {
        Some(TimeRange::new(time_start, time_end))
    };

    (symbol_sel, time_range, value_filter)
}

fn invert_op(op: &Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        other => other.clone(),
    }
}

fn scalar_to_string(scalar: &datafusion::common::ScalarValue) -> Option<String> {
    use datafusion::common::ScalarValue;
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

fn extract_int_from_scalar(scalar: &datafusion::common::ScalarValue) -> Option<i64> {
    use datafusion::common::ScalarValue;
    match scalar {
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Date32(Some(v)) => Some(*v as i64),
        ScalarValue::Date64(Some(v)) => Some(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v),
        _ => None,
    }
}

fn build_value_filter(
    field: &str,
    op: &Operator,
    scalar: &datafusion::common::ScalarValue,
    schema: &DFSchema,
) -> Option<SplayedFilter> {
    let arrow_field = schema.field_with_name(field).ok()?;
    let splayed_ty = splayed_arrow::arrow_to_splayed_type(arrow_field.data_type())?;
    let fv = scalar_to_filter_value(scalar, splayed_ty)?;

    Some(match op {
        Operator::Gt => SplayedFilter::GreaterThan {
            field: field.to_string(),
            value: fv,
        },
        Operator::GtEq => SplayedFilter::GreaterOrEqual {
            field: field.to_string(),
            value: fv,
        },
        Operator::Lt => SplayedFilter::LessThan {
            field: field.to_string(),
            value: fv,
        },
        Operator::LtEq => SplayedFilter::LessOrEqual {
            field: field.to_string(),
            value: fv,
        },
        Operator::Eq => SplayedFilter::Equal {
            field: field.to_string(),
            value: fv,
        },
        Operator::NotEq => SplayedFilter::NotEqual {
            field: field.to_string(),
            value: fv,
        },
        _ => return None,
    })
}

fn scalar_to_filter_value(
    scalar: &datafusion::common::ScalarValue,
    splayed_ty: SplayedDataType,
) -> Option<FilterValue> {
    use datafusion::common::ScalarValue;
    match (splayed_ty, scalar) {
        (SplayedDataType::Bool, ScalarValue::Boolean(Some(v))) => Some(FilterValue::Bool(*v)),
        (SplayedDataType::Int32, ScalarValue::Int32(Some(v))) => Some(FilterValue::Int32(*v)),
        (SplayedDataType::Int64, ScalarValue::Int64(Some(v))) => Some(FilterValue::Int64(*v)),
        (SplayedDataType::Float32, ScalarValue::Float32(Some(v))) => Some(FilterValue::Float32(*v)),
        (SplayedDataType::Float64, ScalarValue::Float64(Some(v))) => Some(FilterValue::Float64(*v)),
        (SplayedDataType::Date32, ScalarValue::Int32(Some(v))) => Some(FilterValue::Date32(*v)),
        (SplayedDataType::Date32, ScalarValue::Date32(Some(v))) => Some(FilterValue::Date32(*v)),
        (SplayedDataType::TimestampUs, ScalarValue::Int64(Some(v))) => {
            Some(FilterValue::TimestampUs(*v))
        }
        (SplayedDataType::TimestampUs, ScalarValue::TimestampMicrosecond(Some(v), _)) => {
            Some(FilterValue::TimestampUs(*v))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Execution plan
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SplayedScanPlanExec {
    dataset_dir: PathBuf,
    /// Output schema (after projection — only projected columns).
    output_schema: DFSchemaRef,
    /// Indices into the full table schema that are projected.
    projected_indices: Vec<usize>,
    scan_request: ScanRequest,
    properties: Arc<PlanProperties>,
}

impl SplayedScanPlanExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        _full_schema: DFSchemaRef,
        output_schema: DFSchemaRef,
        dataset_dir: PathBuf,
        scan_request: ScanRequest,
        projected_indices: Vec<usize>,
        _include_time: bool,
        _include_sym: bool,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(output_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self {
            dataset_dir,
            output_schema,
            projected_indices,
            scan_request,
            properties,
        }
    }
}

impl DisplayAs for SplayedScanPlanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SplayedScan: {}", self.dataset_dir.display())
    }
}

impl ExecutionPlan for SplayedScanPlanExec {
    fn name(&self) -> &str {
        "SplayedScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Leaf node — return self unchanged.
        Ok(self)
    }

    fn apply_expressions(
        &self,
        _op: &mut dyn FnMut(&Arc<dyn datafusion::physical_plan::PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let dataset = open_dataset(&self.dataset_dir).map_err(|e| {
            DataFusionError::Execution(format!("open dataset failed: {e}"))
        })?;

        let scanner = Scanner::new(&dataset);
        let plan = scanner.plan(&self.scan_request).map_err(|e| {
            DataFusionError::Execution(format!("scan plan failed: {e}"))
        })?;

        let mut batch_iter = scanner.scan(&plan, &self.scan_request).map_err(|e| {
            DataFusionError::Execution(format!("scan failed: {e}"))
        })?;

        let meta = dataset.meta.clone();
        let output_schema = Arc::clone(&self.output_schema);
        let projected_indices = self.projected_indices.clone();

        // Collect all batches synchronously.
        let mut collected: Vec<DFRecordBatch> = Vec::new();
        while let Some(batch) = batch_iter.next_batch().map_err(|e| {
            DataFusionError::Execution(format!("scan batch error: {e}"))
        })? {
            collected.push(batch_to_record_batch(
                &batch,
                &output_schema,
                &meta,
                &projected_indices,
            ));
        }

        let stream = futures::stream::iter(collected.into_iter().map(Ok::<_, DataFusionError>));

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, stream)))
    }
}

/// Convert a Splayed ScanBatchOwned into a DataFusion RecordBatch.
/// `projected_indices` are indices into the *full* table schema.
/// `output_schema` has one field per projected index, in the same order.
fn batch_to_record_batch(
    batch: &splayed_core::ScanBatchOwned,
    output_schema: &DFSchemaRef,
    meta: &splayed_format::MetaFile,
    projected_indices: &[usize],
) -> DFRecordBatch {
    let row_count = batch.row_count;
    let time_type = meta.time_type();
    let mut arrow_cols: Vec<Arc<dyn ArrowArray>> = Vec::with_capacity(projected_indices.len());

    for (out_pos, &full_idx) in projected_indices.iter().enumerate() {
        let field_name = output_schema.field(out_pos).name();
        match full_idx {
            0 => {
                // TIME column
                match time_type {
                    TimeType::Date32 => {
                        let mut b = DFDate32Builder::with_capacity(row_count);
                        for &t in &batch.time_values {
                            b.append_value(t as i32);
                        }
                        arrow_cols.push(Arc::new(b.finish()));
                    }
                    TimeType::TimestampUs => {
                        let mut b = DFTimestampMicrosecondBuilder::with_capacity(row_count);
                        for &t in &batch.time_values {
                            b.append_value(t);
                        }
                        arrow_cols.push(Arc::new(b.finish()));
                    }
                }
            }
            1 => {
                // SYM column
                let mut b = datafusion::arrow::array::builder::StringBuilder::with_capacity(
                    row_count,
                    row_count * 8,
                );
                for &sym_idx in &batch.sym_indices {
                    let sym = &meta.symbols[sym_idx];
                    b.append_value(sym);
                }
                arrow_cols.push(Arc::new(b.finish()));
            }
            _ => {
                // FIELD column — find by name in the scan batch.
                match batch.column_view(field_name) {
                    Some(view) => {
                        arrow_cols.push(column_view_to_arrow(&view));
                    }
                    None => {
                        // Field was not read — emit null column.
                        arrow_cols.push(Arc::new(DFNullArray::new(row_count)));
                    }
                }
            }
        }
    }

    // Handle empty projection (e.g. SELECT COUNT(*) FROM ...) — produce a
    // batch with row count but no columns using try_new_with_options.
    let options = datafusion::arrow::array::RecordBatchOptions::default()
        .with_row_count(Some(row_count));
    DFRecordBatch::try_new_with_options(
        Arc::clone(output_schema),
        arrow_cols,
        &options,
    )
    .unwrap()
}
