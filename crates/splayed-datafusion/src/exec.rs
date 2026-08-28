//! Execution plans for the Splayed DataFusion provider.
//!
//! - [`SplayedScanExec`]: leaf plan that streams one dataset (one partition)
//!   lazily via a bounded channel + `spawn_blocking`.
//! - [`SplayedTableScanExec`]: table-level plan that exposes each physical
//!   partition as one DataFusion output partition.

use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{tree_node::TreeNodeRecursion, DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    stream::RecordBatchStreamAdapter,
};

use splayed_core::{scan_owned, Dataset, ScanRequest, Scanner, SymbolSelection};

use crate::convert::batch_to_record_batch;

// ---------------------------------------------------------------------------
// Single-dataset leaf plan
// ---------------------------------------------------------------------------

/// A leaf execution plan that scans a single Splayed dataset directory
/// (Layer 1's plan, also reused as one partition of a `SplayedTableScanExec`).
#[derive(Debug)]
pub(crate) struct SplayedScanExec {
    dataset: Arc<Dataset>,
    /// Output schema (after projection — only projected columns).
    output_schema: SchemaRef,
    /// Indices into the full table schema that are projected.
    projected_indices: Vec<usize>,
    scan_request: ScanRequest,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl SplayedScanExec {
    pub(crate) fn new(
        dataset: Arc<Dataset>,
        output_schema: SchemaRef,
        projected_indices: Vec<usize>,
        scan_request: ScanRequest,
        limit: Option<usize>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            equivalence_properties(&output_schema, &scan_request.symbols),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self {
            dataset,
            output_schema,
            projected_indices,
            scan_request,
            limit,
            properties,
        }
    }
}

/// Equivalence properties for a single-dataset scan.
///
/// The scanner emits rows in `(sym, time)` ascending order — *provided* no sym
/// filter reorders the selection (`SymbolSelection::All`) — and the projected
/// output keeps both columns. Advertise that ordering so `ORDER BY sym, time`
/// (e.g. TOP-N) can be satisfied without a sort. A filtered selection may come
/// back in query-literal order, so it never advertises.
fn equivalence_properties(
    output_schema: &SchemaRef,
    symbols: &SymbolSelection,
) -> EquivalenceProperties {
    let plain = || EquivalenceProperties::new(Arc::clone(output_schema));
    if !matches!(symbols, SymbolSelection::All)
        || output_schema.field_with_name("sym").is_err()
        || output_schema.field_with_name("time").is_err()
    {
        return plain();
    }
    match (
        datafusion::physical_expr::expressions::col("sym", output_schema),
        datafusion::physical_expr::expressions::col("time", output_schema),
    ) {
        (Ok(sym), Ok(time)) => EquivalenceProperties::new_with_orderings(
            Arc::clone(output_schema),
            vec![vec![
                PhysicalSortExpr {
                    expr: sym,
                    options: SortOptions::default(),
                },
                PhysicalSortExpr {
                    expr: time,
                    options: SortOptions::default(),
                },
            ]],
        ),
        _ => plain(),
    }
}

impl DisplayAs for SplayedScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SplayedScan: {}", self.dataset.dir.display())
    }
}

impl ExecutionPlan for SplayedScanExec {
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
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let dataset = Arc::clone(&self.dataset);
        let scanner = Scanner::new(&dataset);
        let plan = scanner
            .plan(&self.scan_request)
            .map_err(|e| DataFusionError::Execution(format!("scan plan failed: {e}")))?;
        let request = self.scan_request.clone();

        let iter = scan_owned(Arc::clone(&dataset), &plan, &request)
            .map_err(|e| DataFusionError::Execution(format!("scan failed: {e}")))?;

        let meta = dataset.meta.clone();
        let output_schema = Arc::clone(&self.output_schema);
        let projected_indices = self.projected_indices.clone();
        let limit = self.limit;

        // Bounded channel → backpressure → memory stays ~constant (a couple of
        // batches). A blocking producer thread does the mmap reads off the async
        // executor.
        let (tx, rx) = tokio::sync::mpsc::channel::<DFResult<datafusion::arrow::array::RecordBatch>>(2);
        let stream_schema = Arc::clone(&output_schema);
        tokio::task::spawn_blocking(move || producer(
            iter,
            tx,
            &stream_schema,
            &meta,
            &projected_indices,
            limit,
        ));

        let stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, stream)))
    }
}

/// Drive an `OwnedScanBatches` iterator to completion, sending each batch
/// through a bounded channel. Stops early once `limit` rows are emitted.
fn producer(
    mut iter: splayed_core::OwnedScanBatches,
    tx: tokio::sync::mpsc::Sender<DFResult<datafusion::arrow::array::RecordBatch>>,
    output_schema: &SchemaRef,
    meta: &splayed_format::MetaFile,
    projected_indices: &[usize],
    limit: Option<usize>,
) {
    let mut emitted: usize = 0;
    loop {
        if let Some(lim) = limit {
            if emitted >= lim {
                break;
            }
        }

        let batch = match iter.next_batch() {
            Ok(Some(b)) if b.row_count > 0 => b,
            Ok(Some(_)) => continue, // 0-row batch → keep scanning
            Ok(None) => break,
            Err(e) => {
                let _ = tx.blocking_send(Err(DataFusionError::Execution(format!(
                    "scan batch error: {e}"
                ))));
                break;
            }
        };

        let rb = match batch_to_record_batch(&batch, output_schema, meta, projected_indices) {
            Ok(rb) => rb,
            Err(e) => {
                let _ = tx.blocking_send(Err(e));
                break;
            }
        };

        let n = rb.num_rows();
        if let Some(lim) = limit {
            let keep = lim.saturating_sub(emitted);
            if n > keep {
                // Truncate the final batch to exactly the remaining rows.
                let rb = rb.slice(0, keep);
                let _ = tx.blocking_send(Ok(rb));
                break;
            }
            emitted += n;
        }

        if tx.blocking_send(Ok(rb)).is_err() {
            break; // consumer dropped (cancellation)
        }
    }
}

// ---------------------------------------------------------------------------
// Table-level plan: one output partition per physical partition
// ---------------------------------------------------------------------------

/// A table-level plan that scans N physical partitions, exposing each as one
/// DataFusion output partition (Layer 2's plan).
#[derive(Debug)]
pub(crate) struct SplayedTableScanExec {
    dir: std::path::PathBuf,
    output_schema: SchemaRef,
    plans: Vec<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
}

impl SplayedTableScanExec {
    pub(crate) fn new(
        dir: std::path::PathBuf,
        output_schema: SchemaRef,
        plans: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(output_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(plans.len()),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self {
            dir,
            output_schema,
            plans,
            properties,
        }
    }
}

impl DisplayAs for SplayedTableScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SplayedTableScan: {} ({} partitions)", self.dir.display(), self.plans.len())
    }
}

impl ExecutionPlan for SplayedTableScanExec {
    fn name(&self) -> &str {
        "SplayedTableScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.plans.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != self.plans.len() {
            return Err(DataFusionError::Execution(
                "SplayedTableScanExec: child count changed".to_string(),
            ));
        }
        Ok(Arc::new(SplayedTableScanExec::new(
            self.dir.clone(),
            Arc::clone(&self.output_schema),
            children,
        )))
    }

    fn apply_expressions(
        &self,
        _op: &mut dyn FnMut(&Arc<dyn datafusion::physical_plan::PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let plan = self.plans.get(partition).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "SplayedTableScanExec: partition {partition} out of range ({})",
                self.plans.len()
            ))
        })?;
        // Each partition plan is single-partition itself.
        plan.execute(0, context)
    }
}
