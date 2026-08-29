//! CoreBatch → DataFusion RecordBatch conversion (zero-copy adapter).

use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch as DFRecordBatch, StringArray};
use datafusion::arrow::datatypes::SchemaRef as DFSchemaRef;
use datafusion::common::{DataFusionError, Result as DFResult};

use splayed_arrow::corebatch_into_record_batch;
use splayed_core::CoreBatch;

/// Convert a Splayed `CoreBatch` into a DataFusion `RecordBatch`, **consuming**
/// it (field/time buffers move into Arrow untouched; the SYM dictionary column
/// is unwrapped into the project's Utf8 column).
///
/// `projected_indices` are indices into the *full* table schema
/// (`0`=time, `1`=sym, `2+`=fields), in the same order as `output_schema`'s
/// fields. `sym_dict` is the cached `StringArray` for the SYM dictionary.
pub(crate) fn batch_to_record_batch(
    batch: CoreBatch,
    output_schema: &DFSchemaRef,
    projected_indices: &[usize],
    sym_dict: Arc<StringArray>,
) -> DFResult<DFRecordBatch> {
    // Map full-schema indices → CoreBatch column positions. CoreBatch layout is
    // [time, sym, requested-fields-in-schema-order], and DataFusion projections
    // are sorted ascending, so a field's CoreBatch column = 2 + its rank among
    // the projected field indices.
    let leading = projected_indices.iter().take_while(|&&i| i < 2).count();
    let cb_indices: Vec<usize> = projected_indices
        .iter()
        .enumerate()
        .map(|(out_pos, &idx)| if idx < 2 { idx } else { 2 + (out_pos - leading) })
        .collect();

    let fields: Vec<datafusion::arrow::datatypes::Field> = output_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();

    corebatch_into_record_batch(batch, &cb_indices, &fields, Some(sym_dict))
        .map_err(|e| DataFusionError::Execution(format!("record batch build failed: {e}")))
}