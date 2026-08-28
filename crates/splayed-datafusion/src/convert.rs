//! ScanBatch → DataFusion RecordBatch conversion.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array as ArrowArray, NullArray as DFNullArray, RecordBatch as DFRecordBatch,
    builder::{
        Date32Builder as DFDate32Builder,
        TimestampMicrosecondBuilder as DFTimestampMicrosecondBuilder,
    },
};
use datafusion::arrow::datatypes::SchemaRef as DFSchemaRef;
use datafusion::common::{DataFusionError, Result as DFResult};

use splayed_arrow::column_view_to_arrow;
use splayed_core::ScanBatchOwned;
use splayed_format::{MetaFile, TimeType};

/// Convert a Splayed `ScanBatchOwned` into a DataFusion `RecordBatch`.
///
/// `projected_indices` are indices into the *full* table schema.
/// `output_schema` has one field per projected index, in the same order.
pub(crate) fn batch_to_record_batch(
    batch: &ScanBatchOwned,
    output_schema: &DFSchemaRef,
    meta: &MetaFile,
    projected_indices: &[usize],
) -> DFResult<DFRecordBatch> {
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
    // batch with row count but no columns.
    let options = datafusion::arrow::array::RecordBatchOptions::default()
        .with_row_count(Some(row_count));
    DFRecordBatch::try_new_with_options(Arc::clone(output_schema), arrow_cols, &options)
        .map_err(|e| DataFusionError::Execution(format!("record batch build failed: {e}")))
}
