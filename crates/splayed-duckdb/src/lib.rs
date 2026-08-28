//! Splayed V1 DuckDB integration — Arrow IPC bridge (plan §10.4, Phase 1).
//!
//! Phase 1: `Splayed → Arrow → DuckDB`
//!
//! This crate exports a Splayed dataset to Arrow IPC format, which DuckDB can
//! read natively via:
//! ```sql
//! SELECT * FROM read_arrow('dataset.arrow');
//! ```
//!
//! The conversion chain is:
//! ```text
//! Splayed Dataset
//!   → Scanner (META pruning + ColumnView)
//!   → Arrow RecordBatch (via column_view_to_arrow)
//!   → Arrow IPC file (streaming writer)
//! ```
//!
//! Phase 2 (future): `Splayed → Native ColumnView → DuckDB DataChunk`
//! would bypass the Arrow conversion by using DuckDB's C API directly,
//! but Phase 1 via Arrow IPC is simpler and already very efficient.

use std::path::Path;
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;
use arrow_schema::{
    DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit as ArrowTimeUnit,
};

use splayed_arrow::column_view_to_arrow;
use splayed_core::{
    open_dataset, Dataset, ScanBatchOwned, ScanRequest, Scanner, SymbolSelection, TimeRange,
};
use splayed_format::{DataType as SplayedDataType, TimeType};

/// Errors that can occur during Splayed → Arrow IPC export.
#[derive(Debug)]
pub enum ExportError {
    /// Failed to open the Splayed dataset.
    OpenDataset(String),
    /// Failed to scan the dataset.
    Scan(String),
    /// Failed to write Arrow IPC.
    Ipc(String),
    /// I/O error writing the file.
    Io(std::io::Error),
}

impl std::fmt::Display for ExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenDataset(s) => write!(f, "failed to open dataset: {s}"),
            Self::Scan(s) => write!(f, "scan error: {s}"),
            Self::Ipc(s) => write!(f, "arrow IPC error: {s}"),
            Self::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for ExportError {}

/// Build the Arrow schema for a Splayed dataset.
///
/// The schema is: `time, sym, <field1>, <field2>, ...`
/// matching the DataFusion provider's schema.
pub fn build_arrow_schema(dataset: &Dataset) -> SchemaRef {
    let time_type = dataset.meta.time_type();
    let time_arrow = match time_type {
        TimeType::Date32 => ArrowDataType::Date32,
        TimeType::TimestampUs => ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None),
    };

    let mut fields = vec![
        Field::new("time", time_arrow, true),
        Field::new("sym", ArrowDataType::Utf8, true),
    ];

    if let Ok(field_names) = dataset.list_fields() {
        for name in field_names {
            let path = dataset.field_path(&name);
            if let Ok(reader) = splayed_core::FieldReader::open(&path) {
                let arrow_ty = splayed_to_arrow_type(reader.data_type());
                fields.push(Field::new(&name, arrow_ty, true));
            }
        }
    }

    Arc::new(Schema::new(fields))
}

/// Map a Splayed `DataType` to its Arrow equivalent.
fn splayed_to_arrow_type(ty: SplayedDataType) -> ArrowDataType {
    match ty {
        SplayedDataType::Bool => ArrowDataType::Boolean,
        SplayedDataType::Int32 => ArrowDataType::Int32,
        SplayedDataType::Int64 => ArrowDataType::Int64,
        SplayedDataType::Float32 => ArrowDataType::Float32,
        SplayedDataType::Float64 => ArrowDataType::Float64,
        SplayedDataType::Date32 => ArrowDataType::Date32,
        SplayedDataType::TimestampUs => {
            ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, None)
        }
    }
}

/// Convert a `ScanBatchOwned` to an Arrow `RecordBatch`.
///
/// Reconstructs the `time` and `sym` columns from the batch's `time_values`
/// and `sym_indices`, then converts each FIELD column via `column_view_to_arrow`.
fn scan_batch_to_record_batch(
    batch: &ScanBatchOwned,
    schema: &SchemaRef,
    dataset: &Dataset,
    field_names: &[String],
) -> Result<RecordBatch, ExportError> {
    use arrow_array::{Date32Array, StringArray, TimestampMicrosecondArray};

    let time_type = dataset.meta.time_type();

    // Build time column
    let time_array: Arc<dyn arrow_array::Array> = match time_type {
        TimeType::Date32 => {
            Arc::new(Date32Array::from(
                batch.time_values.iter().map(|&t| t as i32).collect::<Vec<_>>(),
            ))
        }
        TimeType::TimestampUs => {
            Arc::new(TimestampMicrosecondArray::from(batch.time_values.clone()))
        }
    };

    // Build sym column from sym_indices
    let symbols = &dataset.meta.symbols;
    let sym_vals: Vec<&str> = batch
        .sym_indices
        .iter()
        .map(|&i| symbols[i].as_str())
        .collect();
    let sym_array = Arc::new(StringArray::from(sym_vals));

    // Build field columns
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
    columns.push(time_array);
    columns.push(sym_array);

    for name in field_names {
        if let Some(view) = batch.column_view(name) {
            let arrow_arr = column_view_to_arrow(&view);
            columns.push(arrow_arr);
        } else {
            // Missing column — fill with nulls
            let arrow_ty = schema
                .field_with_name(name)
                .map_err(|e| ExportError::Ipc(e.to_string()))?
                .data_type()
                .clone();
            columns.push(arrow_array::new_null_array(&arrow_ty, batch.row_count));
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| ExportError::Ipc(e.to_string()))
}

/// Export a Splayed dataset to Arrow IPC format, writing to a file.
///
/// This is the Phase 1 bridge: `Splayed → Arrow → IPC file → DuckDB`.
///
/// DuckDB can then read this file via:
/// ```sql
/// SELECT * FROM read_arrow('output.arrow');
/// ```
///
/// Returns the total number of rows exported.
pub fn export_to_arrow_ipc(dataset_dir: &Path, output_path: &Path) -> Result<usize, ExportError> {
    let dataset = open_dataset(dataset_dir)
        .map_err(|e| ExportError::OpenDataset(e.to_string()))?;
    let schema = build_arrow_schema(&dataset);

    let field_names = dataset
        .list_fields()
        .map_err(|e| ExportError::OpenDataset(e.to_string()))?;

    let req = ScanRequest {
        columns: field_names.clone(),
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filter: None,
        batch_size: 65536,
        parallelism: 1,
    };

    let scanner = Scanner::new(&dataset);
    let plan = scanner
        .plan(&req)
        .map_err(|e| ExportError::Scan(e.to_string()))?;
    let mut batches = scanner
        .scan(&plan, &req)
        .map_err(|e| ExportError::Scan(e.to_string()))?;

    let file = std::fs::File::create(output_path).map_err(ExportError::Io)?;
    let mut writer =
        StreamWriter::try_new(file, &schema).map_err(|e| ExportError::Ipc(e.to_string()))?;

    let mut total_rows = 0;
    while let Some(batch) = batches
        .next_batch()
        .map_err(|e| ExportError::Scan(e.to_string()))?
    {
        let record_batch = scan_batch_to_record_batch(&batch, &schema, &dataset, &field_names)?;
        writer
            .write(&record_batch)
            .map_err(|e| ExportError::Ipc(e.to_string()))?;
        total_rows += record_batch.num_rows();
    }

    writer
        .finish()
        .map_err(|e| ExportError::Ipc(e.to_string()))?;

    Ok(total_rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_error_display() {
        let e = ExportError::OpenDataset("not found".into());
        assert!(e.to_string().contains("not found"));
    }
}
