//! Arrow IPC 桥（`feature = "arrow"`）：Splayed → Arrow → IPC 文件。
//!
//! 仅此模块触碰 Arrow；与 [`crate::native`] 原生路径完全隔离。

use std::path::Path;
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;
use arrow_schema::{DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit as ArrowTimeUnit};

use splayed_core::{Dataset, ScanRequest, Scanner, SymbolSelection, TimeRange, open_dataset};
use splayed_format::TimeType;

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
        Field::new("time", time_arrow, false),
        Field::new("sym", ArrowDataType::Utf8, false),
    ];

    if let Ok(field_names) = dataset.list_fields() {
        for name in field_names {
            let path = dataset.field_path(&name);
            if let Ok(reader) = splayed_core::FieldReader::open(&path) {
                // Use the shared type mapper from splayed-arrow (avoids D1 duplication).
                let arrow_ty = splayed_arrow::splayed_to_arrow_type(reader.data_type());
                fields.push(Field::new(&name, arrow_ty, true));
            }
        }
    }

    Arc::new(Schema::new(fields))
}

/// Convert a `CoreBatch` to an Arrow `RecordBatch` via the zero-copy adapter.
///
/// The batch layout `[time, sym, fields...]` matches `build_arrow_schema`, so
/// the projection is the identity and the schema fields carry the types.
fn scan_batch_to_record_batch(
    batch: splayed_core::CoreBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, ExportError> {
    let indices: Vec<usize> = (0..schema.fields().len()).collect();
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    splayed_arrow::corebatch_into_record_batch(batch, &indices, &fields, None)
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
    let dataset = open_dataset(dataset_dir).map_err(|e| ExportError::OpenDataset(e.to_string()))?;
    let schema = build_arrow_schema(&dataset);

    let field_names = dataset
        .list_fields()
        .map_err(|e| ExportError::OpenDataset(e.to_string()))?;

    let req = ScanRequest {
        columns: field_names.clone(),
        symbols: SymbolSelection::All,
        time_range: TimeRange::all(),
        filters: vec![],
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
        let record_batch = scan_batch_to_record_batch(batch, &schema)?;
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