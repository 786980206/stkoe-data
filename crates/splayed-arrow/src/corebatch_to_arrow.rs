//! `CoreBatch` → Arrow **zero-copy** conversion (splayed-arrow adapter).
//!
//! The CoreBatch buffers are handed straight to Arrow (`arrow::buffer::Buffer::from_vec`)
//! — no per-value copying — and the sentinel→validity translation was already
//! done once when the scan materialized the batch. The SYM column becomes a
//! `DictionaryArray` whose index buffer is reused directly.
//!
//! Design (plan §10.1): the consuming variant (`corebatch_into_record_batch`)
//! moves the buffers into Arrow untouched; NULL bitmaps become Arrow
//! `NullBuffer`s (absent when a column has no NULLs); non-canonical NaNs remain
//! valid values (only canonical sentinels were marked invalid at scan time).

use std::sync::Arc;

use arrow::array::{
    types::{
        Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        Int8Type, TimestampMicrosecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    ArrayRef, BooleanArray, DictionaryArray, PrimitiveArray, RecordBatch, RecordBatchOptions,
    StringArray, UInt32Array,
};
use arrow::buffer::{BooleanBuffer, Buffer as ArrowBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use arrow::error::ArrowError;

use splayed_core::{
    Bitmap, CoreBatch, CoreColumn, CoreColumnKind, CoreStringDict, CoreTimeUnit, CoreType,
};

/// Convert a `CoreBatch` into an Arrow `RecordBatch`, **consuming** it.
///
/// - `projected_indices`: positions into the batch's column layout
///   (`0` = time, `1` = sym, `2+` = fields), in the desired output order.
/// - `output_fields`: an Arrow field for each projected index (same order).
/// - `sym_dict`: cached `StringArray` for the SYM dictionary (built once per
///   dataset); when `None`, it is built from the batch's own dictionary.
///
/// Non-NULL data buffers are transferred without copying (verify with a
/// pointer-equality test on the produced array and the original bytes).
pub fn corebatch_into_record_batch(
    batch: CoreBatch,
    projected_indices: &[usize],
    output_fields: &[Field],
    sym_dict: Option<Arc<StringArray>>,
) -> Result<RecordBatch, ArrowError> {
    let num_rows = batch.num_rows();
    let mut columns = batch.into_columns();
    let n_cols = columns.len();

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(projected_indices.len());
    for (array_idx, &idx) in projected_indices.iter().enumerate() {
        if idx >= n_cols {
            return Err(ArrowError::InvalidArgumentError(format!(
                "corebatch: projected index {idx} out of range ({n_cols})"
            )));
        }
        // `projected_indices` must be strictly ascending (as DataFusion and
        // this adapter always produce); the live position shifts down by the
        // number of already-removed columns.
        let pos = idx - array_idx;
        let col = columns.remove(pos);
        let target = output_fields
            .get(array_idx)
            .ok_or_else(|| ArrowError::InvalidArgumentError("output field missing".into()))?;
        arrays.push(column_into_array(col, target.data_type(), &sym_dict)?);
    }
    build_record_batch(output_fields.to_vec(), arrays, num_rows)
}

/// Convert a `CoreBatch` by reference (COPIES buffers) — for reuse where the
/// batch must stay alive. Prefer the consuming variant for zero-copy.
pub fn corebatch_to_record_batch(
    batch: &CoreBatch,
    projected_indices: &[usize],
    output_fields: &[Field],
    sym_dict: Option<Arc<StringArray>>,
) -> Result<RecordBatch, ArrowError> {
    let num_rows = batch.num_rows();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(projected_indices.len());
    for (array_idx, &idx) in projected_indices.iter().enumerate() {
        let col = batch.column(idx);
        let target = output_fields
            .get(array_idx)
            .ok_or_else(|| ArrowError::InvalidArgumentError("output field missing".into()))?;
        arrays.push(column_copy_to_array(col, target.data_type(), &sym_dict)?);
    }
    build_record_batch(output_fields.to_vec(), arrays, num_rows)
}

fn build_record_batch(
    fields: Vec<Field>,
    arrays: Vec<ArrayRef>,
    num_rows: usize,
) -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    if arrays.is_empty() {
        // Empty projection (e.g. COUNT(*)) — rows only.
        let opts = RecordBatchOptions::default().with_row_count(Some(num_rows));
        return RecordBatch::try_new_with_options(schema, arrays, &opts);
    }
    RecordBatch::try_new(schema, arrays)
}

/// Consuming conversion: buffers move into Arrow untouched.
fn column_into_array(
    col: CoreColumn,
    target: &ArrowDataType,
    sym_dict: &Option<Arc<StringArray>>,
) -> Result<ArrayRef, ArrowError> {
    let nulls = col.nulls.as_ref().map(to_null_buffer).transpose()?;
    match col.kind {
        CoreColumnKind::Primitive { ty, data } => primitive_array(&ty, data.take(), nulls),
        CoreColumnKind::Dictionary { indices, values } => {
            let idx_bytes = indices.take();
            if *target == ArrowDataType::Utf8 {
                // Unwrap the dictionary into a plain Utf8 array (the exported
                // schemas declare sym = Utf8); index buffer is decoded, the
                // string data is small relative to the numerical payload.
                let arr = StringArray::from_iter_values(
                    idx_bytes
                        .chunks_exact(4)
                        .map(|c| u32::from_le_bytes(c.try_into().unwrap()) as usize)
                        .map(|i| values.values[i].as_str()),
                );
                Ok(Arc::new(arr))
            } else {
                // Dictionary target → keep the dictionary encoding.
                let keys =
                    UInt32Array::try_new(ScalarBuffer::from(ArrowBuffer::from_vec(idx_bytes)), None)?;
                let dict = match sym_dict {
                    Some(d) => Arc::clone(d),
                    None => Arc::new(build_string_array(&values)),
                };
                Ok(Arc::new(DictionaryArray::try_new(keys, dict)?))
            }
        }
        CoreColumnKind::Varlen { offsets, data } => {
            let offsets_buf = ArrowBuffer::from_vec(offsets.take());
            let data_buf = ArrowBuffer::from_vec(data.take());
            let offsets_buf = OffsetBuffer::new(ScalarBuffer::from(offsets_buf));
            Ok(Arc::new(StringArray::try_new(offsets_buf, data_buf, nulls)?))
        }
    }
}

/// Copying conversion (borrowed batch).
fn column_copy_to_array(
    col: &CoreColumn,
    target: &ArrowDataType,
    sym_dict: &Option<Arc<StringArray>>,
) -> Result<ArrayRef, ArrowError> {
    let nulls = col.nulls.as_ref().map(to_null_buffer).transpose()?;
    match &col.kind {
        CoreColumnKind::Primitive { ty, data } => {
            primitive_array(ty, data.as_slice().to_vec(), nulls)
        }
        CoreColumnKind::Dictionary { indices, values } => {
            if *target == ArrowDataType::Utf8 {
                let arr = StringArray::from_iter_values(
                    indices
                        .as_slice()
                        .chunks_exact(4)
                        .map(|c| u32::from_le_bytes(c.try_into().unwrap()) as usize)
                        .map(|i| values.values[i].as_str()),
                );
                Ok(Arc::new(arr))
            } else {
                let keys = UInt32Array::try_new(
                    ScalarBuffer::from(ArrowBuffer::from_vec(indices.as_slice().to_vec())),
                    None,
                )?;
                let dict = match sym_dict {
                    Some(d) => Arc::clone(d),
                    None => Arc::new(build_string_array(values)),
                };
                Ok(Arc::new(DictionaryArray::try_new(keys, dict)?))
            }
        }
        CoreColumnKind::Varlen { offsets, data } => {
            let offsets_buf = ArrowBuffer::from_vec(offsets.as_slice().to_vec());
            let data_buf = ArrowBuffer::from_vec(data.as_slice().to_vec());
            let offsets_buf = OffsetBuffer::new(ScalarBuffer::from(offsets_buf));
            Ok(Arc::new(StringArray::try_new(offsets_buf, data_buf, nulls)?))
        }
    }
}

/// Build a concrete primitive Arrow array from owned LE bytes (each arm moves
/// the byte Vec into its own `ArrowBuffer` — only one arm executes).
fn primitive_array(
    ty: &CoreType,
    bytes: Vec<u8>,
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef, ArrowError> {
    match ty {
        CoreType::Boolean => {
            // On-disk BOOL is 1 byte/value; Arrow bit-packs — unpack (copy).
            let vals: Vec<bool> = bytes.iter().map(|&b| b != 0).collect();
            Ok(Arc::new(BooleanArray::new(vals.iter().copied().collect(), nulls)))
        }
        CoreType::Int8 => Ok(Arc::new(PrimitiveArray::<Int8Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::Int16 => Ok(Arc::new(PrimitiveArray::<Int16Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::Int32 => Ok(Arc::new(PrimitiveArray::<Int32Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::Int64 => Ok(Arc::new(PrimitiveArray::<Int64Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::UInt8 => Ok(Arc::new(PrimitiveArray::<UInt8Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::UInt16 => Ok(Arc::new(PrimitiveArray::<UInt16Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::UInt32 => Ok(Arc::new(PrimitiveArray::<UInt32Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::UInt64 => Ok(Arc::new(PrimitiveArray::<UInt64Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::Float32 => Ok(Arc::new(PrimitiveArray::<Float32Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::Float64 => Ok(Arc::new(PrimitiveArray::<Float64Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::Date32 => Ok(Arc::new(PrimitiveArray::<Date32Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::Date64 => Ok(Arc::new(PrimitiveArray::<Date64Type>::try_new(
            ArrowBuffer::from_vec(bytes).into(),
            nulls,
        )?)),
        CoreType::Timestamp(CoreTimeUnit::Microsecond, None) => Ok(Arc::new(
            PrimitiveArray::<TimestampMicrosecondType>::try_new(
                ArrowBuffer::from_vec(bytes).into(),
                nulls,
            )?,
        )),
        other => Err(ArrowError::InvalidArgumentError(format!(
            "corebatch: unsupported primitive {other:?}"
        ))),
    }
}

fn build_string_array(dict: &CoreStringDict) -> StringArray {
    StringArray::from_iter_values(dict.values.iter())
}

fn to_null_buffer(bm: &Bitmap) -> Result<NullBuffer, ArrowError> {
    let bytes = bm.as_bytes().to_vec();
    let buffer = BooleanBuffer::new(ArrowBuffer::from_vec(bytes), 0, bm.len());
    Ok(NullBuffer::new(buffer))
}

/// Map a `CoreType` to the Arrow type used by the produced arrays.
pub fn core_type_to_arrow_type(ty: &CoreType) -> Result<ArrowDataType, ArrowError> {
    match ty {
        CoreType::Boolean => Ok(ArrowDataType::Boolean),
        CoreType::Int8 => Ok(ArrowDataType::Int8),
        CoreType::Int16 => Ok(ArrowDataType::Int16),
        CoreType::Int32 => Ok(ArrowDataType::Int32),
        CoreType::Int64 => Ok(ArrowDataType::Int64),
        CoreType::UInt8 => Ok(ArrowDataType::UInt8),
        CoreType::UInt16 => Ok(ArrowDataType::UInt16),
        CoreType::UInt32 => Ok(ArrowDataType::UInt32),
        CoreType::UInt64 => Ok(ArrowDataType::UInt64),
        CoreType::Float32 => Ok(ArrowDataType::Float32),
        CoreType::Float64 => Ok(ArrowDataType::Float64),
        CoreType::Utf8 => Ok(ArrowDataType::Utf8),
        CoreType::Binary => Ok(ArrowDataType::Binary),
        CoreType::Date32 => Ok(ArrowDataType::Date32),
        CoreType::Date64 => Ok(ArrowDataType::Date64),
        CoreType::Timestamp(u, tz) => Ok(ArrowDataType::Timestamp(
            match u {
                CoreTimeUnit::Second => ArrowTimeUnit::Second,
                CoreTimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
                CoreTimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
                CoreTimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
            },
            tz.as_deref().map(Arc::from),
        )),
        other => Err(ArrowError::InvalidArgumentError(format!(
            "core_type_to_arrow_type: unsupported {other:?}"
        ))),
    }
}