//! Arrow ↔ Splayed type and value conversion.
//!
//! See `plan.md` §10.1 (Arrow 转换) and §5.4 (NULL 编码).
//!
//! NULL semantics at the Arrow boundary:
//! - Splayed canonical NaN (0x7FF8000000000000) → Arrow validity bitmap = 0 (NULL)
//! - Other NaN (non-canonical payload) → Arrow validity = 1, value = NaN
//! - Splayed INT32_MIN / INT64_MIN → Arrow validity = 0 (NULL)
//! - Arrow NULL (validity = 0) → Splayed canonical sentinel

use splayed_format::{DataType, RawValue, TimeType};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, TimestampMicrosecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array, builder::BooleanBuilder,
};
use arrow_schema::{DataType as ArrowDataType, TimeUnit};

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Map an Arrow `DataType` to a Splayed `DataType`.
/// Returns `None` for unsupported types (e.g. String, Utf8View, etc.).
pub fn arrow_to_splayed_type(arrow: &ArrowDataType) -> Option<DataType> {
    match arrow {
        ArrowDataType::Boolean => Some(DataType::Bool),
        ArrowDataType::Int8 => Some(DataType::Int8),
        ArrowDataType::Int16 => Some(DataType::Int16),
        ArrowDataType::Int32 => Some(DataType::Int32),
        ArrowDataType::Int64 => Some(DataType::Int64),
        ArrowDataType::UInt8 => Some(DataType::UInt8),
        ArrowDataType::UInt16 => Some(DataType::UInt16),
        ArrowDataType::UInt32 => Some(DataType::UInt32),
        ArrowDataType::UInt64 => Some(DataType::UInt64),
        ArrowDataType::Float32 => Some(DataType::Float32),
        ArrowDataType::Float64 => Some(DataType::Float64),
        ArrowDataType::Date32 => Some(DataType::Date32),
        ArrowDataType::Date64 => Some(DataType::Date64),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => Some(DataType::TimestampUs),
        _ => None,
    }
}

/// Map a Splayed `DataType` to an Arrow `DataType`.
pub fn splayed_to_arrow_type(splayed: DataType) -> ArrowDataType {
    match splayed {
        DataType::Bool => ArrowDataType::Boolean,
        DataType::Int8 => ArrowDataType::Int8,
        DataType::Int16 => ArrowDataType::Int16,
        DataType::Int32 => ArrowDataType::Int32,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::UInt8 => ArrowDataType::UInt8,
        DataType::UInt16 => ArrowDataType::UInt16,
        DataType::UInt32 => ArrowDataType::UInt32,
        DataType::UInt64 => ArrowDataType::UInt64,
        DataType::Float32 => ArrowDataType::Float32,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::Date32 => ArrowDataType::Date32,
        DataType::Date64 => ArrowDataType::Date64,
        DataType::TimestampUs => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
    }
}

/// Determine the `TimeType` from an Arrow TIME column's `DataType`.
pub fn arrow_time_type(arrow: &ArrowDataType) -> Option<TimeType> {
    match arrow {
        ArrowDataType::Date32 => Some(TimeType::Date32),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => Some(TimeType::TimestampUs),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Arrow → Splayed (per-value)
// ---------------------------------------------------------------------------

/// Convert one value from an Arrow array at index `i` into a `RawValue`.
/// Returns `None` if the value is null.
pub fn arrow_value_to_raw(arr: &dyn Array, i: usize, splayed_ty: DataType) -> Option<RawValue> {
    if arr.is_null(i) {
        return None;
    }
    Some(match splayed_ty {
        DataType::Bool => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            RawValue::from_bool(a.value(i))
        }
        DataType::Int8 => {
            let a = arr.as_any().downcast_ref::<Int8Array>().unwrap();
            RawValue::from_i8(a.value(i))
        }
        DataType::Int16 => {
            let a = arr.as_any().downcast_ref::<Int16Array>().unwrap();
            RawValue::from_i16(a.value(i))
        }
        DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            RawValue::from_i32(a.value(i))
        }
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            RawValue::from_i64(a.value(i))
        }
        DataType::UInt8 => {
            let a = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
            RawValue::from_u8(a.value(i))
        }
        DataType::UInt16 => {
            let a = arr.as_any().downcast_ref::<UInt16Array>().unwrap();
            RawValue::from_u16(a.value(i))
        }
        DataType::UInt32 => {
            let a = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
            RawValue::from_u32(a.value(i))
        }
        DataType::UInt64 => {
            let a = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
            RawValue::from_u64(a.value(i))
        }
        DataType::Float32 => {
            let a = arr.as_any().downcast_ref::<Float32Array>().unwrap();
            RawValue::from_f32(a.value(i))
        }
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            RawValue::from_f64(a.value(i))
        }
        DataType::Date32 => {
            let a = arr.as_any().downcast_ref::<Date32Array>().unwrap();
            RawValue::from_date32(a.value(i))
        }
        DataType::Date64 => {
            let a = arr.as_any().downcast_ref::<Date64Array>().unwrap();
            RawValue::from_date64(a.value(i))
        }
        DataType::TimestampUs => {
            let a = arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            RawValue::from_timestamp_us(a.value(i))
        }
    })
}

// ---------------------------------------------------------------------------
// Splayed → Arrow (column-level)
// ---------------------------------------------------------------------------

/// Convert a `ColumnView` (Splayed native) into an Arrow `ArrayRef`.
///
/// This is the primary Splayed → Arrow path.  NULL detection is via sentinel
/// bit pattern comparison — the resulting Arrow array gets a validity bitmap
/// where NULL sentinels become validity=0.
///
/// Float NULL/NaN semantics (plan §10.1):
/// - canonical NaN → Arrow validity = 0 (NULL)
/// - other NaN → Arrow validity = 1, value = NaN (preserved)
pub fn column_view_to_arrow(view: &splayed_core::ColumnView) -> ArrayRef {
    let dt = view.data_type;
    let sz = dt.size_of();
    let null_pat = dt.null_bytes();
    let n = view.row_count;
    let bytes = view.bytes;

    match dt {
        DataType::Bool => {
            let mut b = BooleanBuilder::with_capacity(n);
            for i in 0..n {
                let off = i * sz;
                if &bytes[off..off + sz] == null_pat {
                    b.append_null();
                } else {
                    b.append_value(bytes[off] != 0);
                }
            }
            Arc::new(b.finish())
        }
        DataType::Int32 => {
            let vals: Vec<Option<i32>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(i32::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(Int32Array::from(vals))
        }
        DataType::Int64 => {
            let vals: Vec<Option<i64>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(i64::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(Int64Array::from(vals))
        }
        DataType::Int8 => {
            let vals: Vec<Option<i8>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(i8::from_le_bytes([bytes[off]]))
                    }
                })
                .collect();
            Arc::new(Int8Array::from(vals))
        }
        DataType::Int16 => {
            let vals: Vec<Option<i16>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(i16::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(Int16Array::from(vals))
        }
        DataType::UInt8 => {
            let vals: Vec<Option<u8>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(bytes[off])
                    }
                })
                .collect();
            Arc::new(UInt8Array::from(vals))
        }
        DataType::UInt16 => {
            let vals: Vec<Option<u16>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(u16::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(UInt16Array::from(vals))
        }
        DataType::UInt32 => {
            let vals: Vec<Option<u32>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(u32::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(UInt32Array::from(vals))
        }
        DataType::UInt64 => {
            let vals: Vec<Option<u64>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(u64::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(UInt64Array::from(vals))
        }
        DataType::Date64 => {
            let vals: Vec<Option<i64>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(i64::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(Date64Array::from(vals))
        }
        DataType::Float32 => {
            let vals: Vec<Option<f32>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(f32::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(Float32Array::from(vals))
        }
        DataType::Float64 => {
            let vals: Vec<Option<f64>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(f64::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(Float64Array::from(vals))
        }
        DataType::Date32 => {
            let vals: Vec<Option<i32>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(i32::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(Date32Array::from(vals))
        }
        DataType::TimestampUs => {
            let vals: Vec<Option<i64>> = (0..n)
                .map(|i| {
                    let off = i * sz;
                    if &bytes[off..off + sz] == null_pat {
                        None
                    } else {
                        Some(i64::from_le_bytes(bytes[off..off + sz].try_into().unwrap()))
                    }
                })
                .collect();
            Arc::new(TimestampMicrosecondArray::from(vals))
        }
    }
}

// Need Arc for ArrayRef
use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;
    use splayed_core::ColumnView;

    fn make_bytes(data_type: DataType, raw_values: &[RawValue]) -> Vec<u8> {
        let sz = data_type.size_of();
        let mut bytes = Vec::with_capacity(raw_values.len() * sz);
        for v in raw_values {
            let offset = bytes.len();
            bytes.resize(offset + sz, 0);
            v.write_le(&mut bytes, offset);
        }
        bytes
    }

    #[test]
    fn float64_with_null_and_nan_to_arrow() {
        let nan_bits: u64 = 0x7FF8000000000001;
        let values = vec![
            RawValue::from_f64(1.0),
            RawValue::null(DataType::Float64),
            RawValue::from_f64(f64::from_bits(nan_bits)),
            RawValue::from_f64(4.0),
        ];
        let bytes = make_bytes(DataType::Float64, &values);
        let view = ColumnView::new(DataType::Float64, &bytes, values.len());
        let arr = column_view_to_arrow(&view);
        let f64_arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(f64_arr.len(), 4);
        assert!(!f64_arr.is_null(0));
        assert_eq!(f64_arr.value(0), 1.0);
        // Row 1: canonical NaN → Arrow NULL
        assert!(f64_arr.is_null(1));
        // Row 2: non-canonical NaN → Arrow valid, value = NaN
        assert!(!f64_arr.is_null(2));
        assert!(f64_arr.value(2).is_nan());
        assert!(!f64_arr.is_null(3));
        assert_eq!(f64_arr.value(3), 4.0);
    }

    #[test]
    fn int32_with_null_to_arrow() {
        let values = vec![
            RawValue::from_i32(42),
            RawValue::null(DataType::Int32),
            RawValue::from_i32(99),
        ];
        let bytes = make_bytes(DataType::Int32, &values);
        let view = ColumnView::new(DataType::Int32, &bytes, values.len());
        let arr = column_view_to_arrow(&view);
        let i32_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(i32_arr.len(), 3);
        assert_eq!(i32_arr.value(0), 42);
        assert!(i32_arr.is_null(1));
        assert_eq!(i32_arr.value(2), 99);
    }

    #[test]
    fn bool_with_null_to_arrow() {
        let values = vec![
            RawValue::from_bool(true),
            RawValue::null(DataType::Bool),
            RawValue::from_bool(false),
        ];
        let bytes = make_bytes(DataType::Bool, &values);
        let view = ColumnView::new(DataType::Bool, &bytes, values.len());
        let arr = column_view_to_arrow(&view);
        let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(bool_arr.len(), 3);
        assert!(bool_arr.value(0));
        assert!(bool_arr.is_null(1));
        assert!(!bool_arr.value(2));
    }

    #[test]
    fn date32_with_null_to_arrow() {
        let values = vec![
            RawValue::from_date32(0),
            RawValue::null(DataType::Date32),
            RawValue::from_date32(365),
        ];
        let bytes = make_bytes(DataType::Date32, &values);
        let view = ColumnView::new(DataType::Date32, &bytes, values.len());
        let arr = column_view_to_arrow(&view);
        let date_arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();

        assert_eq!(date_arr.len(), 3);
        assert_eq!(date_arr.value(0), 0);
        assert!(date_arr.is_null(1));
        assert_eq!(date_arr.value(2), 365);
    }

    #[test]
    fn timestamp_us_with_null_to_arrow() {
        let values = vec![
            RawValue::from_timestamp_us(1_000_000),
            RawValue::null(DataType::TimestampUs),
        ];
        let bytes = make_bytes(DataType::TimestampUs, &values);
        let view = ColumnView::new(DataType::TimestampUs, &bytes, values.len());
        let arr = column_view_to_arrow(&view);
        let ts_arr = arr
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        assert_eq!(ts_arr.len(), 2);
        assert_eq!(ts_arr.value(0), 1_000_000);
        assert!(ts_arr.is_null(1));
    }

    #[test]
    fn type_mapping_roundtrip() {
        for splayed_ty in [
            DataType::Bool,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::TimestampUs,
        ] {
            let arrow_ty = splayed_to_arrow_type(splayed_ty);
            let back = arrow_to_splayed_type(&arrow_ty);
            assert_eq!(back, Some(splayed_ty), "roundtrip failed for {splayed_ty}");
        }
    }
}
