//! Arrow ↔ Splayed type and value conversion.

use splayed_format::{DataType, RawValue, TimeType};
use arrow_array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int32Array, Int64Array,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType as ArrowDataType, TimeUnit};

/// Map an Arrow `DataType` to a Splayed `DataType`.
/// Returns `None` for unsupported types (e.g. String, Utf8View, etc.).
pub fn arrow_to_splayed_type(arrow: &ArrowDataType) -> Option<DataType> {
    match arrow {
        ArrowDataType::Boolean => Some(DataType::Bool),
        ArrowDataType::Int32 => Some(DataType::Int32),
        ArrowDataType::Int64 => Some(DataType::Int64),
        ArrowDataType::Float32 => Some(DataType::Float32),
        ArrowDataType::Float64 => Some(DataType::Float64),
        ArrowDataType::Date32 => Some(DataType::Date32),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => Some(DataType::TimestampUs),
        _ => None,
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
        DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            RawValue::from_i32(a.value(i))
        }
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            RawValue::from_i64(a.value(i))
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
        DataType::TimestampUs => {
            let a = arr.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            RawValue::from_timestamp_us(a.value(i))
        }
    })
}
