//! arrow-rs（Splayed 侧）→ polars 的转换桥，走 **Arrow C data interface**：
//! 两边 C 结构体布局逐字段一致（C data 规范保证），把 our 侧 `FFI_ArrowArray`
//! 所有权转移给 polars（`release` 回调只依赖 `private_data`，各归其位）。

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use arrow::ffi::FFI_ArrowArray;
use polars::prelude::*;

/// arrow-rs DataType → polars-core DataType（用于 Series / schema）。
pub fn to_polars_dtype(dt: &ArrowDataType) -> PolarsResult<DataType> {
    Ok(match dt {
        ArrowDataType::Boolean => DataType::Boolean,
        ArrowDataType::Int8 => DataType::Int8,
        ArrowDataType::Int16 => DataType::Int16,
        ArrowDataType::Int32 => DataType::Int32,
        ArrowDataType::Int64 => DataType::Int64,
        ArrowDataType::UInt8 => DataType::UInt8,
        ArrowDataType::UInt16 => DataType::UInt16,
        ArrowDataType::UInt32 => DataType::UInt32,
        ArrowDataType::UInt64 => DataType::UInt64,
        ArrowDataType::Float32 => DataType::Float32,
        ArrowDataType::Float64 => DataType::Float64,
        ArrowDataType::Utf8 => DataType::String,
        ArrowDataType::Date32 => DataType::Date,
        ArrowDataType::Date64 => DataType::Date,
        ArrowDataType::Timestamp(ArrowTimeUnit::Microsecond, _) => {
            DataType::Datetime(TimeUnit::Microseconds, None)
        }
        ArrowDataType::Timestamp(ArrowTimeUnit::Nanosecond, _) => {
            DataType::Datetime(TimeUnit::Nanoseconds, None)
        }
        other => {
            polars_bail!(ComputeError: "unmappable arrow dtype {other:?} for polars")
        }
    })
}

/// arrow-rs DataType → **物理** polars-arrow DataType（用于 `import_array_from_c`）。
///
/// polars 的时间列 chunk 是物理类型（Date=Int32、Datetime=Int64），
/// 逻辑 dtype 由 `Series::from_chunks_and_dtype_unchecked` 侧携带。
pub fn to_polars_arrow_dtype(
    dt: &ArrowDataType,
) -> PolarsResult<polars_arrow::datatypes::ArrowDataType> {
    use polars_arrow::datatypes::ArrowDataType as PD;
    Ok(match dt {
        ArrowDataType::Boolean => PD::Boolean,
        ArrowDataType::Int8 => PD::Int8,
        ArrowDataType::Int16 => PD::Int16,
        ArrowDataType::Int32 => PD::Int32,
        ArrowDataType::Int64 => PD::Int64,
        ArrowDataType::UInt8 => PD::UInt8,
        ArrowDataType::UInt16 => PD::UInt16,
        ArrowDataType::UInt32 => PD::UInt32,
        ArrowDataType::UInt64 => PD::UInt64,
        ArrowDataType::Float32 => PD::Float32,
        ArrowDataType::Float64 => PD::Float64,
        ArrowDataType::Utf8 => PD::Utf8,
        // 时间列物理化：Date32/Date64 → Int32/Int64；Timestamp → Int64。
        ArrowDataType::Date32 => PD::Int32,
        ArrowDataType::Date64 => PD::Int64,
        ArrowDataType::Timestamp(_, _) => PD::Int64,
        other => {
            polars_bail!(ComputeError: "unmappable arrow dtype {other:?} for polars")
        }
    })
}

/// 把一个 arrow-rs `ArrayRef` 零拷贝转成 polars `Series`。
///
/// 数据缓冲所有权转移到 polars（本函数消费一次）；结构体布局等价于
/// polars-arrow 的 `ffi::ArrowArray`（C data interface 规范），逐字段相同。
///
/// # Safety
/// `arr` 的类型必须是 `to_polars_dtype` 支持的类型之一；对非 `Utf8` 类型会
/// 构造 C data interface 结构体并转移缓冲所有权（调用方不得再使用该数组）。
pub unsafe fn arrow_array_to_series(
    name: &str,
    arr: ArrayRef,
) -> PolarsResult<Series> {
    // 字符串：polars 0.45 的 newest compat 把 String 映射为 Utf8View，C data
    // interface 的标准 Utf8 数组无法直接灌入；字符串数据量小，直接按值构造。
    if *arr.data_type() == ArrowDataType::Utf8 {
        let s = arr
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| polars_err!(ComputeError: "utf8 downcast failed"))?;
        let vals: Vec<Option<&str>> = s.iter().collect();
        return Ok(Series::new(name.into(), vals));
    }

    let core_dt = to_polars_dtype(arr.data_type())?;
    let arrow_dt = to_polars_arrow_dtype(arr.data_type())?;

    let ffi = FFI_ArrowArray::new(&arr.to_data());

    // 布局一致的 C 结构体：把所有权交给 polars（其 release 回调即 arrow-rs 的回调）。
    debug_assert_eq!(
        std::mem::size_of::<FFI_ArrowArray>(),
        std::mem::size_of::<polars_arrow::ffi::ArrowArray>()
    );
    let pa = std::mem::transmute_copy::<FFI_ArrowArray, polars_arrow::ffi::ArrowArray>(&ffi);
    std::mem::forget(ffi);

    let array = unsafe { polars_arrow::ffi::import_array_from_c(pa, arrow_dt) }
        .map_err(|e| polars_err!(ComputeError: "import array from C: {e}"))?;

    Ok(unsafe {
        Series::from_chunks_and_dtype_unchecked(PlSmallStr::from_str(name), vec![array], &core_dt)
    })
}

/// arrow-rs `RecordBatch` → polars `DataFrame`（列按 `columns` 给定的顺序取）。
pub fn record_batch_to_dataframe(
    rb: &RecordBatch,
    columns: &[String],
) -> PolarsResult<DataFrame> {
    let mut series = Vec::with_capacity(columns.len());
    for name in columns {
        let idx = rb
            .schema()
            .index_of(name)
            .map_err(|e| polars_err!(ComputeError: "column {name} missing: {e}"))?;
        let arr = Arc::clone(rb.column(idx));
        series.push(unsafe { arrow_array_to_series(name, arr) }?);
    }
    DataFrame::new(series.into_iter().map(Column::from).collect())
}

#[cfg(test)]
mod probe {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::DataType as ADT;

    fn try_one(name: &str, dt: ADT, arr: Arc<dyn Array>) {
        let core_dt = to_polars_dtype(&dt).unwrap();
        let arrow_dt = to_polars_arrow_dtype(&dt).unwrap();
        println!("PROBE {name}: core={core_dt:?} arrow(phys)={arrow_dt:?}");
        let s = unsafe { arrow_array_to_series(name, arr) };
        println!("PROBE {name} series: {s:?}");
    }

    #[test]
    fn probe_dtypes() {
        try_one("date", ADT::Date32, Arc::new(Date32Array::from(vec![0, 1])) as ArrayRef);
        try_one("str", ADT::Utf8, Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef);
        try_one("f64", ADT::Float64, Arc::new(Float64Array::from(vec![1.0])) as ArrayRef);
        try_one(
            "i64_null",
            ADT::Int64,
            Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
        );
        try_one(
            "ts",
            ADT::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![1])) as ArrayRef,
        );
    }
}