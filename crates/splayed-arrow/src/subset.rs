//! 子集（`.sub.xxx`）→ Arrow `RecordBatch`。
//!
//! 子集是父 `.meta` 网格上的行区间索引（见 `docs/format/subset-format.md`）；
//! 本模块把子集视角的一列式数据直接物化成 Arrow，供 Python / polars /
//! DataFusion 等消费端使用。行序 = 子集的父全局行序（父 (SYM, TIME) 升序），
//! 输出列 `[time, sym, ...columns]` 与 `scan_dataset` 一致。

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Date32Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema, TimeUnit};
use splayed_core::{ColumnView, FieldReader, SubsetReader, open_dataset};
use splayed_format::TimeType;

use crate::arrow_conv::{column_view_to_arrow, splayed_to_arrow_type};
use crate::scan::ScanError;

/// 读取子集 `.sub.{sub_name}` → 单个 `RecordBatch`。
///
/// - 输出列 `[time, sym, ...columns]`（列序与 `scan_dataset` 一致）；
///   `columns` 为空 → 全部字段，否则按请求顺序（不存在的字段报
///   `ScanError::UnknownField`）。
/// - 行序 = 子集的父全局行序（父 (SYM, TIME) 升序）；时间/符号来自父
///   `.meta` 的 TIME AXIS / SYM DICT。
/// - 通过 `SubsetReader::open_with_parent` 校验父 `generation`/`time_type`：
///   父表 `update_meta` 重排后旧子集报 `StaleParent`（需重建）。
pub fn read_subset(
    dir: impl AsRef<Path>,
    sub_name: &str,
    columns: &[String],
) -> Result<RecordBatch, ScanError> {
    let dir = dir.as_ref();
    let dataset = open_dataset(dir).map_err(ScanError::Dataset)?;
    let meta = &dataset.meta;
    let time_type = meta.time_type();

    let subset = SubsetReader::open_with_parent(dir, sub_name, meta)
        .map_err(|e| ScanError::Field(sub_name.to_string(), e.to_string()))?;
    let n = subset.total_rows() as usize;

    // 选列：columns 空 → 全部字段；否则按请求顺序（存在性校验）。
    let all_names = dataset.list_fields().map_err(ScanError::Io)?;
    let chosen: Vec<&String> = if columns.is_empty() {
        all_names.iter().collect()
    } else {
        for name in columns {
            if !all_names.contains(name) {
                return Err(ScanError::UnknownField(name.clone()));
            }
        }
        columns.iter().collect()
    };

    // [time, sym] 两列：按子集父全局行序展开（父 TIME AXIS / SYM DICT）。
    let mut times: Vec<i64> = Vec::with_capacity(n);
    let mut syms: Vec<&str> = Vec::with_capacity(n);
    for (si, rec) in subset.iter_ranges() {
        let sym = meta.symbols[si].as_str();
        let lo = rec.time_start as usize;
        let hi = lo + rec.time_count as usize;
        for &t in &meta.time_axis[lo..hi] {
            times.push(t);
            syms.push(sym);
        }
    }

    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::with_capacity(chosen.len() + 2);
    let time_arrow = match time_type {
        TimeType::Date32 => ArrowDataType::Date32,
        TimeType::TimestampUs => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
    };
    arrays.push(match time_type {
        TimeType::Date32 => Arc::new(Date32Array::from(
            times.iter().map(|&t| t as i32).collect::<Vec<_>>(),
        )) as _,
        TimeType::TimestampUs => Arc::new(TimestampMicrosecondArray::from(times)) as _,
    });
    arrays.push(Arc::new(StringArray::from(syms)) as _);
    let mut fields = vec![
        ArrowField::new("time", time_arrow, false),
        ArrowField::new("sym", ArrowDataType::Utf8, false),
    ];

    // FIELD 列：`read_field_values` 按子集父行序拼出原始字节 → ColumnView →
    // Arrow（NULL 哨兵 → validity 位图）。
    for name in &chosen {
        let path = dataset.field_path(name);
        let reader = FieldReader::open(&path)
            .map_err(|e| ScanError::Field((*name).clone(), e.to_string()))?;
        let bytes = subset
            .read_field_values(&reader)
            .map_err(|e| ScanError::Field((*name).clone(), e.to_string()))?;
        let view = ColumnView::new(reader.data_type(), &bytes, n);
        arrays.push(column_view_to_arrow(&view));
        fields.push(ArrowField::new(
            (*name).clone(),
            splayed_to_arrow_type(reader.data_type()),
            true,
        ));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(ScanError::Arrow)
}
