//! 便捷扫描：把 splayed 扫描结果直接转成 Arrow `RecordBatch`（`Vec`），
//! 供 Python / polars 等消费端使用——读侧与 `splayed-core` 的 `Scanner` /
//! `PartitionedTable` 对齐，只做「CoreBatch → Arrow」的零拷贝转换。
//!
//! 输出列顺序固定为 `[time, sym, ...request.columns]`；分区扫描按分区名升序
//! 合并。

use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use arrow::error::ArrowError;
use splayed_core::{
    DatasetError, PartitionError, PartitionScanRequest, PartitionedTable, ScanRequest,
    ScannerError, open_dataset, scan_owned_parallel,
};
use splayed_format::TimeType;

use crate::arrow_conv::splayed_to_arrow_type;
use crate::corebatch_to_arrow::corebatch_into_record_batch;

/// 扫描一个单 dataset（目录直接含 `.meta`），产出全部 `RecordBatch`。
///
/// 投影、符号/时间/值过滤、LIMIT 全部经 `request` 下推到 core 层。
pub fn scan_dataset(
    dir: impl AsRef<Path>,
    request: &ScanRequest,
) -> Result<Vec<RecordBatch>, ScanError> {
    let dir = dir.as_ref();
    let dataset = open_dataset(dir).map_err(ScanError::Dataset)?;
    let time_type = dataset.meta.time_type();

    // 输出列：time / sym / request.columns（按请求顺序）。
    let field_types = dataset
        .list_fields()
        .map_err(ScanError::Io)?
        .into_iter()
        .map(|name| {
            let reader = splayed_core::FieldReader::open(dataset.field_path(&name))
                .map_err(|e| ScanError::Field(name.clone(), e.to_string()))?;
            Ok((name, splayed_to_arrow_type(reader.data_type())))
        })
        .collect::<Result<Vec<_>, ScanError>>()?;
    let output_fields = build_output_fields(time_type, &field_types, &request.columns)?;
    let projected: Vec<usize> = (0..2 + request.columns.len()).collect();

    let scanner = splayed_core::Scanner::new(&dataset);
    let plan = scanner.plan(request).map_err(ScanError::Scanner)?;
    let mut stream = scan_owned_parallel(
        Arc::new(dataset),
        &plan,
        request,
        request.parallelism.max(1),
    )
    .map_err(ScanError::Scanner)?;

    let mut out = Vec::new();
    while let Some(cb) = stream.next_batch().map_err(ScanError::Scanner)? {
        out.push(
            corebatch_into_record_batch(cb, &projected, &output_fields, None)
                .map_err(ScanError::Arrow)?,
        );
    }
    Ok(out)
}

/// 扫描一个分区表（根目录下各子目录含 `.meta`），按分区名升序产出全部
/// `RecordBatch`。分区/时间/符号/统计剪裁经 `request` 下推。
pub fn scan_partitioned(
    dir: impl AsRef<Path>,
    request: &PartitionScanRequest,
) -> Result<Vec<RecordBatch>, ScanError> {
    let dir = dir.as_ref();
    let table = PartitionedTable::open(dir).map_err(ScanError::Partition)?;
    let time_type = table.schema().time_type;

    // 输出列：time / sym / request.columns（字段类型取全表 schema）。
    let field_types: Vec<(String, ArrowDataType)> = table
        .fields()
        .iter()
        .map(|(name, dt)| (name.clone(), splayed_to_arrow_type(*dt)))
        .collect();
    let output_fields = build_output_fields(time_type, &field_types, &request.columns)?;
    let projected: Vec<usize> = (0..2 + request.columns.len()).collect();

    let plan = table.plan(request).map_err(ScanError::Partition)?;
    let mut stream = table.scan(&plan, request).map_err(ScanError::Partition)?;

    let mut out = Vec::new();
    while let Some(cb) = stream.next_batch().map_err(ScanError::Partition)? {
        out.push(
            corebatch_into_record_batch(cb, &projected, &output_fields, None)
                .map_err(ScanError::Arrow)?,
        );
    }
    Ok(out)
}

/// 按 `[time, sym, ...columns]` 构造输出 Arrow 字段。
///
/// `field_types`: (字段名, Arrow 类型) 查找表；`columns` 中不存在的字段报错。
fn build_output_fields(
    time_type: TimeType,
    field_types: &[(String, ArrowDataType)],
    columns: &[String],
) -> Result<Vec<ArrowField>, ScanError> {
    let time_arrow = match time_type {
        TimeType::Date32 => ArrowDataType::Date32,
        TimeType::TimestampUs => {
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
    };
    let mut fields = vec![
        ArrowField::new("time", time_arrow, false),
        ArrowField::new("sym", ArrowDataType::Utf8, false),
    ];
    for name in columns {
        let ty = field_types
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, t)| t.clone())
            .ok_or_else(|| ScanError::UnknownField(name.clone()))?;
        fields.push(ArrowField::new(name.clone(), ty, true));
    }
    Ok(fields)
}

/// 扫描错误。
#[derive(Debug)]
pub enum ScanError {
    Io(std::io::Error),
    Dataset(DatasetError),
    Scanner(ScannerError),
    Partition(PartitionError),
    Field(String, String),
    UnknownField(String),
    Arrow(ArrowError),
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "scan io error: {e}"),
            Self::Dataset(e) => write!(f, "dataset open error: {e}"),
            Self::Scanner(e) => write!(f, "scanner error: {e}"),
            Self::Partition(e) => write!(f, "partition scan error: {e}"),
            Self::Field(name, e) => write!(f, "open field '{name}': {e}"),
            Self::UnknownField(name) => write!(f, "unknown field '{name}' in request.columns"),
            Self::Arrow(e) => write!(f, "arrow conversion error: {e}"),
        }
    }
}

impl std::error::Error for ScanError {}
