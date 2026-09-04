//! 分区写能力（Arrow RecordBatch 输入）——与 `splayed-core::partition` 的
//! 写接口对齐：`create_partitioned_table` / `append_partition` /
//! `update_partition_table` / `update_partition_meta` / `drop_partition`。
//!
//! 每个 `PartitionWriteInputArrow` 是一个分区：`data`（RecordBatch，含
//! TIME + SYM + 全表字段列）经 `convert_batch` 转成原生 `TableColumn` 后委托
//! core。写选项 `FieldWriteOptions` 非默认时，新字段直接以编码/压缩形式落盘
//! （写后只读）。

use std::path::Path;

use arrow_array::RecordBatch;
use splayed_core::{
    FieldWriteOptions, PartitionError, PartitionWriteInput, append_partition_with_options,
    create_partitioned_table_with_options, drop_partition as core_drop_partition,
    update_partition_meta_with_options, update_partition_table_with_options,
};

use crate::table_writer::{TableError, convert_batch};

/// 一个分区的 Arrow 写入输入（建表 / 追加 / 布局重排共用）。
///
/// `data` 须含 TIME（Date32 / Timestamp[µs]）、SYM（Utf8）及全表 schema 一致
/// 的字段列；`sorted` 为 (SYM, TIME) 升序性能提示。
#[derive(Debug, Clone)]
pub struct PartitionWriteInputArrow {
    /// 分区名 = 目录名（可含 `key=value`，如 `"month=2026-07"`）。
    pub name: String,
    pub data: RecordBatch,
    pub sorted: bool,
}

impl PartitionWriteInputArrow {
    pub fn new(name: impl Into<String>, data: RecordBatch, sorted: bool) -> Self {
        Self {
            name: name.into(),
            data,
            sorted,
        }
    }
}

/// 一次建出整个分区表：root + N 个分区（每分区并发建表）。
///
/// `opts` 非默认时各分区的新 FIELD 直接以编码/压缩形式落盘（写后只读）。
pub fn create_partitioned_table(
    root: impl AsRef<Path>,
    time_type: splayed_format::TimeType,
    inputs: &[PartitionWriteInputArrow],
    opts: FieldWriteOptions,
) -> Result<(), PartitionError> {
    let inputs = convert_inputs(inputs)?;
    create_partitioned_table_with_options(root, time_type, &inputs, opts)
}

/// 追加一个分区（校验与既有分区 schema / 命名风格 / 分区列一致）。
pub fn append_partition(
    root: impl AsRef<Path>,
    input: &PartitionWriteInputArrow,
    opts: FieldWriteOptions,
) -> Result<(), PartitionError> {
    let input = convert_input(input)?;
    append_partition_with_options(root, &input, opts)
}

/// 表级格子写入（跨分区路由）。
///
/// - `target_partition = Some(name)`：所有行必须落在该分区；
/// - `None`：每行路由到唯一包含该 (SYM, TIME) 的分区。
///
/// `opts` 非默认时新创建的字段直接以编码/压缩形式落盘（写后只读）。
pub fn update_partition_table(
    root: impl AsRef<Path>,
    data: &RecordBatch,
    create_missing_fields: bool,
    target_partition: Option<&str>,
    opts: FieldWriteOptions,
) -> Result<(), PartitionError> {
    let (_time_type, sym, time, columns) = convert_batch(data).map_err(to_partition_error)?;
    update_partition_table_with_options(
        root,
        &sym,
        &time,
        &columns,
        create_missing_fields,
        target_partition,
        opts,
    )
}

/// 表级布局重排：输入里已存在的分区 → `update_meta`（数据保留）；新增分区 →
/// `create_table`；未出现在输入中的既有分区 → 删除。分区处理并发执行。
///
/// `opts` 非默认时新增分区的字段直接以编码/压缩形式落盘（写后只读）；既有
/// 分区走 `update_meta` 重写为 PLAIN+NONE 可写。
pub fn update_partition_meta(
    root: impl AsRef<Path>,
    inputs: &[PartitionWriteInputArrow],
    opts: FieldWriteOptions,
) -> Result<(), PartitionError> {
    let inputs = convert_inputs(inputs)?;
    update_partition_meta_with_options(root, &inputs, opts)
}

/// 删除一个分区（目录整体删除；分区不存在报错）。
pub fn drop_partition(root: impl AsRef<Path>, name: &str) -> Result<(), PartitionError> {
    core_drop_partition(root, name)
}

/// 把 Arrow 输入批量转成 core 原生输入。
fn convert_inputs(
    inputs: &[PartitionWriteInputArrow],
) -> Result<Vec<PartitionWriteInput>, PartitionError> {
    inputs.iter().map(convert_input).collect()
}

/// 单个 Arrow 输入 → core 原生输入。
fn convert_input(input: &PartitionWriteInputArrow) -> Result<PartitionWriteInput, PartitionError> {
    let (time_type, sym, time, columns) = convert_batch(&input.data).map_err(to_partition_error)?;
    Ok(PartitionWriteInput {
        name: input.name.clone(),
        time_type,
        sym,
        time,
        columns,
        sorted: input.sorted,
    })
}

/// arrow `TableError` → core `PartitionError::Table`（解包 core 错误；箭头侧的
/// schema 转换错误包装为 `TableError::Io(Other)`）。
fn to_partition_error(e: TableError) -> PartitionError {
    match e {
        TableError::Core(ce) => PartitionError::Table(ce),
        other => PartitionError::Table(splayed_core::TableError::Io(
            std::io::Error::other(format!("arrow input conversion: {other}")),
        )),
    }
}
