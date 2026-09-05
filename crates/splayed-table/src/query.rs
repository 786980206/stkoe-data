//! Table 级查询：scan_table（定位）→ TableScanner → read_table（读取 + batch），
//! 以及组合入口 query_table。

use std::collections::VecDeque;
use std::sync::Arc;

use splayed_core::{CmpOp, Predicate, RowRange, Scalar, ScanRequest};
use splayed_format::DataView;

use crate::table::TableHandle;

/// Table 级扫描请求（独立于 core `ScanRequest`；row range 只由 Dataset scan 产生）。
#[derive(Debug, Clone, Default)]
pub struct TableScanRequest {
    /// Table-level symbol 条件（不参与 Partition pruning）。
    pub sym: Option<String>,
    /// Table-level 时间条件（参与 pruning，同时下传做精确过滤）。
    pub time: Option<(i64, i64)>,
    /// Table 逻辑谓词（可提取 pruning 时间条件，其余 residual 下传）。
    pub predicate: Option<Predicate>,
    /// Table-level projection（跨 Partition 一致）。
    pub projection: Vec<String>,
    /// Table-level 全局 limit。
    pub limit: Option<u64>,
}

/// 跨 Partition 的扫描位置：`(partition, 该 Dataset 的逻辑行范围)`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRowRange {
    pub partition: String,
    pub row_range: RowRange,
}

/// Table Scanner：一次 `next()` 返回一个连续 `PartitionRowRange`；不负责 batch。
/// （携带请求的 projection，供 read_table 组装视图。）
pub struct TableScanner {
    ranges: VecDeque<PartitionRowRange>,
    pub(crate) projection: Vec<String>,
}

impl TableScanner {
    pub fn next(&mut self) -> Result<Option<PartitionRowRange>, splayed_core::CoreError> {
        Ok(self.ranges.pop_front())
    }

    pub fn close(self) -> Result<(), splayed_core::CoreError> {
        Ok(())
    }
}

/// Table Reader：消费 Scanner 定位结果，按 `batch_size` 聚合为 `DataView` 输出
/// （多 Partition 段通过 ColumnView 多 segment 拼接，零拷贝）。
pub struct TableReader<'t> {
    table: &'t TableHandle,
    scanner: TableScanner,
    /// `Some(v)` 且 v 非空 = 已展开；`Some(vec![])` / `None` = 待首次 next() 展开。
    projection: Option<Vec<String>>,
    batch_size: usize,
    exhausted: bool,
}

impl<'t> TableReader<'t> {
    /// 每次返回一批 `DataView`；结束返回 `None`。最后一批允许小于 `batch_size`。
    pub fn next(&mut self) -> Result<Option<DataView<'_>>, splayed_core::CoreError> {
        if self.exhausted {
            return Ok(None);
        }
        // 空投影 = 全字段（SELECT *）：首次 next() 时展开为最后 Partition Schema 的字段
        if self.projection.as_ref().is_none_or(|p| p.is_empty()) {
            let schema = self.table.read_table_schema()?;
            let full: Vec<String> = schema
                .fields
                .iter()
                .filter(|f| f.name.as_ref() != "sym" && f.name.as_ref() != "time")
                .map(|f| f.name.to_string())
                .collect();
            self.projection = Some(full);
        }
        let projection = self.projection.as_ref().unwrap();
        let target = self.batch_size.max(1);
        let mut total = 0usize;
        let mut pulled: Vec<PartitionRowRange> = Vec::new();
        while total < target {
            match self.scanner.next()? {
                Some(prr) => {
                    total += prr.row_range.length as usize;
                    pulled.push(prr);
                }
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }
        if pulled.is_empty() {
            return Ok(None);
        }
        // 组装：以第一段 schema 为准
        let mut schema: Option<splayed_format::Schema> = None;
        let mut column_segments: Vec<Vec<splayed_format::ColumnSegment<'_>>> = Vec::new();
        for prr in &pulled {
            let ds = self.table.dataset_for(&prr.partition)?;
            let cols: Vec<&str> = projection.iter().map(|s| s.as_str()).collect();
            let view = ds.read_dataset(
                prr.row_range.offset,
                prr.row_range.length,
                Some(&cols),
            )?;
            if schema.is_none() {
                schema = Some(view.schema.clone());
                column_segments = view
                    .schema
                    .fields
                    .iter()
                    .map(|_| Vec::new())
                    .collect();
            }
            for (j, segs) in column_segments.iter_mut().enumerate() {
                segs.extend(view.columns[j].segments().iter().copied());
            }
        }
        let schema = schema.expect("pulled is non-empty");
        let mut columns = Vec::with_capacity(column_segments.len());
        for (j, segs) in column_segments.into_iter().enumerate() {
            columns.push(splayed_format::ColumnView::new(
                schema.fields[j].data_type,
                segs,
            )?);
        }
        let length = columns.first().map(|c| c.length()).unwrap_or(0);
        let _ = length;
        Ok(Some(DataView::new(schema, columns).map_err(splayed_core::CoreError::from)?))
    }

    /// 关闭：在任何时刻（正常结束 / LIMIT 提前结束 / 错误 / 取消）都可安全调用。
    pub fn close(self) -> Result<(), splayed_core::CoreError> {
        self.scanner.close()
    }
}

/// 组合入口：`query_table ≡ read_table(scan_table(table, request), batch_size)`。
pub fn query_table(
    table: &TableHandle,
    request: TableScanRequest,
    batch_size: Option<usize>,
) -> Result<TableReader<'_>, splayed_core::CoreError> {
    let scanner = scan_table(table, request)?;
    Ok(read_table(table, scanner, batch_size))
}

/// Table 级条件扫描：partition pruning（仅 time 条件）→ 逐 Partition scan_dataset
/// （residual 下传，全局 limit 递减）→ 输出 Partition ASC 的 PartitionRowRange 流。
pub fn scan_table(
    table: &TableHandle,
    request: TableScanRequest,
) -> Result<TableScanner, splayed_core::CoreError> {
    let tt = table.peek_time_type()?;
    let mut partitions = table.discover_partitions();
    // partition pruning：只用 time 条件
    if let Some((lo, hi)) = request.time {
        partitions.retain(|name| {
            let Some((plo, phi)) = crate::partition::partition_range(table.scheme(), name, tt)
            else {
                return false;
            };
            phi > lo && plo < hi
        });
    }
    let mut ranges: VecDeque<PartitionRowRange> = VecDeque::new();
    let mut remaining = request.limit;
    for partition in &partitions {
        if remaining.is_some_and(|r| r == 0) {
            break;
        }
        // 组合 residual：sym 条件 + time 条件 + 用户 predicate
        let mut parts: Vec<Predicate> = Vec::new();
        if let Some(sym) = &request.sym {
            parts.push(Predicate::cmp("sym", CmpOp::Eq, Scalar::Str(sym.clone().into())));
        }
        if let Some((lo, hi)) = request.time {
            parts.push(Predicate::cmp("time", CmpOp::Ge, Scalar::Int(lo)));
            parts.push(Predicate::cmp("time", CmpOp::Lt, Scalar::Int(hi)));
        }
        if let Some(p) = &request.predicate {
            parts.push(p.clone());
        }
        let predicate = match parts.len() {
            0 => None,
            _ => Some(Predicate::And(parts)),
        };
        let projection: Vec<Arc<str>> =
            request.projection.iter().map(|s| Arc::from(s.as_str())).collect();
        let ds = table.dataset_for(partition)?;
        let core_req = ScanRequest {
            ranges: vec![],
            projection,
            predicate,
            limit: remaining,
        };
        let mut scanner = ds.scan_dataset(&core_req)?;
        while let Some(r) = scanner.next()? {
            if let Some(rem) = &mut remaining {
                *rem = rem.saturating_sub(r.length);
            }
            ranges.push_back(PartitionRowRange {
                partition: partition.clone(),
                row_range: r,
            });
        }
        scanner.close()?;
    }
    Ok(TableScanner {
        ranges,
        projection: request.projection.clone(),
    })
}

/// 消费 Scanner，读取实际 `DataView` 并按 `batch_size` 聚合输出。
pub fn read_table<'t>(
    table: &'t TableHandle,
    scanner: TableScanner,
    batch_size: Option<usize>,
) -> TableReader<'t> {
    let projection = Some(scanner.projection.clone());
    TableReader {
        table,
        scanner,
        projection,
        batch_size: batch_size.unwrap_or(1024),
        exhausted: false,
    }
}
