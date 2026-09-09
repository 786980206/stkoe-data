//! Table 级查询：scan_table（定位）→ TableScanner → read_table（读取 + batch），
//! 以及组合入口 query_table。

use std::sync::Arc;

use splayed_core::{CmpOp, DatasetScanner, Predicate, RowRange, Scalar, ScanRequest};
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
    /// 单次扫描允许的最大并行度预算；None 则默认遵循 TableHandle 配置。
    pub max_parallelism: Option<usize>,
}

/// 跨 Partition 的扫描位置：`(partition, 该 Dataset 的逻辑行范围)`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRowRange {
    pub partition: String,
    pub row_range: RowRange,
}

/// Table Scanner（**惰性**）：`scan_table` 只做裁剪与构造、不打开任何 Dataset；
/// `next()` 按分区 ASC 顺序惰性打开（Dataset 缓存复用）并扫描，任意时刻至多持有一个
/// `DatasetScanner`。不负责 batch；实际数据读取由 read_table 承担。
pub struct TableScanner<'t> {
    table: &'t TableHandle,
    /// 裁剪后的分区名（升序；时间裁剪经二分定位后仍按序）
    partitions: Vec<String>,
    next_idx: usize,
    current_partition: Option<String>,
    current_scanner: Option<DatasetScanner>,
    /// 组合好的**完整**谓词（sym + time + 用户谓词）——裁剪是粗筛，边界分区仍需
    /// Dataset 内精确过滤，不因已做时间裁剪而剥离时间条件
    predicate: Option<Predicate>,
    pub(crate) projection: Vec<Arc<str>>,
    /// 全局剩余 limit（逐分区下推 + 返回前防御性裁剪）
    remaining: Option<u64>,
    /// 单次扫描指定的最大并行度
    max_parallelism: Option<usize>,
}

impl<'t> TableScanner<'t> {
    pub fn next(&mut self) -> Result<Option<PartitionRowRange>, splayed_core::CoreError> {
        loop {
            // 全局 limit 已满足 → 立即终止（绝不打开后续分区）
            if self.remaining == Some(0) {
                return Ok(None);
            }
            // 情况 1：当前 DatasetScanner 还有结果
            if let Some(scanner) = &mut self.current_scanner {
                if let Some(mut r) = scanner.next()? {
                    // 防御性 limit 裁剪（正常时与下推 limit 一致，不裁即等价）
                    if let Some(rem) = &mut self.remaining {
                        let take = r.length.min(*rem);
                        *rem -= take;
                        if take < r.length {
                            r = RowRange::new(r.offset, take);
                        }
                    }
                    return Ok(Some(PartitionRowRange {
                        partition: self
                            .current_partition
                            .clone()
                            .expect("active partition with live scanner"),
                        row_range: r,
                    }));
                }
                // 当前分区耗尽 → 关闭并清理（DatasetScanner 无资源，close 为空操作）
                self.current_scanner = None;
                self.current_partition = None;
            }
            // 情况 2：惰性打开下一个分区（Dataset 缓存复用；打开 / META 错误在此延迟返回）
            let Some(partition) = self.partitions.get(self.next_idx) else {
                return Ok(None);
            };
            self.next_idx += 1;
            let ds = self.table.dataset_for(partition)?;
            if let Some(mp) = self.max_parallelism {
                ds.set_max_parallelism(mp);
            }
            let core_req = ScanRequest {
                ranges: vec![],
                projection: self.projection.clone(),
                predicate: self.predicate.clone(),
                limit: self.remaining,
            };
            self.current_partition = Some(partition.clone());
            self.current_scanner = Some(ds.scan(&core_req)?);
        }
    }

    /// 核心转换接口：将 scanner 直接无缝转换为流式批次读取器
    pub fn into_reader(self, batch_size: Option<usize>) -> TableBatchReader<'t> {
        let table = self.table;
        let projection = Some(self.projection.clone());
        TableBatchReader {
            table,
            scanner: self,
            projection,
            batch_size,
            pending: None,
            exhausted: false,
        }
    }

    /// 关闭当前实际打开的 DatasetScanner（无多余操作；可重复安全调用语义由
    /// take 保证）。
    pub fn close(mut self) -> Result<(), splayed_core::CoreError> {
        if let Some(s) = self.current_scanner.take() {
            s.close()?;
        }
        Ok(())
    }

    pub fn partitions(&self) -> &[String] {
        &self.partitions
    }

    pub fn predicate(&self) -> Option<&Predicate> {
        self.predicate.as_ref()
    }

    pub fn projection(&self) -> &[Arc<str>] {
        &self.projection
    }

    pub fn remaining(&self) -> Option<u64> {
        self.remaining
    }

    pub fn max_parallelism(&self) -> Option<usize> {
        self.max_parallelism
    }

    pub fn table_root(&self) -> &std::path::Path {
        self.table.path()
    }

    pub fn table_scheme(&self) -> crate::partition::PartitionScheme {
        self.table.scheme()
    }
}

/// Table 流式批次读取器：消费 Scanner 定位的 ranges，装配为 `DataView` 输出。
/// 只做物理读取与 batch 装配，不重复任何查询逻辑（谓词 / 裁剪归 Scanner）。
pub struct TableBatchReader<'t> {
    table: &'t TableHandle,
    scanner: TableScanner<'t>,
    /// `Some(v)` 且 v 非空 = 已展开；`Some(vec![])` / `None` = 待首次 next() 展开。
    projection: Option<Vec<Arc<str>>>,
    batch_size: Option<usize>,
    /// 聚合路径的待消费剩余 range（分区 + 行范围）。
    pending: Option<PartitionRowRange>,
    exhausted: bool,
}

impl<'t> TableBatchReader<'t> {
    /// 每次返回一批 `DataView`；结束返回 `None`（exhausted 后幂等）。
    pub fn next(&mut self) -> Result<Option<DataView<'_>>, splayed_core::CoreError> {
        if self.exhausted {
            return Ok(None);
        }
        match self.batch_size {
            None => self.next_range_view(),
            Some(0) => Err(splayed_core::CoreError::Invalid(
                "batch_size must be non-zero".into(),
            )),
            Some(size) => self.next_batch(size),
        }
    }

    /// 空投影 = 全字段（SELECT *）：首次 next() 时展开为最后 Partition Schema 的字段
    /// （sym / time 恒由 read_dataset 返回，不进 projection）。
    fn resolve_projection(&mut self) -> Result<Vec<Arc<str>>, splayed_core::CoreError> {
        if self.projection.as_ref().is_none_or(|p| p.is_empty()) {
            let schema = self.table.read_table_schema()?;
            let full: Vec<Arc<str>> = schema
                .fields
                .iter()
                .filter(|f| f.name.as_ref() != "sym" && f.name.as_ref() != "time")
                .map(|f| Arc::from(f.name.as_ref()))
                .collect();
            self.projection = Some(full);
        }
        Ok(self.projection.as_ref().unwrap().clone())
    }

    /// 取下一个待读 range：pending 优先，否则 Scanner（Scanner 耗尽 → exhausted）。
    fn pull_range(&mut self) -> Result<Option<PartitionRowRange>, splayed_core::CoreError> {
        if let Some(p) = self.pending.take() {
            return Ok(Some(p));
        }
        match self.scanner.next()? {
            Some(prr) => Ok(Some(prr)),
            None => {
                self.exhausted = true;
                Ok(None)
            }
        }
    }

    /// 读一个分区的行区间（零拷贝；Dataset 经 TableHandle 缓存复用）。
    fn read_rows(
        &self,
        partition: &str,
        offset: u64,
        length: u64,
        cols: &[&str],
    ) -> Result<DataView<'t>, splayed_core::CoreError> {
        let ds = self.table.dataset_for(partition)?;
        ds.read(offset, length, Some(cols))
    }

    /// 原始路径：一个 range 原样返回。
    fn next_range_view(&mut self) -> Result<Option<DataView<'_>>, splayed_core::CoreError> {
        let Some(prr) = self.pull_range()? else {
            return Ok(None);
        };
        let cols = self.resolve_projection()?;
        let cols: Vec<&str> = cols.iter().map(|s| s.as_ref()).collect();
        Ok(Some(self.read_rows(
            &prr.partition,
            prr.row_range.offset,
            prr.row_range.length,
            &cols,
        )?))
    }

    /// 聚合路径：恰好装配 size 行；range 超出剩余 → 截断头读取 + pending 剩余；
    /// Scanner 耗尽时允许最后一批小于 size。
    fn next_batch(&mut self, size: usize) -> Result<Option<DataView<'_>>, splayed_core::CoreError> {
        let cols = self.resolve_projection()?;
        let cols: Vec<&str> = cols.iter().map(|s| s.as_ref()).collect();
        let mut schema: Option<splayed_format::Schema> = None;
        let mut column_segments: Vec<Vec<splayed_format::ColumnSegment<'_>>> = Vec::new();
        let mut total = 0usize;
        while total < size {
            let Some(prr) = self.pull_range()? else { break };
            let take = (size - total).min(prr.row_range.length as usize) as u64;
            let view = self.read_rows(&prr.partition, prr.row_range.offset, take, &cols)?;
            if schema.is_none() {
                schema = Some(view.schema.clone());
                column_segments = view.schema.fields.iter().map(|_| Vec::new()).collect();
            }
            for (j, segs) in column_segments.iter_mut().enumerate() {
                segs.extend(view.columns[j].segments().iter().copied());
            }
            total += take as usize;
            if take < prr.row_range.length {
                self.pending = Some(PartitionRowRange {
                    partition: prr.partition.clone(),
                    row_range: RowRange::new(
                        prr.row_range.offset + take,
                        prr.row_range.length - take,
                    ),
                });
            }
        }
        let Some(schema) = schema else {
            return Ok(None);
        };
        let mut columns = Vec::with_capacity(column_segments.len());
        for (j, segs) in column_segments.into_iter().enumerate() {
            columns.push(splayed_format::ColumnView::new(schema.fields[j].data_type, segs)?);
        }
        Ok(Some(
            DataView::new(schema, columns).map_err(splayed_core::CoreError::from)?,
        ))
    }

    /// 显式关闭批次读取器。
    pub fn close(self) -> Result<(), splayed_core::CoreError> {
        self.scanner.close()
    }
}

/// Table 级条件扫描（**惰性**）：主线程只做校验、裁剪与构造，不打开任何 Dataset。
///
/// 并发模型：无并行——分区扫描保持串行（保持顺序、limit 早停、无嵌套并行），
/// 性能杠杆留给 read_table 的数据读取。裁剪仅用 time 条件（sym / 字段谓词留给
/// Dataset 层）；分区时间界由分区名纯推导，按 time_min 排序后二分定位
/// （O(P) 建序 + O(log P + K) 裁剪；显式排序不依赖分区名字典序——年号位数不同的
/// 字典序 ≠ 时间序）。完整谓词原样下传；limit 逐分区下推 + 返回前防御性裁剪。
pub fn scan_table<'t>(
    table: &'t TableHandle,
    request: TableScanRequest,
) -> Result<TableScanner<'t>, splayed_core::CoreError> {
    // ① residual 谓词组合：sym 条件 + time 条件 + 用户 predicate（完整下传）
    let mut parts: Vec<Predicate> = Vec::new();
    if let Some(sym) = &request.sym {
        parts.push(Predicate::cmp("sym", CmpOp::Eq, Scalar::Str(sym.clone().into())));
    }
    if let Some((lo, hi)) = &request.time {
        parts.push(Predicate::cmp("time", CmpOp::Ge, Scalar::Int(*lo)));
        parts.push(Predicate::cmp("time", CmpOp::Lt, Scalar::Int(*hi)));
    }
    if let Some(p) = &request.predicate {
        parts.push(p.clone());
    }
    let predicate = (!parts.is_empty()).then(|| Predicate::And(parts));

    // ② 分区裁剪：仅 time 条件参与。无时间条件 → 全部分区（分区名序，不读任何 META）；
    //    有时间条件 → 时间界由分区名纯推导（零 META I/O），按 time_min 排序后二分定位
    //    连续命中段（none 模式分区名无时间语义，天然走全选分支）
    let selected: Vec<String> = match &request.time {
        None => table.discover_partitions(),
        Some((lo, hi)) => {
            let tt = table.peek_time_type()?;
            let mut infos: Vec<(String, i64, i64)> = table
                .discover_partitions()
                .into_iter()
                .map(|name| {
                    let (plo, phi) =
                        crate::partition::partition_range(table.scheme(), &name, tt)
                            .ok_or_else(|| {
                                splayed_core::CoreError::Invalid(format!(
                                    "bad partition name {name}"
                                ))
                                // none 模式无时间条件，不会到达此处
                            })?;
                    Ok((name, plo, phi))
                })
                .collect::<Result<_, splayed_core::CoreError>>()?;
            infos.sort_by_key(|(_, plo, _)| *plo);
            // 区间互不相交且按 lo 升序：首个 hi > lo 的分区起，连续取 lo' < hi 的段
            let start = infos.partition_point(|(_, _, phi)| *phi <= *lo);
            infos[start..]
                .iter()
                .take_while(|(_, plo, _)| *plo < *hi)
                .map(|(n, _, _)| n.clone())
                .collect()
        }
    };

    // ③ 惰性构造：不打开任何 Dataset（错误延迟到 next()）
    Ok(TableScanner {
        table,
        partitions: selected,
        next_idx: 0,
        current_partition: None,
        current_scanner: None,
        predicate,
        projection: request
            .projection
            .iter()
            .map(|s| Arc::from(s.as_str()))
            .collect(),
        remaining: request.limit,
        max_parallelism: request.max_parallelism,
    })
}

/// 消费 Scanner，读取实际 `DataView` 并按 `batch_size` 聚合输出。
pub fn read_table<'t>(
    _table: &'t TableHandle,
    scanner: TableScanner<'t>,
    batch_size: Option<usize>,
) -> TableBatchReader<'t> {
    scanner.into_reader(batch_size)
}

/// 扫描加读取一步式查询入口。
pub fn query_table<'t>(
    table: &'t TableHandle,
    request: TableScanRequest,
    batch_size: Option<usize>,
) -> Result<TableBatchReader<'t>, splayed_core::CoreError> {
    let scanner = scan_table(table, request)?;
    Ok(scanner.into_reader(batch_size))
}

/// 只读表对象
pub struct TableReader {
    pub(crate) inner: TableHandle,
}

impl TableReader {
    pub fn open(table_path: &std::path::Path) -> Result<Self, splayed_core::CoreError> {
        let inner = crate::table::open_table(table_path, splayed_core::Mode::Read, crate::table::TableOptions::default())?;
        Ok(Self { inner })
    }

    pub fn open_with_options(table_path: &std::path::Path, options: crate::table::TableOptions) -> Result<Self, splayed_core::CoreError> {
        let inner = crate::table::open_table(table_path, splayed_core::Mode::Read, options)?;
        Ok(Self { inner })
    }

    /// 1. 惰性分区剪枝扫描（仅返回 PartitionRowRange 迭代器）
    pub fn scan(&self, request: TableScanRequest) -> Result<TableScanner<'_>, splayed_core::CoreError> {
        scan_table(&self.inner, request)
    }

    /// 2. 一步式便捷流式读取（内部自动 scan + 转换为 TableBatchReader）
    pub fn read<'t>(&'t self, request: TableScanRequest, batch_size: Option<usize>) -> Result<TableBatchReader<'t>, splayed_core::CoreError> {
        let scanner = self.scan(request)?;
        Ok(scanner.into_reader(batch_size))
    }

    /// 3. 细粒度点查：直接读取单个 PartitionRowRange 的数据视图
    pub fn read_range(
        &self,
        range: &PartitionRowRange,
        projection: Option<&[&str]>,
    ) -> Result<DataView<'_>, splayed_core::CoreError> {
        let ds = self.inner.dataset_for(&range.partition)?;
        ds.read(range.row_range.offset, range.row_range.length, projection)
    }

    pub fn schema(&self) -> Result<splayed_format::Schema, splayed_core::CoreError> {
        self.inner.schema()
    }

    pub fn metadata(&self) -> Result<crate::table::TableMetadata, splayed_core::CoreError> {
        self.inner.metadata()
    }

    pub fn statistics(&self) -> Result<crate::table::TableStatistics, splayed_core::CoreError> {
        self.inner.statistics()
    }

    pub fn max_parallelism(&self) -> usize {
        self.inner.max_parallelism()
    }

    pub fn set_max_parallelism(&mut self, max_parallelism: usize) {
        self.inner.set_max_parallelism(max_parallelism);
    }

    pub fn path(&self) -> &std::path::Path {
        self.inner.path()
    }

    pub fn handle(&self) -> &TableHandle {
        &self.inner
    }

    pub fn discover_partitions(&self) -> Vec<String> {
        self.inner.discover_partitions()
    }

    pub fn scheme(&self) -> crate::partition::PartitionScheme {
        self.inner.scheme()
    }

    pub fn close(self) -> Result<(), splayed_core::CoreError> {
        self.inner.close()
    }
}
