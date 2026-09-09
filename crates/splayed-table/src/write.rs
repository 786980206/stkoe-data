//! Table 级写入：对已有 `(sym, time)` 行执行批量覆盖写入（`.lock` 互斥）。
//! `create_table_columns`：按分区 + 按 index 对齐，只为缺失列建列（绝不覆盖已有列）。

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use splayed_core::{CoreError, DatasetFieldInit, DatasetHandle, Mode, RowRange};
use splayed_format::{Bitmap, Buffer, Column, DataType, DataView, Schema};

use crate::partition::PartitionScheme;
use crate::table::{field_options, infer_tt, partition_key, partition_spans, TableHandle, TableOptions};

/// `.lock` 守卫：Drop 时释放（删除锁文件）。进程崩溃会留下残留锁，需人工删除。
struct LockGuard {
    path: PathBuf,
}

impl LockGuard {
    fn acquire(table_root: &std::path::Path) -> Result<Self, CoreError> {
        let path = table_root.join(".lock");
        File::options().write(true).create_new(true).open(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                CoreError::InvalidState(format!(
                    "table is locked by another writer ({})",
                    path.display()
                ))
            } else {
                CoreError::Io(e)
            }
        })?;
        Ok(LockGuard { path })
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// 一个分区的写入计划：分区名 + (网格行偏移, 全局输入行起点, 行数) 三元组列表。
/// 输入行段在该分区内连续（locate 的 range 与输入行段一一对应）。
struct PartitionWrite {
    name: String,
    writes: Vec<(u64, usize, usize)>,
}

/// 阶段①：一次扫描校验输入排序 + 划分分区 run。
///
/// 相邻 key 严格递增校验（零分配，`string_at` 借用比较）+ 分区 run 划分
/// （整数粗键判别，分区名仅换段时构造——同一分区可因 sym 优先出现多个 run，全部收集）。
/// 供 `write_table` 与 `create_table_columns` 共享；调用方需先保证 `data` 含 sym/time 且非空。
fn scan_partition_runs(
    table: &TableHandle,
    data: &DataView<'_>,
) -> Result<HashMap<String, Vec<(usize, usize)>>, CoreError> {
    let rows = data.length();
    let scheme = table.scheme();
    let tt = table.peek_time_type()?;
    let sym_view = data.column("sym").unwrap();
    let time_view = data.column("time").unwrap();

    let mut buckets: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
    let mut prev_sym: Option<&str> = None;
    let mut prev_t: Option<i64> = None;
    let (mut cur_key, mut cur_first_t, mut cur_start, mut cur_len) = (None, 0i64, 0usize, 0usize);
    for i in 0..rows {
        let sym = sym_view
            .string_at(i)
            .ok_or_else(|| CoreError::Invalid(format!("sym value at row {i} is NULL")))?;
        let t = crate::table::time_value_at(&time_view, i)?;
        // 相邻 key 严格递增：sym ASC，同 sym 内 time ASC 且不重复（零分配借用比较）
        if let (Some(ps), Some(pt)) = (prev_sym, prev_t) {
            if sym < ps || (sym == ps && t <= pt) {
                return Err(CoreError::Invalid(
                    "input must be strictly sorted and unique by (sym ASC, time ASC)".into(),
                ));
            }
        }
        prev_sym = Some(sym);
        prev_t = Some(t);
        let key = partition_key(scheme, t, tt);
        match cur_key {
            Some(k) if k == key => cur_len += 1,
            _ => {
                if cur_key.is_some() {
                    let name = crate::partition::partition_name(scheme, cur_first_t, tt);
                    buckets.entry(name).or_default().push((cur_start, cur_len));
                }
                cur_key = Some(key);
                cur_first_t = t;
                cur_start = i;
                cur_len = 1;
            }
        }
    }
    if cur_key.is_some() {
        let name = crate::partition::partition_name(scheme, cur_first_t, tt);
        buckets.entry(name).or_default().push((cur_start, cur_len));
    }
    Ok(buckets)
}

/// 阶段②：分区存在性校验（不存在则自动创建新分区）+ 逐分区定位，把输入行段映射回网格偏移。
///
/// 逐分区 `locate_dataset_index`（Dataset 缓存复用）+ 匹配行数校验——**任何写入/建列前**
/// 完成全部定位与校验，key 缺失 / 定位不足都不产生部分修改。
/// 若分区尚不存在，自动通过分区创建流程安全初始化该新分区！
/// 返回按分区名 ASC 排序的 `PartitionWrite`（网格偏移对齐）。
fn locate_partition_plan(
    table: &TableHandle,
    data: &DataView<'_>,
    buckets: &HashMap<String, Vec<(usize, usize)>>,
) -> Result<Vec<PartitionWrite>, CoreError> {
    let scheme = table.scheme();
    let none_scheme = scheme == PartitionScheme::None;
    let existing: HashSet<String> = table.discover_partitions().into_iter().collect();

    // 检查是否有缺失分区：若有，先通过 gather_runs 提取该分区数据并自动创建新分区
    let mut missing_partitions: Vec<String> = Vec::new();
    for name in buckets.keys() {
        if none_scheme {
            if !name.is_empty() {
                return Err(CoreError::Invalid(
                    "table has 'none' partitioning; input time spans other partitions".into(),
                ));
            }
        } else if !existing.contains(name) {
            missing_partitions.push(name.clone());
        }
    }

    if !missing_partitions.is_empty() {
        let owned = splayed_core::dataset::data_view_to_owned_data(data)?;
        for name in &missing_partitions {
            let runs = &buckets[name];
            let sub = crate::table::gather_runs(&owned, runs)?;
            let sub_view = sub.as_view();
            table.create_partition(name, &sub_view, None)?;
        }
    }

    // 主线程：逐分区定位 + 匹配校验（全部完成后才进入修改）
    let sym_view = data.column("sym").unwrap();
    let time_view = data.column("time").unwrap();
    let mut names: Vec<String> = buckets.keys().cloned().collect();
    names.sort();
    let mut plan: Vec<PartitionWrite> = Vec::with_capacity(names.len());
    let mut writes_by_name: HashMap<String, Vec<(u64, usize, usize)>> = HashMap::new();
    for name in &names {
        let ds = table.dataset_for(name)?;
        let runs = &buckets[name];
        // 分区内的 pairs 按输入序拼接（跨 run 仍满足 (sym ASC, time ASC) 有序唯一），借用 &str 零多余分配
        let total_rows_in_runs: usize = runs.iter().map(|&(_, l)| l).sum();
        let mut pairs: Vec<(&str, i64)> = Vec::with_capacity(total_rows_in_runs);
        for &(start, len) in runs {
            for i in start..start + len {
                pairs.push((
                    sym_view
                        .string_at(i)
                        .ok_or_else(|| {
                            CoreError::Invalid(format!("sym value at row {i} is NULL"))
                        })?,
                    crate::table::time_value_at(&time_view, i)?,
                ));
            }
        }
        let located = ds.locate_borrowed(&pairs)?;
        let total: u64 = located.iter().map(|r| r.length).sum();
        if total as usize != pairs.len() {
            return Err(CoreError::InvalidState(format!(
                "locate total {total} != input rows {}",
                pairs.len()
            )));
        }
        // located range ↔ 输入行段一一对应（连续输入行）；把网格范围映射回
        // 全局输入行段：跨 run 的合并不会发生（不同 sym 的网格块必不相邻），
        // 但按通用消费路径防御性处理
        let mut writes: Vec<(u64, usize, usize)> = Vec::with_capacity(located.len());
        let (mut ri, mut pos_in_run) = (0usize, 0usize);
        for r in located {
            let RowRange { offset, length } = r;
            let mut rest = length as usize;
            let mut grid_off = offset;
            while rest > 0 {
                let (rs, rl) = runs[ri];
                let take = rest.min(rl - pos_in_run);
                writes.push((grid_off, rs + pos_in_run, take));
                grid_off += take as u64;
                pos_in_run += take;
                rest -= take;
                if pos_in_run == rl {
                    ri += 1;
                    pos_in_run = 0;
                }
            }
        }
        writes_by_name.insert(name.clone(), writes.clone());
        plan.push(PartitionWrite {
            name: name.clone(),
            writes,
        });
    }
    Ok(plan)
}

/// 对 Table 中**已存在**的 `(sym, time)` 行执行批量覆盖写入。
///
/// 并发模型（三阶段）：
/// ① 主线程一次扫描（`scan_partition_runs`：相邻 key 严格递增校验 + 分区 run 划分）
/// ② 主线程定位全部（`locate_partition_plan`：逐分区 locate + 匹配校验，
///    任何写入前完成——key 缺失 / 分区不存在不产生部分写入）
/// ③ 写入：单分区 / 并行度 1 → 串行；否则分区级并行（`std::thread::scope`
///    round-robin 分桶，Field 级预算切分 `P_field = max(1, max_parallelism / P_part)`
///    并在完成后恢复——消除 Table → Dataset 两层并发叠加）。
/// 不保证跨 Partition 原子性：并行写入中某分区失败，已成功分区保留，返回首个错误。
pub fn write_table(table: &TableHandle, data: &DataView<'_>) -> Result<(), CoreError> {
    table.mode().require_write("write_table")?;
    let _lock = LockGuard::acquire(table.path())?;

    if data.column("sym").is_none() || data.column("time").is_none() {
        return Err(CoreError::Invalid(
            "write_table requires sym and time columns".into(),
        ));
    }
    if data.length() == 0 {
        return Ok(());
    }
    let buckets = scan_partition_runs(table, data)?;
    let plan = locate_partition_plan(table, data, &buckets)?;

    // 检查并自动补全缺失列（支持缺列自愈）：
    // 若输入数据包含表中尚不存在的列，先自动为各分区创建缺失列并对齐
    ensure_missing_columns(table, data, &plan)?;

    // ③ 写入：单分区 / 并行度 1 → 串行（Field 级并行拿满预算）；
    //    否则分区级并行，Field 级预算切分（P_field = max(1, max_parallelism / P_part)）
    let max_par = table.max_parallelism();
    let p_part = max_par.min(plan.len());
    if p_part <= 1 {
        for pw in &plan {
            let ds = table.dataset_for(&pw.name)?;
            write_partition(ds, data, &pw.writes)?;
        }
        return Ok(());
    }
    let p_field = (max_par / p_part).max(1);
    let names: Vec<&str> = plan.iter().map(|pw| pw.name.as_str()).collect();
    let writes_by_name: HashMap<&str, &Vec<(u64, usize, usize)>> =
        plan.iter().map(|pw| (pw.name.as_str(), &pw.writes)).collect();
    let olds: Vec<(String, usize)> = {
        let mut guard = table.datasets.borrow_mut();
        // 互不相交 &mut DatasetHandle（iter_mut 一次遍历收集；RefMut 只在主线程存活）
        let mut targets: HashMap<&str, &mut DatasetHandle> = guard
            .iter_mut()
            .filter(|(k, _)| buckets.contains_key(k.as_str()))
            .map(|(k, v)| (k.as_str(), &mut **v))
            .collect();
        // Field 级预算临时下调至 P_field（消除两层并发叠加），join 后恢复
        let mut olds: Vec<(String, usize)> = Vec::new();
        for (k, h) in targets.iter_mut() {
            olds.push(((*k).to_string(), h.max_parallelism()));
            h.set_max_parallelism(p_field);
        }
        // round-robin 分桶（分区独立；并行区域不触碰 Table 缓存）
        let mut groups: Vec<Vec<(&str, &mut DatasetHandle)>> =
            (0..p_part).map(|_| Vec::new()).collect();
        for (i, name) in names.iter().enumerate() {
            // remove 逐个移出 &mut（每个值恰好移动一次，与后续 remove 无借用冲突）
            let handle = targets.remove(name).expect("target partition handle");
            groups[i % p_part].push((name, handle));
        }
        let data_ref = data;
        let writes_ref = &writes_by_name;
        std::thread::scope(|s| {
            let mut joins = Vec::new();
            for group in groups {
                joins.push(s.spawn(move || -> Result<(), CoreError> {
                    for (name, handle) in group {
                        let writes = &writes_ref[name];
                        write_partition(handle, data_ref, writes)?;
                    }
                    Ok(())
                }));
            }
            let mut first_err: Option<CoreError> = None;
            for j in joins {
                match j.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        first_err.get_or_insert(e);
                    }
                    Err(_) => {
                        first_err.get_or_insert(CoreError::InvalidState(
                            "partition writer thread panicked".into(),
                        ));
                    }
                }
            }
            first_err.map_or(Ok(()), Err)
        })?;
        olds
    };
    // 恢复各 DatasetHandle 的 Field 级并行预算（targets 已释放，新的借用周期）
    let mut guard = table.datasets.borrow_mut();
    for (name, old) in olds {
        if let Some(h) = guard.get_mut(&name) {
            h.set_max_parallelism(old);
        }
    }
    Ok(())
}

/// 写入单个分区的全部行段（每段一次零拷贝切片 + write_dataset）。
/// 字段 Schema 只构建一次（剔除 sym / time），按行段零拷贝切片。
fn write_partition(
    ds: &DatasetHandle,
    data: &DataView<'_>,
    writes: &[(u64, usize, usize)],
) -> Result<(), CoreError> {
    let mut schema = Schema::new(Vec::new());
    let mut col_idx: Vec<usize> = Vec::new();
    for field in &data.schema.fields {
        let name = field.name.as_ref();
        if name == "sym" || name == "time" {
            continue; // sym / time 由 META 管理，不作为写入列
        }
        schema.fields.push(field.clone());
        col_idx.push(
            data.schema
                .position(name)
                .expect("schema iteration guarantees"),
        );
    }
    for &(offset, start, len) in writes {
        let mut columns = Vec::with_capacity(col_idx.len());
        for &ci in &col_idx {
            columns.push(
                data.columns[ci]
                    .slice_rows(start, len)
                    .map_err(CoreError::from)?,
            );
        }
        let sliced = DataView::new(schema.clone(), columns).map_err(CoreError::from)?;
        ds.write(offset, &sliced)?;
    }
    Ok(())
}

/// 为 Table 中不存在的列自动补全建列（按分区 + 按 index 对齐）。
/// 新列在所有已发现分区统一创建（表 Schema 一致）；输入未覆盖的分区全 NULL。
fn ensure_missing_columns(
    table: &TableHandle,
    data: &DataView<'_>,
    plan: &[PartitionWrite],
) -> Result<(), CoreError> {
    let compression = table
        .options
        .compression
        .unwrap_or(splayed_format::Compression::None);
    let chunk_target_rows = table.options.chunk_target_rows.unwrap_or(8192);

    let partitions = table.discover_partitions();
    let mut queue: Vec<(String, String, DataType, DatasetFieldInit)> = Vec::new();
    for p in &partitions {
        let ds = table.dataset_for(p)?;
        let l_p = ds.statistics()?.row_count as usize;
        let pw = plan.iter().find(|w| &w.name == p);
        let writes: &[(u64, usize, usize)] = pw.map(|w| w.writes.as_slice()).unwrap_or(&[]);
        for field in &data.schema.fields {
            let name = field.name.as_ref();
            if name == "sym" || name == "time" {
                continue;
            }
            match ds.schema().data_type_of(name) {
                Some(dt) if dt != field.data_type => {
                    return Err(CoreError::Invalid(format!(
                        "field '{name}' exists with type {dt:?}, but input has type {:?}",
                        field.data_type
                    )));
                }
                Some(_) => {} // 已存在同类型 → 不触碰
                None => {
                    let init = if writes.is_empty() {
                        // 该分区未被输入覆盖：直接全 NULL 初始化（header-only 64B 文件，零磁盘占用）
                        DatasetFieldInit::AllNull
                    } else {
                        let full = build_aligned_column(data, writes, name, field.data_type, l_p)?;
                        DatasetFieldInit::Data(full)
                    };
                    queue.push((p.clone(), name.to_string(), field.data_type, init));
                }
            }
        }
    }
    if queue.is_empty() {
        return Ok(());
    }

    // 执行：逐分区 create_dataset_field
    let mut guard = table.datasets.borrow_mut();
    let mut handles: HashMap<&str, &mut DatasetHandle> = guard
        .iter_mut()
        .map(|(k, v)| (k.as_str(), &mut **v))
        .collect();
    for (pname, name, dtype, init) in queue {
        let ds = handles.get_mut(pname.as_str()).expect("plan partition handle");
        let fo = field_options(compression, chunk_target_rows, ds);
        ds.create_dataset_field(&name, dtype, init, fo)?;
    }
    Ok(())
}

/// 构建分区级全长度（`l_p`）拥有型列：把输入行段按 index 对齐拷入对应网格偏移，
/// 未覆盖的网格行保持 NULL（validity=0）。定宽类型（与 `column_view_to_owned` 一致；
/// dict / Utf8 新列暂不支持）。
fn build_aligned_column(
    data: &DataView<'_>,
    writes: &[(u64, usize, usize)],
    name: &str,
    dtype: DataType,
    l_p: usize,
) -> Result<Column, CoreError> {
    let size = dtype.size_of();
    let mut values = vec![0u8; l_p * size];
    let mut validity = Bitmap::zeros(l_p);
    let src = data
        .column(name)
        .ok_or_else(|| CoreError::Invalid(format!("input column '{name}' missing")))?;
    for &(grid_offset, input_start, len) in writes {
        let seg = src.slice_rows(input_start, len).map_err(CoreError::from)?;
        let mut pos = (grid_offset as usize) * size;
        for s in seg.segments() {
            match s.fixed_bytes() {
                Some(bytes) => {
                    values[pos..pos + bytes.len()].copy_from_slice(bytes);
                    pos += bytes.len();
                }
                None => {
                    return Err(CoreError::Invalid(format!(
                        "create_table_columns supports fixed-width columns only (column '{name}')"
                    )));
                }
            }
        }
        validity.set_range(grid_offset as usize, len, true);
    }
    Ok(Column {
        data_type: dtype,
        values: Buffer::from_vec(values),
        validity: Some(validity),
        dict: None,
    })
}

/// 流式表构建器（Out-of-Core Streaming Ingestion）：
///
/// 专为海量时序数据（数百 GB）冷启动大灌库设计。逐批接收 `DataView`，
/// 逐分区流式落盘，内存开销仅为 O(当前批次 / 单分区大小)，不随全表总数据量累积。
pub struct TableStreamWriter {
    root: PathBuf,
    scheme: PartitionScheme,
    options: TableOptions,
    _lock: LockGuard,
    written_partitions: Vec<String>,
}

impl TableStreamWriter {
    /// 创建流式写入器（初始化表根目录并获取独占写锁）。
    pub fn new(
        table_path: &Path,
        scheme: PartitionScheme,
        options: Option<TableOptions>,
    ) -> Result<Self, CoreError> {
        if table_path.exists() && table_path.read_dir().map_err(CoreError::Io)?.next().is_some() {
            return Err(CoreError::AlreadyExists(table_path.to_path_buf()));
        }
        std::fs::create_dir_all(table_path).map_err(CoreError::Io)?;
        let lock = LockGuard::acquire(table_path)?;
        Ok(Self {
            root: table_path.to_path_buf(),
            scheme,
            options: options.unwrap_or_default(),
            _lock: lock,
            written_partitions: Vec::new(),
        })
    }

    /// 流式写入一个完整分区的数据（零拷贝直接落盘，0 中间堆内存复制）。
    pub fn write_partition(
        &mut self,
        partition_name: &str,
        data: &DataView<'_>,
    ) -> Result<(), CoreError> {
        let max_p = self
            .options
            .max_parallelism
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
            .max(1);
        let ds_opts = splayed_core::CreateDatasetOptions {
            max_parallelism: max_p,
            compression: self.options.compression.unwrap_or(splayed_format::Compression::None),
            chunk_target_rows: self.options.chunk_target_rows.unwrap_or(8192),
        };
        if self.scheme == PartitionScheme::None {
            splayed_core::create_dataset_from_view(&self.root, data, ds_opts)?;
        } else {
            crate::table::create_table_partition_from_view(&self.root, partition_name, data, ds_opts)?;
        }
        self.written_partitions.push(partition_name.to_string());
        Ok(())
    }

    /// 接收流式批次数据：自动按 PartitionScheme 切分并逐分区落盘。
    pub fn write_batch(&mut self, batch: &DataView<'_>) -> Result<(), CoreError> {
        if self.scheme == PartitionScheme::None {
            return self.write_partition("", batch);
        }
        let owned = splayed_core::dataset::data_view_to_owned_data(batch)?;
        let tt = infer_tt(batch.column("time").unwrap().data_type())?;
        let buckets = partition_spans(&owned, self.scheme, tt)?;
        for (name, runs) in buckets {
            let sub = crate::table::gather_runs(&owned, &runs)?;
            let sub_view = sub.as_view();
            self.write_partition(&name, &sub_view)?;
        }
        Ok(())
    }

    /// 完成流式写入，释放写锁并返回 TableHandle。
    pub fn finish(self) -> Result<TableHandle, CoreError> {
        drop(self._lock);
        crate::table::open_table(&self.root, Mode::Read, self.options.clone())
    }
}

/// 全量替换更新表数据（.lock 互斥，支持按分区原子替换，或自动追加新分区）。
pub fn update_table(table: &TableHandle, data: &DataView<'_>) -> Result<(), CoreError> {
    table.mode.require_write("update_table")?;
    let _lock = LockGuard::acquire(&table.root)?;
    if data.column("sym").is_none() || data.column("time").is_none() {
        return Err(CoreError::Invalid("table input requires sym and time columns".into()));
    }
    let tt = infer_tt(data.column("time").unwrap().data_type())?;
    if table.scheme == PartitionScheme::None {
        table.ensure_dataset("")?;
        let mut guard = table.datasets.borrow_mut();
        let ds = guard.get_mut("").expect("just ensured");
        ds.update_dataset(data)?;
        table.stats_cache.borrow_mut().clear();
        return Ok(());
    }

    let owned = splayed_core::dataset::data_view_to_owned_data(data)?;
    let buckets = partition_spans(&owned, table.scheme, tt)?;
    let mut sorted_buckets = buckets;
    sorted_buckets.sort_by_key(|(_, runs)| std::cmp::Reverse(runs.iter().map(|&(_, l)| l).sum::<usize>()));

    // 先清空已有句柄缓存，释放 Windows 文件映射与锁
    table.datasets.borrow_mut().clear();

    let max_p = table.max_parallelism();
    let p_part = max_p.min(sorted_buckets.len());

    let ds_options = splayed_core::CreateDatasetOptions {
        max_parallelism: 1,
        compression: table.options.compression.unwrap_or(splayed_format::Compression::None),
        chunk_target_rows: table.options.chunk_target_rows.unwrap_or(8192),
    };

    let root_path = &table.root;
    let data_ref = &owned;
    let ds_options_ref = &ds_options;
    let buckets_ref = &sorted_buckets;
    let task_idx = std::sync::atomic::AtomicUsize::new(0);
    let num_tasks = sorted_buckets.len();

    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for _ in 0..p_part {
            handles.push(s.spawn(|| -> Result<(), CoreError> {
                loop {
                    let i = task_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= num_tasks {
                        break;
                    }
                    let (name, runs) = &buckets_ref[i];
                    let sub = crate::table::gather_runs(data_ref, runs)?;
                    let part_path = root_path.join(name);
                    if part_path.exists() {
                        let _ = std::fs::remove_dir_all(&part_path);
                    }
                    splayed_core::create_dataset_data(
                        &part_path,
                        sub,
                        ds_options_ref.clone(),
                    )?;
                }
                Ok(())
            }));
        }
        let mut first_err: Option<CoreError> = None;
        for h in handles {
            match h.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    first_err.get_or_insert(e);
                }
                Err(_) => {
                    first_err.get_or_insert(CoreError::Invalid("partition thread panicked".into()));
                }
            }
        }
        first_err.map_or(Ok(()), Err)
    })?;

    table.stats_cache.borrow_mut().clear();
    Ok(())
}

/// 可写表对象（统一负责单批写入、连续批次写入与所有 DDL 管理能力）
pub struct TableWriter {
    pub(crate) inner: TableHandle,
}

impl TableWriter {
    pub fn create(
        path: &Path,
        schema: &Schema,
        scheme: PartitionScheme,
        initial_partition: Option<&str>,
        options: Option<TableOptions>,
    ) -> Result<Self, CoreError> {
        let inner = crate::table::create_table(path, schema, scheme, initial_partition, options)?;
        Ok(Self { inner })
    }

    pub fn init(
        path: &Path,
        scheme: PartitionScheme,
        data: &DataView<'_>,
        options: Option<TableOptions>,
    ) -> Result<Self, CoreError> {
        let owned = splayed_core::dataset::data_view_to_owned_data(data)?;
        Self::init_data(path, scheme, owned, options)
    }

    pub fn init_data(
        path: &Path,
        scheme: PartitionScheme,
        data: splayed_format::Data,
        options: Option<TableOptions>,
    ) -> Result<Self, CoreError> {
        let opts = options.unwrap_or_default();
        crate::table::init_table_data(path, data, scheme, opts.clone())?;
        Ok(Self {
            inner: TableHandle {
                root: path.to_path_buf(),
                scheme,
                time_type: std::cell::RefCell::new(None),
                mode: Mode::Write,
                options: opts,
                datasets: std::cell::RefCell::new(std::collections::HashMap::new()),
                stats_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            },
        })
    }

    pub fn open(path: &Path) -> Result<Self, CoreError> {
        let inner = crate::table::open_table(path, Mode::Write, TableOptions::default())?;
        Ok(Self { inner })
    }

    pub fn open_with_options(path: &Path, options: TableOptions) -> Result<Self, CoreError> {
        let inner = crate::table::open_table(path, Mode::Write, options)?;
        Ok(Self { inner })
    }

    /// 写入通道：按分区自动切分并分发，支持原地局部覆盖与缺列自愈，全表 .lock 互斥
    pub fn write(&self, data: &DataView<'_>) -> Result<(), CoreError> {
        write_table(&self.inner, data)
    }

    /// 全量替换更新表数据：支持按分区原子替换，或自动追加新分区，全表 .lock 互斥
    pub fn update(&self, data: &DataView<'_>) -> Result<(), CoreError> {
        update_table(&self.inner, data)
    }

    /// 分区生命周期管理：删除分区
    pub fn delete_partition(&self, partition_name: &str) -> Result<(), CoreError> {
        self.inner.delete_partition(partition_name)
    }

    // DDL 字段结构变更
    /// 在已有表中新增单列初始化（仅在最新分区创建物理文件，无需指定 rows，自动与最新分区的 index 行数对齐）
    pub fn init_field(
        &self,
        field_name: &str,
        data_type: DataType,
        opts: Option<splayed_core::CreateFieldOptions>,
    ) -> Result<(), CoreError> {
        self.inner.init_field(field_name, data_type, opts)
    }

    pub fn delete_field(&self, field: &str) -> Result<(), CoreError> {
        self.inner.delete_field(field)
    }

    pub fn rename_field(&self, field: &str, new_name: &str) -> Result<(), CoreError> {
        self.inner.rename_field(field, new_name)
    }

    pub fn cast_field(&self, field: &str, target_type: DataType) -> Result<(), CoreError> {
        self.inner.cast_field(field, target_type)
    }

    pub fn compress_field(&self, field: &str) -> Result<(), CoreError> {
        self.inner.compress_field(field)
    }

    pub fn decompress_field(&self, field: &str) -> Result<(), CoreError> {
        self.inner.decompress_field(field)
    }

    /// 检查并修复表物理存储与元数据完整性：
    /// 1. 遍历每个分区清理因删除字段产生的空文件夹；
    /// 2. 检查是否有历史/中间分区存在的字段而最新分区缺少，若存在则在最新分区中通过 `create_field` 补齐。
    pub fn fix(&self) -> Result<(), CoreError> {
        self.inner.fix()
    }

    pub fn update_field(&self, field: &str, header: &splayed_format::FieldHeader) -> Result<(), CoreError> {
        self.inner.update_field(field, *header)
    }

    pub fn as_reader(&self) -> Result<crate::query::TableReader, CoreError> {
        crate::query::TableReader::open(self.inner.path())
    }

    pub fn schema(&self) -> Result<Schema, CoreError> {
        self.inner.schema()
    }

    pub fn metadata(&self) -> Result<crate::table::TableMetadata, CoreError> {
        self.inner.metadata()
    }

    pub fn statistics(&self) -> Result<crate::table::TableStatistics, CoreError> {
        self.inner.statistics()
    }

    pub fn max_parallelism(&self) -> usize {
        self.inner.max_parallelism()
    }

    pub fn set_max_parallelism(&mut self, max_parallelism: usize) {
        self.inner.set_max_parallelism(max_parallelism);
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn scheme(&self) -> PartitionScheme {
        self.inner.scheme()
    }

    pub fn close(self) -> Result<(), CoreError> {
        self.inner.close()
    }

    pub fn remove(self) -> Result<(), CoreError> {
        let path = self.inner.path().to_path_buf();
        self.close()?;
        crate::table::delete_table(&path)
    }
}
