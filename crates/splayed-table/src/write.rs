//! Table 级写入：对已有 `(sym, time)` 行执行批量覆盖写入（`.lock` 互斥）。

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

use splayed_core::{CoreError, DatasetHandle, RowRange};
use splayed_format::{DataView, Schema};

use crate::table::{partition_key, TableHandle};

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

/// 对 Table 中**已存在**的 `(sym, time)` 行执行批量覆盖写入。
///
/// 并发模型（三阶段）：
/// ① 主线程一次扫描：相邻 key 严格递增校验（零分配，`string_at` 借用比较）+
///    分区 run 划分（整数粗键判别，分区名仅换段时构造——同一分区可因 sym 优先
///    出现多个 run，全部收集）；
/// ② 主线程定位全部：逐分区 `locate_dataset_index`（Dataset 缓存复用）+ 匹配行数
///    校验——**任何写入前**完成全部定位与校验，key 缺失 / 分区不存在 /
///    定位不足都不产生部分写入；
/// ③ 写入：单分区 / 并行度 1 → 串行；否则分区级并行（`std::thread::scope`
///    round-robin 分桶，Field 级预算切分 `P_field = max(1, max_parallelism / P_part)`
///    并在完成后恢复——消除 Table → Dataset 两层并发叠加）。
/// 不保证跨 Partition 原子性：并行写入中某分区失败，已成功分区保留，返回首个错误。
pub fn write_table(table: &TableHandle, data: &DataView<'_>) -> Result<(), CoreError> {
    table.mode().require_write("write_table")?;
    let _lock = LockGuard::acquire(table.path())?;

    // ① 主线程：一次扫描完成校验 + 分区 run 划分
    if data.column("sym").is_none() || data.column("time").is_none() {
        return Err(CoreError::Invalid(
            "write_table requires sym and time columns".into(),
        ));
    }
    let rows = data.length();
    if rows == 0 {
        return Ok(());
    }
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
                    "write_table input must be strictly sorted and unique by (sym ASC, time ASC)"
                        .into(),
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

    // 前置校验：分区存在性（write_table 不创建分区）
    let none_scheme = scheme == crate::partition::PartitionScheme::None;
    let existing = table.discover_partitions();
    for name in buckets.keys() {
        if none_scheme {
            if !name.is_empty() {
                return Err(CoreError::Invalid(
                    "table has 'none' partitioning; input time spans other partitions".into(),
                ));
            }
        } else if !existing.contains(name) {
            return Err(CoreError::Invalid(format!(
                "partition '{name}' does not exist; write_table does not create partitions"
            )));
        }
    }

    // ② 主线程：逐分区定位 + 匹配校验（全部完成后才进入写入）
    let mut names: Vec<String> = buckets.keys().cloned().collect();
    names.sort();
    let mut plan: Vec<PartitionWrite> = Vec::with_capacity(names.len());
    let mut writes_by_name: HashMap<String, Vec<(u64, usize, usize)>> = HashMap::new();
    for name in &names {
        let ds = table.dataset_for(name)?;
        let runs = &buckets[name];
        // 分区内的 pairs 按输入序拼接（跨 run 仍满足 (sym ASC, time ASC) 有序唯一）
        let mut pairs: Vec<(String, i64)> = Vec::new();
        for &(start, len) in runs {
            for i in start..start + len {
                pairs.push((
                    sym_view
                        .string_at(i)
                        .ok_or_else(|| {
                            CoreError::Invalid(format!("sym value at row {i} is NULL"))
                        })?
                        .to_owned(),
                    crate::table::time_value_at(&time_view, i)?,
                ));
            }
        }
        let located = ds.locate_dataset_index(&pairs)?;
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

    // ③ 写入：单分区 / 并行度 1 → 串行（Field 级并行拿满预算）；
    //    否则分区级并行，Field 级预算切分（P_field = max(1, max_parallelism / P_part)）
    let max_par = table
        .options
        .max_parallelism
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
        .max(1);
    let p_part = max_par.min(plan.len());
    if p_part <= 1 {
        for pw in &plan {
            let ds = table.dataset_for(&pw.name)?;
            write_partition(ds, data, &pw.writes)?;
        }
        return Ok(());
    }
    let p_field = (max_par / p_part).max(1);
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
            let handle = targets.remove(name.as_str()).expect("target partition handle");
            groups[i % p_part].push((name.as_str(), handle));
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
        ds.write_dataset(offset, &sliced)?;
    }
    Ok(())
}
