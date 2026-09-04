//! Table 级写入：对已有 `(sym, time)` 行执行批量覆盖写入（`.lock` 互斥）。

use std::fs::File;
use std::path::PathBuf;

use splayed_core::{CoreError, RowRange};
use splayed_format::{DataView, Schema};

use crate::table::TableHandle;

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

/// 对 Table 中**已存在**的 `(sym, time)` 行执行批量覆盖写入。
///
/// 流程：acquire `.lock` → 校验（key 存在 / 唯一 / 有序）→ 按 scheme 仅按 time 切分
/// → 逐 Partition `locate_dataset_index` → `write_dataset` 输入切片 → release。
/// 不创建 Partition / Dataset、不扩容、不修改 META；不提供跨 Partition 回滚。
pub fn write_table(table: &TableHandle, data: &DataView<'_>) -> Result<(), CoreError> {
    table.mode().require_write("write_table")?;
    let _lock = LockGuard::acquire(table.path())?;

    // 校验输入：必须含 sym/time，列等长
    if data.column("sym").is_none() || data.column("time").is_none() {
        return Err(CoreError::Invalid(
            "write_table requires sym and time columns".into(),
        ));
    }
    let rows = data.length();
    if rows == 0 {
        return Ok(());
    }
    // 输入 (sym, time) 有序且唯一
    let mut prev: Option<(String, i64)> = None;
    for i in 0..rows {
        let s = data
            .column("sym")
            .unwrap()
            .string_at(i)
            .ok_or_else(|| CoreError::Invalid("sym value is NULL".into()))?
            .to_owned();
        let t = crate::table::time_value_at(data.column("time").unwrap(), i)?;
        if let Some((ps, pt)) = &prev {
            if ps.as_str() > s.as_str() || (ps.as_str() == s.as_str() && *pt >= t) {
                return Err(CoreError::Invalid(
                    "write_table input must be sorted and unique by (sym ASC, time ASC)".into(),
                ));
            }
        }
        prev = Some((s, t));
    }

    // 按 partition scheme 切分（仅按 time；输入 time 有序 → 同名分区桶连续）
    let tt = table.peek_time_type()?;
    let mut buckets: Vec<(String, Vec<usize>)> = Vec::new();
    {
        let time_col = data.column("time").unwrap();
        for i in 0..rows {
            let t = crate::table::time_value_at(time_col, i)?;
            let name = crate::partition::partition_name(table.scheme(), t, tt);
            match buckets.last_mut() {
                Some((last_name, list)) if *last_name == name => list.push(i),
                _ => buckets.push((name, vec![i])),
            }
        }
    }

    // 前置校验分区存在性（任一不存在 → Error，不做任何修改；write_table 不创建分区）
    {
        let none_scheme = table.scheme() == crate::partition::PartitionScheme::None;
        let existing = table.discover_partitions();
        for (name, _) in &buckets {
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
    }

    // 逐 Partition：locate（双指针）→ 输入切片 → write_dataset（不保证跨 Partition 原子）
    for (name, indices) in &buckets {
        let pairs: Vec<(String, i64)> = indices
            .iter()
            .map(|&i| {
                Ok((
                    data.column("sym")
                        .unwrap()
                        .string_at(i)
                        .ok_or_else(|| CoreError::Invalid("sym NULL".into()))?
                        .to_owned(),
                    crate::table::time_value_at(data.column("time").unwrap(), i)?,
                ))
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        let ds = table.dataset_for(name)?;
        let located = ds.locate_dataset_index(&pairs)?;
        let total: u64 = located.iter().map(|r| r.length).sum();
        if total != pairs.len() as u64 {
            return Err(CoreError::InvalidState(format!(
                "locate total {total} != input rows {}",
                pairs.len()
            )));
        }
        // located ranges 与输入分段一一对应：连续命中 = 连续输入行（同 sym 内时间连续）
        let mut consumed = 0usize;
        for range in located {
            let RowRange { offset, length } = range;
            let len = length as usize;
            let input_start = indices[consumed];
            for (k, &idx) in indices[consumed..consumed + len].iter().enumerate() {
                debug_assert_eq!(idx, input_start + k, "located range maps to contiguous input rows");
            }
            let sliced = slice_fields_view(data, input_start, len)?;
            ds.write_dataset(offset, &sliced)?;
            consumed += len;
        }
    }
    Ok(())
}

/// 取输入 DataView 中**字段列**（剔除 sym / time）的行区间切片（零拷贝多段视图）。
fn slice_fields_view<'a>(
    data: &'a DataView<'a>,
    row_start: usize,
    len: usize,
) -> Result<DataView<'a>, CoreError> {
    let mut schema = Schema::new(Vec::new());
    let mut columns = Vec::new();
    for field in &data.schema.fields {
        let name = field.name.as_ref();
        if name == "sym" || name == "time" {
            continue; // sym / time 由 META 管理，不作为写入列
        }
        schema.fields.push(field.clone());
        let col = data.column(name).expect("schema iteration guarantees");
        columns.push(col.slice_rows(row_start, len).map_err(CoreError::from)?);
    }
    DataView::new(schema, columns).map_err(CoreError::from)
}
