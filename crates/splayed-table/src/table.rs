//! Table 层核心：创建 / 打开 / 删除 / 重命名 / 分区操作 / 元数据。

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use splayed_core::{create_dataset, open_dataset, CoreError, DatasetHandle, Mode};
use splayed_format::{Buffer, Column, Data, DataType, DictBuffers, Schema, TimeType};

use crate::partition::{partition_name, partition_range, PartitionScheme};

/// Table Options：`max_parallelism` 限制 Table 内部并行度上限（实现细节，v1 顺序执行）。
#[derive(Debug, Clone, Default)]
pub struct TableOptions {
    pub max_parallelism: Option<usize>,
}

/// Table 的打开态对象（partition discovery + 按需打开的 Dataset 缓存）。
pub struct TableHandle {
    pub(crate) root: PathBuf,
    pub(crate) scheme: PartitionScheme,
    pub(crate) time_type: std::cell::RefCell<Option<TimeType>>,
    pub(crate) mode: Mode,
    #[allow(dead_code)]
    pub(crate) options: TableOptions,
    pub(crate) datasets: RefCell<HashMap<String, Box<DatasetHandle>>>,
}

impl TableHandle {
    pub fn path(&self) -> &Path {
        &self.root
    }

    pub fn scheme(&self) -> PartitionScheme {
        self.scheme
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// 打开（或取缓存）Partition 对应的 Dataset。`none` 模式 name 为空 → 根目录 Dataset。
    pub(crate) fn dataset_for(&self, partition: &str) -> Result<&DatasetHandle, CoreError> {
        if !self.datasets.borrow().contains_key(partition) {
            let dir = if self.scheme == PartitionScheme::None {
                self.root.clone()
            } else {
                self.root.join(partition)
            };
            let ds = open_dataset(&dir, self.mode)?;
            self.datasets.borrow_mut().insert(partition.to_string(), Box::new(ds));
        }
        let borrow = self.datasets.borrow();
        let ptr: *const DatasetHandle = match borrow.get(partition) {
            Some(b) => &**b as *const DatasetHandle,
            None => unreachable!("just inserted"),
        };
        // 安全性：Box 地址稳定；条目只增不删
        Ok(unsafe { &*ptr })
    }

    /// 时间单位（从第一个可用 Dataset 的 META 推断，缓存）。
    pub(crate) fn peek_time_type(&self) -> Result<TimeType, CoreError> {
        if let Some(tt) = self.time_type.borrow().as_ref() {
            return Ok(*tt);
        }
        let tt = if self.scheme == PartitionScheme::None {
            self.dataset_for("")?.peek_time_type()
        } else {
            let first = self
                .discover_partitions()
                .first()
                .cloned()
                .ok_or_else(|| CoreError::Invalid("table has no partitions".into()))?;
            self.dataset_for(&first)?.peek_time_type()
        };
        *self.time_type.borrow_mut() = Some(tt);
        Ok(tt)
    }

    /// 分区发现：目录中符合 scheme 的子目录，按名称升序。
    /// `none` 模式返回单个隐式根分区（名称为空串）。
    pub(crate) fn discover_partitions(&self) -> Vec<String> {
        if self.scheme == PartitionScheme::None {
            return vec![String::new()];
        }
        let mut out = Vec::new();
        let Ok(entries) = fs::read_dir(&self.root) else {
            return out;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') || name.ends_with(".tmp") || name == ".lock" {
                continue;
            }
            if entry.path().is_dir() && PartitionScheme::from_partition_name(&name).is_some() {
                out.push(name);
            }
        }
        out.sort();
        out
    }
}

/// Table 的分区方案一致性探测（目录内既有分区名的前缀必须统一）。
fn existing_scheme(table_path: &Path) -> Result<Option<PartitionScheme>, CoreError> {
    if table_path.join(".meta").exists() {
        return Err(CoreError::Invalid(
            "table uses 'none' partitioning (root is a Dataset); partitions are not allowed".into(),
        ));
    }
    let mut found: Option<PartitionScheme> = None;
    for entry in fs::read_dir(table_path).map_err(|e| CoreError::Io(e))?.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(s) = PartitionScheme::from_partition_name(&name) {
            match found {
                None => found = Some(s),
                Some(prev) if prev != s => {
                    return Err(CoreError::Invalid(
                        "existing partitions use a different partition scheme".into(),
                    ))
                }
                _ => {}
            }
        }
    }
    Ok(found)
}

/// `read_table_metadata` 的组成部分。
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub ordering: Vec<&'static str>,
    pub partitioning: Partitioning,
    pub capabilities: Capabilities,
    pub partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partitioning {
    pub kind: &'static str,
    pub scheme: String,
}

/// Table 可接受的下推能力。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    pub projection_pushdown: bool,
    pub predicate_pushdown: bool,
    pub limit_pushdown: bool,
}

/// partition 信息（等价于 list_table_partitions，不单独暴露 API）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    pub name: String,
    pub time_min: i64,
    pub time_max: i64,
}

/// Table 级统计（各 Partition 聚合；row_count 求和，min/max 聚合）。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableStatistics {
    pub row_count: u64,
    pub partition_count: u32,
    pub sym_min: Option<String>,
    pub sym_max: Option<String>,
    pub time_min: i64,
    pub time_max: i64,
}

fn infer_tt(dt: DataType) -> Result<TimeType, CoreError> {
    match dt {
        DataType::Date32 | DataType::Int32 => Ok(TimeType::Date32),
        DataType::TimestampUs | DataType::Date64 | DataType::Int64 => Ok(TimeType::TimestampUs),
        other => Err(CoreError::Invalid(format!(
            "unsupported time column type {other:?} for partitioning"
        ))),
    }
}

/// 从定宽 time 列视图读取第 i 行的 i64 值。
pub(crate) fn time_value_at(time_col: &splayed_format::ColumnView<'_>, i: usize) -> Result<i64, CoreError> {
    let mut offset = i;
    for seg in time_col.segments() {
        if offset < seg.rows() {
            let bytes = seg
                .fixed_bytes()
                .ok_or_else(|| CoreError::Invalid("time column must be fixed-width".into()))?;
            let v = match time_col.data_type() {
            DataType::Date32 | DataType::Int32 => i32::from_le_bytes(
                bytes[offset * 4..offset * 4 + 4].try_into().unwrap(),
            ) as i64,
            DataType::TimestampUs | DataType::Date64 | DataType::Int64 => i64::from_le_bytes(
                bytes[offset * 8..offset * 8 + 8].try_into().unwrap(),
            ),
            other => {
                return Err(CoreError::Invalid(format!(
                    "unsupported time column type {other:?}"
                )))
            }
        };
        return Ok(v);
        }
        offset -= seg.rows();
    }
    Err(CoreError::Invalid("time row out of range".into()))
}

/// 按行索引集从 Data 聚出子 Data（保持行序；sym/time/字段全部聚集）。
pub(crate) fn gather_data(data: &Data, indices: &[usize]) -> Result<Data, CoreError> {
    let rows = indices.len();
    let mut columns = Vec::new();
    for field in &data.schema.fields {
        let col = data.column(&field.name).expect("schema iteration guarantees");
        let column = match field.data_type {
            DataType::Utf8 => {
                // 字典列：重建分区局部字典
                let mut dict: HashMap<String, u32> = HashMap::new();
                let mut order: Vec<String> = Vec::new();
                let mut keys: Vec<u32> = Vec::with_capacity(rows);
                let mut bits: Vec<u8> = vec![0u8; (rows + 7) / 8];
                for (out_i, &src) in indices.iter().enumerate() {
                    match col.as_view().string_at(src) {
                        Some(s) => {
                            let id = *dict.entry(s.to_owned()).or_insert_with(|| {
                                order.push(s.to_owned());
                                order.len() as u32 - 1
                            });
                            keys.push(id);
                            bits[out_i / 8] |= 1 << (out_i % 8);
                        }
                        None => {}
                    }
                }
                let mut offsets = vec![0u64];
                let mut strings: Vec<u8> = Vec::new();
                for s in &order {
                    strings.extend_from_slice(s.as_bytes());
                    offsets.push(strings.len() as u64);
                }
                Column {
                    data_type: DataType::Utf8,
                    values: Buffer::from_vec(
                        keys.iter().flat_map(|k| k.to_le_bytes()).collect::<Vec<u8>>(),
                    ),
                    validity: Some(splayed_format::Bitmap::from_bytes(bits, rows)),
                    dict: Some(DictBuffers {
                        offsets: Buffer::from_vec(
                            offsets.iter().flat_map(|o| o.to_le_bytes()).collect(),
                        ),
                        strings: Buffer::from_vec(strings),
                    }),
                }
            }
            fixed => {
                let size = fixed.size_of();
                let mut values = Buffer::zeroed_aligned(rows * size, 8);
                {
                    let dst = values.as_mut_slice();
                    let src = col.values.as_slice();
                    for (out_i, &src_i) in indices.iter().enumerate() {
                        dst[out_i * size..(out_i + 1) * size]
                            .copy_from_slice(&src[src_i * size..(src_i + 1) * size]);
                    }
                }
                let validity = col.validity.as_ref().map(|bm| {
                    let mut bits = vec![0u8; (rows + 7) / 8];
                    for (out_i, &src_i) in indices.iter().enumerate() {
                        if bm.as_view().is_valid(src_i) {
                            bits[out_i / 8] |= 1 << (out_i % 8);
                        }
                    }
                    splayed_format::Bitmap::from_bytes(bits, rows)
                });
                Column { data_type: fixed, values, validity, dict: None }
            }
        };
        columns.push(column);
    }
    Data::new(data.schema.clone(), columns).map_err(CoreError::from)
}

/// 创建完整 Table（`none` → 根目录 Dataset；year/month/date → 按 time 切分 Partition）。
/// 不同 Partition 可并行创建（本实现顺序执行）；不改变 Partition 内数据顺序。
pub fn create_table(
    table_path: &Path,
    data: Data,
    scheme: PartitionScheme,
) -> Result<(), CoreError> {
    if table_path.exists() {
        return Err(CoreError::AlreadyExists(table_path.to_path_buf()));
    }
    if data.column("sym").is_none() || data.column("time").is_none() {
        return Err(CoreError::Invalid("table input requires sym and time columns".into()));
    }
    if scheme == PartitionScheme::None {
        return create_dataset(table_path, data);
    }
    let tt = infer_tt(data.column("time").unwrap().data_type)?;
    let time_view = data.column("time").unwrap().as_view();
    // 按 time 分桶（保持行序：输入 (sym,time) 有序 → 桶内有序）
    let mut buckets: HashMap<String, Vec<usize>> = HashMap::new();
    for i in 0..data.length() {
        let t = time_value_at(&time_view, i)?;
        buckets
            .entry(partition_name(scheme, t, tt))
            .or_default()
            .push(i);
    }
    let mut names: Vec<String> = buckets.keys().cloned().collect();
    names.sort();
    for name in &names {
        let indices = &buckets[name];
        let sub = gather_data(&data, indices)?;
        if sub.length() == 0 {
            continue;
        }
        let dir = table_path.join(name);
        splayed_core::create_dataset(&dir, sub)?;
    }
    Ok(())
}

/// 新增单个 Partition / Dataset（partition_name 必须符合既有 scheme 且不存在）。
pub fn create_table_partition(
    table_path: &Path,
    partition_name: &str,
    data: Data,
) -> Result<(), CoreError> {
    if table_path.join(partition_name).exists() {
        return Err(CoreError::AlreadyExists(table_path.join(partition_name)));
    }
    let existing = existing_scheme(table_path)?;
    let scheme = PartitionScheme::from_partition_name(partition_name).ok_or_else(|| {
        CoreError::Invalid(format!(
            "partition name '{partition_name}' does not match any scheme"
        ))
    })?;
    match existing {
        Some(prev) if prev != scheme => {
            return Err(CoreError::Invalid(
                "existing partitions use a different partition scheme".into(),
            ))
        }
        _ => {}
    }
    if data.length() == 0 {
        return Err(CoreError::Invalid("partition data must not be empty".into()));
    }
    let indices: Vec<usize> = (0..data.length()).collect();
    let sub = gather_data(&data, &indices)?;
    splayed_core::create_dataset(&table_path.join(partition_name), sub)
}

/// 删除整个 Table（根目录及全部 Partition Dataset）。
pub fn delete_table(table_path: &Path) -> Result<(), CoreError> {
    fs::remove_dir_all(table_path).map_err(|e| CoreError::Io(e))
}

/// 删除单个 Partition（Dataset 目录）。
pub fn delete_table_partition(table_path: &Path, partition_name: &str) -> Result<(), CoreError> {
    let dir = table_path.join(partition_name);
    if !dir.is_dir() {
        return Err(CoreError::NotFound(dir));
    }
    fs::remove_dir_all(&dir).map_err(|e| CoreError::Io(e))
}

/// 重命名 Table 根目录（同一父目录内，原子；调用前 Table 必须无打开 Handle）。
pub fn rename_table(table_path: &Path, new_name: &str) -> Result<(), CoreError> {
    if new_name.is_empty() || new_name.starts_with('.') || new_name.contains(['/', '\\', '=']) {
        return Err(CoreError::Invalid(format!("invalid table name '{new_name}'")));
    }
    let parent = table_path.parent().unwrap_or_else(|| Path::new("."));
    let target = parent.join(new_name);
    if target.exists() {
        return Err(CoreError::AlreadyExists(target));
    }
    fs::rename(table_path, &target)?;
    Ok(())
}

/// 打开已有 Table（只打开 Table 级元信息与 Partition 组织；Dataset 按需打开）。
pub fn open_table(
    table_path: &Path,
    mode: Mode,
    options: TableOptions,
) -> Result<TableHandle, CoreError> {
    if !table_path.is_dir() {
        return Err(CoreError::NotFound(table_path.to_path_buf()));
    }
    let scheme = if table_path.join(".meta").exists() {
        PartitionScheme::None
    } else {
        let partitions: Vec<String> = fs::read_dir(table_path)
            .map_err(|e| CoreError::Io(e))?
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| PartitionScheme::from_partition_name(n).is_some())
            .collect();
        match partitions.first() {
            Some(first) => {
                PartitionScheme::from_partition_name(first).expect("filtered by prefix")
            }
            None => {
                return Err(CoreError::Invalid(
                    "cannot infer partition scheme: table is empty".into(),
                ))
            }
        }
    };
    Ok(TableHandle {
        root: table_path.to_path_buf(),
        scheme,
        time_type: std::cell::RefCell::new(None),
        mode,
        options,
        datasets: RefCell::new(HashMap::new()),
    })
}

/// 关闭 TableHandle（方法形式的别名保持命名一致）。
pub fn close_table(handle: TableHandle) -> Result<(), CoreError> {
    handle.close()
}

impl TableHandle {
    /// 关闭 TableHandle（释放全部 Dataset Handle；compressed write 收尾在此触发）。
    pub fn close(mut self) -> Result<(), CoreError> {
        let datasets = self.datasets.get_mut();
        for (_, ds) in datasets.drain() {
            ds.close_dataset()?;
        }
        Ok(())
    }
    /// Table Schema：最后一个 Partition 的 Dataset Schema（不做多 Partition merge）。
    pub fn read_table_schema(&self) -> Result<Schema, CoreError> {
        let last = self
            .discover_partitions()
            .last()
            .cloned()
            .unwrap_or_default();
        Ok(self.dataset_for(&last)?.read_dataset_schema())
    }

    /// 聚合各 Partition 的统计（row_count 求和；min/max 跨 Partition 聚合）。
    pub fn read_table_statistics(&self) -> Result<TableStatistics, CoreError> {
        let mut stats = TableStatistics::default();
        let partitions = self.discover_partitions();
        stats.partition_count = partitions.len() as u32;
        for p in &partitions {
            let s = self.dataset_for(p)?.read_dataset_statistics()?;
            stats.row_count += s.row_count;
            stats.sym_min = match (&stats.sym_min, &s.sym_min) {
                (_, None) => stats.sym_min,
                (None, Some(x)) => Some(x.clone()),
                (Some(a), Some(b)) => Some(if a <= b { a.clone() } else { b.clone() }),
            };
            stats.sym_max = match (&stats.sym_max, &s.sym_max) {
                (_, None) => stats.sym_max,
                (None, Some(x)) => Some(x.clone()),
                (Some(a), Some(b)) => Some(if a >= b { a.clone() } else { b.clone() }),
            };
            stats.time_min = stats.time_min.min(s.time_min);
            stats.time_max = stats.time_max.max(s.time_max);
        }
        Ok(stats)
    }

    /// Table 组织信息（ordering / partitioning / capabilities / partitions；
    /// none 模式无分区子目录，partitions 为空）。
    pub fn read_table_metadata(&self) -> Result<TableMetadata, CoreError> {
        if self.scheme == PartitionScheme::None {
            return Ok(TableMetadata {
                ordering: vec!["sym", "time"],
                partitioning: Partitioning {
                    kind: "time",
                    scheme: format!("{:?}", self.scheme),
                },
                capabilities: Capabilities {
                    projection_pushdown: true,
                    predicate_pushdown: true,
                    limit_pushdown: true,
                },
                partitions: Vec::new(),
            });
        }
        let tt = self.peek_time_type()?;
        let infos: Result<Vec<PartitionInfo>, CoreError> = self
            .discover_partitions()
            .iter()
            .map(|name| {
                let (lo, hi) = partition_range(self.scheme, name, tt)
                    .ok_or_else(|| CoreError::Invalid(format!("bad partition name {name}")))?;
                Ok(PartitionInfo { name: name.clone(), time_min: lo, time_max: hi })
            })
            .collect();
        Ok(TableMetadata {
            ordering: vec!["sym", "time"],
            partitioning: Partitioning {
                kind: "time",
                scheme: format!("{:?}", self.scheme),
            },
            capabilities: Capabilities {
                projection_pushdown: true,
                predicate_pushdown: true,
                limit_pushdown: true,
            },
            partitions: infos?,
        })
    }
}
