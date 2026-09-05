//! Table 层核心：创建 / 打开 / 删除 / 重命名 / 分区操作 / 元数据。

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use splayed_core::{
    CreateDatasetOptions, CoreError, DatasetHandle, Mode, create_dataset, delete_dataset,
    open_dataset,
};
use splayed_format::{Buffer, Column, Data, DataType, DictBuffers, Schema, TimeType};

use crate::partition::{civil_from_days, partition_name, partition_range, value_to_days, PartitionScheme};

/// Table Options：`max_parallelism` 是 Table 内部并行**总预算**——create_table 按
/// `P_part × P_field ≤ max_parallelism` 在「分区并行 × Field 并行」之间切分，
/// 并下沉为每个 Partition `DatasetHandle` 的 Field 级并行上限（驱动
/// write_dataset / scan_dataset 的并行分桶），避免多层并发无上界叠加。
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
            let mut ds = ds;
            if let Some(mp) = self.options.max_parallelism {
                ds.set_max_parallelism(mp);
            }
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

    /// 结构操作用的可变访问（不与视图并存；调用方契约）。
    pub(crate) fn dataset_for_mut(
        &mut self,
        partition: &str,
    ) -> Result<&mut DatasetHandle, CoreError> {
        if !self.datasets.borrow().contains_key(partition) {
            let dir = if self.scheme == PartitionScheme::None {
                self.root.clone()
            } else {
                self.root.join(partition)
            };
            let ds = open_dataset(&dir, self.mode)?;
            let mut ds = ds;
            if let Some(mp) = self.options.max_parallelism {
                ds.set_max_parallelism(mp);
            }
            self.datasets.borrow_mut().insert(partition.to_string(), Box::new(ds));
        }
        let mut borrow = self.datasets.borrow_mut();
        let ptr: *mut DatasetHandle = match borrow.get_mut(partition) {
            Some(b) => &mut **b as *mut DatasetHandle,
            None => unreachable!("just inserted"),
        };
        drop(borrow);
        // 安全性：Box 地址稳定；此刻无其他借用（结构操作不与视图并存）
        Ok(unsafe { &mut *ptr })
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
/// 连续行片段：(输入行起点, 行数)；分区片段按输入行序排列。
type RowSpan = (usize, usize);

/// 把 `data` 中若干**连续行片段**（按序）拼接为新的 owned `Data`（分区 gather）。
///
/// 批量拼接原则：
/// - 定宽列：目标缓冲一次预分配，逐片段 `copy_from_slice`（每片段一次 memcpy，不逐行）；
/// - Utf8 字典列：`remap` 表把全局字典 id 映射为分区局部 id（首现序），字符串仅在
///   首现时拷贝一次；逐行只做 u32 键读取 + 查表，无 String 分配、无哈希表；
/// - validity：`Bitmap::copy_bits_from` 逐片段位拼接（word 级批量，不逐 bit）；
///   源列无 validity → 目标无 validity；拼接后全 1 → 收缩为 None（全有效）。
pub(crate) fn gather_runs(data: &Data, runs: &[RowSpan]) -> Result<Data, CoreError> {
    let total: usize = runs.iter().map(|&(_, len)| len).sum();
    let mut columns = Vec::with_capacity(data.schema.fields.len());
    for field in &data.schema.fields {
        let col = data.column(&field.name).expect("schema iteration guarantees");
        let column = match &col.dict {
            Some(dict) => {
                // Utf8 字典列：remap 重建分区局部字典（NULL 行同样压键，由 validity 屏蔽）
                let dict_offsets = dict.offsets.as_slice();
                let dict_strings = dict.strings.as_slice();
                let n_dict = dict_offsets.len() / 8 - 1;
                let mut remap: Vec<u32> = vec![u32::MAX; n_dict];
                let mut new_keys: Vec<u32> = Vec::with_capacity(total);
                let mut new_offsets: Vec<u64> = vec![0];
                let mut new_strings: Vec<u8> = Vec::new();
                let mut dict_count: u32 = 0;
                let keys = col.values.as_slice();
                for &(start, len) in runs {
                    let row_keys = &keys[start * 4..(start + len) * 4];
                    for kb in row_keys.chunks_exact(4) {
                        let k = u32::from_le_bytes(kb.try_into().unwrap()) as usize;
                        let id = match remap[k] {
                            u32::MAX => {
                                let lo = u64::from_le_bytes(
                                    dict_offsets[k * 8..k * 8 + 8].try_into().unwrap(),
                                ) as usize;
                                let hi = u64::from_le_bytes(
                                    dict_offsets[k * 8 + 8..k * 8 + 16].try_into().unwrap(),
                                ) as usize;
                                new_strings.extend_from_slice(&dict_strings[lo..hi]);
                                new_offsets.push(new_strings.len() as u64);
                                // id 独立计数（new_offsets[0] 为哨兵，不能以 len-1 推 id）
                                let id = dict_count;
                                dict_count += 1;
                                remap[k] = id;
                                id
                            }
                            mapped => mapped,
                        };
                        new_keys.push(id);
                    }
                }
                let validity = gather_validity(&col.validity, runs, total)?;
                Column {
                    data_type: DataType::Utf8,
                    values: Buffer::from_vec(
                        new_keys.iter().flat_map(|k| k.to_le_bytes()).collect::<Vec<u8>>(),
                    ),
                    validity,
                    dict: Some(DictBuffers {
                        offsets: Buffer::from_vec(
                            new_offsets.iter().flat_map(|o| o.to_le_bytes()).collect::<Vec<u8>>(),
                        ),
                        strings: Buffer::from_vec(new_strings),
                    }),
                }
            }
            None => {
                let size = field.data_type.size_of();
                let mut values = Buffer::zeroed_aligned(total * size, 8);
                {
                    let dst = values.as_mut_slice();
                    let src = col.values.as_slice();
                    let mut out = 0usize;
                    for &(start, len) in runs {
                        dst[out..out + len * size]
                            .copy_from_slice(&src[start * size..(start + len) * size]);
                        out += len * size;
                    }
                }
                let validity = gather_validity(&col.validity, runs, total)?;
                Column { data_type: field.data_type, values, validity, dict: None }
            }
        };
        columns.push(column);
    }
    Data::new(data.schema.clone(), columns).map_err(CoreError::from)
}

/// 逐片段位拼接 validity（`Bitmap::copy_bits_from` word 级批量）；
/// 源列无 validity → None（全有效）；拼接后无一个 NULL → 收缩为 None。
fn gather_validity(
    validity: &Option<splayed_format::Bitmap>,
    runs: &[RowSpan],
    total: usize,
) -> Result<Option<splayed_format::Bitmap>, CoreError> {
    let src = match validity {
        Some(bm) => bm,
        None => return Ok(None),
    };
    let src_view = src.as_view();
    let mut dst = splayed_format::Bitmap::zeros(total);
    let mut out = 0usize;
    for &(start, len) in runs {
        let piece = src_view.slice(start, len).map_err(CoreError::from)?;
        dst.copy_bits_from(out, &piece, len);
        out += len;
    }
    if dst.count_ones() == total {
        Ok(None) // 全有效：不落 validity 区
    } else {
        Ok(Some(dst))
    }
}

/// 分区粗键：同 key ⇒ 同分区名（整数判别，避免逐行构造分区名字符串）。
fn partition_key(scheme: PartitionScheme, value: i64, tt: TimeType) -> i64 {
    match scheme {
        PartitionScheme::Date => value_to_days(value, tt),
        PartitionScheme::Year | PartitionScheme::Month => {
            let (y, m, _) = civil_from_days(value_to_days(value, tt));
            match scheme {
                PartitionScheme::Year => y,
                _ => y * 12 + i64::from(m) - 1,
            }
        }
        PartitionScheme::None => 0,
    }
}

/// 一次线性扫描 time 列，产出每个分区的**连续行片段**（片段按输入行序）。
///
/// 输入按 (sym ASC, time ASC) 契约有序 → sym run 内 time 单调 → 同名分区的行
/// 在输入中连续成段（片段可跨 sym 边界，拼接后分区内仍保持 (sym, time) 序）；
/// 分区名字符串只在换段时构造（段内用整数粗键判别，无逐行分配）。
fn partition_spans(
    data: &Data,
    scheme: PartitionScheme,
    tt: TimeType,
) -> Result<Vec<(String, Vec<RowSpan>)>, CoreError> {
    let time_view = data.column("time").unwrap().as_view();
    let mut buckets: HashMap<String, Vec<RowSpan>> = HashMap::new();
    let mut cur_key: Option<i64> = None;
    let mut cur_first_t = 0i64;
    let (mut cur_start, mut cur_len) = (0usize, 0usize);
    for i in 0..data.length() {
        let t = time_value_at(&time_view, i)?;
        let key = partition_key(scheme, t, tt);
        match cur_key {
            Some(k) if k == key => cur_len += 1,
            _ => {
                if let Some(_) = cur_key {
                    let name = partition_name(scheme, cur_first_t, tt);
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
        let name = partition_name(scheme, cur_first_t, tt);
        buckets.entry(name).or_default().push((cur_start, cur_len));
    }
    let mut names: Vec<String> = buckets.keys().cloned().collect();
    names.sort();
    Ok(names
        .into_iter()
        .map(|n| {
            let runs = buckets.remove(&n).expect("name from keys");
            (n, runs)
        })
        .collect())
}

/// 创建完整 Table（`none` → 根目录 Dataset；year/month/date → 按 time 切分 Partition）。
///
/// 并发模型：主线程一次线性扫描产出分区**连续片段** → 分区并行创建（round-robin
/// 分桶），并行预算切分 `P_part × P_field ≤ max_parallelism`（与 create_dataset
/// 内部的 Field 级并行共享总预算，消除无上界嵌套）；分区 gather 在各工作线程内
/// 批量拼接（连续片段 memcpy + validity word 级位拼接）。失败语义：单分区原子
/// （tmp + rename），已创建分区保留、不回滚；不改变 Partition 内数据顺序。
pub fn create_table(
    table_path: &Path,
    data: Data,
    scheme: PartitionScheme,
    options: TableOptions,
) -> Result<(), CoreError> {
    if table_path.exists() {
        return Err(CoreError::AlreadyExists(table_path.to_path_buf()));
    }
    if data.column("sym").is_none() || data.column("time").is_none() {
        return Err(CoreError::Invalid("table input requires sym and time columns".into()));
    }
    let tt = infer_tt(data.column("time").unwrap().data_type)?;
    let max_p = options
        .max_parallelism
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
        .max(1);
    if data.length() == 0 {
        // 空数据：仅创建根目录（无 META、无分区；open_table 发现为空表）
        fs::create_dir_all(table_path).map_err(CoreError::Io)?;
        return Ok(());
    }
    if scheme == PartitionScheme::None {
        // 单 Dataset 快速路径：列所有权直接移动，不克隆
        return create_dataset(table_path, data, CreateDatasetOptions { max_parallelism: max_p });
    }
    // ① 主线程：一次线性扫描 → 每分区连续行片段（分区名仅换段时构造）
    let buckets = partition_spans(&data, scheme, tt)?;
    // ② 并行创建分区：P_part × P_field ≤ max_parallelism 预算切分
    let p_part = max_p.min(buckets.len());
    if p_part <= 1 {
        // 单分区 / 并行度 1：串行创建，Field 级并行拿满预算
        for (name, runs) in buckets {
            let sub = gather_runs(&data, &runs)?;
            create_dataset(
                &table_path.join(&name),
                sub,
                CreateDatasetOptions { max_parallelism: max_p },
            )?;
        }
        return Ok(());
    }
    let p_field = (max_p / p_part).max(1);
    let mut groups: Vec<Vec<(String, Vec<RowSpan>)>> = vec![Vec::new(); p_part];
    for (i, bucket) in buckets.into_iter().enumerate() {
        groups[i % p_part].push(bucket);
    }
    // 共享引用先行绑定：move 闭包只捕获 &Data，不移动本体
    let data_ref = &data;
    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for group in groups {
            handles.push(s.spawn(move || -> Result<(), CoreError> {
                for (name, runs) in group {
                    // 分区内 gather（批量拼接）+ create_dataset（Field 级并行 p_field）
                    let sub = gather_runs(data_ref, &runs)?;
                    create_dataset(
                        &table_path.join(&name),
                        sub,
                        CreateDatasetOptions { max_parallelism: p_field },
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
    })
}

/// 新增单个 Partition / Dataset（partition_name 必须符合既有 scheme 且不存在）。
///
/// 并发模型：主线程轻量校验（路径 / 分区名 / 非空）后**直接委托** `create_dataset`——
/// 它是唯一执行数据 I/O 的地方（META 校验 + Field 并行创建由 Dataset 层统一管理，
/// Table 层不嵌套并行）。信任调用者：`data` 属于 `partition_name`，不做 O(N) 逐行
/// 分区归属校验；行序合法性由 MetaBuilder 构建期校验（在任何盘上落痕之前失败）。
pub fn create_table_partition(
    table_path: &Path,
    partition_name: &str,
    data: Data,
) -> Result<(), CoreError> {
    // ① 主线程轻量校验：Table 根必须存在（不隐式引导建表）
    if !table_path.is_dir() {
        return Err(CoreError::NotFound(table_path.to_path_buf()));
    }
    let scheme = PartitionScheme::from_partition_name(partition_name).ok_or_else(|| {
        CoreError::Invalid(format!(
            "partition name '{partition_name}' does not match any scheme"
        ))
    })?;
    let existing = existing_scheme(table_path)?;
    match existing {
        Some(prev) if prev != scheme => {
            return Err(CoreError::Invalid(
                "existing partitions use a different partition scheme".into(),
            ))
        }
        _ => {}
    }
    let partition_path = table_path.join(partition_name);
    if partition_path.exists() {
        return Err(CoreError::AlreadyExists(partition_path));
    }
    // ② 输入校验：必须含 sym / time；空分区拒绝（不创建空 Dataset）
    if data.column("sym").is_none() || data.column("time").is_none() {
        return Err(CoreError::Invalid("partition data requires sym and time columns".into()));
    }
    if data.length() == 0 {
        return Err(CoreError::Invalid("partition data must not be empty".into()));
    }
    // ③ 委托 Dataset 层：列所有权直接移交（无 gather / 克隆）；
    //    失败语义与 create_table 一致——不回滚，已写入文件保留
    create_dataset(&partition_path, data, CreateDatasetOptions::default())
}

/// 删除整个 Table（根目录及全部 Partition Dataset）。
pub fn delete_table(table_path: &Path) -> Result<(), CoreError> {
    fs::remove_dir_all(table_path).map_err(|e| CoreError::Io(e))
}

/// 删除单个 Partition（Dataset 目录）。
///
/// 委托 `delete_dataset` 递归删除；不打开 Dataset、不扫描数据（删除非热路径，
/// 无需并行）。分区不存在 → `NotFound`（显式语义，不做幂等删除，避免掩盖逻辑错误）。
/// 并发契约：调用方保证该 Partition 无打开的 Dataset 句柄（Windows 下打开的
/// mmap 会阻止删除；由 Table 层生命周期保证）。
pub fn delete_table_partition(table_path: &Path, partition_name: &str) -> Result<(), CoreError> {
    if !table_path.is_dir() {
        return Err(CoreError::NotFound(table_path.to_path_buf()));
    }
    // 分区名校验：非 scheme 命名的子目录一律拒绝删除（防误删任意目录）
    if PartitionScheme::from_partition_name(partition_name).is_none() {
        return Err(CoreError::Invalid(format!(
            "partition name '{partition_name}' does not match any scheme"
        )));
    }
    let partition_path = table_path.join(partition_name);
    if !partition_path.is_dir() {
        return Err(CoreError::NotFound(partition_path));
    }
    delete_dataset(&partition_path)
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

    // -------------------------------------------------- Field 结构操作

    /// 所有 Partition 新增全 NULL 字段（core `create_field_file(init = length(L_p))`）。
    /// 前置：所有 Partition 均不含 `field`；不保证跨 Partition 原子。
    pub fn create_table_field(
        &mut self,
        field: &str,
        data_type: DataType,
    ) -> Result<(), CoreError> {
        if field == "sym" || field == "time" {
            return Err(CoreError::Invalid(format!("'{field}' is managed by META")));
        }
        let partitions = self.discover_partitions();
        for p in &partitions {
            if self.dataset_for(p)?.read_dataset_schema().position(field).is_some() {
                return Err(CoreError::Invalid(format!(
                    "field '{field}' already exists in partition '{p}'"
                )));
            }
        }
        for p in &partitions {
            self.dataset_for_mut(p)?
                .create_dataset_field(field, data_type, splayed_core::DatasetFieldInit::AllNull)?;
        }
        Ok(())
    }

    /// 删除所有 Partition 中的同名字段。前置：所有 Partition 均含该字段。
    pub fn delete_table_field(&mut self, field: &str) -> Result<(), CoreError> {
        if field == "sym" || field == "time" {
            return Err(CoreError::Invalid(format!("'{field}' is managed by META")));
        }
        let partitions = self.discover_partitions();
        for p in &partitions {
            if self.dataset_for(p)?.read_dataset_schema().position(field).is_none() {
                return Err(CoreError::Invalid(format!(
                    "field '{field}' missing in partition '{p}'"
                )));
            }
        }
        for p in &partitions {
            self.dataset_for_mut(p)?.delete_dataset_field(field)?;
        }
        Ok(())
    }

    /// 更新所有 Partition 中该字段的 header 物理属性
    /// （data_type / row_count 由 core 强制为现值；类型转换走 `cast_table_field`）。
    pub fn update_table_field(
        &mut self,
        field: &str,
        header: splayed_format::FieldHeader,
    ) -> Result<(), CoreError> {
        let partitions = self.discover_partitions();
        for p in &partitions {
            if self.dataset_for(p)?.read_dataset_schema().position(field).is_none() {
                return Err(CoreError::Invalid(format!(
                    "field '{field}' missing in partition '{p}'"
                )));
            }
        }
        for p in &partitions {
            self.dataset_for_mut(p)?.update_dataset_field_header(field, header)?;
        }
        Ok(())
    }

    /// 重命名所有 Partition 中的同名字段。前置：均含 `field` 且均无 `new_name`。
    pub fn rename_table_field(&mut self, field: &str, new_name: &str) -> Result<(), CoreError> {
        let partitions = self.discover_partitions();
        for p in &partitions {
            let schema = self.dataset_for(p)?.read_dataset_schema();
            if schema.position(field).is_none() {
                return Err(CoreError::Invalid(format!(
                    "field '{field}' missing in partition '{p}'"
                )));
            }
            if schema.position(new_name).is_some() {
                return Err(CoreError::Invalid(format!(
                    "field '{new_name}' already exists in partition '{p}'"
                )));
            }
        }
        for p in &partitions {
            self.dataset_for_mut(p)?.rename_dataset_field(field, new_name)?;
        }
        Ok(())
    }

    /// 转换所有 Partition 中该字段的类型；中途失败直接重试补齐
    /// （已为目标类型的 Partition 再转换是无害 no-op）。
    pub fn cast_table_field(&mut self, field: &str, target_type: DataType) -> Result<(), CoreError> {
        let partitions = self.discover_partitions();
        for p in &partitions {
            if self.dataset_for(p)?.read_dataset_schema().position(field).is_none() {
                return Err(CoreError::Invalid(format!(
                    "field '{field}' missing in partition '{p}'"
                )));
            }
        }
        for p in &partitions {
            self.dataset_for_mut(p)?.cast_dataset_field(field, target_type)?;
        }
        Ok(())
    }

    /// 压缩所有 Partition 中的同名字段（前置：均为 uncompressed）。
    pub fn compress_table_field(&mut self, field: &str) -> Result<(), CoreError> {
        let partitions = self.discover_partitions();
        for p in &partitions {
            if self.dataset_for(p)?.read_dataset_schema().position(field).is_none() {
                return Err(CoreError::Invalid(format!(
                    "field '{field}' missing in partition '{p}'"
                )));
            }
        }
        for p in &partitions {
            self.dataset_for_mut(p)?.compress_dataset_field(field)?;
        }
        Ok(())
    }

    /// 解压所有 Partition 中的同名字段（前置：均为 compressed）。
    pub fn decompress_table_field(&mut self, field: &str) -> Result<(), CoreError> {
        let partitions = self.discover_partitions();
        for p in &partitions {
            if self.dataset_for(p)?.read_dataset_schema().position(field).is_none() {
                return Err(CoreError::Invalid(format!(
                    "field '{field}' missing in partition '{p}'"
                )));
            }
        }
        for p in &partitions {
            self.dataset_for_mut(p)?.decompress_dataset_field(field)?;
        }
        Ok(())
    }
}
