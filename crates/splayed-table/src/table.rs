//! Table 层核心：创建 / 打开 / 删除 / 重命名 / 分区操作 / 元数据。

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use splayed_core::{
    CreateDatasetOptions, CreateFieldOptions, CoreError, DatasetHandle, DatasetStatistics, Mode,
    delete_dataset, open_dataset,
};
use splayed_format::{Buffer, Column, Data, DataView, DataType, DictBuffers, Schema, TimeType};

use crate::partition::{civil_from_days, partition_name, partition_range, value_to_days, PartitionScheme};

/// Table Options：`max_parallelism` 是 Table 内部并行**总预算**——create_table 按
/// `P_part × P_field ≤ max_parallelism` 在「分区并行 × Field 并行」之间切分，
/// 并下沉为每个 Partition `DatasetHandle` 的 Field 级并行上限（驱动
/// write_dataset / scan_dataset 的并行分桶），避免多层并发无上界叠加。
/// `compression` / `chunk_syms` 为**创建即压缩**策略：create_table /
/// create_table_field 按 sym 对齐 chunk 边界（每 chunk_syms 个连续 sym 一组，
/// 行数超过 64K 的 sym 劈开）直接创建压缩 Field；create_table_partition 可按
/// 分区覆盖（新旧分区差异化压缩）。
#[derive(Debug, Clone, Default)]
pub struct TableOptions {
    pub max_parallelism: Option<usize>,
    /// 新建 Field 的 chunk 压缩算法（None / 缺省 = 未压缩）。
    pub compression: Option<splayed_format::Compression>,
    /// 压缩 chunk 的目标行数（自动规划 sym 对齐边界；缺省 = 8192）。
    pub chunk_target_rows: Option<usize>,
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
    /// 逐分区统计缓存（统一缓存模型）：分区名 → DatasetStatistics。
    /// 逐分区统计**不可变**——META immutable + positional overwrite 不改 row_count /
    /// TIME AXIS / sym 字典——按名字 memo 后永不失效；分区集合本身不缓存（read_dir
    /// 微秒级且保证外部 create / delete 的正确性），新建分区在下一次统计时自动纳入。
    pub(crate) stats_cache: RefCell<HashMap<String, DatasetStatistics>>,
}

/// New-field creation options derived from the Table compression strategy.
/// Uncompressed -> default; compressed -> sym-aligned chunk offsets from the
/// partition's own META grid (`chunk_target_rows` default 8192; syms exceeding the
/// chunk row cap are split). Parameterized (does not capture `&self`) so it can be
/// captured safely by `structural_for_each`'s `Sync` closure.
pub(crate) fn field_options(
    compression: splayed_format::Compression,
    chunk_target_rows: usize,
    ds: &DatasetHandle,
) -> splayed_core::CreateFieldOptions {
    if matches!(compression, splayed_format::Compression::None) {
        splayed_core::CreateFieldOptions::default()
    } else {
        splayed_core::CreateFieldOptions {
            compression,
            chunk_offsets: Some(
                ds.sym_aligned_chunk_offsets(chunk_target_rows, splayed_core::CHUNK_ROW_CAP),
            ),
        }
    }
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

    /// 获取当前 Table 的最大并行度预算。
    pub fn max_parallelism(&self) -> usize {
        self.options
            .max_parallelism
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()))
            .max(1)
    }

    /// 动态设置当前 Table 的最大并行度预算，并级联更新所有已打开的 Dataset 缓存。
    pub fn set_max_parallelism(&mut self, max_parallelism: usize) {
        let p = max_parallelism.max(1);
        self.options.max_parallelism = Some(p);
        let guard = self.datasets.borrow();
        for ds in guard.values() {
            ds.set_max_parallelism(p);
        }
    }

    /// 确保目标分区的 DatasetHandle 已打开并缓存。
    pub(crate) fn ensure_dataset(&self, partition: &str) -> Result<(), CoreError> {
        if !self.datasets.borrow().contains_key(partition) {
            let dir = if self.scheme == PartitionScheme::None {
                self.root.clone()
            } else {
                self.root.join(partition)
            };
            let ds = open_dataset(&dir, self.mode)?;
            ds.set_max_parallelism(self.max_parallelism());
            self.datasets.borrow_mut().insert(partition.to_string(), Box::new(ds));
        }
        Ok(())
    }

    /// 打开（或取缓存）Partition 对应的 Dataset。`none` 模式 name 为空 → 根目录 Dataset。
    pub(crate) fn dataset_for(&self, partition: &str) -> Result<&DatasetHandle, CoreError> {
        self.ensure_dataset(partition)?;
        let borrow = self.datasets.borrow();
        let ptr: *const DatasetHandle = match borrow.get(partition) {
            Some(b) => &**b as *const DatasetHandle,
            None => unreachable!("just ensured"),
        };
        // 安全性：Box 地址稳定；条目只增不删
        Ok(unsafe { &*ptr })
    }

    /// 时间单位（统一缓存模型：直接读第一个可用分区的 **64B META header** 推断，
    /// 不经 DatasetHandle 打开——保持 scan / 元数据读路径的惰性；结果缓存）。
    pub(crate) fn peek_time_type(&self) -> Result<TimeType, CoreError> {
        if let Some(tt) = self.time_type.borrow().as_ref() {
            return Ok(*tt);
        }
        let meta_path = if self.scheme == PartitionScheme::None {
            self.root.join(".meta")
        } else {
            let first = self
                .discover_partitions()
                .first()
                .cloned()
                .ok_or_else(|| CoreError::Invalid("table has no partitions".into()))?;
            self.root.join(&first).join(".meta")
        };
        let mut f = fs::File::open(&meta_path).map_err(|e| CoreError::Io(e))?;
        let mut head = [0u8; splayed_format::META_HEADER_SIZE];
        f.read_exact(&mut head).map_err(|e| CoreError::Io(e))?;
        let header = splayed_format::MetaHeader::from_bytes(&head).map_err(CoreError::from)?;
        header.validate().map_err(CoreError::from)?;
        let tt = header.time_type().map_err(CoreError::from)?;
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
/// `time_min` / `time_max` 为该分区**含端点**的时间值范围（由分区名纯推导，
/// 零 META I/O；是数据实际范围的覆盖超集——裁剪 / 元数据语义，非精确统计）。
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

pub(crate) fn infer_tt(dt: DataType) -> Result<TimeType, CoreError> {
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
    if std::env::var("GATHER_DEBUG").is_ok() {
        eprintln!("DBG gather_runs total={} runs={:?} fields={:?}",
            total, runs, data.schema.fields.iter().map(|f| f.name.to_string()).collect::<Vec<_>>());
    }
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
                let keys_u32: &[u32] = bytemuck::cast_slice(keys);
                let mut last_k = u32::MAX;
                let mut last_id = 0u32;
                for &(start, len) in runs {
                    let sub_keys = &keys_u32[start..start + len];
                    for &k_raw in sub_keys {
                        if k_raw == last_k {
                            new_keys.push(last_id);
                            continue;
                        }
                        let k = k_raw as usize;
                        let id = match remap[k] {
                            u32::MAX => {
                                let lo_bytes = dict_offsets
                                    .get(k * 8..k * 8 + 8)
                                    .expect("dict offsets lo out of range");
                                let hi_bytes = dict_offsets
                                    .get(k * 8 + 8..k * 8 + 16)
                                    .expect("dict offsets hi out of range");
                                let lo =
                                    u64::from_le_bytes(lo_bytes.try_into().unwrap()) as usize;
                                let hi =
                                    u64::from_le_bytes(hi_bytes.try_into().unwrap()) as usize;
                                if hi > dict_strings.len() {
                                    panic!(
                                        "dict strings out of range: field={} k={k} lo={lo} hi={hi} strings_len={} offs_len={} n_dict={}",
                                        field.name,
                                        dict_strings.len(),
                                        dict_offsets.len(),
                                        n_dict,
                                    );
                                }
                                new_strings.extend_from_slice(&dict_strings[lo..hi]);
                                new_offsets.push(new_strings.len() as u64);
                                let id = dict_count;
                                dict_count += 1;
                                remap[k] = id;
                                id
                            }
                            mapped => mapped,
                        };
                        last_k = k_raw;
                        last_id = id;
                        new_keys.push(id);
                    }
                }
                let new_keys_raw = bytemuck::cast_slice::<u32, u8>(&new_keys).to_vec();
                let new_offs_raw = bytemuck::cast_slice::<u64, u8>(&new_offsets).to_vec();
                let validity = gather_validity(&col.validity, runs, total)?;
                Column {
                    data_type: DataType::Utf8,
                    values: Buffer::from_vec(new_keys_raw),
                    validity,
                    dict: Some(DictBuffers {
                        offsets: Buffer::from_vec(new_offs_raw),
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
pub(crate) fn partition_key(scheme: PartitionScheme, value: i64, tt: TimeType) -> i64 {
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
pub(crate) fn partition_spans(
    data: &Data,
    scheme: PartitionScheme,
    tt: TimeType,
) -> Result<Vec<(String, Vec<RowSpan>)>, CoreError> {
    let time_col = data.column("time").unwrap();
    let time_bytes = time_col.values.as_slice();
    let rows = data.length();
    let mut buckets: HashMap<String, Vec<RowSpan>> = HashMap::new();
    let mut cur_key: Option<i64> = None;
    let mut cur_first_t = 0i64;
    let (mut cur_start, mut cur_len) = (0usize, 0usize);

    let mut last_days: i64 = i64::MIN;
    let mut last_k: i64 = 0;

    let mut compute_key = |t: i64| -> i64 {
        if scheme == PartitionScheme::None {
            return 0;
        }
        let days = value_to_days(t, tt);
        if days == last_days {
            return last_k;
        }
        let k = match scheme {
            PartitionScheme::Date => days,
            PartitionScheme::Year => {
                let (y, _, _) = civil_from_days(days);
                y
            }
            PartitionScheme::Month => {
                let (y, m, _) = civil_from_days(days);
                y * 12 + i64::from(m) - 1
            }
            PartitionScheme::None => 0,
        };
        last_days = days;
        last_k = k;
        k
    };

    macro_rules! scan_time {
        ($slice:expr) => {
            for i in 0..rows {
                let t = $slice[i] as i64;
                let key = compute_key(t);
                match cur_key {
                    Some(k) if k == key => cur_len += 1,
                    _ => {
                        if cur_key.is_some() {
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
        };
    }

    match tt {
        TimeType::Date32 => {
            let slice: &[i32] = bytemuck::cast_slice(time_bytes);
            scan_time!(slice);
        }
        TimeType::TimestampUs => {
            let slice: &[i64] = bytemuck::cast_slice(time_bytes);
            scan_time!(slice);
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

/// 连带拥有型数据创建完整分区表（底层实现路径）。
pub fn init_table_data(
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
    // 创建即压缩策略（sym 对齐 chunk 边界由 core 从输入 sym run 推导）
    let ds_options = CreateDatasetOptions {
        max_parallelism: max_p,
        compression: options.compression.unwrap_or(splayed_format::Compression::None),
        chunk_target_rows: options.chunk_target_rows.unwrap_or(8192),
    };
    if scheme == PartitionScheme::None {
        // 单 Dataset 快速路径：列所有权直接移动，不克隆
        return splayed_core::create_dataset_data(table_path, data, ds_options);
    }
    // ① 主线程：一次线性扫描 → 每分区连续行片段（分区名仅换段时构造）
    let buckets = partition_spans(&data, scheme, tt)?;
    // ② 并行创建分区：P_part × P_field ≤ max_parallelism 预算切分
    let p_part = max_p.min(buckets.len());
    if p_part <= 1 {
        // 单分区 / 并行度 1：串行创建，Field 级并行拿满预算
        for (name, runs) in buckets {
            let sub = gather_runs(&data, &runs)?;
            splayed_core::create_dataset_data(&table_path.join(&name), sub, ds_options.clone())?;
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
    let ds_options_ref = &ds_options;
    std::thread::scope(|s| {
        let mut handles = Vec::new();
        for group in groups {
            handles.push(s.spawn(move || -> Result<(), CoreError> {
                for (name, runs) in group {
                    // 分区内 gather（批量拼接）+ create_dataset（Field 级并行 p_field）
                    let sub = gather_runs(data_ref, &runs)?;
                    splayed_core::create_dataset_data(
                        &table_path.join(&name),
                        sub,
                        CreateDatasetOptions {
                            max_parallelism: p_field,
                            ..ds_options_ref.clone()
                        },
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
    options: CreateDatasetOptions,
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
    // ③ 委托 Dataset 层：列所有权直接移交（无 gather / 克隆）；options 携带该
    //    分区的创建即压缩策略（可与既有分区差异化——冷热分层）；
    //    失败语义与 create_table 一致——不回滚，已写入文件保留
    splayed_core::create_dataset_data(&partition_path, data, options)
}

/// 基于 `&DataView` 零拷贝视图新增单个 Partition / Dataset。
pub fn create_table_partition_from_view(
    table_path: &Path,
    partition_name: &str,
    data: &DataView<'_>,
    options: CreateDatasetOptions,
) -> Result<(), CoreError> {
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
    if data.column("sym").is_none() || data.column("time").is_none() {
        return Err(CoreError::Invalid("partition data requires sym and time columns".into()));
    }
    if data.length() == 0 {
        return Err(CoreError::Invalid("partition data must not be empty".into()));
    }
    splayed_core::create_dataset_from_view(&partition_path, data, options)
}

/// 连带数据直接初始化创建完整 Table 并返回 TableHandle（零拷贝入参）。
pub fn init_table(
    table_path: &Path,
    scheme: PartitionScheme,
    data: &DataView<'_>,
    options: Option<TableOptions>,
) -> Result<TableHandle, CoreError> {
    let opts = options.unwrap_or_default();
    let owned = splayed_core::dataset::data_view_to_owned_data(data)?;
    init_table_data(table_path, owned, scheme, opts.clone())?;
    open_table(table_path, Mode::Write, opts)
}

/// 兼容拥有型 Data 初始化。
pub fn create_table_data(
    table_path: &Path,
    data: Data,
    scheme: PartitionScheme,
    options: TableOptions,
) -> Result<(), CoreError> {
    init_table_data(table_path, data, scheme, options)
}

/// 默认当前最新分区名推导（公历当天/当月/当年）。
pub(crate) fn default_latest_partition_name(scheme: PartitionScheme) -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = (secs / 86400) as i64;
    let (y, m, d) = crate::partition::civil_from_days(days);
    match scheme {
        PartitionScheme::None => String::new(),
        PartitionScheme::Year => format!("year={y}"),
        PartitionScheme::Month => format!("month={y:04}-{m:02}"),
        PartitionScheme::Date => format!("date={y:04}-{m:02}-{d:02}"),
    }
}

/// 按 Schema 创建空 Table，并预先初始化最新分区（或指定分区）的空骨架。
/// 物理磁盘上仅生成各字段 64B Header-Only 文件，0 实际数据 I/O。
pub fn create_table(
    table_path: &Path,
    schema: &Schema,
    scheme: PartitionScheme,
    initial_partition: Option<&str>,
    options: Option<TableOptions>,
) -> Result<TableHandle, CoreError> {
    if table_path.exists() && (table_path.read_dir().map_err(CoreError::Io)?.next().is_some()) {
        return Err(CoreError::AlreadyExists(table_path.to_path_buf()));
    }
    fs::create_dir_all(table_path).map_err(CoreError::Io)?;
    let opts = options.unwrap_or_default();
    if scheme == PartitionScheme::None {
        splayed_core::create_dataset(table_path, schema)?;
    } else {
        let part_name = match initial_partition {
            Some(p) => p.to_string(),
            None => default_latest_partition_name(scheme),
        };
        let part_path = table_path.join(&part_name);
        splayed_core::create_dataset(&part_path, schema)?;
    }
    open_table(table_path, Mode::Write, opts)
}

/// 销毁并删除 Table 根目录（关闭句柄并删除物理目录）。
pub fn drop_table(handle: TableHandle) -> Result<(), CoreError> {
    let path = handle.root.clone();
    close_table(handle)?;
    fs::remove_dir_all(&path).map_err(|e| CoreError::Io(e))
}

/// 删除整个 Table（根目录及全部 Partition Dataset）。
///
/// 不逐 Partition 并行删除——文件系统级递归删除（remove_dir_all）比用户态遍历
/// 更快且无锁竞争。显式 `NotFound`（不做幂等删除）。并发契约：调用方保证无打开的
/// TableHandle 引用该 Table（悬垂句柄防护由上层生命周期保证）；失败时目录可能
/// 半删（API 无事务保证）。
pub fn delete_table(table_path: &Path) -> Result<(), CoreError> {
    if !table_path.is_dir() {
        return Err(CoreError::NotFound(table_path.to_path_buf()));
    }
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
///
/// 绝对 O(1)：只做目录 rename，不重建 Table、不复制数据；跨文件系统不支持
/// （fs::rename 失败即报错，无移动语义）。重命名后路径缓存失效——上层需重新
/// `open_table`；并发契约：调用方保证无打开的 TableHandle 引用旧路径。
pub fn rename_table(table_path: &Path, new_name: &str) -> Result<(), CoreError> {
    if new_name.is_empty() || new_name.starts_with('.') || new_name.contains(['/', '\\', '=']) {
        return Err(CoreError::Invalid(format!("invalid table name '{new_name}'")));
    }
    if !table_path.is_dir() {
        return Err(CoreError::NotFound(table_path.to_path_buf()));
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
        stats_cache: RefCell::new(HashMap::new()),
    })
}

/// 读取 Table 的统合 Schema。
#[inline]
pub fn read_table_schema(table: &TableHandle) -> Result<Schema, CoreError> {
    table.read_table_schema()
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

    /// 显式关闭 TableHandle（与 close() 等价）。
    #[inline]
    pub fn close_table(self) -> Result<(), CoreError> {
        self.close()
    }
    /// Table Schema：最后一个 Partition 的 Dataset Schema（不做多 Partition merge）。
    /// 逐分区统计（统一缓存模型）：命中 `stats_cache` 直接返回（零 I/O）；
    /// 未命中经缓存的 DatasetHandle 读取一次（META header + TIME AXIS 端点）后 memo。
    /// 逐分区统计不可变（META immutable + positional overwrite 不改 row_count），
    /// memo 永不失效；分区集合每次 discover（read_dir 微秒级），外部 create / delete
    /// 的分区在下一次调用自动纳入 / 剔除。
    fn partition_stats(&self, name: &str) -> Result<DatasetStatistics, CoreError> {
        if let Some(s) = self.stats_cache.borrow().get(name) {
            return Ok(s.clone());
        }
        let s = self.dataset_for(name)?.statistics()?;
        self.stats_cache.borrow_mut().insert(name.to_string(), s.clone());
        Ok(s)
    }

    /// Table 逻辑 Schema（sym / time + 字段）= 最后一个 Partition（分区名稳定排序）
    /// 的 Dataset Schema。统一缓存模型：只访问最后一个 Partition，不遍历、不 merge
    /// （跨分区 Schema 一致性由写侧保证，读侧信任——见 §4.6 严格前置校验）；经
    /// `dataset_for` 打开的 Dataset 内部缓存 Schema，重复调用零 I/O。
    /// 空表（无分区）无法经 `open_table` 打开（scheme 无法推断），此处不出现。
    pub fn schema(&self) -> Result<Schema, CoreError> {
        let last = self
            .discover_partitions()
            .last()
            .cloned()
            .unwrap_or_default();
        Ok(self.dataset_for(&last)?.schema())
    }

    #[inline]
    pub fn read_table_schema(&self) -> Result<Schema, CoreError> {
        self.schema()
    }

    /// 聚合各 Partition 的统计（row_count 求和；min/max 跨 Partition 聚合）。
    /// Table 级统计：各 Partition 统计聚合（row_count 求和、min/max 归并）。
    pub fn statistics(&self) -> Result<TableStatistics, CoreError> {
        let partitions = self.discover_partitions();
        // 先确保逐分区统计全部 memo（命中零 I/O），再按引用归并（无逐分区 clone）
        for p in &partitions {
            self.partition_stats(p)?;
        }
        let cache = self.stats_cache.borrow();
        let mut row_count: u64 = 0;
        let mut time_min: Option<i64> = None;
        let mut time_max: Option<i64> = None;
        let mut sym_min: Option<&str> = None;
        let mut sym_max: Option<&str> = None;
        for p in &partitions {
            let s = &cache[p.as_str()];
            row_count = row_count
                .checked_add(s.row_count)
                .ok_or_else(|| CoreError::Invalid("row_count overflow in statistics".into()))?;
            time_min = match (time_min, s.time_min) {
                (None, v) => Some(v),
                (Some(a), b) => Some(a.min(b)),
            };
            time_max = match (time_max, s.time_max) {
                (None, v) => Some(v),
                (Some(a), b) => Some(a.max(b)),
            };
            sym_min = match (sym_min, s.sym_min.as_deref()) {
                (None, Some(x)) => Some(x),
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, None) => a,
            };
            sym_max = match (sym_max, s.sym_max.as_deref()) {
                (None, Some(x)) => Some(x),
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, None) => a,
            };
        }
        Ok(TableStatistics {
            row_count,
            partition_count: partitions.len() as u32,
            time_min: time_min.unwrap_or(0),
            time_max: time_max.unwrap_or(0),
            sym_min: sym_min.map(String::from),
            sym_max: sym_max.map(String::from),
        })
    }

    #[inline]
    pub fn read_table_statistics(&self) -> Result<TableStatistics, CoreError> {
        self.statistics()
    }

    /// Table 组织信息（ordering / partitioning / capabilities / partitions；
    /// none 模式无分区子目录，partitions 为空）。
    /// Table 组织信息与执行能力。统一缓存模型中最优路径：PartitionInfo 的
    /// time_min / time_max 直接由分区名经 `partition_range` **纯计算推导**（零 META
    /// I/O、天然不可变），分区列表来自 discover（保证外部 create / delete 正确）；
    /// 不含 row_count（当前无消费者，避免把统计 I/O 拖进本路径；需要时走
    /// `read_table_statistics` 的 memo 缓存）。
    pub fn metadata(&self) -> Result<TableMetadata, CoreError> {
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
                // 含端点语义（与 DatasetStatistics / RowRange 一致）：hi 为排他上界，
                // 有效分区名保证 hi > lo
                Ok(PartitionInfo { name: name.clone(), time_min: lo, time_max: hi - 1 })
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

    #[inline]
    pub fn read_table_metadata(&self) -> Result<TableMetadata, CoreError> {
        self.metadata()
    }

    // -------------------------------------------------- Field 结构操作

    /// Field 结构操作的统一执行器（统一并发模型）：
    /// 主线程串行确保所有 Partition 打开（Dataset 缓存复用）→ `values_mut` 收集
    /// 互不相交的 `&mut DatasetHandle` → round-robin 分桶 `std::thread::scope`
    /// 并行执行（P = min(max_parallelism, 分区数)；单分区 / 并行度 1 走串行快路径）。
    /// Table 层不管理 Field 级并发（各 Dataset 内部策略自理，无嵌套并行）。
    /// 失败语义：返回首个错误，已完成的 Partition 不回滚（与 create_table 一致）；
    /// 空表（无分区）vacuous Ok。
    fn structural_for_each<F>(&self, f: F) -> Result<(), CoreError>
    where
        F: Fn(&mut DatasetHandle) -> Result<(), CoreError> + Sync,
    {
        let partitions = self.discover_partitions();
        if partitions.is_empty() {
            return Ok(());
        }
        for p in &partitions {
            self.dataset_for(p)?;
        }
        let mut guard = self.datasets.borrow_mut();
        // 仅当前分区集：排除分区被外部删除后残留在缓存的过期句柄
        let name_set: std::collections::HashSet<&str> =
            partitions.iter().map(|s| s.as_str()).collect();
        let targets: Vec<&mut DatasetHandle> = guard
            .iter_mut()
            .filter_map(|(k, v)| name_set.contains(k.as_str()).then(|| &mut **v))
            .collect();
        let p = self.max_parallelism().min(targets.len());
        if p <= 1 {
            for h in targets {
                f(h)?;
            }
            return Ok(());
        }
        let mut buckets: Vec<Vec<&mut DatasetHandle>> = (0..p).map(|_| Vec::new()).collect();
        for (i, h) in targets.into_iter().enumerate() {
            buckets[i % p].push(h);
        }
        // 共享引用先行绑定：move 闭包只捕获 &F（F: Sync），不按值移动
        let f_ref = &f;
        std::thread::scope(|s| {
            let mut joins = Vec::new();
            for bucket in buckets {
                joins.push(s.spawn(move || -> Result<(), CoreError> {
                    for h in bucket {
                        f_ref(h)?;
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
                            "structural op thread panicked".into(),
                        ));
                    }
                }
            }
            first_err.map_or(Ok(()), Err)
        })
    }


    /// 在最新分区中初始化新增单列：
    /// - 仅在自然排序的最新分区（最新 Dataset）创建对应字段物理文件；
    /// - 无需指定 rows，自动与最新分区的 index 行数（row_count）对齐；
    /// - 产出全 NULL 延迟展开骨架（64B Header-Only，零磁盘数据页分配）；
    /// - 自动更新最新分区的元数据与 Schema 缓存。
    pub fn init_field(
        &self,
        field_name: &str,
        data_type: DataType,
        opts: Option<CreateFieldOptions>,
    ) -> Result<(), CoreError> {
        self.mode.require_write("init_field")?;
        if field_name == "sym" || field_name == "time" {
            return Err(CoreError::Invalid(format!("'{field_name}' is managed by META")));
        }
        let latest_name = self
            .discover_partitions()
            .last()
            .cloned()
            .unwrap_or_default();

        let latest_ds = self.dataset_for(&latest_name)?;
        if latest_ds.schema().position(field_name).is_some() {
            return Err(CoreError::AlreadyExists(
                self.root.join(&latest_name).join(field_name.replace('.', "/")),
            ));
        }

        let latest_rows = latest_ds.row_count();
        let part_dir = if self.scheme == PartitionScheme::None {
            self.root.clone()
        } else {
            self.root.join(&latest_name)
        };
        let field_path = part_dir.join(field_name.replace('.', "/"));
        if let Some(parent) = field_path.parent() {
            let _ = fs::create_dir_all(parent);
        }

        let field_opts = opts.unwrap_or_else(|| {
            let compression = self.options.compression.unwrap_or(splayed_format::Compression::None);
            let chunk_target_rows = self.options.chunk_target_rows.unwrap_or(8192);
            field_options(compression, chunk_target_rows, latest_ds)
        });

        splayed_core::init_field_empty(
            &field_path,
            data_type,
            latest_rows,
            Some(field_opts),
        )?;

        // 剔除最新分区缓存，以便下次访问重新载入新 Schema
        self.datasets.borrow_mut().remove(&latest_name);
        Ok(())
    }

    /// 删除所有包含该字段的 Partition 中的同名字段（直接删物理文件，不打开 Field）。
    pub fn delete_field(&self, field: &str) -> Result<(), CoreError> {
        if field == "sym" || field == "time" {
            return Err(CoreError::Invalid(format!("'{field}' is managed by META")));
        }
        let partitions = self.discover_partitions();
        let mut found = false;
        for p in &partitions {
            if self.dataset_for(p)?.schema().position(field).is_some() {
                found = true;
            }
        }
        if !found {
            return Err(CoreError::NotFound(self.root.join(field.replace('.', "/"))));
        }
        self.structural_for_each(|ds| {
            if ds.schema().position(field).is_some() {
                ds.delete_field(field)?;
            }
            Ok(())
        })
    }

    #[inline]
    pub fn delete_table_field(&self, field: &str) -> Result<(), CoreError> {
        self.delete_field(field)
    }

    /// 更新所有包含该字段的 Partition 中该字段的 header 物理属性。
    pub fn update_field(
        &self,
        field: &str,
        header: splayed_format::FieldHeader,
    ) -> Result<(), CoreError> {
        let partitions = self.discover_partitions();
        let mut found = false;
        for p in &partitions {
            if self.dataset_for(p)?.schema().position(field).is_some() {
                found = true;
            }
        }
        if !found {
            return Err(CoreError::NotFound(self.root.join(field.replace('.', "/"))));
        }
        self.structural_for_each(|ds| {
            if ds.schema().position(field).is_some() {
                ds.update_field(field, &header)?;
            }
            Ok(())
        })
    }

    #[inline]
    pub fn update_table_field(
        &self,
        field: &str,
        header: splayed_format::FieldHeader,
    ) -> Result<(), CoreError> {
        self.update_field(field, header)
    }

    /// 重命名所有包含该字段的 Partition 中的同名字段。
    pub fn rename_field(&self, field: &str, new_name: &str) -> Result<(), CoreError> {
        let partitions = self.discover_partitions();
        let mut found = false;
        for p in &partitions {
            let schema = self.dataset_for(p)?.schema();
            if schema.position(field).is_some() {
                found = true;
            }
            if schema.position(new_name).is_some() {
                return Err(CoreError::AlreadyExists(
                    self.root.join(p).join(new_name.replace('.', "/")),
                ));
            }
        }
        if !found {
            return Err(CoreError::NotFound(self.root.join(field.replace('.', "/"))));
        }
        self.structural_for_each(|ds| {
            if ds.schema().position(field).is_some() {
                ds.rename_field(field, new_name)?;
            }
            Ok(())
        })
    }

    #[inline]
    pub fn rename_table_field(&self, field: &str, new_name: &str) -> Result<(), CoreError> {
        self.rename_field(field, new_name)
    }

    /// 转换所有包含该字段的 Partition 中该字段的类型。
    pub fn cast_field(&self, field: &str, target_type: DataType) -> Result<(), CoreError> {
        let partitions = self.discover_partitions();
        let mut cur_type: Option<DataType> = None;
        let mut found = false;
        for p in &partitions {
            let schema = self.dataset_for(p)?.schema();
            if let Some(pos) = schema.position(field) {
                found = true;
                let dt = schema.fields[pos].data_type;
                match cur_type {
                    None => cur_type = Some(dt),
                    Some(prev) if prev != dt => {
                        return Err(CoreError::Invalid(format!(
                            "field '{field}' has inconsistent types across partitions ({prev:?} vs {dt:?})"
                        )));
                    }
                    _ => {}
                }
            }
        }
        if !found {
            return Err(CoreError::NotFound(self.root.join(field.replace('.', "/"))));
        }
        self.structural_for_each(|ds| {
            if ds.schema().position(field).is_some() {
                ds.cast_field(field, target_type)?;
            }
            Ok(())
        })
    }

    #[inline]
    pub fn cast_table_field(&self, field: &str, target_type: DataType) -> Result<(), CoreError> {
        self.cast_field(field, target_type)
    }

    /// 压缩所有包含该字段的 Partition 中的该字段。
    pub fn compress_field(&self, field: &str) -> Result<(), CoreError> {
        let partitions = self.discover_partitions();
        let mut found = false;
        for p in &partitions {
            let ds = self.dataset_for(p)?;
            if ds.schema().position(field).is_some() {
                found = true;
                if ds.dataset_field_is_chunked(field)? {
                    return Err(CoreError::Invalid(format!(
                        "field '{field}' in partition '{p}' is already compressed"
                    )));
                }
            }
        }
        if !found {
            return Err(CoreError::NotFound(self.root.join(field.replace('.', "/"))));
        }
        self.structural_for_each(|ds| {
            if ds.schema().position(field).is_some() {
                ds.compress_field(field)?;
            }
            Ok(())
        })
    }

    #[inline]
    pub fn compress_table_field(&self, field: &str) -> Result<(), CoreError> {
        self.compress_field(field)
    }

    /// 解压所有包含该字段的 Partition 中的同名字段。
    pub fn decompress_field(&self, field: &str) -> Result<(), CoreError> {
        let partitions = self.discover_partitions();
        let mut found = false;
        for p in &partitions {
            let ds = self.dataset_for(p)?;
            if ds.schema().position(field).is_some() {
                found = true;
                if !ds.dataset_field_is_chunked(field)? {
                    return Err(CoreError::Invalid(format!(
                        "field '{field}' in partition '{p}' is not compressed"
                    )));
                }
            }
        }
        if !found {
            return Err(CoreError::NotFound(self.root.join(field.replace('.', "/"))));
        }
        self.structural_for_each(|ds| {
            if ds.schema().position(field).is_some() {
                ds.decompress_field(field)?;
            }
            Ok(())
        })
    }

    #[inline]
    pub fn decompress_table_field(&self, field: &str) -> Result<(), CoreError> {
        self.decompress_field(field)
    }

    /// 删除指定 Partition（释放句柄并删除物理分区目录）。
    pub fn delete_partition(&self, partition_name: &str) -> Result<(), CoreError> {
        delete_table_partition(&self.root, partition_name)?;
        self.datasets.borrow_mut().remove(partition_name);
        self.stats_cache.borrow_mut().remove(partition_name);
        Ok(())
    }

    /// 删除指定 Partition（方法别名，保持与 delete_table_partition 契约一致）。
    #[inline]
    pub fn delete_table(&self, partition_name: &str) -> Result<(), CoreError> {
        self.delete_partition(partition_name)
    }

    /// 流式创建并注册新 Partition（基于 &DataView 零拷贝视图）。
    pub fn create_partition(
        &self,
        partition_name: &str,
        data: &DataView<'_>,
        options: Option<CreateDatasetOptions>,
    ) -> Result<(), CoreError> {
        let opts = options.unwrap_or_else(|| CreateDatasetOptions {
            max_parallelism: self.max_parallelism(),
            compression: self.options.compression.unwrap_or(splayed_format::Compression::None),
            chunk_target_rows: self.options.chunk_target_rows.unwrap_or(8192),
        });
        create_table_partition_from_view(&self.root, partition_name, data, opts)?;
        self.dataset_for(partition_name)?;
        self.stats_cache.borrow_mut().remove(partition_name);
        Ok(())
    }

    /// 检查并修复表物理存储与元数据完整性：
    /// 1. 遍历每个分区，自底向上清理因删除嵌套字段后遗留的空文件夹；
    /// 2. 检查是否有历史/中间分区存在的字段而最新分区缺少，若存在则在最新分区中通过 `init_field_empty` 以当前最新分区的逻辑行数补齐（全 NULL 延迟展开骨架），并刷新元数据。
    pub fn fix(&self) -> Result<(), CoreError> {
        self.mode.require_write("fix")?;

        let partitions = self.discover_partitions();

        // 1. 每个分区检查并清理空文件夹
        for p in &partitions {
            let part_dir = if self.scheme == PartitionScheme::None {
                self.root.clone()
            } else {
                self.root.join(p)
            };
            clean_empty_subdirs_in_partition(&part_dir)?;
        }

        // 2. 检查有没有中间分区有的字段而最新分区没有，存在的话在最新分区中通过 create_field 补齐
        if self.scheme != PartitionScheme::None && partitions.len() >= 2 {
            let latest_name = partitions.last().unwrap().clone();
            let prior_partitions = &partitions[..partitions.len() - 1];

            let latest_schema = self.dataset_for(&latest_name)?.schema();
            let latest_field_set: std::collections::HashSet<String> =
                latest_schema.names().into_iter().map(|s| s.to_string()).collect();

            let mut missing_fields: std::collections::BTreeMap<String, DataType> =
                std::collections::BTreeMap::new();
            for prior in prior_partitions {
                let schema = self.dataset_for(prior)?.schema();
                for f in &schema.fields {
                    if f.name.as_ref() == "sym" || f.name.as_ref() == "time" {
                        continue;
                    }
                    if !latest_field_set.contains(f.name.as_ref()) {
                        if let Some(existing_type) = missing_fields.get(f.name.as_ref()) {
                            if *existing_type != f.data_type {
                                return Err(CoreError::Invalid(format!(
                                    "conflicting data types for field '{}' across partitions: {:?} vs {:?}",
                                    f.name, existing_type, f.data_type
                                )));
                            }
                        } else {
                            missing_fields.insert(f.name.to_string(), f.data_type);
                        }
                    }
                }
            }

            if !missing_fields.is_empty() {
                let latest_rows = self.dataset_for(&latest_name)?.row_count();
                let latest_dir = self.root.join(&latest_name);
                for (field_name, data_type) in &missing_fields {
                    let field_path = latest_dir.join(field_name.replace('.', "/"));
                    if let Some(parent) = field_path.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    splayed_core::init_field_empty(
                        &field_path,
                        *data_type,
                        latest_rows,
                        None,
                    )?;
                }
                // 剔除最新分区的已缓存句柄，让后续访问重新 open_dataset 加载补齐后的 Schema
                self.datasets.borrow_mut().remove(&latest_name);
            }
        }

        Ok(())
    }
}

/// 递归清理单个分区内部因删除字段产生的空子文件夹
fn clean_empty_subdirs_in_partition(part_dir: &Path) -> Result<(), CoreError> {
    if !part_dir.is_dir() {
        return Ok(());
    }
    let Ok(entries) = fs::read_dir(part_dir) else {
        return Ok(());
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name.ends_with(".tmp") || name == ".lock" {
            continue;
        }
        if path.is_dir() {
            if clean_empty_subdirs(&path)? {
                let _ = fs::remove_dir(&path);
            }
        }
    }
    Ok(())
}

fn clean_empty_subdirs(dir: &Path) -> Result<bool, CoreError> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(false);
    };
    let mut is_empty = true;
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name.ends_with(".tmp") || name == ".lock" {
            is_empty = false;
            continue;
        }
        if path.is_dir() {
            let child_empty = clean_empty_subdirs(&path)?;
            if child_empty {
                let _ = fs::remove_dir(&path);
            } else {
                is_empty = false;
            }
        } else {
            is_empty = false;
        }
    }
    Ok(is_empty)
}
