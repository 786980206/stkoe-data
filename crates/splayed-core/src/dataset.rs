use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use splayed_format::{
    Column, ColumnValues, ColumnView, Data, DataView, DataType, FieldSchema, Schema, TimeType,
};

use crate::error::{map_io_path, CoreError, Mode};
use crate::field_file::{
    close_field_handle, create_field_file, delete_field_file, open_field_file, rename_field_file,
    cast_field_file, compress_field_file, decompress_field_file, FieldChunkReader, FieldHandle,
    FieldInit, StreamValues,
};
use crate::meta_file::{create_meta_file, MetaBuilder, MetaHandle};
use crate::scan::{clamp_ranges, merge_ranges, Predicate, RowRange, ScanRequest};

/// `write_dataset` Field 级并行的总字节门槛：低于此值走串行快路径
/// （线程创建 ~几十 µs，高于小体量 memcpy 的并行收益）。
const WRITE_PARALLEL_MIN_BYTES: usize = 1 << 20;
/// `scan_dataset` Field 级并行的候选总行数门槛：低于此值走串行快路径。
const SCAN_PARALLEL_MIN_ROWS: u64 = 1 << 16;

/// Dataset 逻辑目录布局：
/// ```text
/// dataset/
/// ├── .meta          // Index：sym/time → 逻辑行
/// └── <name>         // 字段数据文件（字段名即文件名，沿用 V1.0 约定）
/// ```
pub const META_FILE_NAME: &str = ".meta";
/// 保留字段名：sym / time 由 META 管理，不作为普通 Field。
pub const RESERVED_FIELD_NAMES: [&str; 2] = ["sym", "time"];

fn is_reserved(name: &str) -> bool {
    RESERVED_FIELD_NAMES.contains(&name)
}

fn validate_field_name(name: &str) -> Result<(), CoreError> {
    if name.is_empty()
        || name.starts_with('.')
        || name.contains(['/', '\\'])
        || is_reserved(name)
    {
        return Err(CoreError::Invalid(format!("invalid field name '{name}'")));
    }
    Ok(())
}

/// `create_dataset_field` 的初始化方式（对齐 core `create_field_file` 的 init 模型）。
pub enum DatasetFieldInit {
    /// 全 NULL（`length(L)`，L 为 Dataset 逻辑长度）。
    AllNull,
    /// 带数据初始化；行数必须恰为 `L`。
    Data(Column),
    /// 流式初始化；累计行数必须恰为 `L`。
    Stream { reader: Box<dyn FieldChunkReader> },
}

/// 校验流式总长度恰为 L 的包装 reader。
struct LengthCheckReader {
    inner: Box<dyn FieldChunkReader>,
    expected: u64,
    got_values: u64,
    got_validity: u64,
}

impl FieldChunkReader for LengthCheckReader {
    fn next_values(&mut self) -> Result<Option<StreamValues>, CoreError> {
        match self.inner.next_values()? {
            Some(v) => {
                self.got_values += v.rows as u64;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }

    fn next_validity(&mut self) -> Result<Option<Vec<u8>>, CoreError> {
        match self.inner.next_validity()? {
            Some(bits) => {
                self.got_validity += bits.len() as u64 * 8;
                Ok(Some(bits))
            }
            None => {
                if self.got_values != self.expected {
                    return Err(CoreError::Invalid(format!(
                        "stream produced {} rows, expected {}",
                        self.got_values, self.expected
                    )));
                }
                Ok(None)
            }
        }
    }
}

/// Dataset 的打开态对象：常驻 META + 按需打开的 Field Handle 缓存。
pub struct DatasetHandle {
    root: PathBuf,
    meta: MetaHandle,
    /// [sym Utf8, time] + 各 Field（按名称排序）
    schema: Schema,
    mode: Mode,
    /// Field Handle 缓存（Box 稳定地址；只增不删，构造后仅共享访问）。
    fields: RefCell<HashMap<String, Box<FieldHandle>>>,
    /// Field 级并行上限（write_dataset / scan_dataset 的并行度旋钮，由最上层控制；
    /// 1 = 串行。Dataset 层不自建线程池，只用 `std::thread::scope` 按
    /// min(max_parallelism, 任务数) 分桶）。
    max_parallelism: usize,
}

impl DatasetHandle {
    pub fn path(&self) -> &Path {
        &self.root
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// 设置 Field 级并行上限（影响 write_dataset / scan_dataset；1 = 串行）。
    pub fn set_max_parallelism(&mut self, max_parallelism: usize) {
        self.max_parallelism = max_parallelism.max(1);
    }

    fn field_path(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    fn logical_length(&self) -> u64 {
        self.meta.header().row_count as u64
    }

    /// Dataset Schema（sym / time + 各 Field）。
    pub fn read_dataset_schema(&self) -> Schema {
        self.schema.clone()
    }

    /// META 时间类型（Table 层构造分区边界用）。
    pub fn peek_time_type(&self) -> TimeType {
        self.meta.time_type()
    }


    fn ensure_field(
        fields: &RefCell<HashMap<String, Box<FieldHandle>>>,
        root: &Path,
        mode: Mode,
        name: &str,
    ) -> Result<(), CoreError> {
        if !fields.borrow().contains_key(name) {
            let path = root.join(name);
            let handle = Box::new(open_field_file(&path, mode)?);
            fields.borrow_mut().insert(name.to_string(), handle);
        }
        Ok(())
    }

    fn field_handle<'a>(
        fields: &'a RefCell<HashMap<String, Box<FieldHandle>>>,
        name: &str,
    ) -> Result<&'a FieldHandle, CoreError> {
        let borrow = fields.borrow();
        let ptr: *const FieldHandle = match borrow.get(name) {
            Some(b) => &**b as *const FieldHandle,
            None => return Err(CoreError::NotFound(PathBuf::from(name))),
        };
        // 安全性：Box 目标地址稳定；条目只增不删；push 后不再有 &mut 访问（写经 RefMut 就地完成）
        Ok(unsafe { &*ptr })
    }

    /// 读取 Dataset 级统计（来自 META，不扫描 Field 数据；`row_count` = 容量网格逻辑行数）。
    pub fn read_dataset_statistics(&self) -> Result<DatasetStatistics, CoreError> {
        let header = self.meta.header();
        let sym_count = header.sym_count;
        let sym_min =
            (sym_count > 0).then(|| self.meta.sym_str(0)).transpose()?.map(str::to_owned);
        let sym_max = (sym_count > 0)
            .then(|| self.meta.sym_str(sym_count - 1))
            .transpose()?
            .map(str::to_owned);
        let time_min =
            (header.time_count > 0).then(|| self.meta.time_at(0)).transpose()?.unwrap_or(0) as i64;
        let time_max = (header.time_count > 0)
            .then(|| self.meta.time_at(header.time_count - 1))
            .transpose()?
            .unwrap_or(0) as i64;
        Ok(DatasetStatistics {
            row_count: header.row_count as u64,
            sym_count,
            sym_min,
            sym_max,
            time_count: header.time_count,
            time_min,
            time_max,
        })
    }

    /// 按逻辑行范围读取多列。**默认只返回 sym 与 time**；其余 Field 由 `columns`
    /// 指定（不需要重复指定 sym / time），输出按 projection **请求序**组装。
    ///
    /// 并发模型（单线程）：META 零拷贝 → 串行 ensure_field → 逐 Field 零拷贝切片
    /// → 请求序组装。读路径全程 O(1) 指针运算、不触碰数据页（uncompressed =
    /// mmap 切片；compressed = working 切片），Field 级并行的调度开销远高于
    /// 切片收益——并行的插入点在 chunk 惰性解码落地之后（读变成解码密集）。
    pub fn read_dataset(
        &self,
        offset: u64,
        length: u64,
        columns: Option<&[&str]>,
    ) -> Result<DataView<'_>, CoreError> {
        let l = self.logical_length();
        if offset.checked_add(length).map_or(true, |end| end > l) {
            return Err(CoreError::Invalid(format!(
                "read range [{offset}, {}) exceeds dataset length {l}",
                offset.saturating_add(length)
            )));
        }
        // ① 主线程：解析 projection（请求序去重，跳过 sym/time 保留名）+ 串行 ensure_field
        let mut requested: Vec<String> = Vec::new();
        if let Some(cols) = columns {
            for c in cols {
                if is_reserved(c) {
                    continue;
                }
                if self.schema.position(c).is_none() {
                    return Err(CoreError::Invalid(format!("unknown field '{c}'")));
                }
                if !requested.iter().any(|r| r == c) {
                    requested.push(c.to_string());
                }
            }
        }
        for name in &requested {
            DatasetHandle::ensure_field(&self.fields, &self.root, self.mode, name)?;
        }
        // ② META 零拷贝：sym RepeatDict 段（零物化）+ time 轴切片
        let base = self.meta.read_index_handle(offset, length)?;
        let mut columns_out: Vec<splayed_format::ColumnView<'_>> =
            Vec::with_capacity(2 + requested.len());
        columns_out.extend(base.columns);
        // ③ 逐 Field 零拷贝读取（串行切片），按请求序追加
        let mut schema_fields: Vec<FieldSchema> = base.schema.fields.to_vec();
        for name in &requested {
            let handle = DatasetHandle::field_handle(&self.fields, name)?;
            columns_out.push(handle.read_field_handle(offset, length)?);
            schema_fields.push(FieldSchema::new(name.as_str(), handle.data_type()));
        }
        DataView::new(Schema::new(schema_fields), columns_out).map_err(CoreError::from)
    }

    /// 对已有逻辑行做 positional overwrite（projection write；不保证跨 Field 原子性）。
    ///
    /// 并发模型（三阶段）：① 主线程校验 + 串行 ensure_field（并行区域不触碰字段
    /// 缓存）；② 收集互不相交的 `&mut FieldHandle`；③ Field 级并行写
    /// （`std::thread::scope` round-robin 分桶，各 Field 完全独立）。单字段或
    /// 总字节 < `WRITE_PARALLEL_MIN_BYTES` 走串行快路径。
    pub fn write_dataset(&self, offset: u64, data: &DataView<'_>) -> Result<(), CoreError> {
        // ① 主线程：mode / 边界 / 列名校验 / 类型校验 + 串行 ensure_field
        self.mode.require_write("write_dataset")?;
        let l = self.logical_length();
        if offset
            .checked_add(data.length() as u64)
            .map_or(true, |end| end > l)
        {
            return Err(CoreError::Invalid("write range exceeds dataset length".into()));
        }
        if data.length() == 0 {
            return Ok(()); // length == 0 是合法 no-op
        }
        for field in &data.schema.fields {
            if is_reserved(&field.name) {
                return Err(CoreError::Invalid(
                    "write_dataset must not contain sym/time columns".into(),
                ));
            }
            let schema_type = self
                .schema
                .data_type_of(&field.name)
                .ok_or_else(|| CoreError::Invalid(format!("unknown field '{}'", field.name)))?;
            if schema_type != field.data_type {
                return Err(CoreError::Invalid(format!(
                    "field '{}' type {:?} does not match dataset schema {schema_type:?}",
                    field.name, field.data_type
                )));
            }
        }
        for field in &data.schema.fields {
            DatasetHandle::ensure_field(&self.fields, &self.root, self.mode, &field.name)?;
        }
        // ② 收集不相交 &mut FieldHandle：values_mut 一次性独占借用整个缓存，
        //    各值天然互不相交（无需 unsafe / 多次 get_mut）；字段名（= 文件名）
        //    配对到 data.columns 下标。RefMut 只在主线程存活，并行任务不触碰缓存。
        {
            let mut seen = std::collections::HashSet::new();
            for field in &data.schema.fields {
                if !seen.insert(field.name.as_ref()) {
                    return Err(CoreError::Invalid(format!(
                        "duplicate field '{}' in write_dataset",
                        field.name
                    )));
                }
            }
        }
        let mut guard = self.fields.borrow_mut();
        let mut targets: Vec<(usize, &mut FieldHandle)> = Vec::with_capacity(data.columns.len());
        for handle in guard.values_mut() {
            let name = handle
                .path()
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            if let Some(i) = data.schema.position(&name) {
                targets.push((i, handle));
            }
        }
        // ③ 写入：单字段 / 小写 / 并行度 1 → 串行快路径；否则 Field 级并行
        let total: usize = data.columns.iter().map(view_bytes).sum();
        let p = self.max_parallelism;
        if targets.len() <= 1 || total < WRITE_PARALLEL_MIN_BYTES || p <= 1 {
            for (i, handle) in targets {
                handle.write_field_handle(offset, &data.columns[i])?;
            }
            return Ok(());
        }
        let p = p.min(targets.len());
        // round-robin 分桶（字段大小不均时负载更均匀）；桶内顺序、桶间并行。
        // 不用 vec![Vec::new(); p]（&mut 不满足 Clone），逐个构造
        let mut buckets: Vec<Vec<(usize, &mut FieldHandle)>> =
            (0..p).map(|_| Vec::new()).collect();
        for (j, target) in targets.into_iter().enumerate() {
            buckets[j % p].push(target);
        }
        std::thread::scope(|s| {
            let mut joins = Vec::new();
            for bucket in buckets {
                joins.push(s.spawn(move || -> Result<(), CoreError> {
                    for (i, handle) in bucket {
                        handle.write_field_handle(offset, &data.columns[i])?;
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
                            "field writer thread panicked".into(),
                        ));
                    }
                }
            }
            first_err.map_or(Ok(()), Err)
        })
    }

    /// 条件扫描：组合 META 与各 Field 的扫描结果，输出 Dataset 逻辑 RowRange。
    ///
    /// 并发模型（三阶段）：① 主线程 clamp + META（sym/time）先行（单线程，体积小
    /// 且能大幅缩小候选）+ 串行 ensure_field；② 各 Field 以**相同候选范围**独立
    /// 扫描（`std::thread::scope` round-robin 分桶；并行区域不触碰字段缓存）；
    /// ③ 主线程顺序求交（空集提前退出）+ 相邻合并。多字段并行扫描与逐字段串行
    /// 收窄结果一致（谓词逐行求值，∩ 满足交换/结合），并行只改变求值次序。
    /// 跨字段的 OR / NOT 组合不支持（返回 Invalid），行级过滤兜底由上层完成。
    pub fn scan_dataset(&self, request: &ScanRequest) -> Result<DatasetScanner, CoreError> {
        // ① 主线程：裁剪 + META sym/time 先行
        let l = self.logical_length();
        let mut current = clamp_ranges(&request.ranges, l);
        let mut groups: Vec<(String, Predicate)> = Vec::new();
        if let Some(pred) = &request.predicate {
            let sym_time = collect_for_fields(pred, &["sym", "time"]);
            if !matches!(&sym_time, Predicate::And(children) if children.is_empty()) {
                let index_req = ScanRequest {
                    ranges: current.clone(),
                    projection: vec![],
                    predicate: Some(sym_time),
                    limit: None,
                };
                let mut scanner = self.meta.scan_index_handle(&index_req)?;
                let mut narrowed = Vec::new();
                while let Some(r) = scanner.next()? {
                    narrowed.push(r);
                }
                scanner.close()?;
                current = intersect_range_lists(&current, &narrowed);
            }
            groups = predicate_groups(pred)?;
            groups.sort_by(|a, b| a.0.cmp(&b.0));
        }
        // ② 串行 ensure_field 全部目标字段，收集共享 handle（并行阶段不触碰缓存）
        let handles: Vec<&FieldHandle> = groups
            .iter()
            .map(|(name, _)| {
                DatasetHandle::ensure_field(&self.fields, &self.root, self.mode, name)?;
                DatasetHandle::field_handle(&self.fields, name)
            })
            .collect::<Result<_, CoreError>>()?;
        // ③ 各 Field 独立扫描（相同输入 ranges）；多字段且候选足够大才并行。
        //    limit 不下推子扫描：单字段截断后求交会漏行，最终由 Scanner.remaining 严格控制
        let results: Vec<Vec<RowRange>> =
            if groups.is_empty() || current.is_empty() {
                Vec::new()
            } else {
                let total_rows: u64 = current.iter().map(|r| r.length).sum();
                let p = self.max_parallelism;
                if groups.len() > 1 && total_rows >= SCAN_PARALLEL_MIN_ROWS && p > 1 {
                    let p = p.min(groups.len());
                    // 共享引用先行绑定：move 闭包只捕获 &Vec，不移动本体
                    let (groups_ref, handles_ref, current_ref) = (&groups, &handles, &current);
                    let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); p];
                    for (gi, _) in groups.iter().enumerate() {
                        buckets[gi % p].push(gi);
                    }
                    std::thread::scope(|s| -> Result<Vec<Vec<RowRange>>, CoreError> {
                        let mut joins = Vec::new();
                        for bucket in buckets {
                            joins.push(s.spawn(
                                move || -> Result<Vec<(usize, Vec<RowRange>)>, CoreError> {
                                    let mut out = Vec::new();
                                    for gi in bucket {
                                        let sub = groups_ref[gi].1.clone();
                                        let req = ScanRequest {
                                            ranges: current_ref.clone(),
                                            projection: vec![],
                                            predicate: Some(sub),
                                            limit: None,
                                        };
                                        let mut sc = handles_ref[gi].scan_field_handle(&req)?;
                                        let mut hit = Vec::new();
                                        while let Some(r) = sc.next()? {
                                            hit.push(r);
                                        }
                                        sc.close()?;
                                        out.push((gi, hit));
                                    }
                                    Ok(out)
                                },
                            ));
                        }
                        let mut results: Vec<Vec<RowRange>> = vec![Vec::new(); groups.len()];
                        for j in joins {
                            let done = j.join().map_err(|_| {
                                CoreError::InvalidState("field scanner thread panicked".into())
                            })??;
                            for (gi, hit) in done {
                                results[gi] = hit;
                            }
                        }
                        Ok(results)
                    })?
                } else {
                    // 单字段 / 小候选串行快路径
                    let mut results = Vec::with_capacity(groups.len());
                    for (gi, (_, sub)) in groups.iter().enumerate() {
                        let req = ScanRequest {
                            ranges: current.clone(),
                            projection: vec![],
                            predicate: Some(sub.clone()),
                            limit: None,
                        };
                        let mut sc = handles[gi].scan_field_handle(&req)?;
                        let mut hit = Vec::new();
                        while let Some(r) = sc.next()? {
                            hit.push(r);
                        }
                        sc.close()?;
                        results.push(hit);
                    }
                    results
                }
            };
        // ④ 主线程收尾：顺序求交（空集提前退出）+ 相邻合并
        let mut final_ranges = current;
        for r in &results {
            if final_ranges.is_empty() {
                break;
            }
            final_ranges = intersect_range_lists(&final_ranges, r);
        }
        final_ranges = merge_ranges(final_ranges);
        Ok(DatasetScanner { ranges: VecDeque::from(final_ranges), remaining: request.limit })
    }

    /// 在已有 Dataset 中新增一个 Field。
    pub fn create_dataset_field(
        &mut self,
        name: &str,
        data_type: DataType,
        init: DatasetFieldInit,
    ) -> Result<(), CoreError> {
        validate_field_name(name)?;
        if self.schema.position(name).is_some() {
            return Err(CoreError::AlreadyExists(self.field_path(name)));
        }
        let l = self.logical_length();
        let init = match init {
            DatasetFieldInit::AllNull => FieldInit::Length(l),
            DatasetFieldInit::Data(col) => {
                if col.data_type != data_type {
                    return Err(CoreError::Invalid(format!(
                        "field data type {:?} does not match requested {data_type:?}",
                        col.data_type
                    )));
                }
                if col.length() as u64 != l {
                    return Err(CoreError::Invalid(format!(
                        "field data length {} must equal dataset logical length {l}",
                        col.length()
                    )));
                }
                FieldInit::Data(col)
            }
            DatasetFieldInit::Stream { reader } => FieldInit::Stream {
                reader: Box::new(LengthCheckReader { inner: reader, expected: l, got_values: 0, got_validity: 0 }),
            },
        };
        if let Err(e) = create_field_file(&self.field_path(name), data_type, init) {
            // 失败清理半成品：create 直接写最终路径，残留文件带零填充占位 header，
            // 会污染后续 build_schema / open_dataset（Schema 尚未同步，文件必须不落痕）
            let _ = fs::remove_file(self.field_path(name));
            return Err(e);
        }
        self.reload_schema();
        Ok(())
    }

    /// 删除指定 Field（META 与 sym / time 不受影响）。
    pub fn delete_dataset_field(&mut self, name: &str) -> Result<(), CoreError> {
        validate_field_name(name)?;
        if self.schema.position(name).is_none() {
            return Err(CoreError::NotFound(self.field_path(name)));
        }
        self.fields.borrow_mut().remove(name);
        delete_field_file(&self.field_path(name))?;
        self.reload_schema();
        Ok(())
    }

    /// 重命名指定 Field（同目录原子 rename；失败时原字段名保持不变）。
    pub fn rename_dataset_field(&mut self, name: &str, new_name: &str) -> Result<(), CoreError> {
        validate_field_name(new_name)?;
        if self.schema.position(name).is_none() {
            return Err(CoreError::NotFound(self.field_path(name)));
        }
        if self.schema.position(new_name).is_some() {
            return Err(CoreError::AlreadyExists(self.field_path(new_name)));
        }
        self.fields.borrow_mut().remove(name);
        rename_field_file(&self.field_path(name), new_name)?;
        self.reload_schema();
        Ok(())
    }

    /// 修改指定 Field 的 header（不改 data；data_type / row_count 由 core 强制为现值）。
    /// 为 Table 层 `update_table_field` 的下沉通道。
    pub fn update_dataset_field_header(
        &self,
        name: &str,
        header: splayed_format::FieldHeader,
    ) -> Result<(), CoreError> {
        if self.schema.position(name).is_none() {
            return Err(CoreError::NotFound(self.field_path(name)));
        }
        DatasetHandle::ensure_field(&self.fields, &self.root, self.mode, name)?;
        let mut handle = self.fields.borrow_mut();
        handle
            .get_mut(name)
            .expect("just ensured")
            .update_field_handle(header)?;
        Ok(())
    }

    /// 转换指定 Field 的数据类型（临时文件 + 原子替换在 `cast_field_file` 内部完成）。
    pub fn cast_dataset_field(&mut self, name: &str, target_type: DataType) -> Result<(), CoreError> {
        validate_field_name(name)?;
        if self.schema.position(name).is_none() {
            return Err(CoreError::NotFound(self.field_path(name)));
        }
        self.fields.borrow_mut().remove(name);
        cast_field_file(&self.field_path(name), target_type)?;
        self.reload_schema();
        Ok(())
    }

    /// 压缩指定 Field（sym 对齐 chunk 边界由 META 网格生成：k 个连续 sym，超长 sym 按 cap 劈开）。
    pub fn compress_dataset_field(&mut self, name: &str) -> Result<(), CoreError> {
        if self.schema.position(name).is_none() {
            return Err(CoreError::NotFound(self.field_path(name)));
        }
        self.fields.borrow_mut().remove(name);
        let offsets = self.sym_aligned_offsets(8, 64 * 1024);
        compress_field_file(&self.field_path(name), Some(offsets))?;
        Ok(())
    }

    /// 解压指定 Field。
    pub fn decompress_dataset_field(&mut self, name: &str) -> Result<(), CoreError> {
        if self.schema.position(name).is_none() {
            return Err(CoreError::NotFound(self.field_path(name)));
        }
        self.fields.borrow_mut().remove(name);
        decompress_field_file(&self.field_path(name))?;
        Ok(())
    }

    /// `(sym, time)` 联合键批量定位（转发 META，供 Table 层 write_table 使用）。
    pub fn locate_dataset_index(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError> {
        self.meta.locate_index_handle(pairs)
    }

    fn sym_aligned_offsets(&self, k: usize, cap: u32) -> Vec<u64> {
        let mut boundaries: Vec<u64> = vec![0];
        let mut syms_in_chunk = 0usize;
        for id in 0..self.meta.header().sym_count {
            let Ok(rec) = self.meta.sym_record(id) else { break };
            let (start, len) = (rec.row_start as u64, rec.time_count as u64);
            if syms_in_chunk == k {
                boundaries.push(start);
                syms_in_chunk = 0;
            }
            syms_in_chunk += 1;
            if len > cap as u64 {
                boundaries.push(start);
                let mut p = start + cap as u64;
                while p < start + len {
                    boundaries.push(p);
                    p += cap as u64;
                }
                syms_in_chunk = 0;
            }
        }
        boundaries.sort_unstable();
        boundaries.dedup();
        boundaries
    }

    fn reload_schema(&mut self) {
        self.schema = build_schema(&self.root, self.meta.time_type()).unwrap_or_else(|_| {
            Schema::new(vec![
                FieldSchema::new("sym", DataType::Utf8),
                FieldSchema::new("time", self.meta.time_type().data_type()),
            ])
        });
    }

    /// 关闭 Dataset：释放 META Handle 与全部 Field Handle（compressed write 收尾在此触发）。
    pub fn close_dataset(mut self) -> Result<(), CoreError> {
        let fields = self.fields.get_mut();
        for (_, handle) in fields.drain() {
            close_field_handle(*handle)?;
        }
        self.meta.close()
    }
}

/// 列的字节量近似（写并行门槛用）：Fixed 取 values 长度，Dict 取 keys
/// （4B/行），RepeatDict 零存储取 0；validity 为 1/8 量级，忽略。
fn view_bytes(view: &ColumnView<'_>) -> usize {
    view.segments()
        .iter()
        .map(|s| match s.values() {
            ColumnValues::Fixed(v) => v.len(),
            ColumnValues::Dict { keys, .. } => keys.len(),
            ColumnValues::RepeatDict { .. } => 0,
        })
        .sum()
}

/// Dataset 级统计信息（来自 META）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetStatistics {
    pub row_count: u64,
    pub sym_count: u32,
    pub sym_min: Option<String>,
    pub sym_max: Option<String>,
    pub time_count: u32,
    pub time_min: i64,
    pub time_max: i64,
}

/// 从目录内容构建 Schema（[sym, time] + 字段文件按名称排序）。
fn build_schema(root: &Path, time_type: TimeType) -> Result<Schema, CoreError> {
    let mut fields = vec![
        FieldSchema::new("sym", DataType::Utf8),
        FieldSchema::new("time", time_type.data_type()),
    ];
    let mut names: Vec<String> = Vec::new();
    for entry in fs::read_dir(root).map_err(|e| map_io_path(root, e))? {
        let entry = entry.map_err(CoreError::Io)?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name.ends_with(".tmp") {
            continue;
        }
        names.push(name);
    }
    names.sort();
    for name in names {
        let mut f = File::open(root.join(&name)).map_err(|e| map_io_path(root, e))?;
        let mut head = [0u8; 64];
        std::io::Read::read_exact(&mut f, &mut head)?;
        let header = splayed_format::FieldHeader::from_bytes(&head)?;
        fields.push(FieldSchema::new(name.as_str(), header.data_type()?));
    }
    Ok(Schema::new(fields))
}

/// Dataset Scanner：输出综合 META 与参与条件判断的多个 Field 的扫描结果，
/// 一次 `next()` 返回一个连续的 Dataset **逻辑** RowRange（不承担 batch 语义）。
pub struct DatasetScanner {
    ranges: VecDeque<RowRange>,
    remaining: Option<u64>,
}

impl DatasetScanner {
    pub fn next(&mut self) -> Result<Option<RowRange>, CoreError> {
        if let Some(r) = self.ranges.pop_front() {
            if let Some(rem) = &mut self.remaining {
                let take = r.length.min(*rem);
                *rem -= take;
                if *rem == 0 {
                    self.ranges.clear();
                }
                if take < r.length {
                    return Ok(Some(RowRange::new(r.offset, take)));
                }
            }
            Ok(Some(r))
        } else {
            Ok(None)
        }
    }

    pub fn close(self) -> Result<(), CoreError> {
        Ok(())
    }
}

/// 两个有序合并后的 range 列表求交：双指针归并 O(a + b)。
fn intersect_range_lists(a: &[RowRange], b: &[RowRange]) -> Vec<RowRange> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        let lo = a[i].offset.max(b[j].offset);
        let hi = a[i].end().min(b[j].end());
        if hi > lo {
            out.push(RowRange::new(lo, hi - lo));
        }
        // 推进先结束的一侧
        if a[i].end() <= b[j].end() {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

/// 把 Dataset 级谓词按字段分组：(字段名, 该字段的子谓词)。
/// sym / time 条件不分组（META 扫描已处理）；仅支持 Cmp 各自归属单一字段、
/// And 组合；跨字段的 Or / Not 返回 Invalid（行级过滤兜底由上层完成）。
fn predicate_groups(pred: &Predicate) -> Result<Vec<(String, Predicate)>, CoreError> {
    let mut groups: Vec<(String, Predicate)> = Vec::new();
    fn push(groups: &mut Vec<(String, Predicate)>, name: String, sub: Predicate) {
        if let Some(g) = groups.iter_mut().find(|(n, _)| *n == name) {
            let old = std::mem::replace(&mut g.1, Predicate::And(vec![]));
            g.1 = Predicate::And(vec![old, sub]);
        } else {
            groups.push((name, sub));
        }
    }
    fn walk(pred: &Predicate, groups: &mut Vec<(String, Predicate)>) -> Result<(), CoreError> {
        match pred {
            Predicate::And(children) => {
                for c in children {
                    walk(c, groups)?;
                }
                Ok(())
            }
            Predicate::Or(_) | Predicate::Not(_) => {
                let mut fields = Vec::new();
                pred.fields(&mut fields);
                if fields.iter().all(|f| is_reserved(f)) {
                    return Ok(()); // sym/time 组合由 META 扫描处理
                }
                if fields.len() == 1 && !is_reserved(&fields[0]) {
                    let name = fields[0].to_string();
                    push(groups, name, strip_all_fields(pred));
                    Ok(())
                } else {
                    Err(CoreError::Invalid(
                        "predicate crosses fields within Or/Not; not supported by dataset scan"
                            .into(),
                    ))
                }
            }
            Predicate::Cmp { field: Some(name), .. } => {
                if is_reserved(name) {
                    return Ok(()); // sym/time 由 META 扫描处理
                }
                validate_field_name(name)?;
                push(groups, name.to_string(), strip_all_fields(pred));
                Ok(())
            }
            Predicate::Cmp { field: None, .. } => Ok(()),
        }
    }
    walk(pred, &mut groups)?;
    Ok(groups)
}

/// 抽取谓词中属于 `allowed` 字段的比较节点，组合为 And（无则空 And）。
fn collect_for_fields(pred: &Predicate, allowed: &[&str]) -> Predicate {
    fn collect(pred: &Predicate, allowed: &[&str], out: &mut Vec<Predicate>) {
        match pred {
            Predicate::And(children) => {
                for c in children {
                    collect(c, allowed, out);
                }
            }
            Predicate::Cmp { field: Some(f), .. }
                if allowed.iter().any(|a| f.as_ref() == *a) =>
            {
                out.push(pred.clone());
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    collect(pred, allowed, &mut out);
    Predicate::And(out)
}

/// 把谓词中所有 Cmp 的字段名剥掉（下沉到单字段扫描时字段即自身）。
fn strip_all_fields(pred: &Predicate) -> Predicate {
    match pred {
        Predicate::And(children) => {
            Predicate::And(children.iter().map(strip_all_fields).collect())
        }
        Predicate::Or(children) => Predicate::Or(children.iter().map(strip_all_fields).collect()),
        Predicate::Not(inner) => Predicate::Not(Box::new(strip_all_fields(inner))),
        Predicate::Cmp { field: _, op, value } => Predicate::Cmp { field: None, op: *op, value: value.clone() },
    }
}

// ------------------------------------------------------------------ File API

/// `create_dataset` 的并行选项。
#[derive(Debug, Clone)]
pub struct CreateDatasetOptions {
    /// Field 文件并行创建的线程上限（1 = 串行）；默认 = 逻辑核数。
    pub max_parallelism: usize,
}

impl Default for CreateDatasetOptions {
    fn default() -> Self {
        CreateDatasetOptions {
            max_parallelism: std::thread::available_parallelism().map_or(1, |n| n.get()),
        }
    }
}

/// 创建完整 Dataset：META 单线程先行（一次扫描完成全局合法性校验），
/// Field 文件并行创建（各字段完全独立，列所有权零拷贝移动），
/// 临时目录中全部成功后原子 rename 到目标路径。
pub fn create_dataset(
    path: &Path,
    data: Data,
    options: CreateDatasetOptions,
) -> Result<(), CoreError> {
    if path.exists() {
        return Err(CoreError::AlreadyExists(path.to_path_buf()));
    }
    if data.column("sym").is_none() || data.column("time").is_none() {
        return Err(CoreError::Invalid("dataset input requires sym and time columns".into()));
    }
    // ① META 单线程先行：一次扫描完成全局校验（排序 / 连续子区间 / 容量网格）+ 构建；
    //    失败时不落任何盘上痕迹
    let meta = MetaBuilder::build(&data.as_view())?;
    // ② 抽走非 sym/time 列（owned 移动，零拷贝）——Field 之间完全独立，是并行创建的基本单元
    let mut data = data;
    let names: Vec<String> = data.schema.fields.iter().map(|f| f.name.to_string()).collect();
    let types: Vec<DataType> = data.schema.fields.iter().map(|f| f.data_type).collect();
    let columns = std::mem::take(&mut data.columns);
    let field_cols: Vec<(String, DataType, Column)> = names
        .into_iter()
        .zip(types)
        .zip(columns)
        .filter(|((name, _), _)| !is_reserved(name))
        .map(|((name, data_type), col)| (name, data_type, col))
        .collect();

    // ③ 临时目录保证最终原子发布
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .ok_or_else(|| CoreError::Invalid("invalid dataset path".into()))?;
    let tmp = parent.join(format!(".{name}.tmp"));
    fs::create_dir_all(&tmp).map_err(|e| map_io_path(&tmp, e))?;

    let result = (|| {
        // ④ META 字节直写（已构建，不二次扫描）+ fsync
        {
            let mut f = File::options()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp.join(META_FILE_NAME))?;
            f.write_all(&meta)?;
            f.sync_all()?;
        }
        // ⑤ Field 并行创建：桶内顺序、桶间并行；P = min(max_parallelism, 字段数)
        let p = options.max_parallelism.max(1).min(field_cols.len()).max(1);
        if p <= 1 {
            for (name, data_type, col) in field_cols {
                create_field_file(&tmp.join(&name), data_type, FieldInit::Data(col))?;
            }
        } else {
            // round-robin 分桶（列大小不均时负载更均匀）；列所有权移动，零拷贝
            let mut buckets: Vec<Vec<(String, DataType, Column)>> = vec![Vec::new(); p];
            for (i, item) in field_cols.into_iter().enumerate() {
                buckets[i % p].push(item);
            }
            std::thread::scope(|s| -> Result<(), CoreError> {
                let mut handles = Vec::new();
                for bucket in buckets {
                    let tmp = &tmp;
                    handles.push(s.spawn(move || -> Result<(), CoreError> {
                        for (name, data_type, col) in bucket {
                            create_field_file(&tmp.join(&name), data_type, FieldInit::Data(col))?;
                        }
                        Ok(())
                    }));
                }
                for h in handles {
                    h.join()
                        .map_err(|_| CoreError::InvalidState("field writer thread panicked".into()))??;
                }
                Ok(())
            })?;
        }
        Ok(())
    })();
    if let Err(e) = result {
        let _ = fs::remove_dir_all(&tmp);
        return Err(e);
    }
    fs::rename(&tmp, path).map_err(|e| map_io_path(path, e))?;
    Ok(())
}

/// 创建 / 重建 Dataset 的 Index（即 `.meta`）；只创建 META，不创建 Field。
pub fn create_dataset_index(path: &Path, data: &DataView<'_>) -> Result<(), CoreError> {
    create_meta_file(&path.join(META_FILE_NAME), data)
}

/// 打开已有 Dataset（只打开 META；Field Handle 按需打开）。
pub fn open_dataset(path: &Path, mode: Mode) -> Result<DatasetHandle, CoreError> {
    if !path.is_dir() {
        return Err(CoreError::NotFound(path.to_path_buf()));
    }
    let meta = MetaHandle::open(&path.join(META_FILE_NAME))?;
    let schema = build_schema(path, meta.time_type())?;
    Ok(DatasetHandle {
        root: path.to_path_buf(),
        meta,
        schema,
        mode,
        fields: RefCell::new(HashMap::new()),
        max_parallelism: std::thread::available_parallelism().map_or(1, |n| n.get()),
    })
}

/// 删除完整 Dataset 根目录（META + 全部 Field）。
pub fn delete_dataset(path: &Path) -> Result<(), CoreError> {
    fs::remove_dir_all(path).map_err(|e| map_io_path(path, e))
}
