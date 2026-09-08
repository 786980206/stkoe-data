use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use splayed_format::{
    BufferView, ColumnSegment, ColumnView, DataView, FieldSchema, MetaHeader, Schema,
    SymIndexRecord, TimeType, DataType, DATA_OFFSET, HEADER_SIZE, META_HEADER_SIZE,
};

use crate::error::{map_io_path, CoreError};
use crate::scan::{
    clamp_ranges, merge_ranges, CmpOp, Predicate, RowRange, ScanRequest, Scalar,
};

// ------------------------------------------------------------------ MetaBuilder

/// 从 `(sym, time)` 两列逻辑数据构建 META 文件字节。
///
/// 布局：`HEADER 64 | TIME AXIS | SYM DICT INDEX (n+1)×u64 | SYM STRING DATA | SYM INDEX n×12`。
pub struct MetaBuilder;

impl MetaBuilder {
    /// `data` 必须包含 `sym`（Utf8 字典视图）与 `time`（整数列），按 `(sym ASC, time ASC)` 排序。
    ///
    /// 核心原则：每个 symbol 的 time 必须严格等于 TIME AXIS 的一个连续子区间，
    /// SYM INDEX 只保存 `row_start + time_start + time_count`，不保存任何 symbol 内的 time 信息。
    /// 单遍扫描收集 run 边界与全部 time 值 → 轴排序去重 → 每 run 二分定位 first/last
    /// 并校验 span == run 行数（排序破坏 / sym 内重复 / sym 内跳空 → `NonContiguousTime`）；
    /// 无 HashMap、无逐行字符串操作（字符串解析仅在 run 边界，O(sym 段数)）。
    pub fn build(data: &DataView<'_>) -> Result<Vec<u8>, CoreError> {
        let sym = data
            .column("sym")
            .ok_or_else(|| CoreError::Invalid("meta input requires a 'sym' column".into()))?;
        let time = data
            .column("time")
            .ok_or_else(|| CoreError::Invalid("meta input requires a 'time' column".into()))?;
        if sym.data_type() != DataType::Utf8 {
            return Err(CoreError::Invalid("meta 'sym' column must be Utf8".into()));
        }
        let time_type = infer_time_type(time.data_type())?;
        let ts = time_type.size_of();

        let rows = data.length();
        if sym.length() != rows || time.length() != rows {
            return Err(CoreError::Invalid("sym/time column length mismatch".into()));
        }
        if rows == 0 {
            return Err(CoreError::Invalid("meta input must not be empty".into()));
        }

        // 批量 cast：sym keys → &[u32]，time → &[u64]（零逐行函数调用）
        let mut keys_flat: Vec<u32> = Vec::with_capacity(rows);
        for seg in sym.segments() {
            let keys: &[u32] = match seg.values() {
                splayed_format::ColumnValues::Dict { keys, .. } => {
                    bytemuck::cast_slice(keys.as_slice())
                }
                _ => {
                    return Err(CoreError::Invalid(
                        "sym column must be dictionary-encoded".into(),
                    ))
                }
            };
            keys_flat.extend_from_slice(keys);
        }
        let mut time_flat: Vec<u64> = Vec::with_capacity(rows);
        for seg in time.segments() {
            let bytes = seg
                .fixed_bytes()
                .ok_or_else(|| CoreError::Invalid("time column must be fixed-width".into()))?;
            match time_type {
                TimeType::Date32 => {
                    let typed: &[i32] = bytemuck::cast_slice(bytes);
                    time_flat.extend(typed.iter().map(|&v| v as u64));
                }
                TimeType::TimestampUs => {
                    let typed: &[i64] = bytemuck::cast_slice(bytes);
                    time_flat.extend(typed.iter().map(|&v| v as u64));
                }
            }
        }

        // 单遍扫描：收集全部 time 值 + sym run 边界（仅 run 切换时解析字符串）
        struct SymRun {
            row_start: usize,
            time_first: u64,
            time_last: u64,
            rows: usize,
        }
        let mut time_values: Vec<u64> = Vec::with_capacity(rows);
        let mut sym_runs: Vec<SymRun> = Vec::new();
        let mut run_names: Vec<String> = Vec::new();

        for i in 0..rows {
            time_values.push(time_flat[i]);
            let is_new = sym_runs.last().map_or(true, |r| keys_flat[i] != keys_flat[r.row_start]);
            if is_new {
                let name = sym
                    .string_at(i)
                    .ok_or_else(|| CoreError::Invalid("sym value at row is NULL".into()))?
                    .to_owned();
                sym_runs.push(SymRun {
                    row_start: i,
                    time_first: time_flat[i],
                    time_last: time_flat[i],
                    rows: 1,
                });
                run_names.push(name);
            } else {
                let run = sym_runs.last_mut().unwrap();
                run.time_last = time_flat[i];
                run.rows += 1;
            }
        }

        // TIME AXIS：排序 + 去重
        time_values.sort_unstable();
        time_values.dedup();
        let axis: Vec<u64> = time_values;

        // SYM INDEX：每 run 二分定位 time_first / time_last 在轴上的下标。
        // 连续子区间原则（docs/splayed-format.md §SYM INDEX record）：每个 sym 的 time
        // 必须严格等于 TIME AXIS 的一个连续子区间，故 span（轴跨度）必须等于 run 行数——
        // 未按 (sym ASC, time ASC) 排序、sym 内 time 重复或跳空一律拒绝，
        // 保证 time_count（行容量）与实际数据行严格对齐。
        let mut row_start = 0u32;
        let mut sym_index = Vec::with_capacity(sym_runs.len());
        for (ri, run) in sym_runs.iter().enumerate() {
            let start = axis
                .binary_search(&run.time_first)
                .expect("run time must exist in axis built from the same values")
                as u32;
            let end = axis
                .binary_search(&run.time_last)
                .expect("run time must exist in axis built from the same values")
                as u32;
            if end < start || (end - start + 1) as usize != run.rows {
                return Err(CoreError::NonContiguousTime(run_names[ri].clone()));
            }
            let span = end - start + 1;
            sym_index.push(SymIndexRecord {
                time_start: start,
                time_count: span,
                row_start,
            });
            row_start += span;
        }
        let sym_count = sym_runs.len() as u32;
        let time_count = axis.len() as u32;
        let row_count = row_start as u64;
        if row_count > u32::MAX as u64 {
            return Err(CoreError::Invalid("total row capacity exceeds u32".into()));
        }

        // 序列化
        let axis_len = time_count as usize * ts;
        let sym_dict_offset = DATA_OFFSET + axis_len as u64;
        let strings_start = sym_dict_offset as usize + (sym_count as usize + 1) * 8;
        let mut string_bytes: Vec<u8> = Vec::new();
        let mut dict_offsets: Vec<u64> = vec![0u64];
        for name in &run_names {
            string_bytes.extend_from_slice(name.as_bytes());
            dict_offsets.push(string_bytes.len() as u64);
        }
        let sym_index_offset = strings_start as u64 + string_bytes.len() as u64;
        let file_size = sym_index_offset + sym_count as u64 * 12;

        let header = MetaHeader::new(
            time_type,
            1,
            time_count,
            sym_count,
            row_count as u32,
            sym_dict_offset,
            sym_index_offset,
            file_size,
        );
        let mut out = Vec::with_capacity(file_size as usize);
        out.extend_from_slice(&header.to_bytes());
        for t in &axis {
            out.extend_from_slice(&t.to_le_bytes()[..ts]);
        }
        for off in &dict_offsets {
            out.extend_from_slice(&off.to_le_bytes());
        }
        out.extend_from_slice(&string_bytes);
        for rec in &sym_index {
            out.extend_from_slice(&rec.to_bytes());
        }
        Ok(out)
    }
}

fn infer_time_type(dt: DataType) -> Result<TimeType, CoreError> {
    match dt {
        DataType::Date32 | DataType::Int32 => Ok(TimeType::Date32),
        DataType::TimestampUs | DataType::Date64 | DataType::Int64 => Ok(TimeType::TimestampUs),
        other => Err(CoreError::Invalid(format!(
            "unsupported time column type {other:?}"
        ))),
    }
}





/// 原子写出 META（临时文件 → fsync → rename）。
pub(crate) fn write_meta_atomic(path: &Path, bytes: &[u8]) -> Result<(), CoreError> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let result = (|| -> Result<(), CoreError> {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        // tmp 完整落盘后再原子替换：rename 生效时新 META 内容已持久
        f.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
        return result;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Index 句柄（与 MetaHandle 等价）。
pub type IndexHandle = MetaHandle;

/// 创建仅包含 64 字节 MetaHeader 的空主索引文件（骨架）。
pub fn create_index(path: &Path, time_type: TimeType) -> Result<IndexHandle, CoreError> {
    if path.exists() {
        return Err(CoreError::AlreadyExists(path.to_path_buf()));
    }
    let header = MetaHeader {
        magic: splayed_format::META_MAGIC,
        version: 2,
        flags: 0,
        time_type: time_type.id(),
        reserved0: [0; 3],
        generation: 1,
        time_count: 0,
        sym_count: 0,
        row_count: 0,
        reserved1: 0,
        sym_dict_offset: META_HEADER_SIZE as u64,
        sym_index_offset: (META_HEADER_SIZE + 8) as u64,
        file_size: (META_HEADER_SIZE + 8) as u64,
    };
    let mut f = File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| map_io_path(path, e))?;
    f.write_all(&header.to_bytes())?;
    f.write_all(&[0u8; 8])?;
    f.sync_all()?;
    drop(f);
    open_index(path)
}

/// 连带数据直接初始化主索引文件并打开 IndexHandle。
pub fn init_index(path: &Path, data: &DataView<'_>) -> Result<IndexHandle, CoreError> {
    create_meta_file(path, data)?;
    open_index(path)
}

/// 打开已存在的主索引文件。
#[inline]
pub fn open_index(path: &Path) -> Result<IndexHandle, CoreError> {
    MetaHandle::open(path)
}

/// 显式关闭 Index 句柄并释放内存映射。
#[inline]
pub fn close_index(handle: IndexHandle) -> Result<(), CoreError> {
    handle.close()
}

/// 销毁并删除主索引文件。
pub fn drop_index(handle: IndexHandle) -> Result<(), CoreError> {
    let path = handle.path().to_path_buf();
    handle.close()?;
    delete_meta_file(&path)
}

/// 根据排序后的 `(sym, time)` 两列创建 META 文件（`path` 为 META 文件完整路径）。
pub fn create_meta_file(path: &Path, data: &DataView<'_>) -> Result<(), CoreError> {
    let bytes = MetaBuilder::build(data)?;
    write_meta_atomic(path, &bytes)
}

/// 删除 META 物理文件。
pub fn delete_meta_file(path: &Path) -> Result<(), CoreError> {
    fs::remove_file(path).map_err(|e| CoreError::Io(e))
}

// ------------------------------------------------------------------ MetaHandle

/// META 的只读 Handle：MetaInfo 读取、Index 读取 / 扫描与批量定位。
pub struct MetaHandle {
    path: PathBuf,
    mmap: Mmap,
    header: MetaHeader,

}

impl MetaHandle {
    pub fn open(path: &Path) -> Result<Self, CoreError> {
        let file = File::open(path).map_err(|e| map_io_path(path, e))?;
        let mmap = unsafe { Mmap::map(&file)? };
        let header = MetaHeader::from_bytes(&mmap)?;
        if mmap.len() as u64 != header.file_size {
            return Err(CoreError::InvalidState(format!(
                "meta file size {} != header file_size {}",
                mmap.len(),
                header.file_size
            )));
        }
        Ok(MetaHandle { path: path.to_path_buf(), mmap, header })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// META 自身的结构化信息（header 投影，不含 Index 查询）。
    pub fn read_meta_handle(&self) -> MetaInfo {
        MetaInfo {
            version: self.header.version,
            time_type: self.header.time_type().expect("validated at open"),
            generation: self.header.generation,
            time_count: self.header.time_count,
            sym_count: self.header.sym_count,
            row_count: self.header.row_count,
        }
    }

    pub fn header(&self) -> &MetaHeader {
        &self.header
    }

    /// TIME AXIS 元素类型。
    pub fn time_type(&self) -> TimeType {
        self.header.time_type().expect("validated at open")
    }

    fn axis_bytes(&self) -> &[u8] {
        let end = HEADER_SIZE + self.header.time_count as usize * self.header.time_type_size();
        &self.mmap[HEADER_SIZE..end]
    }

    fn dict_offsets_bytes(&self) -> &[u8] {
        let start = self.header.sym_dict_offset as usize;
        let n = self.header.sym_count as usize + 1;
        &self.mmap[start..start + n * 8]
    }

    fn strings_bytes(&self) -> &[u8] {
        let start = self.header.sym_dict_offset as usize + (self.header.sym_count as usize + 1) * 8;
        let end = self.header.sym_index_offset as usize;
        &self.mmap[start..end]
    }

    fn sym_index_bytes(&self) -> &[u8] {
        let start = self.header.sym_index_offset as usize;
        &self.mmap[start..start + self.header.sym_count as usize * 12]
    }

    pub fn sym_record(&self, sym_id: u32) -> Result<SymIndexRecord, CoreError> {
        if sym_id as usize >= self.header.sym_count as usize {
            return Err(CoreError::Invalid(format!("sym id {sym_id} out of range")));
        }
        let off = sym_id as usize * 12;
        SymIndexRecord::from_bytes(&self.sym_index_bytes()[off..]).map_err(CoreError::from)
    }

    pub fn sym_str(&self, sym_id: u32) -> Result<&str, CoreError> {
        let offsets = self.dict_offsets_bytes();
        let read = |i: usize| -> u64 {
            u64::from_le_bytes(offsets[i * 8..i * 8 + 8].try_into().unwrap())
        };
        let (lo, hi) = (read(sym_id as usize), read(sym_id as usize + 1));
        std::str::from_utf8(&self.strings_bytes()[lo as usize..hi as usize])
            .map_err(|e| CoreError::InvalidState(format!("sym dictionary is not utf-8: {e}")))
    }

    /// 字典查找：字典按首现序 = 排序序，二分 O(log S)。
    pub fn sym_id_of(&self, name: &str) -> Result<Option<u32>, CoreError> {
        let count = self.header.sym_count;
        let mut lo = 0u32;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let s = self.sym_str(mid)?;
            match s.as_bytes().cmp(name.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(Some(mid)),
            }
        }
        Ok(None)
    }

    /// TIME AXIS 的值（零扩展为 u64）。
    pub fn time_at(&self, index: u32) -> Result<u64, CoreError> {
        if index >= self.header.time_count {
            return Err(CoreError::Invalid(format!("time index {index} out of range")));
        }
        let ts = self.header.time_type_size();
        let bytes = &self.axis_bytes()[index as usize * ts..index as usize * ts + ts];
        Ok(match self.header.time_type()? {
            TimeType::Date32 => u32::from_le_bytes(bytes.try_into().unwrap()) as u64,
            TimeType::TimestampUs => u64::from_le_bytes(bytes.try_into().unwrap()),
        })
    }

    /// 逻辑行 → `(sym_id, time_index)`（容量网格，二分 SYM INDEX 的 row_start）。
    pub fn locate_row(&self, row: u64) -> Result<(u32, u32), CoreError> {
        if row >= self.header.row_count as u64 {
            return Err(CoreError::Invalid(format!("row {row} out of range")));
        }
        let bytes = self.sym_index_bytes();
        let mut lo = 0usize;
        let mut hi = self.header.sym_count as usize;
        while lo + 1 < hi {
            let mid = (lo + hi) / 2;
            let rec = SymIndexRecord::from_bytes(&bytes[mid * 12..]).map_err(CoreError::from)?;
            if rec.row_start <= row as u32 {
                lo = mid;
            } else {
                hi = mid;
            }
        }
        let rec = SymIndexRecord::from_bytes(&bytes[lo * 12..]).map_err(CoreError::from)?;
        let time_index = rec.time_start as u64 + (row - rec.row_start as u64);
        Ok((lo as u32, time_index as u32))
    }

    /// 时间值 → TIME AXIS index（精确匹配；不存在返回 None）。
    pub fn axis_index_of(&self, value: u64) -> Option<u32> {
        let mut lo = 0u64;
        let mut hi = self.header.time_count as u64;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let t = self.time_at(mid as u32).unwrap_or(u64::MAX);
            match t.cmp(&value) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid as u32),
            }
        }
        None
    }

    #[inline]
    pub fn read_index(&self, offset: u64, length: u64) -> Result<DataView<'_>, CoreError> {
        self.read_index_handle(offset, length)
    }

    #[inline]
    pub fn scan_index(&self, request: &ScanRequest) -> Result<IndexScanner, CoreError> {
        self.scan_index_handle(request)
    }

    #[inline]
    pub fn locate_index(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError> {
        self.locate_index_handle(pairs)
    }

    pub fn read_index_schema(&self) -> Schema {
        let time_type = self.header.time_type().unwrap_or(TimeType::TimestampUs);
        Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", time_type.data_type()),
        ])
    }

    pub fn update_index(&mut self, data: &DataView<'_>) -> Result<(), CoreError> {
        let tmp = self.path.with_extension("tmp");
        let bytes = MetaBuilder::build(data)?;
        let mut f = File::create(&tmp).map_err(|e| map_io_path(&tmp, e))?;
        f.write_all(&bytes).map_err(|e| map_io_path(&tmp, e))?;
        f.sync_all().map_err(|e| map_io_path(&tmp, e))?;
        drop(f);
        let new_mmap = unsafe { Mmap::map(&File::open(&tmp)?)? };
        let new_header = MetaHeader::from_bytes(&new_mmap[..META_HEADER_SIZE])?;
        // Drop existing mmap before rename on Windows
        self.mmap = memmap2::MmapOptions::new().map_anon()?.make_read_only()?;
        fs::rename(&tmp, &self.path)?;
        self.mmap = new_mmap;
        self.header = new_header;
        Ok(())
    }

    /// `read_index_handle`：按逻辑行区间返回 `(sym, time)` 两列 DataView。
    ///
    /// time 列按 sym 区间切成多段（TIME AXIS 零拷贝切片）；sym 列为 RepeatDict 段
    /// （零存储、不物化 keys），段与切片都直接指向 mmap，生命周期随 Handle。
    pub fn read_index_handle(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<DataView<'_>, CoreError> {
        if offset + length > self.header.row_count as u64 {
            return Err(CoreError::Invalid("index read range out of bounds".into()));
        }
        let time_type = self.header.time_type()?;
        if length == 0 {
            let schema = Schema::new(vec![
                FieldSchema::new("sym", DataType::Utf8),
                FieldSchema::new("time", time_type.data_type()),
            ]);
            let sym_view = ColumnView::empty(DataType::Utf8);
            let time_view = ColumnView::empty(time_type.data_type());
            return DataView::new(schema, vec![sym_view, time_view]).map_err(CoreError::from);
        }
        let ts = time_type.size_of();
        let end = offset + length;

        let mut sym_segments: Vec<ColumnSegment<'_>> = Vec::new();
        let mut time_segments: Vec<ColumnSegment<'_>> = Vec::new();

        // row_start 单调递增 → 二分找第一个可能重叠的 sym
        let mut sym_id = {
            let idx_bytes = self.sym_index_bytes();
            let mut lo = 0usize;
            let mut hi = self.header.sym_count as usize;
            while lo + 1 < hi {
                let mid = (lo + hi) / 2;
                let rec = SymIndexRecord::from_bytes(&idx_bytes[mid * 12..])
                    .map_err(CoreError::from)?;
                if rec.row_start as u64 <= offset {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            lo
        };

        while sym_id < self.header.sym_count as usize {
            let rec = self.sym_record(sym_id as u32)?;
            let rec_row_start = rec.row_start as u64;
            let rec_row_end = rec_row_start + rec.time_count as u64;

            let lo = offset.max(rec_row_start);
            let hi = end.min(rec_row_end);
            if lo >= hi {
                if rec_row_start >= end {
                    break;
                }
                sym_id += 1;
                continue;
            }
            let n = (hi - lo) as usize;

            // sym：RepeatDict 段（零存储，不物化 keys）
            let dict_offsets = BufferView::new(self.dict_offsets_bytes());
            let dict_strings = BufferView::new(self.strings_bytes());
            sym_segments.push(ColumnSegment::new_repeat_dict(
                dict_offsets,
                dict_strings,
                sym_id as u32,
                None,
                n,
            )?);

            // time：TIME AXIS 零拷贝切片
            let time_start_idx = rec.time_start as usize + (lo - rec_row_start) as usize;
            let axis = self.axis_bytes();
            let time_bytes = BufferView::new(&axis[time_start_idx * ts..(time_start_idx + n) * ts]);
            time_segments.push(ColumnSegment::new(time_type.data_type(), time_bytes, None, n)?);

            sym_id += 1;
        }

        let schema = Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", time_type.data_type()),
        ]);
        let sym_view = ColumnView::new(DataType::Utf8, sym_segments)?;
        let time_view = ColumnView::new(time_type.data_type(), time_segments)?;
        DataView::new(schema, vec![sym_view, time_view]).map_err(CoreError::from)
    }

    pub fn scan_index_handle(&self, request: &ScanRequest) -> Result<IndexScanner, CoreError> {
        // 1) 编译谓词 → (sym_ids, time_lo, time_hi)
        let (sym_ids, time_lo, time_hi) = self.compile_predicate(request.predicate.as_ref())?;

        // 2) 单次线性扫描 SYM INDEX → 直接生成 RowRange
        let mut ranges: Vec<RowRange> = Vec::new();
        let mut produced: u64 = 0u64;

        match &sym_ids {
            Some(ids) => {
                for &id in ids {
                    if request.limit.is_some_and(|l| produced >= l) { break; }
                    let rec = self.sym_record(id)?;
                    let axis_lo = rec.time_start as u64;
                    let axis_hi = axis_lo + rec.time_count as u64;
                    let wlo = time_lo.max(axis_lo).min(axis_hi);
                    let whi = time_hi.max(axis_lo).min(axis_hi);
                    if whi <= wlo { continue; }
                    ranges.push(RowRange::new(
                        rec.row_start as u64 + (wlo - axis_lo), whi - wlo,
                    ));
                    produced += whi - wlo;
                }
            }
            None => {
                for id in 0..self.header.sym_count {
                    if request.limit.is_some_and(|l| produced >= l) { break; }
                    let rec = self.sym_record(id)?;
                    let axis_lo = rec.time_start as u64;
                    let axis_hi = axis_lo + rec.time_count as u64;
                    let wlo = time_lo.max(axis_lo).min(axis_hi);
                    let whi = time_hi.max(axis_lo).min(axis_hi);
                    if whi <= wlo { continue; }
                    ranges.push(RowRange::new(
                        rec.row_start as u64 + (wlo - axis_lo), whi - wlo,
                    ));
                    produced += whi - wlo;
                }
            }
        }

        // 3) 与 request.ranges 求交（一次）
        let merged = merge_ranges(ranges);
        let ranges = if request.ranges.is_empty() {
            merged
        } else {
            let clamped = clamp_ranges(&request.ranges, self.header.row_count as u64);
            // 双指针归并求交 O(a + b)
            let mut out = Vec::new();
            let (mut i, mut j) = (0usize, 0usize);
            while i < merged.len() && j < clamped.len() {
                let lo = merged[i].offset.max(clamped[j].offset);
                let hi = merged[i].end().min(clamped[j].end());
                if hi > lo { out.push(RowRange::new(lo, hi - lo)); }
                if merged[i].end() <= clamped[j].end() { i += 1; } else { j += 1; }
            }
            out
        };

        Ok(IndexScanner { ranges, pos: 0, limit: request.limit })
    }

    /// `locate_index_handle`：`(sym, time)` 联合键批量定位。
    ///
    /// `pairs` 按 `(sym ASC, time ASC)` 排序且唯一；返回合并后的连续 RowRanges，
    /// `sum(length) == pairs.len()` 是定位成功的充要条件；key 不存在 → Error。
    pub fn locate_index_handle(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError> {
        if pairs.is_empty() {
            return Ok(Vec::new());
        }

        let mut ranges: Vec<RowRange> = Vec::new();
        let mut sym_cursor = 0usize;   // SYM INDEX 游标（单调，只前进）
        let mut time_cursor = 0usize;  // TIME AXIS 游标（sym 切换时重置到 time_start）
        let mut cached_rec = SymIndexRecord { time_start: 0, time_count: 0, row_start: 0 };
        let mut cached_sym_id = usize::MAX;

        for i in 0..pairs.len() {
            let (sym, time) = &pairs[i];
            let time_u = *time as u64;

            // 排序校验（内联，无额外遍历）
            if i > 0 {
                let (ps, pt) = &pairs[i - 1];
                if ps.as_str() > sym.as_str() || (ps == sym && *pt >= *time) {
                    return Err(CoreError::Invalid(
                        "locate input must be sorted and unique by (sym ASC, time ASC)".into(),
                    ));
                }
            }

            // sym 游标单调推进（O(S) 总计，两指针）
            while sym_cursor < self.header.sym_count as usize {
                let s = self.sym_str(sym_cursor as u32)?;
                match s.as_bytes().cmp(sym.as_bytes()) {
                    std::cmp::Ordering::Less => { sym_cursor += 1; }
                    std::cmp::Ordering::Equal => break,
                    std::cmp::Ordering::Greater => {
                        return Err(CoreError::Invalid(format!(
                            "locate: sym '{sym}' does not exist in META"
                        )));
                    }
                }
            }
            if sym_cursor >= self.header.sym_count as usize {
                return Err(CoreError::Invalid(format!(
                    "locate: sym '{sym}' does not exist in META"
                )));
            }

            // sym 切换时刷新 record 缓存 + 重置 time 游标
            if cached_sym_id != sym_cursor {
                cached_rec = self.sym_record(sym_cursor as u32)?;
                cached_sym_id = sym_cursor;
                time_cursor = cached_rec.time_start as usize;
            }

            // time 游标单调推进（sym 区间内 O(1) amortized）
            let interval_end = cached_rec.time_start as usize + cached_rec.time_count as usize;
            while time_cursor < interval_end {
                let axis_t = self.time_at(time_cursor as u32)?;
                if axis_t >= time_u { break; }
                time_cursor += 1;
            }

            // 精确匹配
            if time_cursor >= interval_end
                || self.time_at(time_cursor as u32)? != time_u
            {
                return Err(CoreError::Invalid(format!(
                    "locate: (sym, time) key does not exist: time {time} for sym '{}'",
                    self.sym_str(sym_cursor as u32)?
                )));
            }

            let row = cached_rec.row_start as u64
                + (time_cursor as u64 - cached_rec.time_start as u64);

            // 边走边合并连续行
            match ranges.last_mut() {
                Some(last) if last.end() == row => last.length += 1,
                _ => ranges.push(RowRange::new(row, 1)),
            }
        }

        let total: u64 = ranges.iter().map(|r| r.length).sum();
        if total != pairs.len() as u64 {
            return Err(CoreError::InvalidState("locate total mismatch".into()));
        }

        Ok(ranges)
    }

    /// 关闭 Handle（META 无任何写回）。
    pub fn close(self) -> Result<(), CoreError> {
        Ok(())
    }

    // ------------------------------------------------- predicate 编译

    /// 编译谓词 → (sym_ids, time_lo, time_hi)。
    ///
    /// - `sym_ids: None` = 全部 sym；`Some(sorted_ids)` = 特定 sym 集合
    /// - `time_lo / time_hi`：TIME AXIS index 窗口 `[lo, hi)`
    /// - And → sym 交集 + time 窗口收窄
    /// - Or → 仅 sym Eq 并集；其余回退
    /// - Not / 无法静态求值 → 回退（行级过滤由上层兜底）
    fn compile_predicate(
        &self,
        pred: Option<&Predicate>,
    ) -> Result<(Option<Vec<u32>>, u64, u64), CoreError> {
        let mut result = (None::<Vec<u32>>, 0u64, self.header.time_count as u64);
        if let Some(p) = pred {
            self.compile_node(p, &mut result)?;
        }
        Ok(result)
    }

    fn compile_node(
        &self,
        pred: &Predicate,
        acc: &mut (Option<Vec<u32>>, u64, u64),
    ) -> Result<(), CoreError> {
        match pred {
            Predicate::And(children) => {
                for c in children {
                    self.compile_node(c, acc)?;
                }
                Ok(())
            }
            Predicate::Or(children) => {
                // Or：仅支持全为 sym Eq 的并集
                let mut ids: Vec<u32> = Vec::new();
                for c in children {
                    match c {
                        Predicate::Cmp {
                            field: Some(f), op: CmpOp::Eq, value: Scalar::Str(s),
                        } if f.as_ref() == "sym" => {
                            if let Some(id) = self.sym_id_of(s)? {
                                ids.push(id as u32);
                            }
                        }
                        _ => return Ok(()), // 回退
                    }
                }
                ids.sort_unstable();
                // 并集合并到 acc
                match &mut acc.0 {
                    None => acc.0 = Some(ids),
                    Some(existing) => {
                        existing.extend(ids);
                        existing.sort_unstable();
                        existing.dedup();
                    }
                }
                Ok(())
            }
            Predicate::Not(_) => Ok(()),
            Predicate::Cmp { field: Some(f), op, value } => {
                if f.as_ref() == "sym" {
                    if let (CmpOp::Eq, Scalar::Str(s)) = (op, value) {
                        match self.sym_id_of(s)? {
                            Some(id) => match &mut acc.0 {
                                None => acc.0 = Some(vec![id as u32]),
                                Some(existing) => existing.retain(|&e| e == id as u32),
                            },
                            None => acc.0 = Some(Vec::new()), // 不存在 → 空集
                        }
                    }
                    Ok(())
                } else if f.as_ref() == "time" {
                    let v: u64 = match value {
                        Scalar::Int(i) => *i as u64,
                        Scalar::UInt(u) => *u,
                        Scalar::Float(f) => *f as u64,
                        _ => return Ok(()),
                    };
                    let lower = self.axis_lower_bound(v);
                    let upper = self.axis_lower_bound(v.saturating_add(1));
                    match op {
                        CmpOp::Eq => { acc.1 = acc.1.max(lower); acc.2 = acc.2.min(upper); }
                        CmpOp::Ge => { acc.1 = acc.1.max(lower); }
                        CmpOp::Gt => { acc.1 = acc.1.max(upper); }
                        CmpOp::Le => { acc.2 = acc.2.min(upper); }
                        CmpOp::Lt => { acc.2 = acc.2.min(lower); }
                        CmpOp::Ne => {}
                    }
                    Ok(())
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }

    /// TIME AXIS 上第一个 `>= v` 的 index。
    fn axis_lower_bound(&self, v: u64) -> u64 {
        let mut lo = 0u64;
        let mut hi = self.header.time_count as u64;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let t = self.time_at(mid as u32).unwrap_or(u64::MAX);
            if t < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}



/// META 自身结构化信息（`read_meta_handle` 返回）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaInfo {
    pub version: u16,
    pub time_type: TimeType,
    pub generation: u64,
    pub time_count: u32,
    pub sym_count: u32,
    pub row_count: u32,
}



/// Index Scanner：逐个返回满足 SYM / TIME 条件的 FIELD row range（`next()` 一次一个）。
pub struct IndexScanner {
    ranges: Vec<RowRange>,
    pos: usize,
    limit: Option<u64>,
}

impl IndexScanner {
    pub fn next(&mut self) -> Result<Option<RowRange>, CoreError> {
        if self.pos >= self.ranges.len() {
            return Ok(None);
        }
        let r = self.ranges[self.pos];
        self.pos += 1;
        if let Some(rem) = &mut self.limit {
            let take = r.length.min(*rem);
            *rem -= take;
            if take < r.length {
                return Ok(Some(RowRange::new(r.offset, take)));
            }
        }
        Ok(Some(r))
    }

    pub fn close(self) -> Result<(), CoreError> {
        Ok(())
    }
}
