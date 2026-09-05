use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use memmap2::Mmap;
use splayed_format::{
    Buffer, BufferView, ColumnSegment, ColumnView, DataView, FieldSchema, MetaHeader, Schema,
    SymIndexRecord, TimeType, DataType, DATA_OFFSET, HEADER_SIZE,
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
    /// run-length 优化：字符串分配与比较发生在 sym run 边界（O(sym 段数)），
    /// 行内只做数值比较；轴定位用单调双指针（run 内 time 递增）。
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

        // 遍历 1：收集全部 time 值 → 全局去重有序 TIME AXIS（无字符串分配）
        let mut axis_set: Vec<u64> = Vec::with_capacity(rows);
        for seg in time.segments() {
            let bytes = seg
                .fixed_bytes()
                .ok_or_else(|| CoreError::Invalid("time column must be fixed-width".into()))?;
            let seg_rows = seg.rows();
            for i in 0..seg_rows {
                axis_set.push(read_time_bytes(bytes, i, time_type)?);
            }
        }
        axis_set.sort_unstable();
        axis_set.dedup();
        let axis: Vec<u64> = axis_set;
        // 全局行号 → 轴 index（精确匹配；值必在轴上，因为遍历 1 已全量入轴）
        let axis_pos = |v: u64| -> usize { axis.partition_point(|&x| x < v) };

        // 遍历 2：按 sym run 推进（run = 字典 key 相同的连续行）
        let mut syms: Vec<SymState> = Vec::new();
        let mut prev_name: Option<String> = None;
        let mut prev_time: Option<u64> = None;
        let mut row = 0usize; // 当前段之前累计的全局行数（段结束时 += seg_rows）
        for seg in sym.segments() {
            let keys = match seg.values() {
                splayed_format::ColumnValues::Dict { keys, .. } => keys.as_slice(),
                _ => {
                    return Err(CoreError::Invalid(
                        "sym column must be dictionary-encoded".into(),
                    ))
                }
            };
            let seg_rows = seg.rows();
            let mut i = 0usize;
            while i < seg_rows {
                let key = u32::from_le_bytes(keys[i * 4..i * 4 + 4].try_into().unwrap());
                let run_start = i;
                while i < seg_rows
                    && u32::from_le_bytes(keys[i * 4..i * 4 + 4].try_into().unwrap()) == key
                {
                    i += 1;
                }
                let run = i - run_start;
                // run 边界解析一次字符串（跨段字典不同 → 以字符串为准）
                let name = sym
                    .string_at(row + run_start)
                    .ok_or_else(|| CoreError::Invalid("sym value at row is NULL".into()))?
                    .to_owned();
                if let Some(prev) = &prev_name {
                    if prev.as_str() > name.as_str() {
                        return Err(CoreError::Invalid(
                            "meta input must be sorted by (sym ASC, time ASC)".into(),
                        ));
                    }
                }
                let same_sym = prev_name.as_deref() == Some(name.as_str());
                let mut last_t = if same_sym {
                    prev_time.unwrap_or(u64::MIN)
                } else {
                    u64::MIN // 新 sym 的时间可从头开始
                };
                // run 内逐行：time 严格递增 + 轴双指针推进
                let mut pos = axis_pos(read_time(time, row + run_start, time_type)?);
                for k in 0..run {
                    let t = read_time(time, row + run_start + k, time_type)?;
                    if t <= last_t {
                        return Err(CoreError::Invalid(
                            "meta input must be sorted by (sym ASC, time ASC)".into(),
                        ));
                    }
                    // t 必在轴上：单调推进（ amortized O(1) ）
                    while pos < axis.len() && axis[pos] < t {
                        pos += 1;
                    }
                    debug_assert_eq!(axis.get(pos), Some(&t));
                    last_t = t;
                }
                match syms.last_mut() {
                    Some(state) if same_sym => state.last_pos = pos,
                    _ => syms.push(SymState {
                        name: name.clone(),
                        first_pos: axis_pos(read_time(time, row + run_start, time_type)?),
                        last_pos: pos,
                    }),
                }
                prev_name = Some(name);
                prev_time = Some(last_t);
            }
            row += seg_rows;
        }

        let sym_count = syms.len() as u32;
        let time_count = axis.len() as u32;
        let row_count: u64 = syms
            .iter()
            .map(|s| (s.last_pos - s.first_pos + 1) as u64)
            .sum();
        if row_count > u32::MAX as u64 {
            return Err(CoreError::Invalid("total row capacity exceeds u32".into()));
        }

        let axis_len = time_count as usize * ts;
        let sym_dict_offset = DATA_OFFSET + axis_len as u64;
        let strings_start = sym_dict_offset as usize + (sym_count as usize + 1) * 8;
        let mut string_bytes: Vec<u8> = Vec::new();
        let mut dict_offsets: Vec<u64> = vec![0u64];
        for s in &syms {
            string_bytes.extend_from_slice(s.name.as_bytes());
            dict_offsets.push(string_bytes.len() as u64);
        }
        let sym_index_offset = strings_start as u64 + string_bytes.len() as u64;
        let file_size = sym_index_offset + sym_count as u64 * 12;

        let header = MetaHeader::new(
            time_type,
            1, // builder 产出初始 generation；重建时由调用方递增
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
        let mut row_start = 0u32;
        for st in &syms {
            let rec = SymIndexRecord {
                time_start: st.first_pos as u32,
                time_count: (st.last_pos - st.first_pos + 1) as u32,
                row_start,
            };
            row_start += rec.time_count;
            out.extend_from_slice(&rec.to_bytes());
        }
        Ok(out)
    }
}

struct SymState {
    name: String,
    first_pos: usize,
    last_pos: usize,
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

fn read_time(time: &ColumnView<'_>, row: usize, time_type: TimeType) -> Result<u64, CoreError> {
    let mut offset = row;
    for seg in time.segments() {
        let seg_rows = seg.rows();
        if offset < seg_rows {
            let bytes = seg
                .fixed_bytes()
                .ok_or_else(|| CoreError::Invalid("time column must be fixed-width".into()))?;
            return read_time_bytes(bytes, offset, time_type);
        }
        offset -= seg_rows;
    }
    Err(CoreError::Invalid("time row out of range".into()))
}

fn read_time_bytes(bytes: &[u8], row: usize, time_type: TimeType) -> Result<u64, CoreError> {
    let v = match time_type {
        TimeType::Date32 => {
            u32::from_le_bytes(bytes[row * 4..row * 4 + 4].try_into().unwrap()) as u64
        }
        TimeType::TimestampUs => {
            u64::from_le_bytes(bytes[row * 8..row * 8 + 8].try_into().unwrap())
        }
    };
    Ok(v)
}

/// 原子写出 META（临时文件 → fsync → rename）。
pub(crate) fn write_meta_atomic(path: &Path, bytes: &[u8]) -> Result<(), CoreError> {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
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
    /// scratch：read_index 组装视图时物化的 keys 缓冲（Box 稳定地址，逐次累积，随 Handle 存活）。
    scratch: RefCell<Vec<Box<Buffer>>>,
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
        Ok(MetaHandle { path: path.to_path_buf(), mmap, header, scratch: RefCell::new(Vec::new()) })
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

    /// `read_index_handle`：按逻辑行区间返回 `(sym, time)` 两列 DataView。
    ///
    /// time 列按 sym 区间切成多段（TIME AXIS 零拷贝切片）；sym 列的字典 keys 需物化
    /// （缓存进 Handle 的 scratch，生命周期随 Handle）。
    pub fn read_index_handle(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<DataView<'_>, CoreError> {
        if offset + length > self.header.row_count as u64 {
            return Err(CoreError::Invalid("index read range out of bounds".into()));
        }
        let time_type = self.header.time_type()?;
        let ts = time_type.size_of();
        let mut keys: Vec<u8> = Vec::with_capacity(length as usize * 4);
        let mut spans: Vec<(usize, usize)> = Vec::new(); // (axis 起始 index, 行数)
        let end = offset + length;
        let mut sym_id = self.locate_row(offset)?.0;
        while sym_id < self.header.sym_count {
            let rec = self.sym_record(sym_id)?;
            let row_start = rec.row_start as u64;
            let row_end = row_start + rec.time_count as u64;
            if row_start >= end {
                break;
            }
            let lo = offset.max(row_start);
            let hi = end.min(row_end);
            if hi > lo {
                let k = sym_id.to_le_bytes();
                for _ in 0..(hi - lo) {
                    keys.extend_from_slice(&k);
                }
                let t_lo = rec.time_start as usize + (lo - row_start) as usize;
                spans.push((t_lo, (hi - lo) as usize));
            }
            sym_id += 1;
        }
        let keys_buf = crate::arena::push(&self.scratch, Buffer::from_vec(keys));
        let schema = Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", time_type.data_type()),
        ]);
        let sym_view = dict_column_view(
            BufferView::from_buffer(keys_buf),
            BufferView::new(self.dict_offsets_bytes()),
            BufferView::new(self.strings_bytes()),
            length as usize,
        )?;
        let axis = self.axis_bytes();
        let mut time_segments: Vec<ColumnSegment<'_>> = Vec::with_capacity(spans.len());
        for (t_lo, n) in spans {
            let values = BufferView::new(&axis[t_lo * ts..(t_lo + n) * ts]);
            time_segments.push(ColumnSegment::new(time_type.data_type(), values, None, n)?);
        }
        let time_view = ColumnView::new(time_type.data_type(), time_segments)?;
        let schema2 = schema;
        DataView::new(schema2, vec![sym_view, time_view]).map_err(CoreError::from)
    }

    /// `scan_index_handle`：SYM / TIME 条件 → FIELD row ranges。
    pub fn scan_index_handle(&self, request: &ScanRequest) -> Result<IndexScanner, CoreError> {
        let time_window = self.extract_time_window(request.predicate.as_ref());
        let sym_ids = self.extract_sym_filter(request.predicate.as_ref())?;
        let candidates: Vec<u32> = match sym_ids {
            SymFilter::All => (0..self.header.sym_count).collect(),
            SymFilter::Ids(ids) => ids,
        };
        let mut ranges: Vec<RowRange> = Vec::new();
        let mut produced: u64 = 0;
        for id in candidates {
            if request.limit.is_some_and(|l| produced >= l) {
                break;
            }
            let rec = self.sym_record(id)?;
            let axis_lo = rec.time_start as u64;
            let axis_hi = axis_lo + rec.time_count as u64;
            let (lo, hi) = time_window;
            let wlo = lo.max(axis_lo).min(axis_hi);
            let whi = hi.max(axis_lo).min(axis_hi);
            if whi <= wlo {
                continue;
            }
            let range = RowRange::new(rec.row_start as u64 + (wlo - axis_lo), whi - wlo);
            for r in clamp_ranges(&request.ranges, self.header.row_count as u64) {
                if let Some(inter) = r.intersect(&range) {
                    produced += inter.length;
                    ranges.push(inter);
                }
            }
        }
        Ok(IndexScanner { ranges: merge_ranges(ranges), pos: 0, limit: request.limit })
    }

    /// `locate_index_handle`：`(sym, time)` 联合键批量定位。
    ///
    /// `pairs` 按 `(sym ASC, time ASC)` 排序且唯一；返回合并后的连续 RowRanges，
    /// `sum(length) == pairs.len()` 是定位成功的充要条件；key 不存在 → Error。
    pub fn locate_index_handle(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError> {
        // 1) 输入 sym → 字典 id（不存在 → Error）
        let mut ids: Vec<(u32, i64)> = Vec::with_capacity(pairs.len());
        for (sym, t) in pairs {
            match self.sym_id_of(sym)? {
                Some(id) => ids.push((id, *t)),
                None => {
                    return Err(CoreError::Invalid(format!(
                        "locate: sym '{sym}' does not exist in META"
                    )))
                }
            }
        }
        // 2) 校验有序唯一
        for w in ids.windows(2) {
            if w[0] >= w[1] {
                return Err(CoreError::Invalid(
                    "locate input must be sorted and unique by (sym, time)".into(),
                ));
            }
        }
        // 3) 逐 key 定位：时间值 → 轴 index → sym 区间内判存 → 连续行合并
        let mut raw: Vec<RowRange> = Vec::with_capacity(ids.len());
        let mut current_sym: Option<u32> = None;
        let mut record = SymIndexRecord { time_start: 0, time_count: 0, row_start: 0 };
        for (id, t) in &ids {
            if current_sym != Some(*id) {
                record = self.sym_record(*id)?;
                current_sym = Some(*id);
            }
            // 时间值 → 轴 index（精确匹配；不在轴上 = key 不存在）
            let Some(t_idx) = self.axis_index_of(*t as u64) else {
                return Err(CoreError::Invalid(format!(
                    "locate: (sym, time) key does not exist: time {t} for sym '{}'",
                    self.sym_str(*id)?
                )));
            };
            let Some(row) = record.global_row(t_idx) else {
                return Err(CoreError::Invalid(format!(
                    "locate: (sym, time) key does not exist: time {t} for sym '{}'",
                    self.sym_str(*id)?
                )));
            };
            match raw.last_mut() {
                Some(last) if last.end() == row as u64 => last.length += 1,
                _ => raw.push(RowRange::new(row as u64, 1)),
            }
        }
        let merged = merge_ranges(raw);
        let total: u64 = merged.iter().map(|r| r.length).sum();
        if total != pairs.len() as u64 {
            return Err(CoreError::InvalidState("locate total mismatch".into()));
        }
        Ok(merged)
    }

    /// 关闭 Handle（META 无任何写回）。
    pub fn close(self) -> Result<(), CoreError> {
        Ok(())
    }

    // ------------------------------------------------- predicate 提取（内部）

    /// 谓词中 sym 条件 → 候选 sym id 集合。无法静态求值的部分回退为 All（行级过滤兜底）。
    fn extract_sym_filter(&self, pred: Option<&Predicate>) -> Result<SymFilter, CoreError> {
        match pred {
            None => Ok(SymFilter::All),
            Some(p) => self.sym_filter_of(p),
        }
    }

    fn sym_filter_of(&self, pred: &Predicate) -> Result<SymFilter, CoreError> {
        match pred {
            Predicate::And(children) => {
                // AND = 各子过滤的交集
                let mut acc: Option<SymFilter> = None;
                for c in children {
                    let f = self.sym_filter_of(c)?;
                    acc = Some(match acc {
                        None => f,
                        Some(a) => a.intersect(f),
                    });
                }
                Ok(acc.unwrap_or(SymFilter::All))
            }
            Predicate::Or(children) => {
                // OR = 并集；任一子项回退 All → 整体 All
                let mut acc: Option<SymFilter> = None;
                for c in children {
                    let f = self.sym_filter_of(c)?;
                    acc = Some(match acc {
                        None => f,
                        Some(a) => a.union(f),
                    });
                }
                Ok(acc.unwrap_or(SymFilter::All))
            }
            Predicate::Not(_) => Ok(SymFilter::All),
            Predicate::Cmp { field: Some(f), op: CmpOp::Eq, value: Scalar::Str(s) }
                if f.as_ref() == "sym" =>
            {
                match self.sym_id_of(s)? {
                    Some(id) => Ok(SymFilter::Ids(vec![id])),
                    None => Ok(SymFilter::Ids(Vec::new())),
                }
            }
            Predicate::Cmp {
                field: Some(f),
                op: CmpOp::Eq | CmpOp::Ne,
                value: Scalar::Str(_),
            } if f.as_ref() == "sym" => Ok(SymFilter::All), // Ne / 不等式回退
            _ => Ok(SymFilter::All),
        }
    }

    /// 谓词中 time 条件 → 轴 index 窗口 `[lo, hi)`（AND 收窄；OR / NOT 回退全轴）。
    fn extract_time_window(&self, pred: Option<&Predicate>) -> (u64, u64) {
        let full = (0u64, self.header.time_count as u64);
        match pred {
            None => full,
            Some(p) => self.time_window_of(p, full),
        }
    }

    fn time_window_of(&self, pred: &Predicate, window: (u64, u64)) -> (u64, u64) {
        let full = (0u64, self.header.time_count as u64);
        match pred {
            Predicate::And(children) => {
                let mut acc = window;
                for c in children {
                    acc = self.time_window_of(c, acc);
                }
                acc
            }
            Predicate::Or(_) | Predicate::Not(_) => full,
            Predicate::Cmp { field: Some(f), op, value }
                if f.as_ref() == "time" =>
            {
                // 标量 → 轴 index：TIME AXIS 有序，二分定位
                let v: u64 = match value {
                    Scalar::Int(i) => *i as u64,
                    Scalar::UInt(u) => *u,
                    Scalar::Float(f) => *f as u64,
                    _ => return window,
                };
                let lower = self.axis_lower_bound(v);
                let upper = self.axis_lower_bound(v.saturating_add(1));
                match op {
                    CmpOp::Eq => (lower.max(window.0), upper.min(window.1)),
                    CmpOp::Ge => (lower.max(window.0), window.1),
                    CmpOp::Gt => (upper.max(window.0), window.1),
                    CmpOp::Lt => (window.0, lower.min(window.1)),
                    CmpOp::Le => (window.0, upper.min(window.1)),
                    CmpOp::Ne => full,
                }
            }
            _ => window,
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

#[derive(Debug, Clone)]
enum SymFilter {
    All,
    Ids(Vec<u32>),
}

impl SymFilter {
    fn intersect(self, other: SymFilter) -> SymFilter {
        match (self, other) {
            (SymFilter::All, f) | (f, SymFilter::All) => f,
            (SymFilter::Ids(a), SymFilter::Ids(b)) => {
                SymFilter::Ids(a.into_iter().filter(|x| b.contains(x)).collect())
            }
        }
    }

    fn union(self, other: SymFilter) -> SymFilter {
        match (self, other) {
            (SymFilter::All, _) | (_, SymFilter::All) => SymFilter::All,
            (SymFilter::Ids(a), SymFilter::Ids(b)) => {
                let mut ids = a;
                ids.extend(b);
                ids.sort_unstable();
                ids.dedup();
                SymFilter::Ids(ids)
            }
        }
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

/// 由 keys + 字典段组装 Utf8 字典列视图（keys 需已物化）。
fn dict_column_view<'a>(
    keys: BufferView<'a>,
    dict_offsets: BufferView<'a>,
    dict_strings: BufferView<'a>,
    rows: usize,
) -> Result<ColumnView<'a>, CoreError> {
    let seg = ColumnSegment::new_dict(keys, dict_offsets, dict_strings, None, rows)?;
    ColumnView::new(DataType::Utf8, vec![seg]).map_err(CoreError::from)
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
