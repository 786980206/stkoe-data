//! SUBSET 文件格式（`.sub.xxx`）——父 `.meta` 的 `SYM × TIME` 网格子集。
//!
//! 见 `plan.md` §5.7。与 META（§5.1）的**相似接口**：同样的 64B header、
//! 同样的 SYM 字典编码（`(sym_count+1) × 8` 偏移表 + 字符串数据）、区间记录复用
//! 12B `SymIndexRecord`。**差异点**：META 的 `sym_index` 每个 SYM 只有一条连续区间
//! 记录；SUBSET 每个 SYM 可有多条**不连续**区间（例如某股票进出沪深300多次）。
//!
//! ```text
//! [Header 64B]
//! [SYM DICT INDEX: (sym_count + 1) × 8 bytes]   ← u64 offsets into STRING DATA
//! [SYM STRING DATA: variable]
//! [RANGE INDEX: sym_count × (u32 count + count × 12B SymIndexRecord)]
//! ```
//!
//! 区间语义（与父 `.meta` 对齐）：
//! - `time_start` / `time_count`：索引父 `.meta` 的**全局 TIME AXIS**；
//! - `row_start`：父 `.meta` 的**全局行空间**（即 FIELD 文件的 data 区行号），
//!   `row = sym_row_start + (time_idx - sym.time_start)`（仿射，父 SYM INDEX 定义）。
//! - `total_rows` = 全部区间 `time_count` 之和 = 子集覆盖的行数。
//!
//! 父绑定与失效：header 记录 `parent_generation`（父 `.meta` 建/重排时的
//! generation）。父被 `update_meta` 重排后 generation 递增 → `SubsetReader`
//! 检测 `StaleParent`，须重建 `.sub.xxx`。
//!
//! 文件名规则：`name` 如 `hs300` → 文件 `.sub.hs300`。以 `.` 开头 → 天然被
//! `Dataset::list_fields` / `update_meta::list_field_names` 忽略，不会被当成 FIELD。

use crate::header::{TimeType, HEADER_SIZE};
use crate::meta::SymIndexRecord;

/// SUBSET 文件 magic：`SPLAYSUB`（LE u64）。
pub const SUBSET_MAGIC: u64 = u64::from_le_bytes(*b"SPLAYSUB");

/// `.sub.xxx` 文件名的固定前缀。
pub const SUBSET_PREFIX: &str = ".sub.";

/// 当前 SUBSET 格式版本。
pub const SUBSET_VERSION: u16 = 1;

/// SUBSET header（固定 64 字节）。见 plan §5.7。
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SubsetHeader {
    pub magic: u64,             // 0
    pub version: u16,           // 8
    pub flags: u16,             // 10
    pub time_type: u8,          // 12（必须与父 `.meta` 一致）
    pub reserved: [u8; 3],      // 13
    pub generation: u64,        // 16（子集自身代数；重写时递增）
    pub parent_generation: u64, // 24（父 `.meta` 的 generation；失效检测）
    pub sym_count: u32,         // 32
    pub range_count: u32,       // 36（全符号区间总数）
    pub total_rows: u32,        // 40（子集覆盖行数 = Σ time_count）
    pub _pad: [u8; 20],         // 44..64
}

const _: () = {
    assert!(
        std::mem::size_of::<SubsetHeader>() == HEADER_SIZE,
        "SubsetHeader must be 64 bytes"
    );
};

unsafe impl bytemuck::Pod for SubsetHeader {}
unsafe impl bytemuck::Zeroable for SubsetHeader {}

impl SubsetHeader {
    pub fn new(
        time_type: TimeType,
        generation: u64,
        parent_generation: u64,
        sym_count: u32,
        range_count: u32,
        total_rows: u32,
    ) -> Self {
        Self {
            magic: SUBSET_MAGIC,
            version: SUBSET_VERSION,
            flags: 0,
            time_type: time_type as u8,
            reserved: [0; 3],
            generation,
            parent_generation,
            sym_count,
            range_count,
            total_rows,
            _pad: [0; 20],
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.magic != SUBSET_MAGIC {
            return Err("invalid SUBSET magic");
        }
        if self.version != SUBSET_VERSION {
            return Err("unsupported SUBSET version");
        }
        Ok(())
    }

    pub fn time_type(&self) -> Result<TimeType, &'static str> {
        TimeType::from_id(self.time_type).ok_or("invalid SUBSET time_type")
    }
}

/// 已解析的 `.sub.xxx` 文件。
#[derive(Debug, Clone)]
pub struct SubsetFile {
    pub header: SubsetHeader,
    /// 子集内符号（升序）。
    pub symbols: Vec<String>,
    /// 每符号的区间列表（按 `time_start` 升序）；一个符号可有多条不连续区间。
    pub sym_ranges: Vec<Vec<SymIndexRecord>>,
}

impl SubsetFile {
    pub fn time_type(&self) -> TimeType {
        self.header.time_type().expect("validated on read")
    }

    /// 子集覆盖的总行数（= 父 FIELD 行空间中选中行数）。
    pub fn total_rows(&self) -> u32 {
        self.header.total_rows
    }

    pub fn sym_count(&self) -> usize {
        self.symbols.len()
    }

    /// 二分查找符号。
    pub fn find_symbol(&self, sym: &str) -> Option<usize> {
        self.symbols.binary_search_by_key(&sym, |s| s.as_str()).ok()
    }

    /// 某符号的区间列表（无此符号 → None）。
    pub fn ranges(&self, sym: &str) -> Option<&[SymIndexRecord]> {
        self.find_symbol(sym).map(|i| self.sym_ranges[i].as_slice())
    }

    // -- Serialization -------------------------------------------------------

    /// 序列化整个 SUBSET 文件。
    ///
    /// 布局：Header(64) | SYM DICT INDEX | SYM STRING DATA | RANGE INDEX
    pub fn serialize(&self) -> Vec<u8> {
        let sym_count = self.symbols.len();

        let dict_index_bytes = (sym_count + 1) * 8;
        let string_data: Vec<u8> = self
            .symbols
            .iter()
            .flat_map(|s| s.as_bytes().iter().copied())
            .collect();
        let string_data_bytes = string_data.len();
        let range_index_bytes: usize = self
            .sym_ranges
            .iter()
            .map(|ranges| 4 + ranges.len() * 12)
            .sum();

        let dict_offset = HEADER_SIZE as u64;
        let string_offset = dict_offset + dict_index_bytes as u64;
        let range_offset = string_offset + string_data_bytes as u64;
        let file_size = range_offset + range_index_bytes as u64;

        let total = file_size as usize;
        let mut buf = vec![0u8; total];

        // --- Header ---
        buf[..HEADER_SIZE].copy_from_slice(bytemuck::bytes_of(&self.header));

        // --- SYM DICT INDEX (sym_count + 1 offsets) ---
        let mut off = HEADER_SIZE;
        let mut str_off: u64 = 0;
        for s in &self.symbols {
            buf[off..off + 8].copy_from_slice(&str_off.to_le_bytes());
            off += 8;
            str_off += s.len() as u64;
        }
        buf[off..off + 8].copy_from_slice(&str_off.to_le_bytes());
        off += 8;
        debug_assert_eq!(off, string_offset as usize);

        // --- SYM STRING DATA ---
        buf[off..off + string_data_bytes].copy_from_slice(&string_data);
        off += string_data_bytes;
        debug_assert_eq!(off, range_offset as usize);

        // --- RANGE INDEX ---
        for ranges in &self.sym_ranges {
            buf[off..off + 4].copy_from_slice(&(ranges.len() as u32).to_le_bytes());
            off += 4;
            for rec in ranges {
                buf[off..off + 12].copy_from_slice(bytemuck::bytes_of(rec));
                off += 12;
            }
        }
        debug_assert_eq!(off, total);

        buf
    }

    // -- Deserialization -----------------------------------------------------

    /// 从字节缓冲解析 SUBSET 文件。
    pub fn deserialize(buf: &[u8]) -> Result<Self, SubsetError> {
        if buf.len() < HEADER_SIZE {
            return Err(SubsetError::TooShort);
        }
        let header: SubsetHeader = bytemuck::pod_read_unaligned(&buf[..HEADER_SIZE]);
        header.validate().map_err(SubsetError::Header)?;
        header.time_type().map_err(SubsetError::Header)?;

        let sym_count = header.sym_count as usize;

        // --- SYM DICT INDEX + STRING DATA ---
        let dict_off = HEADER_SIZE;
        let dict_index_end = dict_off + (sym_count + 1) * 8;
        if buf.len() < dict_index_end {
            return Err(SubsetError::Truncated);
        }
        let mut symbols = Vec::with_capacity(sym_count);
        for i in 0..sym_count {
            let start =
                u64::from_le_bytes(buf[dict_off + i * 8..dict_off + i * 8 + 8].try_into().unwrap()) as usize;
            let end = u64::from_le_bytes(
                buf[dict_off + (i + 1) * 8..dict_off + (i + 1) * 8 + 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let str_start = dict_index_end + start;
            let str_end = dict_index_end + end;
            if str_start > str_end || str_end > buf.len() {
                return Err(SubsetError::Truncated);
            }
            symbols.push(
                std::str::from_utf8(&buf[str_start..str_end])
                    .map_err(SubsetError::Utf8)?
                    .to_string(),
            );
        }

        // --- RANGE INDEX ---
        // RANGE INDEX 起点 = dict_index_end + 字符串数据总长（SYM DICT 最后一项的 end）。
        let last_end = u64::from_le_bytes(
            buf[dict_off + sym_count * 8..dict_off + sym_count * 8 + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let range_off = dict_index_end + last_end;
        let mut off = range_off;
        let mut sym_ranges = Vec::with_capacity(sym_count);
        let mut total_ranges = 0u32;
        let mut total_rows = 0u32;
        for _ in 0..sym_count {
            if off + 4 > buf.len() {
                return Err(SubsetError::Truncated);
            }
            let n = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            let end = off + n * 12;
            if end > buf.len() {
                return Err(SubsetError::Truncated);
            }
            let mut ranges = Vec::with_capacity(n);
            for _ in 0..n {
                let rec: SymIndexRecord = bytemuck::pod_read_unaligned(&buf[off..off + 12]);
                off += 12;
                ranges.push(rec);
            }
            total_ranges += n as u32;
            total_rows += ranges.iter().map(|r| r.time_count).sum::<u32>();
            sym_ranges.push(ranges);
        }

        // --- file_size 一致性：header.total_rows / range_count 校验 ---
        if header.range_count != total_ranges {
            return Err(SubsetError::Header("range_count mismatch"));
        }
        if header.total_rows != total_rows {
            return Err(SubsetError::Header("total_rows mismatch"));
        }

        Ok(Self {
            header,
            symbols,
            sym_ranges,
        })
    }
}

/// 从已解析的（time_idx, count）段构建 `SubsetFile`（按 SYM 合并相邻/重叠区间）。
///
/// 段已由调用方（core `create_subset`）解析到父 `.meta` 的全局 TIME AXIS 下标：
/// 段 `(time_start, time_count)` 与父 SYM INDEX 记录同语义，`row_start` 已在
/// 父全局行空间中。
pub struct SubsetBuilder {
    time_type: TimeType,
    generation: u64,
    parent_generation: u64,
    /// sym → 未排序区间（构建时排序+合并）。
    entries: Vec<(String, SymIndexRecord)>,
}

impl SubsetBuilder {
    pub fn new(time_type: TimeType, generation: u64, parent_generation: u64) -> Self {
        Self {
            time_type,
            generation,
            parent_generation,
            entries: Vec::new(),
        }
    }

    /// 添加一个连续区间（time_start/time_count 索引父 TIME AXIS；row_start 为父全局行）。
    pub fn add(&mut self, sym: &str, range: SymIndexRecord) {
        self.entries.push((sym.to_string(), range));
    }

    pub fn build(self) -> Result<SubsetFile, SubsetError> {
        if self.entries.is_empty() {
            return Err(SubsetError::Empty);
        }
        // 按 sym 分组（保留字典序），每 sym 内按 time_start 升序后合并。
        let mut by_sym: std::collections::BTreeMap<&str, Vec<SymIndexRecord>> =
            std::collections::BTreeMap::new();
        for (sym, range) in &self.entries {
            if range.time_count == 0 {
                return Err(SubsetError::EmptyRange);
            }
            by_sym.entry(sym.as_str()).or_default().push(*range);
        }

        let mut symbols = Vec::with_capacity(by_sym.len());
        let mut sym_ranges = Vec::with_capacity(by_sym.len());
        let mut range_count = 0u32;
        let mut total_rows = 0u32;

        for (sym, mut ranges) in by_sym {
            ranges.sort_by_key(|r| r.time_start);
            // 合并相邻/重叠区间：time_start_2 <= time_start_1 + count_1。
            let mut merged: Vec<SymIndexRecord> = Vec::with_capacity(ranges.len());
            for r in ranges {
                match merged.last_mut() {
                    Some(prev) if r.time_start <= prev.time_start + prev.time_count => {
                        let end = r.time_start + r.time_count;
                        if end > prev.time_start + prev.time_count {
                            prev.time_count = end - prev.time_start;
                        }
                    }
                    _ => merged.push(r),
                }
            }
            for r in &merged {
                range_count += 1;
                total_rows += r.time_count;
            }
            symbols.push(sym.to_string());
            sym_ranges.push(merged);
        }

        let header = SubsetHeader::new(
            self.time_type,
            self.generation,
            self.parent_generation,
            symbols.len() as u32,
            range_count,
            total_rows,
        );

        Ok(SubsetFile {
            header,
            symbols,
            sym_ranges,
        })
    }
}

/// SUBSET 格式错误。
#[derive(Debug)]
pub enum SubsetError {
    TooShort,
    Truncated,
    Header(&'static str),
    Utf8(std::str::Utf8Error),
    /// 没有任何区间。
    Empty,
    /// 某区间 time_count == 0。
    EmptyRange,
}

impl std::fmt::Display for SubsetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort => write!(f, "subset file too short for header"),
            Self::Truncated => write!(f, "subset file truncated"),
            Self::Header(m) => write!(f, "subset header invalid: {m}"),
            Self::Utf8(e) => write!(f, "subset symbol utf8 error: {e}"),
            Self::Empty => write!(f, "subset has no ranges"),
            Self::EmptyRange => write!(f, "subset contains an empty range"),
        }
    }
}

impl std::error::Error for SubsetError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// 3 符号：SYM01 两条不连续区间；SYM02 一条；SYM03 两条（相邻→合并成一条）。
    fn make_subset() -> SubsetFile {
        let mut b = SubsetBuilder::new(TimeType::Date32, 1, 7);
        // SYM01: [0,2) ∪ [4,5)（day 3 缺席）
        b.add("SYM01", SymIndexRecord { time_start: 0, time_count: 2, row_start: 10 });
        b.add("SYM01", SymIndexRecord { time_start: 4, time_count: 1, row_start: 14 });
        // SYM02: [1,3)
        b.add("SYM02", SymIndexRecord { time_start: 1, time_count: 2, row_start: 100 });
        // SYM03: [2,2) 相邻 → 合并为 [2,4)
        b.add("SYM03", SymIndexRecord { time_start: 2, time_count: 2, row_start: 200 });
        b.add("SYM03", SymIndexRecord { time_start: 4, time_count: 2, row_start: 202 });
        b.build().unwrap()
    }

    #[test]
    fn builder_merges_and_sorts() {
        let s = make_subset();
        assert_eq!(s.symbols, vec!["SYM01", "SYM02", "SYM03"]);
        // SYM01：两段不合并
        assert_eq!(
            s.ranges("SYM01").unwrap(),
            &[
                SymIndexRecord { time_start: 0, time_count: 2, row_start: 10 },
                SymIndexRecord { time_start: 4, time_count: 1, row_start: 14 },
            ]
        );
        // SYM03：相邻两段合并
        assert_eq!(
            s.ranges("SYM03").unwrap(),
            &[SymIndexRecord { time_start: 2, time_count: 4, row_start: 200 }]
        );
        assert_eq!(s.total_rows(), 2 + 1 + 2 + 4);
        assert_eq!(s.header.range_count, 4);
        assert_eq!(s.header.parent_generation, 7);
        assert_eq!(s.time_type(), TimeType::Date32);
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let s = make_subset();
        let bytes = s.serialize();
        let s2 = SubsetFile::deserialize(&bytes).unwrap();
        assert_eq!(s2.symbols, s.symbols);
        assert_eq!(s2.sym_ranges, s.sym_ranges);
        assert_eq!(s2.total_rows(), s.total_rows());
        assert_eq!(s2.header.range_count, s.header.range_count);
        assert_eq!(s2.header.parent_generation, 7);
        assert_eq!(s2.time_type(), TimeType::Date32);
    }

    #[test]
    fn empty_subset_rejected() {
        let b = SubsetBuilder::new(TimeType::Date32, 1, 1);
        assert!(matches!(b.build(), Err(SubsetError::Empty)));
    }

    #[test]
    fn overlapping_segments_merge() {
        let mut b = SubsetBuilder::new(TimeType::Date32, 1, 1);
        b.add("A", SymIndexRecord { time_start: 0, time_count: 3, row_start: 0 });
        b.add("A", SymIndexRecord { time_start: 2, time_count: 4, row_start: 2 });
        let s = b.build().unwrap();
        assert_eq!(s.ranges("A").unwrap().len(), 1);
        assert_eq!(s.ranges("A").unwrap()[0].time_count, 6);
        assert_eq!(s.total_rows(), 6);
    }
}
