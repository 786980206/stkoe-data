//! SUBSET（`.sub.xxx`）：父 `.meta` 网格子集的读写。
//!
//! `.sub.xxx` 与 `.meta` 同目录，记录**父 `.meta` 全局行空间**中的行区间
//! （指向 FIELD 文件的 data 区），用于"全市场 A 股 + 沪深300 子集"这类场景：
//! `.sub.hs300` 只记录选中 (SYM, TIME) 对应的行区间，读字段时按区间
//! `read_range_raw` 取子集值。
//!
//! 与 `.meta` 的差异：`sym_index` 每 SYM 一条连续区间，`.sub.xxx` 每 SYM 可多条
//! **不连续**区间（如股票进出指数多次）。区间记录复用 12B `SymIndexRecord`
//! （`time_start`/`time_count` 索引父 TIME AXIS；`row_start` 为父全局行）。
//!
//! 失效检测：header 记父 `.meta` 的 `parent_generation`；父被 `update_meta`
//! 重排后 generation 递增 → `SubsetReader::open_with_parent` 报 `StaleParent`，
//! 须重建 `.sub.xxx`。

use std::fs;
use std::path::{Path, PathBuf};

use splayed_format::{
    MetaFile, SubsetBuilder, SubsetError as FormatSubsetError, SubsetFile, SymIndexRecord,
    TimeType, SUBSET_PREFIX,
};

use crate::dataset::{open_dataset, DatasetError};
use crate::reader::{FieldReader, ReaderError};

/// 一个符号在子集内的输入：符号名 + 若干连续 TIME 段（值域为父 `.meta` 的
/// `time_type`：Date32=天数，TimestampUs=µs）。相邻/重叠段会自动合并。
#[derive(Debug, Clone)]
pub struct SubsetInput {
    pub sym: String,
    /// `(time_start_value, count)`：`time_start` 起连续 `count` 个时间点属于子集。
    pub segments: Vec<(i64, u32)>,
}

impl SubsetInput {
    pub fn new(sym: impl Into<String>, segments: Vec<(i64, u32)>) -> Self {
        Self {
            sym: sym.into(),
            segments,
        }
    }
}

/// 创建 `.sub.{name}`：把输入解析到父 `.meta` 的全局行空间并原子落盘。
///
/// - 每个 (SYM, TIME 段) 必须落在父 `.meta` 中该 SYM 的连续时间块内（否则报错）；
/// - 相邻/重叠段按 SYM 自动合并；
/// - 父 `.meta` 的 generation 记入 header（失效检测用）。
pub fn create_subset(
    dir: impl AsRef<Path>,
    name: &str,
    inputs: &[SubsetInput],
) -> Result<(), SubsetError> {
    validate_name(name)?;
    let dir = dir.as_ref();

    let dataset = open_dataset(dir).map_err(SubsetError::Dataset)?;
    let parent = &dataset.meta;
    let parent_gen = parent.header.generation;

    let mut builder = SubsetBuilder::new(parent.time_type(), 0, parent_gen);
    for input in inputs {
        let sym_idx = parent
            .find_symbol(&input.sym)
            .ok_or_else(|| SubsetError::SymNotFound(input.sym.clone()))?;
        let sym_rec = parent.sym_index[sym_idx];
        let block_end = sym_rec.time_start + sym_rec.time_count;
        for &(t, count) in &input.segments {
            if count == 0 {
                return Err(SubsetError::EmptyRange);
            }
            let ti = parent
                .find_time(t)
                .ok_or(SubsetError::TimeNotFound(t))? as u32;
            if ti < sym_rec.time_start || ti + count > block_end {
                return Err(SubsetError::SegmentOutOfRange {
                    sym: input.sym.clone(),
                    time_start: t,
                    count,
                });
            }
            let row_start = sym_rec.row_start + (ti - sym_rec.time_start);
            builder.add(
                &input.sym,
                SymIndexRecord {
                    time_start: ti,
                    time_count: count,
                    row_start,
                },
            );
        }
    }
    let file = builder.build().map_err(SubsetError::Format)?;
    let bytes = file.serialize();

    let path = dir.join(format!("{SUBSET_PREFIX}{name}"));
    let tmp = dir.join(format!("{SUBSET_PREFIX}{name}.tmp"));
    {
        use std::io::Write;
        let mut f = fs::File::create(&tmp).map_err(SubsetError::Io)?;
        f.write_all(&bytes).map_err(SubsetError::Io)?;
        f.sync_all().map_err(SubsetError::Io)?;
    }
    fs::rename(&tmp, &path).map_err(SubsetError::Io)?;
    Ok(())
}

/// 读取一个 `.sub.xxx` 文件。
#[derive(Debug)]
pub struct SubsetReader {
    pub file: SubsetFile,
}

impl SubsetReader {
    /// 打开 `.sub.{name}`（仅解析文件本身，不校验父绑定）。
    pub fn open(dir: impl AsRef<Path>, name: &str) -> Result<Self, SubsetError> {
        let path = dir.as_ref().join(format!("{SUBSET_PREFIX}{name}"));
        let bytes = fs::read(&path).map_err(SubsetError::Io)?;
        let file = SubsetFile::deserialize(&bytes).map_err(SubsetError::Format)?;
        Ok(Self { file })
    }

    /// 打开并校验父绑定：父 `.meta` 的 generation / time_type 必须与 header 一致，
    /// 否则父已被 `update_meta` 重排（或换库），需重建 `.sub.xxx`。
    pub fn open_with_parent(
        dir: impl AsRef<Path>,
        name: &str,
        parent: &MetaFile,
    ) -> Result<Self, SubsetError> {
        let reader = Self::open(dir, name)?;
        if reader.file.header.parent_generation != parent.header.generation {
            return Err(SubsetError::StaleParent {
                expected: parent.header.generation,
                got: reader.file.header.parent_generation,
            });
        }
        if reader.file.time_type() != parent.time_type() {
            return Err(SubsetError::TimeTypeMismatch {
                expected: parent.time_type(),
                got: reader.file.time_type(),
            });
        }
        Ok(reader)
    }

    pub fn time_type(&self) -> TimeType {
        self.file.time_type()
    }

    /// 子集覆盖的总行数（父 FIELD 行空间中的选中行数）。
    pub fn total_rows(&self) -> u32 {
        self.file.total_rows()
    }

    pub fn symbols(&self) -> &[String] {
        &self.file.symbols
    }

    pub fn contains(&self, sym: &str) -> bool {
        self.file.find_symbol(sym).is_some()
    }

    /// 某符号的区间列表（父全局行空间；无此符号 → None）。
    pub fn ranges(&self, sym: &str) -> Option<&[SymIndexRecord]> {
        self.file.ranges(sym)
    }

    /// 按父全局行序迭代 `(sym_idx, &SymIndexRecord)`（符号升序 × 区间 time 升序）。
    pub fn iter_ranges(&self) -> impl Iterator<Item = (usize, &SymIndexRecord)> + '_ {
        self.file
            .sym_ranges
            .iter()
            .enumerate()
            .flat_map(|(i, ranges)| ranges.iter().map(move |r| (i, r)))
    }

    /// 按父全局行序迭代 `(sym, time_value, global_row)` 三元组。
    /// `time_value` 来自父 `.meta` 的 TIME AXIS。
    pub fn iter_entries<'a>(
        &'a self,
        parent: &'a MetaFile,
    ) -> impl Iterator<Item = (&'a str, i64, u32)> + 'a {
        self.iter_ranges().flat_map(move |(sym_idx, r)| {
            let sym = self.file.symbols[sym_idx].as_str();
            let end = (r.time_start + r.time_count) as usize;
            parent.time_axis[r.time_start as usize..end]
                .iter()
                .enumerate()
                .map(move |(off, &t)| (sym, t, r.row_start + off as u32))
        })
    }

    /// 读取一个字段在子集内的全部值（父全局行序拼接），返回原始 LE 字节
    /// （长度 = `total_rows × sizeof(type)`）。
    ///
    /// 每个区间用 `FieldReader::read_range_raw` 按行段读取（NONE 时零拷贝切片
    /// 后拷贝拼接；压缩字段自动解压）。
    pub fn read_field_values(&self, reader: &FieldReader) -> Result<Vec<u8>, SubsetError> {
        let elem_sz = reader.data_type().size_of();
        let mut out = Vec::with_capacity(self.file.total_rows() as usize * elem_sz);
        for (_, r) in self.iter_ranges() {
            let slice = reader
                .read_range_raw(r.row_start, r.time_count as usize)
                .map_err(SubsetError::Reader)?;
            out.extend_from_slice(slice);
        }
        Ok(out)
    }
}

fn validate_name(name: &str) -> Result<(), SubsetError> {
    if name.is_empty()
        || name.starts_with('.')
        || name.contains('/')
        || name.contains('\\')
        || name == "."
        || name == ".."
    {
        return Err(SubsetError::BadName(name.to_string()));
    }
    Ok(())
}

/// `.sub.xxx` 文件名（含前缀）。
pub fn subset_path(dir: impl AsRef<Path>, name: &str) -> PathBuf {
    dir.as_ref().join(format!("{SUBSET_PREFIX}{name}"))
}

#[derive(Debug)]
pub enum SubsetError {
    Io(std::io::Error),
    Dataset(DatasetError),
    Format(FormatSubsetError),
    Reader(ReaderError),
    /// 子集名非法（空 / 以 `.` 开头 / 含路径分隔符）。
    BadName(String),
    /// 输入符号不在父 `.meta` 中。
    SymNotFound(String),
    /// 输入 TIME 值不在父 `.meta` 的 TIME AXIS 中。
    TimeNotFound(i64),
    /// 输入 TIME 段落在该 SYM 的连续时间块之外。
    SegmentOutOfRange {
        sym: String,
        time_start: i64,
        count: u32,
    },
    EmptyRange,
    /// 父 `.meta` 已被重排（generation 递增），`.sub.xxx` 失效，须重建。
    StaleParent { expected: u64, got: u64 },
    /// 子集 time_type 与父不一致。
    TimeTypeMismatch { expected: TimeType, got: TimeType },
}

impl std::fmt::Display for SubsetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "subset io error: {e}"),
            Self::Dataset(e) => write!(f, "subset dataset error: {e}"),
            Self::Format(e) => write!(f, "subset format error: {e}"),
            Self::Reader(e) => write!(f, "subset field read error: {e}"),
            Self::BadName(n) => write!(f, "invalid subset name: {n:?}"),
            Self::SymNotFound(s) => write!(f, "subset symbol not in parent meta: {s}"),
            Self::TimeNotFound(t) => write!(f, "subset time not in parent axis: {t}"),
            Self::SegmentOutOfRange { sym, time_start, count } => {
                write!(f, "subset segment outside sym block: {sym} @ {time_start} x{count}")
            }
            Self::EmptyRange => write!(f, "subset segment has count == 0"),
            Self::StaleParent { expected, got } => write!(
                f,
                "subset parent meta stale: parent generation {expected} != recorded {got}; recreate .sub file"
            ),
            Self::TimeTypeMismatch { expected, got } => {
                write!(f, "subset time_type {got:?} != parent {expected:?}")
            }
        }
    }
}

impl std::error::Error for SubsetError {}
