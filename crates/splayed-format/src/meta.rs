use bytemuck::{Pod, Zeroable};

use crate::error::FormatError;
use crate::types::TimeType;
use crate::{FORMAT_VERSION, HEADER_SIZE, META_MAGIC};

pub const META_HEADER_SIZE: usize = HEADER_SIZE;
pub const SYM_INDEX_RECORD_SIZE: usize = 12;

/// META header，固定 64 字节（docs/splayed-format.md §7）。
///
/// 布局（offset）：
/// `magic 0 / version 8 / flags 10 / time_type 12 / reserved 13 / generation 16 /
/// time_count 24 / sym_count 28 / row_count 32 / reserved 36 / sym_dict_offset 40 /
/// sym_index_offset 48 / file_size 56`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct MetaHeader {
    pub magic: u64,
    pub version: u16,
    pub flags: u16,
    pub time_type: u8,
    pub reserved0: [u8; 3],
    pub generation: u64,
    pub time_count: u32,
    pub sym_count: u32,
    pub row_count: u32,
    pub reserved1: u32,
    pub sym_dict_offset: u64,
    pub sym_index_offset: u64,
    pub file_size: u64,
}

impl MetaHeader {
    /// 构建一个待写出的 header；`row_count` 恒等于 `Σ time_count`，由构建方保证。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        time_type: TimeType,
        generation: u64,
        time_count: u32,
        sym_count: u32,
        row_count: u32,
        sym_dict_offset: u64,
        sym_index_offset: u64,
        file_size: u64,
    ) -> Self {
        MetaHeader {
            magic: META_MAGIC,
            version: FORMAT_VERSION,
            flags: 0,
            time_type: time_type.id(),
            reserved0: [0; 3],
            generation,
            time_count,
            sym_count,
            row_count,
            reserved1: 0,
            sym_dict_offset,
            sym_index_offset,
            file_size,
        }
    }

    pub fn to_bytes(&self) -> [u8; META_HEADER_SIZE] {
        bytemuck::bytes_of(self).try_into().expect("MetaHeader is 64 bytes")
    }

    /// 从 64 字节解析并校验 magic / version。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() < META_HEADER_SIZE {
            return Err(FormatError::SizeMismatch {
                expected: META_HEADER_SIZE,
                found: bytes.len(),
            });
        }
        let header: Self = bytemuck::pod_read_unaligned::<Self>(&bytes[..META_HEADER_SIZE]);
        header.validate()?;
        Ok(header)
    }

    pub fn validate(&self) -> Result<(), FormatError> {
        if self.magic != META_MAGIC {
            return Err(FormatError::BadMagic { expected: META_MAGIC, found: self.magic });
        }
        if self.version != FORMAT_VERSION {
            return Err(FormatError::BadVersion { expected: FORMAT_VERSION, found: self.version });
        }
        TimeType::from_id(self.time_type)?;
        Ok(())
    }

    pub fn time_type(&self) -> Result<TimeType, FormatError> {
        TimeType::from_id(self.time_type)
    }

    pub fn time_type_size(&self) -> usize {
        self.time_type().map(|t| t.size_of()).unwrap_or(0)
    }
}

/// SYM INDEX record，固定 12 字节。
///
/// `time_count` 双重语义：SYM 在全局 TIME AXIS 中的连续区间长度 = FIELD 中的行容量。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct SymIndexRecord {
    pub time_start: u32,
    pub time_count: u32,
    pub row_start: u32,
}

impl SymIndexRecord {
    pub const SIZE: usize = SYM_INDEX_RECORD_SIZE;

    pub fn to_bytes(&self) -> [u8; SYM_INDEX_RECORD_SIZE] {
        bytemuck::bytes_of(self).try_into().expect("SymIndexRecord is 12 bytes")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() < SYM_INDEX_RECORD_SIZE {
            return Err(FormatError::SizeMismatch {
                expected: SYM_INDEX_RECORD_SIZE,
                found: bytes.len(),
            });
        }
        Ok(bytemuck::pod_read_unaligned::<Self>(bytes))
    }

    /// 该 SYM 在 FIELD 中的预分配行范围 `[row_start, row_start + time_count)`。
    pub fn row_range(&self) -> (u32, u32) {
        (self.row_start, self.row_start + self.time_count)
    }

    /// `time_index` 是否落在该 SYM 的 TIME AXIS 区间内。
    pub fn contains_time(&self, time_index: u32) -> bool {
        time_index >= self.time_start && time_index < self.time_start + self.time_count
    }

    /// 逻辑行定位：`global_row = row_start + (time_index - time_start)`。
    pub fn global_row(&self, time_index: u32) -> Option<u32> {
        if self.contains_time(time_index) {
            Some(self.row_start + (time_index - self.time_start))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validity_size;

    #[test]
    fn meta_header_layout_is_64_bytes() {
        assert_eq!(std::mem::size_of::<MetaHeader>(), 64);
        let header = MetaHeader::new(TimeType::TimestampUs, 7, 1000, 8000, 2_000_000, 64 + 8000, 64 + 8000 + 8001, 64 + 8000 + 8001 + 96_000);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), 64);
        // 关键字段 offset
        assert_eq!(u16::from_le_bytes([bytes[8], bytes[9]]), FORMAT_VERSION);
        assert_eq!(bytes[12], TimeType::TimestampUs.id());
        assert_eq!(
            u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            7
        );
        assert_eq!(u32::from_le_bytes(bytes[32..36].try_into().unwrap()), 2_000_000);

        let parsed = MetaHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed, header);
        assert_eq!(parsed.time_type().unwrap(), TimeType::TimestampUs);
        assert_eq!(parsed.time_type_size(), 8);
    }

    #[test]
    fn meta_header_rejects_bad_magic_and_version() {
        let mut header = MetaHeader::new(TimeType::Date32, 0, 0, 0, 0, 64, 64, 64);
        header.magic = 0x1234;
        assert!(matches!(
            MetaHeader::from_bytes(&header.to_bytes()),
            Err(FormatError::BadMagic { .. })
        ));
        header.magic = META_MAGIC;
        header.version = 1;
        assert!(matches!(
            MetaHeader::from_bytes(&header.to_bytes()),
            Err(FormatError::BadVersion { expected: 2, found: 1 })
        ));
    }

    #[test]
    fn sym_index_record_row_mapping() {
        assert_eq!(std::mem::size_of::<SymIndexRecord>(), 12);
        let rec = SymIndexRecord { time_start: 100, time_count: 250, row_start: 7500 };
        // capacity grid：区间内时间点都是有效逻辑行（数据可能为 NULL）
        assert_eq!(rec.global_row(100), Some(7500));
        assert_eq!(rec.global_row(349), Some(7749));
        assert_eq!(rec.global_row(99), None);
        assert_eq!(rec.global_row(350), None);
        assert_eq!(rec.row_range(), (7500, 7750));
        let bytes = rec.to_bytes();
        assert_eq!(SymIndexRecord::from_bytes(&bytes).unwrap(), rec);
    }

    #[test]
    fn validity_helper_agrees_with_format_doc() {
        assert_eq!(validity_size(2_000_000), 250_000);
    }
}
