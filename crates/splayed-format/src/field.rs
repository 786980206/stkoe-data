use bytemuck::{Pod, Zeroable};

use crate::error::FormatError;
use crate::types::{Compression, DataType, Encoding};
use crate::{FIELD_MAGIC, FORMAT_VERSION, HEADER_SIZE, validity_size};

pub const FIELD_HEADER_SIZE: usize = HEADER_SIZE;
/// header `flags` bit0：存在 VALIDITY 区。
pub const FIELD_FLAGS_HAS_VALIDITY: u16 = 1 << 0;

/// FIELD header，固定 64 字节（docs/splayed-format.md §8）。
///
/// 布局（offset）：
/// `magic 0 / version 8 / flags 10 / data_type 12 / encoding 13 / compression 14 /
/// reserved 15 / generation 16 / row_count 24 / null_count 28 / reserved 32 / reserved 36 /
/// data_length 40 / validity_offset 48 / reserved 56`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct FieldHeader {
    pub magic: u64,
    pub version: u16,
    pub flags: u16,
    pub data_type: u8,
    pub encoding: u8,
    pub compression: u8,
    pub reserved0: u8,
    pub generation: u64,
    pub row_count: u32,
    pub null_count: u32,
    pub reserved1: u32,
    pub reserved2: u32,
    pub data_length: u64,
    pub validity_offset: u64,
    pub reserved3: u64,
}

impl FieldHeader {
    /// 构建一个 uncompressed（PLAIN + NONE）header。
    pub fn new_uncompressed(
        data_type: DataType,
        generation: u64,
        row_count: u32,
        null_count: u32,
        has_validity: bool,
    ) -> Self {
        let data_length = row_count as u64 * data_type.size_of() as u64;
        let validity_offset = if has_validity { 64 + data_length } else { 0 };
        FieldHeader {
            magic: FIELD_MAGIC,
            version: FORMAT_VERSION,
            flags: if has_validity { FIELD_FLAGS_HAS_VALIDITY } else { 0 },
            data_type: data_type.id(),
            encoding: Encoding::Plain.id(),
            compression: Compression::None.id(),
            reserved0: 0,
            generation,
            row_count,
            null_count,
            reserved1: 0,
            reserved2: 0,
            data_length,
            validity_offset,
            reserved3: 0,
        }
    }

    pub fn to_bytes(&self) -> [u8; FIELD_HEADER_SIZE] {
        bytemuck::bytes_of(self).try_into().expect("FieldHeader is 64 bytes")
    }

    /// 从 64 字节解析并校验 magic / version。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() < FIELD_HEADER_SIZE {
            return Err(FormatError::SizeMismatch {
                expected: FIELD_HEADER_SIZE,
                found: bytes.len(),
            });
        }
        let header: Self = bytemuck::pod_read_unaligned::<Self>(&bytes[..FIELD_HEADER_SIZE]);
        header.validate()?;
        Ok(header)
    }

    pub fn validate(&self) -> Result<(), FormatError> {
        if self.magic != FIELD_MAGIC {
            return Err(FormatError::BadMagic { expected: FIELD_MAGIC, found: self.magic });
        }
        if self.version != FORMAT_VERSION {
            return Err(FormatError::BadVersion { expected: FORMAT_VERSION, found: self.version });
        }
        DataType::from_id(self.data_type)?;
        Encoding::from_id(self.encoding)?;
        Compression::from_id(self.compression)?;
        Ok(())
    }

    pub fn data_type(&self) -> Result<DataType, FormatError> {
        DataType::from_id(self.data_type)
    }

    pub fn encoding(&self) -> Result<Encoding, FormatError> {
        Encoding::from_id(self.encoding)
    }

    pub fn compression(&self) -> Result<Compression, FormatError> {
        Compression::from_id(self.compression)
    }

    pub fn has_validity(&self) -> bool {
        self.flags & FIELD_FLAGS_HAS_VALIDITY != 0
    }

    pub fn set_has_validity(&mut self, has_validity: bool) {
        if has_validity {
            self.flags |= FIELD_FLAGS_HAS_VALIDITY;
        } else {
            self.flags &= !FIELD_FLAGS_HAS_VALIDITY;
        }
    }

    pub fn data_offset(&self) -> u64 {
        64
    }

    /// VALIDITY 区字节数（无 validity 区时为 0）。
    pub fn validity_size(&self) -> usize {
        if self.has_validity() { validity_size(self.row_count) } else { 0 }
    }

    /// 该 header 描述的是否为 compressed 物理表示（chunk 序列）。
    pub fn is_chunked(&self) -> bool {
        self.encoding != Encoding::Plain.id() || self.compression != Compression::None.id()
    }

    /// 该 header 是否表示全 NULL（row_count > 0 且 null_count == row_count）。
    pub fn is_all_null(&self) -> bool {
        self.row_count > 0 && self.null_count == self.row_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_header_layout_is_64_bytes() {
        assert_eq!(std::mem::size_of::<FieldHeader>(), 64);
        let header = FieldHeader::new_uncompressed(DataType::Float64, 42, 250, 3, true);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), 64);
        assert_eq!(
            u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
            250 * 8
        );
        assert_eq!(
            u64::from_le_bytes(bytes[48..56].try_into().unwrap()),
            64 + 250 * 8
        );
        let parsed = FieldHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed, header);
        assert!(parsed.has_validity());
        assert_eq!(parsed.validity_size(), 32);
        assert!(!parsed.is_chunked());
    }

    #[test]
    fn all_valid_header_has_no_validity_region() {
        let header = FieldHeader::new_uncompressed(DataType::Int32, 1, 100, 0, false);
        assert!(!header.has_validity());
        assert_eq!(header.validity_offset, 0);
        assert_eq!(header.validity_size(), 0);
    }

    #[test]
    fn chunked_flag_follows_encoding_or_compression() {
        let mut header = FieldHeader::new_uncompressed(DataType::Int32, 1, 100, 0, false);
        header.encoding = Encoding::Delta.id();
        assert!(header.is_chunked());
        header.encoding = Encoding::Plain.id();
        header.compression = Compression::Zstd.id();
        assert!(header.is_chunked());
        header.compression = Compression::None.id();
        assert!(!header.is_chunked());
    }

    #[test]
    fn rejects_unknown_ids() {
        let mut header = FieldHeader::new_uncompressed(DataType::Int32, 1, 100, 0, false);
        header.data_type = 99;
        assert!(matches!(
            FieldHeader::from_bytes(&header.to_bytes()),
            Err(FormatError::UnknownDataType(99))
        ));
    }
}
