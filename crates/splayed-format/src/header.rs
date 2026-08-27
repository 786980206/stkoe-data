//! Shared header structures: encoding/compression enums, magic constants,
//! and the fixed 64-byte headers for META and FIELD files.
//!
//! See `plan.md` §5.1 (META Header) and §5.2 (FIELD Header).

use crate::DataType;

// ---------------------------------------------------------------------------
// Magic numbers (stored as the first 8 bytes of each file).
// ---------------------------------------------------------------------------

/// META file magic: `SPLAYMTA` as a little-endian u64.
pub const META_MAGIC: u64 = u64::from_le_bytes(*b"SPLAYMTA");

/// FIELD file magic: `SPLAYFLD` as a little-endian u64.
pub const FIELD_MAGIC: u64 = u64::from_le_bytes(*b"SPLAYFLD");

/// Current format version for both META and FIELD headers.
pub const FORMAT_VERSION: u16 = 1;

/// The filename of the metadata file inside a dataset directory.
pub const META_FILE_NAME: &str = ".meta";

/// Fixed header size in bytes for both file types.
pub const HEADER_SIZE: usize = 64;

/// Data starts immediately after the header.
pub const DATA_OFFSET: u64 = 64;

// ---------------------------------------------------------------------------
// Encoding & Compression enums (stored as single bytes in FIELD header).
// ---------------------------------------------------------------------------

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Plain = 0,
    Delta = 1,
    Rle = 2,
    Bitpack = 3,
}

impl Encoding {
    pub fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(Self::Plain),
            1 => Some(Self::Delta),
            2 => Some(Self::Rle),
            3 => Some(Self::Bitpack),
            _ => None,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None = 0,
    Zstd = 1,
    Lz4 = 2,
}

impl Compression {
    pub fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(Self::None),
            1 => Some(Self::Zstd),
            2 => Some(Self::Lz4),
            _ => None,
        }
    }

    /// Only `None` supports in-place random update.
    #[inline]
    pub const fn is_writable(self) -> bool {
        matches!(self, Self::None)
    }
}

// ---------------------------------------------------------------------------
// Time type enum (stored as 1 byte in META header `time_type`).
// ---------------------------------------------------------------------------

/// Which TIME type the dataset uses — a binary choice per plan §5.3.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeType {
    Date32 = 0,
    TimestampUs = 1,
}

impl TimeType {
    pub fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(Self::Date32),
            1 => Some(Self::TimestampUs),
            _ => None,
        }
    }

    #[inline]
    pub const fn size_of(self) -> usize {
        match self {
            Self::Date32 => 4,
            Self::TimestampUs => 8,
        }
    }

    #[inline]
    pub const fn as_data_type(self) -> DataType {
        match self {
            Self::Date32 => DataType::Date32,
            Self::TimestampUs => DataType::TimestampUs,
        }
    }
}

// ---------------------------------------------------------------------------
// Raw header structs — these are laid out exactly as on disk.
// All multi-byte fields are little-endian.
// ---------------------------------------------------------------------------

/// META header (fixed 64 bytes).  See plan §5.1.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MetaHeader {
    pub magic: u64,            // 0
    pub version: u16,          // 8
    pub flags: u16,            // 10
    pub time_type: u8,         // 12
    pub reserved: [u8; 3],     // 13
    pub generation: u64,       // 16
    pub time_count: u32,       // 24
    pub sym_count: u32,        // 28
    pub sym_dict_offset: u64,  // 32
    pub sym_index_offset: u64, // 40
    pub file_size: u64,        // 48
                              // 56..64 padding (8 bytes)
    pub _pad: [u8; 8],
}

/// FIELD header (fixed 64 bytes).  See plan §5.2.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FieldHeader {
    pub magic: u64,          // 0
    pub version: u16,        // 8
    pub flags: u16,          // 10
    pub data_type: u8,       // 12
    pub encoding: u8,        // 13
    pub compression: u8,     // 14
    pub reserved: u8,        // 15
    pub generation: u64,     // 16
    pub row_count: u32,      // 24
    pub null_count: u32,     // 28
    pub reserved2: u32,      // 32
    pub reserved3: u32,      // 36
    pub data_length: u64,    // 40
    pub reserved4: [u8; 16], // 48
}

// Compile-time assertions that the structs are exactly 64 bytes.
const _: () = {
    assert!(
        std::mem::size_of::<MetaHeader>() == HEADER_SIZE,
        "MetaHeader must be 64 bytes"
    );
    assert!(
        std::mem::size_of::<FieldHeader>() == HEADER_SIZE,
        "FieldHeader must be 64 bytes"
    );
};

impl MetaHeader {
    /// Create a new META header with default fields.
    pub fn new(time_type: TimeType, generation: u64, time_count: u32, sym_count: u32) -> Self {
        Self {
            magic: META_MAGIC,
            version: FORMAT_VERSION,
            flags: 0,
            time_type: time_type as u8,
            reserved: [0; 3],
            generation,
            time_count,
            sym_count,
            sym_dict_offset: 0, // filled during serialization
            sym_index_offset: 0, // filled during serialization
            file_size: 0,        // filled during serialization
            _pad: [0; 8],
        }
    }

    /// Validate magic and version.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.magic != META_MAGIC {
            return Err("invalid META magic");
        }
        if self.version != FORMAT_VERSION {
            return Err("unsupported META version");
        }
        Ok(())
    }

    pub fn time_type(&self) -> Result<TimeType, &'static str> {
        TimeType::from_id(self.time_type).ok_or("invalid time_type")
    }
}

impl FieldHeader {
    /// Create a new FIELD header.
    pub fn new(
        data_type: DataType,
        encoding: Encoding,
        compression: Compression,
        generation: u64,
        row_count: u32,
        data_length: u64,
    ) -> Self {
        Self {
            magic: FIELD_MAGIC,
            version: FORMAT_VERSION,
            flags: 0,
            data_type: data_type as u8,
            encoding: encoding as u8,
            compression: compression as u8,
            reserved: 0,
            generation,
            row_count,
            null_count: row_count, // initially all NULL
            reserved2: 0,
            reserved3: 0,
            data_length,
            reserved4: [0; 16],
        }
    }

    /// Validate magic and version.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.magic != FIELD_MAGIC {
            return Err("invalid FIELD magic");
        }
        if self.version != FORMAT_VERSION {
            return Err("unsupported FIELD version");
        }
        Ok(())
    }

    pub fn data_type(&self) -> Result<DataType, &'static str> {
        DataType::from_id(self.data_type).ok_or("invalid data_type")
    }

    pub fn encoding(&self) -> Result<Encoding, &'static str> {
        Encoding::from_id(self.encoding).ok_or("invalid encoding")
    }

    pub fn compression(&self) -> Result<Compression, &'static str> {
        Compression::from_id(self.compression).ok_or("invalid compression")
    }
}

// Both headers are POD — safe to reinterpret as raw bytes.
unsafe impl bytemuck::Pod for MetaHeader {}
unsafe impl bytemuck::Zeroable for MetaHeader {}
unsafe impl bytemuck::Pod for FieldHeader {}
unsafe impl bytemuck::Zeroable for FieldHeader {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_sizes() {
        assert_eq!(std::mem::size_of::<MetaHeader>(), 64);
        assert_eq!(std::mem::size_of::<FieldHeader>(), 64);
    }

    #[test]
    fn meta_header_roundtrip() {
        let h = MetaHeader::new(TimeType::Date32, 42, 100, 5);
        let bytes = bytemuck::bytes_of(&h);
        assert_eq!(bytes.len(), 64);
        let back: &MetaHeader = bytemuck::from_bytes(bytes);
        assert_eq!(back.magic, META_MAGIC);
        assert_eq!(back.generation, 42);
        assert_eq!(back.time_count, 100);
        assert_eq!(back.sym_count, 5);
        assert!(back.validate().is_ok());
        assert_eq!(back.time_type().unwrap(), TimeType::Date32);
    }

    #[test]
    fn field_header_roundtrip() {
        let h = FieldHeader::new(DataType::Float64, Encoding::Plain, Compression::None, 7, 1000, 8000);
        let bytes = bytemuck::bytes_of(&h);
        assert_eq!(bytes.len(), 64);
        let back: &FieldHeader = bytemuck::from_bytes(bytes);
        assert!(back.validate().is_ok());
        assert_eq!(back.data_type().unwrap(), DataType::Float64);
        assert_eq!(back.encoding().unwrap(), Encoding::Plain);
        assert_eq!(back.compression().unwrap(), Compression::None);
        assert_eq!(back.row_count, 1000);
        assert_eq!(back.data_length, 8000);
    }
}
