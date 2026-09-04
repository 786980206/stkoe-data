use crate::error::FormatError;

/// 数据类型（ID 与单值大小见 docs/splayed-format.md §2）。
/// V2.0 中 NULL 一律由 validity bitmap 表示，与 DataType 无关。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum DataType {
    Bool = 0,
    Int32 = 1,
    Int64 = 2,
    Float32 = 3,
    Float64 = 4,
    Date32 = 5,
    TimestampUs = 6,
    Int8 = 7,
    Int16 = 8,
    UInt8 = 9,
    UInt16 = 10,
    UInt32 = 11,
    UInt64 = 12,
    Date64 = 13,
    /// 变宽 UTF-8 字符串（仅以字典视图承载：keys u32 + offsets + strings）。
    Utf8 = 14,
}

impl DataType {
    pub const fn id(self) -> u8 {
        self as u8
    }

    pub const fn size_of(self) -> usize {
        match self {
            DataType::Bool | DataType::Int8 | DataType::UInt8 => 1,
            DataType::Int16 | DataType::UInt16 => 2,
            DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,
            DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::TimestampUs
            | DataType::Date64 => 8,
            // 变宽类型无固定宽度；字典段以 keys 长度（4B/行）校验
            DataType::Utf8 => 0,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            DataType::Bool => "BOOL",
            DataType::Int32 => "INT32",
            DataType::Int64 => "INT64",
            DataType::Float32 => "FLOAT32",
            DataType::Float64 => "FLOAT64",
            DataType::Date32 => "DATE32",
            DataType::TimestampUs => "TIMESTAMP_US",
            DataType::Int8 => "INT8",
            DataType::Int16 => "INT16",
            DataType::UInt8 => "UINT8",
            DataType::UInt16 => "UINT16",
            DataType::UInt32 => "UINT32",
            DataType::UInt64 => "UINT64",
            DataType::Date64 => "DATE64",
            DataType::Utf8 => "UTF8",
        }
    }

    /// 有符号整数（BITPACK 需要 zigzag；DELTA 按位差分无需区分）。
    pub const fn is_signed_int(self) -> bool {
        matches!(
            self,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Date32
                | DataType::TimestampUs
                | DataType::Date64
        )
    }

    /// 整数族（含 DATE/TIMESTAMP，按整数位型处理；Utf8 不是整数）。
    pub const fn is_integer(self) -> bool {
        !matches!(self, DataType::Float32 | DataType::Float64 | DataType::Utf8)
    }

    pub fn from_id(id: u8) -> Result<Self, FormatError> {
        Self::try_from(id)
    }
}

impl TryFrom<u8> for DataType {
    type Error = FormatError;

    fn try_from(id: u8) -> Result<Self, Self::Error> {
        match id {
            0 => Ok(DataType::Bool),
            1 => Ok(DataType::Int32),
            2 => Ok(DataType::Int64),
            3 => Ok(DataType::Float32),
            4 => Ok(DataType::Float64),
            5 => Ok(DataType::Date32),
            6 => Ok(DataType::TimestampUs),
            7 => Ok(DataType::Int8),
            8 => Ok(DataType::Int16),
            9 => Ok(DataType::UInt8),
            10 => Ok(DataType::UInt16),
            11 => Ok(DataType::UInt32),
            12 => Ok(DataType::UInt64),
            13 => Ok(DataType::Date64),
            14 => Ok(DataType::Utf8),
            other => Err(FormatError::UnknownDataType(other)),
        }
    }
}

/// META header `time_type` 字段：TIME AXIS 的元素类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TimeType {
    Date32 = 0,
    TimestampUs = 1,
}

impl TimeType {
    pub const fn id(self) -> u8 {
        self as u8
    }

    pub const fn size_of(self) -> usize {
        match self {
            TimeType::Date32 => 4,
            TimeType::TimestampUs => 8,
        }
    }

    pub const fn data_type(self) -> DataType {
        match self {
            TimeType::Date32 => DataType::Date32,
            TimeType::TimestampUs => DataType::TimestampUs,
        }
    }

    pub fn from_id(id: u8) -> Result<Self, FormatError> {
        match id {
            0 => Ok(TimeType::Date32),
            1 => Ok(TimeType::TimestampUs),
            other => Err(FormatError::UnknownTimeType(other)),
        }
    }
}

/// Encoding ID（docs/splayed-format.md §5）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
    Plain = 0,
    Delta = 1,
    Rle = 2,
    Bitpack = 3,
}

impl Encoding {
    pub const fn id(self) -> u8 {
        self as u8
    }

    pub fn from_id(id: u8) -> Result<Self, FormatError> {
        match id {
            0 => Ok(Encoding::Plain),
            1 => Ok(Encoding::Delta),
            2 => Ok(Encoding::Rle),
            3 => Ok(Encoding::Bitpack),
            other => Err(FormatError::UnknownEncoding(other)),
        }
    }
}

/// Compression ID（docs/splayed-format.md §6）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Zstd = 1,
    Lz4 = 2,
}

impl Compression {
    pub const fn id(self) -> u8 {
        self as u8
    }

    pub fn from_id(id: u8) -> Result<Self, FormatError> {
        match id {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Zstd),
            2 => Ok(Compression::Lz4),
            other => Err(FormatError::UnknownCompression(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type_ids_and_sizes() {
        let expected: [(u8, DataType, usize); 14] = [
            (0, DataType::Bool, 1),
            (1, DataType::Int32, 4),
            (2, DataType::Int64, 8),
            (3, DataType::Float32, 4),
            (4, DataType::Float64, 8),
            (5, DataType::Date32, 4),
            (6, DataType::TimestampUs, 8),
            (7, DataType::Int8, 1),
            (8, DataType::Int16, 2),
            (9, DataType::UInt8, 1),
            (10, DataType::UInt16, 2),
            (11, DataType::UInt32, 4),
            (12, DataType::UInt64, 8),
            (13, DataType::Date64, 8),
        ];
        for (id, dt, size) in expected {
            assert_eq!(DataType::from_id(id).unwrap(), dt);
            assert_eq!(dt.id(), id);
            assert_eq!(dt.size_of(), size);
        }
        assert_eq!(DataType::from_id(14).unwrap(), DataType::Utf8);
        assert!(matches!(
            DataType::from_id(15),
            Err(FormatError::UnknownDataType(15))
        ));
    }

    #[test]
    fn encoding_compression_time_type_ids() {
        for id in 0..4u8 {
            assert_eq!(Encoding::from_id(id).unwrap().id(), id);
        }
        assert!(Encoding::from_id(4).is_err());
        for id in 0..3u8 {
            assert_eq!(Compression::from_id(id).unwrap().id(), id);
        }
        assert!(Compression::from_id(3).is_err());
        assert_eq!(TimeType::from_id(0).unwrap().size_of(), 4);
        assert_eq!(TimeType::from_id(1).unwrap().size_of(), 8);
        assert!(TimeType::from_id(2).is_err());
    }
}
