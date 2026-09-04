use std::fmt;

/// splayed-format 错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    BadMagic { expected: u64, found: u64 },
    BadVersion { expected: u16, found: u16 },
    UnknownDataType(u8),
    UnknownTimeType(u8),
    UnknownEncoding(u8),
    UnknownCompression(u8),
    SizeMismatch { expected: usize, found: usize },
    Alignment { required: usize, found: usize },
    InvalidLayout(String),
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FormatError::BadMagic { expected, found } => {
                write!(f, "bad magic: expected {expected:#x}, found {found:#x}")
            }
            FormatError::BadVersion { expected, found } => {
                write!(f, "bad version: expected {expected}, found {found}")
            }
            FormatError::UnknownDataType(id) => write!(f, "unknown data type id {id}"),
            FormatError::UnknownTimeType(id) => write!(f, "unknown time type id {id}"),
            FormatError::UnknownEncoding(id) => write!(f, "unknown encoding id {id}"),
            FormatError::UnknownCompression(id) => write!(f, "unknown compression id {id}"),
            FormatError::SizeMismatch { expected, found } => {
                write!(f, "size mismatch: expected {expected}, found {found}")
            }
            FormatError::Alignment { required, found } => {
                write!(f, "misaligned: required {required}-byte alignment, found {found}")
            }
            FormatError::InvalidLayout(msg) => write!(f, "invalid layout: {msg}"),
        }
    }
}

impl std::error::Error for FormatError {}
