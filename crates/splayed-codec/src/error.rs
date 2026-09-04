use splayed_format::FormatError;
use std::fmt;

/// splayed-codec 错误。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    Format(FormatError),
    /// 输入行数与期望不符。
    RowCountMismatch { expected: usize, found: usize },
    /// chunk / payload 被截断。
    Truncated { needed: usize, found: usize },
    /// payload 结构性损坏（长度不自洽、多余尾部等）。
    Corrupt(&'static str),
    /// 后端压缩库错误（ZSTD / LZ4）。
    Io(String),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::Format(e) => write!(f, "format: {e}"),
            CodecError::RowCountMismatch { expected, found } => {
                write!(f, "row count mismatch: expected {expected}, found {found}")
            }
            CodecError::Truncated { needed, found } => {
                write!(f, "truncated input: need {needed} bytes, found {found}")
            }
            CodecError::Corrupt(msg) => write!(f, "corrupt payload: {msg}"),
            CodecError::Io(msg) => write!(f, "compression backend: {msg}"),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<FormatError> for CodecError {
    fn from(e: FormatError) -> Self {
        CodecError::Format(e)
    }
}
