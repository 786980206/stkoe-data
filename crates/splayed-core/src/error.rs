use std::fmt;
use std::path::{Path, PathBuf};

use splayed_codec::CodecError;
use splayed_format::FormatError;

/// splayed-core 错误。
#[derive(Debug)]
pub enum CoreError {
    Format(FormatError),
    Codec(CodecError),
    Io(std::io::Error),
    /// 非法参数 / 请求（越界、类型不匹配、名称冲突等）。
    Invalid(String),
    /// 状态不符（如对已压缩 Field 再次压缩、read mode 下写入）。
    InvalidState(String),
    /// 目标不存在。
    NotFound(PathBuf),
    /// 目标已存在（create / rename 的目标冲突）。
    AlreadyExists(PathBuf),
}

impl fmt::Display for CoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoreError::Format(e) => write!(f, "format: {e}"),
            CoreError::Codec(e) => write!(f, "codec: {e}"),
            CoreError::Io(e) => write!(f, "io: {e}"),
            CoreError::Invalid(msg) => write!(f, "invalid: {msg}"),
            CoreError::InvalidState(msg) => write!(f, "invalid state: {msg}"),
            CoreError::NotFound(p) => write!(f, "not found: {}", p.display()),
            CoreError::AlreadyExists(p) => write!(f, "already exists: {}", p.display()),
        }
    }
}

impl std::error::Error for CoreError {}

impl From<FormatError> for CoreError {
    fn from(e: FormatError) -> Self {
        CoreError::Format(e)
    }
}

impl From<CodecError> for CoreError {
    fn from(e: CodecError) -> Self {
        CoreError::Codec(e)
    }
}

impl From<std::io::Error> for CoreError {
    fn from(e: std::io::Error) -> Self {
        CoreError::Io(e)
    }
}

/// 访问模式（open 的 mode 参数）：表示访问意图，不是文件的 compression 状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Read,
    Write,
}

impl Mode {
    pub fn require_write(&self, api: &str) -> Result<(), CoreError> {
        match self {
            Mode::Write => Ok(()),
            Mode::Read => Err(CoreError::InvalidState(format!(
                "{api} requires a write-mode handle"
            ))),
        }
    }
}

pub(crate) fn map_io_path(path: &Path, e: std::io::Error) -> CoreError {
    if e.kind() == std::io::ErrorKind::NotFound {
        CoreError::NotFound(path.to_path_buf())
    } else {
        CoreError::Io(e)
    }
}
