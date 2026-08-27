//! Encoding (PLAIN/DELTA/RLE/BITPACK) and compression (NONE/ZSTD/LZ4) for Splayed V1.
//!
//! Phase 1 implements PLAIN + NONE.  `compact_field` (ZSTD) is implemented
//! for Phase 3's function interface.  LZ4, DELTA, RLE, BITPACK are stubbed
//! for later phases.

pub mod compact;
pub mod plain;

pub use compact::{compact_field, decompress_field_data, CompactError, DecompressError};
pub use plain::PlainCodec;

/// Encoding result type.
pub type CodecResult<T> = std::result::Result<T, CodecError>;

#[derive(Debug)]
pub enum CodecError {
    UnsupportedEncoding,
    UnsupportedCompression,
    InvalidInput,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedEncoding => write!(f, "unsupported encoding"),
            Self::UnsupportedCompression => write!(f, "unsupported compression"),
            Self::InvalidInput => write!(f, "invalid input for codec"),
        }
    }
}

impl std::error::Error for CodecError {}
