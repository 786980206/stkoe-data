//! Encoding (PLAIN/DELTA/RLE/BITPACK) and compression (NONE/ZSTD/LZ4) for Splayed V1.
//!
//! - PLAIN + NONE: fastest path, mmap zero-copy (Phase 1)
//! - ZSTD/LZ4 compression: cold data path (Phase 3/6)
//! - DELTA/RLE/BITPACK encodings: column-specific encoding (Phase 6)

pub mod bitpack;
pub mod compact;
pub mod delta;
pub mod plain;
pub mod rle;

pub use compact::{
    compact_field, compact_field_with_encoding, decode_encoding, decompress_field_data,
    encode_encoding, CompactError, DecompressError,
};
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
