//! `compact_field` — compress a PLAIN+NONE FIELD file into a read-only compressed FIELD.
//!
//! See `plan.md` §8.4 `compact_field`.
//!
//! ## On-disk layout after compaction
//!
//! The FIELD header (64 bytes) is unchanged except `compression` is updated
//! from `NONE` (0) to the target algorithm, and `data_length` reflects the
//! new compressed payload size.
//!
//! The DATA region for a compressed FIELD stores:
//! ```text
//! [u64 uncompressed_length]   ← 8 bytes: original raw data length
//! [compressed payload]        ← zstd-compressed bytes
//! ```
//! This lets the reader decompress in one shot.  Compressed FIELDs are
//! read-only — `update_field` will reject writes when `compression != NONE`.
//!
//! Random single-row access is not supported on compressed fields; they are
//! meant for large-range scans / cold data / archival (plan §5.6).
//!
//! ## Crash safety
//!
//! `compact_field` writes to a temporary file (`<field>.tmp`), fsyncs it,
//! then atomically renames over the original — matching the META commit
//! pattern (plan §6). A crash during compaction leaves the original FIELD
//! intact.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use splayed_format::{Compression, FieldHeader, HEADER_SIZE};

use crate::CodecError;

/// Compress a FIELD file atomically.
///
/// The field must currently be `compression = NONE`.  After compaction the
/// file is read-only (`update_field` will refuse to write).
///
/// Writes to `<field_path>.tmp`, fsyncs, then atomically renames over the
/// original — a crash leaves the original file intact.
pub fn compact_field(
    field_path: impl AsRef<Path>,
    compression: Compression,
) -> Result<(), CompactError> {
    let field_path = field_path.as_ref();

    // --- Read current header + data ---
    let mut file = OpenOptions::new()
        .read(true)
        .open(field_path)
        .map_err(CompactError::Io)?;

    let mut header_buf = [0u8; HEADER_SIZE];
    file.read_exact(&mut header_buf).map_err(CompactError::Io)?;
    let header: FieldHeader = bytemuck::pod_read_unaligned(&header_buf);
    header.validate().map_err(CompactError::Format)?;

    // Must currently be NONE.
    let current_comp = header.compression().map_err(CompactError::Format)?;
    if current_comp != Compression::None {
        return Err(CompactError::AlreadyCompressed);
    }

    // Don't re-compress if target is also NONE (no-op).
    if compression == Compression::None {
        return Ok(());
    }

    let raw_data_len = header.data_length as usize;

    // --- Read raw DATA region ---
    let mut raw_data = vec![0u8; raw_data_len];
    file.seek(SeekFrom::Start(HEADER_SIZE as u64))
        .map_err(CompactError::Io)?;
    file.read_exact(&mut raw_data).map_err(CompactError::Io)?;

    // Close the original file before rename (Windows mmap/lock safety).
    drop(file);

    // --- Compress ---
    let compressed = compress(&raw_data, compression)?;

    // New DATA region: [u64 uncompressed_len] + [compressed bytes]
    let mut new_data = Vec::with_capacity(8 + compressed.len());
    new_data.extend_from_slice(&(raw_data_len as u64).to_le_bytes());
    new_data.extend_from_slice(&compressed);

    let new_data_length = new_data.len() as u64;

    // --- Write to temp file, fsync, then atomic rename ---
    let mut new_header = header;
    new_header.compression = compression as u8;
    new_header.data_length = new_data_length;

    let tmp_path = field_path.with_extension("tmp");
    {
        let mut tmp = File::create(&tmp_path).map_err(CompactError::Io)?;
        tmp.write_all(bytemuck::bytes_of(&new_header))
            .map_err(CompactError::Io)?;
        tmp.write_all(&new_data).map_err(CompactError::Io)?;
        tmp.sync_all().map_err(CompactError::Io)?;
    }

    std::fs::rename(&tmp_path, field_path).map_err(CompactError::Io)?;

    Ok(())
}

/// Decompress a compressed FIELD's data region back to raw bytes.
///
/// Returns the original uncompressed data.  Used by the reader when serving
/// compressed fields (full-scan path only in V1).
pub fn decompress_field_data(
    field_path: impl AsRef<Path>,
) -> Result<Vec<u8>, DecompressError> {
    let field_path = field_path.as_ref();

    let mut file = OpenOptions::new()
        .read(true)
        .open(field_path)
        .map_err(DecompressError::Io)?;

    let mut header_buf = [0u8; HEADER_SIZE];
    file.read_exact(&mut header_buf).map_err(DecompressError::Io)?;
    let header: FieldHeader = bytemuck::pod_read_unaligned(&header_buf);
    header.validate().map_err(DecompressError::Format)?;

    let comp = header.compression().map_err(DecompressError::Format)?;
    if comp == Compression::None {
        // Not compressed — return raw data.
        // Validate data_length against file size to prevent huge allocations.
        let file_size = file
            .metadata()
            .map_err(DecompressError::Io)?
            .len();
        let data_len = header.data_length as usize;
        if HEADER_SIZE as u64 + data_len as u64 > file_size {
            return Err(DecompressError::Format("data_length exceeds file size"));
        }
        let mut data = vec![0u8; data_len];
        file.read_exact(&mut data).map_err(DecompressError::Io)?;
        return Ok(data);
    }

    // Read [u64 uncompressed_len] + [compressed payload].
    // Guard against malformed headers where data_length < 8.
    if header.data_length < 8 {
        return Err(DecompressError::Format(
            "compressed field data_length < 8 (missing uncompressed_len prefix)",
        ));
    }

    let mut len_buf = [0u8; 8];
    file.read_exact(&mut len_buf).map_err(DecompressError::Io)?;
    let uncompressed_len = u64::from_le_bytes(len_buf) as usize;

    let compressed_len = header.data_length as usize - 8;
    let mut compressed = vec![0u8; compressed_len];
    file.read_exact(&mut compressed).map_err(DecompressError::Io)?;

    let decompressed = decompress(&compressed, comp, uncompressed_len)?;
    Ok(decompressed)
}

// ---------------------------------------------------------------------------
// Compression backends
// ---------------------------------------------------------------------------

fn compress(data: &[u8], compression: Compression) -> Result<Vec<u8>, CodecError> {
    match compression {
        Compression::None => Ok(data.to_vec()),
        Compression::Zstd => {
            zstd::encode_all(data, 3).map_err(|_| CodecError::InvalidInput)
        }
        Compression::Lz4 => {
            // Use compress without size prefix — our own [u64 uncompressed_len]
            // header stores the original length.
            Ok(lz4_flex::compress(data))
        }
    }
}

fn decompress(
    data: &[u8],
    compression: Compression,
    expected_len: usize,
) -> Result<Vec<u8>, CodecError> {
    match compression {
        Compression::None => Ok(data.to_vec()),
        Compression::Zstd => {
            zstd::decode_all(data).map_err(|_| CodecError::InvalidInput)
        }
        Compression::Lz4 => {
            // Decompress using the known size from our [u64] header.
            lz4_flex::decompress(data, expected_len)
                .map_err(|_| CodecError::InvalidInput)
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum CompactError {
    Io(std::io::Error),
    Format(&'static str),
    AlreadyCompressed,
    Codec(CodecError),
}

impl std::fmt::Display for CompactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "compact_field io error: {e}"),
            Self::Format(msg) => write!(f, "compact_field format error: {msg}"),
            Self::AlreadyCompressed => write!(f, "field is already compressed"),
            Self::Codec(e) => write!(f, "compact_field codec error: {e}"),
        }
    }
}
impl std::error::Error for CompactError {}

impl From<CodecError> for CompactError {
    fn from(e: CodecError) -> Self {
        Self::Codec(e)
    }
}

#[derive(Debug)]
pub enum DecompressError {
    Io(std::io::Error),
    Format(&'static str),
    Codec(CodecError),
}

impl std::fmt::Display for DecompressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "decompress io error: {e}"),
            Self::Format(msg) => write!(f, "decompress format error: {msg}"),
            Self::Codec(e) => write!(f, "decompress codec error: {e}"),
        }
    }
}
impl std::error::Error for DecompressError {}

impl From<CodecError> for DecompressError {
    fn from(e: CodecError) -> Self {
        Self::Codec(e)
    }
}
