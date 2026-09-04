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

use splayed_format::field_footer;
use splayed_format::{Compression, DataType, Encoding, FieldHeader, HEADER_SIZE};

use crate::CodecError;

/// 编码调度：PLAIN=原样；DELTA/RLE/BITPACK → 各模块 encode。
///
/// 非 PLAIN 编码的数据区布局：`[u64 原始字节数][编码 payload]`（与压缩的
/// `[u64 l][payload]` 同构；编码后再压缩时压缩对象是编码 payload）。
pub fn encode_encoding(
    encoding: Encoding,
    data_type: DataType,
    data: &[u8],
) -> Result<Vec<u8>, CodecError> {
    let count = data.len() / data_type.size_of();
    match encoding {
        Encoding::Plain => Ok(data.to_vec()),
        Encoding::Delta => crate::delta::encode(data, data_type, count),
        Encoding::Rle => crate::rle::encode(data, data_type, count),
        Encoding::Bitpack => crate::bitpack::encode(data, data_type, count),
    }
}

/// 编码解码（读侧恢复原始 PLAIN 布局）。
pub fn decode_encoding(
    encoding: Encoding,
    data_type: DataType,
    encoded: &[u8],
) -> Result<Vec<u8>, CodecError> {
    match encoding {
        Encoding::Plain => Ok(encoded.to_vec()),
        Encoding::Delta => crate::delta::decode(encoded, data_type),
        Encoding::Rle => crate::rle::decode(encoded, data_type),
        Encoding::Bitpack => crate::bitpack::decode(encoded, data_type),
    }
}

/// 编码（可选）+ 压缩（可选），产出 FIELD **数据区**（一步到位）。
///
/// 布局与 `compact_field_with_encoding` 完全一致：
/// - `PLAIN + NONE` → 原样原始数据（无前缀，零拷贝 mmap 快路径）；
/// - 其它 → `[u64 编码后字节数][payload]`，其中 payload = 编码结果
///   （`compression != NONE` 时再压缩该编码结果）。
///
/// 供 `create_field_with_data_encoded` 等「直接写成压缩字段」的入口复用，
/// 保证磁盘布局与既有的压缩/编码路径唯一。
pub fn encode_compress_data(
    encoding: Encoding,
    compression: Compression,
    data_type: DataType,
    data: &[u8],
) -> Result<Vec<u8>, CodecError> {
    if encoding == Encoding::Plain && compression == Compression::None {
        return Ok(data.to_vec());
    }
    let encoded = encode_encoding(encoding, data_type, data)?;
    let payload = if compression == Compression::None {
        encoded.clone()
    } else {
        compress(&encoded, compression)?
    };
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// 编码（可选）+ 压缩（可选）FIELD：`[u64 原始字节数][编码 payload]`，
/// 再对 payload 压缩（`compression != NONE` 时）。
///
/// 原文件必须为 `compression = NONE`；完成后只读（`update_field` 拒绝）。
/// 原子提交（`<field>.tmp` → fsync → rename），按原始数据重算统计 footer。
pub fn compact_field_with_encoding(
    field_path: impl AsRef<Path>,
    encoding: Encoding,
    compression: Compression,
) -> Result<(), CompactError> {
    let field_path = field_path.as_ref();

    // PLAIN 编码 = 既有 compact_field 路径（保持历史格式：NONE 无 raw_len 前缀）。
    if encoding == Encoding::Plain {
        return compact_field(field_path, compression);
    }

    // --- 读当前 header + 原始数据 ---
    let mut file = OpenOptions::new()
        .read(true)
        .open(field_path)
        .map_err(CompactError::Io)?;

    let mut header_buf = [0u8; HEADER_SIZE];
    file.read_exact(&mut header_buf).map_err(CompactError::Io)?;
    let header: FieldHeader = bytemuck::pod_read_unaligned(&header_buf);
    header.validate().map_err(CompactError::Format)?;

    let current_comp = header.compression().map_err(CompactError::Format)?;
    if current_comp != Compression::None {
        return Err(CompactError::AlreadyCompressed);
    }
    let data_type = header.data_type().map_err(CompactError::Format)?;

    let raw_data_len = header.data_length as usize;
    if header.encoding().map_err(CompactError::Format)? != Encoding::Plain {
        return Err(CompactError::Format("already encoded"));
    }

    let mut raw_data = vec![0u8; raw_data_len];
    file.seek(SeekFrom::Start(HEADER_SIZE as u64))
        .map_err(CompactError::Io)?;
    file.read_exact(&mut raw_data).map_err(CompactError::Io)?;
    drop(file); // Windows：先释放再 rename

    // --- 编码 → (可选)压缩 → [u64 编码后字节数][payload] ---
    let new_data = encode_compress_data(encoding, compression, data_type, &raw_data)?;
    let new_data_length = new_data.len() as u64;

    // --- 写 tmp → fsync → 原子 rename ---
    let mut new_header = header;
    new_header.encoding = encoding as u8;
    new_header.compression = compression as u8;
    new_header.data_length = new_data_length;

    let stats = field_footer::compute_stats(data_type, &raw_data);
    let footer = field_footer::encode_footer(stats);

    let tmp_path = field_path.with_extension("tmp");
    {
        let mut tmp = File::create(&tmp_path).map_err(CompactError::Io)?;
        tmp.write_all(bytemuck::bytes_of(&new_header))
            .map_err(CompactError::Io)?;
        tmp.write_all(&new_data).map_err(CompactError::Io)?;
        tmp.write_all(&footer).map_err(CompactError::Io)?;
        tmp.sync_all().map_err(CompactError::Io)?;
    }
    std::fs::rename(&tmp_path, field_path).map_err(CompactError::Io)?;
    Ok(())
}

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

    // compacted 文件只读 → 统计永久有效：按原始数据重算 footer 一起写入。
    let stats = splayed_format::field_footer::compute_stats(
        header.data_type().map_err(CompactError::Format)?,
        &raw_data,
    );
    let footer = splayed_format::field_footer::encode_footer(stats);

    let tmp_path = field_path.with_extension("tmp");
    {
        let mut tmp = File::create(&tmp_path).map_err(CompactError::Io)?;
        tmp.write_all(bytemuck::bytes_of(&new_header))
            .map_err(CompactError::Io)?;
        tmp.write_all(&new_data).map_err(CompactError::Io)?;
        tmp.write_all(&footer).map_err(CompactError::Io)?;
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
