//! FIELD reader: mmap-based zero-copy column reads.
//!
//! See `plan.md` §7 (Reader Design) — the V1 most-important performance path.
//!
//! For `PLAIN + NONE` (the default), reading is:
//! ```text
//! META → row range → mmap FIELD → pointer/slice (no copy)
//! ```
//!
//! For compressed fields (`PLAIN + ZSTD/LZ4`), the full data is decompressed
//! into an in-memory `Vec<u8>` on open. Random access is then served from
//! this buffer. This is the "cold data" path — random access is still O(1)
//! but requires a one-time decompression cost.

use std::fs::File;
use std::path::Path;

use memmap2::Mmap;

use splayed_format::{
    Compression, DataType, Encoding, FieldHeader, RawValue, HEADER_SIZE,
};
use splayed_format::field_footer::{FOOTER_SIZE, parse_footer};

/// 列级统计（来自 FIELD footer；min/max 为原始 LE 槽位，宽度按 `data_type`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldStats {
    pub min: [u8; 8],
    pub max: [u8; 8],
}

/// The data source backing a `FieldReader`.
enum FieldData {
    /// Zero-copy mmap path for `compression = NONE`（PLAIN 编码）。
    /// `data_end` 是数据区的结束下标（footer 之前）。
    Mmap { map: Mmap, data_end: usize },
    /// Decompressed data buffer for `compression = ZSTD/LZ4`（PLAIN 编码）。
    Decompressed(Vec<u8>),
    /// 非 PLAIN 编码（DELTA/RLE/BITPACK）解码后恢复的原始 PLAIN 布局缓冲。
    Decoded(Vec<u8>),
}

impl FieldData {
    fn data_slice(&self) -> &[u8] {
        match self {
            FieldData::Mmap { map, data_end } => &map[HEADER_SIZE..*data_end],
            FieldData::Decompressed(v) => v,
            FieldData::Decoded(v) => v,
        }
    }
}

/// A FIELD file opened for reading.
///
/// For `PLAIN + NONE` fields, the data is memory-mapped for zero-copy access.
/// For compressed fields (`PLAIN + ZSTD/LZ4`), the data is decompressed into
/// an owned `Vec<u8>` on open, after which reads serve from that buffer.
pub struct FieldReader {
    data: FieldData,
    header: FieldHeader,
    stats: Option<FieldStats>,
}

impl FieldReader {
    /// Open a FIELD file for reading.
    ///
    /// For `compression = NONE`: mmaps the file for zero-copy reads.
    /// For `compression = ZSTD/LZ4`: reads and decompresses the full data region.
    ///
    /// Detects an optional stats footer at the file tail (see
    /// [`splayed_format::field_footer`]) and excludes it from the data region.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReaderError> {
        let file = File::open(path.as_ref()).map_err(ReaderError::Io)?;
        let mmap = unsafe { Mmap::map(&file) }.map_err(ReaderError::Mmap)?;

        if mmap.len() < HEADER_SIZE {
            return Err(ReaderError::TooShort);
        }
        let header_ref: &FieldHeader = bytemuck::from_bytes(&mmap[..HEADER_SIZE]);
        header_ref.validate().map_err(ReaderError::Format)?;
        let header = *header_ref;

        // 尾部 footer 检测（统计信息；无 footer 的旧文件不受影响）。
        let (stats, payload_end) = if mmap.len() >= HEADER_SIZE + FOOTER_SIZE {
            let tail = &mmap[mmap.len() - FOOTER_SIZE..];
            match parse_footer(tail) {
                // footer 必须不与数据区重叠（data_length 是纯数据字节数）。
                Some((valid, mn, mx)) if HEADER_SIZE as u64 + header.data_length as u64
                    <= mmap.len() as u64 - FOOTER_SIZE as u64 =>
                {
                    let st = valid.then_some(FieldStats { min: mn, max: mx });
                    (st, mmap.len() - FOOTER_SIZE)
                }
                _ => (None, mmap.len()),
            }
        } else {
            (None, mmap.len())
        };

        let comp = header.compression().map_err(ReaderError::Format)?;
        let encoding = header.encoding().map_err(ReaderError::Format)?;
        let expected =
            header.row_count as usize * header.data_type().expect("validated").size_of();
        let dt = header.data_type().map_err(ReaderError::Format)?;

        let decode_payload =
            |encoded: &[u8]| -> Result<FieldData, ReaderError> {
                let decoded = splayed_codec::decode_encoding(encoding, dt, encoded)
                    .map_err(|_| ReaderError::DecodeFailed)?;
                if decoded.len() != expected {
                    return Err(ReaderError::DecodeLengthMismatch {
                        expected,
                        actual: decoded.len(),
                    });
                }
                Ok(FieldData::Decoded(decoded))
            };

        let data = if comp == Compression::None {
            // Fast path: mmap zero-copy（数据区 = [HEADER_SIZE, payload_end)）。
            let data_end = (HEADER_SIZE as u64 + header.data_length) as usize;
            let data_end = data_end.min(payload_end);
            let mmap_data = FieldData::Mmap { map: mmap, data_end };
            if encoding == Encoding::Plain {
                mmap_data
            } else {
                // 非 PLAIN：数据区 = [u64 编码后字节数][编码 payload]。
                let region = mmap_data.data_slice();
                if region.len() < 8 {
                    return Err(ReaderError::TooShort);
                }
                decode_payload(&region[8..])?
            }
        } else {
            // Compressed path: decompress into owned buffer.
            // Data region layout: [u64 uncompressed_len][compressed payload]
            let data_region = &mmap[HEADER_SIZE..payload_end];
            if data_region.len() < 8 {
                return Err(ReaderError::TooShort);
            }
            let uncompressed_len =
                u64::from_le_bytes(data_region[..8].try_into().unwrap()) as usize;
            let compressed_payload = &data_region[8..];

            let decompressed = match comp {
                Compression::Zstd => {
                    zstd::decode_all(compressed_payload)
                        .map_err(|_| ReaderError::DecompressFailed)?
                }
                Compression::Lz4 => {
                    lz4_flex::decompress(compressed_payload, uncompressed_len)
                        .map_err(|_| ReaderError::DecompressFailed)?
                }
                Compression::None => unreachable!(),
            };

            if encoding == Encoding::Plain {
                // Sanity check: decompressed length should match header expectation.
                if decompressed.len() != expected {
                    return Err(ReaderError::DecompressLengthMismatch {
                        expected,
                        actual: decompressed.len(),
                    });
                }
                FieldData::Decompressed(decompressed)
            } else {
                // 非 PLAIN：解压后的流即「编码 payload」（前缀已作为解压目标
                // 长度被解压层消费），直接解码。
                decode_payload(&decompressed)?
            }
        };

        Ok(Self { data, header, stats })
    }

    /// 统计（min/max），来自文件尾部 footer；无 footer 或已失效 → `None`。
    pub fn stats(&self) -> Option<FieldStats> {
        self.stats
    }

    pub fn header(&self) -> &FieldHeader {
        &self.header
    }

    pub fn data_type(&self) -> DataType {
        self.header.data_type().expect("validated on open")
    }

    pub fn row_count(&self) -> u32 {
        self.header.row_count
    }

    pub fn compression(&self) -> Compression {
        self.header.compression().expect("validated on open")
    }

    /// Get a raw slice over the entire data region (zero-copy for NONE).
    pub fn data_slice(&self) -> &[u8] {
        self.data.data_slice()
    }

    /// Read a single value at `row`.
    ///
    /// This is the hottest path for point lookups. `#[inline(always)]` ensures
    /// the compiler can fuse the bounds check with the load.
    #[inline(always)]
    pub fn read_row(&self, row: u32) -> Result<RawValue, ReaderError> {
        if row >= self.header.row_count {
            return Err(ReaderError::RowOutOfRange {
                row,
                total: self.header.row_count,
            });
        }
        let offset = row as usize * self.data_type().size_of();
        Ok(RawValue::read_le(self.data_slice(), offset, self.data_type()))
    }

    /// Read a range of rows as a raw byte slice.
    ///
    /// Returns `[start_row, start_row + count)` bytes from the data region.
    /// For `compression = NONE` this is zero-copy from the mmap.
    /// For compressed fields it returns a slice from the decompressed buffer.
    ///
    /// On x86_64/ARM64, the compiler generates cache-line-aware loads when
    /// the scanner calls this in a tight loop over consecutive ranges.
    /// The `#[inline(always)]` hint ensures the read is not hidden behind
    /// a function call boundary, allowing LLVM to fuse consecutive loads.
    #[inline(always)]
    pub fn read_range_raw(&self, start_row: u32, count: usize) -> Result<&[u8], ReaderError> {
        let elem_sz = self.data_type().size_of();
        let start_byte = start_row as usize * elem_sz;
        let end_byte = start_byte + count * elem_sz;
        let data = self.data_slice();
        if end_byte > data.len() {
            return Err(ReaderError::RangeOutOfRange {
                start_row,
                count,
                total_rows: self.header.row_count,
            });
        }
        Ok(&data[start_byte..end_byte])
    }

    /// Get a zero-copy `ColumnView` over the entire FIELD data region.
    ///
    /// For `compression = NONE` this borrows from the mmap.
    /// For compressed fields this borrows from the decompressed buffer.
    pub fn as_column_view(&self) -> crate::ColumnView<'_> {
        let dt = self.data_type();
        let data = self.data_slice();
        crate::ColumnView::new(dt, data, self.row_count() as usize)
    }

    /// Get a zero-copy `ColumnView` over a sub-range `[start_row, start_row + count)`.
    pub fn column_view_range(
        &self,
        start_row: u32,
        count: usize,
    ) -> Result<crate::ColumnView<'_>, ReaderError> {
        let dt = self.data_type();
        let raw = self.read_range_raw(start_row, count)?;
        Ok(crate::ColumnView::new(dt, raw, count))
    }
}

#[derive(Debug)]
pub enum ReaderError {
    Io(std::io::Error),
    Mmap(std::io::Error),
    TooShort,
    Format(&'static str),
    DecompressFailed,
    DecompressLengthMismatch { expected: usize, actual: usize },
    DecodeFailed,
    DecodeLengthMismatch { expected: usize, actual: usize },
    RowOutOfRange { row: u32, total: u32 },
    RangeOutOfRange { start_row: u32, count: usize, total_rows: u32 },
}

impl std::fmt::Display for ReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "reader io error: {e}"),
            Self::Mmap(e) => write!(f, "reader mmap error: {e}"),
            Self::TooShort => write!(f, "field file too short for header"),
            Self::Format(msg) => write!(f, "reader format error: {msg}"),
            Self::DecompressFailed => write!(f, "failed to decompress field data"),
            Self::DecodeFailed => write!(f, "failed to decode field data"),
            Self::DecodeLengthMismatch { expected, actual } => write!(
                f,
                "decoded field length {actual} != expected {expected}"
            ),
            Self::DecompressLengthMismatch { expected, actual } => {
                write!(
                    f,
                    "decompressed length mismatch: expected {expected}, got {actual}"
                )
            }
            Self::RowOutOfRange { row, total } => {
                write!(f, "row {row} out of range (total {total} rows)")
            }
            Self::RangeOutOfRange {
                start_row,
                count,
                total_rows,
            } => {
                write!(
                    f,
                    "range [{start_row}, +{count}) out of range (total {total_rows} rows)"
                )
            }
        }
    }
}

impl std::error::Error for ReaderError {}
