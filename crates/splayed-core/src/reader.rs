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

use splayed_format::{Compression, DataType, FieldHeader, RawValue, HEADER_SIZE};

/// The data source backing a `FieldReader`.
/// Either mmap (for PLAIN+NONE) or an owned buffer (for compressed fields).
enum FieldData {
    /// Zero-copy mmap path for `compression = NONE`.
    Mmap(Mmap),
    /// Decompressed data buffer for `compression = ZSTD/LZ4`.
    /// The `Vec<u8>` holds the fully decompressed raw data region.
    Decompressed(Vec<u8>),
}

impl FieldData {
    fn data_slice(&self) -> &[u8] {
        match self {
            FieldData::Mmap(m) => &m[HEADER_SIZE..],
            FieldData::Decompressed(v) => v,
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
}

impl FieldReader {
    /// Open a FIELD file for reading.
    ///
    /// For `compression = NONE`: mmaps the file for zero-copy reads.
    /// For `compression = ZSTD/LZ4`: reads and decompresses the full data region.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReaderError> {
        let file = File::open(path.as_ref()).map_err(ReaderError::Io)?;
        let mmap = unsafe { Mmap::map(&file) }.map_err(ReaderError::Mmap)?;

        if mmap.len() < HEADER_SIZE {
            return Err(ReaderError::TooShort);
        }
        let header_ref: &FieldHeader = bytemuck::from_bytes(&mmap[..HEADER_SIZE]);
        header_ref.validate().map_err(ReaderError::Format)?;
        let header = *header_ref;

        let comp = header.compression().map_err(ReaderError::Format)?;

        let data = if comp == Compression::None {
            // Fast path: mmap zero-copy.
            FieldData::Mmap(mmap)
        } else {
            // Compressed path: decompress into owned buffer.
            // Data region layout: [u64 uncompressed_len][compressed payload]
            let data_region = &mmap[HEADER_SIZE..];
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

            // Sanity check: decompressed length should match header expectation.
            let expected = header.row_count as usize * header.data_type().expect("validated").size_of();
            if decompressed.len() != expected {
                return Err(ReaderError::DecompressLengthMismatch {
                    expected,
                    actual: decompressed.len(),
                });
            }

            FieldData::Decompressed(decompressed)
        };

        Ok(Self { data, header })
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

    /// Read a range of rows into a `Vec<RawValue>` (copy).
    pub fn read_range(&self, start_row: u32, count: usize) -> Result<Vec<RawValue>, ReaderError> {
        let dt = self.data_type();
        let raw = self.read_range_raw(start_row, count)?;
        let mut out = Vec::with_capacity(count);
        let sz = dt.size_of();
        for i in 0..count {
            out.push(RawValue::read_le(raw, i * sz, dt));
        }
        Ok(out)
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
