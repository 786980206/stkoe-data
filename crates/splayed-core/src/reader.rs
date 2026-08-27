//! FIELD reader: mmap-based zero-copy column reads.
//!
//! See `plan.md` §7 (Reader Design) — the V1 most-important performance path.
//!
//! For `PLAIN + NONE` (the default), reading is:
//! ```text
//! META → row range → mmap FIELD → pointer/slice (no copy)
//! ```

use std::fs::File;
use std::path::Path;

use memmap2::Mmap;

use splayed_format::{DataType, FieldHeader, RawValue, HEADER_SIZE};

/// An mmap'd FIELD file ready for zero-copy reads.
pub struct FieldReader {
    mmap: Mmap,
    header: FieldHeader,
}

impl FieldReader {
    /// Open and mmap a FIELD file (read-only).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReaderError> {
        let file = File::open(path.as_ref()).map_err(ReaderError::Io)?;
        let mmap = unsafe { Mmap::map(&file) }.map_err(ReaderError::Mmap)?;

        if mmap.len() < HEADER_SIZE {
            return Err(ReaderError::TooShort);
        }
        let header_ref: &FieldHeader = bytemuck::from_bytes(&mmap[..HEADER_SIZE]);
        header_ref.validate().map_err(ReaderError::Format)?;
        // Copy the header out so we can move mmap into the struct.
        let header = *header_ref;

        Ok(Self { mmap, header })
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

    /// Get a raw slice over the entire data region (no copy).
    pub fn data_slice(&self) -> &[u8] {
        &self.mmap[HEADER_SIZE..]
    }

    /// Read a single value at `row` (zero-copy into a stack RawValue).
    pub fn read_row(&self, row: u32) -> Result<RawValue, ReaderError> {
        if row >= self.header.row_count {
            return Err(ReaderError::RowOutOfRange {
                row,
                total: self.header.row_count,
            });
        }
        let offset = HEADER_SIZE + row as usize * self.data_type().size_of();
        Ok(RawValue::read_le(&self.mmap, offset, self.data_type()))
    }

    /// Read a range of rows as a raw byte slice (zero-copy).
    ///
    /// Returns `[start_row, start_row + count)` bytes from the DATA region.
    pub fn read_range_raw(&self, start_row: u32, count: usize) -> Result<&[u8], ReaderError> {
        let elem_sz = self.data_type().size_of();
        let start_byte = HEADER_SIZE + start_row as usize * elem_sz;
        let end_byte = start_byte + count * elem_sz;
        if end_byte > self.mmap.len() {
            return Err(ReaderError::RangeOutOfRange {
                start_row,
                count,
                total_rows: self.header.row_count,
            });
        }
        Ok(&self.mmap[start_byte..end_byte])
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
    /// This is the primary read path for the Scanner and Arrow conversion.
    /// The view borrows from this reader's mmap.
    pub fn as_column_view(&self) -> crate::ColumnView<'_> {
        crate::ColumnView::from_field_data(self.data_type(), &self.mmap, self.row_count() as usize)
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
            Self::RowOutOfRange { row, total } => {
                write!(f, "row {row} out of range (total {total})")
            }
            Self::RangeOutOfRange {
                start_row,
                count,
                total_rows,
            } => {
                write!(f, "range [{start_row}, +{count}) out of range (total {total_rows} rows)")
            }
        }
    }
}

impl std::error::Error for ReaderError {}
