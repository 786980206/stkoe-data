//! `ColumnView` — a zero-copy column view over raw FIELD data.
//!
//! See `plan.md` §10.2 (Native ColumnView).
//!
//! This is the **core read interface** that simultaneously serves Arrow and
//! (future) DuckDB without forcing either format on the core layer.
//!
//! ```text
//! ColumnView
//! ├── data_type
//! ├── raw bytes (ptr/slice)
//! ├── length (rows)
//! └── null information
//! ```
//!
//! For `PLAIN + NONE`, the raw bytes are a direct slice into the mmap'd FIELD
//! data region — no allocation, no copy.  NULL detection is via sentinel bit
//! pattern comparison, not a validity bitmap.

use splayed_format::{DataType, RawValue, HEADER_SIZE};

/// A zero-copy view over a contiguous slice of FIELD data.
///
/// The underlying `bytes` slice is borrowed from the mmap (or a decompressed
/// buffer for compressed fields).  Each element is `data_type.size_of()` bytes
/// wide.  NULL is determined by comparing each element's bit pattern against
/// the type's sentinel — there is no separate validity bitmap.
pub struct ColumnView<'a> {
    pub data_type: DataType,
    pub bytes: &'a [u8],
    pub row_count: usize,
}

impl<'a> ColumnView<'a> {
    /// Create a view from a raw byte slice and data type.
    /// The slice must contain exactly `row_count × size_of(type)` bytes.
    pub fn new(data_type: DataType, bytes: &'a [u8], row_count: usize) -> Self {
        debug_assert_eq!(
            bytes.len(),
            row_count * data_type.size_of(),
            "ColumnView byte slice length must match row_count × size_of"
        );
        Self {
            data_type,
            bytes,
            row_count,
        }
    }

    /// Create a view from a `FieldReader`'s data slice (the mmap region
    /// after the 64-byte header).
    ///
    /// This is the zero-copy path for `PLAIN + NONE`.
    pub fn from_field_data(data_type: DataType, mmap: &'a [u8], row_count: usize) -> Self {
        let bytes = &mmap[HEADER_SIZE..HEADER_SIZE + row_count * data_type.size_of()];
        Self::new(data_type, bytes, row_count)
    }

    /// Get the raw byte slice for a single row (zero-copy).
    #[inline]
    pub fn row_bytes(&self, row: usize) -> Option<&'a [u8]> {
        if row >= self.row_count {
            return None;
        }
        let sz = self.data_type.size_of();
        let off = row * sz;
        Some(&self.bytes[off..off + sz])
    }

    /// Read a single value as `RawValue` (zero-copy into a stack value).
    #[inline]
    pub fn get(&self, row: usize) -> Option<RawValue> {
        self.row_bytes(row)
            .map(|b| RawValue::read_le(b, 0, self.data_type))
    }

    /// Is the value at `row` NULL?  Compares the bit pattern against the sentinel.
    #[inline]
    pub fn is_null(&self, row: usize) -> bool {
        match self.row_bytes(row) {
            Some(b) => {
                let null_pat = self.data_type.null_bytes();
                b == null_pat
            }
            None => true, // out-of-range treated as NULL
        }
    }

    /// Count the number of NULL values in this view.
    pub fn null_count(&self) -> usize {
        (0..self.row_count).filter(|&i| self.is_null(i)).count()
    }

    /// Count the number of non-NULL values.
    #[inline]
    pub fn valid_count(&self) -> usize {
        self.row_count - self.null_count()
    }

    /// Iterator over all (row_index, RawValue) pairs.
    pub fn iter(&self) -> ColumnViewIter<'_> {
        ColumnViewIter {
            view: self,
            row: 0,
        }
    }
}

/// Iterator over a `ColumnView`, yielding `(row_index, RawValue)` pairs.
pub struct ColumnViewIter<'a> {
    view: &'a ColumnView<'a>,
    row: usize,
}

impl<'a> Iterator for ColumnViewIter<'a> {
    type Item = (usize, RawValue);

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.view.row_count {
            return None;
        }
        let val = self.view.get(self.row)?;
        let idx = self.row;
        self.row += 1;
        Some((idx, val))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_view_basic() {
        // 3 Float64 values: 1.0, NULL (canonical NaN), 3.0
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1.0f64.to_le_bytes());
        // canonical NaN = 0x7FF8000000000000
        bytes.extend_from_slice(&0x7FF8000000000000u64.to_le_bytes());
        bytes.extend_from_slice(&3.0f64.to_le_bytes());

        let view = ColumnView::new(DataType::Float64, &bytes, 3);

        assert_eq!(view.row_count, 3);
        assert_eq!(view.get(0).unwrap().as_f64(), Some(1.0));
        assert!(view.is_null(1));
        assert_eq!(view.get(1).unwrap().as_f64(), None);
        assert_eq!(view.get(2).unwrap().as_f64(), Some(3.0));

        assert_eq!(view.null_count(), 1);
        assert_eq!(view.valid_count(), 2);
    }

    #[test]
    fn column_view_iter() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&10i32.to_le_bytes());
        // INT32 NULL = 0x80000000
        bytes.extend_from_slice(&0x80000000u32.to_le_bytes());
        bytes.extend_from_slice(&30i32.to_le_bytes());

        let view = ColumnView::new(DataType::Int32, &bytes, 3);
        let values: Vec<_> = view.iter().collect();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].1.as_i32(), Some(10));
        assert_eq!(values[1].1.as_i32(), None); // NULL
        assert_eq!(values[2].1.as_i32(), Some(30));
    }

    #[test]
    fn column_view_int_null() {
        // INT32 NULL = 0x80000000
        let bytes = 0x80000000u32.to_le_bytes();
        let view = ColumnView::new(DataType::Int32, &bytes, 1);
        assert!(view.is_null(0));
        assert_eq!(view.get(0).unwrap().as_i32(), None);
        assert_eq!(view.null_count(), 1);
    }
}
