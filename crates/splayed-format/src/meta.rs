//! META file format: serialization, deserialization, and in-memory structures.
//!
//! See `plan.md` §5.1 for the authoritative layout:
//! ```text
//! [Header 64B]
//! [TIME AXIS:    time_count × (4 or 8) bytes]
//! [SYM DICT INDEX: (sym_count + 1) × 8 bytes]   ← u64 offsets into STRING DATA
//! [SYM STRING DATA: variable]
//! [SYM INDEX:    sym_count × 12 bytes]          ← SymIndexRecord per SYM
//! ```

use crate::header::{MetaHeader, TimeType, HEADER_SIZE};

/// Fixed 12-byte SYM INDEX record (plan §5.1).
///
/// - `time_start`: this SYM's start index in the global TIME AXIS.
/// - `time_count`: number of time points = row capacity (full pre-declaration).
/// - `row_start`: this SYM's start row in all FIELD files
///   = `sum(time_count)` of all preceding SYMs.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SymIndexRecord {
    pub time_start: u32,
    pub time_count: u32,
    pub row_start: u32,
}

const _: () = {
    assert!(std::mem::size_of::<SymIndexRecord>() == 12, "SymIndexRecord must be 12 bytes");
};
unsafe impl bytemuck::Pod for SymIndexRecord {}
unsafe impl bytemuck::Zeroable for SymIndexRecord {}

/// In-memory representation of a fully parsed `.meta` file.
#[derive(Debug, Clone)]
pub struct MetaFile {
    pub header: MetaHeader,
    /// Global deduplicated ascending TIME axis.
    /// DATE32 stored as i32 (days since epoch), TIMESTAMP_US as i64 (microseconds).
    pub time_axis: Vec<i64>,
    /// Symbol strings in ascending order.
    pub symbols: Vec<String>,
    /// One SYM INDEX record per symbol.
    pub sym_index: Vec<SymIndexRecord>,
}

/// Describes how TIME is stored.
pub enum TimeAxisRef<'a> {
    Date32(&'a [i32]),
    TimestampUs(&'a [i64]),
}

impl MetaFile {
    /// The TIME type of this dataset.
    pub fn time_type(&self) -> TimeType {
        self.header.time_type().expect("validated on read")
    }

    /// Size of one TIME element in bytes.
    pub fn time_elem_size(&self) -> usize {
        self.time_type().size_of()
    }

    /// Total rows across all SYMs = `sum(time_count)`.
    /// This is the size every FIELD file must be pre-allocated to.
    pub fn total_rows(&self) -> u32 {
        self.sym_index
            .iter()
            .map(|r| r.time_count)
            .sum()
    }

    /// Look up a symbol's index record by symbol string (binary search).
    /// Returns `Some(idx)` into `sym_index` / `symbols`, or `None`.
    pub fn find_symbol(&self, sym: &str) -> Option<usize> {
        self.symbols.binary_search_by_key(&sym, |s| s.as_str()).ok()
    }

    /// Look up a symbol's record by symbol string.
    pub fn symbol_record(&self, sym: &str) -> Option<&SymIndexRecord> {
        self.find_symbol(sym).map(|i| &self.sym_index[i])
    }

    /// Find the index of `time` in the global TIME AXIS (binary search).
    /// `time` is i64 — for DATE32 it's the day count, for TIMESTAMP_US it's µs.
    pub fn find_time(&self, time: i64) -> Option<usize> {
        self.time_axis.binary_search(&time).ok()
    }

    /// Map a `(sym, time)` pair to a global row number.
    ///
    /// Returns `None` if the symbol or time is not found, or if the time falls
    /// outside the symbol's `[time_start, time_start + time_count)` range.
    pub fn global_row(&self, sym: &str, time: i64) -> Option<u32> {
        let rec = self.symbol_record(sym)?;
        let time_idx = self.find_time(time)?;
        if time_idx < rec.time_start as usize
            || time_idx >= (rec.time_start + rec.time_count) as usize
        {
            return None;
        }
        Some(rec.row_start + (time_idx as u32 - rec.time_start))
    }

    /// Map a `(sym_idx, time_idx)` pair (both direct indices) to a global row.
    /// This is the O(1) path — no string search.
    pub fn global_row_indices(&self, sym_idx: usize, time_idx: usize) -> Option<u32> {
        let rec = self.sym_index.get(sym_idx)?;
        if time_idx < rec.time_start as usize
            || time_idx >= (rec.time_start + rec.time_count) as usize
        {
            return None;
        }
        Some(rec.row_start + (time_idx as u32 - rec.time_start))
    }

    // -- Serialization -------------------------------------------------------

    /// Serialize the entire META file to a byte vector.
    ///
    /// Layout: Header(64) | TIME AXIS | SYM DICT INDEX | SYM STRING DATA | SYM INDEX
    pub fn serialize(&self) -> Vec<u8> {
        let tt = self.time_type();
        let elem_sz = tt.size_of();
        let sym_count = self.symbols.len();

        // Compute section sizes.
        let time_axis_bytes = self.time_axis.len() * elem_sz;
        let sym_dict_index_bytes = (sym_count + 1) * 8;
        // String data: all symbols concatenated (no NUL terminators; offsets delimit).
        let string_data: Vec<u8> = self
            .symbols
            .iter()
            .flat_map(|s| s.as_bytes().iter().copied())
            .collect();
        let string_data_bytes = string_data.len();
        let sym_index_bytes = sym_count * 12;

        // Offsets.
        let time_axis_offset = HEADER_SIZE as u64;
        let sym_dict_offset = time_axis_offset + time_axis_bytes as u64;
        let sym_string_offset = sym_dict_offset + sym_dict_index_bytes as u64;
        let sym_index_offset = sym_string_offset + string_data_bytes as u64;
        let file_size = sym_index_offset + sym_index_bytes as u64;

        let total = file_size as usize;
        let mut buf = vec![0u8; total];

        // --- Header ---
        let mut hdr = self.header;
        hdr.sym_dict_offset = sym_dict_offset;
        hdr.sym_index_offset = sym_index_offset;
        hdr.file_size = file_size;
        buf[..HEADER_SIZE].copy_from_slice(bytemuck::bytes_of(&hdr));

        // --- TIME AXIS ---
        let mut off = HEADER_SIZE;
        match tt {
            TimeType::Date32 => {
                for &t in &self.time_axis {
                    buf[off..off + 4].copy_from_slice(&(t as i32).to_le_bytes());
                    off += 4;
                }
            }
            TimeType::TimestampUs => {
                for &t in &self.time_axis {
                    buf[off..off + 8].copy_from_slice(&t.to_le_bytes());
                    off += 8;
                }
            }
        }
        debug_assert_eq!(off, sym_dict_offset as usize);

        // --- SYM DICT INDEX (sym_count + 1 offsets) ---
        let mut str_off: u64 = 0;
        for s in &self.symbols {
            buf[off..off + 8].copy_from_slice(&str_off.to_le_bytes());
            off += 8;
            str_off += s.len() as u64;
        }
        // Trailing offset = total string data length.
        buf[off..off + 8].copy_from_slice(&str_off.to_le_bytes());
        off += 8;
        debug_assert_eq!(off, sym_string_offset as usize);

        // --- SYM STRING DATA ---
        buf[off..off + string_data_bytes].copy_from_slice(&string_data);
        off += string_data_bytes;
        debug_assert_eq!(off, sym_index_offset as usize);

        // --- SYM INDEX ---
        for rec in &self.sym_index {
            let rec_bytes = bytemuck::bytes_of(rec);
            buf[off..off + 12].copy_from_slice(rec_bytes);
            off += 12;
        }
        debug_assert_eq!(off, total);

        buf
    }

    // -- Deserialization -----------------------------------------------------

    /// Parse a META file from a byte buffer.
    pub fn deserialize(buf: &[u8]) -> Result<Self, MetaError> {
        if buf.len() < HEADER_SIZE {
            return Err(MetaError::TooShort);
        }
        // Copy header into an aligned buffer (Vec<u8> may only be 1-byte aligned).
        let header: MetaHeader = bytemuck::pod_read_unaligned(&buf[..HEADER_SIZE]);
        header.validate().map_err(MetaError::Header)?;

        let tt = header.time_type().map_err(MetaError::Header)?;
        let elem_sz = tt.size_of();
        let time_count = header.time_count as usize;
        let sym_count = header.sym_count as usize;

        // --- TIME AXIS ---
        let time_axis_off = HEADER_SIZE;
        let time_axis_end = time_axis_off + time_count * elem_sz;
        if buf.len() < time_axis_end {
            return Err(MetaError::Truncated);
        }
        let time_axis: Vec<i64> = match tt {
            TimeType::Date32 => (0..time_count)
                .map(|i| {
                    let o = time_axis_off + i * 4;
                    i32::from_le_bytes(buf[o..o + 4].try_into().unwrap()) as i64
                })
                .collect(),
            TimeType::TimestampUs => (0..time_count)
                .map(|i| {
                    let o = time_axis_off + i * 8;
                    i64::from_le_bytes(buf[o..o + 8].try_into().unwrap())
                })
                .collect(),
        };

        // --- SYM DICT INDEX + STRING DATA ---
        let dict_off = header.sym_dict_offset as usize;
        let n_index_entries = sym_count + 1;
        let dict_index_end = dict_off + n_index_entries * 8;
        if buf.len() < dict_index_end {
            return Err(MetaError::Truncated);
        }
        let mut symbols = Vec::with_capacity(sym_count);
        for i in 0..sym_count {
            let start = u64::from_le_bytes(buf[dict_off + i * 8..dict_off + i * 8 + 8].try_into().unwrap()) as usize;
            let end = u64::from_le_bytes(buf[dict_off + (i + 1) * 8..dict_off + (i + 1) * 8 + 8].try_into().unwrap()) as usize;
            let str_start = dict_index_end + start;
            let str_end = dict_index_end + end;
            if str_start > str_end {
                return Err(MetaError::Truncated);
            }
            if str_end > buf.len() {
                return Err(MetaError::Truncated);
            }
            symbols.push(
                std::str::from_utf8(&buf[str_start..str_end])
                    .map_err(MetaError::Utf8)?
                    .to_string(),
            );
        }

        // --- SYM INDEX ---
        let idx_off = header.sym_index_offset as usize;
        let idx_end = idx_off + sym_count * 12;
        if buf.len() < idx_end {
            return Err(MetaError::Truncated);
        }
        let mut sym_index = Vec::with_capacity(sym_count);
        for i in 0..sym_count {
            let o = idx_off + i * 12;
            let rec: SymIndexRecord = bytemuck::pod_read_unaligned(&buf[o..o + 12]);
            sym_index.push(rec);
        }

        // --- file_size check ---
        if header.file_size as usize != buf.len() {
            return Err(MetaError::FileSizeMismatch);
        }

        Ok(Self {
            header,
            time_axis,
            symbols,
            sym_index,
        })
    }
}

/// Builder for constructing a `MetaFile` from raw (SYM, TIME) data.
///
/// This is the core logic shared by `create_meta` / `create_table`.
pub struct MetaBuilder {
    pub time_type: TimeType,
    pub generation: u64,
    /// Pairs of (sym_string, time_value).  `time_value` is i64:
    /// for DATE32 it's the day count, for TIMESTAMP_US it's µs.
    pub pairs: Vec<(String, i64)>,
    pub sorted: bool,
}

impl MetaBuilder {
    pub fn new(time_type: TimeType, generation: u64) -> Self {
        Self {
            time_type,
            generation,
            pairs: Vec::new(),
            sorted: true,
        }
    }

    /// Add a single (sym, time) data point.
    pub fn add(&mut self, sym: &str, time: i64) {
        // Check ordering against the previous element before pushing.
        if let Some((prev_sym, prev_time)) = self.pairs.last() {
            if sym < prev_sym.as_str() || (sym == prev_sym.as_str() && time < *prev_time) {
                self.sorted = false;
            }
        }
        self.pairs.push((sym.to_string(), time));
    }

    /// Build the META file structure.
    ///
    /// Steps (per plan §8.4 `create_meta`):
    /// 1. TIME column deduplicated ascending → global TIME AXIS
    /// 2. SYM column deduplicated ascending → SYM DICT + SYM INDEX
    /// 3. Per SYM: time_start / time_count (continuous interval [min, max])
    /// 4. row_start = cumulative sum of prior time_counts
    pub fn build(mut self) -> Result<MetaFile, MetaError> {
        // Sort by (sym, time) if not already sorted.
        if !self.sorted {
            self.pairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        }

        // 1. TIME AXIS: deduplicated ascending.
        let mut time_axis: Vec<i64> = self.pairs.iter().map(|(_, t)| *t).collect();
        time_axis.sort_unstable();
        time_axis.dedup();
        let time_count = time_axis.len() as u32;

        // Build time → index lookup (O(1) amortized vs per-pair binary search).
        let time_index: std::collections::HashMap<i64, usize> = time_axis
            .iter()
            .copied()
            .enumerate()
            .map(|(i, t)| (t, i))
            .collect();

        // 2. SYM: deduplicated ascending.
        let mut symbols: Vec<String> = self.pairs.iter().map(|(s, _)| s.clone()).collect();
        symbols.sort();
        symbols.dedup();
        let sym_count = symbols.len() as u32;

        // 3. Per SYM: collect time indices, compute time_start / time_count.
        let mut sym_index = Vec::with_capacity(symbols.len());
        let mut row_start: u32 = 0;

        // Group pairs by symbol (data is already sorted by sym).
        let mut pair_idx = 0usize;
        for sym in &symbols {
            // Find the first and last time indices for this symbol.
            // Since pairs are sorted by (sym, time), the first element is the min
            // and the last is the max — no need for a Vec allocation or min/max scan.
            let mut first_ti: Option<usize> = None;
            let mut last_ti: usize = 0;
            while pair_idx < self.pairs.len() && self.pairs[pair_idx].0 == *sym {
                let t = self.pairs[pair_idx].1;
                let ti = time_index[&t];
                if first_ti.is_none() {
                    first_ti = Some(ti);
                }
                last_ti = ti;
                pair_idx += 1;
            }
            let min_ti = first_ti.unwrap();
            let time_start = min_ti as u32;
            let time_count = (last_ti - min_ti + 1) as u32;

            sym_index.push(SymIndexRecord {
                time_start,
                time_count,
                row_start,
            });
            row_start += time_count;
        }

        let header = MetaHeader::new(self.time_type, self.generation, time_count, sym_count);

        Ok(MetaFile {
            header,
            time_axis,
            symbols,
            sym_index,
        })
    }
}

#[derive(Debug)]
pub enum MetaError {
    TooShort,
    Truncated,
    FileSizeMismatch,
    Header(&'static str),
    Utf8(std::str::Utf8Error),
}

impl std::fmt::Display for MetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort => write!(f, "meta file too short for header"),
            Self::Truncated => write!(f, "meta file truncated"),
            Self::FileSizeMismatch => write!(f, "meta file_size does not match actual length"),
            Self::Header(msg) => write!(f, "meta header invalid: {msg}"),
            Self::Utf8(e) => write!(f, "meta symbol utf8 error: {e}"),
        }
    }
}

impl std::error::Error for MetaError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_meta() -> MetaFile {
        let mut b = MetaBuilder::new(TimeType::Date32, 1);
        // SYM01: days 0,1,2
        b.add("SYM01", 0);
        b.add("SYM01", 1);
        b.add("SYM01", 2);
        // SYM02: days 0,1,2
        b.add("SYM02", 0);
        b.add("SYM02", 1);
        b.add("SYM02", 2);
        // SYM03: days 2,3 (time_start=2, time_count=2)
        b.add("SYM03", 2);
        b.add("SYM03", 3);
        b.build().unwrap()
    }

    #[test]
    fn build_correct_sym_index() {
        let meta = make_test_meta();
        assert_eq!(meta.symbols, vec!["SYM01", "SYM02", "SYM03"]);

        // SYM01: time_start=0, time_count=3, row_start=0
        assert_eq!(meta.sym_index[0], SymIndexRecord { time_start: 0, time_count: 3, row_start: 0 });
        // SYM02: time_start=0, time_count=3, row_start=3
        assert_eq!(meta.sym_index[1], SymIndexRecord { time_start: 0, time_count: 3, row_start: 3 });
        // SYM03: time_start=2, time_count=2, row_start=6
        assert_eq!(meta.sym_index[2], SymIndexRecord { time_start: 2, time_count: 2, row_start: 6 });

        assert_eq!(meta.total_rows(), 8); // 3+3+2
    }

    #[test]
    fn global_row_lookup() {
        let meta = make_test_meta();
        // SYM01 day 0 → row 0
        assert_eq!(meta.global_row("SYM01", 0), Some(0));
        // SYM01 day 2 → row 2
        assert_eq!(meta.global_row("SYM01", 2), Some(2));
        // SYM02 day 0 → row 3
        assert_eq!(meta.global_row("SYM02", 0), Some(3));
        // SYM03 day 2 → row 6
        assert_eq!(meta.global_row("SYM03", 2), Some(6));
        // SYM03 day 3 → row 7
        assert_eq!(meta.global_row("SYM03", 3), Some(7));
        // SYM01 day 3 → out of range → None
        assert_eq!(meta.global_row("SYM01", 3), None);
        // Unknown sym → None
        assert_eq!(meta.global_row("SYM99", 0), None);
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let meta = make_test_meta();
        let bytes = meta.serialize();
        let meta2 = MetaFile::deserialize(&bytes).unwrap();

        assert_eq!(meta2.time_type(), TimeType::Date32);
        assert_eq!(meta2.symbols, meta.symbols);
        assert_eq!(meta2.sym_index, meta.sym_index);
        assert_eq!(meta2.time_axis, meta.time_axis);
        assert_eq!(meta2.total_rows(), meta.total_rows());
        assert_eq!(meta2.header.generation, 1);
        assert_eq!(meta2.header.file_size as usize, bytes.len());
    }

    #[test]
    fn unsorted_input_works() {
        let mut b = MetaBuilder::new(TimeType::Date32, 1);
        b.add("SYM02", 2);
        b.add("SYM01", 1);
        b.add("SYM02", 0);
        b.add("SYM01", 0);
        let meta = b.build().unwrap();
        assert_eq!(meta.symbols, vec!["SYM01", "SYM02"]);
        assert_eq!(meta.sym_index[0].time_start, 0);
        assert_eq!(meta.sym_index[0].time_count, 2); // days 0,1
        assert_eq!(meta.sym_index[1].time_start, 0);
        assert_eq!(meta.sym_index[1].time_count, 3); // days 0,2 → interval [0,2]
    }
}
