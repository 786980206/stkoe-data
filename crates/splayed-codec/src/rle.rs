//! RLE (Run-Length Encoding): compress runs of repeated values.
//!
//! See `plan.md` §5.5 (Encoding ID=2).
//!
//! RLE is most effective for columns with long runs of the same value
//! (e.g. sparse data, categorical columns with many NULLs).
//!
//! On-disk layout:
//! ```text
//! [u32 count]              ← total number of logical elements (4 bytes)
//! [u32 value_size]         ← size of each value in bytes (4 bytes)
//! [u32 run_count]          ← number of runs (4 bytes)
//! [run entries...]         ← each: [u32 run_length][value_bytes]
//! ```
//!
//! Each run entry stores the length of the run and the repeated value.

use splayed_format::DataType;

use crate::CodecError;

/// RLE-encode a raw data buffer.
///
/// `data` must be a PLAIN-encoded buffer of `count` elements of `data_type`.
/// Returns the RLE-encoded buffer.
pub fn encode(data: &[u8], data_type: DataType, count: usize) -> Result<Vec<u8>, CodecError> {
    let sz = data_type.size_of();
    if data.len() < count * sz {
        return Err(CodecError::InvalidInput);
    }

    // Collect runs.
    let mut runs: Vec<(u32, Vec<u8>)> = Vec::new();
    let mut i = 0;
    while i < count {
        let current = &data[i * sz..(i + 1) * sz];
        let mut run_len = 1u32;
        while (i + run_len as usize) < count {
            let next = &data[(i + run_len as usize) * sz..(i + run_len as usize + 1) * sz];
            if next == current {
                run_len += 1;
            } else {
                break;
            }
        }
        runs.push((run_len, current.to_vec()));
        i += run_len as usize;
    }

    let mut out = Vec::with_capacity(12 + runs.len() * (4 + sz));

    // Header: count + value_size + run_count
    out.extend_from_slice(&(count as u32).to_le_bytes());
    out.extend_from_slice(&(sz as u32).to_le_bytes());
    out.extend_from_slice(&(runs.len() as u32).to_le_bytes());

    // Run entries
    for (run_len, value) in &runs {
        out.extend_from_slice(&run_len.to_le_bytes());
        out.extend_from_slice(value);
    }

    Ok(out)
}

/// RLE-decode a buffer back to raw PLAIN data.
pub fn decode(encoded: &[u8], data_type: DataType) -> Result<Vec<u8>, CodecError> {
    let sz = data_type.size_of();

    if encoded.len() < 12 {
        return Err(CodecError::InvalidInput);
    }

    let count = u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize;
    let val_size = u32::from_le_bytes(encoded[4..8].try_into().unwrap()) as usize;
    let run_count = u32::from_le_bytes(encoded[8..12].try_into().unwrap()) as usize;

    if val_size != sz {
        return Err(CodecError::InvalidInput);
    }

    let mut out = Vec::with_capacity(count * sz);
    let mut offset = 12;
    for _ in 0..run_count {
        if offset + 4 + sz > encoded.len() {
            return Err(CodecError::InvalidInput);
        }
        let run_len = u32::from_le_bytes(encoded[offset..offset + 4].try_into().unwrap());
        let value = &encoded[offset + 4..offset + 4 + sz];
        for _ in 0..run_len {
            out.extend_from_slice(value);
        }
        offset += 4 + sz;
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rle_roundtrip_with_runs() {
        // Values: 5, 5, 5, 10, 10, 5
        let values: Vec<i32> = vec![5, 5, 5, 10, 10, 5];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int32, 6).unwrap();
        let decoded = decode(&encoded, DataType::Int32).unwrap();

        assert_eq!(decoded, raw);
    }

    #[test]
    fn rle_all_same() {
        // 100 identical values — RLE should produce a single run.
        let values: Vec<i64> = vec![42i64; 100];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int64, 100).unwrap();

        // Header (12) + 1 run entry (4 + 8) = 24 bytes vs 800 raw.
        assert_eq!(encoded.len(), 12 + 4 + 8);
        assert!(encoded.len() < raw.len());

        let decoded = decode(&encoded, DataType::Int64).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn rle_no_repeats() {
        // All distinct values — RLE won't help, but must still roundtrip.
        let values: Vec<i32> = (0..10).collect();
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int32, 10).unwrap();
        let decoded = decode(&encoded, DataType::Int32).unwrap();

        assert_eq!(decoded, raw);
        // 10 runs × (4 + 4) + 12 header = 92, vs 40 raw — RLE is larger.
        assert_eq!(encoded.len(), 12 + 10 * (4 + 4));
    }

    #[test]
    fn rle_single_element() {
        let values: Vec<i64> = vec![77];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int64, 1).unwrap();
        let decoded = decode(&encoded, DataType::Int64).unwrap();

        assert_eq!(decoded, raw);
    }
}
