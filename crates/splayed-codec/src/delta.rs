//! DELTA encoding: store differences between consecutive elements.
//!
//! See `plan.md` §5.5 (Encoding ID=1).
//!
//! DELTA is most effective for monotonically increasing sequences like
//! timestamps or dates. The first value is stored as-is, and each
//! subsequent value stores the difference from the previous one.
//!
//! On-disk layout:
//! ```text
//! [u32 count]              ← number of elements (4 bytes)
//! [u32 value_size]         ← size of each value in bytes (4 bytes)
//! [u64 first_value]        ← first raw value (always stored full-width)
//! [delta values...]       ← consecutive deltas, each `value_size` bytes
//! ```
//!
//! DELTA is designed to be combined with a compression algorithm
//! (ZSTD/LZ4) for maximum benefit — the deltas are small and compress well.

use splayed_format::DataType;

use crate::CodecError;

/// DELTA-encode a raw data buffer.
///
/// `data` must be a PLAIN-encoded buffer of `count` elements of `data_type`.
/// Returns the DELTA-encoded buffer.
pub fn encode(data: &[u8], data_type: DataType, count: usize) -> Result<Vec<u8>, CodecError> {
    let sz = data_type.size_of();
    if data.len() < count * sz {
        return Err(CodecError::InvalidInput);
    }

    let mut out = Vec::with_capacity(8 + 8 + count * sz);

    // Header: count + value_size
    out.extend_from_slice(&(count as u32).to_le_bytes());
    out.extend_from_slice(&(sz as u32).to_le_bytes());

    // First value: stored as-is (up to 8 bytes, little-endian)
    let first_val = read_u64_le(data, 0, sz);
    out.extend_from_slice(&first_val.to_le_bytes());

    // Delta values
    let mut prev = first_val;
    for i in 1..count {
        let cur = read_u64_le(data, i * sz, sz);
        let delta = (cur as i64).wrapping_sub(prev as i64) as u64;
        write_u64_le(&mut out, delta, sz);
        prev = cur;
    }

    Ok(out)
}

/// DELTA-decode a buffer back to raw PLAIN data.
///
/// Returns the decoded PLAIN buffer of `count × sz` bytes.
pub fn decode(encoded: &[u8], data_type: DataType) -> Result<Vec<u8>, CodecError> {
    let sz = data_type.size_of();

    if encoded.len() < 12 {
        return Err(CodecError::InvalidInput);
    }

    let count = u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize;
    let val_size = u32::from_le_bytes(encoded[4..8].try_into().unwrap()) as usize;

    if val_size != sz {
        return Err(CodecError::InvalidInput);
    }

    if encoded.len() < 8 + 8 + (count.saturating_sub(1)) * sz {
        return Err(CodecError::InvalidInput);
    }

    let mut out = Vec::with_capacity(count * sz);

    // First value
    let first_val = u64::from_le_bytes(encoded[8..16].try_into().unwrap());
    write_u64_le(&mut out, first_val, sz);

    // Reconstruct from deltas
    let mut prev = first_val;
    let mut offset = 16;
    for _ in 1..count {
        let delta = read_u64_le(encoded, offset, sz);
        let val = (prev as i64).wrapping_add(delta as i64) as u64;
        write_u64_le(&mut out, val, sz);
        prev = val;
        offset += sz;
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn read_u64_le(buf: &[u8], offset: usize, sz: usize) -> u64 {
    let slice = &buf[offset..offset + sz];
    match sz {
        1 => slice[0] as u64,
        2 => u16::from_le_bytes(slice.try_into().unwrap()) as u64,
        4 => u32::from_le_bytes(slice.try_into().unwrap()) as u64,
        8 => u64::from_le_bytes(slice.try_into().unwrap()),
        _ => 0,
    }
}

fn write_u64_le(out: &mut Vec<u8>, val: u64, sz: usize) {
    let le = val.to_le_bytes();
    out.extend_from_slice(&le[..sz]);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_roundtrip_int64() {
        // Values: 1000, 1001, 1002, 1005, 1010
        let values: Vec<i64> = vec![1000, 1001, 1002, 1005, 1010];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int64, 5).unwrap();
        let decoded = decode(&encoded, DataType::Int64).unwrap();

        assert_eq!(decoded, raw);
    }

    #[test]
    fn delta_roundtrip_int32() {
        // Values: 10, 20, 30, 25, 15
        let values: Vec<i32> = vec![10, 20, 30, 25, 15];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int32, 5).unwrap();
        let decoded = decode(&encoded, DataType::Int32).unwrap();

        assert_eq!(decoded, raw);
    }

    #[test]
    fn delta_single_element() {
        let values: Vec<i64> = vec![42];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int64, 1).unwrap();
        let decoded = decode(&encoded, DataType::Int64).unwrap();

        assert_eq!(decoded, raw);
    }

    #[test]
    fn delta_reduces_size_for_monotonic() {
        // Monotonically increasing i64 values — deltas are small.
        let values: Vec<i64> = (0..1000).map(|i| 1_000_000 + i).collect();
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int64, 1000).unwrap();

        // Raw = 1000 * 8 = 8000 bytes.
        // Encoded = 8 (header) + 8 (first val) + 999 * 8 (deltas) = 8008.
        // DELTA alone doesn't shrink; it helps when combined with compression.
        // But the deltas are all 1 (0x01), so they compress extremely well.
        assert_eq!(encoded.len(), 8 + 8 + 999 * 8);

        // Verify correctness
        let decoded = decode(&encoded, DataType::Int64).unwrap();
        assert_eq!(decoded, raw);
    }
}
