//! BITPACK encoding: pack small integers into fewer bits.
//!
//! See `plan.md` §5.5 (Encoding ID=3).
//!
//! BITPACK is most effective for columns where values fit in fewer bits
//! than the native width (e.g. volumes 0-1000 stored as 10-bit instead of 64-bit).
//!
//! On-disk layout:
//! ```text
//! [u32 count]              ← number of elements (4 bytes)
//! [u32 value_size]        ← original value width in bytes (4 bytes)
//! [u8  bit_width]         ← bits per packed value (1 byte)
//! [u64 min_value]         ← min value (for offset subtraction) (8 bytes)
//! [u32 packed_byte_len]    ← length of packed bit data (4 bytes)
//! [packed bits...]        ← bit-packed values, MSB first per group
//! ```
//!
//! The encoding subtracts `min_value` from each value and packs the
//! resulting non-negative deltas into `bit_width` bits each. This works
//! best for columns with a narrow value range.

use splayed_format::DataType;

use crate::CodecError;

/// BITPACK-encode a raw data buffer.
///
/// `data` must be a PLAIN-encoded buffer of `count` elements of `data_type`.
/// Only integer types (Int32, Int64) are supported.
pub fn encode(data: &[u8], data_type: DataType, count: usize) -> Result<Vec<u8>, CodecError> {
    let sz = data_type.size_of();
    if data.len() < count * sz {
        return Err(CodecError::InvalidInput);
    }
    if !matches!(data_type, DataType::Int32 | DataType::Int64) {
        return Err(CodecError::InvalidInput);
    }

    // Read all values as i64.
    let values: Vec<i64> = (0..count)
        .map(|i| {
            let off = i * sz;
            match sz {
                4 => i32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as i64,
                8 => i64::from_le_bytes(data[off..off + 8].try_into().unwrap()),
                _ => 0,
            }
        })
        .collect();

    if values.is_empty() {
        let mut out = Vec::new();
        out.extend_from_slice(&0u32.to_le_bytes()); // count
        out.extend_from_slice(&(sz as u32).to_le_bytes()); // value_size
        out.push(0); // bit_width
        out.extend_from_slice(&0u64.to_le_bytes()); // min_value
        out.extend_from_slice(&0u32.to_le_bytes()); // packed_byte_len
        return Ok(out);
    }

    // Find min and max to determine bit width.
    let min_val = *values.iter().min().unwrap();
    let max_val = *values.iter().max().unwrap();
    // Use wrapping subtraction to avoid overflow on wide-range i64.
    // Then validate the range fits within 32 bits (BITPACK is not worthwhile beyond that).
    let range = max_val.wrapping_sub(min_val) as u64;
    if range > (1u64 << 32) {
        // Range too wide — BITPACK would use >32 bits, negating any savings.
        return Err(CodecError::InvalidInput);
    }

    // Determine bit width: number of bits to represent `range`.
    let bit_width = if range == 0 {
        1
    } else {
        64 - range.leading_zeros() as u8
    };
    let bit_width = bit_width.clamp(1, 32);

    // Pack values: subtract min, pack into bit_width bits.
    let total_bits = count as u64 * bit_width as u64;
    let packed_byte_len = total_bits.div_ceil(8) as usize;
    let mut packed = vec![0u8; packed_byte_len];

    let mut bit_offset = 0usize;
    for &v in &values {
        let delta = (v - min_val) as u64;
        write_bits(&mut packed, bit_offset, bit_width, delta);
        bit_offset += bit_width as usize;
    }

    // Build output.
    let mut out = Vec::with_capacity(21 + packed_byte_len);
    out.extend_from_slice(&(count as u32).to_le_bytes());
    out.extend_from_slice(&(sz as u32).to_le_bytes());
    out.push(bit_width);
    out.extend_from_slice(&(min_val as u64).to_le_bytes());
    out.extend_from_slice(&(packed_byte_len as u32).to_le_bytes());
    out.extend_from_slice(&packed);

    Ok(out)
}

/// BITPACK-decode a buffer back to raw PLAIN data.
pub fn decode(encoded: &[u8], data_type: DataType) -> Result<Vec<u8>, CodecError> {
    let sz = data_type.size_of();
    if encoded.len() < 21 {
        return Err(CodecError::InvalidInput);
    }

    let count = u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize;
    let val_size = u32::from_le_bytes(encoded[4..8].try_into().unwrap()) as usize;
    let bit_width = encoded[8];
    let min_val = u64::from_le_bytes(encoded[9..17].try_into().unwrap()) as i64;
    let packed_byte_len = u32::from_le_bytes(encoded[17..21].try_into().unwrap()) as usize;

    if val_size != sz {
        return Err(CodecError::InvalidInput);
    }
    if encoded.len() < 21 + packed_byte_len {
        return Err(CodecError::InvalidInput);
    }

    let packed = &encoded[21..21 + packed_byte_len];
    let mut out = Vec::with_capacity(count * sz);

    let mut bit_offset = 0usize;
    for _ in 0..count {
        let delta = read_bits(packed, bit_offset, bit_width);
        let val = min_val + delta as i64;
        match sz {
            4 => out.extend_from_slice(&(val as i32).to_le_bytes()),
            8 => out.extend_from_slice(&val.to_le_bytes()),
            _ => return Err(CodecError::InvalidInput),
        }
        bit_offset += bit_width as usize;
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Bit-level helpers
// ---------------------------------------------------------------------------

/// Write `bit_width` bits of `value` into `buf` at `bit_offset` (LSB-first packing).
fn write_bits(buf: &mut [u8], bit_offset: usize, bit_width: u8, value: u64) {
    if bit_width == 0 {
        return;
    }
    let bw = bit_width as usize;
    for i in 0..bw {
        if (value >> i) & 1 != 0 {
            let abs_bit = bit_offset + i;
            buf[abs_bit / 8] |= 1 << (abs_bit % 8);
        }
    }
}

/// Read `bit_width` bits from `buf` at `bit_offset` (LSB-first packing).
fn read_bits(buf: &[u8], bit_offset: usize, bit_width: u8) -> u64 {
    if bit_width == 0 {
        return 0;
    }
    let bw = bit_width as usize;
    let mut result = 0u64;
    for i in 0..bw {
        let abs_bit = bit_offset + i;
        if abs_bit / 8 < buf.len() && (buf[abs_bit / 8] >> (abs_bit % 8)) & 1 != 0 {
            result |= 1 << i;
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bitpack_roundtrip_int32() {
        // Values 0..100 — fits in 7 bits.
        let values: Vec<i32> = (0..100).collect();
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int32, 100).unwrap();
        let decoded = decode(&encoded, DataType::Int32).unwrap();

        assert_eq!(decoded, raw);
    }

    #[test]
    fn bitpack_roundtrip_int64_small_range() {
        // Values near 1_000_000 with small range — bit_width should be small.
        let values: Vec<i64> = (0..50).map(|i| 1_000_000 + i).collect();
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int64, 50).unwrap();

        // Should use 6 bits per value (range 0..49 → 6 bits).
        // Total packed = 50 * 6 = 300 bits = 38 bytes.
        // vs raw = 50 * 8 = 400 bytes — significant savings.
        assert!(encoded.len() < raw.len());

        let decoded = decode(&encoded, DataType::Int64).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn bitpack_roundtrip_with_negative() {
        // Values: -10..10 — min = -10, range = 20, bit_width = 5.
        let values: Vec<i32> = (-10..10).collect();
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int32, 20).unwrap();
        let decoded = decode(&encoded, DataType::Int32).unwrap();

        assert_eq!(decoded, raw);
    }

    #[test]
    fn bitpack_all_same() {
        let values: Vec<i64> = vec![42i64; 100];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int64, 100).unwrap();
        // bit_width=1, packed = 100 bits = 13 bytes.
        // Total = 21 + 13 = 34 bytes vs 800 raw.
        assert!(encoded.len() < raw.len());

        let decoded = decode(&encoded, DataType::Int64).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn bitpack_single_element() {
        let values: Vec<i32> = vec![42];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let encoded = encode(&raw, DataType::Int32, 1).unwrap();
        let decoded = decode(&encoded, DataType::Int32).unwrap();

        assert_eq!(decoded, raw);
    }

    #[test]
    fn bitpack_rejects_float() {
        let values: Vec<u8> = vec![0; 8];
        let result = encode(&values, DataType::Float64, 1);
        assert!(result.is_err());
    }
}
