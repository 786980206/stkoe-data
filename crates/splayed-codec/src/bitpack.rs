use crate::error::CodecError;
use splayed_format::DataType;

/// 把一个 native 宽度字无损映射到 u64：
/// 无符号 / Bool → 零扩展；有符号 → 符号扩展后 zigzag（小值 → 小 u64）；
/// Float → 原始 bit 型零扩展（仅保证可逆，位宽可能为满宽）。
fn map_to_u64(data_type: DataType, bytes: &[u8]) -> u64 {
    let mut word = [0u8; 8];
    word[..bytes.len()].copy_from_slice(bytes);
    let raw = u64::from_le_bytes(word);
    if data_type.is_signed_int() {
        // 符号扩展：低 s 字节的补码值
        let s = data_type.size_of();
        let shift = 64 - s * 8;
        let signed = ((raw << shift) as i64) >> shift;
        zigzag_encode(signed)
    } else {
        raw
    }
}

fn map_from_u64(data_type: DataType, value: u64, out: &mut [u8]) {
    if data_type.is_signed_int() {
        let signed = zigzag_decode(value);
        out.copy_from_slice(&signed.to_le_bytes()[..out.len()]);
    } else {
        out.copy_from_slice(&value.to_le_bytes()[..out.len()]);
    }
}

fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn zigzag_decode(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

/// BITPACK：块内统计最大位宽，LSB-first 紧密打包。
/// 布局：`[bit_width u8][packed bytes...]`；`bit_width = 0` 表示全 0 值（无 payload）。
pub fn encode(data_type: DataType, values: &[u8]) -> Result<Vec<u8>, CodecError> {
    let s = data_type.size_of();
    debug_assert_eq!(values.len() % s, 0);
    let n = values.len() / s;
    let mut max_bits = 0u32;
    let mut mapped: Vec<u64> = Vec::with_capacity(n);
    for i in 0..n {
        let v = map_to_u64(data_type, &values[i * s..i * s + s]);
        max_bits = max_bits.max(64 - v.leading_zeros());
        mapped.push(v);
    }
    let bit_width = max_bits as u8;
    let byte_len = (n * bit_width as usize + 7) / 8;
    let mut out = Vec::with_capacity(1 + byte_len);
    out.push(bit_width);
    out.resize(1 + byte_len, 0);
    for (i, v) in mapped.iter().enumerate() {
        write_bits(&mut out, 1, i * bit_width as usize, bit_width, *v);
    }
    Ok(out)
}

pub fn decode(data_type: DataType, payload: &[u8], rows: usize) -> Result<Vec<u8>, CodecError> {
    let s = data_type.size_of();
    if payload.is_empty() {
        return Err(CodecError::Truncated { needed: 1, found: 0 });
    }
    let bit_width = payload[0] as usize;
    if bit_width > 64 {
        return Err(CodecError::Corrupt("bit_width > 64"));
    }
    let byte_len = (rows * bit_width + 7) / 8;
    if payload.len() < 1 + byte_len {
        return Err(CodecError::Truncated { needed: 1 + byte_len, found: payload.len() });
    }
    let mut out = vec![0u8; rows * s];
    let mut value_bytes = [0u8; 8];
    for i in 0..rows {
        let v = read_bits(&payload[1..], i * bit_width, bit_width);
        map_from_u64(data_type, v, &mut value_bytes[..s]);
        out[i * s..i * s + s].copy_from_slice(&value_bytes[..s]);
    }
    Ok(out)
}

fn write_bits(buf: &mut [u8], base_byte: usize, bit_pos: usize, width: u8, mut value: u64) {
    if width == 0 {
        return;
    }
    let mut p = base_byte * 8 + bit_pos;
    let mut remaining = width as usize;
    while remaining > 0 {
        let byte = p / 8;
        let bit = p % 8;
        let take = core::cmp::min(8 - bit, remaining);
        let mask = ((1u16 << take) - 1) as u8;
        buf[byte] |= ((value & (mask as u64)) as u8) << bit;
        value >>= take;
        p += take;
        remaining -= take;
    }
}

fn read_bits(buf: &[u8], bit_pos: usize, width: usize) -> u64 {
    if width == 0 {
        return 0;
    }
    let mut p = bit_pos;
    let mut value = 0u64;
    let mut shifted = 0usize;
    while shifted < width {
        let byte = p / 8;
        let bit = p % 8;
        let take = core::cmp::min(8 - bit, width - shifted);
        let chunk = (buf[byte] >> bit) as u64 & ((1u16 << take) - 1) as u64;
        value |= chunk << shifted;
        shifted += take;
        p += take;
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;

    fn roundtrip(dt: DataType, raw: &[u8]) {
        let rows = raw.len() / dt.size_of();
        let encoded = encode(dt, raw).unwrap();
        let decoded = decode(dt, &encoded, rows).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn small_ints_get_narrow_width() {
        let raw: Vec<u8> = (0..=100).collect();
        let encoded = encode(DataType::UInt8, &raw).unwrap();
        assert_eq!(encoded[0], 7); // 100 需要 7 bit
        roundtrip(DataType::UInt8, &raw);
    }

    #[test]
    fn signed_uses_zigzag() {
        let raw: Vec<i32> = (-50..=50).collect();
        let encoded = encode(DataType::Int32, bytemuck::cast_slice(&raw)).unwrap();
        assert!(encoded[0] <= 8);
        roundtrip(DataType::Int32, bytemuck::cast_slice(&raw));
        // MIN / MAX 边界
        let edges: Vec<i64> = [i64::MIN, -1, 0, 1, i64::MAX].to_vec();
        roundtrip(DataType::Int64, bytemuck::cast_slice(&edges));
    }

    #[test]
    fn all_zero_values_have_empty_payload() {
        let raw = vec![0u8; 8 * 500];
        let encoded = encode(DataType::Int64, &raw).unwrap();
        assert_eq!(encoded, vec![0]);
        assert_eq!(decode(DataType::Int64, &encoded, 500).unwrap(), raw);
    }

    #[test]
    fn garbage_bits_roundtrip() {
        let garbage: Vec<u8> = (0..97 * 8).map(|i| (i * 53 + 7) as u8).collect();
        roundtrip(DataType::Float64, &garbage);
        roundtrip(DataType::Bool, &garbage[..97]);
    }

    #[test]
    fn rejects_oversized_width() {
        let mut payload = vec![65u8]; // bit_width = 65
        payload.extend(std::iter::repeat(0u8).take(100));
        assert!(matches!(
            decode(DataType::UInt8, &payload, 10),
            Err(CodecError::Corrupt("bit_width > 64"))
        ));
    }
}
