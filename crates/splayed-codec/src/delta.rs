use crate::error::CodecError;
use splayed_format::DataType;

/// 读取宽度 `s`（1/2/4/8）的 LE 字，零扩展到 u64。
fn read_word(buf: &[u8], index: usize, s: usize) -> u64 {
    let mut word = [0u8; 8];
    word[..s].copy_from_slice(&buf[index * s..index * s + s]);
    u64::from_le_bytes(word)
}

/// DELTA：首值原样存储，其余存相邻差分（native 位宽 wrapping 减法，LE）。
///
/// 位级可逆：对任意 bit 型（含 NULL 未定义位）成立——截断与 wrapping 加减可交换。
pub fn encode(data_type: DataType, values: &[u8]) -> Result<Vec<u8>, CodecError> {
    let s = data_type.size_of();
    debug_assert_eq!(values.len() % s, 0);
    let n = values.len() / s;
    let mut out = Vec::with_capacity(values.len());
    for i in 0..n {
        let word = if i == 0 {
            read_word(values, 0, s)
        } else {
            read_word(values, i, s).wrapping_sub(read_word(values, i - 1, s))
        };
        out.extend_from_slice(&word.to_le_bytes()[..s]);
    }
    Ok(out)
}

pub fn decode(data_type: DataType, payload: &[u8], rows: usize) -> Result<Vec<u8>, CodecError> {
    let s = data_type.size_of();
    let expected = rows * s;
    if payload.len() != expected {
        return Err(CodecError::RowCountMismatch { expected, found: payload.len() });
    }
    let mut out = vec![0u8; expected];
    let mut prev = 0u64;
    for i in 0..rows {
        let delta = read_word(payload, i, s);
        let value = if i == 0 { delta } else { prev.wrapping_add(delta) };
        out[i * s..i * s + s].copy_from_slice(&value.to_le_bytes()[..s]);
        prev = value;
    }
    Ok(out)
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
    fn bit_exact_on_ordered_and_garbage() {
        // 有序时间列
        let times: Vec<u64> = (0..1000).map(|i| 1_700_000_000_000 + i * 60_000_000).collect();
        roundtrip(DataType::TimestampUs, bytemuck::cast_slice(&times));
        // 含负数 / MIN 边界的有符号列
        let ints: Vec<i32> = [i32::MIN, -1, 0, 1, i32::MAX, -12345].to_vec();
        roundtrip(DataType::Int32, bytemuck::cast_slice(&ints));
        // NULL 未定义位（随机 bit 型）
        let garbage: Vec<u8> = (0..64 * 8).map(|i| (i * 91 + 13) as u8).collect();
        roundtrip(DataType::Float64, &garbage);
    }

    #[test]
    fn rejects_bad_length() {
        let payload = vec![0u8; 10];
        assert!(matches!(
            decode(DataType::Int64, &payload, 2),
            Err(CodecError::RowCountMismatch { expected: 16, found: 10 })
        ));
    }
}
