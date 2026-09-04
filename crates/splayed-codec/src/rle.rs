use crate::error::CodecError;
use splayed_format::DataType;

/// RLE：`(run_length u32 LE, value)` 序列，重复值列友好（docs/splayed-codec.md §5）。
pub fn encode(data_type: DataType, values: &[u8]) -> Result<Vec<u8>, CodecError> {
    let s = data_type.size_of();
    debug_assert_eq!(values.len() % s, 0);
    let n = values.len() / s;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < n {
        let start = i * s;
        let mut run = 1usize;
        while i + run < n && values[(i + run) * s..(i + run) * s + s] == values[start..start + s] {
            run += 1;
        }
        out.extend_from_slice(&(run as u32).to_le_bytes());
        out.extend_from_slice(&values[start..start + s]);
        i += run;
    }
    Ok(out)
}

pub fn decode(data_type: DataType, payload: &[u8], rows: usize) -> Result<Vec<u8>, CodecError> {
    let s = data_type.size_of();
    let mut out: Vec<u8> = Vec::with_capacity(rows * s);
    let mut got = 0usize;
    let mut pos = 0usize;
    while got < rows {
        if pos + 4 + s > payload.len() {
            return Err(CodecError::Truncated { needed: pos + 4 + s, found: payload.len() });
        }
        let run = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        let value = &payload[pos + 4..pos + 4 + s];
        pos += 4 + s;
        got += run;
        if got > rows {
            return Err(CodecError::RowCountMismatch { expected: rows, found: got });
        }
        for _ in 0..run {
            out.extend_from_slice(value);
        }
    }
    if pos != payload.len() {
        return Err(CodecError::Corrupt("trailing bytes after final run"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;

    #[test]
    fn run_length_roundtrip() {
        // 前 5 个相同 + 后续变化
        let mut raw: Vec<u64> = vec![7; 5];
        raw.extend((0..100).map(|i| i * 31));
        raw.push(u64::MAX);
        raw.push(u64::MAX);
        let encoded = encode(DataType::UInt64, bytemuck::cast_slice(&raw)).unwrap();
        let decoded = decode(DataType::UInt64, &encoded, raw.len()).unwrap();
        assert_eq!(bytemuck::cast_slice::<u8, u64>(&decoded), raw.as_slice());
        // 纯重复列压缩收益
        let flat = vec![1u8; 8 * 1000];
        let encoded = encode(DataType::Int64, &flat).unwrap();
        assert_eq!(encoded.len(), 4 + 8);
    }

    #[test]
    fn rejects_row_overflow_and_trailing_bytes() {
        let dt = DataType::UInt8;
        let encoded = encode(dt, &[9, 9, 9]).unwrap();
        assert!(matches!(
            decode(dt, &encoded, 2),
            Err(CodecError::RowCountMismatch { expected: 2, found: 3 })
        ));
        let mut extra = encoded.clone();
        extra.push(0);
        assert!(matches!(decode(dt, &extra, 3), Err(CodecError::Corrupt(_))));
    }
}
