use splayed_format::{Compression, DataType, Encoding};

use crate::bitpack;
use crate::delta;
use crate::error::CodecError;
use crate::plain;
use crate::rle;

/// 单列值编码（docs/splayed-codec.md §4.1）：只作用于 values，不感知 NULL / validity。
///
/// `values` 必须是 `row_count * data_type.size_of()` 字节的连续数组。
pub fn encode_values(
    encoding: Encoding,
    data_type: DataType,
    values: &[u8],
    row_count: usize,
) -> Result<Vec<u8>, CodecError> {
    let expected = row_count * data_type.size_of();
    if values.len() != expected {
        return Err(CodecError::RowCountMismatch { expected, found: values.len() });
    }
    match encoding {
        Encoding::Plain => plain::encode(values),
        Encoding::Delta => delta::encode(data_type, values),
        Encoding::Rle => rle::encode(data_type, values),
        Encoding::Bitpack => bitpack::encode(data_type, values),
    }
}

/// 单列值解码：产出自有 Buffer（解码必然物化）。
pub fn decode_values(
    encoding: Encoding,
    data_type: DataType,
    payload: &[u8],
    row_count: usize,
) -> Result<Vec<u8>, CodecError> {
    match encoding {
        Encoding::Plain => plain::decode(payload, row_count, data_type.size_of()),
        Encoding::Delta => delta::decode(data_type, payload, row_count),
        Encoding::Rle => rle::decode(data_type, payload, row_count),
        Encoding::Bitpack => bitpack::decode(data_type, payload, row_count),
    }
}

/// 便捷组合：编码 + 压缩（chunk payload 的内层）。
pub fn encode_and_compress(
    encoding: Encoding,
    compression: Compression,
    data_type: DataType,
    values: &[u8],
    row_count: usize,
) -> Result<Vec<u8>, CodecError> {
    let encoded = encode_values(encoding, data_type, values, row_count)?;
    crate::compression::compress(compression, &encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn length_mismatch_is_rejected() {
        let values = vec![0u8; 10];
        assert!(matches!(
            encode_values(Encoding::Plain, DataType::Int32, &values, 3),
            Err(CodecError::RowCountMismatch { expected: 12, found: 10 })
        ));
    }

    #[test]
    fn delta_on_sorted_time_gets_compressible() {
        let times: Vec<u64> = (0..1000).map(|i| 1_700_000_000_000 + i * 60_000_000).collect();
        let encoded =
            encode_values(Encoding::Delta, DataType::TimestampUs, bytemuck::cast_slice(&times), 1000)
                .unwrap();
        let packed = crate::compression::compress(Compression::Zstd, &encoded).unwrap();
        // 差分后全为常量 60_000_000 → ZSTD 后远小于原大小
        assert!(packed.len() < 200);
        let raw = crate::compression::decompress(Compression::Zstd, &packed).unwrap();
        let decoded =
            decode_values(Encoding::Delta, DataType::TimestampUs, &raw, 1000).unwrap();
        assert_eq!(decoded, bytemuck::cast_slice::<u64, u8>(&times));
    }
}
