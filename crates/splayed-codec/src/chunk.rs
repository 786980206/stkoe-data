use splayed_format::{validity_size, Compression, DataType, Encoding};

use crate::compression::{compress, decompress};
use crate::encoding::{decode_values, encode_values};
use crate::error::CodecError;

/// chunk 明文头大小：`rows` / `values_len` / `payload_len`，各 u32 LE。
pub const CHUNK_HEADER_SIZE: usize = 12;

/// chunk 明文头（docs/splayed-codec.md §3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkHeader {
    pub rows: u32,
    pub values_len: u32,
    pub payload_len: u32,
}

impl ChunkHeader {
    pub fn to_bytes(&self) -> [u8; CHUNK_HEADER_SIZE] {
        let mut out = [0u8; CHUNK_HEADER_SIZE];
        out[0..4].copy_from_slice(&self.rows.to_le_bytes());
        out[4..8].copy_from_slice(&self.values_len.to_le_bytes());
        out[8..12].copy_from_slice(&self.payload_len.to_le_bytes());
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() < CHUNK_HEADER_SIZE {
            return Err(CodecError::Truncated { needed: CHUNK_HEADER_SIZE, found: bytes.len() });
        }
        Ok(ChunkHeader {
            rows: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            values_len: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            payload_len: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
        })
    }
}

/// 编码一个 chunk：
///
/// `payload = compression( encode_encoding(values) || validity_bits )`
///
/// `validity = None` 表示该 chunk 全部有效（payload 中不拼接 validity 位）。
/// 返回完整 chunk 字节（12B 明文头 + payload）。
pub fn encode_chunk(
    encoding: Encoding,
    compression: Compression,
    data_type: DataType,
    values: &[u8],
    validity: Option<&[u8]>,
    rows: usize,
) -> Result<Vec<u8>, CodecError> {
    let expected = rows * data_type.size_of();
    if values.len() != expected {
        return Err(CodecError::RowCountMismatch { expected, found: values.len() });
    }
    if let Some(bits) = validity {
        if bits.len() != validity_size(rows as u32) {
            return Err(CodecError::Corrupt("validity byte length does not match rows"));
        }
    }
    let mut raw = encode_values(encoding, data_type, values, rows)?;
    if let Some(bits) = validity {
        raw.extend_from_slice(bits);
    }
    let payload = compress(compression, &raw)?;
    let header = ChunkHeader {
        rows: rows as u32,
        values_len: (raw.len() - validity.map_or(0, |v| v.len())) as u32,
        payload_len: payload.len() as u32,
    };
    let mut out = Vec::with_capacity(CHUNK_HEADER_SIZE + payload.len());
    out.extend_from_slice(&header.to_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// 解码一个 chunk：返回 `(values, validity, rows)`；`validity = None` 表示全有效。
pub fn decode_chunk(
    encoding: Encoding,
    compression: Compression,
    data_type: DataType,
    chunk: &[u8],
) -> Result<(Vec<u8>, Option<Vec<u8>>, usize), CodecError> {
    let header = ChunkHeader::from_bytes(chunk)?;
    let rows = header.rows as usize;
    let payload_len = header.payload_len as usize;
    if chunk.len() < CHUNK_HEADER_SIZE + payload_len {
        return Err(CodecError::Truncated {
            needed: CHUNK_HEADER_SIZE + payload_len,
            found: chunk.len(),
        });
    }
    let raw = decompress(compression, &chunk[CHUNK_HEADER_SIZE..CHUNK_HEADER_SIZE + payload_len])?;
    let values_len = header.values_len as usize;
    if raw.len() < values_len {
        return Err(CodecError::Corrupt("decompressed payload shorter than values_len"));
    }
    let values = decode_values(encoding, data_type, &raw[..values_len], rows)?;
    let bits_len = raw.len() - values_len;
    let validity = match bits_len {
        0 => None,
        n if n == validity_size(header.rows) => Some(raw[values_len..].to_vec()),
        _ => {
            return Err(CodecError::Corrupt("validity bit length does not match rows"));
        }
    };
    Ok((values, validity, rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip_and_truncation() {
        let header = ChunkHeader { rows: 8192, values_len: 65_536, payload_len: 12_345 };
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), 12);
        assert_eq!(ChunkHeader::from_bytes(&bytes).unwrap(), header);
        assert!(matches!(
            ChunkHeader::from_bytes(&bytes[..8]),
            Err(CodecError::Truncated { needed: 12, found: 8 })
        ));
    }

    #[test]
    fn encode_rejects_value_length_mismatch() {
        assert!(matches!(
            encode_chunk(Encoding::Plain, Compression::None, DataType::Int32, &[0u8; 10], None, 2),
            Err(CodecError::RowCountMismatch { expected: 8, found: 10 })
        ));
        assert!(matches!(
            encode_chunk(
                Encoding::Plain,
                Compression::None,
                DataType::Int8,
                &[0u8; 8],
                Some(&[0u8; 2]),
                8
            ),
            Err(CodecError::Corrupt("validity byte length does not match rows"))
        ));
    }
}
