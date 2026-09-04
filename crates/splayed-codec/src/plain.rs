use crate::error::CodecError;

/// PLAIN：原始定宽连续数组；encode = 直通（docs/splayed-codec.md §5）。
pub fn encode(values: &[u8]) -> Result<Vec<u8>, CodecError> {
    Ok(values.to_vec())
}

/// 解码：校验 payload 长度与 `rows * size` 一致后直通。
pub fn decode(payload: &[u8], rows: usize, size: usize) -> Result<Vec<u8>, CodecError> {
    let expected = rows * size;
    if payload.len() != expected {
        return Err(CodecError::RowCountMismatch {
            expected,
            found: payload.len(),
        });
    }
    Ok(payload.to_vec())
}
