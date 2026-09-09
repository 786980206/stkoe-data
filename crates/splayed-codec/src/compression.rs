use crate::error::CodecError;
use splayed_format::Compression;

/// ZSTD 内置默认压缩级别（docs/splayed-codec.md §4.1：级别为 codec 常量，不进 header）。
pub const DEFAULT_ZSTD_LEVEL: i32 = 1;

/// 压缩一段字节（`NONE` = 直通拷贝）。
pub fn compress(compression: Compression, data: &[u8]) -> Result<Vec<u8>, CodecError> {
    match compression {
        Compression::None => Ok(data.to_vec()),
        Compression::Zstd => {
            zstd::bulk::compress(data, DEFAULT_ZSTD_LEVEL)
                .map_err(|e| CodecError::Io(e.to_string()))
        }
        Compression::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
    }
}

/// 解压一段字节。
pub fn decompress(compression: Compression, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
    match compression {
        Compression::None => Ok(payload.to_vec()),
        Compression::Zstd => {
            // 获取解压内容大小（若无法从帧头获取，则退回流式）
            if let Ok(Some(size)) = zstd::zstd_safe::get_frame_content_size(payload) {
                zstd::bulk::decompress(payload, size as usize).map_err(|e| CodecError::Io(e.to_string()))
            } else {
                zstd::stream::decode_all(payload).map_err(|e| CodecError::Io(e.to_string()))
            }
        }
        Compression::Lz4 => {
            lz4_flex::decompress_size_prepended(payload).map_err(|e| CodecError::Io(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_backends() {
        let data: Vec<u8> = (0..10_000usize).map(|i| (i % 7) as u8).collect();
        for c in [Compression::None, Compression::Zstd, Compression::Lz4] {
            let packed = compress(c, &data).unwrap();
            let out = decompress(c, &packed).unwrap();
            assert_eq!(out, data, "{c:?}");
        }
        // 重复数据下 ZSTD / LZ4 都应有收益
        assert!(compress(Compression::Zstd, &data).unwrap().len() < data.len() / 2);
        assert!(compress(Compression::Lz4, &data).unwrap().len() < data.len());
    }

    #[test]
    fn empty_payload_roundtrip() {
        for c in [Compression::None, Compression::Zstd, Compression::Lz4] {
            let packed = compress(c, &[]).unwrap();
            assert_eq!(decompress(c, &packed).unwrap(), Vec::<u8>::new());
        }
    }
}
