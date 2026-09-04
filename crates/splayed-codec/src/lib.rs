//! splayed-codec：V2.0 编码（PLAIN / DELTA / RLE / BITPACK）与压缩（NONE / ZSTD / LZ4）原语，
//! 以及 compressed FIELD 的 chunk 布局（docs/splayed-codec.md §3）。
//!
//! codec 只做内存字节 ↔ 编码/压缩 payload 的纯变换：不做文件 IO、不感知 header 与 META。

pub mod bitpack;
pub mod chunk;
pub mod compression;
pub mod delta;
pub mod encoding;
pub mod error;
pub mod plain;
pub mod rle;

pub use chunk::{decode_chunk, encode_chunk, ChunkHeader, CHUNK_HEADER_SIZE};
pub use compression::{compress, decompress, DEFAULT_ZSTD_LEVEL};
pub use encoding::{decode_values, encode_values};
pub use error::CodecError;

#[cfg(test)]
mod tests {
    use super::*;
    use splayed_format::{Compression, DataType, Encoding};

    /// 全编码 × 全压缩 × 有/无 validity 的逐位 roundtrip（含 NULL 未定义位型）。
    #[test]
    fn chunk_roundtrip_all_combinations() {
        let data_types = [
            DataType::Bool,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::TimestampUs,
            DataType::Date64,
        ];
        let rows = 257usize; // 非整除 8，覆盖不完整 validity 字节
        for dt in data_types {
            // 值 bit 型任意（含 NULL 单元的未定义位）
            let mut values = Vec::with_capacity(rows * dt.size_of());
            for i in 0..rows * dt.size_of() {
                values.push((i * 37 + (i >> 3) * 191) as u8);
            }
            for with_validity in [false, true] {
                let validity: Option<Vec<u8>> = with_validity.then(|| {
                    (0..splayed_format::validity_size(rows as u32))
                        .map(|b| (b as u8).wrapping_mul(73).wrapping_add(5))
                        .collect()
                });
                for encoding in [Encoding::Plain, Encoding::Delta, Encoding::Rle, Encoding::Bitpack] {
                    for compression in [Compression::None, Compression::Zstd, Compression::Lz4] {
                        let chunk = encode_chunk(
                            encoding,
                            compression,
                            dt,
                            &values,
                            validity.as_deref(),
                            rows,
                        )
                        .unwrap_or_else(|e| panic!("{dt:?} {encoding:?} {compression:?}: {e}"));
                        let (out_values, out_validity, out_rows) =
                            decode_chunk(encoding, compression, dt, &chunk).unwrap();
                        assert_eq!(out_rows, rows);
                        assert_eq!(out_values, values, "{dt:?} {encoding:?} {compression:?}");
                        assert_eq!(out_validity.as_deref(), validity.as_deref());
                    }
                }
            }
        }
    }
}
