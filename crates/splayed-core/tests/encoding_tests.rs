//! 编码器接线集成测试：`compact_field_with_encoding`（DELTA/RLE/BITPACK，
//! 可选压缩）→ `FieldReader` 解码恢复原始值；统计 footer 保留。

use std::fs;
use std::path::PathBuf;

use splayed_codec::compact_field_with_encoding;
use splayed_core::{
    FieldReader, TableColumn, create_table,
};
use splayed_format::{Compression, DataType as ST, Encoding, RawValue, TimeType};

fn temp_dir(suffix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("splayed_core_enc_{suffix}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// 2 sym × 3 time（6 行）Int64 字段：值 1..6（含结构性数据便于编码）。
fn build_dataset(dir: &PathBuf) {
    let syms: Vec<String> = ["SYM01", "SYM01", "SYM01", "SYM02", "SYM02", "SYM02"]
        .map(|s| s.to_string())
        .to_vec();
    let time: Vec<i64> = vec![0, 1, 2, 0, 1, 2];
    let mut values = vec![0u8; 6 * 8];
    for (i, v) in [1i64, 2, 3, 4, 5, 6].iter().enumerate() {
        RawValue::from_i64(*v).write_le(&mut values, i * 8);
    }
    let mut ts = vec![0u8; 6 * 8];
    for (i, v) in [10i64, 11, 12, 13, 14, 15].iter().enumerate() {
        RawValue::from_i64(*v).write_le(&mut ts, i * 8);
    }
    create_table(
        dir,
        TimeType::Date32,
        &syms,
        &time,
        &[
            TableColumn {
                name: "v".to_string(),
                data_type: ST::Int64,
                values,
            },
            TableColumn {
                name: "t".to_string(),
                data_type: ST::Int64,
                values: ts,
            },
        ],
        true,
    )
    .unwrap();
}

fn read_all(dir: &std::path::Path, field: &str) -> Vec<i64> {
    let r = FieldReader::open(dir.join(field)).unwrap();
    (0..r.row_count())
        .map(|i| r.read_row(i).unwrap().as_i64().unwrap())
        .collect()
}

#[test]
fn encoding_roundtrip_delta_rle_bitpack() {
    for (encoding, case) in [
        (Encoding::Delta, "delta"),
        (Encoding::Rle, "rle"),
        (Encoding::Bitpack, "bitpack"),
    ] {
        let dir = temp_dir(case);
        build_dataset(&dir);

        // 不压缩 + 编码。
        compact_field_with_encoding(dir.join("v"), encoding, Compression::None).unwrap();
        assert_eq!(read_all(&dir, "v"), vec![1, 2, 3, 4, 5, 6]);
        // 统计 footer 保留（min=1 max=6）。
        let r = FieldReader::open(dir.join("v")).unwrap();
        let st = r.stats().unwrap();
        assert_eq!(i64::from_le_bytes(st.min), 1);
        assert_eq!(i64::from_le_bytes(st.max), 6);
        // 第二字段仍 PLAIN 不受影响。
        assert_eq!(read_all(&dir, "t"), vec![10, 11, 12, 13, 14, 15]);

        fs::remove_dir_all(&dir).ok();
    }
}

#[test]
fn encoding_roundtrip_with_compression() {
    let dir = temp_dir("enc_zstd");
    build_dataset(&dir);

    // 编码 + 压缩（DELTA + ZSTD）。
    compact_field_with_encoding(dir.join("v"), Encoding::Delta, Compression::Zstd).unwrap();
    assert_eq!(read_all(&dir, "v"), vec![1, 2, 3, 4, 5, 6]);

    // RLE + LZ4。
    compact_field_with_encoding(dir.join("t"), Encoding::Rle, Compression::Lz4).unwrap();
    assert_eq!(read_all(&dir, "t"), vec![10, 11, 12, 13, 14, 15]);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn plain_encoding_still_plain_compact() {
    // Encoding::Plain → 走原 compact_field 路径（格式不变）。
    let dir = temp_dir("plain");
    build_dataset(&dir);
    compact_field_with_encoding(dir.join("v"), Encoding::Plain, Compression::Zstd).unwrap();
    let r = FieldReader::open(dir.join("v")).unwrap();
    assert_eq!(r.header().encoding().unwrap(), Encoding::Plain);
    assert_eq!(r.header().compression().unwrap(), Compression::Zstd);
    assert_eq!(read_all(&dir, "v"), vec![1, 2, 3, 4, 5, 6]);
    fs::remove_dir_all(&dir).ok();
}