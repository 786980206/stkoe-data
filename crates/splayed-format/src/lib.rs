//! splayed-format：V2.0 磁盘格式定义与零依赖内存数据模型。
//!
//! 权威定义见 `docs/splayed-format.md`。本 crate 只包含格式常量、header 结构与
//! 内存数据视图（Buffer / BitmapView / ColumnView / DataView）；不含任何 IO 与编解码逻辑。

pub mod bitmap;
pub mod buffer;
pub mod column;
pub mod dataview;
pub mod error;
pub mod field;
pub mod meta;
pub mod schema;
pub mod types;

pub use bitmap::{Bitmap, BitmapView, bitmap_count_ones, bitmap_fill_bits};
pub use buffer::{Buffer, BufferView};
pub use column::{Column, ColumnSegment, ColumnValues, ColumnView, DictBuffers};
pub use dataview::{Data, DataView};
pub use error::FormatError;
pub use field::{FieldHeader, FIELD_FLAGS_HAS_VALIDITY, FIELD_HEADER_SIZE};
pub use meta::{MetaHeader, SymIndexRecord, META_HEADER_SIZE, SYM_INDEX_RECORD_SIZE};
pub use schema::{FieldSchema, Schema};
pub use types::{Compression, DataType, Encoding, TimeType};

/// META 文件 magic：`SPLAYMTA`（u64 LE，8 字节）。
pub const META_MAGIC: u64 = u64::from_le_bytes(*b"SPLAYMTA");
/// FIELD 文件 magic：`SPLAYFLD`（u64 LE，8 字节）。
pub const FIELD_MAGIC: u64 = u64::from_le_bytes(*b"SPLAYFLD");
/// META / FIELD header 固定 64 字节。
pub const HEADER_SIZE: usize = 64;
/// 数据区固定起始于 offset 64。
pub const DATA_OFFSET: u64 = HEADER_SIZE as u64;
/// 格式版本（V2.0）。
pub const FORMAT_VERSION: u16 = 2;

/// VALIDITY 区大小：`ceil(row_count / 8)` 字节，LSB-first（bit i ↔ row i）。
pub const fn validity_size(row_count: u32) -> usize {
    (row_count as usize + 7) / 8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn magic_constants_are_8_bytes() {
        assert_eq!(META_MAGIC.to_le_bytes(), *b"SPLAYMTA");
        assert_eq!(FIELD_MAGIC.to_le_bytes(), *b"SPLAYFLD");
    }

    #[test]
    fn validity_size_rounds_up() {
        assert_eq!(validity_size(0), 0);
        assert_eq!(validity_size(1), 1);
        assert_eq!(validity_size(8), 1);
        assert_eq!(validity_size(9), 2);
        assert_eq!(validity_size(250), 32);
    }
}
