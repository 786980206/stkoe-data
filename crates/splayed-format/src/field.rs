//! FIELD file format helpers — header access and data offset computation.
//!
//! See `plan.md` §5.2 and §5.2.1.

use crate::header::{Compression, DATA_OFFSET, Encoding, FieldHeader, HEADER_SIZE};
use crate::DataType;

/// Compute the byte offset of a row in a PLAIN+NONE FIELD file.
///
/// ```text
/// offset = 64 + row × sizeof(type)
/// ```
#[inline]
pub fn row_byte_offset(data_type: DataType, row: u32) -> u64 {
    DATA_OFFSET + (row as u64) * (data_type.size_of() as u64)
}

/// Compute the total data length in bytes for a FIELD.
///
/// ```text
/// data_length = row_count × sizeof(type)
/// ```
#[inline]
pub fn data_length(data_type: DataType, row_count: u32) -> u64 {
    (row_count as u64) * (data_type.size_of() as u64)
}

/// Total file size for a PLAIN+NONE FIELD.
#[inline]
pub fn field_file_size(data_type: DataType, row_count: u32) -> u64 {
    HEADER_SIZE as u64 + data_length(data_type, row_count)
}

/// Validate a FIELD header against expected parameters.
pub fn validate_field_header(
    header: &FieldHeader,
    expected_type: DataType,
    meta_generation: u64,
) -> Result<(), &'static str> {
    header.validate()?;
    if header.data_type()? != expected_type {
        return Err("data_type mismatch");
    }
    if header.generation != meta_generation {
        return Err("generation mismatch with META");
    }
    Ok(())
}

/// Create a default PLAIN+NONE FIELD header for a newly pre-allocated field.
pub fn new_plain_field_header(
    data_type: DataType,
    generation: u64,
    row_count: u32,
) -> FieldHeader {
    let dl = data_length(data_type, row_count);
    FieldHeader::new(data_type, Encoding::Plain, Compression::None, generation, row_count, dl)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_computation() {
        assert_eq!(row_byte_offset(DataType::Int32, 0), 64);
        assert_eq!(row_byte_offset(DataType::Int32, 10), 64 + 10 * 4);
        assert_eq!(row_byte_offset(DataType::Float64, 100), 64 + 100 * 8);
        assert_eq!(row_byte_offset(DataType::Bool, 5), 64 + 5);
    }

    #[test]
    fn data_length_and_file_size() {
        assert_eq!(data_length(DataType::Int32, 1000), 4000);
        assert_eq!(field_file_size(DataType::Int32, 1000), 64 + 4000);
        assert_eq!(field_file_size(DataType::Float64, 1000), 64 + 8000);
    }
}
