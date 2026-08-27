//! PLAIN encoding: no transformation, raw fixed-width array.
//!
//! This is the fastest path — `mmap + PLAIN + NONE` gives O(1) random access
//! via `offset = 64 + row × sizeof(type)`.

use splayed_format::DataType;

/// PLAIN codec: identity transform.  Data is stored as-is.
pub struct PlainCodec;

impl PlainCodec {
    /// "Encode" = no-op copy (PLAIN is identity).
    pub fn encode(data: &[u8]) -> &[u8] {
        data
    }

    /// "Decode" = no-op copy (PLAIN is identity).
    pub fn decode(data: &[u8]) -> &[u8] {
        data
    }

    /// Compute the encoded size for `n` rows of `data_type`.
    pub fn encoded_size(data_type: DataType, n: u32) -> usize {
        (n as usize) * data_type.size_of()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_identity() {
        let data = [1u8, 2, 3, 4, 5];
        assert_eq!(PlainCodec::encode(&data), &data);
        assert_eq!(PlainCodec::decode(&data), &data);
        assert_eq!(PlainCodec::encoded_size(DataType::Int32, 10), 40);
    }
}
