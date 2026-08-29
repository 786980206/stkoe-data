//! Data types, NULL bit patterns, and fixed-width layout for Splayed V1.
//!
//! See `plan.md` §5.3 and §5.4 for the authoritative definition.

/// Numeric ID stored in FIELD header `data_type` and META header `time_type`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Bool = 0,
    Int32 = 1,
    Int64 = 2,
    Float32 = 3,
    Float64 = 4,
    Date32 = 5,
    TimestampUs = 6,
    /// Extended fixed-width types (added incrementally; IDs 7+ don't affect
    /// existing files which only ever use 0–6).
    Int8 = 7,
    Int16 = 8,
    UInt8 = 9,
    UInt16 = 10,
    UInt32 = 11,
    UInt64 = 12,
    Date64 = 13,
}

impl DataType {
    /// Parse from a raw `u8` ID (as stored in file headers).
    pub fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(Self::Bool),
            1 => Some(Self::Int32),
            2 => Some(Self::Int64),
            3 => Some(Self::Float32),
            4 => Some(Self::Float64),
            5 => Some(Self::Date32),
            6 => Some(Self::TimestampUs),
            7 => Some(Self::Int8),
            8 => Some(Self::Int16),
            9 => Some(Self::UInt8),
            10 => Some(Self::UInt16),
            11 => Some(Self::UInt32),
            12 => Some(Self::UInt64),
            13 => Some(Self::Date64),
            _ => None,
        }
    }

    /// Fixed-width size in bytes of a single value of this type.
    #[inline]
    pub const fn size_of(self) -> usize {
        match self {
            Self::Bool | Self::Int8 | Self::UInt8 => 1,
            Self::Int16 | Self::UInt16 => 2,
            Self::Int32 | Self::Float32 | Self::Date32 | Self::UInt32 => 4,
            Self::Int64 | Self::Float64 | Self::TimestampUs | Self::UInt64 | Self::Date64 => 8,
        }
    }

    /// Whether this type is one of the two valid TIME types.
    #[inline]
    pub const fn is_time_type(self) -> bool {
        matches!(self, Self::Date32 | Self::TimestampUs)
    }

    /// The NULL sentinel bit-pattern for this type, as raw little-endian bytes.
    ///
    /// Comparison must be done on the **bit pattern**, not the logical value
    /// (e.g. for floats `value == NaN` is false; compare the raw bits instead).
    #[inline]
    pub const fn null_bytes(self) -> &'static [u8] {
        match self {
            // BOOL: 0x02
            Self::Bool => &[0x02],
            // INT8: 0x80
            Self::Int8 => &[0x80],
            // INT16: 0x8000
            Self::Int16 => &[0x00, 0x80],
            // INT32 / DATE32: 0x80000000
            Self::Int32 | Self::Date32 => &[0x00, 0x00, 0x00, 0x80],
            // INT64 / TIMESTAMP_US / DATE64: 0x8000000000000000
            Self::Int64 | Self::TimestampUs | Self::Date64 => {
                &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80]
            }
            // FLOAT32: 0x7FC00000 (canonical NaN)
            Self::Float32 => &[0x00, 0x00, 0xC0, 0x7F],
            // FLOAT64: 0x7FF8000000000000 (canonical NaN)
            Self::Float64 => &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x7F],
            // Unsigned: all-ones (MAX bit pattern is stolen as NULL).
            Self::UInt8 => &[0xFF],
            Self::UInt16 => &[0xFF, 0xFF],
            Self::UInt32 => &[0xFF, 0xFF, 0xFF, 0xFF],
            Self::UInt64 => &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bool => "BOOL",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Float32 => "FLOAT32",
            Self::Float64 => "FLOAT64",
            Self::Date32 => "DATE32",
            Self::TimestampUs => "TIMESTAMP_US",
            Self::Int8 => "INT8",
            Self::Int16 => "INT16",
            Self::UInt8 => "UINT8",
            Self::UInt16 => "UINT16",
            Self::UInt32 => "UINT32",
            Self::UInt64 => "UINT64",
            Self::Date64 => "DATE64",
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// Typed value helpers — work on raw byte buffers so the format layer stays
// free of any Arrow dependency.
// ---------------------------------------------------------------------------

/// A fixed-width typed value backed by raw bytes.  All comparisons are on the
/// **bit pattern**, which is essential for correct NULL detection on floats.
#[derive(Debug, Clone, Copy)]
pub struct RawValue {
    pub ty: DataType,
    pub bytes: [u8; 8],
}

impl RawValue {
    /// Construct a NULL sentinel for `ty`.
    #[inline]
    pub fn null(ty: DataType) -> Self {
        let mut bytes = [0u8; 8];
        let n = ty.null_bytes();
        bytes[..n.len()].copy_from_slice(n);
        Self { ty, bytes }
    }

    /// Compare the bit pattern against the NULL sentinel.
    #[inline]
    pub fn is_null(&self) -> bool {
        let n = self.ty.null_bytes();
        let sz = self.ty.size_of();
        &self.bytes[..sz] == n
    }

    /// Write this value into a little-endian byte buffer at the given offset.
    /// `buf` must have length `>= offset + ty.size_of()`.
    #[inline]
    pub fn write_le(&self, buf: &mut [u8], offset: usize) {
        let sz = self.ty.size_of();
        buf[offset..offset + sz].copy_from_slice(&self.bytes[..sz]);
    }

    /// Read a value from a little-endian byte buffer at the given offset.
    #[inline]
    pub fn read_le(buf: &[u8], offset: usize, ty: DataType) -> Self {
        let sz = ty.size_of();
        let mut bytes = [0u8; 8];
        bytes[..sz].copy_from_slice(&buf[offset..offset + sz]);
        Self { ty, bytes }
    }

    /// Element byte width of this value's type.
    #[inline]
    pub fn size_of_ty(&self) -> usize {
        self.ty.size_of()
    }

    /// The NULL sentinel byte pattern of this value's type.
    #[inline]
    pub fn null_bytes_slice(&self) -> &'static [u8] {
        self.ty.null_bytes()
    }
}

/// Typed constructors for RawValue.  We use inherent methods rather than
/// `From` impls because multiple Splayed types map to the same Rust type
/// (e.g. Int32 and Date32 are both i32).
impl RawValue {
    #[inline]
    pub fn from_bool(v: bool) -> Self {
        let mut bytes = [0u8; 8];
        bytes[0] = if v { 1 } else { 0 };
        Self { ty: DataType::Bool, bytes }
    }

    #[inline]
    pub fn from_i32(v: i32) -> Self {
        let mut bytes = [0u8; 8];
        bytes[..4].copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::Int32, bytes }
    }

    #[inline]
    pub fn from_i64(v: i64) -> Self {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::Int64, bytes }
    }

    #[inline]
    pub fn from_f32(v: f32) -> Self {
        let mut bytes = [0u8; 8];
        bytes[..4].copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::Float32, bytes }
    }

    #[inline]
    pub fn from_f64(v: f64) -> Self {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::Float64, bytes }
    }

    #[inline]
    pub fn from_date32(v: i32) -> Self {
        let mut bytes = [0u8; 8];
        bytes[..4].copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::Date32, bytes }
    }

    #[inline]
    pub fn from_timestamp_us(v: i64) -> Self {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::TimestampUs, bytes }
    }

    #[inline]
    pub fn from_i8(v: i8) -> Self {
        let mut bytes = [0u8; 8];
        bytes[0] = v.to_le_bytes()[0];
        Self { ty: DataType::Int8, bytes }
    }

    #[inline]
    pub fn from_i16(v: i16) -> Self {
        let mut bytes = [0u8; 8];
        bytes[..2].copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::Int16, bytes }
    }

    #[inline]
    pub fn from_u8(v: u8) -> Self {
        let mut bytes = [0u8; 8];
        bytes[0] = v;
        Self { ty: DataType::UInt8, bytes }
    }

    #[inline]
    pub fn from_u16(v: u16) -> Self {
        let mut bytes = [0u8; 8];
        bytes[..2].copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::UInt16, bytes }
    }

    #[inline]
    pub fn from_u32(v: u32) -> Self {
        let mut bytes = [0u8; 8];
        bytes[..4].copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::UInt32, bytes }
    }

    #[inline]
    pub fn from_u64(v: u64) -> Self {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::UInt64, bytes }
    }

    #[inline]
    pub fn from_date64(v: i64) -> Self {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&v.to_le_bytes());
        Self { ty: DataType::Date64, bytes }
    }
}

impl RawValue {
    pub fn as_bool(&self) -> Option<bool> {
        debug_assert_eq!(self.ty, DataType::Bool);
        if self.is_null() {
            return None;
        }
        Some(self.bytes[0] != 0)
    }

    pub fn as_i32(&self) -> Option<i32> {
        if self.is_null() {
            return None;
        }
        Some(i32::from_le_bytes([
            self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3],
        ]))
    }

    pub fn as_i64(&self) -> Option<i64> {
        if self.is_null() {
            return None;
        }
        Some(i64::from_le_bytes(self.bytes))
    }

    pub fn as_f32(&self) -> Option<f32> {
        if self.is_null() {
            return None;
        }
        Some(f32::from_le_bytes([
            self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3],
        ]))
    }

    pub fn as_f64(&self) -> Option<f64> {
        if self.is_null() {
            return None;
        }
        Some(f64::from_le_bytes(self.bytes))
    }

    pub fn as_i8(&self) -> Option<i8> {
        if self.is_null() {
            return None;
        }
        Some(i8::from_le_bytes([self.bytes[0]]))
    }

    pub fn as_i16(&self) -> Option<i16> {
        if self.is_null() {
            return None;
        }
        Some(i16::from_le_bytes([self.bytes[0], self.bytes[1]]))
    }

    pub fn as_u8(&self) -> Option<u8> {
        if self.is_null() {
            return None;
        }
        Some(self.bytes[0])
    }

    pub fn as_u16(&self) -> Option<u16> {
        if self.is_null() {
            return None;
        }
        Some(u16::from_le_bytes([self.bytes[0], self.bytes[1]]))
    }

    pub fn as_u32(&self) -> Option<u32> {
        if self.is_null() {
            return None;
        }
        Some(u32::from_le_bytes([
            self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3],
        ]))
    }

    pub fn as_u64(&self) -> Option<u64> {
        if self.is_null() {
            return None;
        }
        Some(u64::from_le_bytes(self.bytes))
    }

    pub fn as_date64(&self) -> Option<i64> {
        if self.is_null() {
            return None;
        }
        Some(i64::from_le_bytes(self.bytes))
    }
}

/// Fill `buf` with the NULL sentinel pattern for `ty`, repeated.
/// Used by `create_field` to pre-allocate a FIELD data region.
pub fn fill_null(buf: &mut [u8], ty: DataType) {
    let sz = ty.size_of();
    let null = ty.null_bytes();
    for chunk in buf.chunks_exact_mut(sz) {
        chunk.copy_from_slice(&null[..sz]);
    }
    // Handle any remainder (shouldn't happen if buf is exact multiple).
    let rem = buf.len() % sz;
    if rem > 0 {
        let start = buf.len() - rem;
        buf[start..].copy_from_slice(&null[..rem]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        assert_eq!(DataType::Bool.size_of(), 1);
        assert_eq!(DataType::Int32.size_of(), 4);
        assert_eq!(DataType::Int64.size_of(), 8);
        assert_eq!(DataType::Float32.size_of(), 4);
        assert_eq!(DataType::Float64.size_of(), 8);
        assert_eq!(DataType::Date32.size_of(), 4);
        assert_eq!(DataType::TimestampUs.size_of(), 8);
    }

    #[test]
    fn null_detection_int() {
        let n = RawValue::null(DataType::Int32);
        assert!(n.is_null());
        assert_eq!(n.as_i32(), None);

        let v = RawValue::from_i32(42);
        assert!(!v.is_null());
        assert_eq!(v.as_i32(), Some(42));
    }

    #[test]
    fn null_detection_float() {
        let n = RawValue::null(DataType::Float64);
        assert!(n.is_null());
        assert_eq!(n.as_f64(), None);

        // A regular NaN must NOT be detected as NULL.
        // Use 0x7FF8000000000001 — a non-canonical NaN (payload bit set).
        let nan_bits: u64 = 0x7FF8000000000001;
        let nan = RawValue::from_f64(f64::from_bits(nan_bits));
        assert!(!nan.is_null());

        let v = RawValue::from_f64(3.14);
        assert!(!v.is_null());
        assert_eq!(v.as_f64(), Some(3.14));
    }

    #[test]
    fn null_detection_bool() {
        let n = RawValue::null(DataType::Bool);
        assert!(n.is_null());
        assert_eq!(n.as_bool(), None);

        let t = RawValue::from_bool(true);
        assert!(!t.is_null());
        assert_eq!(t.as_bool(), Some(true));
    }

    #[test]
    fn roundtrip_le_bytes() {
        let v = RawValue::from_i64(1234567);
        let mut buf = [0u8; 16];
        v.write_le(&mut buf, 4);
        let r = RawValue::read_le(&buf, 4, DataType::Int64);
        assert_eq!(r.as_i64(), Some(1234567));
    }

    #[test]
    fn fill_null_works() {
        let mut buf = vec![0u8; 16]; // 4 × i32
        fill_null(&mut buf, DataType::Int32);
        for chunk in buf.chunks_exact(4) {
            assert_eq!(chunk, &[0x00, 0x00, 0x00, 0x80]);
        }
        // Verify each is detected as null.
        for i in 0..4 {
            let v = RawValue::read_le(&buf, i * 4, DataType::Int32);
            assert!(v.is_null());
        }
    }

    #[test]
    fn extended_types_roundtrip_and_sentinels() {
        use super::DataType as DT;

        // Concrete roundtrips.
        let u32 = RawValue::from_u32(4_000_000_000);
        assert_eq!(u32.ty, DT::UInt32);
        assert!(!u32.is_null());
        assert_eq!(u32.as_u32(), Some(4_000_000_000));
        assert_eq!(u32.size_of_ty(), 4);

        let u64 = RawValue::from_u64(18_000_000_000_000_000_000);
        assert!(!u64.is_null());
        assert_eq!(u64.as_u64(), Some(18_000_000_000_000_000_000));
        assert_eq!(u64.size_of_ty(), 8);

        let i16 = RawValue::from_i16(-300);
        assert!(!i16.is_null());
        assert_eq!(i16.as_i16(), Some(-300));
        assert_eq!(i16.size_of_ty(), 2);

        let d64 = RawValue::from_date64(86400000);
        assert!(!d64.is_null());
        assert_eq!(d64.as_date64(), Some(86400000));

        // NULL sentinels: signed → MIN pattern; unsigned → all-ones (MAX stolen).
        for ty in [DT::Int8, DT::Int16, DT::Int32, DT::Int64, DT::Date64, DT::TimestampUs] {
            assert!(RawValue::null(ty).is_null(), "sentinel for {ty:?}");
            assert_eq!(RawValue::null(ty).size_of_ty(), ty.size_of());
        }
        for ty in [DT::UInt8, DT::UInt16, DT::UInt32, DT::UInt64] {
            assert!(RawValue::null(ty).is_null(), "sentinel for {ty:?}");
            assert!(RawValue::null(ty).null_bytes_slice().iter().all(|&b| b == 0xFF));
        }

        // Size table + ID parsing for all extended types.
        assert_eq!(DT::Int8.size_of(), 1);
        assert_eq!(DT::Int16.size_of(), 2);
        assert_eq!(DT::UInt32.size_of(), 4);
        assert_eq!(DT::Date64.size_of(), 8);
        for id in 0..=13u8 {
            assert!(DT::from_id(id).is_some(), "id {id} must parse");
        }
        assert!(DT::from_id(14).is_none());
    }
}
