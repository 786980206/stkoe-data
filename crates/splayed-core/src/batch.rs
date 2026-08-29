//! Engine-agnostic in-memory columnar batch: `CoreType`, `CoreSchema`,
//! `CoreBatch` (Buffer + validity bitmap + optional offsets / dictionary).
//!
//! This is the **memory** representation of the core layer — it has **no Arrow
//! dependency** and deliberately does not depend on one. Adapters (splayed-arrow,
//! DataFusion, DuckDB, …) convert `CoreBatch` into their own formats, reusing
//! the buffers zero-copy where possible via [`CoreColumn::into_buffer`].
//!
//! On-disk files remain fixed-width with sentinel NULLs (see `splayed-format`);
//! the sentinel → validity-bitmap translation happens exactly once, when a scan
//! batch is materialized here (and back at write time).
//!
//! Bitmap semantics: bit `i` set == element `i` is **valid** (matches Arrow).

use std::sync::Arc;

use splayed_format::{DataType, TimeType};

// ---------------------------------------------------------------------------
// Type system
// ---------------------------------------------------------------------------

/// Time unit for [`CoreType::Timestamp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoreTimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

/// In-memory type system — a superset of the on-disk [`DataType`].
///
/// The on-disk format only stores the fixed-width subset; varlen / nested /
/// decimal types exist here for adapters and future columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CoreType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Date32,
    Date64,
    Timestamp(CoreTimeUnit, Option<String>),
    Decimal(u8, u8), // precision, scale
    List(Box<CoreType>),
    Struct(Vec<CoreField>),
    Dictionary(Box<CoreType>),
}

/// A named, typed field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CoreField {
    pub name: String,
    pub ty: CoreType,
    pub nullable: bool,
}

impl CoreField {
    pub fn new(name: impl Into<String>, ty: CoreType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            ty,
            nullable,
        }
    }
}

/// An ordered set of fields.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CoreSchema {
    pub fields: Vec<CoreField>,
}

impl CoreSchema {
    pub fn new(fields: Vec<CoreField>) -> Self {
        Self { fields }
    }

    pub fn field(&self, i: usize) -> &CoreField {
        &self.fields[i]
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }
}

impl CoreType {
    /// Map an on-disk type to its in-memory counterpart.
    pub fn from_disk(dt: DataType) -> Self {
        match dt {
            DataType::Bool => CoreType::Boolean,
            DataType::Int32 => CoreType::Int32,
            DataType::Int64 => CoreType::Int64,
            DataType::Float32 => CoreType::Float32,
            DataType::Float64 => CoreType::Float64,
            DataType::Date32 => CoreType::Date32,
            DataType::TimestampUs => CoreType::Timestamp(CoreTimeUnit::Microsecond, None),
            // Extended disk types (Phase 2) map 1:1.
            DataType::Int8 => CoreType::Int8,
            DataType::Int16 => CoreType::Int16,
            DataType::UInt8 => CoreType::UInt8,
            DataType::UInt16 => CoreType::UInt16,
            DataType::UInt32 => CoreType::UInt32,
            DataType::UInt64 => CoreType::UInt64,
            DataType::Date64 => CoreType::Date64,
        }
    }

    /// Map back to an on-disk type, or `None` for types the disk doesn't store.
    pub fn to_disk(&self) -> Option<DataType> {
        match self {
            CoreType::Boolean => Some(DataType::Bool),
            CoreType::Int32 => Some(DataType::Int32),
            CoreType::Int64 => Some(DataType::Int64),
            CoreType::Float32 => Some(DataType::Float32),
            CoreType::Float64 => Some(DataType::Float64),
            CoreType::Date32 => Some(DataType::Date32),
            CoreType::Timestamp(CoreTimeUnit::Microsecond, None) => Some(DataType::TimestampUs),
            CoreType::Int8 => Some(DataType::Int8),
            CoreType::Int16 => Some(DataType::Int16),
            CoreType::UInt8 => Some(DataType::UInt8),
            CoreType::UInt16 => Some(DataType::UInt16),
            CoreType::UInt32 => Some(DataType::UInt32),
            CoreType::UInt64 => Some(DataType::UInt64),
            CoreType::Date64 => Some(DataType::Date64),
            _ => None,
        }
    }

    /// Fixed-width byte width, if this is a fixed-width primitive.
    pub fn byte_width(&self) -> Option<usize> {
        Some(match self {
            CoreType::Boolean | CoreType::Int8 | CoreType::UInt8 => 1,
            CoreType::Int16 | CoreType::UInt16 => 2,
            CoreType::Int32 | CoreType::UInt32 | CoreType::Float32 | CoreType::Date32 => 4,
            CoreType::Int64 | CoreType::UInt64 | CoreType::Float64 | CoreType::Date64 => 8,
            CoreType::Timestamp(CoreTimeUnit::Second | CoreTimeUnit::Millisecond | CoreTimeUnit::Microsecond | CoreTimeUnit::Nanosecond, _) => 8,
            CoreType::Decimal(..) => 16, // i128
            _ => return None,
        })
    }

    /// Resolve the scan time type of a dataset.
    pub fn time_from_disk_meta(tt: TimeType) -> Self {
        match tt {
            TimeType::Date32 => CoreType::Date32,
            TimeType::TimestampUs => CoreType::Timestamp(CoreTimeUnit::Microsecond, None),
        }
    }
}

// ---------------------------------------------------------------------------
// Buffer & Bitmap
// ---------------------------------------------------------------------------

/// An owned byte buffer for one column's data (LE values, contiguous).
///
/// The zero-copy handoff is [`Buffer::take`] — the underlying `Vec<u8>` moves
/// out without copying.
#[derive(Debug, Clone, Default)]
pub struct Buffer {
    data: Vec<u8>,
}

impl Buffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_vec(v: Vec<u8>) -> Self {
        Self { data: v }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            data: Vec::with_capacity(cap),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn extend_from_slice(&mut self, s: &[u8]) {
        self.data.extend_from_slice(s);
    }

    /// Hand the underlying `Vec<u8>` over without copying (zero-copy handoff).
    pub fn take(self) -> Vec<u8> {
        self.data
    }

    /// Typed view of the buffer as `&[T]`, when the data pointer happens to be
    /// aligned for `T` (checked at runtime). Returns `None` when unaligned —
    /// prefer [`Buffer::take`] + the target format's own buffer construction
    /// for the guaranteed-copy-free path.
    pub fn typed_slice<T: Copy>(&self) -> Option<&[T]> {
        let ptr = self.data.as_ptr();
        let align = std::mem::align_of::<T>();
        if (ptr as usize) % align != 0 {
            return None;
        }
        let n = self.data.len() / std::mem::size_of::<T>();
        if n * std::mem::size_of::<T>() != self.data.len() {
            return None;
        }
        Some(unsafe { std::slice::from_raw_parts(ptr as *const T, n) })
    }
}

/// A validity bitmap (LSB-first, bit set == valid). Mirrors the Arrow validity
/// layout so adapters can hand `into_vec()` straight to a `NullBuffer`/`BooleanBuffer`.
#[derive(Debug, Clone, Default)]
pub struct Bitmap {
    bytes: Vec<u8>,
    len: usize,
}

impl Bitmap {
    /// All-invalid bitmap of `len` bits (useful before a scan fills it).
    pub fn new(len: usize) -> Self {
        Self {
            bytes: vec![0u8; len.div_ceil(8)],
            len,
        }
    }

    /// All-valid bitmap of `len` bits.
    pub fn with_all_valid(len: usize) -> Self {
        Self {
            bytes: vec![0xFFu8; len.div_ceil(8)],
            len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn set(&mut self, idx: usize, valid: bool) {
        let byte = idx / 8;
        let bit = idx % 8;
        if valid {
            self.bytes[byte] |= 1 << bit;
        } else {
            self.bytes[byte] &= !(1 << bit);
        }
    }

    pub fn is_valid(&self, idx: usize) -> bool {
        (self.bytes[idx / 8] >> (idx % 8)) & 1 == 1
    }

    pub fn contains_null(&self) -> bool {
        // All-valid requires every set bit in the last partial byte to be 1 too.
        let full = self.len / 8;
        for b in &self.bytes[..full] {
            if *b != 0xFF {
                return true;
            }
        }
        let rem = self.len % 8;
        if rem > 0 {
            let mask = (1u8 << rem) - 1;
            if self.bytes[full] != mask {
                return true;
            }
        }
        false
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// A sub-bitmap over `[offset, offset+len)` — for row slicing.
    pub fn slice(&self, offset: usize, len: usize) -> Bitmap {
        let mut out = Bitmap::with_all_valid(len);
        for i in 0..len {
            if !self.is_valid(offset + i) {
                out.set(i, false);
            }
        }
        out
    }

    /// Hand the raw byte vec over (for Arrow `BooleanBuffer`/`NullBuffer`).
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

// ---------------------------------------------------------------------------
// Columns & batch
// ---------------------------------------------------------------------------

/// A shared string dictionary (e.g. the SYM dictionary from META).
#[derive(Debug, Clone, Default)]
pub struct CoreStringDict {
    pub values: Vec<String>,
    pub is_sorted: bool,
}

impl CoreStringDict {
    pub fn new(values: Vec<String>, is_sorted: bool) -> Self {
        Self { values, is_sorted }
    }
}

/// The physical layout of a column.
#[derive(Debug, Clone)]
pub enum CoreColumnKind {
    /// Fixed-width primitive values as contiguous LE bytes.
    Primitive { ty: CoreType, data: Buffer },
    /// Variable-length Utf8/Binary: LE `i32` offsets + data bytes.
    Varlen { offsets: Buffer, data: Buffer },
    /// Dictionary-encoded strings: LE `u32` indices into a shared dictionary.
    Dictionary {
        indices: Buffer,
        values: Arc<CoreStringDict>,
    },
}

/// One column of a [`CoreBatch`].
#[derive(Debug, Clone)]
pub struct CoreColumn {
    pub kind: CoreColumnKind,
    /// None == no NULLs (zero allocation, zero scan).
    pub nulls: Option<Bitmap>,
}

impl CoreColumn {
    pub fn primitive(ty: CoreType, data: Buffer, nulls: Option<Bitmap>) -> Self {
        Self {
            kind: CoreColumnKind::Primitive { ty, data },
            nulls,
        }
    }

    pub fn dictionary(indices: Buffer, values: Arc<CoreStringDict>) -> Self {
        Self {
            kind: CoreColumnKind::Dictionary { indices, values },
            nulls: None,
        }
    }

    pub fn varlen(offsets: Buffer, data: Buffer, nulls: Option<Bitmap>) -> Self {
        Self {
            kind: CoreColumnKind::Varlen { offsets, data },
            nulls,
        }
    }

    /// Raw data bytes (primitive data or varlen payload).
    pub fn data(&self) -> &[u8] {
        match &self.kind {
            CoreColumnKind::Primitive { data, .. } => data.as_slice(),
            CoreColumnKind::Varlen { data, .. } => data.as_slice(),
            CoreColumnKind::Dictionary { .. } => &[],
        }
    }

    pub fn offsets(&self) -> Option<&[u8]> {
        match &self.kind {
            CoreColumnKind::Varlen { offsets, .. } => Some(offsets.as_slice()),
            _ => None,
        }
    }

    /// Raw dictionary index bytes (LE `u32` per row), if a dictionary column.
    pub fn dictionary_indices(&self) -> Option<&[u8]> {
        match &self.kind {
            CoreColumnKind::Dictionary { indices, .. } => Some(indices.as_slice()),
            _ => None,
        }
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.nulls.as_ref()
    }

    pub fn is_primitive(&self) -> bool {
        matches!(self.kind, CoreColumnKind::Primitive { .. })
    }

    /// Kind as the on-disk-oriented byte width, if primitive.
    pub fn typed_slice<T: Copy>(&self) -> Option<&[T]> {
        match &self.kind {
            CoreColumnKind::Primitive { data, .. } => data.typed_slice::<T>(),
            _ => None,
        }
    }

    /// Zero-copy handoff of the primitive data buffer (consumes the column).
    pub fn into_buffer(self) -> Option<(CoreType, Vec<u8>)> {
        match self.kind {
            CoreColumnKind::Primitive { ty, data } => Some((ty, data.take())),
            _ => None,
        }
    }

    /// Zero-copy handoff of dictionary indices (consumes the column).
    pub fn into_dictionary(self) -> Option<(Vec<u8>, Arc<CoreStringDict>)> {
        match self.kind {
            CoreColumnKind::Dictionary { indices, values } => Some((indices.take(), values)),
            _ => None,
        }
    }
}

/// An engine-agnostic columnar batch.
#[derive(Debug, Clone)]
pub struct CoreBatch {
    schema: Arc<CoreSchema>,
    columns: Vec<CoreColumn>,
    num_rows: usize,
}

impl CoreBatch {
    pub fn new(schema: Arc<CoreSchema>, columns: Vec<CoreColumn>, num_rows: usize) -> Self {
        debug_assert_eq!(
            schema.fields.len(),
            columns.len(),
            "schema/column count mismatch"
        );
        Self {
            schema,
            columns,
            num_rows,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn schema(&self) -> &CoreSchema {
        &self.schema
    }

    pub fn column(&self, i: usize) -> &CoreColumn {
        &self.columns[i]
    }

    /// A contiguous row window `[offset, offset+len)` of this batch (copies the
    /// window's bytes / bitmap bits).
    pub fn slice(&self, offset: usize, len: usize) -> CoreBatch {
        debug_assert!(offset + len <= self.num_rows);
        let columns = self
            .columns
            .iter()
            .map(|c| {
                let nulls = c.nulls.as_ref().map(|bm| bm.slice(offset, len));
                let kind = match &c.kind {
                    CoreColumnKind::Primitive { ty, data } => {
                        let w = ty.byte_width().unwrap_or(0);
                        let from = offset * w;
                        let to = (offset + len) * w;
                        CoreColumnKind::Primitive {
                            ty: ty.clone(),
                            data: Buffer::from_vec(data.as_slice()[from..to].to_vec()),
                        }
                    }
                    CoreColumnKind::Varlen { offsets, data } => {
                        // Offsets may begin before `offset`; copy the payload
                        // window and re-base the offsets from the first value.
                        let elem = std::mem::size_of::<i32>();
                        let offs = offsets.as_slice();
                        let (start_off, end_off) = (
                            i32::from_le_bytes(offs[offset * elem..offset * elem + elem].try_into().unwrap())
                                as usize,
                            i32::from_le_bytes(offs[(offset + len) * elem..(offset + len) * elem + elem]
                                .try_into()
                                .unwrap()) as usize,
                        );
                        let payload = data.as_slice()[start_off..end_off].to_vec();
                        let mut new_offsets = Vec::with_capacity(len + 1);
                        for i in offset..=offset + len {
                            let o = i32::from_le_bytes(
                                offs[i * elem..i * elem + elem].try_into().unwrap(),
                            ) as usize;
                            new_offsets.extend_from_slice(&((o - start_off) as i32).to_le_bytes());
                        }
                        CoreColumnKind::Varlen {
                            offsets: Buffer::from_vec(new_offsets),
                            data: Buffer::from_vec(payload),
                        }
                    }
                    CoreColumnKind::Dictionary { indices, values } => {
                        let from = offset * 4;
                        let to = (offset + len) * 4;
                        CoreColumnKind::Dictionary {
                            indices: Buffer::from_vec(indices.as_slice()[from..to].to_vec()),
                            values: Arc::clone(values),
                        }
                    }
                };
                CoreColumn { kind, nulls }
            })
            .collect();
        CoreBatch {
            schema: Arc::clone(&self.schema),
            columns,
            num_rows: len,
        }
    }

    pub fn into_columns(self) -> Vec<CoreColumn> {
        self.columns
    }
}