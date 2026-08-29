//! FIELD file writer: `create_field`, `update_field`, `delete_field`.
//!
//! See `plan.md` §5.2.1 (pre-allocation & in-place update) and §8.4.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use splayed_format::{fill_null, DataType, FieldHeader, HEADER_SIZE, META_FILE_NAME};

/// One update entry: write `values` starting at absolute row `start_row`.
#[derive(Debug, Clone)]
pub struct UpdateItem {
    pub start_row: u32,
    pub values: Vec<u8>, // raw little-endian bytes, length = count × sizeof(type)
}

impl UpdateItem {
    pub fn new(start_row: u32, values: Vec<u8>) -> Self {
        Self { start_row, values }
    }
}

/// Create a pre-allocated FIELD file (all NULL).
///
/// Reads `.meta` from the same directory, computes `total_rows = sum(time_count)`,
/// and writes the FIELD header + NULL-filled data region.
pub fn create_field(field_path: impl AsRef<Path>, data_type: DataType) -> Result<(), CreateFieldError> {
    let field_path = field_path.as_ref();
    let dir = field_path.parent().ok_or(CreateFieldError::NoParentDir)?;

    // Read .meta from same directory.
    let meta_path = dir.join(META_FILE_NAME);
    let meta_bytes = fs::read(&meta_path).map_err(CreateFieldError::Io)?;
    let meta = splayed_format::MetaFile::deserialize(&meta_bytes)
        .map_err(CreateFieldError::Meta)?;

    let total_rows = meta.total_rows();
    let elem_sz = data_type.size_of();
    let data_length = (total_rows as u64) * (elem_sz as u64);

    let header = splayed_format::new_plain_field_header(data_type, meta.header.generation, total_rows);

    let mut file = File::create(field_path).map_err(CreateFieldError::Io)?;

    // Write header (64 bytes).
    let header_bytes = bytemuck::bytes_of(&header);
    file.write_all(header_bytes).map_err(CreateFieldError::Io)?;

    // Write NULL-filled data region.
    if total_rows > 0 {
        // Fill in chunks to avoid allocating the full buffer at once for huge fields.
        const CHUNK_ROWS: usize = 65536;
        let chunk_bytes = CHUNK_ROWS * elem_sz;
        let mut chunk = vec![0u8; chunk_bytes];
        fill_null(&mut chunk, data_type);

        let mut remaining = data_length;
        while remaining > 0 {
            let to_write = remaining.min(chunk.len() as u64) as usize;
            file.write_all(&chunk[..to_write]).map_err(CreateFieldError::Io)?;
            remaining -= to_write as u64;
        }
    }

    file.sync_all().map_err(CreateFieldError::Io)?;
    Ok(())
}

/// Create a FIELD file **and fill it with data in a single write pass**.
///
/// Reads `.meta` from the same directory for `total_rows` / `generation`,
/// validates that `values` covers exactly `total_rows` elements, then writes
/// the header + data region once — no separate NULL pre-allocation pass.
///
/// Use this when the full column is available up-front (e.g. `create_table`);
/// keep `create_field` + `update_field` for the pre-allocate-then-fill-in-place
/// workflow.
pub fn create_field_with_data(
    field_path: impl AsRef<Path>,
    data_type: DataType,
    values: &[u8],
) -> Result<(), CreateFieldError> {
    let field_path = field_path.as_ref();
    let dir = field_path.parent().ok_or(CreateFieldError::NoParentDir)?;

    // Read .meta from same directory (authoritative row count + generation).
    let meta_path = dir.join(META_FILE_NAME);
    let meta_bytes = fs::read(&meta_path).map_err(CreateFieldError::Io)?;
    let meta = splayed_format::MetaFile::deserialize(&meta_bytes)
        .map_err(CreateFieldError::Meta)?;

    let total_rows = meta.total_rows();
    let elem_sz = data_type.size_of();
    let data_length = (total_rows as usize) * elem_sz;
    if values.len() != data_length {
        return Err(CreateFieldError::LengthMismatch {
            expected: data_length,
            got: values.len(),
        });
    }

    let header = splayed_format::new_plain_field_header(data_type, meta.header.generation, total_rows);

    let mut file = File::create(field_path).map_err(CreateFieldError::Io)?;

    // Write header (64 bytes) + data region in one pass.
    file.write_all(bytemuck::bytes_of(&header))
        .map_err(CreateFieldError::Io)?;
    file.write_all(values).map_err(CreateFieldError::Io)?;

    file.sync_all().map_err(CreateFieldError::Io)?;
    Ok(())
}

/// Update a FIELD file in-place with one or more update items.
///
/// Each item writes raw bytes starting at `start_row × sizeof(type)` offset.
/// The FIELD must be `compression = NONE` (writable).
pub fn update_field(
    field_path: impl AsRef<Path>,
    items: &[UpdateItem],
) -> Result<(), UpdateError> {
    let field_path = field_path.as_ref();

    // Read & validate header.
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(field_path)
        .map_err(UpdateError::Io)?;

    let mut header_buf = [0u8; HEADER_SIZE];
    use std::io::Read;
    file.read_exact(&mut header_buf).map_err(UpdateError::Io)?;
    let header: &FieldHeader = bytemuck::from_bytes(&header_buf);
    header.validate().map_err(UpdateError::Format)?;

    // Must be writable (NONE compression).
    let compression = header.compression().map_err(UpdateError::Format)?;
    if !compression.is_writable() {
        return Err(UpdateError::ReadOnlyAfterCompress);
    }

    let data_type = header.data_type().map_err(UpdateError::Format)?;
    let elem_sz = data_type.size_of();
    let row_count = header.row_count;

    // Validate all items first, then write.
    for item in items {
        if item.values.len() % elem_sz != 0 {
            return Err(UpdateError::UnalignedValues {
                len: item.values.len(),
                elem_sz,
            });
        }
        let n_values = (item.values.len() / elem_sz) as u32;
        let end_row = item
            .start_row
            .checked_add(n_values)
            .ok_or(UpdateError::RowOverflow)?;
        if end_row > row_count {
            return Err(UpdateError::OutOfRange {
                start: item.start_row,
                end: end_row,
                total: row_count,
            });
        }
    }

    // Write each item.
    for item in items {
        let offset = splayed_format::row_byte_offset(data_type, item.start_row);
        file.seek(SeekFrom::Start(offset))
            .map_err(UpdateError::Io)?;
        file.write_all(&item.values)
            .map_err(UpdateError::Io)?;
    }

    // Bump generation.
    let new_gen = header.generation.wrapping_add(1);
    let mut new_header = *header;
    new_header.generation = new_gen;
    file.seek(SeekFrom::Start(0)).map_err(UpdateError::Io)?;
    file.write_all(bytemuck::bytes_of(&new_header))
        .map_err(UpdateError::Io)?;

    file.sync_all().map_err(UpdateError::Io)?;
    Ok(())
}

/// Delete a FIELD file.  No-op if it doesn't exist.
pub fn delete_field(field_path: impl AsRef<Path>) -> Result<(), DeleteFieldError> {
    let field_path = field_path.as_ref();
    match fs::remove_file(field_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(DeleteFieldError::Io(e)),
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum CreateFieldError {
    NoParentDir,
    Io(std::io::Error),
    Meta(splayed_format::MetaError),
    /// `create_field_with_data`: values length ≠ total_rows × element size.
    LengthMismatch { expected: usize, got: usize },
}

impl std::fmt::Display for CreateFieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoParentDir => write!(f, "field path has no parent directory"),
            Self::Io(e) => write!(f, "create_field io error: {e}"),
            Self::Meta(e) => write!(f, "create_field meta error: {e}"),
            Self::LengthMismatch { expected, got } => write!(
                f,
                "create_field_with_data: values length {got} != expected {expected} (total_rows × element size)"
            ),
        }
    }
}
impl std::error::Error for CreateFieldError {}

#[derive(Debug)]
pub enum UpdateError {
    Io(std::io::Error),
    Format(&'static str),
    ReadOnlyAfterCompress,
    UnalignedValues { len: usize, elem_sz: usize },
    OutOfRange { start: u32, end: u32, total: u32 },
    RowOverflow,
}

impl std::fmt::Display for UpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "update_field io error: {e}"),
            Self::Format(msg) => write!(f, "update_field format error: {msg}"),
            Self::ReadOnlyAfterCompress => write!(f, "field is read-only after compression"),
            Self::UnalignedValues { len, elem_sz } => {
                write!(f, "values length {len} is not a multiple of element size {elem_sz}")
            }
            Self::OutOfRange { start, end, total } => {
                write!(f, "update range [{start}, {end}) exceeds total rows {total}")
            }
            Self::RowOverflow => write!(f, "row offset overflow"),
        }
    }
}
impl std::error::Error for UpdateError {}

#[derive(Debug)]
pub enum DeleteFieldError {
    Io(std::io::Error),
}

impl std::fmt::Display for DeleteFieldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "delete_field io error: {e}"),
        }
    }
}
impl std::error::Error for DeleteFieldError {}
