//! FIELD file writer: `create_field`, `update_field`, `delete_field`.
//!
//! See `plan.md` §5.2.1 (pre-allocation & in-place update) and §8.4.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use splayed_format::field_footer::{FOOTER_SIZE, compute_stats, encode_footer, parse_footer};
use splayed_format::{
    fill_null, Compression, DataType, Encoding, FieldHeader, RawValue, HEADER_SIZE,
    META_FILE_NAME,
};

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

/// 拒绝以 `.` 开头的字段文件名（`.meta` 及隐藏/元数据命名）——避免建出
/// 一个会被 `list_fields` 忽略、"看不见"的字段文件。字段名中间含 `.`（如
/// `close.bid`）不受影响。
fn reject_hidden_name(field_path: &Path) -> Result<(), CreateFieldError> {
    if let Some(name) = field_path.file_name().and_then(|s| s.to_str()) {
        if name.starts_with('.') {
            return Err(CreateFieldError::HiddenFileName(name.to_string()));
        }
    }
    Ok(())
}

/// Create a pre-allocated FIELD file (all NULL).
///
/// Reads `.meta` from the same directory, computes `total_rows = sum(time_count)`,
/// and writes the FIELD header + NULL-filled data region.
pub fn create_field(field_path: impl AsRef<Path>, data_type: DataType) -> Result<(), CreateFieldError> {
    let field_path = field_path.as_ref();
    reject_hidden_name(field_path)?;
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
    reject_hidden_name(field_path)?;
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

    let mut header = splayed_format::new_plain_field_header(data_type, meta.header.generation, total_rows);
    // 真实 NULL 计数（区别于预分配占位语义；update_field 增量维护以此为基线）。
    header.null_count = values
        .chunks(elem_sz)
        .filter(|c| *c == data_type.null_bytes())
        .count() as u32;

    let mut file = File::create(field_path).map_err(CreateFieldError::Io)?;

    // Write header (64 bytes) + data region in one pass.
    file.write_all(bytemuck::bytes_of(&header))
        .map_err(CreateFieldError::Io)?;
    file.write_all(values).map_err(CreateFieldError::Io)?;

    // 追加统计 footer（min/max；全 NULL 时 flags=0，仍保留 28 字节定长）。
    let stats = compute_stats(data_type, values);
    file.write_all(&encode_footer(stats)).map_err(CreateFieldError::Io)?;

    file.sync_all().map_err(CreateFieldError::Io)?;
    Ok(())
}

/// Create a FIELD file **directly in encoded/compressed form** — one pass,
/// no separate NULL pre-allocation, no separate `compact_field` step.
///
/// Same contract as [`create_field_with_data`] (reads `.meta` from the same
/// directory, `values` must cover exactly `total_rows` elements) but the data
/// region is encoded (DELTA/RLE/BITPACK) and/or compressed (ZSTD/LZ4) at write
/// time. `encoding = PLAIN, compression = NONE` produces the identical on-disk
/// layout as [`create_field_with_data`].
///
/// The resulting FIELD is **read-only** (`update_field` rejects writes), so
/// this suits "write once, read many" columns — e.g. 90%~99% NULL factor data
/// written directly as `RLE + ZSTD` without the extra compact pass.
pub fn create_field_with_data_encoded(
    field_path: impl AsRef<Path>,
    data_type: DataType,
    values: &[u8],
    encoding: Encoding,
    compression: Compression,
) -> Result<(), CreateFieldError> {
    let field_path = field_path.as_ref();
    reject_hidden_name(field_path)?;
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

    // 编码（可选）+ 压缩（可选）→ 数据区（与 compact_field_with_encoding 布局一致）。
    let data_region = splayed_codec::encode_compress_data(encoding, compression, data_type, values)
        .map_err(CreateFieldError::Codec)?;

    let mut header = FieldHeader::new(
        data_type,
        encoding,
        compression,
        meta.header.generation,
        total_rows,
        data_region.len() as u64,
    );
    // 真实 NULL 计数（从原始值统计；压缩字段只读，永久有效）。
    header.null_count = values
        .chunks(elem_sz)
        .filter(|c| *c == data_type.null_bytes())
        .count() as u32;

    let mut file = File::create(field_path).map_err(CreateFieldError::Io)?;

    // Write header (64 bytes) + encoded/compressed data region in one pass.
    file.write_all(bytemuck::bytes_of(&header))
        .map_err(CreateFieldError::Io)?;
    file.write_all(&data_region).map_err(CreateFieldError::Io)?;

    // 统计 footer（按原始值重算，永久有效——压缩字段只读）。
    let stats = compute_stats(data_type, values);
    file.write_all(&encode_footer(stats)).map_err(CreateFieldError::Io)?;

    file.sync_all().map_err(CreateFieldError::Io)?;
    Ok(())
}
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

    // 写前快照被覆盖行段的旧值（统计维护需要「覆盖前」的值判断 null/极值）。
    let mut old_spans: Vec<(u32, Vec<u8>)> = Vec::with_capacity(items.len());
    for item in items {
        let offset = splayed_format::row_byte_offset(data_type, item.start_row);
        let mut old = vec![0u8; item.values.len()];
        file.seek(SeekFrom::Start(offset))
            .map_err(UpdateError::Io)?;
        file.read_exact(&mut old).map_err(UpdateError::Io)?;
        old_spans.push((item.start_row, old));
    }

    // Write each item.
    for item in items {
        let offset = splayed_format::row_byte_offset(data_type, item.start_row);
        file.seek(SeekFrom::Start(offset))
            .map_err(UpdateError::Io)?;
        file.write_all(&item.values)
            .map_err(UpdateError::Io)?;
    }

    // Bump generation（header 末尾统一重写，含 null_count 增量）。
    let new_gen = header.generation.wrapping_add(1);
    let mut new_header = *header;
    new_header.generation = new_gen;

    // -- 统计维护（增量，O(k)；命中旧极值才全列重扫）----------------------------
    // 读取待写入行段的旧值（null 增量 + 极值命中检测）。
    let mut null_delta: i64 = 0;
    let mut batch_min: Option<[u8; 8]> = None; // 写入批次（非 NULL）极值槽
    let mut batch_max: Option<[u8; 8]> = None;
    let mut extreme_touched = false;

    let file_len = file.metadata().map_err(UpdateError::Io)?.len();
    let footer_old = if file_len >= (HEADER_SIZE + FOOTER_SIZE) as u64 {
        let mut tail = [0u8; FOOTER_SIZE];
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))
            .map_err(UpdateError::Io)?;
        file.read_exact(&mut tail).map_err(UpdateError::Io)?;
        parse_footer(&tail)
    } else {
        None
    };
    let old_valid = footer_old.map(|(v, _, _)| v).unwrap_or(false);
    let (old_min, old_max) = footer_old
        .map(|(_, mn, mx)| (mn, mx))
        .unwrap_or(([0u8; 8], [0u8; 8]));

    for (item, (_, old_bytes)) in items.iter().zip(old_spans.iter()) {
        let n_values = (item.values.len() / elem_sz) as u32;

        let nulls = data_type.null_bytes();
        for i in 0..n_values as usize {
            let old = &old_bytes[i * elem_sz..(i + 1) * elem_sz];
            let new = &item.values[i * elem_sz..(i + 1) * elem_sz];
            let old_null = old == nulls;
            let new_null = new == nulls;
            if old_null != new_null {
                null_delta += if new_null { 1 } else { -1 };
            }
            if old_valid && !old_null
                && (old == &old_min[..elem_sz] || old == &old_max[..elem_sz])
            {
                extreme_touched = true;
            }
            if !new_null {
                let mut slot = [0u8; 8];
                slot[..elem_sz].copy_from_slice(new);
                batch_min = Some(match batch_min {
                    Some(m) if slot_cmp(data_type, &m, &slot) == std::cmp::Ordering::Less => m,
                    _ => slot,
                });
                batch_max = Some(match batch_max {
                    Some(m) if slot_cmp(data_type, &m, &slot) == std::cmp::Ordering::Greater => m,
                    _ => slot,
                });
            }
        }
    }

    // header null_count 精确维护（增量）。
    let new_null_count = (header.null_count as i64 + null_delta).max(0) as u32;
    new_header.null_count = new_null_count;

    // footer：命中旧极值 → 全列重扫精确重算；否则 O(1) 保守上界。
    if old_valid {
        let stats_opt = if extreme_touched {
            // 覆盖了旧极值所在行：精确重算（写一次读多次，罕见路径）。
            let data_len = header.data_length as usize;
            let mut all = vec![0u8; data_len];
            file.seek(SeekFrom::Start(HEADER_SIZE as u64))
                .map_err(UpdateError::Io)?;
            file.read_exact(&mut all).map_err(UpdateError::Io)?;
            splayed_format::field_footer::compute_stats(data_type, &all)
        } else {
            // 保守上界：新区间 ⊇ 真实区间（永远安全）。
            let mut mn = old_min;
            let mut mx = old_max;
            if let Some(b) = batch_min {
                if slot_cmp(data_type, &b, &mn) == std::cmp::Ordering::Less {
                    mn = b;
                }
            }
            if let Some(b) = batch_max {
                if slot_cmp(data_type, &b, &mx) == std::cmp::Ordering::Greater {
                    mx = b;
                }
            }
            Some((mn, mx))
        };
        let footer = splayed_format::field_footer::encode_footer(stats_opt);
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))
            .map_err(UpdateError::Io)?;
        file.write_all(&footer).map_err(UpdateError::Io)?;
    } else {
        // 无有效 footer（从未建立或已失效）：保持无统计（归档时重算）。
        let _ = (old_min, old_max);
    }

    // 先重写 header（generation + null_count）。
    file.seek(SeekFrom::Start(0)).map_err(UpdateError::Io)?;
    file.write_all(bytemuck::bytes_of(&new_header))
        .map_err(UpdateError::Io)?;

    file.sync_all().map_err(UpdateError::Io)?;
    Ok(())
}

/// 槽值数值比较（小端；按类型语义：有符号/无符号/浮点/BOOL）。
fn slot_cmp(data_type: DataType, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    use splayed_format::DataType;
    let av = RawValue::read_le(a, 0, data_type);
    let bv = RawValue::read_le(b, 0, data_type);
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Date32
        | DataType::Date64
        | DataType::TimestampUs => av.as_i64().cmp(&bv.as_i64()),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            av.as_u64().cmp(&bv.as_u64())
        }
        DataType::Float32 | DataType::Float64 => {
            av.as_f64()
                .partial_cmp(&bv.as_f64())
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        DataType::Bool => av.as_bool().cmp(&bv.as_bool()),
    }
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
    /// 字段文件名以 `.` 开头（`.meta` 等隐藏/元数据命名，会被 `list_fields` 忽略）。
    HiddenFileName(String),
    /// `create_field_with_data_encoded`: 编码/压缩失败。
    Codec(splayed_codec::CodecError),
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
            Self::HiddenFileName(name) => write!(
                f,
                "field file name '{name}' starts with '.' — leading-dot (hidden) field names are not allowed"
            ),
            Self::Codec(e) => write!(f, "create_field_with_data_encoded codec error: {e}"),
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
