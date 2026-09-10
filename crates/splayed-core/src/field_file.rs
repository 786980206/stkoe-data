use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{Mmap, MmapMut};
use splayed_codec::{decode_chunk, encode_chunk};
use splayed_format::{
    bitmap_count_ones, bitmap_fill_bits, Bitmap, BitmapView, Buffer, BufferView, Column,
    ColumnSegment, ColumnView, Compression, DataType, Encoding, FieldHeader, FieldSchema,
    HEADER_SIZE, validity_size,
};

use crate::error::{CoreError, Mode};
use crate::scan::{read_row_scalar, CmpOp, Predicate, RowRange, Scalar, ScanRequest};

/// 单列值写入 / 初始化的流式读取器（`create_field_file` 的 `stream` init）。
///
/// 两相迭代：先 `next_values()` 消费全部 values，再 `next_validity()` 消费全部
/// validity 位。分离后 create_field_file 可以纯顺序写（DATA → VALIDITY），
/// 不需要内存中位级拼接。
pub trait FieldChunkReader {
    /// Phase 1：返回下一批 values（`rows * size_of(type)` 字节）；`None` = values 结束。
    fn next_values(&mut self) -> Result<Option<StreamValues>, CoreError>;

    /// Phase 2：返回下一批 validity 字节（`ceil(rows / 8)`）；`None` = validity 结束。
    /// 必须在 `next_values()` 返回 `None` 之后调用。
    fn next_validity(&mut self) -> Result<Option<Vec<u8>>, CoreError>;
}

/// 流式 values 批次。
#[derive(Debug, Clone)]
pub struct StreamValues {
    pub values: Vec<u8>,
    pub rows: usize,
}

/// 创建即压缩的默认 chunk 行数（与 `compress_field_file` 均匀分块一致）。
const CREATE_CHUNK_ROWS: usize = 8192;

pub(crate) fn compute_chunk_boundaries(
    rows_total: usize,
    chunk_offsets: Option<&[u64]>,
) -> Result<Vec<(usize, usize)>, CoreError> {
    match chunk_offsets {
        Some(list) => {
            if list.first() != Some(&0) {
                return Err(CoreError::Invalid("chunk offsets must start at 0".into()));
            }
            let mut list = list.to_vec();
            list.sort_unstable();
            list.dedup();
            if list.windows(2).any(|w| w[0] >= w[1])
                || list.iter().any(|&o| o >= rows_total as u64)
            {
                return Err(CoreError::Invalid(
                    "chunk offsets must be strictly ascending within [0, row_count)".into(),
                ));
            }
            let mut ranges: Vec<(usize, usize)> = list
                .windows(2)
                .map(|w| (w[0] as usize, w[1] as usize))
                .collect();
            let last = *list.last().unwrap() as usize;
            if last < rows_total {
                ranges.push((last, rows_total));
            }
            Ok(ranges)
        }
        None => Ok((0..rows_total)
            .step_by(CREATE_CHUNK_ROWS)
            .map(|o| (o, (o + CREATE_CHUNK_ROWS).min(rows_total)))
            .collect()),
    }
}

/// 拥有型 Data 的**单遍 chunked 直接创建**（压缩创建主路径）：
/// 占位 header → 按 `CREATE_CHUNK_ROWS` 行切 chunk、逐 chunk `encode_chunk`
/// 顺序直写（values 零拷贝切片、validity 按位切片重打包）→ 回填 header。
/// header 约定与 `compress_field_file_encoded` 输出一致：chunked、
/// has_validity = false（chunk 自描述携带 validity）、data_length = 逻辑字节数；
/// null_count 精确（打开时会从解压位图重建，写值仅为一致性）。
fn create_field_file_chunked(
    path: &Path,
    data_type: DataType,
    init: FieldInit,
    options: &CreateFieldOptions,
) -> Result<(), CoreError> {
    let compression = options.compression;
    // chunk 边界：显式 offsets（sym 对齐）或均匀 8192 行（与 compress 均匀分块一致）
    let rows_total: usize = match &init {
        FieldInit::Data(col) => col.length(),
        FieldInit::Length(n) => *n as usize,
        FieldInit::Stream { .. } => {
            return Err(CoreError::Invalid("chunked creation requires Data/Length init".into()))
        }
    };
    let boundaries = compute_chunk_boundaries(rows_total, options.chunk_offsets.as_deref())?;
    match init {
        FieldInit::Data(col) => {
            create_field_file_chunked_data(path, data_type, col, compression, &boundaries)
        }
        FieldInit::Length(n) => {
            create_field_file_chunked_all_null(path, data_type, n as usize, compression, &boundaries)
        }
        FieldInit::Stream { .. } => unreachable!("handled at entry"),
    }
}

/// 拥有型 Data 的**单遍 chunked 直接创建**（压缩创建主路径）：
/// 占位 header → 按边界切 chunk、逐 chunk `encode_chunk` 顺序直写（values 零拷贝
/// 切片、validity 按位切片重打包）→ 回填 header。header 约定与
/// `compress_field_file_encoded` 输出一致：chunked、has_validity = false（chunk
/// 自描述携带 validity）、data_length = 逻辑字节数；null_count 精确（打开时会从
/// 解压位图重建，写值仅为一致性）。
fn create_field_file_chunked_data(
    path: &Path,
    data_type: DataType,
    col: Column,
    compression: Compression,
    boundaries: &[(usize, usize)],
) -> Result<(), CoreError> {
    if col.data_type != data_type {
        return Err(CoreError::Invalid(format!(
            "init data type {:?} does not match requested {data_type:?}",
            col.data_type
        )));
    }
    let rows = col.length();
    // 每行字节数：定宽 = size_of；Utf8 = 4（字典 keys）
    let per_row = if rows > 0 { col.values.len() / rows } else { 0 };
    let mut f = File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| map_io(path, e))?;

    // Phase 1: 占位 HEADER
    f.write_all(&[0u8; HEADER_SIZE])?;

    // Phase 2: 逐 chunk 编码直写（values 零拷贝切片；validity 按位切片重打包）
    let bits = col.validity.as_ref().map(|b| b.as_view());
    for &(lo, hi) in boundaries {
        let take = hi - lo;
        let values = &col.values.as_slice()[lo * per_row..hi * per_row];
        let validity = bits.as_ref().map(|b| {
            // chunk 起点非字节对齐：按位切片后重新打包
            b.slice(lo, take).and_then(|v| Ok(v.to_packed_bytes()))
        }).transpose()?;
        let chunk = encode_chunk(
            Encoding::Plain,
            compression,
            data_type,
            values,
            validity.as_deref(),
            take,
        )?;
        f.write_all(&chunk)?;
    }

    // Phase 3: HEADER 回填（与 compress_field_file_encoded 输出约定一致）
    let mut header = FieldHeader::new_uncompressed(
        data_type,
        1,
        rows as u32,
        col.null_count() as u32,
        false,
    );
    header.compression = compression.id();
    header.data_length = col.values.len() as u64;
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&header.to_bytes())?;
    Ok(())
}

/// 全 NULL 字段的 chunked 创建（`Length` + 压缩）：每 chunk 的 values 为零填充、
/// validity 为全 0 位（行级 NULL 由位图表达，压缩后全零 chunk 体积极小）。
/// 后续 `write_dataset` 写入走 working 表示、close 按原 chunk 分组重压缩——
/// 字段生命周期保持压缩。
fn create_field_file_chunked_all_null(
    path: &Path,
    data_type: DataType,
    rows: usize,
    compression: Compression,
    _boundaries: &[(usize, usize)],
) -> Result<(), CoreError> {
    let size = data_type.size_of();
    let mut f = File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| map_io(path, e))?;

    // 全 NULL（Length）字段：仅写入 64 字节 HEADER 表示，不向磁盘写入空 chunk
    let mut header = FieldHeader::new_uncompressed(data_type, 1, rows as u32, rows as u32, true);
    header.compression = compression.id();
    header.data_length = rows as u64 * size as u64;
    f.write_all(&header.to_bytes())?;
    Ok(())
}


/// `create_field_file` 的初始化方式（docs/splayed-core.md §5.1）：
/// 分配与写值一步完成，不做先预分配再写值的二次写入。
pub enum FieldInit {
    /// 指定逻辑长度的空占位 Field（全 NULL，validity 全 0）。
    Length(u64),
    /// 以给定数据初始化；`row_count` = 数据长度。
    Data(Column),
    /// 流式初始化；最终长度无需预先知道，由流结束决定。
    Stream { reader: Box<dyn FieldChunkReader> },
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

/// 唯一临时文件路径：`{path}.{tag}.{pid}.{n}.tmp`，并发的结构性操作互不覆盖。
fn unique_tmp_path(path: &Path, tag: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let mut s = path.as_os_str().to_os_string();
    s.push(format!(".{tag}.{}.{}.tmp", std::process::id(), seq));
    PathBuf::from(s)
}

fn map_io(path: &Path, e: std::io::Error) -> CoreError {
    if e.kind() == std::io::ErrorKind::NotFound {
        CoreError::NotFound(path.to_path_buf())
    } else {
        CoreError::Io(e)
    }
}

/// 创建并初始化一个 Field 文件（创建完成后才可被 `open_field_file` 打开）。
///
/// 统一三阶段顺序写：HEADER 占位 → DATA 顺序写 → VALIDITY 顺序写 → HEADER 回填。
/// 不拷贝、不拼接、不预构造大 Buffer。
#[allow(unused_assignments)] // match arms 内赋值后由 HEADER 回填统一读取
/// 创建 Field 文件的选项（`CreateFieldOptions::default()` = 未压缩 + 均匀分块）。
#[derive(Debug, Clone)]
pub struct CreateFieldOptions {
    /// chunk 压缩算法（默认 None = 未压缩 PLAIN + NONE 路径）。
    pub compression: Compression,
    /// chunk 起始行号：升序、首项 0、末块隐含延伸到 row_count；用于按 sym 边界
    /// 对齐 chunk（上层由 META 网格推导）。`None` = 按 8192 行均匀分块。
    /// 仅 `compression != None` 时生效。
    pub chunk_offsets: Option<Vec<u64>>,
}

impl Default for CreateFieldOptions {
    fn default() -> Self {
        CreateFieldOptions { compression: Compression::None, chunk_offsets: None }
    }
}

pub fn column_view_to_owned_column(view: &ColumnView<'_>) -> Column {
    let dt = view.data_type();
    if view.segments().len() == 1 {
        let seg = &view.segments()[0];
        let validity = seg.validity().map(|b| Bitmap::from_bytes(b.to_packed_bytes(), b.len()));
        match (dt, seg.values()) {
            (DataType::Utf8, splayed_format::ColumnValues::Dict { keys, dict_offsets, dict_strings }) => {
                let keys: Vec<u32> = keys
                    .as_slice()
                    .chunks_exact(4)
                    .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
                    .collect();
                let offs: Vec<u64> = dict_offsets
                    .as_slice()
                    .chunks_exact(8)
                    .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                    .collect();
                Column::from_dict(keys, offs, dict_strings.as_slice().to_vec(), validity)
            }
            (DataType::Utf8, splayed_format::ColumnValues::RepeatDict { dict_offsets, dict_strings, dict_index }) => {
                let offs: Vec<u64> = dict_offsets
                    .as_slice()
                    .chunks_exact(8)
                    .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                    .collect();
                let keys: Vec<u32> = vec![*dict_index; seg.rows()];
                Column::from_dict(keys, offs, dict_strings.as_slice().to_vec(), validity)
            }
            _ => Column {
                data_type: dt,
                values: Buffer::from_vec(seg.fixed_bytes().unwrap_or(&[]).to_vec()),
                validity,
                dict: None,
            },
        }
    } else {
        let total_rows = view.length();
        let mut vals = Vec::new();
        let mut bit_bm = Bitmap::ones(total_rows);
        let mut has_null = false;
        let mut curr_row = 0;
        for s in view.segments() {
            if let Some(b) = s.fixed_bytes() {
                vals.extend_from_slice(b);
            }
            if let Some(v) = s.validity() {
                has_null = true;
                for i in 0..s.rows() {
                    bit_bm.set(curr_row + i, v.is_valid(i));
                }
            }
            curr_row += s.rows();
        }
        Column {
            data_type: dt,
            values: Buffer::from_vec(vals),
            validity: if has_null { Some(bit_bm) } else { None },
            dict: None,
        }
    }
}

/// 创建空字段骨架或指定行数的全 NULL 字段（当 rows = None 或 rows = Some(0) 时为 0 行骨架；当 rows > 0 时为指定行数的全 NULL Header-Only 延迟展开字段）。
pub fn create_field(
    path: &Path,
    field_type: DataType,
    rows: Option<u64>,
    options: Option<CreateFieldOptions>,
) -> Result<(), CoreError> {
    create_field_file(path, field_type, FieldInit::Length(rows.unwrap_or(0)), options.unwrap_or_default())
}

/// 连带数据初始化创建字段文件（带数据一步直写，全 NULL 产出 64B Header-Only 文件，零缓冲内存分配）。
pub fn init_field(
    path: &Path,
    column: &ColumnView<'_>,
    options: Option<CreateFieldOptions>,
) -> Result<(), CoreError> {
    create_field_file_from_view(path, column, &options.unwrap_or_default())
}

/// 指定行数初始化全 NULL 字段文件（64B Header-Only 延迟展开，零磁盘数据页分配）。
pub fn init_field_empty(
    path: &Path,
    data_type: DataType,
    rows: u64,
    options: Option<CreateFieldOptions>,
) -> Result<(), CoreError> {
    create_field_file(path, data_type, FieldInit::Length(rows), options.unwrap_or_default())
}

/// 直接基于 `&ColumnView` 零拷贝写入 Field 文件。
pub fn create_field_file_from_view(
    path: &Path,
    column: &ColumnView<'_>,
    options: &CreateFieldOptions,
) -> Result<(), CoreError> {
    if path.exists() {
        return Err(CoreError::AlreadyExists(path.to_path_buf()));
    }
    let data_type = column.data_type();
    let rows = column.length();
    let null_count = column.null_count();

    // 1. 全 NULL 字段：仅写 64B Header 骨架
    if rows > 0 && null_count == rows {
        if matches!(options.compression, Compression::None) {
            return create_field_file_plain(path, data_type, FieldInit::Length(rows as u64));
        } else if data_type != DataType::Utf8 {
            return create_field_file_chunked(path, data_type, FieldInit::Length(rows as u64), options);
        }
    }

    // 2. 未压缩或 Utf8 字段：走单遍直写
    if matches!(options.compression, Compression::None) || data_type == DataType::Utf8 {
        return create_field_file_view_plain(path, column);
    }

    // 3. 压缩单段：直接在原有内存上切片逐 chunk 编码直写（零多余分配）
    if column.segments().len() == 1 {
        return create_field_file_view_chunked(path, column, options);
    }

    // 多段回退路径
    let col = column_view_to_owned_column(column);
    create_field_file_chunked(path, data_type, FieldInit::Data(col), options)
}

fn create_field_file_view_plain(path: &Path, column: &ColumnView<'_>) -> Result<(), CoreError> {
    let mut f = File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| map_io(path, e))?;

    // Phase 1: 占位 Header
    f.write_all(&[0u8; HEADER_SIZE])?;

    let dt = column.data_type();
    let rows = column.length();
    let null_count = column.null_count();
    let mut has_validity = false;
    let mut data_len = 0u64;

    // Phase 2: DATA 顺序写
    for seg in column.segments() {
        if let Some(b) = seg.fixed_bytes() {
            f.write_all(b)?;
            data_len += b.len() as u64;
        } else if let splayed_format::ColumnValues::Dict { keys, .. } = seg.values() {
            let kb = keys.as_slice();
            f.write_all(kb)?;
            data_len += kb.len() as u64;
        }
        if seg.validity().is_some() {
            has_validity = true;
        }
    }

    // Phase 3: VALIDITY 顺序写
    if has_validity {
        if column.segments().len() == 1 {
            if let Some(bm) = column.segments()[0].validity() {
                f.write_all(bm.as_raw())?;
            }
        } else {
            let mut bit_bm = Bitmap::ones(rows);
            let mut curr_row = 0;
            for s in column.segments() {
                if let Some(v) = s.validity() {
                    for i in 0..s.rows() {
                        bit_bm.set(curr_row + i, v.is_valid(i));
                    }
                }
                curr_row += s.rows();
            }
            f.write_all(bit_bm.as_view().as_raw())?;
        }
    }

    let mut header = FieldHeader::new_uncompressed(
        dt,
        1,
        rows as u32,
        null_count as u32,
        has_validity,
    );
    header.data_length = data_len;
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&header.to_bytes())?;
    Ok(())
}

fn create_field_file_view_chunked(
    path: &Path,
    column: &ColumnView<'_>,
    options: &CreateFieldOptions,
) -> Result<(), CoreError> {
    let seg = &column.segments()[0];
    let data_type = column.data_type();
    let rows = column.length();
    let values_bytes = seg.fixed_bytes().ok_or_else(|| {
        CoreError::Invalid("chunked field requires fixed width values".into())
    })?;
    let per_row = if rows > 0 { values_bytes.len() / rows } else { 0 };

    let boundaries = compute_chunk_boundaries(rows, options.chunk_offsets.as_deref())?;

    let mut f = File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| map_io(path, e))?;

    // Phase 1: 占位 Header
    f.write_all(&[0u8; HEADER_SIZE])?;

    // Phase 2: 逐 chunk 编码直写
    let bits = seg.validity();
    for &(lo, hi) in &boundaries {
        let take = hi - lo;
        let values = &values_bytes[lo * per_row..hi * per_row];
        let validity = bits.map(|b| {
            b.slice(lo, take).and_then(|v| Ok(v.to_packed_bytes()))
        }).transpose()?;
        let chunk = encode_chunk(
            Encoding::Plain,
            options.compression,
            data_type,
            values,
            validity.as_deref(),
            take,
        )?;
        f.write_all(&chunk)?;
    }

    // Phase 3: Header 回填
    let mut header = FieldHeader::new_uncompressed(
        data_type,
        1,
        rows as u32,
        column.null_count() as u32,
        false,
    );
    header.compression = options.compression.id();
    header.data_length = values_bytes.len() as u64;
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&header.to_bytes())?;
    Ok(())
}

/// 只读字段对象
pub struct FieldReader {
    inner: FieldHandle,
}

impl FieldReader {
    pub fn open(path: &Path) -> Result<Self, CoreError> {
        let inner = open_field_file(path, Mode::Read)?;
        Ok(Self { inner })
    }

    pub fn read(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError> {
        self.inner.read(offset, length)
    }

    pub fn scan(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError> {
        self.inner.scan(request)
    }

    pub fn schema(&self) -> FieldSchema {
        self.inner.schema()
    }

    pub fn row_count(&self) -> u64 {
        self.inner.row_count()
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn close(self) -> Result<(), CoreError> {
        self.inner.close_field()
    }
}

/// 可写字段对象
pub struct FieldWriter {
    inner: FieldHandle,
}

impl FieldWriter {
    pub fn create(
        path: &Path,
        field_type: DataType,
        rows: Option<u64>,
        options: Option<CreateFieldOptions>,
    ) -> Result<Self, CoreError> {
        create_field(path, field_type, rows, options)?;
        Self::open(path)
    }

    pub fn init(path: &Path, data: &ColumnView<'_>, options: Option<CreateFieldOptions>) -> Result<Self, CoreError> {
        init_field(path, data, options)?;
        Self::open(path)
    }

    /// 指定行数初始化全 NULL 字段（64B Header-Only 延迟展开）
    pub fn init_empty(path: &Path, data_type: DataType, rows: u64, options: Option<CreateFieldOptions>) -> Result<Self, CoreError> {
        init_field_empty(path, data_type, rows, options)?;
        Self::open(path)
    }

    pub fn open(path: &Path) -> Result<Self, CoreError> {
        let inner = open_field_file(path, Mode::Write)?;
        Ok(Self { inner })
    }

    pub fn write(&mut self, offset: u64, data: &ColumnView<'_>) -> Result<(), CoreError> {
        self.inner.write(offset, data)
    }

    pub fn read(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError> {
        self.inner.read(offset, length)
    }

    pub fn scan(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError> {
        self.inner.scan(request)
    }

    pub fn update(&mut self, data: &ColumnView<'_>) -> Result<(), CoreError> {
        self.inner.update(data)
    }

    pub fn rename(&mut self, new_name: &str) -> Result<(), CoreError> {
        let old_path = self.inner.path.clone();
        let parent = old_path.parent().unwrap_or_else(|| Path::new("."));
        let target = parent.join(new_name);
        if target.exists() {
            return Err(CoreError::AlreadyExists(target));
        }
        self.inner.mode = Mode::Read;
        drop(std::mem::replace(&mut self.inner.backing, Backing::Empty));
        self.inner.working = None;
        fs::rename(&old_path, &target).map_err(|e| map_io(&old_path, e))?;
        self.inner = open_field_file(&target, Mode::Write)?;
        Ok(())
    }

    pub fn cast(&mut self, target_type: DataType) -> Result<(), CoreError> {
        let path = self.inner.path.clone();
        self.inner.mode = Mode::Read;
        drop(std::mem::replace(&mut self.inner.backing, Backing::Empty));
        self.inner.working = None;
        cast_field_file(&path, target_type)?;
        self.inner = open_field_file(&path, Mode::Write)?;
        Ok(())
    }

    pub fn compress(&mut self) -> Result<(), CoreError> {
        let path = self.inner.path.clone();
        self.inner.mode = Mode::Read;
        drop(std::mem::replace(&mut self.inner.backing, Backing::Empty));
        self.inner.working = None;
        compress_field_file(&path, None)?;
        self.inner = open_field_file(&path, Mode::Write)?;
        Ok(())
    }

    pub fn decompress(&mut self) -> Result<(), CoreError> {
        let path = self.inner.path.clone();
        self.inner.mode = Mode::Read;
        drop(std::mem::replace(&mut self.inner.backing, Backing::Empty));
        self.inner.working = None;
        decompress_field_file(&path)?;
        self.inner = open_field_file(&path, Mode::Write)?;
        Ok(())
    }

    pub fn update_header(&mut self, header: FieldHeader) -> Result<(), CoreError> {
        self.inner.update_header(header)
    }

    pub fn schema(&self) -> FieldSchema {
        self.inner.schema()
    }

    pub fn row_count(&self) -> u64 {
        self.inner.row_count()
    }

    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    pub fn as_reader(&self) -> Result<FieldReader, CoreError> {
        FieldReader::open(self.inner.path())
    }

    pub fn close(self) -> Result<(), CoreError> {
        self.inner.close_field()
    }

    pub fn remove(self) -> Result<(), CoreError> {
        let path = self.inner.path.clone();
        self.close()?;
        delete_field_file(&path)
    }
}

/// 打开已有 Field（open 不负责创建）。
#[inline]
pub fn open_field(path: &Path, mode: Mode) -> Result<FieldHandle, CoreError> {
    open_field_file(path, mode)
}

/// 关闭 Handle。
#[inline]
pub fn close_field(handle: FieldHandle) -> Result<(), CoreError> {
    close_field_handle(handle)
}

/// 销毁删除字段（安全释放句柄并删除物理文件）。
#[inline]
pub fn drop_field(handle: FieldHandle) -> Result<(), CoreError> {
    drop_field_handle(handle)
}

#[inline]
pub fn drop_field_handle(handle: FieldHandle) -> Result<(), CoreError> {
    let path = handle.path.clone();
    close_field(handle)?;
    delete_field_file(&path)
}

/// 路径删除物理文件。
#[inline]
pub fn drop_field_path(path: &Path) -> Result<(), CoreError> {
    delete_field_file(path)
}

/// 读取字段元数据结构（只读 64B Header，不建立 Mmap 映射）。
pub fn read_field_schema(path: &Path) -> Result<FieldSchema, CoreError> {
    let mut f = File::open(path).map_err(|e| map_io(path, e))?;
    let mut buf = [0u8; HEADER_SIZE];
    f.read_exact(&mut buf).map_err(|e| map_io(path, e))?;
    let header = FieldHeader::from_bytes(&buf)?;
    let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
    Ok(FieldSchema::new(name, header.data_type()?))
}

/// 同目录内重命名 Field 文件。
#[inline]
pub fn rename_field(path: &Path, new_name: &str) -> Result<(), CoreError> {
    rename_field_file(path, new_name)
}

/// 字段类型就地转换（基于物理路径执行流式批次转换，避免 Mmap 占用产生 Windows 文件锁冲突与陈旧句柄）。
pub fn cast_field(path: &Path, target_type: DataType) -> Result<(), CoreError> {
    cast_field_file(path, target_type)
}

#[inline]
pub fn cast_field_path(path: &Path, target_type: DataType) -> Result<(), CoreError> {
    cast_field_file(path, target_type)
}

/// 字段压缩（基于物理路径）。
pub fn compress_field(path: &Path, offsets: Option<Vec<u64>>) -> Result<(), CoreError> {
    compress_field_file(path, offsets)
}

#[inline]
pub fn compress_field_path(path: &Path, offsets: Option<Vec<u64>>) -> Result<(), CoreError> {
    compress_field_file(path, offsets)
}

/// 字段解压（基于物理路径）。
pub fn decompress_field(path: &Path) -> Result<(), CoreError> {
    decompress_field_file(path)
}

#[inline]
pub fn decompress_field_path(path: &Path) -> Result<(), CoreError> {
    decompress_field_file(path)
}

/// 创建 Field 文件。
///
/// `compression = None` → 未压缩（PLAIN + NONE）路径；`Length`（全 NULL）+ 压缩 →
/// chunked 全 NULL（后续 write 生命周期保持压缩，行级 NULL 由 validity 位图表达）；
/// `Data` + 压缩 → **单遍 chunked 直接创建**（逐 chunk `encode_chunk` 顺序直写，
/// 内存 O(单 chunk)，无 tmp / 无二次读）；`Stream` + 压缩 → 组合路径（两阶段
/// reader 协议使单遍编码需物化全列：流式写未压缩 O(1) 内存 +
/// `compress_field_file_encoded` 原地压缩 O(单 chunk)）。
pub fn create_field_file(
    path: &Path,
    data_type: DataType,
    init: FieldInit,
    options: CreateFieldOptions,
) -> Result<(), CoreError> {
    if path.exists() {
        return Err(CoreError::AlreadyExists(path.to_path_buf()));
    }
    if matches!(options.compression, Compression::None) {
        return create_field_file_plain(path, data_type, init);
    }
    if data_type == DataType::Utf8 {
        // encode_chunk 值域为定宽（Utf8 keys 不适用）——压缩创建不支持 Utf8，
        // 保持未压缩（与 compress_field_file 的既有约束一致）
        return create_field_file_plain(path, data_type, init);
    }
    if matches!(init, FieldInit::Data(_) | FieldInit::Length(_)) {
        return create_field_file_chunked(path, data_type, init, &options);
    }
    create_field_file_plain(path, data_type, init)?;
    compress_field_file_encoded(
        path,
        options.chunk_offsets,
        Encoding::Plain,
        options.compression,
    )
}

/// 未压缩（PLAIN + NONE）创建路径：Length 稀疏 / Data 直写 / Stream 直写，
/// 调用方保证 path 不存在。
fn create_field_file_plain(
    path: &Path,
    data_type: DataType,
    init: FieldInit,
) -> Result<(), CoreError> {
    let mut f = File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| map_io(path, e))?;

    // Phase 1: 占位 HEADER
    f.write_all(&[0u8; HEADER_SIZE])?;

    let row_count: u32;
    let mut data_length: u64 = 0;
    let mut null_count: u32 = 0;
    let has_validity;

    match init {
        FieldInit::Length(n) => {
            row_count = n as u32;
            null_count = n as u32;
            has_validity = true;
            // 全 NULL 字段：仅写入 64 字节 HEADER 表示，不向磁盘写入 DATA / VALIDITY 区
        }
        FieldInit::Data(col) => {
            if col.data_type != data_type {
                return Err(CoreError::Invalid(format!(
                    "init data type {:?} does not match requested {data_type:?}",
                    col.data_type
                )));
            }
            row_count = col.length() as u32;
            has_validity = col.validity.is_some();

            // Phase 2: DATA 顺序写
            f.write_all(col.values.as_slice())?;

            // Phase 3: VALIDITY 顺序写
            if let Some(bm) = &col.validity {
                null_count = bm.as_view().null_count() as u32;
                f.write_all(bm.as_view().as_raw())?;
            }
        }
        FieldInit::Stream { mut reader } => {
            has_validity = true; // stream 始终写 validity 区

            // Phase 2: DATA 顺序写
            let mut total = 0u32;
            while let Some(batch) = reader.next_values()? {
                f.write_all(&batch.values)?;
                total += batch.rows as u32;
                data_length += batch.values.len() as u64;
            }
            row_count = total;

            // Phase 3: VALIDITY 顺序写（顺带 word 批量 popcount 精确计 null_count）
            f.seek(SeekFrom::Start(HEADER_SIZE as u64 + data_length))?;
            let validity_bytes = validity_size(row_count);
            let total_bits = row_count as u64;
            let mut ones = 0u64;
            let mut written_bits = 0u64;
            let mut written = 0usize;
            while written < validity_bytes {
                match reader.next_validity()? {
                    Some(bits) => {
                        // 批内行尾填充位（不足 8 的尾字节）不计入 1 位统计
                        let take =
                            (((bits.len() * 8) as u64).min(total_bits - written_bits)) as usize;
                        ones += bitmap_count_ones(&bits, 0, take);
                        f.write_all(&bits)?;
                        written_bits += bits.len() as u64 * 8;
                        written += bits.len();
                    }
                    None => break,
                }
            }
            // 规整文件长度（补齐不足 = OS 零填充 NULL；截去批尾越界的填充字节）
            f.set_len(HEADER_SIZE as u64 + data_length + validity_bytes as u64)?;
            null_count = (row_count - ones as u32).max(0);
        }
    }

    // Phase 4: HEADER 回填（seek(0) 只写一次）
    let header = FieldHeader::new_uncompressed(data_type, 1, row_count, null_count, has_validity);
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&header.to_bytes())?;

    Ok(())
}

enum Backing {
    Mmap(Mmap),
    MmapMut(MmapMut),
    Empty,
}

/// compressed Field 打开时全量解压的工作表示（写路径必需；读路径暂以同一形态复用，
/// chunk 级惰性解码为后续性能优化项）。
struct Working {
    values: Buffer,
    validity: Option<Bitmap>,
}

/// Field 的不透明生命周期对象（docs/splayed-core.md §3.8）。
pub struct FieldHandle {
    path: PathBuf,
    header: FieldHeader,
    mode: Mode,
    backing: Backing,
    /// compressed：打开时从 chunk 头读得的分组（自描述，close 重压缩沿用）。
    chunk_rows: Vec<u32>,
    /// compressed：chunk 累积行末（chunk_ends[i] = Σ chunk_rows[..=i]），读侧二分定位用。
    chunk_ends: Vec<u64>,
    /// compressed：解压后的工作表示。
    working: Option<Working>,
    modified: bool,
}

impl FieldHandle {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn header(&self) -> &FieldHeader {
        &self.header
    }

    pub fn mode(&self) -> Mode {
        self.mode
    }

    pub fn data_type(&self) -> DataType {
        self.header.data_type().expect("validated at open")
    }

    pub fn row_count(&self) -> u64 {
        self.header.row_count as u64
    }

    pub fn is_chunked(&self) -> bool {
        self.header.is_chunked()
    }

    #[inline]
    pub fn schema(&self) -> FieldSchema {
        let name = self.path.file_name().unwrap_or_default().to_string_lossy().to_string();
        FieldSchema::new(name, self.data_type())
    }

    #[inline]
    pub fn read_field_schema(&self) -> FieldSchema {
        self.schema()
    }

    fn uncompressed_slices(&self) -> Result<(&[u8], Option<&[u8]>), CoreError> {
        if let Some(work) = &self.working {
            let data = work.values.as_slice();
            let validity = work.validity.as_ref().map(|b| b.as_view().as_raw());
            return Ok((data, validity));
        }
        let bytes: &[u8] = match &self.backing {
            Backing::Mmap(m) => m,
            Backing::MmapMut(m) => m,
            Backing::Empty => return Err(CoreError::InvalidState("backing is unmapped".into())),
        };
        let data_len = self.header.data_length as usize;
        let vlen = self.header.validity_size();
        if bytes.len() < HEADER_SIZE + data_len + vlen {
            return Err(CoreError::InvalidState(format!(
                "field file truncated: {} < {}",
                bytes.len(),
                HEADER_SIZE + data_len + vlen
            )));
        }
        let data = &bytes[HEADER_SIZE..HEADER_SIZE + data_len];
        let validity =
            (vlen > 0).then(|| &bytes[HEADER_SIZE + data_len..HEADER_SIZE + data_len + vlen]);
        Ok((data, validity))
    }

    fn uncompressed_slices_mut(&mut self) -> Result<(&mut [u8], &mut [u8]), CoreError> {
        let data_len = self.header.data_length as usize;
        let vlen = self.header.validity_size();
        let bytes: &mut [u8] = match &mut self.backing {
            Backing::MmapMut(m) => m,
            Backing::Mmap(_) => return Err(CoreError::InvalidState("not writable".into())),
            Backing::Empty => return Err(CoreError::InvalidState("backing is unmapped".into())),
        };
        if bytes.len() < HEADER_SIZE + data_len + vlen {
            return Err(CoreError::InvalidState("field file truncated".into()));
        }
        let (head, rest) = bytes.split_at_mut(HEADER_SIZE + data_len);
        Ok((&mut head[HEADER_SIZE..], &mut rest[..vlen]))
    }

    fn working_ref(&self) -> Result<&Working, CoreError> {
        self.working.as_ref().ok_or_else(|| {
            CoreError::InvalidState("compressed working representation missing".into())
        })
    }

    /// 解压 compressed 文件字节为工作表示（open 时一次性完成）。
    /// validity 按字节批量合并（copy_bits_from 处理 chunk 与全局位图的字节错位），
    /// 不逐 bit；null_count 基线由解压位图精确重建（open_field_file 中回写 header）。
    fn decode_working(bytes: &[u8], header: &FieldHeader) -> Result<Working, CoreError> {
        let dt = header.data_type()?;
        let encoding = header.encoding()?;
        let compression = header.compression()?;
        let size = dt.size_of();
        let mut values = Buffer::zeroed_aligned(header.row_count as usize * size, 8);
        let vsize = validity_size(header.row_count);
        let mut bits = (vsize > 0).then(|| Bitmap::zeros(header.row_count as usize));
        let mut pos = HEADER_SIZE;
        let mut decoded_rows = 0usize;
        while pos < bytes.len() {
            let hdr = splayed_codec::ChunkHeader::from_bytes(&bytes[pos..])?;
            let chunk_end = pos + splayed_codec::CHUNK_HEADER_SIZE + hdr.payload_len as usize;
            let (chunk_values, chunk_validity, rows) =
                decode_chunk(encoding, compression, dt, &bytes[pos..chunk_end])?;
            pos = chunk_end;
            values.as_mut_slice()[decoded_rows * size..(decoded_rows + rows) * size]
                .copy_from_slice(&chunk_values);
            match (&mut bits, chunk_validity) {
                (Some(dst), Some(src)) => {
                    let view = BitmapView::new(BufferView::new(&src), 0, rows)?;
                    dst.copy_bits_from(decoded_rows, &view, rows);
                }
                (Some(dst), None) => {
                    dst.set_range(decoded_rows, rows, true);
                }
                (None, _) => {}
            }
            decoded_rows += rows;
        }
        if decoded_rows != header.row_count as usize {
            return Err(CoreError::InvalidState(format!(
                "chunk rows sum {decoded_rows} != header row_count {}",
                header.row_count
            )));
        }
        Ok(Working { values, validity: bits })
    }

    // --------------------------------------------------------------- read

    /// 按逻辑行（= 物理行）读取，返回 zero-copy ColumnView。
    pub fn read(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError> {
        self.read_field_handle(offset, length)
    }

    #[inline]
    pub fn read_field(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError> {
        self.read(offset, length)
    }

    #[inline]
    pub fn write(&mut self, offset: u64, data: &ColumnView) -> Result<(), CoreError> {
        self.write_field_handle(offset, data)
    }

    #[inline]
    pub fn write_field(&mut self, offset: u64, data: &ColumnView) -> Result<(), CoreError> {
        self.write(offset, data)
    }

    #[inline]
    pub fn update(&mut self, data: &ColumnView<'_>) -> Result<(), CoreError> {
        self.update_field(data)
    }

    #[inline]
    pub fn scan_field(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError> {
        self.scan(request)
    }

    pub fn update_field(&mut self, data: &ColumnView<'_>) -> Result<(), CoreError> {
        self.mode.require_write("update_field")?;
        if data.data_type() != self.data_type() {
            return Err(CoreError::Invalid("update data type mismatch".into()));
        }
        let tmp = tmp_path(&self.path);
        let _ = fs::remove_file(&tmp);
        init_field(&tmp, data, None)?;
        self.backing = Backing::Empty;
        self.working = None;
        fs::rename(&tmp, &self.path)?;
        let reloaded = open_field(&self.path, self.mode)?;
        *self = reloaded;
        Ok(())
    }

    /// 按逻辑行（= 物理行）读取，返回 zero-copy ColumnView。
    pub fn read_field_handle(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError> {
        let row_count = self.row_count();
        // 边界检查溢出安全
        let end = offset
            .checked_add(length)
            .ok_or_else(|| CoreError::Invalid("read range offset + length overflows".into()))?;
        if end > row_count {
            return Err(CoreError::Invalid(format!(
                "read range [{offset}, {end}) exceeds row_count {row_count}"
            )));
        }
        let dt = self.data_type();
        let size = dt.size_of();
        if length == 0 {
            return Ok(ColumnView::empty(dt));
        }
        if self.is_chunked() {
            let work = self.working_ref()?;
            let first = self.chunk_ends.partition_point(|&e| e <= offset);
            // 段容量按平均 chunk 行数预分配，避免逐段扩容
            let avg_chunk = (self.header.row_count as usize / self.chunk_rows.len().max(1)).max(1);
            let mut segments = Vec::with_capacity(length as usize / avg_chunk + 2);
            for ci in first..self.chunk_rows.len() {
                let cend = self.chunk_ends[ci];
                let cstart = cend - self.chunk_rows[ci] as u64;
                if cstart >= end {
                    break;
                }
                let lo = offset.max(cstart) as usize;
                let hi = end.min(cend) as usize;
                let values = BufferView::new(&work.values.as_slice()[lo * size..hi * size]);
                let validity = work
                    .validity
                    .as_ref()
                    .map(|b| b.as_view().slice(lo, hi - lo))
                    .transpose()?;
                segments.push(ColumnSegment::new(dt, values, validity, hi - lo)?);
            }
            return ColumnView::new(dt, segments).map_err(CoreError::from);
        }
        let (data, validity) = self.uncompressed_slices()?;
        let lo = offset as usize;
        let hi = lo + length as usize;
        let values = BufferView::new(&data[lo * size..hi * size]);
        let validity = validity
            .map(|bits| BitmapView::new(BufferView::new(bits), lo, length as usize))
            .transpose()?;
        ColumnView::from_one(dt, values, validity, length as usize).map_err(CoreError::from)
    }

    // -------------------------------------------------------------- write

    /// 按逻辑行覆盖写入（positional overwrite）：values + validity 成对写入，
    /// 不改逻辑长度；成功后递增 generation。
    ///
    /// 写入三原则：values 每段一次连续 memcpy；validity 按字节/word 批量位操作
    /// （不逐 bit）；null_count 只按覆盖区域的位变化增量维护（不重扫整列）。
    /// 整体复杂度 O(data.length)，实际执行接近 memcpy。
    pub fn write_field_handle(&mut self, offset: u64, data: &ColumnView) -> Result<(), CoreError> {
        self.mode.require_write("write_field_handle")?;
        if data.data_type() != self.data_type() {
            return Err(CoreError::Invalid(format!(
                "write data type {:?} does not match field {:?}",
                data.data_type(),
                self.data_type()
            )));
        }
        // 边界检查溢出安全
        let end = offset.checked_add(data.length() as u64).ok_or_else(|| {
            CoreError::Invalid("write range offset + length overflows".into())
        })?;
        if end > self.row_count() {
            return Err(CoreError::Invalid("write range exceeds row_count".into()));
        }
        if data.length() == 0 {
            // 空写入 no-op：不改数据、不递增 generation
            return Ok(());
        }
        if self.is_chunked() {
            self.write_into_working(offset as usize, data)?;
        } else {
            // Header-only 延迟展开：首次写入时将物理文件扩展至完整大小并重新 mmap
            if self.working.is_some() {
                self.expand_header_only_file()?;
            }
            self.write_into_mmap(offset as usize, data)?;
        }
        // generation 每次 write 调用恰好 +1（不按段递增）；header 回写在其后，落盘即含新值
        self.header.generation += 1;
        self.modified = true;
        if let Backing::MmapMut(m) = &mut self.backing {
            let bytes = self.header.to_bytes();
            m[..HEADER_SIZE].copy_from_slice(&bytes);
        }
        Ok(())
    }

    fn write_into_mmap(&mut self, offset: usize, data: &ColumnView) -> Result<(), CoreError> {
        let width = if self.data_type() == DataType::Utf8 {
            4
        } else {
            self.data_type().size_of()
        };
        let has_validity = self.header.has_validity();
        if !has_validity {
            // 先校验后写入：无 validity 区的字段不接受含 NULL 的段
            for seg in data.segments() {
                if let Some(src) = seg.validity() {
                    if src.count_ones() < seg.rows() {
                        return Err(CoreError::Invalid(
                            "field has no validity region; cannot write NULLs".into(),
                        ));
                    }
                }
            }
        }
        let mut null_delta: i64 = 0;
        {
            let (values, validity) = self.uncompressed_slices_mut()?;
            let mut row = offset;
            for seg in data.segments() {
                let rows = seg.rows();
                // values：每段一次连续 memcpy
                values[row * width..(row + rows) * width]
                    .copy_from_slice(seg.raw_values_bytes().expect("field values or keys"));
                match seg.validity() {
                    Some(src) if has_validity => {
                        // validity：按字节批量位复制；返回覆盖前后 1 位数做增量
                        let (old_ones, new_ones) = src.copy_bits_into(validity, row, rows);
                        null_delta += old_ones as i64 - new_ones as i64;
                    }
                    Some(_) => {}
                    None if has_validity => {
                        // 段全有效：目标区间批量置 1
                        let old_ones = bitmap_fill_bits(validity, row, rows, true);
                        null_delta += old_ones as i64 - rows as i64;
                    }
                    None => {}
                }
                row += rows;
            }
        }
        if has_validity {
            // null_count 增量维护：只按覆盖区域的位变化修正，不重扫整列
            self.header.null_count = (self.header.null_count as i64 + null_delta).max(0) as u32;
        }
        Ok(())
    }

    fn write_into_working(&mut self, offset: usize, data: &ColumnView) -> Result<(), CoreError> {
        // 任一段携带 NULL 且当前无位图时，先物化全 1 位图
        let needs_bits = data
            .segments()
            .iter()
            .any(|s| s.validity().map(|b| b.count_ones() < b.len()).unwrap_or(false));
        if needs_bits
            && self
                .working
                .as_ref()
                .expect("compressed open materializes working")
                .validity
                .is_none()
        {
            self.working.as_mut().unwrap().validity =
                Some(Bitmap::ones(self.header.row_count as usize));
        }
        let width = if self.data_type() == DataType::Utf8 {
            4
        } else {
            self.data_type().size_of()
        };
        let work = self.working.as_mut().expect("compressed open materializes working");
        let mut null_delta: i64 = 0;
        let mut row = offset;
        for seg in data.segments() {
            let rows = seg.rows();
            // values：每段一次连续 memcpy
            work.values.as_mut_slice()[row * width..(row + rows) * width]
                .copy_from_slice(seg.raw_values_bytes().expect("field values or keys"));
            match seg.validity() {
                Some(src) => {
                    if let Some(full) = work.validity.as_mut() {
                        // validity：按字节批量位复制；返回覆盖前后 1 位数做增量
                        let (old_ones, new_ones) = full.copy_bits_from(row, &src, rows);
                        null_delta += old_ones as i64 - new_ones as i64;
                    }
                }
                None => {
                    if let Some(full) = work.validity.as_mut() {
                        // 段全有效：目标区间批量置 1
                        let old_ones = full.set_range(row, rows, true);
                        null_delta += old_ones as i64 - rows as i64;
                    }
                }
            }
            row += rows;
        }
        // null_count 增量维护（基线在 open 时由解压位图精确重建）
        self.header.null_count = (self.header.null_count as i64 + null_delta).max(0) as u32;
        Ok(())
    }

    /// 对 uncompressed header-only 文件做延迟展开：扩展物理文件并重新 mmapMut。
    fn expand_header_only_file(&mut self) -> Result<(), CoreError> {
        let total = HEADER_SIZE as u64
            + self.header.data_length
            + self.header.validity_size() as u64;
        // Windows 下必须先释放原有 mmap 句柄才能扩展文件
        self.backing = Backing::Empty;
        let file = File::options()
            .read(true)
            .write(true)
            .open(&self.path)
            .map_err(|e| map_io(&self.path, e))?;
        file.set_len(total).map_err(|e| map_io(&self.path, e))?;
        let mmap_mut = unsafe { MmapMut::map_mut(&file).map_err(|e| map_io(&self.path, e))? };
        self.backing = Backing::MmapMut(mmap_mut);
        self.working = None;
        Ok(())
    }

    /// 修改 header（不改 data）：data_type / row_count 必须与现值一致；
    /// 成功后递增 generation。uncompressed 立即落盘（mmap），compressed 随 close 收尾。
    pub fn update_header(&mut self, mut header: FieldHeader) -> Result<(), CoreError> {
        self.mode.require_write("update_header")?;
        header.magic = self.header.magic;
        header.version = self.header.version;
        header.data_type = self.header.data_type;
        header.row_count = self.header.row_count;
        // null_count 是派生统计，由写路径增量维护，不接受调用方改写
        header.null_count = self.header.null_count;
        header.generation = self.header.generation + 1;
        header.validate()?;
        // 结构派生字段按现值重算，防止调用方传入不一致的布局描述
        if !header.is_chunked() {
            header.data_length =
                header.row_count as u64 * header.data_type()?.size_of() as u64;
            header.validity_offset =
                u64::from(header.has_validity()) * (64 + header.data_length);
        }
        self.header = header;
        self.modified = true;
        if self.working.is_some() && !self.is_chunked() {
            self.expand_header_only_file()?;
        }
        if let Backing::MmapMut(m) = &mut self.backing {
            let bytes = self.header.to_bytes();
            m[..HEADER_SIZE].copy_from_slice(&bytes);
        }
        Ok(())
    }

    #[inline]
    pub fn update_field_handle(&mut self, header: FieldHeader) -> Result<(), CoreError> {
        self.update_header(header)
    }

    // --------------------------------------------------------------- scan

    /// 条件扫描：只返回 ranges，不物化数据（docs/core/field.md §5.7）。
    ///
    /// ranges 直接顺序消费：仅与 `[0, row_count)` 求交防越界（保序，不排序不合并——
    /// 有序不重叠由上游保证）；空 ranges = 整个 Field。
    pub fn scan(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError> {
        let ranges = if request.ranges.is_empty() {
            vec![RowRange::new(0, self.row_count())]
        } else {
            request
                .ranges
                .iter()
                .filter_map(|r| r.intersect(&RowRange::new(0, self.row_count())))
                .collect()
        };
        Ok(FieldScanner {
            handle: self,
            ranges,
            predicate: request.predicate.clone(),
            remaining: request.limit,
            range_index: 0,
            row: 0,
            mask: Vec::new(),
            mask_rows: 0,
            mask_pos: 0,
            done: false,
        })
    }

    #[inline]
    pub fn scan_field_handle(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError> {
        self.scan(request)
    }

    /// 供 scanner 的整段连续视图（PLAIN: mmap 切片；compressed: working 切片，零拷贝）。
    fn range_views(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<(&[u8], Option<BitmapView<'_>>), CoreError> {
        let size = self.data_type().size_of();
        if self.is_chunked() {
            let work = self.working_ref()?;
            let values =
                &work.values.as_slice()[offset as usize * size..(offset + length) as usize * size];
            let validity = work
                .validity
                .as_ref()
                .map(|b| {
                    BitmapView::new(
                        BufferView::new(b.as_view().as_raw()),
                        offset as usize,
                        length as usize,
                    )
                })
                .transpose()?;
            Ok((values, validity))
        } else {
            let (data, validity) = self.uncompressed_slices()?;
            let values =
                &data[offset as usize * size..(offset + length) as usize * size];
            let validity = validity
                .map(|bits| {
                    BitmapView::new(BufferView::new(bits), offset as usize, length as usize)
                })
                .transpose()?;
            Ok((values, validity))
        }
    }

    /// 关闭 FieldHandle 并安全刷盘。
    #[inline]
    pub fn close_field(self) -> Result<(), CoreError> {
        close_field_handle(self)
    }

    /// 销毁并物理删除字段文件。
    #[inline]
    pub fn drop_field(self) -> Result<(), CoreError> {
        drop_field_handle(self)
    }
}

/// 关闭 Handle：read 无写回；uncompressed write 已直接生效（mmap flush 即落盘）；
/// compressed write 发生修改 → 流式重压缩收尾（临时文件 + 原子替换，
/// 分组沿用打开时读得的 chunk 头；写路径不改 row_count，分组可精确复用）。
pub fn close_field_handle(handle: FieldHandle) -> Result<(), CoreError> {
    let FieldHandle { path, header, mode, backing, chunk_rows, chunk_ends, working, modified } =
        handle;
    match mode {
        Mode::Read => return Ok(()),
        Mode::Write => {}
    }
    if !header.is_chunked() {
        if let Backing::MmapMut(m) = &backing {
            m.flush()?;
        }
        drop(backing);
        return Ok(());
    }
    if !modified {
        return Ok(());
    }
    let work = working.expect("compressed write handle materializes working");
    let dt = header.data_type()?;
    let size = dt.size_of();
    let encoding = header.encoding()?;
    let compression = header.compression()?;
    // 流式写出：header → 逐 chunk 编码直写 tmp，不拼接整个重压缩文件，
    // 内存复杂度 O(working + 一个 chunk)
    let tmp = tmp_path(&path);
    let mut f = File::options().write(true).create(true).truncate(true).open(&tmp)?;
    let write_result = (|| -> Result<(), CoreError> {
        // header 在编码前即完全确定（写路径不改 row_count；generation / null_count 已在
        // 内存更新；chunked 布局的 data_length / validity_offset 不依赖编码输出），
        // 直接写真实 header，无需占位回填
        f.write_all(&header.to_bytes())?;
        for (ci, &rows) in chunk_rows.iter().enumerate() {
            let hi = chunk_ends[ci] as usize;
            let lo = hi - rows as usize;
            let values = &work.values.as_slice()[lo * size..hi * size];
            let validity = work
                .validity
                .as_ref()
                .map(|b| b.extract_bits(lo, rows as usize))
                .transpose()?;
            let encoded =
                encode_chunk(encoding, compression, dt, values, validity.as_deref(), rows as usize)?;
            f.write_all(&encoded)?;
        }
        // tmp 完整落盘后再原子替换：rename 生效时新文件内容已持久
        f.sync_all()?;
        Ok(())
    })();
    if let Err(e) = write_result {
        // 编码 / 写出失败：清理 tmp，原文件保持不变
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    drop(f);
    fs::rename(&tmp, &path)?;
    Ok(())
}

// ------------------------------------------------------------------ File API

/// 删除 Field 物理文件。
pub fn delete_field_file(path: &Path) -> Result<(), CoreError> {
    fs::remove_file(path).map_err(|e| map_io(path, e))
}

/// 同目录内重命名 Field 文件（文件名即字段名）；目标已存在时 Error，不覆盖。
pub fn rename_field_file(path: &Path, new_name: &str) -> Result<(), CoreError> {
    if new_name.is_empty() || new_name.starts_with('.') || new_name.contains(['/', '\\']) {
        return Err(CoreError::Invalid(format!("invalid field name '{new_name}'")));
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let target = parent.join(new_name);
    if target.exists() {
        return Err(CoreError::AlreadyExists(target));
    }
    fs::rename(path, &target)?;
    Ok(())
}

/// cast 单批行数（限定批次内存：256K 行 × 8B ≈ 2MB values + 位级 validity）。
const CAST_BATCH_ROWS: u64 = 1 << 18;

/// cast 的流式 reader：批次读取源 Field → 逐批类型转换。
///
/// 拥有源 Handle（`create_field_file` 结束时随 reader 一起释放）。
/// 两相输出：Phase 1 产出转换后的 values 批次，同时把 validity 位跨批拼接为
/// 全局字节对齐位流（批尾填充位不计，O(total/8) 内存，远小于 values）；
/// Phase 2 只做位流分块产出（64KB/批），不重读源。
struct CastReader {
    handle: FieldHandle,
    src: DataType,
    dst: DataType,
    total: u64,
    offset: u64,
    phase: u8,
    validity_stream: Vec<u8>,
    bit_acc: u64,
    acc_bits: u32,
    validity_written: usize,
}

impl FieldChunkReader for CastReader {
    fn next_values(&mut self) -> Result<Option<StreamValues>, CoreError> {
        if self.offset >= self.total {
            return Ok(None);
        }
        let n = CAST_BATCH_ROWS.min(self.total - self.offset) as usize;
        let view = self.handle.read(self.offset, n as u64)?;
        // 逐段处理：values 按段转换拼接；validity 按段位级拼接进全局位流。
        // （clone_view 的 validity 是逐段字节对齐拼接，段边界非字节对齐时不能跨段消费）
        let mut converted = Vec::with_capacity(n * self.dst.size_of());
        let mut rows = 0usize;
        for seg in view.segments() {
            let seg_rows = seg.rows();
            let values = seg.fixed_bytes().expect("field files are fixed-width");
            let (out, _, _) = convert_values(self.src, self.dst, values, None, seg_rows)?;
            converted.extend_from_slice(&out);
            let bits = match seg.validity() {
                Some(bm) => bm.to_packed_bytes(),
                None => vec![0xFFu8; (seg_rows + 7) / 8],
            };
            for i in 0..seg_rows {
                let bit = (bits[i / 8] >> (i % 8)) & 1;
                self.bit_acc |= (bit as u64) << self.acc_bits;
                self.acc_bits += 1;
                if self.acc_bits == 8 {
                    self.validity_stream.push(self.bit_acc as u8);
                    self.bit_acc = 0;
                    self.acc_bits = 0;
                }
            }
            rows += seg_rows;
        }
        // validity 不参与类型转换（原样保留在位流中）
        self.offset += rows as u64;
        Ok(Some(StreamValues { values: converted, rows }))
    }

    fn next_validity(&mut self) -> Result<Option<Vec<u8>>, CoreError> {
        if self.phase == 0 {
            self.phase = 1;
        }
        if self.validity_written < self.validity_stream.len() {
            let end = (self.validity_written + 64 * 1024).min(self.validity_stream.len());
            let chunk = self.validity_stream[self.validity_written..end].to_vec();
            self.validity_written = end;
            return Ok(Some(chunk));
        }
        if self.acc_bits > 0 {
            // 收尾：不足 8 位的部分字节（高位零填充，与 validity_size 的零填充一致）
            let last = self.bit_acc as u8;
            self.bit_acc = 0;
            self.acc_bits = 0;
            return Ok(Some(vec![last]));
        }
        Ok(None)
    }
}

/// 将 `path` 处 Field 原地转换为 `target_type`。
///
/// read → 逐批 cast → create tmp → sync_all → rename；失败清理 tmp，原文件保持不变。
/// 逐批流式转换（`CAST_BATCH_ROWS` 行/批；uncompressed 源全程 O(一个批次) 内存）；
/// validity / NULL 不参与类型转换、原样保留；保持原 Field 的压缩状态——
/// uncompressed → uncompressed，compressed → 按原 encoding / compression / chunk
/// 分组重新编码。header 仅更新 `data_type`（generation 保持源值）。
/// 逐行 `as` 语义转换（bool ↔ 数值 ↔ 浮点；Utf8 不支持）。
pub fn cast_field_file(path: &Path, target_type: DataType) -> Result<(), CoreError> {
    let tmp = unique_tmp_path(path, "cast");
    let result = (|| -> Result<(), CoreError> {
        let (source_gen, was_chunked, encoding, compression, chunk_starts, is_header_only) = {
            let handle = open_field_file(path, Mode::Read)?;
            let dt = handle.data_type();
            if dt == target_type {
                return Ok(());
            }
            if dt == DataType::Utf8 || target_type == DataType::Utf8 {
                return Err(CoreError::Invalid("cast involving Utf8 is not supported".into()));
            }
            let file_len = fs::metadata(path).map_err(|e| map_io(path, e))?.len() as usize;
            let is_header_only = file_len == HEADER_SIZE;
            let chunk_starts: Vec<u64> = handle
                .chunk_rows
                .iter()
                .scan(0u64, |acc, &r| {
                    let start = *acc;
                    *acc += r as u64;
                    Some(start)
                })
                .collect();
            let info = (
                handle.header.generation,
                handle.is_chunked(),
                handle.header.encoding()?,
                handle.header.compression()?,
                chunk_starts,
                is_header_only,
            );
            if !is_header_only {
                let total = handle.row_count();
                let reader = CastReader {
                    handle,
                    src: dt,
                    dst: target_type,
                    total,
                    offset: 0,
                    phase: 0,
                    validity_stream: Vec::new(),
                    bit_acc: 0,
                    acc_bits: 0,
                    validity_written: 0,
                };
                // 批次流式写出目标 Field（values → validity 三阶段顺序写；
                // reader 拥有源 Handle，结束即释放）
                create_field_file(
                    &tmp,
                    target_type,
                    FieldInit::Stream { reader: Box::new(reader) },
                    CreateFieldOptions::default(),
                )?;
            } else {
                // Header-only：O(1) 直接创建 64 字节全 NULL 字段
                create_field_file(
                    &tmp,
                    target_type,
                    FieldInit::Length(handle.row_count()),
                    CreateFieldOptions::default(),
                )?;
            }
            info
        };

        // generation 保持源值（与 compress / decompress 一致；create 写 1，此处回填）
        {
            let mut f = File::options().read(true).write(true).open(&tmp)?;
            let mut hb = [0u8; HEADER_SIZE];
            f.read_exact(&mut hb)?;
            let mut h = FieldHeader::from_bytes(&hb)?;
            h.generation = source_gen;
            f.seek(SeekFrom::Start(0))?;
            f.write_all(&h.to_bytes())?;
        }

        // 保持压缩状态：compressed 源按原 encoding / compression / chunk 分组重编码
        if was_chunked {
            if is_header_only {
                // Header-only：直接写回压缩标志，不落 chunk
                let mut f = File::options().read(true).write(true).open(&tmp)?;
                let mut hb = [0u8; HEADER_SIZE];
                f.read_exact(&mut hb)?;
                let mut h = FieldHeader::from_bytes(&hb)?;
                h.compression = compression.id();
                h.encoding = encoding.id();
                h.set_has_validity(true);
                f.seek(SeekFrom::Start(0))?;
                f.write_all(&h.to_bytes())?;
            } else {
                compress_field_file_encoded(&tmp, Some(chunk_starts), encoding, compression)?;
            }
        }

        // tmp 完整落盘后再原子替换：rename 生效时新文件内容已持久
        {
            let f = File::options().write(true).open(&tmp)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        // 失败清理：cast tmp 与 compress 步自身的 tmp，原文件保持不变
        let _ = fs::remove_file(&tmp);
        let _ = fs::remove_file(tmp_path(&tmp));
    }
    result
}

fn scalar_to_f64(v: &Scalar) -> f64 {
    match v {
        Scalar::Bool(b) => u8::from(*b) as f64,
        Scalar::Int(i) => *i as f64,
        Scalar::UInt(u) => *u as f64,
        Scalar::Float(f) => *f,
        Scalar::Str(_) => 0.0,
    }
}

fn scalar_to_i64(v: &Scalar) -> i64 {
    match v {
        Scalar::Bool(b) => u8::from(*b) as i64,
        Scalar::Int(i) => *i,
        Scalar::UInt(u) => *u as i64,
        Scalar::Float(f) => *f as i64,
        Scalar::Str(_) => 0,
    }
}

fn scalar_to_u64(v: &Scalar) -> u64 {
    match v {
        Scalar::Bool(b) => u8::from(*b) as u64,
        Scalar::Int(i) => *i as u64,
        Scalar::UInt(u) => *u,
        Scalar::Float(f) => *f as u64,
        Scalar::Str(_) => 0,
    }
}

fn convert_values(
    src: DataType,
    dst: DataType,
    values: &[u8],
    validity: Option<Vec<u8>>,
    rows: usize,
) -> Result<(Vec<u8>, Option<Vec<u8>>, usize), CoreError> {
    let ss = src.size_of();
    let ds = dst.size_of();
    let mut out = vec![0u8; rows * ds];
    for i in 0..rows {
        let scalar = read_row_scalar(src, &values[i * ss..i * ss + ss])?;
        // 目标宽度直接生成对应宽度的 LE 字节（禁止用宽类型字节截断：
        // f64 小端低 4 位不是 f32 位型，截断对小整数值产生 0.0 / 大值产生乱码）
        let slot = &mut out[i * ds..i * ds + ds];
        match dst {
            DataType::Float32 => {
                let b = (scalar_to_f64(&scalar) as f32).to_le_bytes();
                slot.copy_from_slice(&b);
            }
            DataType::Float64 => {
                let b = scalar_to_f64(&scalar).to_le_bytes();
                slot.copy_from_slice(&b);
            }
            _ if dst.is_signed_int() => {
                let b = scalar_to_i64(&scalar).to_le_bytes();
                slot.copy_from_slice(&b[..ds]);
            }
            _ => {
                let b = scalar_to_u64(&scalar).to_le_bytes();
                slot.copy_from_slice(&b[..ds]);
            }
        }
    }
    Ok((out, validity, rows))
}

/// 压缩已有 Field（uncompressed → compressed，encoding 置 PLAIN、compression 置 ZSTD，
/// 临时文件 + 原子替换）。`offsets`：可选的 chunk 起始行号（升序、`offsets[0] == 0`，
/// 末块隐含到 row_count）；省略时按固定 8192 行均匀分块。分块策略是调用方的职责
/// （Dataset 层按 META 网格生成 sym 对齐边界）。
pub fn compress_field_file(path: &Path, offsets: Option<Vec<u64>>) -> Result<(), CoreError> {
    compress_field_file_encoded(path, offsets, Encoding::Plain, Compression::Zstd)
}

/// [`compress_field_file`] 的参数化版本：encoding / compression 由调用方指定
/// （cast 用它保持源 Field 的压缩配置）。
///
/// chunk 流式：header（编码前即完全确定，无需占位回填）直写 tmp 后，
/// 逐 chunk 零拷贝切片 → encode_chunk → 直写 tmp；内存 O(一个编码 chunk)，
/// 不全量 clone、不构造完整目标文件。
fn compress_field_file_encoded(
    path: &Path,
    offsets: Option<Vec<u64>>,
    encoding: Encoding,
    compression: Compression,
) -> Result<(), CoreError> {
    let handle = open_field_file(path, Mode::Read)?;
    if handle.is_chunked() {
        return Err(CoreError::InvalidState("field is already compressed".into()));
    }
    let file_len = fs::metadata(path).map_err(|e| map_io(path, e))?.len() as usize;
    let is_header_only = file_len == HEADER_SIZE || handle.header.is_all_null();
    if is_header_only {
        // Header-only：直接写回压缩标志，不写任何 chunk
        let mut new_header = handle.header;
        new_header.compression = compression.id();
        new_header.encoding = encoding.id();
        new_header.set_has_validity(true);
        drop(handle);
        let tmp = tmp_path(path);
        let mut out = File::options().write(true).create(true).truncate(true).open(&tmp)?;
        out.write_all(&new_header.to_bytes())?;
        out.sync_all()?;
        drop(out);
        fs::rename(&tmp, path)?;
        return Ok(());
    }
    let rows = handle.header.row_count as usize;
    let boundaries: Vec<u64> = match offsets {
        Some(mut list) => {
            if list.first() != Some(&0) {
                return Err(CoreError::Invalid("offsets must start at 0".into()));
            }
            list.sort_unstable();
            list.dedup();
            if list.windows(2).any(|w| w[0] >= w[1])
                || list.iter().any(|&o| o >= rows as u64)
                || list.is_empty()
            {
                return Err(CoreError::Invalid(
                    "offsets must be strictly ascending within [0, row_count)".into(),
                ));
            }
            list
        }
        None => (0..rows).step_by(8192).map(|o| o as u64).collect(),
    };
    let dt = handle.data_type();
    let mut new_header = handle.header;
    new_header.compression = compression.id();
    new_header.encoding = encoding.id();
    new_header.set_has_validity(false);
    new_header.validity_offset = 0;
    let tmp = tmp_path(path);
    let mut out = File::options().write(true).create(true).truncate(true).open(&tmp)?;
    let result = (|| -> Result<(), CoreError> {
        out.write_all(&new_header.to_bytes())?;
        let mut ranges: Vec<(u64, u64)> =
            boundaries.windows(2).map(|w| (w[0], w[1])).collect();
        let last = *boundaries.last().unwrap();
        if (last as usize) < rows {
            ranges.push((last, rows as u64));
        }
        for (lo, hi) in ranges {
            let (values, validity) = handle.range_views(lo, hi - lo)?;
            let bits = validity.map(|b| b.to_packed_bytes());
            let encoded = encode_chunk(
                encoding,
                compression,
                dt,
                values,
                bits.as_deref(),
                (hi - lo) as usize,
            )?;
            out.write_all(&encoded)?;
        }
        out.sync_all()?;
        Ok(())
    })();
    if let Err(e) = result {
        // 失败清理 tmp，原文件保持不变
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    drop(out);
    drop(handle);
    fs::rename(&tmp, path)?;
    Ok(())
}

/// 把 chunk 的打包 validity 位按全局位流拼接（acc 跨 chunk 携带）产出到 `emitted`；
/// `bits = None` 表示该 chunk 全部有效（补 1 位）。
fn stitch_chunk_bits(
    bits: Option<&[u8]>,
    rows: usize,
    bit_acc: &mut u64,
    acc_bits: &mut u32,
    emitted: &mut Vec<u8>,
) {
    for i in 0..rows {
        let bit = match bits {
            Some(b) => (b[i / 8] >> (i % 8)) & 1,
            None => 1,
        };
        *bit_acc |= (bit as u64) << *acc_bits;
        *acc_bits += 1;
        if *acc_bits == 8 {
            emitted.push(*bit_acc as u8);
            *bit_acc = 0;
            *acc_bits = 0;
        }
    }
}

/// 解压已有 Field（compressed → uncompressed，PLAIN + NONE，临时文件 + 原子替换）。
///
/// chunk 流式：mmap 原文件直接解析 chunk 位置（不经 `open_field_file`，避免全量解压），
/// 逐 chunk 解码——values 顺序直写 DATA、validity 位跨 chunk 拼接后顺序直写 VALIDITY
/// （单句柄双游标，两个区域各自顺序 IO）；内存 O(一个解码 chunk)。
pub fn decompress_field_file(path: &Path) -> Result<(), CoreError> {
    let file = File::open(path).map_err(|e| map_io(path, e))?;
    let file_len = file.metadata()?.len() as usize;
    let map = unsafe { Mmap::map(&file)? };
    let header = FieldHeader::from_bytes(&map[..HEADER_SIZE])?;
    if !header.is_chunked() {
        return Err(CoreError::InvalidState("field is not compressed".into()));
    }
    let is_header_only = file_len == HEADER_SIZE;
    if is_header_only {
        // Header-only：直接写回解压标志（PLAIN + NONE），不写任何 DATA / VALIDITY
        let mut new_header = header;
        new_header.compression = Compression::None.id();
        new_header.encoding = Encoding::Plain.id();
        new_header.set_has_validity(true);
        drop(map);
        drop(file);
        let tmp = tmp_path(path);
        let mut out = File::options().write(true).create(true).truncate(true).open(&tmp)?;
        out.write_all(&new_header.to_bytes())?;
        out.sync_all()?;
        drop(out);
        fs::rename(&tmp, path)?;
        return Ok(());
    }
    let dt = header.data_type()?;
    let encoding = header.encoding()?;
    let compression = header.compression()?;
    let size = dt.size_of();
    let rows = header.row_count as usize;
    let data_len = rows * size;
    let validity_len = validity_size(header.row_count);
    // chunk 字节区间 (pos, len, rows)
    let mut chunks: Vec<(usize, usize, usize)> = Vec::new();
    {
        let mut pos = HEADER_SIZE;
        while pos < map.len() {
            let h = splayed_codec::ChunkHeader::from_bytes(&map[pos..])?;
            let len = splayed_codec::CHUNK_HEADER_SIZE + h.payload_len as usize;
            chunks.push((pos, len, h.rows as usize));
            pos += len;
        }
        if pos != map.len() {
            return Err(CoreError::InvalidState(
                "chunk stream does not exactly cover file".into(),
            ));
        }
    }
    let tmp = tmp_path(path);
    let mut out = File::options().write(true).create(true).truncate(true).open(&tmp)?;
    // 预分配 DATA + VALIDITY 区（set_len 零填充即占位 HEADER）；
    // 全部 chunk 均无 validity 时收缩掉 VALIDITY 区
    out.set_len(HEADER_SIZE as u64 + data_len as u64 + validity_len as u64)?;
    let result = (|| -> Result<(), CoreError> {
        let mut data_pos = HEADER_SIZE as u64;
        let mut validity_pos = HEADER_SIZE as u64 + data_len as u64;
        let (mut bit_acc, mut acc_bits) = (0u64, 0u32);
        let mut saw_validity = false;
        let mut data_written = 0usize;
        for &(pos, len, crows) in &chunks {
            let (values, validity, decoded_rows) =
                splayed_codec::decode_chunk(encoding, compression, dt, &map[pos..pos + len])?;
            if decoded_rows != crows {
                return Err(CoreError::InvalidState(
                    "chunk header rows mismatch with decoded rows".into(),
                ));
            }
            // DATA 顺序写
            out.seek(SeekFrom::Start(data_pos))?;
            out.write_all(&values)?;
            data_pos += values.len() as u64;
            data_written += values.len();
            // VALIDITY 顺序写（位级拼接，跨 chunk 字节对齐）
            let mut emitted = Vec::new();
            if validity.is_some() {
                saw_validity = true;
            }
            stitch_chunk_bits(validity.as_deref(), decoded_rows, &mut bit_acc, &mut acc_bits, &mut emitted);
            if !emitted.is_empty() {
                out.seek(SeekFrom::Start(validity_pos))?;
                out.write_all(&emitted)?;
                validity_pos += emitted.len() as u64;
            }
        }
        if data_written != data_len {
            return Err(CoreError::InvalidState(format!(
                "decoded data length {data_written} != expected {data_len}"
            )));
        }
        if saw_validity && acc_bits > 0 {
            // 收尾：最后一个不足 8 位的部分字节（高位零填充）
            out.seek(SeekFrom::Start(validity_pos))?;
            out.write_all(&[bit_acc as u8])?;
        }
        // 回填 header：has_validity = 是否有 chunk 携带 validity；
        // data_type / generation / row_count / null_count 保持源值
        let new_header = FieldHeader::new_uncompressed(
            dt,
            header.generation,
            header.row_count,
            header.null_count,
            saw_validity,
        );
        out.seek(SeekFrom::Start(0))?;
        out.write_all(&new_header.to_bytes())?;
        if !saw_validity {
            out.set_len(HEADER_SIZE as u64 + data_len as u64)?;
        }
        out.sync_all()?;
        Ok(())
    })();
    if let Err(e) = result {
        // 失败清理 tmp，原文件保持不变
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    drop(out);
    drop(map);
    drop(file);
    fs::rename(&tmp, path)?;
    Ok(())
}

/// 打开已有 Field（open 不负责创建）。compressed Field 打开时一次性解压为工作表示。
pub fn open_field_file(path: &Path, mode: Mode) -> Result<FieldHandle, CoreError> {
    let mut file = File::open(path).map_err(|e| map_io(path, e))?;
    let mut header_bytes = [0u8; HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let mut header = FieldHeader::from_bytes(&header_bytes)?;
    let file_len = file.metadata()?.len() as usize;
    drop(file);

    let is_header_only = file_len == HEADER_SIZE && header.row_count > 0;

    let mut chunk_rows = Vec::new();
    let working = if is_header_only {
        // Header-Only 全 NULL 字段（包括 uncompressed 与 compressed）：
        // 构造全零 values 与全零 validity 的 Working 内存表示
        let dt = header.data_type()?;
        let rows = header.row_count as usize;
        let size = dt.size_of();
        let values = Buffer::zeroed_aligned(rows * size, 8);
        let validity = Some(Bitmap::zeros(rows));
        if header.is_chunked() {
            // chunked：初始化默认分组
            chunk_rows = (0..rows)
                .step_by(CREATE_CHUNK_ROWS)
                .map(|o| (rows - o).min(CREATE_CHUNK_ROWS) as u32)
                .collect();
        }
        Some(Working { values, validity })
    } else {
        // 以只读 mmap 完成 chunk 分组读取与工作表示解压
        let map = unsafe { Mmap::map(&File::open(path)?)? };
        if header.is_chunked() {
            let mut pos = HEADER_SIZE;
            while pos < file_len {
                let hdr = splayed_codec::ChunkHeader::from_bytes(&map[pos..])?;
                pos += splayed_codec::CHUNK_HEADER_SIZE + hdr.payload_len as usize;
                chunk_rows.push(hdr.rows);
            }
            if pos != file_len {
                return Err(CoreError::InvalidState(
                    "chunk stream does not exactly cover file".into(),
                ));
            }
        }
        let w = if header.is_chunked() {
            Some(FieldHandle::decode_working(&map, &header)?)
        } else {
            None
        };
        drop(map);
        w
    };

    let chunk_ends: Vec<u64> = chunk_rows
        .iter()
        .scan(0u64, |acc, &r| {
            *acc += r as u64;
            Some(*acc)
        })
        .collect();

    // compressed 基线：null_count 从解压位图精确重建（word 批量 popcount，O(rows/64)），
    // 后续写路径只做增量维护
    if let Some(w) = &working {
        if header.is_chunked() && !is_header_only {
            header.null_count = w.validity.as_ref().map(|b| b.null_count() as u32).unwrap_or(0);
        }
    }

    if mode == Mode::Read {
        let file = File::open(path)?;
        Ok(FieldHandle {
            path: path.to_path_buf(),
            header,
            mode,
            backing: Backing::Mmap(unsafe { Mmap::map(&file)? }),
            chunk_rows,
            chunk_ends,
            working,
            modified: false,
        })
    } else {
        let file = File::options().read(true).write(true).open(path)?;
        Ok(FieldHandle {
            path: path.to_path_buf(),
            header,
            mode,
            backing: Backing::MmapMut(unsafe { MmapMut::map_mut(&file)? }),
            chunk_rows,
            chunk_ends,
            working,
            modified: false,
        })
    }
}

// ------------------------------------------------------------------ Scanner

/// 单段求值行数上限（仅用于限定掩码内存：1M 行 → 1MB 字节掩码 + 128KB 命中位图）。
const EVAL_CAP: u64 = 1 << 20;

/// Field Scanner：单 Field 条件扫描，只输出物理 RowRange（docs/core/field.md §5.7）。
///
/// 管线：候选 ranges 顺序消费 → 整段连续 values 批量求谓词（类型化循环，可自动向量化）
/// → 根部统一与 validity 求交 → 打包命中位图 → word 级 `next_true_run` → 连续命中 RowRange。
/// 不复制 values、不物化数据、不逐行生成 RowRange、不做 ranges merge、不处理 sym / time。
pub struct FieldScanner<'h> {
    handle: &'h FieldHandle,
    ranges: Vec<RowRange>,
    predicate: Option<Predicate>,
    remaining: Option<u64>,
    range_index: usize,
    /// 当前候选段起点（物理行）
    row: u64,
    /// 当前段命中位图缓存（跨 next() 重用）+ 行数 + 消费游标
    mask: Vec<u64>,
    mask_rows: usize,
    mask_pos: usize,
    done: bool,
}

impl<'h> FieldScanner<'h> {
    /// 每次返回一个连续命中 RowRange；结束返回 `None`。
    pub fn next(&mut self) -> Result<Option<RowRange>, CoreError> {
        loop {
            if self.done {
                return Ok(None);
            }
            // 1) 消费当前段命中位图：word 级找下一连续命中区
            if self.mask_pos < self.mask_rows {
                if let Some((s, e)) = next_true_run(&self.mask, self.mask_rows, self.mask_pos) {
                    self.mask_pos = e;
                    return Ok(Some(self.take(RowRange::new(
                        self.row + s as u64,
                        (e - s) as u64,
                    ))));
                }
                self.mask_pos = self.mask_rows;
            }
            // 2) 段耗尽：推进游标 / 切换 range
            self.row += self.mask_rows as u64;
            self.mask_rows = 0;
            self.mask_pos = 0;
            loop {
                if self.range_index >= self.ranges.len() {
                    self.done = true;
                    return Ok(None);
                }
                let range = self.ranges[self.range_index];
                if self.row < range.offset {
                    self.row = range.offset;
                }
                if self.row < range.end() {
                    break;
                }
                self.range_index += 1;
                self.row = 0;
            }
            // 3) 当前 range 的下一段：整段连续 values 批量求值
            let range = self.ranges[self.range_index];
            let end = range.end().min(self.row + EVAL_CAP);
            let rows = (end - self.row) as usize;
            self.evaluate_range(rows)?;
        }
    }

    /// 应用 limit；达到后标记结束并截断本段输出。
    fn take(&mut self, range: RowRange) -> RowRange {
        match &mut self.remaining {
            Some(rem) if *rem <= range.length => {
                let out = RowRange::new(range.offset, *rem);
                *rem = 0;
                self.done = true;
                out
            }
            Some(rem) => {
                *rem -= range.length;
                range
            }
            None => range,
        }
    }

    /// 对 `[self.row, self.row + rows)` 整段连续 values 批量求谓词 → 命中位图缓存。
    fn evaluate_range(&mut self, rows: usize) -> Result<(), CoreError> {
        self.mask.clear();
        self.mask.resize(rows.div_ceil(64), 0);
        self.mask_rows = rows;
        self.mask_pos = 0;
        let (values, validity) = self.handle.range_views(self.row, rows as u64)?;
        // 值匹配掩码（不含 validity）：无谓词 = 全部行参与
        let mut hit = match &self.predicate {
            Some(pred) => eval_predicate_bytes(pred, self.handle.data_type(), values, rows)?,
            None => vec![1u8; rows],
        };
        // 根部统一排除 NULL 行：NULL 不命中任何条件（含 NOT / OR）
        if let Some(v) = &validity {
            for (i, slot) in hit.iter_mut().enumerate() {
                *slot &= v.is_valid(i) as u8;
            }
        }
        // 字节掩码 → 打包位图（branchless：0/1 字节按位左移或）
        for (wi, word) in self.mask.iter_mut().enumerate() {
            let lo = wi * 64;
            let hi = rows.min(lo + 64);
            let mut w = 0u64;
            for (j, &bit) in hit[lo..hi].iter().enumerate() {
                w |= (bit as u64) << j;
            }
            *word = w;
        }
        Ok(())
    }

    /// 关闭（资源随 handle 生命周期管理，此处仅为契约完备）。
    pub fn close(self) -> Result<(), CoreError> {
        Ok(())
    }
}

/// 在打包位图 `mask`（有效位 `len`，字内填充位必须为 0）中从 `from` 位起找下一连续置位段。
/// word 级跳过零字 / 扩展满字；返回位区间 `[start, end)`（相对 mask）。
fn next_true_run(mask: &[u64], len: usize, from: usize) -> Option<(usize, usize)> {
    if from >= len {
        return None;
    }
    // 定位首个置位
    let mut pos = from;
    let start = loop {
        let w = mask[pos / 64] & (u64::MAX << (pos % 64));
        if w != 0 {
            break (pos / 64) * 64 + w.trailing_zeros() as usize;
        }
        pos = (pos / 64 + 1) * 64;
        if pos >= len {
            return None;
        }
    };
    // 向右扩展连续段
    let mut end = start;
    loop {
        let zeros = (!mask[end / 64]) >> (end % 64);
        if zeros == 0 {
            // 本 word 剩余全 1，继续下一 word
            end = (end / 64 + 1) * 64;
            if end / 64 >= mask.len() {
                break;
            }
        } else {
            end += zeros.trailing_zeros() as usize; // 段止于本 word 内首个 0
            break;
        }
    }
    Some((start, end.min(len)))
}

/// 谓词批量求值：整段连续 values 上产出 0/1 字节掩码（不含 validity；
/// NULL 行由调用方在根部统一排除，因此 NOT / OR 无需逐层处理 NULL）。
/// Cmp 为类型化切片比较循环（LLVM 可自动向量化）；And / Or / Not 为字节级位运算。
fn eval_predicate_bytes(
    pred: &Predicate,
    data_type: DataType,
    values: &[u8],
    rows: usize,
) -> Result<Vec<u8>, CoreError> {
    match pred {
        Predicate::And(children) => {
            let mut out = vec![1u8; rows];
            for child in children {
                let c = eval_predicate_bytes(child, data_type, values, rows)?;
                for (o, &b) in out.iter_mut().zip(&c) {
                    *o &= b;
                }
                if out.iter().all(|&b| b == 0) {
                    break; // 已无命中，短路
                }
            }
            Ok(out)
        }
        Predicate::Or(children) => {
            let mut out = vec![0u8; rows];
            for child in children {
                let c = eval_predicate_bytes(child, data_type, values, rows)?;
                for (o, &b) in out.iter_mut().zip(&c) {
                    *o |= b;
                }
            }
            Ok(out)
        }
        Predicate::Not(inner) => {
            let c = eval_predicate_bytes(inner, data_type, values, rows)?;
            Ok(c.into_iter().map(|b| b ^ 1).collect())
        }
        Predicate::Cmp { op, value, .. } => eval_cmp_bytes(data_type, values, *op, value, rows),
    }
}

/// 按比较算子生成 6 个直线比较循环（算子分派在循环外，循环体可自动向量化）。
/// `$conv` 为行值到比较域的转换闭包（如 `|a: i8| a as i64`，内联后不阻碍向量化）。
macro_rules! fill_cmp {
    ($out:expr, $arr:expr, $op:expr, $conv:expr, $v:expr) => {
        match $op {
            CmpOp::Eq => for (o, &a) in $arr.iter().enumerate() { $out[o] = (($conv(a)) == $v) as u8 },
            CmpOp::Ne => for (o, &a) in $arr.iter().enumerate() { $out[o] = (($conv(a)) != $v) as u8 },
            CmpOp::Lt => for (o, &a) in $arr.iter().enumerate() { $out[o] = (($conv(a)) < $v) as u8 },
            CmpOp::Le => for (o, &a) in $arr.iter().enumerate() { $out[o] = (($conv(a)) <= $v) as u8 },
            CmpOp::Gt => for (o, &a) in $arr.iter().enumerate() { $out[o] = (($conv(a)) > $v) as u8 },
            CmpOp::Ge => for (o, &a) in $arr.iter().enumerate() { $out[o] = (($conv(a)) >= $v) as u8 },
        }
    };
}

fn typed_slice<T: bytemuck::Pod>(values: &[u8]) -> &[T] {
    bytemuck::cast_slice(values)
}

/// Cmp 批量求值：按列类型分派到类型化比较循环，保持 `compare_scalar` 的跨域加宽语义
/// （有符号 ↔ 无符号 ↔ 浮点；bool 仅与 bool 比较）。浮点遵循 IEEE 语义：
/// NaN 行 / NaN 目标按 IEEE 求值（仅 Ne 命中）。
fn eval_cmp_bytes(
    data_type: DataType,
    values: &[u8],
    op: CmpOp,
    value: &Scalar,
    rows: usize,
) -> Result<Vec<u8>, CoreError> {
    if data_type == DataType::Utf8 {
        return Err(CoreError::Invalid(
            "value predicate on Utf8 field is not supported".into(),
        ));
    }
    let incompatible = || {
        CoreError::Invalid(format!(
            "scalar {value:?} incompatible with column type {data_type:?}"
        ))
    };
    let mut out = vec![0u8; rows];
    match data_type {
        DataType::Bool => {
            let Scalar::Bool(v) = value else { return Err(incompatible()) };
            for (o, &a) in values.iter().enumerate() {
                out[o] = op.matches((a != 0).cmp(v)) as u8;
            }
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Date32 => match value {
            Scalar::Int(v) => match data_type {
                DataType::Int8 => fill_cmp!(out, typed_slice::<i8>(values), op, |a: i8| a as i64, *v),
                DataType::Int16 => {
                    fill_cmp!(out, typed_slice::<i16>(values), op, |a: i16| a as i64, *v)
                }
                _ => fill_cmp!(out, typed_slice::<i32>(values), op, |a: i32| a as i64, *v),
            },
            Scalar::UInt(v) => {
                if *v > i64::MAX as u64 {
                    // 任何 i64 行都小于该目标
                    out.fill(op.matches(std::cmp::Ordering::Less) as u8);
                } else {
                    let v = *v as i64;
                    match data_type {
                        DataType::Int8 => {
                            fill_cmp!(out, typed_slice::<i8>(values), op, |a: i8| a as i64, v)
                        }
                        DataType::Int16 => {
                            fill_cmp!(out, typed_slice::<i16>(values), op, |a: i16| a as i64, v)
                        }
                        _ => fill_cmp!(out, typed_slice::<i32>(values), op, |a: i32| a as i64, v),
                    }
                }
            }
            Scalar::Float(f) => match data_type {
                DataType::Int8 => fill_cmp!(out, typed_slice::<i8>(values), op, |a: i8| a as f64, *f),
                DataType::Int16 => {
                    fill_cmp!(out, typed_slice::<i16>(values), op, |a: i16| a as f64, *f)
                }
                _ => fill_cmp!(out, typed_slice::<i32>(values), op, |a: i32| a as f64, *f),
            },
            _ => return Err(incompatible()),
        },
        DataType::Int64 | DataType::TimestampUs | DataType::Date64 => match value {
            Scalar::Int(v) => fill_cmp!(out, typed_slice::<i64>(values), op, |a: i64| a, *v),
            Scalar::UInt(v) => {
                if *v > i64::MAX as u64 {
                    out.fill(op.matches(std::cmp::Ordering::Less) as u8);
                } else {
                    let v = *v as i64;
                    fill_cmp!(out, typed_slice::<i64>(values), op, |a: i64| a, v);
                }
            }
            Scalar::Float(f) => fill_cmp!(out, typed_slice::<i64>(values), op, |a: i64| a as f64, *f),
            _ => return Err(incompatible()),
        },
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => match value {
            Scalar::UInt(v) => match data_type {
                DataType::UInt8 => {
                    fill_cmp!(out, typed_slice::<u8>(values), op, |a: u8| a as u64, *v)
                }
                DataType::UInt16 => {
                    fill_cmp!(out, typed_slice::<u16>(values), op, |a: u16| a as u64, *v)
                }
                DataType::UInt32 => {
                    fill_cmp!(out, typed_slice::<u32>(values), op, |a: u32| a as u64, *v)
                }
                _ => fill_cmp!(out, typed_slice::<u64>(values), op, |a: u64| a, *v),
            },
            Scalar::Int(v) => {
                if *v < 0 {
                    // 无符号行必然大于负目标
                    out.fill(op.matches(std::cmp::Ordering::Greater) as u8);
                } else {
                    let v = *v as u64;
                    match data_type {
                        DataType::UInt8 => {
                            fill_cmp!(out, typed_slice::<u8>(values), op, |a: u8| a as u64, v)
                        }
                        DataType::UInt16 => {
                            fill_cmp!(out, typed_slice::<u16>(values), op, |a: u16| a as u64, v)
                        }
                        DataType::UInt32 => {
                            fill_cmp!(out, typed_slice::<u32>(values), op, |a: u32| a as u64, v)
                        }
                        _ => fill_cmp!(out, typed_slice::<u64>(values), op, |a: u64| a, v),
                    }
                }
            }
            Scalar::Float(f) => match data_type {
                DataType::UInt8 => {
                    fill_cmp!(out, typed_slice::<u8>(values), op, |a: u8| a as f64, *f)
                }
                DataType::UInt16 => {
                    fill_cmp!(out, typed_slice::<u16>(values), op, |a: u16| a as f64, *f)
                }
                DataType::UInt32 => {
                    fill_cmp!(out, typed_slice::<u32>(values), op, |a: u32| a as f64, *f)
                }
                _ => fill_cmp!(out, typed_slice::<u64>(values), op, |a: u64| a as f64, *f),
            },
            _ => return Err(incompatible()),
        },
        DataType::Float32 => match value {
            Scalar::Float(f) => fill_cmp!(out, typed_slice::<f32>(values), op, |a: f32| a as f64, *f),
            Scalar::Int(v) => {
                fill_cmp!(out, typed_slice::<f32>(values), op, |a: f32| a as f64, *v as f64)
            }
            Scalar::UInt(v) => {
                fill_cmp!(out, typed_slice::<f32>(values), op, |a: f32| a as f64, *v as f64)
            }
            _ => return Err(incompatible()),
        },
        DataType::Float64 => match value {
            Scalar::Float(f) => fill_cmp!(out, typed_slice::<f64>(values), op, |a: f64| a, *f),
            Scalar::Int(v) => fill_cmp!(out, typed_slice::<f64>(values), op, |a: f64| a, *v as f64),
            Scalar::UInt(v) => fill_cmp!(out, typed_slice::<f64>(values), op, |a: f64| a, *v as f64),
            _ => return Err(incompatible()),
        },
        DataType::Utf8 => unreachable!("rejected above"),
    }
    Ok(out)
}
