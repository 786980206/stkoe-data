use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use memmap2::{Mmap, MmapMut};
use splayed_codec::{decode_chunk, encode_chunk};
use splayed_format::{
    Bitmap, BitmapView, Buffer, BufferView, Column, ColumnSegment, ColumnView, Compression,
    DataType, Encoding, FieldHeader, HEADER_SIZE, validity_size,
};

use crate::error::{CoreError, Mode};
use crate::scan::{
    clamp_ranges, compare_scalar, read_row_scalar, Predicate, RowRange, Scalar, ScanRequest,
};

/// 单列值写入 / 初始化的流式读取器（`create_field_file` 的 `stream` init）。
pub trait FieldChunkReader {
    /// 返回下一批 values（`rows * size_of(type)` 字节）与可选 validity 位；`None` = 结束。
    fn next_chunk(&mut self) -> Result<Option<StreamChunk>, CoreError>;
}

/// 流式初始化的一批行。
#[derive(Debug, Clone)]
pub struct StreamChunk {
    pub values: Vec<u8>,
    /// `None` = 本批全部有效（写入时按全 1 位补齐 validity 区）。
    pub validity: Option<Vec<u8>>,
    pub rows: usize,
}

/// `create_field_file` 的初始化方式（docs/splayed-core.md §5.1）：
/// 分配与写值一步完成，不做先预分配再写值的二次写入。
pub enum FieldInit {
    /// 指定逻辑长度的空占位 Field（全 NULL，validity 全 0）。
    Length(u64),
    /// 以给定数据初始化；`row_count` = 数据长度。
    Data(Column),
    /// 流式初始化；最终长度无需预先知道，由流结束决定。
    Stream { chunk_rows: usize, reader: Box<dyn FieldChunkReader> },
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

fn map_io(path: &Path, e: std::io::Error) -> CoreError {
    if e.kind() == std::io::ErrorKind::NotFound {
        CoreError::NotFound(path.to_path_buf())
    } else {
        CoreError::Io(e)
    }
}

fn pack_segment_bits(view: BitmapView<'_>, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; (len + 7) / 8];
    for i in 0..len {
        if view.is_valid(i) {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}

/// 把一个 ColumnView 的内容克隆为拥有字节（values, validity, rows）。
/// 无任何位图的列返回 `validity = None`（全有效）；混有位图的段按全 1 补齐。
pub(crate) fn clone_view(view: &ColumnView<'_>) -> (Vec<u8>, Option<Vec<u8>>, usize) {
    let mut values = Vec::with_capacity(view.length() * view.data_type().size_of());
    let mut bits: Vec<u8> = Vec::new();
    let mut had_bitmap = false;
    for seg in view.segments() {
        values.extend_from_slice(seg.fixed_bytes().expect("field files are fixed-width"));
        let packed = match seg.validity() {
            Some(bm) => {
                had_bitmap = true;
                pack_segment_bits(bm, bm.len())
            }
            None => vec![0xFFu8; (seg.rows() + 7) / 8],
        };
        bits.extend_from_slice(&packed);
    }
    let validity = had_bitmap.then_some(bits);
    (values, validity, view.length())
}

/// 创建并初始化一个 Field 文件（创建完成后才可被 `open_field_file` 打开）。
pub fn create_field_file(
    path: &Path,
    data_type: DataType,
    init: FieldInit,
) -> Result<(), CoreError> {
    if path.exists() {
        return Err(CoreError::AlreadyExists(path.to_path_buf()));
    }
    match init {
        FieldInit::Length(n) => {
            let header = FieldHeader::new_uncompressed(data_type, 1, n as u32, n as u32, true);
            let mut f = File::options()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|e| map_io(path, e))?;
            f.write_all(&header.to_bytes())?;
            // 数据区 + validity 区由 set_len 的 OS 零填充完成，无需显式写 NULL
            let total = HEADER_SIZE as u64 + header.data_length + validity_size(n as u32) as u64;
            f.set_len(total)?;
            f.sync_all()?;
        }
        FieldInit::Data(col) => {
            if col.data_type != data_type {
                return Err(CoreError::Invalid(format!(
                    "init data type {:?} does not match requested {data_type:?}",
                    col.data_type
                )));
            }
            let rows = col.length();
            let has_validity = col.validity.is_some();
            let null_count = col.null_count() as u32;
            let header =
                FieldHeader::new_uncompressed(data_type, 1, rows as u32, null_count, has_validity);
            let mut f = File::options()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|e| map_io(path, e))?;
            f.write_all(&header.to_bytes())?;
            f.write_all(col.values.as_slice())?;
            if let Some(bm) = &col.validity {
                f.write_all(bm.as_view().as_raw())?;
            }
            f.sync_all()?;
        }
        FieldInit::Stream { chunk_rows, mut reader } => {
            if chunk_rows == 0 {
                return Err(CoreError::Invalid("stream chunk_rows must be > 0".into()));
            }
            let mut f = File::options()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|e| map_io(path, e))?;
            // stream 始终写 validity 区（无 validity 的批次按全 1 位补齐）
            let placeholder = FieldHeader::new_uncompressed(data_type, 1, 0, 0, true);
            f.write_all(&placeholder.to_bytes())?;
            let mut total = 0u64;
            let mut nulls = 0u64;
            // validity 位按行累积（chunk 行数非 8 倍数时字节不对齐，必须位级拼接）
            let mut validity_bits: Vec<u8> = Vec::new();
            let mut bit_len = 0usize;
            while let Some(chunk) = reader.next_chunk()? {
                if chunk.values.len() != chunk.rows * data_type.size_of() {
                    return Err(CoreError::Invalid(format!(
                        "stream chunk values length {} does not match {} rows",
                        chunk.values.len(),
                        chunk.rows
                    )));
                }
                if chunk.rows > chunk_rows {
                    return Err(CoreError::Invalid(format!(
                        "stream chunk rows {} exceeds chunk_rows {chunk_rows}",
                        chunk.rows
                    )));
                }
                f.write_all(&chunk.values)?;
                let bits = match chunk.validity {
                    Some(bits) => {
                        if bits.len() != validity_size(chunk.rows as u32) {
                            return Err(CoreError::Invalid(
                                "stream chunk validity byte length does not match rows".into(),
                            ));
                        }
                        bits
                    }
                    None => vec![0xFFu8; validity_size(chunk.rows as u32)],
                };
                // 先扩容再置位（全 0 = NULL，写入时再置 1）
                let needed = (bit_len + chunk.rows + 7) / 8;
                if validity_bits.len() < needed {
                    validity_bits.resize(needed, 0);
                }
                for i in 0..chunk.rows {
                    if bits[i / 8] >> (i % 8) & 1 == 1 {
                        let bit = bit_len + i;
                        validity_bits[bit / 8] |= 1 << (bit % 8);
                    }
                }
                bit_len += chunk.rows;
                nulls += (0..chunk.rows).filter(|i| bits[i / 8] >> (i % 8) & 1 == 0).count() as u64;
                total += chunk.rows as u64;
            }
            let header =
                FieldHeader::new_uncompressed(data_type, 1, total as u32, nulls as u32, true);
            f.seek(SeekFrom::Start(0))?;
            f.write_all(&header.to_bytes())?;
            f.seek(SeekFrom::Start(HEADER_SIZE as u64 + header.data_length))?;
            f.write_all(&validity_bits)?;
            f.sync_all()?;
        }
    }
    Ok(())
}

enum Backing {
    Mmap(Mmap),
    MmapMut(MmapMut),
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

    fn uncompressed_slices(&self) -> Result<(&[u8], Option<&[u8]>), CoreError> {
        let bytes: &[u8] = match &self.backing {
            Backing::Mmap(m) => m,
            Backing::MmapMut(m) => m,
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
    fn decode_working(bytes: &[u8], header: &FieldHeader) -> Result<Working, CoreError> {
        let dt = header.data_type()?;
        let encoding = header.encoding()?;
        let compression = header.compression()?;
        let size = dt.size_of();
        let mut values = Buffer::zeroed_aligned(header.row_count as usize * size, 8);
        let vsize = validity_size(header.row_count);
        let mut bits: Option<Vec<u8>> = (vsize > 0).then(|| vec![0u8; vsize]);
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
                    // chunk 位与全局位图存在字节错位，必须逐位合并
                    for i in 0..rows {
                        if src[i / 8] >> (i % 8) & 1 == 1 {
                            dst[(decoded_rows + i) / 8] |= 1 << ((decoded_rows + i) % 8);
                        }
                    }
                }
                (Some(dst), None) => {
                    for i in 0..rows {
                        dst[(decoded_rows + i) / 8] |= 1 << ((decoded_rows + i) % 8);
                    }
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
        let validity = bits.map(|b| Bitmap::from_bytes(b, header.row_count as usize));
        Ok(Working { values, validity })
    }

    fn chunk_start(&self, index: usize) -> u64 {
        self.chunk_rows[..index].iter().map(|&r| r as u64).sum()
    }

    // --------------------------------------------------------------- read

    /// 按逻辑行（= 物理行）读取，返回 zero-copy ColumnView。
    ///
    /// PLAIN + NONE 为单段 mmap 切片；compressed 跨 chunk 的读取返回多段。
    pub fn read_field_handle(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError> {
        let row_count = self.row_count();
        if offset + length > row_count {
            return Err(CoreError::Invalid(format!(
                "read range [{offset}, {}) exceeds row_count {row_count}",
                offset + length
            )));
        }
        let dt = self.data_type();
        let size = dt.size_of();
        if self.is_chunked() {
            let work = self.working_ref()?;
            let mut segments = Vec::new();
            let mut pos = offset;
            let end = offset + length;
            for (ci, &crows) in self.chunk_rows.iter().enumerate() {
                if pos >= end {
                    break;
                }
                let cstart = self.chunk_start(ci);
                let cend = cstart + crows as u64;
                let lo = pos.max(cstart);
                let hi = end.min(cend);
                if hi > lo {
                    let (lo, hi) = (lo as usize, hi as usize);
                    let values = BufferView::new(&work.values.as_slice()[lo * size..hi * size]);
                    let validity = work
                        .validity
                        .as_ref()
                        .map(|b| {
                            BitmapView::new(BufferView::new(b.as_view().as_raw()), lo, hi - lo)
                        })
                        .transpose()?;
                    segments.push(ColumnSegment::new(dt, values, validity, hi - lo)?);
                    pos = hi as u64;
                }
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
    pub fn write_field_handle(&mut self, offset: u64, data: &ColumnView) -> Result<(), CoreError> {
        self.mode.require_write("write_field_handle")?;
        if data.data_type() != self.data_type() {
            return Err(CoreError::Invalid(format!(
                "write data type {:?} does not match field {:?}",
                data.data_type(),
                self.data_type()
            )));
        }
        if offset + data.length() as u64 > self.row_count() {
            return Err(CoreError::Invalid("write range exceeds row_count".into()));
        }
        if self.is_chunked() {
            self.write_into_working(offset, data)?;
        } else {
            self.write_into_mmap(offset, data)?;
        }
        self.header.generation += 1;
        self.modified = true;
        Ok(())
    }

    fn write_into_mmap(&mut self, offset: u64, data: &ColumnView) -> Result<(), CoreError> {
        let size = self.data_type().size_of();
        let has_validity = self.header.has_validity();
        {
            let (values, validity) = self.uncompressed_slices_mut()?;
            let mut pos = offset as usize;
            for seg in data.segments() {
                let rows = seg.rows();
                values[pos * size..(pos + rows) * size]
                    .copy_from_slice(seg.fixed_bytes().expect("field files are fixed-width"));
                if has_validity {
                    let region = &mut validity[pos..pos + rows];
                    apply_segment_bits(region, seg.validity())?;
                } else if let Some(bm) = seg.validity() {
                    if bm.null_count() > 0 {
                        return Err(CoreError::Invalid(
                            "field has no validity region; cannot write NULLs".into(),
                        ));
                    }
                }
                pos += rows;
            }
        }
        let header_bytes = self.header.to_bytes();
        if let Backing::MmapMut(m) = &mut self.backing {
            m[..HEADER_SIZE].copy_from_slice(&header_bytes);
        }
        Ok(())
    }

    fn write_into_working(&mut self, offset: u64, data: &ColumnView) -> Result<(), CoreError> {
        // 任一段携带含 NULL 的位图且当前无位图时，先物化全 1 位图
        let needs_bits = data.segments().iter().any(|s| {
            s.validity().map(|b| b.null_count() > 0).unwrap_or(false)
        });
        if needs_bits && self.working.as_ref().unwrap().validity.is_none() {
            self.working.as_mut().unwrap().validity =
                Some(Bitmap::ones(self.header.row_count as usize));
        }
        let size = self.data_type().size_of();
        let work = self.working.as_mut().expect("compressed open materializes working");
        let mut pos = offset as usize;
        for seg in data.segments() {
            let rows = seg.rows();
            work.values.as_mut_slice()[pos * size..(pos + rows) * size]
                .copy_from_slice(seg.fixed_bytes().expect("field files are fixed-width"));
            if let Some(bm) = seg.validity() {
                if let Some(full) = work.validity.as_mut() {
                    for i in 0..rows {
                        full.set(pos + i, bm.is_valid(i));
                    }
                } else if bm.null_count() > 0 {
                    return Err(CoreError::Invalid(
                        "field has no validity region; cannot write NULLs".into(),
                    ));
                }
            }
            pos += rows;
        }
        self.header.null_count = work
            .validity
            .as_ref()
            .map(|b| b.as_view().null_count() as u32)
            .unwrap_or(0);
        Ok(())
    }

    /// 修改 header（不改 data）：data_type / row_count 必须与现值一致；
    /// 成功后递增 generation。uncompressed 立即落盘（mmap），compressed 随 close 收尾。
    pub fn update_field_handle(&mut self, mut header: FieldHeader) -> Result<(), CoreError> {
        self.mode.require_write("update_field_handle")?;
        header.magic = self.header.magic;
        header.version = self.header.version;
        header.data_type = self.header.data_type;
        header.row_count = self.header.row_count;
        header.generation = self.header.generation + 1;
        header.validate()?;
        self.header = header;
        self.modified = true;
        if let Backing::MmapMut(m) = &mut self.backing {
            let bytes = self.header.to_bytes();
            m[..HEADER_SIZE].copy_from_slice(&bytes);
        }
        Ok(())
    }

    // --------------------------------------------------------------- scan

    /// 条件扫描：只返回 ranges，不物化数据（docs/splayed-core.md §5.7）。
    pub fn scan_field_handle(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError> {
        let ranges = clamp_ranges(&request.ranges, self.row_count());
        Ok(FieldScanner {
            handle: self,
            ranges,
            range_index: 0,
            cursor: 0,
            predicate: request.predicate.clone(),
            remaining: request.limit,
            pending: VecDeque::new(),
            done: false,
        })
    }

    /// 供 scanner 使用的块视图（块不跨 chunk 边界，保证单段连续）。
    fn block_views(
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

    /// 下一个求值块的结束行（不跨 chunk 边界 / 块大小上限 8192）。
    fn block_end(&self, start: u64, range_end: u64) -> u64 {
        let mut end = start.saturating_add(8192).min(range_end);
        if self.is_chunked() {
            let mut cum = 0u64;
            for &r in &self.chunk_rows {
                cum += r as u64;
                if cum > start {
                    end = end.min(cum);
                    break;
                }
            }
        }
        end
    }
}

/// 把段的 validity 位应用到一个 bit 区间（`region[i]` ↔ 行 `pos+i`）。
fn apply_segment_bits(region: &mut [u8], validity: Option<BitmapView<'_>>) -> Result<(), CoreError> {
    match validity {
        Some(bm) => {
            if bm.len() != region.len() {
                return Err(CoreError::Invalid("validity length does not match rows".into()));
            }
            for (i, slot) in region.iter_mut().enumerate() {
                *slot = u8::from(bm.is_valid(i));
            }
        }
        None => {
            for slot in region.iter_mut() {
                *slot = 1;
            }
        }
    }
    Ok(())
}

/// 关闭 Handle：read 无写回；uncompressed write 已直接生效（flush + fsync）；
/// compressed write 发生修改 → 自动 compress + rewrite（临时文件 + 原子替换，
/// 分组沿用打开时读得的 chunk 头；写路径不改 row_count，分组可精确复用）。
pub fn close_field_handle(handle: FieldHandle) -> Result<(), CoreError> {
    let FieldHandle { path, header, mode, backing, chunk_rows, working, modified } = handle;
    match mode {
        Mode::Read => return Ok(()),
        Mode::Write => {}
    }
    if !header.is_chunked() {
        if let Backing::MmapMut(m) = &backing {
            m.flush()?;
        }
        drop(backing);
        let f = File::options().write(true).open(&path)?;
        f.sync_all()?;
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
    let mut out = Vec::with_capacity(HEADER_SIZE + work.values.len() / 2);
    out.extend_from_slice(&header.to_bytes());
    let mut cum = 0u64;
    for &rows in &chunk_rows {
        let lo = cum as usize;
        let hi = lo + rows as usize;
        let values = &work.values.as_slice()[lo * size..hi * size];
        let validity = work
            .validity
            .as_ref()
            .map(|b| b.extract_bits(lo, rows as usize))
            .transpose()?;
        out.extend_from_slice(&encode_chunk(
            encoding,
            compression,
            dt,
            values,
            validity.as_deref(),
            rows as usize,
        )?);
        cum += rows as u64;
    }
    let tmp = tmp_path(&path);
    let mut f = File::options().write(true).create(true).truncate(true).open(&tmp)?;
    f.write_all(&out)?;
    f.sync_all()?;
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

/// 原子写出（临时文件 + fsync + rename）。
fn write_field_atomic(
    path: &Path,
    header: FieldHeader,
    values: &[u8],
    validity: Option<&[u8]>,
) -> Result<(), CoreError> {
    let tmp = tmp_path(path);
    {
        let mut f = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&header.to_bytes())?;
        f.write_all(values)?;
        if let Some(bits) = validity {
            f.write_all(bits)?;
        }
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// 将 `path` 处 Field 原地转换为 `target_type`（临时文件 + 原子 rename；
/// 失败时原文件保持不变）。逐行 `as` 语义转换（bool ↔ 数值 ↔ 浮点；Utf8 不支持）。
pub fn cast_field_file(path: &Path, target_type: DataType) -> Result<(), CoreError> {
    let (rows, values, validity, src_type) = {
        let handle = open_field_file(path, Mode::Read)?;
        let dt = handle.data_type();
        if dt == target_type {
            return Ok(());
        }
        if dt == DataType::Utf8 || target_type == DataType::Utf8 {
            return Err(CoreError::Invalid("cast involving Utf8 is not supported".into()));
        }
        let owned = clone_view(&handle.read_field_handle(0, handle.row_count())?);
        (owned.2, owned.0, owned.1, dt)
    };
    let converted = convert_values(src_type, target_type, &values, validity, rows)?;
    let (values, validity, rows) = converted;
    let null_count = validity
        .as_ref()
        .map(|b| BitmapView::new(BufferView::new(b), 0, rows).unwrap().null_count() as u32)
        .unwrap_or(0);
    let header =
        FieldHeader::new_uncompressed(target_type, 1, rows as u32, null_count, validity.is_some());
    write_field_atomic(path, header, &values, validity.as_deref())
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
        let raw: [u8; 8] = if dst == DataType::Float32 || dst == DataType::Float64 {
            scalar_to_f64(&scalar).to_le_bytes()
        } else if dst.is_signed_int() {
            scalar_to_i64(&scalar).to_le_bytes()
        } else {
            scalar_to_u64(&scalar).to_le_bytes()
        };
        out[i * ds..i * ds + ds].copy_from_slice(&raw[..ds]);
    }
    Ok((out, validity, rows))
}

/// 压缩已有 Field（uncompressed → compressed，保持 encoding、compression 置 ZSTD，
/// 临时文件 + 原子替换）。`offsets`：可选的 chunk 起始行号（升序、`offsets[0] == 0`，
/// 末块隐含到 row_count）；省略时按固定 8192 行均匀分块。分块策略是调用方的职责
/// （Dataset 层按 META 网格生成 sym 对齐边界）。
pub fn compress_field_file(path: &Path, offsets: Option<Vec<u64>>) -> Result<(), CoreError> {
    let (header, values, validity) = {
        let handle = open_field_file(path, Mode::Read)?;
        if handle.is_chunked() {
            return Err(CoreError::InvalidState("field is already compressed".into()));
        }
        let owned = clone_view(&handle.read_field_handle(0, handle.row_count())?);
        (handle.header, owned.0, owned.1)
    };
    let rows = header.row_count as usize;
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
    let dt = header.data_type()?;
    let size = dt.size_of();
    let mut new_header = header;
    new_header.compression = Compression::Zstd.id();
    new_header.encoding = Encoding::Plain.id();
    new_header.set_has_validity(false);
    new_header.validity_offset = 0;
    let mut out = Vec::with_capacity(HEADER_SIZE + values.len() / 2);
    out.extend_from_slice(&new_header.to_bytes());
    for w in boundaries.windows(2) {
        let (lo, hi) = (w[0] as usize, w[1] as usize);
        let bits = validity
            .as_ref()
            .map(|b| pack_segment_bits(BitmapView::new(BufferView::new(b), lo, hi - lo).unwrap(), hi - lo));
        out.extend_from_slice(&encode_chunk(
            Encoding::Plain,
            Compression::Zstd,
            dt,
            &values[lo * size..hi * size],
            bits.as_deref(),
            hi - lo,
        )?);
    }
    let last = *boundaries.last().unwrap() as usize;
    if last < rows {
        let bits = validity
            .as_ref()
            .map(|b| pack_segment_bits(BitmapView::new(BufferView::new(b), last, rows - last).unwrap(), rows - last));
        out.extend_from_slice(&encode_chunk(
            Encoding::Plain,
            Compression::Zstd,
            dt,
            &values[last * size..],
            bits.as_deref(),
            rows - last,
        )?);
    }
    write_field_atomic(path, new_header, &out[HEADER_SIZE..], None)?;
    Ok(())
}

/// 解压已有 Field（compressed → uncompressed，PLAIN + NONE，临时文件 + 原子替换）。
pub fn decompress_field_file(path: &Path) -> Result<(), CoreError> {
    let (header, values, validity) = {
        let handle = open_field_file(path, Mode::Read)?;
        if !handle.is_chunked() {
            return Err(CoreError::InvalidState("field is not compressed".into()));
        }
        let owned = clone_view(&handle.read_field_handle(0, handle.row_count())?);
        (handle.header, owned.0, owned.1)
    };
    let new_header = FieldHeader::new_uncompressed(
        header.data_type()?,
        header.generation,
        header.row_count,
        header.null_count,
        validity.is_some(),
    );
    write_field_atomic(path, new_header, &values, validity.as_deref())
}

/// 打开已有 Field（open 不负责创建）。compressed Field 打开时一次性解压为工作表示。
pub fn open_field_file(path: &Path, mode: Mode) -> Result<FieldHandle, CoreError> {
    let mut file = File::open(path).map_err(|e| map_io(path, e))?;
    let mut header_bytes = [0u8; HEADER_SIZE];
    file.read_exact(&mut header_bytes)?;
    let header = FieldHeader::from_bytes(&header_bytes)?;
    let file_len = file.metadata()?.len() as usize;
    drop(file);
    // 以只读 mmap 完成 chunk 分组读取与工作表示解压
    let map = unsafe { Mmap::map(&File::open(path)?)? };
    let mut chunk_rows = Vec::new();
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
    let working = if header.is_chunked() {
        Some(FieldHandle::decode_working(&map, &header)?)
    } else {
        None
    };
    drop(map);
    if mode == Mode::Read {
        let file = File::open(path)?;
        Ok(FieldHandle {
            path: path.to_path_buf(),
            header,
            mode,
            backing: Backing::Mmap(unsafe { Mmap::map(&file)? }),
            chunk_rows,
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
            working,
            modified: false,
        })
    }
}

// ------------------------------------------------------------------ Scanner

/// Field Scanner：单 Field 条件扫描，只输出物理 RowRange（docs/splayed-core.md §5.7）。
pub struct FieldScanner<'h> {
    handle: &'h FieldHandle,
    ranges: Vec<RowRange>,
    range_index: usize,
    cursor: u64,
    predicate: Option<Predicate>,
    remaining: Option<u64>,
    pending: VecDeque<RowRange>,
    done: bool,
}

impl<'h> FieldScanner<'h> {
    /// 每次返回一个连续 RowRange；结束返回 `None`。
    pub fn next(&mut self) -> Result<Option<RowRange>, CoreError> {
        loop {
            if let Some(r) = self.pending.pop_front() {
                if let Some(rem) = &mut self.remaining {
                    let take = r.length.min(*rem);
                    *rem -= take;
                    if *rem == 0 {
                        self.done = true;
                        self.pending.clear();
                    }
                    if take < r.length {
                        return Ok(Some(RowRange::new(r.offset, take)));
                    }
                }
                return Ok(Some(r));
            }
            if self.done || self.range_index >= self.ranges.len() {
                return Ok(None);
            }
            let range = self.ranges[self.range_index];
            if self.cursor < range.offset {
                self.cursor = range.offset;
            }
            if self.cursor >= range.end() {
                self.range_index += 1;
                self.cursor = 0;
                continue;
            }
            let block_end = self.handle.block_end(self.cursor, range.end());
            let rows = (block_end - self.cursor) as usize;
            let (values, validity) = self.handle.block_views(self.cursor, rows as u64)?;
            let mut selection = vec![true; rows];
            match &self.predicate {
                Some(pred) => eval_predicate(
                    pred,
                    self.handle.data_type(),
                    values,
                    validity.as_ref(),
                    rows,
                    &mut selection,
                )?,
                None => {
                    if let Some(v) = &validity {
                        for (i, sel) in selection.iter_mut().enumerate() {
                            *sel = v.is_valid(i);
                        }
                    }
                }
            }
            let base = self.cursor;
            let mut i = 0usize;
            while i < rows {
                if selection[i] {
                    let start = i;
                    while i < rows && selection[i] {
                        i += 1;
                    }
                    self.pending
                        .push_back(RowRange::new(base + start as u64, (i - start) as u64));
                } else {
                    i += 1;
                }
            }
            self.cursor = block_end;
            if self.cursor >= range.end() {
                self.range_index += 1;
                self.cursor = 0;
            }
        }
    }

    /// 关闭（资源随 handle 生命周期管理，此处仅为契约完备）。
    pub fn close(self) -> Result<(), CoreError> {
        Ok(())
    }
}

/// 在一段连续 values 上求值谓词，产出行选择（true = 命中）。
/// NULL 行不命中任何条件（含 NOT / OR 分支）。
pub(crate) fn eval_predicate(
    pred: &Predicate,
    data_type: DataType,
    values: &[u8],
    validity: Option<&BitmapView<'_>>,
    rows: usize,
    selection: &mut [bool],
) -> Result<(), CoreError> {
    let row_valid = |i: usize| validity.map(|v| v.is_valid(i)).unwrap_or(true);
    match pred {
        Predicate::And(children) => {
            for (i, sel) in selection.iter_mut().enumerate() {
                *sel &= row_valid(i);
            }
            for child in children {
                eval_predicate(child, data_type, values, validity, rows, selection)?;
                if selection.iter().all(|&s| !s) {
                    break;
                }
            }
            Ok(())
        }
        Predicate::Or(children) => {
            let mut acc = vec![false; rows];
            for child in children {
                let mut tmp = vec![false; rows];
                eval_predicate(child, data_type, values, validity, rows, &mut tmp)?;
                for i in 0..rows {
                    acc[i] |= tmp[i];
                }
            }
            for (i, sel) in selection.iter_mut().enumerate() {
                *sel &= acc[i] && row_valid(i);
            }
            Ok(())
        }
        Predicate::Not(inner) => {
            let mut tmp = vec![true; rows];
            eval_predicate(inner, data_type, values, validity, rows, &mut tmp)?;
            for (i, sel) in selection.iter_mut().enumerate() {
                *sel &= row_valid(i) && !tmp[i];
            }
            Ok(())
        }
        Predicate::Cmp { op, value, .. } => {
            if data_type == DataType::Utf8 {
                return Err(CoreError::Invalid(
                    "value predicate on Utf8 field is not supported".into(),
                ));
            }
            let size = data_type.size_of();
            for i in 0..rows {
                let row_scalar = read_row_scalar(data_type, &values[i * size..i * size + size])?;
                let hit = row_valid(i)
                    && compare_scalar(&row_scalar, *op, value)
                    .ok_or_else(|| {
                        CoreError::Invalid(format!(
                            "scalar {value:?} incompatible with column type {data_type:?}"
                        ))
                    })?;
                selection[i] &= hit;
            }
            Ok(())
        }
    }
}
