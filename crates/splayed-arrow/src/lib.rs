//! splayed-arrow：V2.0 Arrow 转换层（core 内存模型 ↔ Arrow RecordBatch）。
//!
//! 权威设计见 `docs/splayed-arrow.md`。三层 API：
//! - **Layer 1 Array 转换**：`column_to_array` / `column_view_to_array`（单列）；
//! - **Layer 2 Batch 转换**：`data_to_record_batch` / `data_view_to_record_batch`
//!   / `record_batch_to_data`（core ↔ Arrow 纯内存转换）；
//! - **Layer 3 Table 流式适配**：`read_table_as_arrow` → `TableArrowReader`
//!   （包装 `TableReader` 逐批输出 RecordBatch，不拥有查询语义）。

use std::sync::Arc;

use arrow_array::{
    types::Int32Type,
    ArrayRef, BooleanArray, Date32Array, Date64Array, DictionaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_array::Array as _;
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType as ArrowDt, Field as ArrowField, Schema as ArrowSchema,
                    TimeUnit};

use splayed_table::{TableBatchReader, TableHandle, TableScanner};
use splayed_format::{Data, DataView};
use splayed_format::{
    Bitmap, Buffer, Column, DataType, DictBuffers, FieldSchema, Schema,
};

/// 转换错误。
#[derive(Debug)]
pub enum ArrowConvError {
    Core(splayed_core::CoreError),
    Format(splayed_format::FormatError),
    Arrow(ArrowError),
    Unsupported(String),
}

impl std::fmt::Display for ArrowConvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowConvError::Core(e) => write!(f, "core: {e}"),
            ArrowConvError::Format(e) => write!(f, "format: {e}"),
            ArrowConvError::Arrow(e) => write!(f, "arrow: {e}"),
            ArrowConvError::Unsupported(msg) => write!(f, "unsupported: {msg}"),
        }
    }
}

impl std::error::Error for ArrowConvError {}

impl From<splayed_core::CoreError> for ArrowConvError {
    fn from(e: splayed_core::CoreError) -> Self {
        ArrowConvError::Core(e)
    }
}

impl From<splayed_format::FormatError> for ArrowConvError {
    fn from(e: splayed_format::FormatError) -> Self {
        ArrowConvError::Format(e)
    }
}

impl From<ArrowError> for ArrowConvError {
    fn from(e: ArrowError) -> Self {
        ArrowConvError::Arrow(e)
    }
}

pub type Result<T> = std::result::Result<T, ArrowConvError>;

// ---------------------------------------------------------------- 类型映射

/// V2.0 DataType → Arrow 逻辑类型（docs/splayed-arrow.md §2）。
pub fn to_arrow_type(dt: DataType) -> ArrowDt {
    match dt {
        DataType::Bool => ArrowDt::Boolean,
        DataType::Int8 => ArrowDt::Int8,
        DataType::Int16 => ArrowDt::Int16,
        DataType::Int32 => ArrowDt::Int32,
        DataType::Date32 => ArrowDt::Date32,
        DataType::Int64 => ArrowDt::Int64,
        DataType::TimestampUs => ArrowDt::Timestamp(TimeUnit::Microsecond, None),
        DataType::Date64 => ArrowDt::Date64,
        DataType::UInt8 => ArrowDt::UInt8,
        DataType::UInt16 => ArrowDt::UInt16,
        DataType::UInt32 => ArrowDt::UInt32,
        DataType::UInt64 => ArrowDt::UInt64,
        DataType::Float32 => ArrowDt::Float32,
        DataType::Float64 => ArrowDt::Float64,
        DataType::Utf8 => {
            ArrowDt::Dictionary(Box::new(ArrowDt::Int32), Box::new(ArrowDt::Utf8))
        }
    }
}

/// Arrow 逻辑类型 → V2.0 DataType（`record_batch_to_data` 反向映射）。
pub fn from_arrow_type(dt: &ArrowDt) -> Result<DataType> {
    Ok(match dt {
        ArrowDt::Boolean => DataType::Bool,
        ArrowDt::Int8 => DataType::Int8,
        ArrowDt::Int16 => DataType::Int16,
        ArrowDt::Int32 => DataType::Int32,
        ArrowDt::Int64 => DataType::Int64,
        ArrowDt::UInt8 => DataType::UInt8,
        ArrowDt::UInt16 => DataType::UInt16,
        ArrowDt::UInt32 => DataType::UInt32,
        ArrowDt::UInt64 => DataType::UInt64,
        ArrowDt::Float32 => DataType::Float32,
        ArrowDt::Float64 => DataType::Float64,
        ArrowDt::Date32 => DataType::Date32,
        ArrowDt::Timestamp(TimeUnit::Microsecond, _) => DataType::TimestampUs,
        ArrowDt::Date64 => DataType::Date64,
        ArrowDt::Utf8 | ArrowDt::LargeUtf8 => DataType::Utf8,
        ArrowDt::Dictionary(_, v) if **v == ArrowDt::Utf8 || **v == ArrowDt::LargeUtf8 => DataType::Utf8,
        other => {
            return Err(ArrowConvError::Unsupported(format!(
                "arrow type {other:?} has no V2.0 mapping"
            )))
        }
    })
}

// ---------------------------------------------------------------- validity

/// V2.0 Bitmap → Arrow NullBuffer（位序完全一致，LSB-first 直接字节级零解包映射）。
fn bitmap_to_null_buffer(bm: &Bitmap) -> NullBuffer {
    let view = bm.as_view();
    let raw = view.as_raw();
    let buffer = arrow_buffer::Buffer::from(raw);
    let boolean_buf = arrow_buffer::BooleanBuffer::new(buffer, view.bit_offset(), view.len());
    NullBuffer::new(boolean_buf)
}

/// Arrow null buffer → V2.0 Bitmap。
fn null_buffer_to_bitmap(nulls: &NullBuffer) -> Bitmap {
    let len = nulls.len();
    let bytes: Vec<u8> = nulls.inner().values().iter().copied().collect();
    Bitmap::from_bytes(bytes, len)
}

// ---------------------------------------------------------------- to arrow

/// 拥有型 Column → Arrow ArrayRef（Layer 1；docs/splayed-arrow.md §5）。
pub fn column_to_array(col: &Column) -> Result<ArrayRef> {
    let validity = col.validity.as_ref().map(bitmap_to_null_buffer);
    let values = col.values.as_slice();
    match col.data_type {
        DataType::Bool => Ok(Arc::new(BooleanArray::new(
            values.iter().map(|&b| b != 0).collect(),
            validity,
        ))),
        DataType::Int8 => Ok(Arc::new(Int8Array::new(
            cast_to_vec::<i8>(values)?.into(),
            validity,
        ))),
        DataType::Int16 => Ok(Arc::new(Int16Array::new(
            cast_to_vec::<i16>(values)?.into(),
            validity,
        ))),
        DataType::Int32 => Ok(Arc::new(Int32Array::new(
            cast_to_vec::<i32>(values)?.into(),
            validity,
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::new(
            cast_to_vec::<i64>(values)?.into(),
            validity,
        ))),
        DataType::UInt8 => Ok(Arc::new(UInt8Array::new(
            cast_to_vec::<u8>(values)?.into(),
            validity,
        ))),
        DataType::UInt16 => Ok(Arc::new(UInt16Array::new(
            cast_to_vec::<u16>(values)?.into(),
            validity,
        ))),
        DataType::UInt32 => Ok(Arc::new(UInt32Array::new(
            cast_to_vec::<u32>(values)?.into(),
            validity,
        ))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::new(
            cast_to_vec::<u64>(values)?.into(),
            validity,
        ))),
        DataType::Float32 => Ok(Arc::new(Float32Array::new(
            cast_to_vec::<f32>(values)?.into(),
            validity,
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::new(
            cast_to_vec::<f64>(values)?.into(),
            validity,
        ))),
        DataType::Date32 => Ok(Arc::new(Date32Array::new(
            cast_to_vec::<i32>(values)?.into(),
            validity,
        ))),
        DataType::TimestampUs => Ok(Arc::new(TimestampMicrosecondArray::new(
            cast_to_vec::<i64>(values)?.into(),
            validity,
        ))),
        DataType::Date64 => Ok(Arc::new(Date64Array::new(
            cast_to_vec::<i64>(values)?.into(),
            validity,
        ))),
        DataType::Utf8 => {
            let dict = col.dict.as_ref().ok_or_else(|| {
                ArrowConvError::Unsupported("Utf8 column without dict buffers".into())
            })?;
            let DictBuffers { offsets, strings } = dict;
            let read = |i: usize| -> (usize, usize) {
                let lo = u64::from_le_bytes(offsets.as_slice()[i * 8..i * 8 + 8].try_into().unwrap())
                    as usize;
                let hi = u64::from_le_bytes(
                    offsets.as_slice()[(i + 1) * 8..(i + 1) * 8 + 8].try_into().unwrap(),
                ) as usize;
                (lo, hi)
            };
            let n = offsets.len() / 8 - 1;
            let mut parts: Vec<&str> = Vec::with_capacity(n);
            for i in 0..n {
                let (lo, hi) = read(i);
                parts.push(std::str::from_utf8(&strings.as_slice()[lo..hi]).map_err(|e| {
                    ArrowConvError::Unsupported(format!("dict string is not utf-8: {e}"))
                })?);
            }
            let values_arr = StringArray::from(parts);
            // keys: u32 → i32（位宽转换，值域校验，兼顾空切片与对齐）
            let keys_i32: Vec<i32> = if values.is_empty() {
                Vec::new()
            } else if let Ok(raw_keys) = bytemuck::try_cast_slice::<u8, u32>(values) {
                raw_keys
                    .iter()
                    .map(|&k| {
                        i32::try_from(k).map_err(|_| {
                            ArrowConvError::Unsupported(format!("dict key {k} exceeds i32"))
                        })
                    })
                    .collect::<Result<_>>()?
            } else {
                values
                    .chunks_exact(4)
                    .map(|chunk| {
                        let k = u32::from_le_bytes(chunk.try_into().unwrap());
                        i32::try_from(k).map_err(|_| {
                            ArrowConvError::Unsupported(format!("dict key {k} exceeds i32"))
                        })
                    })
                    .collect::<Result<_>>()?
            };
            let dict_keys = Int32Array::new(keys_i32.into(), validity);
            Ok(Arc::new(
                DictionaryArray::try_new(dict_keys, Arc::new(values_arr))?,
            ))
        }
    }
}

fn cast_to_vec<T: bytemuck::Pod>(bytes: &[u8]) -> Result<Vec<T>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if let Ok(slice) = bytemuck::try_cast_slice::<u8, T>(bytes) {
        return Ok(slice.to_vec());
    }
    let size = std::mem::size_of::<T>();
    if bytes.len() % size != 0 {
        return Err(ArrowConvError::Unsupported(
            "byte length does not match type size".into(),
        ));
    }
    let count = bytes.len() / size;
    let mut out = Vec::with_capacity(count);
    for chunk in bytes.chunks_exact(size) {
        out.push(bytemuck::pod_read_unaligned(chunk));
    }
    Ok(out)
}

/// 多段视图 → 单 ArrayRef（Layer 1；段间按行序拼接拷贝——RecordBatch 单列单数组
/// 约束；跨段零拷贝依赖 core buffer ownership 与 Arrow buffer layout 的兼容设计，
/// 见 docs/splayed-arrow.md §4）。
pub fn column_view_to_array(view: &splayed_format::ColumnView<'_>) -> Result<ArrayRef> {
    // 单段走拥有列的路径（语义一致）
    if view.segments().len() == 1 {
        let seg = &view.segments()[0];
        let owned = segment_to_owned(seg, view.data_type());
        return column_to_array(&owned);
    }
    // 多段：逐段构造后用 arrow concat 拼接
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(view.segments().len());
    for seg in view.segments() {
        let owned = segment_to_owned(seg, view.data_type());
        arrays.push(column_to_array(&owned)?);
    }
    if arrays.is_empty() {
        // 空列：单空段路径已覆盖；防御分支
        let owned = segment_to_owned(&splayed_format::ColumnSegment::new(
            view.data_type(),
            splayed_format::BufferView::new(&[]),
            None,
            0,
        )?, view.data_type());
        return column_to_array(&owned);
    }
    // concat（同类型）
    let dt = arrays[0].data_type().clone();
    let merged: ArrayRef = match dt {
        ArrowDt::Dictionary(_k, v) => {
            use std::collections::HashMap;
            let mut key_map: HashMap<String, i32> = HashMap::new();
            let mut unique_strings: Vec<String> = Vec::new();
            let mut keys: Vec<Option<i32>> = Vec::new();

            for arr in &arrays {
                let d = arr
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .ok_or_else(|| ArrowConvError::Unsupported("dict concat mismatch".into()))?;
                let vals = d
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| ArrowConvError::Unsupported("dict values not utf8".into()))?;
                for k in d.keys().iter() {
                    match k {
                        Some(idx) => {
                            let s = vals.value(idx as usize);
                            let mapped_id = match key_map.get(s) {
                                Some(&id) => id,
                                None => {
                                    let new_id = unique_strings.len() as i32;
                                    unique_strings.push(s.to_string());
                                    key_map.insert(s.to_string(), new_id);
                                    new_id
                                }
                            };
                            keys.push(Some(mapped_id));
                        }
                        None => keys.push(None),
                    }
                }
                let _ = v;
            }
            let keys_arr = Int32Array::from(keys);
            let values_arr = StringArray::from(unique_strings);
            Arc::new(DictionaryArray::try_new(keys_arr, Arc::new(values_arr))?)
        }
        _ => {
            let refs: Vec<&dyn arrow_array::Array> =
                arrays.iter().map(|a| a.as_ref()).collect();
            arrow_select::concat::concat(&refs)?
        }
    };
    Ok(merged)
}

fn segment_to_owned(
    seg: &splayed_format::ColumnSegment<'_>,
    dt: DataType,
) -> Column {
    let validity = seg.validity().map(|b| Bitmap::from_bytes(pack_bits(b), b.len()));
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
            // Arrow 边界物化：RepeatDict → 重复 keys（core 层零存储，此处按需物化）
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
}

fn pack_bits(view: splayed_format::BitmapView<'_>) -> Vec<u8> {
    view.to_packed_bytes()
}

/// Schema → Arrow 字段列表（类型映射统一入口，两条 batch 路径共用）。
/// V2 不建模 NOT NULL 约束：nullable 恒为 true——schema 跨批次稳定，
/// 不随当批是否含 NULL 变化。
fn arrow_fields(schema: &Schema) -> Vec<ArrowField> {
    schema
        .fields
        .iter()
        .map(|f| ArrowField::new(f.name.as_ref(), to_arrow_type(f.data_type), true))
        .collect()
}

/// Data → RecordBatch（Layer 2；schema 字段顺序保持，逐列复用 `column_to_array`）。
pub fn data_to_record_batch(data: &Data) -> Result<RecordBatch> {
    let fields = arrow_fields(&data.schema);
    let mut arrays = Vec::with_capacity(data.schema.fields.len());
    for field in &data.schema.fields {
        let col = data.column(&field.name).expect("schema iteration guarantees");
        arrays.push(column_to_array(col)?);
    }
    let arrow_schema = Arc::new(ArrowSchema::new(fields));
    Ok(RecordBatch::try_new(arrow_schema, arrays)?)
}

/// 多段 DataView → RecordBatch（Layer 2；逐列视图直转，**不物化为 Data**——
/// 否则引入一次多余的全量拷贝）。
pub fn data_view_to_record_batch<'a>(view: &DataView<'a>) -> Result<RecordBatch> {
    let fields = arrow_fields(&view.schema);
    let mut arrays = Vec::with_capacity(view.schema.fields.len());
    for field in &view.schema.fields {
        let col = view.column(&field.name).expect("schema iteration guarantees");
        arrays.push(column_view_to_array(col)?);
    }
    let arrow_schema = Arc::new(ArrowSchema::new(fields));
    Ok(RecordBatch::try_new(arrow_schema, arrays)?)
}

// ---------------------------------------------------------------- Table → Arrow

// ---------------------------------------------------------------- TableArrowReader & TableArrowWriter

/// Table → Arrow 流式批次读取器
pub struct TableArrowBatchReader<'t> {
    inner: TableBatchReader<'t>,
}

impl<'t> TableArrowBatchReader<'t> {
    pub fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.inner.next()? {
            None => Ok(None),
            Some(view) => data_view_to_record_batch(&view).map(Some),
        }
    }

    pub fn close(self) -> Result<()> {
        self.inner.close().map_err(ArrowConvError::from)
    }
}

/// TableArrowReader：与 TableReader 完全对齐的 Arrow 原生只读表对象
pub struct TableArrowReader {
    inner: splayed_table::TableReader,
}

impl TableArrowReader {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let inner = splayed_table::TableReader::open(path)?;
        Ok(Self { inner })
    }

    pub fn open_with_options(path: &std::path::Path, options: splayed_table::TableOptions) -> Result<Self> {
        let inner = splayed_table::TableReader::open_with_options(path, options)?;
        Ok(Self { inner })
    }

    /// 惰性条件扫描
    pub fn scan(&self, request: splayed_table::TableScanRequest) -> Result<splayed_table::TableScanner<'_>> {
        self.inner.scan(request).map_err(ArrowConvError::from)
    }

    /// 一步式流式读取为 Arrow RecordBatch
    pub fn read<'t>(
        &'t self,
        request: splayed_table::TableScanRequest,
        batch_size: Option<usize>,
    ) -> Result<TableArrowBatchReader<'t>> {
        let scanner = self.inner.scan(request)?;
        Ok(TableArrowBatchReader {
            inner: scanner.into_reader(batch_size),
        })
    }

    /// 读取指定分区行范围并转为 Arrow RecordBatch
    pub fn read_range(
        &self,
        range: &splayed_table::PartitionRowRange,
        projection: Option<&[&str]>,
    ) -> Result<RecordBatch> {
        let view = self.inner.read_range(range, projection)?;
        data_view_to_record_batch(&view)
    }

    /// 读取 Arrow Schema
    pub fn schema(&self) -> Result<Arc<ArrowSchema>> {
        let splayed_schema = self.inner.schema()?;
        let fields = arrow_fields(&splayed_schema);
        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    pub fn metadata(&self) -> Result<splayed_table::TableMetadata> {
        self.inner.metadata().map_err(ArrowConvError::from)
    }

    pub fn statistics(&self) -> Result<splayed_table::TableStatistics> {
        self.inner.statistics().map_err(ArrowConvError::from)
    }

    pub fn path(&self) -> &std::path::Path {
        self.inner.path()
    }

    pub fn scheme(&self) -> splayed_table::PartitionScheme {
        self.inner.scheme()
    }

    pub fn close(self) -> Result<()> {
        self.inner.close().map_err(ArrowConvError::from)
    }
}

/// TableArrowWriter：与 TableWriter 完全对齐的 Arrow 原生可写表对象
pub struct TableArrowWriter {
    inner: splayed_table::TableWriter,
}

impl TableArrowWriter {
    pub fn create(
        path: &std::path::Path,
        arrow_schema: &ArrowSchema,
        scheme: splayed_table::PartitionScheme,
        initial_partition: Option<&str>,
        options: Option<splayed_table::TableOptions>,
    ) -> Result<Self> {
        let mut fields = Vec::with_capacity(arrow_schema.fields().len());
        for f in arrow_schema.fields() {
            let dt = from_arrow_type(f.data_type())?;
            fields.push(FieldSchema::new(f.name().as_str(), dt));
        }
        let schema = Schema::new(fields);
        let inner = splayed_table::TableWriter::create(path, &schema, scheme, initial_partition, options)?;
        Ok(Self { inner })
    }

    pub fn init(
        path: &std::path::Path,
        scheme: splayed_table::PartitionScheme,
        batch: &RecordBatch,
        options: Option<splayed_table::TableOptions>,
    ) -> Result<Self> {
        let data = record_batch_to_data(batch)?;
        let inner = splayed_table::TableWriter::init(path, scheme, &data.as_view(), options)?;
        Ok(Self { inner })
    }

    pub fn open(path: &std::path::Path) -> Result<Self> {
        let inner = splayed_table::TableWriter::open(path)?;
        Ok(Self { inner })
    }

    pub fn open_with_options(path: &std::path::Path, options: splayed_table::TableOptions) -> Result<Self> {
        let inner = splayed_table::TableWriter::open_with_options(path, options)?;
        Ok(Self { inner })
    }

    /// 接收 RecordBatch，覆盖写入对应分区
    pub fn write(&self, batch: &RecordBatch) -> Result<()> {
        let data = record_batch_to_data(batch)?;
        self.inner.write(&data.as_view()).map_err(ArrowConvError::from)
    }

    /// 接收 RecordBatch，全量原子替换更新表数据
    pub fn update(&self, batch: &RecordBatch) -> Result<()> {
        let data = record_batch_to_data(batch)?;
        self.inner.update(&data.as_view()).map_err(ArrowConvError::from)
    }

    /// 删除分区
    pub fn delete_partition(&self, partition_name: &str) -> Result<()> {
        self.inner.delete_partition(partition_name).map_err(ArrowConvError::from)
    }

    /// 字段初始化 DDL 操作：仅在最新分区创建物理文件，自动与最新分区 index 行数对齐
    pub fn init_field(
        &self,
        field_name: &str,
        data_type: DataType,
        opts: Option<splayed_core::CreateFieldOptions>,
    ) -> Result<()> {
        self.inner.init_field(field_name, data_type, opts).map_err(ArrowConvError::from)
    }

    pub fn delete_field(&self, field: &str) -> Result<()> {
        self.inner.delete_field(field).map_err(ArrowConvError::from)
    }

    pub fn rename_field(&self, field: &str, new_name: &str) -> Result<()> {
        self.inner.rename_field(field, new_name).map_err(ArrowConvError::from)
    }

    pub fn cast_field(&self, field: &str, target_type: DataType) -> Result<()> {
        self.inner.cast_field(field, target_type).map_err(ArrowConvError::from)
    }

    pub fn compress_field(&self, field: &str) -> Result<()> {
        self.inner.compress_field(field).map_err(ArrowConvError::from)
    }

    pub fn decompress_field(&self, field: &str) -> Result<()> {
        self.inner.decompress_field(field).map_err(ArrowConvError::from)
    }

    /// 检查并修复表物理存储与元数据完整性（清理空文件夹，补齐最新分区缺失字段）
    pub fn fix(&self) -> Result<()> {
        self.inner.fix().map_err(ArrowConvError::from)
    }

    /// 转换为只读 TableArrowReader
    pub fn as_reader(&self) -> Result<TableArrowReader> {
        TableArrowReader::open(self.inner.path())
    }

    pub fn schema(&self) -> Result<Arc<ArrowSchema>> {
        let splayed_schema = self.inner.schema()?;
        let fields = arrow_fields(&splayed_schema);
        Ok(Arc::new(ArrowSchema::new(fields)))
    }

    pub fn metadata(&self) -> Result<splayed_table::TableMetadata> {
        self.inner.metadata().map_err(ArrowConvError::from)
    }

    pub fn statistics(&self) -> Result<splayed_table::TableStatistics> {
        self.inner.statistics().map_err(ArrowConvError::from)
    }

    pub fn path(&self) -> &std::path::Path {
        self.inner.path()
    }

    pub fn scheme(&self) -> splayed_table::PartitionScheme {
        self.inner.scheme()
    }

    pub fn close(self) -> Result<()> {
        self.inner.close().map_err(ArrowConvError::from)
    }

    pub fn remove(self) -> Result<()> {
        self.inner.remove().map_err(ArrowConvError::from)
    }
}

/// Table 端到端流式读取兼容函数
pub fn read_table_as_arrow<'t>(
    _table: &'t TableHandle,
    scanner: TableScanner<'t>,
    batch_size: Option<usize>,
) -> TableArrowBatchReader<'t> {
    TableArrowBatchReader {
        inner: scanner.into_reader(batch_size),
    }
}

/// 直接自 TableReader 构造流式 Arrow 读取器
pub fn read_reader_as_arrow<'t>(
    reader: &'t splayed_table::TableReader,
    request: splayed_table::TableScanRequest,
    batch_size: Option<usize>,
) -> Result<TableArrowBatchReader<'t>> {
    let scanner = reader.scan(request)?;
    Ok(TableArrowBatchReader {
        inner: scanner.into_reader(batch_size),
    })
}

/// 将来自外部系统的 RecordBatch 数据直接通过 TableWriter 写入表
pub fn write_record_batch(
    writer: &splayed_table::TableWriter,
    batch: &RecordBatch,
) -> Result<()> {
    let data = record_batch_to_data(batch)?;
    writer.write(&data.as_view())?;
    Ok(())
}

/// 将来自外部系统的 RecordBatch 数据直接全量更新到表
pub fn update_record_batch(
    writer: &splayed_table::TableWriter,
    batch: &RecordBatch,
) -> Result<()> {
    let data = record_batch_to_data(batch)?;
    writer.update(&data.as_view())?;
    Ok(())
}

// ---------------------------------------------------------------- from arrow

/// RecordBatch → 拥有型 Data（create / write 路径的反向映射）。
pub fn record_batch_to_data(batch: &RecordBatch) -> Result<Data> {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (arrow_field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let dt = from_arrow_type(arrow_field.data_type())?;
        let (values, validity, dict) = array_to_column(array, dt)?;
        fields.push(FieldSchema::new(arrow_field.name().as_str(), dt));
        columns.push(Column { data_type: dt, values, validity, dict });
    }
    Ok(Data { schema: Schema::new(fields), columns })
}


/// Dictionary(Int32, Utf8) 或纯 StringArray/LargeStringArray → V2 Utf8 字典列。
fn utf8_dict_to_column(
    array: &ArrayRef,
    validity: Option<Bitmap>,
) -> Result<(Buffer, Option<Bitmap>, Option<DictBuffers>)> {
    if let Some(dict) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ArrowConvError::Unsupported("dict values not utf8".into()))?;
        let mut offsets: Vec<u64> = vec![0u64];
        let mut strings: Vec<u8> = Vec::new();
        for i in 0..values.len() {
            strings.extend_from_slice(values.value(i).as_bytes());
            offsets.push(strings.len() as u64);
        }
        let keys: Vec<u32> = dict
            .keys()
            .iter()
            .map(|k| k.unwrap_or(0) as u32)
            .collect();
        return Ok((
            Buffer::from_vec(bytemuck::cast_slice::<u32, u8>(&keys).to_vec()),
            validity,
            Some(DictBuffers {
                offsets: Buffer::from_vec(bytemuck::cast_slice::<u64, u8>(&offsets).to_vec()),
                strings: Buffer::from_vec(strings),
            }),
        ));
    }

    // 处理普通的 StringArray / LargeStringArray：现场构建字典
    let rows = array.len();
    let mut map: ahash::AHashMap<String, u32> = ahash::AHashMap::new();
    let mut offsets: Vec<u64> = vec![0u64];
    let mut strings: Vec<u8> = Vec::new();
    let mut keys: Vec<u32> = Vec::with_capacity(rows);

    let mut last_str = String::new();
    let mut has_last = false;
    let mut last_id: u32 = 0;

    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        for i in 0..rows {
            if arr.is_null(i) {
                keys.push(0);
                has_last = false;
            } else {
                let s = arr.value(i);
                if has_last && last_str == s {
                    keys.push(last_id);
                    continue;
                }
                let id = match map.get(s) {
                    Some(&id) => id,
                    None => {
                        let id = map.len() as u32;
                        strings.extend_from_slice(s.as_bytes());
                        offsets.push(strings.len() as u64);
                        map.insert(s.to_string(), id);
                        id
                    }
                };
                last_str.clear();
                last_str.push_str(s);
                has_last = true;
                last_id = id;
                keys.push(id);
            }
        }
    } else if let Some(arr) = array.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
        for i in 0..rows {
            if arr.is_null(i) {
                keys.push(0);
                has_last = false;
            } else {
                let s = arr.value(i);
                if has_last && last_str == s {
                    keys.push(last_id);
                    continue;
                }
                let id = match map.get(s) {
                    Some(&id) => id,
                    None => {
                        let id = map.len() as u32;
                        strings.extend_from_slice(s.as_bytes());
                        offsets.push(strings.len() as u64);
                        map.insert(s.to_string(), id);
                        id
                    }
                };
                last_str.clear();
                last_str.push_str(s);
                has_last = true;
                last_id = id;
                keys.push(id);
            }
        }
    } else {
        return Err(ArrowConvError::Unsupported(format!(
            "expected DictionaryArray or StringArray for Utf8, found {:?}",
            array.data_type()
        )));
    }

    Ok((
        Buffer::from_vec(bytemuck::cast_slice::<u32, u8>(&keys).to_vec()),
        validity,
        Some(DictBuffers {
            offsets: Buffer::from_vec(bytemuck::cast_slice::<u64, u8>(&offsets).to_vec()),
            strings: Buffer::from_vec(strings),
        }),
    ))
}

fn array_to_column(
    array: &ArrayRef,
    dt: DataType,
) -> Result<(Buffer, Option<Bitmap>, Option<DictBuffers>)> {
    let validity = array.nulls().map(null_buffer_to_bitmap);
    let rows = array.len();

    if dt == DataType::Bool {
        let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        let mut bytes = vec![0u8; rows];
        for i in 0..rows {
            bytes[i] = u8::from(arr.value(i) && !arr.is_null(i));
        }
        return Ok((Buffer::from_vec(bytes), validity, None));
    }
    if dt == DataType::Utf8 {
        return utf8_dict_to_column(array, validity);
    }

    Ok(match dt {
        DataType::Bool | DataType::Utf8 => unreachable!("handled above"),
        DataType::Int8 => (
            Buffer::from_slice_copy(array.as_any().downcast_ref::<Int8Array>().unwrap().values()),
            validity,
            None,
        ),
        DataType::Int16 => (
            Buffer::from_slice_copy(array.as_any().downcast_ref::<Int16Array>().unwrap().values()),
            validity,
            None,
        ),
        DataType::Int32 => (
            Buffer::from_slice_copy(array.as_any().downcast_ref::<Int32Array>().unwrap().values()),
            validity,
            None,
        ),
        DataType::Date32 => (
            Buffer::from_slice_copy(array.as_any().downcast_ref::<Date32Array>().unwrap().values()),
            validity,
            None,
        ),
        DataType::Int64 => (
            Buffer::from_slice_copy(array.as_any().downcast_ref::<Int64Array>().unwrap().values()),
            validity,
            None,
        ),
        DataType::TimestampUs => (
            Buffer::from_slice_copy(
                array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .values(),
            ),
            validity,
            None,
        ),
        DataType::Date64 => (
            Buffer::from_slice_copy(array.as_any().downcast_ref::<Date64Array>().unwrap().values()),
            validity,
            None,
        ),
        DataType::UInt8 => (
            Buffer::from_slice_copy(array.as_any().downcast_ref::<UInt8Array>().unwrap().values()),
            validity,
            None,
        ),
        DataType::UInt16 => (
            Buffer::from_slice_copy(array.as_any().downcast_ref::<UInt16Array>().unwrap().values()),
            validity,
            None,
        ),
        DataType::UInt32 => (
            Buffer::from_slice_copy(array.as_any().downcast_ref::<UInt32Array>().unwrap().values()),
            validity,
            None,
        ),
        DataType::UInt64 => (
            Buffer::from_slice_copy(array.as_any().downcast_ref::<UInt64Array>().unwrap().values()),
            validity,
            None,
        ),
        DataType::Float32 => (
            Buffer::from_slice_copy(
                array.as_any().downcast_ref::<Float32Array>().unwrap().values(),
            ),
            validity,
            None,
        ),
        DataType::Float64 => (
            Buffer::from_slice_copy(
                array.as_any().downcast_ref::<Float64Array>().unwrap().values(),
            ),
            validity,
            None,
        ),
    })
}
