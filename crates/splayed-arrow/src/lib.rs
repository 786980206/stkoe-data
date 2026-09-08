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

use splayed_table::{TableHandle, TableReader, TableScanner};
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
        ArrowDt::Dictionary(_, v) if **v == ArrowDt::Utf8 => DataType::Utf8,
        other => {
            return Err(ArrowConvError::Unsupported(format!(
                "arrow type {other:?} has no V2.0 mapping"
            )))
        }
    })
}

// ---------------------------------------------------------------- validity

/// V2.0 Bitmap → Arrow NullBuffer（位序一致，按字节拷贝）。
fn bitmap_to_null_buffer(bm: &Bitmap) -> NullBuffer {
    let view = bm.as_view();
    let bits: Vec<bool> = (0..view.len()).map(|i| view.is_valid(i)).collect();
    NullBuffer::from(bits)
}
// 说明：位图 → bool vec 为一次 O(n) 拷贝；零拷贝路径依赖 Buffer 的 Arc 变体（§4）

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
            let mut keys: Vec<Option<i32>> = Vec::new();
            let mut dict_values: Vec<Option<String>> = Vec::new();
            let mut offset = 0i32;
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
                            dict_values.push(Some(vals.value(idx as usize).to_owned()));
                            keys.push(Some(idx as i32 + offset));
                        }
                        None => keys.push(None),
                    }
                }
                offset += vals.len() as i32;
                let _ = v;
            }
            let keys_arr = Int32Array::from(keys);
            let values_arr = StringArray::from(dict_values);
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
    let len = view.len();
    let mut out = vec![0u8; (len + 7) / 8];
    for i in 0..len {
        if view.is_valid(i) {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
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

/// Table → Arrow 流式适配器（Layer 3）：包装 `TableReader`，逐批输出
/// `RecordBatch`。**不拥有查询语义**——裁剪 / 谓词 / limit 由 `TableScanner`
/// 完成，本层只做 DataView → RecordBatch 转换；流式输出不物化整个结果集。
pub struct TableArrowReader<'t> {
    inner: TableReader<'t>,
}

/// Table 端到端流式读取（Layer 3）：`scan_table` 产出的 `TableScanner` +
/// `batch_size` → 构造 Arrow Reader。直接接收 Scanner（而非 ScanRequest）——
/// 查询语义归 Table 层，Arrow 层不重新 scan。构造无 I/O（错误延迟到 `next()`）。
///
/// ```text
/// TableScanRequest → scan_table → TableScanner
///     → read_table_as_arrow → TableArrowReader
///     → next() → RecordBatch（逐批，流式）
/// ```
pub fn read_table_as_arrow<'t>(
    table: &'t TableHandle,
    scanner: TableScanner<'t>,
    batch_size: Option<usize>,
) -> TableArrowReader<'t> {
    TableArrowReader {
        inner: splayed_table::read_table(table, scanner, batch_size),
    }
}

impl<'t> TableArrowReader<'t> {
    /// 下一批 `RecordBatch`；结束返回 `None`。`batch_size = Some(n)` 时每批
    /// 恰好 n 行（最后一批允许小）；`None` 时一 range 一批。
    pub fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.inner.next()? {
            None => Ok(None),
            Some(view) => data_view_to_record_batch(&view).map(Some),
        }
    }

    /// 关闭：任何时刻（正常结束 / LIMIT 提前结束 / 错误 / 取消）都可安全调用。
    pub fn close(self) -> Result<()> {
        self.inner.close().map_err(ArrowConvError::from)
    }
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


/// Dictionary(Int32, Utf8) → V2 Utf8 字典列。
fn utf8_dict_to_column(
    array: &ArrayRef,
    validity: Option<Bitmap>,
) -> Result<(Buffer, Option<Bitmap>, Option<DictBuffers>)> {
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| ArrowConvError::Unsupported("Utf8 column requires DictionaryArray".into()))?;
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
    // V2 字典 keys 非空：行级 NULL 由 validity 位图表达；Arrow 端 null key 槽位
    // 以 0 填充（payload 被 validity 掩蔽），不 panic
    let keys: Vec<u32> = dict
        .keys()
        .iter()
        .map(|k| k.unwrap_or(0) as u32)
        .collect();
    Ok((
        Buffer::from_vec(keys.iter().flat_map(|k| k.to_le_bytes()).collect()),
        validity,
        Some(DictBuffers {
            offsets: Buffer::from_vec(offsets.iter().flat_map(|o| o.to_le_bytes()).collect()),
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
    macro_rules! bytes_of {
        ($arr:expr) => {{
            let slice = $arr.values();
            bytemuck::cast_slice::<_, u8>(slice).to_vec()
        }};
    }
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
    let fixed = move |bytes: Vec<u8>| -> (Buffer, Option<Bitmap>, Option<DictBuffers>) {
        (Buffer::zeroed_aligned_bytes(bytes, 8), validity, None)
    };
    Ok(match dt {
        DataType::Bool | DataType::Utf8 => unreachable!("handled above"),
        DataType::Int8 => fixed(bytes_of!(array.as_any().downcast_ref::<Int8Array>().unwrap())),
        DataType::Int16 => fixed(bytes_of!(array.as_any().downcast_ref::<Int16Array>().unwrap())),
        DataType::Int32 => fixed(bytes_of!(array.as_any().downcast_ref::<Int32Array>().unwrap())),
        DataType::Date32 => fixed(bytes_of!(array.as_any().downcast_ref::<Date32Array>().unwrap())),
        DataType::Int64 => fixed(bytes_of!(array.as_any().downcast_ref::<Int64Array>().unwrap())),
        DataType::TimestampUs => {
            fixed(bytes_of!(array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()))
        }
        DataType::Date64 => fixed(bytes_of!(array.as_any().downcast_ref::<Date64Array>().unwrap())),
        DataType::UInt8 => fixed(bytes_of!(array.as_any().downcast_ref::<UInt8Array>().unwrap())),
        DataType::UInt16 => fixed(bytes_of!(array.as_any().downcast_ref::<UInt16Array>().unwrap())),
        DataType::UInt32 => fixed(bytes_of!(array.as_any().downcast_ref::<UInt32Array>().unwrap())),
        DataType::UInt64 => fixed(bytes_of!(array.as_any().downcast_ref::<UInt64Array>().unwrap())),
        DataType::Float32 => {
            fixed(bytes_of!(array.as_any().downcast_ref::<Float32Array>().unwrap()))
        }
        DataType::Float64 => {
            fixed(bytes_of!(array.as_any().downcast_ref::<Float64Array>().unwrap()))
        }
    })
}
