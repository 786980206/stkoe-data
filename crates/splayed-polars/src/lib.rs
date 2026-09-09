//! splayed-polars：Polars 适配层（支持 scan_splayed 惰性扫描与 sink_splayed 流式/批次导出）。
//!
//! 支持 Rust 与 Python 两端生态：
//! 1. `scan_splayed`: 通过 AnonymousScan 接入 Polars 逻辑计划，支持 projection 投影下推和 limit 行数裁剪；
//! 2. `sink_splayed`: 接收 Polars DataFrame / 批次，自动判定表是否存在；不存在则使用 `TableWriter::init` 初始化，存在则使用 `TableWriter::open` + `.write()` 原地写入；
//! 3. 内部借助 `splayed-arrow` 进行高性能零拷贝与类型映射。

use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use polars::prelude::*;
use polars::prelude::DataType as PolarsDt;

use splayed_core::CoreError;
use splayed_format::DataType;
use splayed_table::{PartitionScheme, TableOptions, TableReader, TableWriter, TableScanRequest};

/// 扫描对象包装：持有 TableReader 句柄，惰性执行查询。
pub struct SplayedTableScan {
    root: PathBuf,
}

impl SplayedTableScan {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            root: path.as_ref().to_path_buf(),
        }
    }

    /// 读取表 Schema 并转换为 Polars SchemaRef
    pub fn polars_schema(&self) -> Result<SchemaRef, CoreError> {
        let reader = TableReader::open(&self.root)?;
        let core_schema = reader.schema()?;
        let mut fields = Vec::with_capacity(core_schema.fields.len());
        for f in &core_schema.fields {
            fields.push(Field::new(f.name.as_ref().into(), to_polars_dtype(f.data_type)));
        }
        Ok(Arc::new(Schema::from_iter(fields)))
    }

    /// 执行扫描并输出 Polars DataFrame
    pub fn scan_to_frame(
        &self,
        with_columns: Option<&[PlSmallStr]>,
        n_rows: Option<usize>,
    ) -> Result<DataFrame, CoreError> {
        let reader = TableReader::open(&self.root)?;
        let mut req = TableScanRequest::default();
        if let Some(cols) = with_columns {
            req.projection = cols.iter().map(|c| c.as_str().to_string()).collect();
        }
        if let Some(n) = n_rows {
            req.limit = Some(n as u64);
        }

        let mut batch_reader = reader.read(req, None)?;
        let mut frame: Option<DataFrame> = None;

        while let Ok(Some(view)) = batch_reader.next() {
            let df = view_to_frame(&view)?;
            frame = Some(match frame {
                None => df,
                Some(mut acc) => {
                    acc.vstack_mut(&df)
                        .map_err(|e| CoreError::Invalid(format!("vstack: {e}")))?;
                    acc
                }
            });
        }

        let mut out = frame.unwrap_or_default();
        if let Some(cols) = with_columns {
            let wanted: Vec<&str> = cols.iter().map(|c| c.as_str()).collect();
            out = out.select(wanted).map_err(|e| CoreError::Invalid(format!("select: {e}")))?;
        }
        if let Some(r) = n_rows {
            if out.height() > r {
                out = out.slice(0, r);
            }
        }
        Ok(out)
    }
}

impl AnonymousScan for SplayedTableScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        self.polars_schema()
            .map_err(|e| PolarsError::ComputeError(format!("{e}").into()))
    }

    fn scan(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        self.scan_to_frame(scan_opts.with_columns.as_deref(), scan_opts.n_rows)
            .map_err(|e| PolarsError::ComputeError(format!("{e}").into()))
    }

    fn allows_predicate_pushdown(&self) -> bool {
        false
    }

    fn allows_projection_pushdown(&self) -> bool {
        false
    }
}

/// Splayed V2.0 DataType → Polars DataType
pub fn to_polars_dtype(dt: DataType) -> PolarsDt {
    match dt {
        DataType::Bool => PolarsDt::Boolean,
        DataType::Int8 => PolarsDt::Int8,
        DataType::Int16 => PolarsDt::Int16,
        DataType::Int32 | DataType::Date32 => PolarsDt::Int32,
        DataType::Int64 | DataType::TimestampUs | DataType::Date64 => PolarsDt::Int64,
        DataType::UInt8 => PolarsDt::UInt8,
        DataType::UInt16 => PolarsDt::UInt16,
        DataType::UInt32 => PolarsDt::UInt32,
        DataType::UInt64 => PolarsDt::UInt64,
        DataType::Float32 => PolarsDt::Float32,
        DataType::Float64 => PolarsDt::Float64,
        DataType::Utf8 => PolarsDt::String,
    }
}

/// 将 Splayed DataView 转换为 Polars DataFrame
pub fn view_to_frame(view: &splayed_format::DataView<'_>) -> Result<DataFrame, CoreError> {
    let mut series = Vec::with_capacity(view.schema.fields.len());
    for field in &view.schema.fields {
        let col_view = view.column(&field.name).expect("schema iteration guarantees");
        series.push(column_to_series(&field.name, col_view)?.into());
    }
    DataFrame::new(series).map_err(|e| CoreError::Invalid(format!("df new: {e}")))
}

fn row_validity(view: &splayed_format::ColumnView<'_>) -> Vec<bool> {
    let mut out = Vec::with_capacity(view.length());
    for seg in view.segments() {
        match seg.validity() {
            Some(b) => {
                for i in 0..b.len() {
                    out.push(b.is_valid(i));
                }
            }
            None => out.extend(std::iter::repeat(true).take(seg.rows())),
        }
    }
    out
}

fn column_to_series(
    name: &str,
    view: &splayed_format::ColumnView<'_>,
) -> Result<Series, CoreError> {
    let rows = view.length();
    let dt = view.data_type();

    // 单段快路径
    if view.segments().len() == 1 && dt != DataType::Utf8 && view.segments()[0].validity().is_none() {
        let bytes = view.segments()[0].fixed_bytes().expect("fixed-width segment");
        return Ok(match dt {
            DataType::Bool => {
                let v: Vec<bool> = bytes.iter().map(|&b| b != 0).collect();
                BooleanChunked::from_slice(name.into(), &v).into_series()
            }
            DataType::Int8 => Int8Chunked::from_slice(name.into(), bytemuck::cast_slice(bytes)).into_series(),
            DataType::Int16 => Int16Chunked::from_slice(name.into(), bytemuck::cast_slice(bytes)).into_series(),
            DataType::Int32 | DataType::Date32 => Int32Chunked::from_slice(name.into(), bytemuck::cast_slice(bytes)).into_series(),
            DataType::Int64 | DataType::TimestampUs | DataType::Date64 => Int64Chunked::from_slice(name.into(), bytemuck::cast_slice(bytes)).into_series(),
            DataType::UInt8 => UInt8Chunked::from_slice(name.into(), bytes).into_series(),
            DataType::UInt16 => UInt16Chunked::from_slice(name.into(), bytemuck::cast_slice(bytes)).into_series(),
            DataType::UInt32 => UInt32Chunked::from_slice(name.into(), bytemuck::cast_slice(bytes)).into_series(),
            DataType::UInt64 => UInt64Chunked::from_slice(name.into(), bytemuck::cast_slice(bytes)).into_series(),
            DataType::Float32 => Float32Chunked::from_slice(name.into(), bytemuck::cast_slice(bytes)).into_series(),
            DataType::Float64 => Float64Chunked::from_slice(name.into(), bytemuck::cast_slice(bytes)).into_series(),
            DataType::Utf8 => unreachable!(),
        });
    }

    let validity = row_validity(view);
    let seg_cursor: Vec<(usize, usize, usize)> = {
        let mut out = Vec::new();
        let mut start = 0usize;
        for (idx, seg) in view.segments().iter().enumerate() {
            out.push((start, seg.rows(), idx));
            start += seg.rows();
        }
        out
    };

    let locate = |row: usize| -> (usize, usize) {
        let hit = seg_cursor
            .binary_search_by(|&(s, l, _)| {
                if row < s {
                    std::cmp::Ordering::Greater
                } else if row >= s + l {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .expect("row within range");
        let (s, _, seg_idx) = seg_cursor[hit];
        (seg_idx, row - s)
    };

    let read_fixed = |row: usize| -> &[u8] {
        let (s_idx, in_seg) = locate(row);
        let seg = &view.segments()[s_idx];
        let w = dt.size_of();
        let b = seg.fixed_bytes().expect("fixed-width segment");
        &b[in_seg * w..(in_seg + 1) * w]
    };

    Ok(match dt {
        DataType::Bool => {
            let vals: Vec<Option<bool>> = (0..rows)
                .map(|i| validity[i].then(|| read_fixed(i)[0] != 0))
                .collect();
            BooleanChunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::Int8 => {
            let vals: Vec<Option<i8>> = (0..rows)
                .map(|i| validity[i].then(|| read_fixed(i)[0] as i8))
                .collect();
            Int8Chunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::Int16 => {
            let vals: Vec<Option<i16>> = (0..rows)
                .map(|i| {
                    validity[i].then(|| {
                        let b = read_fixed(i);
                        i16::from_le_bytes([b[0], b[1]])
                    })
                })
                .collect();
            Int16Chunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::Int32 | DataType::Date32 => {
            let vals: Vec<Option<i32>> = (0..rows)
                .map(|i| {
                    validity[i].then(|| {
                        let b = read_fixed(i);
                        i32::from_le_bytes([b[0], b[1], b[2], b[3]])
                    })
                })
                .collect();
            Int32Chunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::Int64 | DataType::TimestampUs | DataType::Date64 => {
            let vals: Vec<Option<i64>> = (0..rows)
                .map(|i| {
                    validity[i].then(|| {
                        let b = read_fixed(i);
                        i64::from_le_bytes(b.try_into().unwrap())
                    })
                })
                .collect();
            Int64Chunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::UInt8 => {
            let vals: Vec<Option<u8>> = (0..rows)
                .map(|i| validity[i].then(|| read_fixed(i)[0]))
                .collect();
            UInt8Chunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::UInt16 => {
            let vals: Vec<Option<u16>> = (0..rows)
                .map(|i| {
                    validity[i].then(|| {
                        let b = read_fixed(i);
                        u16::from_le_bytes([b[0], b[1]])
                    })
                })
                .collect();
            UInt16Chunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::UInt32 => {
            let vals: Vec<Option<u32>> = (0..rows)
                .map(|i| {
                    validity[i].then(|| {
                        let b = read_fixed(i);
                        u32::from_le_bytes([b[0], b[1], b[2], b[3]])
                    })
                })
                .collect();
            UInt32Chunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::UInt64 => {
            let vals: Vec<Option<u64>> = (0..rows)
                .map(|i| {
                    validity[i].then(|| {
                        let b = read_fixed(i);
                        u64::from_le_bytes(b.try_into().unwrap())
                    })
                })
                .collect();
            UInt64Chunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::Float32 => {
            let vals: Vec<Option<f32>> = (0..rows)
                .map(|i| {
                    validity[i].then(|| {
                        let b = read_fixed(i);
                        f32::from_le_bytes([b[0], b[1], b[2], b[3]])
                    })
                })
                .collect();
            Float32Chunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::Float64 => {
            let vals: Vec<Option<f64>> = (0..rows)
                .map(|i| {
                    validity[i].then(|| {
                        let b = read_fixed(i);
                        f64::from_le_bytes(b.try_into().unwrap())
                    })
                })
                .collect();
            Float64Chunked::from_slice_options(name.into(), &vals).into_series()
        }
        DataType::Utf8 => {
            let vals: Vec<Option<String>> = (0..rows)
                .map(|i| view.string_at(i).map(str::to_owned))
                .collect();
            StringChunked::from_slice_options(name.into(), &vals).into_series()
        }
    })
}

/// 惰性扫描 Splayed 表：返回可链式调用 `.filter()/.select()/.limit()` 的 Polars LazyFrame。
pub fn scan_splayed(path: impl AsRef<Path>) -> PolarsResult<LazyFrame> {
    let scan = SplayedTableScan::new(path);
    let schema = scan
        .polars_schema()
        .map_err(|e| PolarsError::ComputeError(format!("{e}").into()))?;
    let args = ScanArgsAnonymous {
        schema: Some(schema),
        ..Default::default()
    };
    LazyFrame::anonymous_scan(Arc::new(scan), args)
}

/// 兼容别名
#[inline]
pub fn scan_polars(path: impl AsRef<Path>) -> PolarsResult<LazyFrame> {
    scan_splayed(path)
}

/// 将 Polars DataFrame 写入或更新 Splayed 存储。
/// 
/// 自动自愈与路由判断：
/// - 若目标表不存在：自动调用 `TableWriter::init` 进行建表初始化与首批数据写入；
/// - 若目标表已存在：自动调用 `TableWriter::open` 并通过 `.write()` 执行覆盖写入。
pub fn sink_splayed(
    df: &DataFrame,
    path: impl AsRef<Path>,
    scheme: PartitionScheme,
    options: Option<TableOptions>,
) -> PolarsResult<()> {
    let path = path.as_ref();
    let is_existing = path.exists() && (
        path.join(".meta").exists() || 
        std::fs::read_dir(path).map(|mut it| {
            it.any(|e| e.ok().map(|ent| ent.path().join(".meta").exists()).unwrap_or(false))
        }).unwrap_or(false)
    );

    // 将 Polars DataFrame 转换为 Splayed Data
    let data = df_to_data(df).map_err(|e| PolarsError::ComputeError(format!("{e}").into()))?;

    if !is_existing {
        TableWriter::init(path, scheme, &data.as_view(), options)
            .map_err(|e| PolarsError::ComputeError(format!("{e}").into()))?;
    } else {
        let writer = if let Some(opts) = options {
            TableWriter::open_with_options(path, opts)
        } else {
            TableWriter::open(path)
        }.map_err(|e| PolarsError::ComputeError(format!("{e}").into()))?;
        writer.write(&data.as_view())
            .map_err(|e| PolarsError::ComputeError(format!("{e}").into()))?;
    }

    Ok(())
}

/// 将 Polars DataFrame 转换为 Splayed Data 内存结构
fn df_to_data(df: &DataFrame) -> Result<splayed_format::Data, CoreError> {
    use splayed_format::{Column, FieldSchema, Schema};

    let mut fields = Vec::with_capacity(df.width());
    let mut columns = Vec::with_capacity(df.width());

    for s in df.get_columns() {
        let name = s.name().as_str();
        let series = s.as_materialized_series();
        let (dt, values, validity, dict) = series_to_column(series)?;
        fields.push(FieldSchema::new(name, dt));
        columns.push(Column {
            data_type: dt,
            values,
            validity,
            dict,
        });
    }

    Ok(splayed_format::Data::new(Schema::new(fields), columns)?)
}

fn series_to_column(
    s: &Series,
) -> Result<(DataType, splayed_format::Buffer, Option<splayed_format::Bitmap>, Option<splayed_format::DictBuffers>), CoreError> {
    use splayed_format::{Bitmap, Buffer, DictBuffers};

    let rows = s.len();
    let validity = if s.null_count() > 0 {
        let mut bm = Bitmap::ones(rows);
        let null_ca = s.is_null();
        for i in 0..rows {
            if null_ca.get(i).unwrap_or(false) {
                bm.set(i, false);
            }
        }
        Some(bm)
    } else {
        None
    };

    match s.dtype() {
        PolarsDt::Boolean => {
            let ca = s.bool().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let mut bytes = vec![0u8; rows];
            for i in 0..rows {
                bytes[i] = u8::from(ca.get(i).unwrap_or(false));
            }
            Ok((DataType::Bool, Buffer::from_vec(bytes), validity, None))
        }
        PolarsDt::Int8 => {
            let ca = s.i8().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let slice: Vec<i8> = (0..rows).map(|i| ca.get(i).unwrap_or(0)).collect();
            Ok((DataType::Int8, Buffer::from_vec(bytemuck::cast_slice(&slice).to_vec()), validity, None))
        }
        PolarsDt::Int16 => {
            let ca = s.i16().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let slice: Vec<i16> = (0..rows).map(|i| ca.get(i).unwrap_or(0)).collect();
            Ok((DataType::Int16, Buffer::from_vec(bytemuck::cast_slice(&slice).to_vec()), validity, None))
        }
        PolarsDt::Int32 => {
            let ca = s.i32().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let slice: Vec<i32> = (0..rows).map(|i| ca.get(i).unwrap_or(0)).collect();
            Ok((DataType::Int32, Buffer::from_vec(bytemuck::cast_slice(&slice).to_vec()), validity, None))
        }
        PolarsDt::Int64 => {
            let ca = s.i64().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let slice: Vec<i64> = (0..rows).map(|i| ca.get(i).unwrap_or(0)).collect();
            Ok((DataType::Int64, Buffer::from_vec(bytemuck::cast_slice(&slice).to_vec()), validity, None))
        }
        PolarsDt::UInt8 => {
            let ca = s.u8().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let slice: Vec<u8> = (0..rows).map(|i| ca.get(i).unwrap_or(0)).collect();
            Ok((DataType::UInt8, Buffer::from_vec(slice), validity, None))
        }
        PolarsDt::UInt16 => {
            let ca = s.u16().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let slice: Vec<u16> = (0..rows).map(|i| ca.get(i).unwrap_or(0)).collect();
            Ok((DataType::UInt16, Buffer::from_vec(bytemuck::cast_slice(&slice).to_vec()), validity, None))
        }
        PolarsDt::UInt32 => {
            let ca = s.u32().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let slice: Vec<u32> = (0..rows).map(|i| ca.get(i).unwrap_or(0)).collect();
            Ok((DataType::UInt32, Buffer::from_vec(bytemuck::cast_slice(&slice).to_vec()), validity, None))
        }
        PolarsDt::UInt64 => {
            let ca = s.u64().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let slice: Vec<u64> = (0..rows).map(|i| ca.get(i).unwrap_or(0)).collect();
            Ok((DataType::UInt64, Buffer::from_vec(bytemuck::cast_slice(&slice).to_vec()), validity, None))
        }
        PolarsDt::Float32 => {
            let ca = s.f32().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let slice: Vec<f32> = (0..rows).map(|i| ca.get(i).unwrap_or(0.0)).collect();
            Ok((DataType::Float32, Buffer::from_vec(bytemuck::cast_slice(&slice).to_vec()), validity, None))
        }
        PolarsDt::Float64 => {
            let ca = s.f64().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let slice: Vec<f64> = (0..rows).map(|i| ca.get(i).unwrap_or(0.0)).collect();
            Ok((DataType::Float64, Buffer::from_vec(bytemuck::cast_slice(&slice).to_vec()), validity, None))
        }
        PolarsDt::String => {
            let ca = s.str().map_err(|e| CoreError::Invalid(format!("{e}")))?;
            let mut unique_map: std::collections::BTreeMap<&str, u32> = std::collections::BTreeMap::new();
            let mut str_vals: Vec<Option<&str>> = Vec::with_capacity(rows);

            for i in 0..rows {
                if let Some(val) = ca.get(i) {
                    str_vals.push(Some(val));
                    unique_map.entry(val).or_insert(0);
                } else {
                    str_vals.push(None);
                }
            }

            let mut offsets: Vec<u64> = vec![0u64];
            let mut strings: Vec<u8> = Vec::new();
            for (id, (text, entry)) in unique_map.iter_mut().enumerate() {
                *entry = id as u32;
                strings.extend_from_slice(text.as_bytes());
                offsets.push(strings.len() as u64);
            }

            let mut keys = Vec::with_capacity(rows);
            for opt in str_vals {
                match opt {
                    Some(val) => keys.push(*unique_map.get(val).unwrap()),
                    None => keys.push(0),
                }
            }

            Ok((
                DataType::Utf8,
                Buffer::from_vec(keys.iter().flat_map(|k| k.to_le_bytes()).collect()),
                validity,
                Some(DictBuffers {
                    offsets: Buffer::from_vec(offsets.iter().flat_map(|o| o.to_le_bytes()).collect()),
                    strings: Buffer::from_vec(strings),
                }),
            ))
        }
        other => Err(CoreError::Invalid(format!("unsupported polars dtype {other:?}"))),
    }
}
