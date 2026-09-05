//! splayed-polars：V2.0 Polars 适配层（AnonymousScan 惰性扫描）。
//!
//! 权威设计见 `docs/splayed-adapters.md` §1。v1 为拷贝级列转换（列 → polars
//! Series）；谓词 v1 不下推（polars 行级过滤兜底，`allows_predicate_pushdown = false`）；
//! sym/time 下推与零拷贝转换为 v2.1 优化项。

use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use polars::prelude::*;
use polars::prelude::DataType as PolarsDt;

use splayed_core::{open_dataset, CoreError, Mode};
use splayed_format::DataType;

/// 扫描对象包装：持有表根目录，惰性打开分区。
pub struct SplayedTable {
    root: PathBuf,
}

impl SplayedTable {
    pub fn new(path: impl AsRef<Path>) -> Self {
        SplayedTable { root: path.as_ref().to_path_buf() }
    }

    /// none 模式 → 根目录；分区模式 → 分区目录列表（名称 ASC）。
    fn dataset_dirs(&self) -> Result<Vec<PathBuf>, CoreError> {
        if self.root.join(".meta").exists() {
            return Ok(vec![self.root.clone()]);
        }
        let mut parts: Vec<PathBuf> = std::fs::read_dir(&self.root)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                p.is_dir()
                    && !p
                        .file_name()
                        .map_or(true, |n| n.to_string_lossy().starts_with('.'))
            })
            .collect();
        parts.sort();
        if parts.is_empty() {
            parts.push(self.root.clone());
        }
        Ok(parts)
    }

    /// Table schema → polars Schema（与 `read_table_schema` 一致：最后 Partition）。
    fn polars_schema(&self) -> Result<SchemaRef, CoreError> {
        let dirs = self.dataset_dirs()?;
        let last = dirs.last().expect("non-empty by construction");
        let ds = open_dataset(last, Mode::Read)?;
        let core_schema = ds.read_dataset_schema();
        ds.close_dataset()?;
        let mut fields = Vec::with_capacity(core_schema.fields.len());
        for f in &core_schema.fields {
            fields.push(Field::new(f.name.as_ref().into(), to_polars_dtype(f.data_type)));
        }
        Ok(Arc::new(Schema::from_iter(fields)))
    }

    /// 全分区（名称 ASC）数据 → DataFrame（拷贝级转换；n_rows 生效时提前截断）。
    fn scan_to_frame(
        &self,
        with_columns: Option<&[PlSmallStr]>,
        n_rows: Option<usize>,
    ) -> Result<DataFrame, CoreError> {
        let dirs = self.dataset_dirs()?;
        let mut frame: Option<DataFrame> = None;
        let mut remaining = n_rows;
        for p in &dirs {
            if remaining.is_some_and(|r| r == 0) {
                break;
            }
            let ds = open_dataset(p, Mode::Read)?;
            let l = ds.read_dataset_statistics()?.row_count as usize;
            let take = remaining.map(|r| r.min(l)).unwrap_or(l);
            // 列集：恒在的 sym/time + with_columns 指定的字段
            let schema = ds.read_dataset_schema();
            let mut names: Vec<String> = vec!["sym".into(), "time".into()];
            for f in schema.fields.iter().skip(2) {
                let n = f.name.to_string();
                if with_columns.map_or(true, |cols| {
                    cols.iter().any(|c| c.as_str() == n.as_str())
                }) {
                    names.push(n);
                }
            }
            let cols_ref: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let full = ds.read_dataset(0, take as u64, Some(&cols_ref))?;
            let mut df = view_to_frame(&full)?;
            // 精确返回请求列（with_columns 顺序；sym/time 仅在被请求时出现）
            if let Some(cols) = with_columns {
                let wanted: Vec<&str> = cols.iter().map(|c| c.as_str()).collect();
                df = df.select(wanted).map_err(|e| CoreError::Invalid(format!("select: {e}")))?;
            }
            if let Some(r) = remaining {
                if df.height() > r {
                    df = df.slice(0, r);
                }
                remaining = Some(r - df.height());
            }
            frame = Some(match frame {
                None => df,
                Some(mut acc) => {
                    acc.vstack_mut(&df)
                        .map_err(|e| CoreError::Invalid(format!("vstack: {e}")))?;
                    acc
                }
            });
            ds.close_dataset()?;
        }
        let out = frame.unwrap_or_default();
        Ok(out)
    }
}

/// V2.0 DataType → polars DataType。
fn to_polars_dtype(dt: DataType) -> PolarsDt {
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

/// 段 validity → 逐行有效标志（跨段拼接；无位图段 = 全有效）。
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

    // 快路径：单段 + 无 validity → 零逐行开销，直接 cast_slice
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
            DataType::Utf8 => {
                // Utf8 无快路径：回退通用路径
                let vals: Vec<Option<String>> = (0..rows)
                    .map(|i| view.string_at(i).map(str::to_owned))
                    .collect();
                StringChunked::from_slice_options(name.into(), &vals).into_series()
            }
        });
    }

    // 通用路径：有 validity 或多段
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
    let read_fixed = |i: usize| -> Vec<u8> {
        let (s, r, idx) = seg_cursor
            .iter()
            .find(|(s, r, _)| i >= *s && i < s + r)
            .map(|(s, r, idx)| (*s, *r, *idx))
            .expect("row within view");
        let seg = &view.segments()[idx];
        let bytes = seg.fixed_bytes().expect("fixed-width segment");
        let width = bytes.len() / r.max(1);
        bytes[i * width - s * width..i * width - s * width + width].to_vec()
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
            let vals: Vec<Option<u8>> =
                (0..rows).map(|i| validity[i].then(|| read_fixed(i)[0])).collect();
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
/// 多列视图 → DataFrame。
fn view_to_frame(view: &splayed_format::DataView<'_>) -> Result<DataFrame, CoreError> {
    let mut series = Vec::with_capacity(view.schema.fields.len());
    for field in &view.schema.fields {
        let col_view = view.column(&field.name).expect("schema iteration guarantees");
        series.push(column_to_series(&field.name, col_view)?.into());
    }
    DataFrame::new(series).map_err(|e| CoreError::Invalid(format!("df new: {e}")))
}

impl AnonymousScan for SplayedTable {
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
        // 谓词 v1 不下推（polars 行级过滤兜底）
        false
    }

    fn allows_projection_pushdown(&self) -> bool {
        // polars 0.45 匿名扫描的投影下推优化器存在 unwrap panic（已报告行为）；
        // 关闭下推由 polars 在返回的 DataFrame 上自行 select，语义不变
        false
    }
}

/// 入口：返回可继续 `.filter()/.select()` 的 LazyFrame。
pub fn scan_polars(path: impl AsRef<Path>) -> PolarsResult<LazyFrame> {
    let table = SplayedTable::new(path);
    let schema = table
        .polars_schema()
        .map_err(|e| PolarsError::ComputeError(format!("{e}").into()))?;
    let args = ScanArgsAnonymous {
        schema: Some(schema),
        ..Default::default()
    };
    LazyFrame::anonymous_scan(std::sync::Arc::new(table), args)
}
