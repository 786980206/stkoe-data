//! Polars `AnonymousScan` 惰性扫描（谓词下推 + 列裁剪）。
//!
//! - `allows_predicate_pushdown`：接受下推——简单 `列 op 字面量` AND 链经
//!   [`crate::predicate`] 翻译到核心层（SYM 选择 / TIME 范围 / 值过滤）做裁剪；
//!   任何未翻译部分在结果上由 polars 物理评估兜底（结果永远正确）。
//! - `allows_projection_pushdown`：接受投影裁剪——只读 `with_columns` 涉及的列。
//! - 数据经 [`crate::arrowconv`] 的 Arrow C data interface 转换进 polars
//!   （数据结构共享 / 接管，不做逐值拷贝）。

use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use polars::lazy::frame::ScanArgsAnonymous;
use polars::prelude::*;

use splayed_core::{
    CoreBatch, Dataset, ScanRequest, Scanner, SymbolSelection, TimeRange, open_dataset,
    scan_owned_parallel,
};
use splayed_format::TimeType;

use crate::arrowconv::{record_batch_to_dataframe, to_polars_dtype};
use crate::predicate;

/// 一个可注册到 Polars 的惰性扫描源。
pub struct SplayedScan {
    dir: PathBuf,
    dataset: Arc<Dataset>,
    schema: SchemaRef,
    /// 我们的全量 arrow schema（time/sym/fields），供批次转换取字段类型。
    arrow_schema: Arc<arrow::datatypes::Schema>,
}

impl std::fmt::Debug for SplayedScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SplayedScan({})", self.dir.display())
    }
}

impl SplayedScan {
    /// 打开一个 dataset（`dir` 直接含 `.meta`）。
    pub fn new(dir: impl AsRef<Path>) -> PolarsResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        let dataset = Arc::new(
            open_dataset(&dir).map_err(|e| polars_err!(ComputeError: "open dataset {dir:?}: {e}"))?,
        );

        let time_type = dataset.meta.time_type();
        let time_arrow = match time_type {
            TimeType::Date32 => ArrowDataType::Date32,
            TimeType::TimestampUs => {
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
        };
        let mut arrow_fields = vec![
            ArrowField::new("time", time_arrow.clone(), false),
            ArrowField::new("sym", ArrowDataType::Utf8, false),
        ];
        let mut polars_fields = vec![
            Field::new("time".into(), to_polars_dtype(&time_arrow)?),
            Field::new("sym".into(), DataType::String),
        ];
        for name in dataset.list_fields().map_err(|e| polars_err!(ComputeError: "{e}"))? {
            let reader = splayed_core::FieldReader::open(dataset.field_path(&name))
                .map_err(|e| polars_err!(ComputeError: "{e}"))?;
            let aty = splayed_arrow::splayed_to_arrow_type(reader.data_type());
            arrow_fields.push(ArrowField::new(name.clone(), aty.clone(), true));
            polars_fields.push(Field::new(name.into(), to_polars_dtype(&aty)?));
        }

        Ok(Self {
            dir,
            dataset,
            schema: Arc::new(Schema::from_iter(polars_fields)),
            arrow_schema: Arc::new(arrow::datatypes::Schema::new(arrow_fields)),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl AnonymousScan for SplayedScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(self.schema())
    }

    fn allows_predicate_pushdown(&self) -> bool {
        true
    }

    fn allows_projection_pushdown(&self) -> bool {
        true
    }

    fn allows_slice_pushdown(&self) -> bool {
        false
    }

    fn scan(&self, args: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        // 1) 投影裁剪：输出列 = with_columns（无则全列）。
        let out_names: Vec<String> = match &args.with_columns {
            Some(cols) => cols.iter().map(|s| s.to_string()).collect(),
            None => self.schema.iter_names().map(|s| s.to_string()).collect(),
        };
        let field_names: Vec<String> = out_names
            .iter()
            .filter(|n| n.as_str() != "time" && n.as_str() != "sym")
            .cloned()
            .collect();

        // 2) 谓词 → 核心裁剪（尽力；未翻译部分由物理评估兜底）。
        let dtype_of = |name: &str| self.schema.get(name).map(|d| d.clone());
        let hint = predicate::translate(args.predicate.as_ref(), &dtype_of);

        let req = ScanRequest {
            columns: field_names,
            symbols: hint.symbols.unwrap_or(SymbolSelection::All),
            time_range: hint.time_range.unwrap_or_else(TimeRange::all),
            filters: hint.filters,
            batch_size: 65536,
            parallelism: 1,
        };
        let scanner = Scanner::new(&self.dataset);
        let plan = scanner
            .plan(&req)
            .map_err(|e| polars_err!(ComputeError: "scan plan: {e}"))?;
        let mut stream = scan_owned_parallel(Arc::clone(&self.dataset), &plan, &req, 1)
            .map_err(|e| polars_err!(ComputeError: "scan open: {e}"))?;

        // 3) CoreBatch → arrow RecordBatch（零拷贝）→ polars DataFrame 累积。
        let mut acc: Option<DataFrame> = None;
        while let Some(cb) = stream
            .next_batch()
            .map_err(|e| polars_err!(ComputeError: "scan: {e}"))?
        {
            let rb = self.corebatch_to_arrow(cb, &out_names)?;
            let df = record_batch_to_dataframe(&rb, &out_names)?;
            acc = match acc {
                Some(mut d) => {
                    d.vstack_mut(&df)
                        .map_err(|e| polars_err!(ComputeError: "{e}"))?;
                    Some(d)
                }
                None => Some(df),
            };
        }
        let mut df = acc.unwrap_or_else(|| DataFrame::empty_with_schema(&self.schema));

        // 4) 行数限制。
        if let Some(n) = args.n_rows {
            df = df.slice(0, n);
        }

        // 5) 谓词兜底：交给 polars 自过滤（核心推送只是裁剪，不改语义）。
        if let Some(pred) = &args.predicate {
            df = df
                .lazy()
                .filter(pred.clone())
                .collect()
                .map_err(|e| polars_err!(ComputeError: "predicate filter: {e}"))?;
        }

        Ok(df)
    }
}

impl SplayedScan {
    /// CoreBatch → arrow RecordBatch。
///
/// `corebatch_into_record_batch` 要求投影索引**升序**，而 polars 的
/// `with_columns` 可能任意顺序——先按升序提取，再在 DataFrame 侧按列名
/// 回查（`record_batch_to_dataframe`），顺序差异无损。
fn corebatch_to_arrow(&self, cb: CoreBatch, out_names: &[String]) -> PolarsResult<RecordBatch> {
    let mut pairs: Vec<(usize, ArrowField)> = Vec::with_capacity(out_names.len());
    for name in out_names {
        let idx = cb
            .schema()
            .index_of(name)
            .ok_or_else(|| polars_err!(ComputeError: "column {name} not in scan"))?;
        let af = self
            .arrow_schema
            .field_with_name(name)
            .map_err(|e| polars_err!(ComputeError: "{e}"))?;
        pairs.push((idx, af.as_ref().clone()));
    }
    pairs.sort_by_key(|(i, _)| *i);
    let indices: Vec<usize> = pairs.iter().map(|(i, _)| *i).collect();
    let fields: Vec<ArrowField> = pairs.iter().map(|(_, f)| f.clone()).collect();
    splayed_arrow::corebatch_into_record_batch(cb, &indices, &fields, None)
        .map_err(|e| polars_err!(ComputeError: "corebatch->arrow: {e}"))
}
}

/// 把 `dir` 注册为一个惰性扫描源（`LazyFrame`），支持 scan_* 风格：
/// ```text
/// use polars::prelude::*;
/// use splayed_polars::splayed_lazyframe;
/// let lf = splayed_lazyframe("data/2024")?;
/// let out = lf.filter(col("close").gt(lit(150.0)))
///              .select([col("sym"), col("close")])
///              .collect()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn splayed_lazyframe(dir: impl AsRef<Path>) -> PolarsResult<LazyFrame> {
    let scan = SplayedScan::new(dir)?;
    let args = ScanArgsAnonymous {
        schema: Some(scan.schema()),
        name: "SPLAYED SCAN",
        ..Default::default()
    };
    LazyFrame::anonymous_scan(Arc::new(scan), args)
}