//! Polars `AnonymousScan` 惰性扫描（谓词下推 + 列裁剪）。
//!
//! - `allows_predicate_pushdown`：接受下推——简单 `列 op 字面量` AND 链经
//!   [`crate::predicate`] 翻译到核心层（SYM 选择 / TIME 范围 / 值过滤）做裁剪；
//!   任何未翻译部分在结果上由 polars 物理评估兜底（结果永远正确）。
//! - `allows_projection_pushdown`：接受投影裁剪——只读 `with_columns` 涉及的列。
//! - 数据经 [`crate::arrowconv`] 的 Arrow C data interface 转换进 polars
//!   （数据结构共享 / 接管，不做逐值拷贝）。

use std::any::Any;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use polars::lazy::frame::ScanArgsAnonymous;
use polars::prelude::*;

use splayed_core::{
    CoreBatch, Dataset, PartitionColumnKind, PartitionScanRequest, PartitionedTable, ScanRequest,
    Scanner, SymbolSelection, TimeRange, open_dataset, scan_owned_parallel,
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
        let dtype_of = |name: &str| self.schema.get(name).cloned();
        let hint = predicate::translate(
            args.predicate.as_ref(),
            &dtype_of,
            &std::collections::HashSet::new(),
        );

        let req = ScanRequest {
            columns: field_names,
            symbols: hint.symbols.unwrap_or(SymbolSelection::All),
            time_range: hint.time_range.unwrap_or_else(TimeRange::all),
            filters: hint.filters,
            batch_size: 65536,
            parallelism: 1,
            // n_rows → 读取期截断（core 扫满即停），不再仅靠收集后 slice。
            limit: args.n_rows,
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

// ---------------------------------------------------------------------------
// 分区表惰性扫描（复用 `splayed-core::partition` 剪裁/合并）
// ---------------------------------------------------------------------------

/// 分区表惰性扫描源：schema = time/sym/fields + 声明式分区列；谓词中的分区列
/// 过滤路由到分区级剪裁，数据集列过滤照常下推。
pub struct SplayedTableScan {
    dir: PathBuf,
    table: PartitionedTable,
    schema: SchemaRef,
    /// dataset 全量 arrow schema（time/sym/fields），供批次转换。
    dataset_arrow_schema: Arc<arrow::datatypes::Schema>,
}

impl std::fmt::Debug for SplayedTableScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SplayedTableScan({})", self.dir.display())
    }
}

impl SplayedTableScan {
    pub fn new(dir: impl AsRef<Path>) -> PolarsResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        let table = PartitionedTable::open(&dir)
            .map_err(|e| polars_err!(ComputeError: "open partitioned table {dir:?}: {e}"))?;

        let time_type = table.schema().time_type;
        let time_arrow = match time_type {
            TimeType::Date32 => ArrowDataType::Date32,
            TimeType::TimestampUs => {
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
        };
        let mut arrow_fields = vec![
            ArrowField::new("time", time_arrow.clone(), false),
            ArrowField::new("sym", arrow::datatypes::DataType::Utf8, false),
        ];
        let mut polars_fields = vec![
            Field::new("time".into(), to_polars_dtype(&time_arrow)?),
            Field::new("sym".into(), DataType::String),
        ];
        for (name, dt) in table.fields() {
            let aty = splayed_arrow::splayed_to_arrow_type(*dt);
            arrow_fields.push(ArrowField::new(name.clone(), aty.clone(), true));
            polars_fields.push(Field::new(name.into(), to_polars_dtype(&aty)?));
        }
        for pc in table.partition_columns() {
            let (aty, pty) = match pc.kind {
                PartitionColumnKind::Int64 => (
                    arrow::datatypes::DataType::Int64,
                    DataType::Int64,
                ),
                PartitionColumnKind::String => (
                    arrow::datatypes::DataType::Utf8,
                    DataType::String,
                ),
            };
            arrow_fields.push(ArrowField::new(pc.name.clone(), aty, false));
            polars_fields.push(Field::new(pc.name.clone().into(), pty));
        }

        Ok(Self {
            dir,
            table,
            schema: Arc::new(Schema::from_iter(polars_fields)),
            dataset_arrow_schema: Arc::new(arrow::datatypes::Schema::new(arrow_fields)),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl AnonymousScan for SplayedTableScan {
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
        // 输出列（schema 序 ∩ with_columns）。
        let schema_cols: Vec<String> = self.schema.iter_names().map(|s| s.to_string()).collect();
        let out_names: Vec<String> = match &args.with_columns {
            Some(cs) => schema_cols
                .into_iter()
                .filter(|n| cs.iter().any(|c| c.as_str() == n.as_str()))
                .collect(),
            None => schema_cols,
        };
        let part_col_names: HashSet<String> = self
            .table
            .partition_columns()
            .iter()
            .map(|c| c.name.clone())
            .collect();
        let dataset_out: Vec<String> = out_names
            .iter()
            .filter(|n| !part_col_names.contains(*n))
            .cloned()
            .collect();
        let field_names: Vec<String> = dataset_out
            .iter()
            .filter(|n| n.as_str() != "time" && n.as_str() != "sym")
            .cloned()
            .collect();

        let dtype_of = |name: &str| self.schema.get(name).cloned();
        let hint = predicate::translate(
            args.predicate.as_ref(),
            &dtype_of,
            &part_col_names,
        );

        let preq = PartitionScanRequest {
            columns: field_names.clone(),
            symbols: hint.symbols.unwrap_or(SymbolSelection::All),
            time_range: hint.time_range.unwrap_or_else(TimeRange::all),
            filters: hint.filters,
            partition_filters: hint.partition_filters,
            batch_size: 65536,
            parallelism: 1,
        };
        let pplan = self
            .table
            .plan(&preq)
            .map_err(|e| polars_err!(ComputeError: "partition plan: {e}"))?;

        let mut acc: Option<DataFrame> = None;
        for task in &pplan.tasks {
            let part = &self.table.partitions()[task.partition];
            let dataset = Arc::new(
                open_dataset(&part.path)
                    .map_err(|e| polars_err!(ComputeError: "open partition: {e}"))?,
            );
            let req = ScanRequest {
                columns: field_names.clone(),
                symbols: task.symbols.clone(),
                time_range: preq.time_range,
                filters: preq.filters.clone(),
                batch_size: preq.batch_size,
                parallelism: 1,
                limit: None,
            };
            let scanner = Scanner::new(&dataset);
            let plan0 = scanner
                .plan(&req)
                .map_err(|e| polars_err!(ComputeError: "scan plan: {e}"))?;
            let mut stream = scan_owned_parallel(dataset, &plan0, &req, 1)
                .map_err(|e| polars_err!(ComputeError: "scan open: {e}"))?;

            while let Some(cb) = stream
                .next_batch()
                .map_err(|e| polars_err!(ComputeError: "scan: {e}"))?
            {
                let rb = table_batch_to_arrow(&self.dataset_arrow_schema, cb, &dataset_out)?;
                let mut df = record_batch_to_dataframe(&rb, &dataset_out)?;
                // 常量分区列（out 顺序追加）。
                for name in out_names.iter().filter(|n| part_col_names.contains(n.as_str())) {
                    let value = part
                        .declared
                        .iter()
                        .find(|(k, _)| k == name)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default();
                    let len = df.height();
                    let pc = self
                        .table
                        .partition_columns()
                        .iter()
                        .find(|c| c.name == *name)
                        .unwrap();
                    let series = match pc.kind {
                        PartitionColumnKind::Int64 => Series::new(
                            name.into(),
                            vec![value.parse::<i64>().unwrap_or_default(); len],
                        ),
                        PartitionColumnKind::String => {
                            Series::new(name.into(), vec![value.clone(); len])
                        }
                    };
                    df.with_column(series).map_err(|e| polars_err!(ComputeError: "{e}"))?;
                }
                acc = match acc {
                    Some(mut d) => {
                        d.vstack_mut(&df).map_err(|e| polars_err!(ComputeError: "{e}"))?;
                        Some(d)
                    }
                    None => Some(df),
                };
            }
        }
        let mut df = acc.unwrap_or_else(|| DataFrame::empty_with_schema(&self.schema));

        if let Some(n) = args.n_rows {
            df = df.slice(0, n);
        }
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

/// CoreBatch → arrow（产出 dataset 列；升序索引契约，列序无损）。
fn table_batch_to_arrow(
    dataset_schema: &arrow::datatypes::Schema,
    cb: CoreBatch,
    out_names: &[String],
) -> PolarsResult<RecordBatch> {
    let mut pairs: Vec<(usize, ArrowField)> = Vec::with_capacity(out_names.len());
    for name in out_names {
        let idx = cb
            .schema()
            .index_of(name)
            .ok_or_else(|| polars_err!(ComputeError: "column {name} not in batch"))?;
        let af = dataset_schema
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

/// 分区表（含 key=value 分区列）注册为 Polars 惰性数据源。
pub fn splayed_lazyframe_table(dir: impl AsRef<Path>) -> PolarsResult<LazyFrame> {
    let scan = SplayedTableScan::new(dir)?;
    let args = ScanArgsAnonymous {
        schema: Some(scan.schema()),
        name: "SPLAYED TABLE SCAN",
        ..Default::default()
    };
    LazyFrame::anonymous_scan(Arc::new(scan), args)
}

// ---------------------------------------------------------------------------
// 子集（`.sub.xxx`）→ 惰性帧（物化视图）
// ---------------------------------------------------------------------------

/// 子集 `.sub.{sub_name}`（父 dataset 目录）注册为 Polars 惰性帧。
///
/// 子集是父 `.meta` 网格的视图：直接经 `splayed_arrow::read_subset` 物化
/// （父全局行序，列 = time/sym/全部字段）后转 polars，适合「父表 + 成分股
/// 子集」这类读取。大表全量扫描请用 [`splayed_lazyframe`]（下推扫描，
/// 子集侧无谓词下推——先物化再过滤）。
pub fn splayed_lazyframe_subset(
    dir: impl AsRef<Path>,
    sub_name: &str,
) -> PolarsResult<LazyFrame> {
    let rb = splayed_arrow::read_subset(dir, sub_name, &[])
        .map_err(|e| polars_err!(ComputeError: "read subset {sub_name}: {e}"))?;
    let names: Vec<String> = rb.schema().fields().iter().map(|f| f.name().clone()).collect();
    let df = record_batch_to_dataframe(&rb, &names)?;
    Ok(df.lazy())
}