//! 分区表（Hive 分区表的对位）——**引擎无关**，DataFusion / DuckDB / Polars
//! 共用这套实现。
//!
//! 模型：一个目录，其**直接子目录各含 `.meta`** 即一个分区（如 `2024/`、
//! `2025/`）；`dir/.meta` 直接存在时看作单分区表（向后兼容）。
//! 提供：发现 + schema 合并校验 + 三层剪裁（TIME / 符号 / 分区统计 min-max）
//! + 按分区名升序的流式合并扫描（产出 `CoreBatch`）。

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use splayed_format::{DataType, MetaFile, RawValue, TimeType, META_FILE_NAME};
use splayed_format::header::FieldHeader;

use crate::dataset::Dataset;
use crate::reader::FieldReader;
use crate::scanner::{
    Filter, ParallelScanBatches, ScanRequest, Scanner, SymbolSelection, TimeRange,
    scan_owned_parallel,
};
use crate::table_writer::{TableColumn, create_table, update_meta, update_table};

/// 分区表错误。
#[derive(Debug)]
pub enum PartitionError {
    Io(std::io::Error),
    Meta(splayed_format::MetaError),
    FieldHeader(String),
    /// 目录下没有任何含 `.meta` 的分区。
    NoPartition { dir: PathBuf },
    /// 分区 schema 不一致。
    SchemaMismatch {
        partition: String,
        expected: String,
        found: String,
    },
    TimeTypeMismatch {
        partition: String,
        expected: TimeType,
        found: TimeType,
    },
    /// key=value 目录名与普通目录名混用。
    MixedPartitionNaming { partition: String },
    /// 分区列声明不一致（不同 key）。
    PartitionColumnMismatch { partition: String, expected: String, found: String },
    /// 下层表写入失败（create_table / update_table / update_meta）。
    Table(crate::TableError),
    /// 分区已存在。
    PartitionExists { name: String },
    /// 分区不存在。
    PartitionNotFound { name: String },
    /// (SYM, TIME) 在目标分区中不存在（表级格子写入路由失败）。
    SymTimeNotFound { row: usize },
    Scan(String),
}

impl std::fmt::Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "partition io error: {e}"),
            Self::Meta(e) => write!(f, "partition meta error: {e}"),
            Self::FieldHeader(s) => write!(f, "field header error: {s}"),
            Self::NoPartition { dir } => {
                write!(f, "no partition (directory containing .meta) under {}", dir.display())
            }
            Self::SchemaMismatch { partition, expected, found } => write!(
                f,
                "partition {partition} schema mismatch — expected {expected}, found {found}"
            ),
            Self::TimeTypeMismatch { partition, expected, found } => write!(
                f,
                "partition {partition} time_type mismatch — expected {expected:?}, found {found:?}"
            ),
            Self::MixedPartitionNaming { partition } => write!(
                f,
                "partition {partition}: key=value dir names and plain dir names must not mix"
            ),
            Self::PartitionColumnMismatch { partition, expected, found } => write!(
                f,
                "partition {partition}: partition column mismatch — expected {expected}, found {found}"
            ),
            Self::Table(e) => write!(f, "partition table write error: {e}"),
            Self::PartitionExists { name } => write!(f, "partition '{name}' already exists"),
            Self::PartitionNotFound { name } => write!(f, "partition '{name}' not found"),
            Self::SymTimeNotFound { row } => {
                write!(f, "(SYM, TIME) at row {row} not found in any partition")
            }
            Self::Scan(s) => write!(f, "partition scan error: {s}"),
        }
    }
}
impl std::error::Error for PartitionError {}

/// 分区列的值类型（全表统一：全部可解析为 i64 → Int64，否则 String）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionColumnKind {
    Int64,
    String,
}

impl PartitionColumnKind {
    pub fn is_int64(&self) -> bool {
        matches!(self, Self::Int64)
    }
}

/// 声明式分区列（由 `key=value` 目录名解析而来，如 `year=2024/` → `year`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionColumn {
    pub name: String,
    pub kind: PartitionColumnKind,
}

/// 一个分区（元数据层面的视图；数据扫描惰性打开）。
#[derive(Debug, Clone)]
pub struct Partition {
    /// 分区名 = 子目录名（单分区表为 "."）。
    pub name: String,
    /// 分区目录（含 `.meta`）。
    pub path: PathBuf,
    /// 分区 meta（TIME AXIS / SYM INDEX 等，供剪裁）。
    pub meta: MetaFile,
    /// 声明式分区列的值（与 `schema().partition_columns` 对齐；普通目录名为空）。
    pub declared: Vec<(String, String)>,
}

/// 合并后的全表 schema（全分区一致）。
#[derive(Debug, Clone)]
pub struct PartitionSchema {
    pub time_type: TimeType,
    /// 有序字段（名, 类型）。
    pub fields: Vec<(String, DataType)>,
    /// 声明式分区列（key=value 目录名解析；空 = 无分区列）。
    pub partition_columns: Vec<PartitionColumn>,
}

/// 分区表（引擎无关）。
#[derive(Debug, Clone)]
pub struct PartitionedTable {
    dir: PathBuf,
    partitions: Vec<Partition>,
    schema: PartitionSchema,
    /// 全表符号并集（升序）。
    symbols: Vec<String>,
}

/// 分区级扫描请求（等价于单 dataset 的 `ScanRequest` 下推条件）。
#[derive(Debug, Clone)]
pub struct PartitionScanRequest {
    pub columns: Vec<String>,
    pub symbols: SymbolSelection,
    pub time_range: TimeRange,
    /// 作用于数据字段的值过滤（分区内行级 + 分区统计剪裁）。
    pub filters: Vec<Filter>,
    /// 作用于**声明式分区列**的过滤（分区级剪裁）。
    pub partition_filters: Vec<Filter>,
    pub batch_size: usize,
    /// 每个分区的并行度。
    pub parallelism: usize,
}

impl Default for PartitionScanRequest {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            symbols: SymbolSelection::All,
            time_range: TimeRange::all(),
            filters: Vec::new(),
            partition_filters: Vec::new(),
            batch_size: 65536,
            parallelism: 1,
        }
    }
}

/// 计划里的一个扫描任务：分区下标 + 该分区内有效的符号选择。
#[derive(Debug, Clone)]
pub struct PartitionTask {
    pub partition: usize,
    pub symbols: SymbolSelection,
}

/// 剪裁后的扫描计划（只含需要扫描的分区）。
#[derive(Debug)]
pub struct PartitionPlan {
    pub tasks: Vec<PartitionTask>,
    pub columns: Vec<String>,
    /// 全表分区总数（含被剪掉的）。
    pub total_partitions: usize,
}

impl PartitionedTable {
    /// 打开分区表：`dir/.meta` 存在 → 单分区；否则枚举直接子目录中含 `.meta`
    /// 者为分区（按名升序；无分区报错）。
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, PartitionError> {
        let dir = dir.as_ref().to_path_buf();
        let partition_dirs = discover_partitions(&dir)?;

        let mut partitions = Vec::with_capacity(partition_dirs.len());
        let mut schemas: Vec<PartitionSchema> = Vec::with_capacity(partition_dirs.len());
        let mut names = Vec::with_capacity(partition_dirs.len());
        let mut declared_per_partition: Vec<Vec<(String, String)>> =
            Vec::with_capacity(partition_dirs.len());

        for pd in &partition_dirs {
            let name = if pd == &dir {
                ".".to_string()
            } else {
                pd.file_name()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
            };
            // key=value 目录名 → 声明式分区列（单层一个 key=value；普通名=无）。
            let declared = name
                .split_once('=')
                .map(|(k, v)| vec![(k.to_string(), v.to_string())])
                .unwrap_or_default();
            if declared.is_empty() && name != "." {
                // 普通目录名：要求全表统一风格（要么都是 key=value，要么都没有）
                if partition_dirs.iter().any(|pd2| {
                    pd2 != pd
                        && pd2
                            .file_name()
                            .map(|s| s.to_string_lossy().contains('='))
                            .unwrap_or(false)
                }) {
                    return Err(PartitionError::MixedPartitionNaming {
                        partition: name,
                    });
                }
            }
            let meta_bytes =
                fs::read(pd.join(META_FILE_NAME)).map_err(PartitionError::Io)?;
            let meta = MetaFile::deserialize(&meta_bytes).map_err(PartitionError::Meta)?;
            let fields = list_field_schema(pd)?;
            schemas.push(PartitionSchema {
                time_type: meta.time_type(),
                fields,
                partition_columns: Vec::new(),
            });
            names.push(name.clone());
            declared_per_partition.push(declared);
            partitions.push(Partition {
                name,
                path: pd.clone(),
                meta,
                declared: Vec::new(), // 下方统一填充
            });
        }

        // 分区列合并：所有 key=value 分区列名一致；kind = 全部值可解析 i64 → Int64，否则 String。
        let mut partition_columns: Vec<PartitionColumn> = Vec::new();
        for (i, declared) in declared_per_partition.iter().enumerate() {
            for (key, value) in declared {
                let col = match partition_columns.iter_mut().find(|c| c.name == *key) {
                    Some(col) => col,
                    None => {
                        partition_columns.push(PartitionColumn {
                            name: key.clone(),
                            kind: PartitionColumnKind::Int64,
                        });
                        partition_columns.last_mut().unwrap()
                    }
                };
                if col.kind.is_int64() && value.parse::<i64>().is_err() {
                    col.kind = PartitionColumnKind::String;
                }
                let _ = i;
            }
        }
        // 单层目录只能表达一个 key=value（多层如 year=2024/month=01 暂不支持）。
        if partition_columns.len() > 1 {
            return Err(PartitionError::PartitionColumnMismatch {
                partition: names[0].clone(),
                expected: "at most one partition column (one-level key=value dirs)".to_string(),
                found: partition_columns[1].name.clone(),
            });
        }
        for (i, declared) in declared_per_partition.iter().enumerate() {
            partitions[i].declared = declared.clone();
        }

        // Schema 合并校验：字段名+类型一致；time_type 一致。
        let base = &schemas[0];
        for (i, s) in schemas[1..].iter().enumerate() {
            if s.fields != base.fields {
                return Err(PartitionError::SchemaMismatch {
                    partition: names[i + 1].clone(),
                    expected: format!("{:?}", base.fields),
                    found: format!("{:?}", s.fields),
                });
            }
            if s.time_type != base.time_type {
                return Err(PartitionError::TimeTypeMismatch {
                    partition: names[i + 1].clone(),
                    expected: base.time_type,
                    found: s.time_type,
                });
            }
        }

        // 符号并集（升序、去重）。
        let mut sym_set: Vec<String> = Vec::new();
        for p in &partitions {
            for s in &p.meta.symbols {
                if !sym_set.contains(s) {
                    sym_set.push(s.clone());
                }
            }
        }
        sym_set.sort();

        // 最终 schema：数据集字段 + 声明式分区列。
        let mut schema = schemas.remove(0);
        schema.partition_columns = partition_columns;

        Ok(Self {
            dir,
            partitions,
            schema,
            symbols: sym_set,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn partitions(&self) -> &[Partition] {
        &self.partitions
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn schema(&self) -> &PartitionSchema {
        &self.schema
    }

    pub fn fields(&self) -> &[(String, DataType)] {
        &self.schema.fields
    }

    /// 声明式分区列（key=value 目录名解析；空 = 无分区列）。
    pub fn partition_columns(&self) -> &[PartitionColumn] {
        &self.schema.partition_columns
    }

    /// 全表符号并集（升序）。
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    /// 惰性打开某分区的 Dataset。
    pub fn open_partition_dataset(&self, idx: usize) -> Result<Arc<Dataset>, PartitionError> {
        let p = &self.partitions[idx];
        let ds = crate::open_dataset(&p.path).map_err(|e| PartitionError::Scan(e.to_string()))?;
        Ok(Arc::new(ds))
    }

    /// 三层剪裁：TIME（分区 meta 时间轴 × 请求区间）→ 符号（分区缺失 →
    /// 跳过）→ 分区统计（值 filter 与分区字段 footer [min,max] 不相交 →
    /// 跳过）。
    pub fn plan(&self, request: &PartitionScanRequest) -> Result<PartitionPlan, PartitionError> {
        let mut tasks = Vec::new();
        for (i, part) in self.partitions.iter().enumerate() {
            // 1) TIME 剪裁：分区时间轴 [tmin, tmax]（含端点）与半开区间
            //    [request.start, request.end) 相交。
            let axis = &part.meta.time_axis;
            let (pmin, pmax) = (
                axis.first().copied().unwrap_or(i64::MIN),
                axis.last().copied().unwrap_or(i64::MAX),
            );
            if request.time_range.end <= pmin || request.time_range.start >= pmax {
                continue;
            }

            // 2) 符号剪裁：Symbols 选择中该分区不存在的符号剔除；一个都不
            //    存在 → 跳过分区。
            let symbols = match &request.symbols {
                SymbolSelection::All => SymbolSelection::All,
                SymbolSelection::Symbols(list) => {
                    let present: Vec<String> = list
                        .iter()
                        .filter(|s| part.meta.find_symbol(s).is_some())
                        .cloned()
                        .collect();
                    if present.is_empty() {
                        continue;
                    }
                    SymbolSelection::Symbols(present)
                }
            };

            // 3) 分区统计剪裁：任一次要分区扫描的 value filter 与该分区
            //    footer [min,max] 不相交 → 分区不可能有匹配行 → 跳过。
            if !self.partition_may_match(part, &request.filters) {
                continue;
            }

            // 4) 分区列剪裁：声明式分区列上的过滤条件与分区 declared 值
            //    不相交 → 跳过（如 year = '2024'）。
            if !self.partition_columns_may_match(part, &request.partition_filters) {
                continue;
            }

            tasks.push(PartitionTask { partition: i, symbols });
        }

        Ok(PartitionPlan {
            tasks,
            columns: request.columns.clone(),
            total_partitions: self.partitions.len(),
        })
    }

    /// 分区级统计剪裁（复用 Scanner 的 min/max 判定）。
    fn partition_may_match(&self, part: &Partition, filters: &[Filter]) -> bool {
        for f in filters {
            let name = f.field_name();
            let path = part.path.join(name);
            if !path.exists() {
                return false; // 缺失字段：该分区无法满足
            }
            let Ok(reader) = FieldReader::open(&path) else {
                return false; // 保守：读不了就当不匹配（跳过）
            };
            let Some(st) = reader.stats() else {
                continue; // 无 footer → 行级过滤保证
            };
            let dt = reader.data_type();
            let min_v = RawValue::read_le(&st.min, 0, dt);
            let max_v = RawValue::read_le(&st.max, 0, dt);
            if !crate::scanner::matches_range(f, &min_v, &max_v) {
                return false;
            }
        }
        true
    }

    /// 分区列剪裁：声明式分区列上的过滤与分区 `declared` 值不相交 → 跳过。
    fn partition_columns_may_match(&self, part: &Partition, filters: &[Filter]) -> bool {
        for f in filters {
            // 只认分区列；其它列名交给 dataset 层。
            let Some((_, value)) = part.declared.iter().find(|(k, _)| k == f.field_name()) else {
                continue;
            };
            if !partition_filter_passes(f, value) {
                return false;
            }
        }
        true
    }

    /// 按计划执行：每分区 `scan_owned_parallel`，按分区名升序（= 计划顺序）
    /// 流式合并产出 `CoreBatch`。
    pub fn scan(
        &self,
        plan: &PartitionPlan,
        request: &PartitionScanRequest,
    ) -> Result<PartitionScanBatches, PartitionError> {
        let mut streams = Vec::with_capacity(plan.tasks.len());
        for task in &plan.tasks {
            let dataset = self.open_partition_dataset(task.partition)?;
            let sub = ScanRequest {
                columns: request.columns.clone(),
                symbols: task.symbols.clone(),
                time_range: request.time_range.clone(),
                filters: request.filters.clone(),
                batch_size: request.batch_size,
                parallelism: 1,
                limit: None, // 表级 limit 由上层（引擎 LIMIT 节点）统一
            };
            let scanner = Scanner::new(&dataset);
            let plan0 = scanner
                .plan(&sub)
                .map_err(|e| PartitionError::Scan(e.to_string()))?;
            let stream = scan_owned_parallel(dataset, &plan0, &sub, request.parallelism)
                .map_err(|e| PartitionError::Scan(e.to_string()))?;
            streams.push(stream);
        }
        Ok(PartitionScanBatches {
            streams,
            current: 0,
        })
    }
}

/// 按分区名升序的流式合并迭代器。
pub struct PartitionScanBatches {
    streams: Vec<ParallelScanBatches>,
    current: usize,
}

impl PartitionScanBatches {
    pub fn next_batch(&mut self) -> Result<Option<crate::CoreBatch>, PartitionError> {
        while self.current < self.streams.len() {
            match self
                .streams[self.current]
                .next_batch()
                .map_err(|e| PartitionError::Scan(e.to_string()))?
            {
                Some(batch) => return Ok(Some(batch)),
                None => self.current += 1, // 该分区流结束，切下一个
            }
        }
        Ok(None)
    }

    pub fn total_partitions(&self) -> usize {
        self.streams.len()
    }
}

// ---------------------------------------------------------------------------
// 分区写能力（引擎无关；DataFusion / DuckDB 下次打开自动看到）
// ---------------------------------------------------------------------------

/// 一个分区的写入输入（建表 / 追加 / 重写共用）。
#[derive(Debug, Clone)]
pub struct PartitionWriteInput {
    /// 分区名 = 目录名（可含 `key=value`，如 `"year=2024"`）。
    pub name: String,
    pub time_type: TimeType,
    /// 逐行 (SYM, TIME)。
    pub sym: Vec<String>,
    pub time: Vec<i64>,
    /// 与全表 schema 一致的列（输入行序原始 LE 字节）。
    pub columns: Vec<TableColumn>,
    /// 输入是否已按 (SYM, TIME) 升序（性能提示；误报自动回退）。
    pub sorted: bool,
}

/// 一次建出整个分区表：root + N 个分区（每分区并发 `create_table`）。
pub fn create_partitioned_table(
    root: impl AsRef<Path>,
    time_type: TimeType,
    inputs: &[PartitionWriteInput],
) -> Result<(), PartitionError> {
    let root = root.as_ref();
    if inputs.is_empty() {
        return Err(PartitionError::NoPartition {
            dir: root.to_path_buf(),
        });
    }
    // 命名风格统一（key=value 与普通目录名不可混用）。
    if let Some(offender) = inputs
        .iter()
        .find(|i| is_kv_name(&i.name) != is_kv_name(&inputs[0].name))
    {
        return Err(PartitionError::MixedPartitionNaming {
            partition: offender.name.clone(),
        });
    }
    // schema 预检：列名+类型一致。
    for inp in &inputs[1..] {
        if !columns_match(&inputs[0].columns, &inp.columns) {
            return Err(PartitionError::SchemaMismatch {
                partition: inp.name.clone(),
                expected: field_fingerprint(&inputs[0].columns),
                found: field_fingerprint(&inp.columns),
            });
        }
    }

    fs::create_dir_all(root).map_err(PartitionError::Io)?;
    first_err(
        std::thread::scope(|s| {
            let handles: Vec<_> = inputs
                .iter()
                .map(|inp| {
                    s.spawn(move || {
                        create_table(
                            root.join(&inp.name),
                            time_type,
                            &inp.sym,
                            &inp.time,
                            &inp.columns,
                            inp.sorted,
                        )
                        .map(|_| ())
                        .map_err(PartitionError::Table)
                    })
                })
                .collect();
            collect_results(handles)
        }),
        root.join(&inputs[0].name),
    )
}

/// 追加一个分区（校验与既有分区 schema / 命名风格 / 分区列一致）。
pub fn append_partition(
    root: impl AsRef<Path>,
    input: &PartitionWriteInput,
) -> Result<(), PartitionError> {
    let root = root.as_ref();
    let table = PartitionedTable::open(root)?;
    if table.partitions().iter().any(|p| p.name == input.name) {
        return Err(PartitionError::PartitionExists {
            name: input.name.clone(),
        });
    }
    let any_kv = table.partitions().iter().any(|p| is_kv_name(&p.name));
    if any_kv != is_kv_name(&input.name) {
        return Err(PartitionError::MixedPartitionNaming {
            partition: input.name.clone(),
        });
    }
    if !columns_match_schema(table.fields(), &input.columns) {
        return Err(PartitionError::SchemaMismatch {
            partition: input.name.clone(),
            expected: fields_fingerprint(table.fields()),
            found: field_fingerprint(&input.columns),
        });
    }
    // key=value 新分区：key 不能与字段同名、且与既有分区列名一致。
    if let Some(key) = input.name.split_once('=').map(|(k, _)| k.to_string()) {
        if table.fields().iter().any(|(n, _)| n == &key) {
            return Err(PartitionError::PartitionColumnMismatch {
                partition: input.name.clone(),
                expected: "key=value key must differ from field names".to_string(),
                found: key,
            });
        }
        if let Some(pc) = table.partition_columns().first() {
            if pc.name != key {
                return Err(PartitionError::PartitionColumnMismatch {
                    partition: input.name.clone(),
                    expected: pc.name.clone(),
                    found: key,
                });
            }
        }
    }
    create_table(
        root.join(&input.name),
        table.schema().time_type,
        &input.sym,
        &input.time,
        &input.columns,
        input.sorted,
    )
    .map_err(PartitionError::Table)
    .map(|_| ())
}

/// 删除一个分区（目录整体删除；分区不存在报错）。
pub fn drop_partition(root: impl AsRef<Path>, name: &str) -> Result<(), PartitionError> {
    let root = root.as_ref();
    let table = PartitionedTable::open(root)?;
    if !table.partitions().iter().any(|p| p.name == name) {
        return Err(PartitionError::PartitionNotFound {
            name: name.to_string(),
        });
    }
    fs::remove_dir_all(root.join(name)).map_err(PartitionError::Io)?;
    Ok(())
}

/// 表级格子写入（跨分区路由）。
///
/// - `target_partition = Some(name)`：所有行必须落在该分区（否则
///   `SymTimeNotFound`）；
/// - `None`：每行路由到**唯一**包含该 (SYM, TIME) 的分区（meta 二分定位）；
///   没有任何分区包含 → `SymTimeNotFound`。
///
/// 路由后按分区分组，逐个委托单 dataset 的 `update_table`。
#[allow(clippy::too_many_arguments)]
pub fn update_partition_table(
    root: impl AsRef<Path>,
    sym: &[String],
    time: &[i64],
    columns: &[TableColumn],
    create_missing_fields: bool,
    target_partition: Option<&str>,
) -> Result<(), PartitionError> {
    let root = root.as_ref();
    if sym.len() != time.len() {
        return Err(PartitionError::Table(
            crate::TableError::LengthMismatch {
                field: "sym/time".to_string(),
                expected: sym.len(),
                got: time.len(),
            },
        ));
    }
    let table = PartitionedTable::open(root)?;
    let n = sym.len();

    // 行 → 分区（下标桶）。
    let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); table.partitions().len()];
    for row in 0..n {
        let target: Option<usize> = match target_partition {
            Some(name) => {
                let idx = table
                    .partitions()
                    .iter()
                    .position(|p| p.name == name)
                    .ok_or_else(|| PartitionError::PartitionNotFound {
                        name: name.to_string(),
                    })?;
                // 行必须真的落在该分区（meta 区间包含）。
                if !meta_contains(&table.partitions()[idx].meta, &sym[row], time[row]) {
                    return Err(PartitionError::SymTimeNotFound { row });
                }
                Some(idx)
            }
            None => table
                .partitions()
                .iter()
                .position(|p| meta_contains(&p.meta, &sym[row], time[row])),
        };
        match target {
            Some(idx) => buckets[idx].push(row),
            None => return Err(PartitionError::SymTimeNotFound { row }),
        }
    }

    // 按分区分组委托 update_table（列值按行切片）。
    for (idx, rows) in buckets.iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        let name = table.partitions()[idx].name.clone();
        let sub_sym: Vec<String> = rows.iter().map(|&r| sym[r].clone()).collect();
        let sub_time: Vec<i64> = rows.iter().map(|&r| time[r]).collect();
        let sub_columns: Vec<TableColumn> = columns
            .iter()
            .map(|col| {
                let w = col.data_type.size_of();
                let mut values = Vec::with_capacity(rows.len() * w);
                for &r in rows {
                    values.extend_from_slice(&col.values[r * w..(r + 1) * w]);
                }
                TableColumn {
                    name: col.name.clone(),
                    data_type: col.data_type,
                    values,
                }
            })
            .collect();
        update_table(
            root.join(&name),
            &sub_sym,
            &sub_time,
            &sub_columns,
            create_missing_fields,
        )
        .map_err(PartitionError::Table)?;
    }
    Ok(())
}

/// 表级布局重排：输入里已存在的分区 → `update_meta`（gather 重散布，数据
/// 保留）；新增分区 → `create_table`（需提供 columns）；未出现在输入中的
/// 既有分区 → 删除。分区处理并发执行。
pub fn update_partition_meta(
    root: impl AsRef<Path>,
    inputs: &[PartitionWriteInput],
) -> Result<(), PartitionError> {
    let root = root.as_ref();
    let table = PartitionedTable::open(root)?;
    let existing: HashSet<String> = table.partitions().iter().map(|p| p.name.clone()).collect();

    // 校验：新增分区 schema/time_type/命名风格一致。
    let any_kv = table.partitions().iter().any(|p| is_kv_name(&p.name));
    for inp in inputs {
        if !existing.contains(&inp.name) {
            if any_kv != is_kv_name(&inp.name) {
                return Err(PartitionError::MixedPartitionNaming {
                    partition: inp.name.clone(),
                });
            }
            if !columns_match_schema(table.fields(), &inp.columns) {
                return Err(PartitionError::SchemaMismatch {
                    partition: inp.name.clone(),
                    expected: fields_fingerprint(table.fields()),
                    found: field_fingerprint(&inp.columns),
                });
            }
            if inp.time_type != table.schema().time_type {
                return Err(PartitionError::TimeTypeMismatch {
                    partition: inp.name.clone(),
                    expected: table.schema().time_type,
                    found: inp.time_type,
                });
            }
        }
    }

    // 并发：update_meta（既有）/ create_table（新增）。
    // `is_existing` 预计算成 Vec<bool>（move 闭包可克隆，避免捕获整个 HashSet）。
    let is_existing: Vec<bool> = inputs
        .iter()
        .map(|inp| existing.contains(&inp.name))
        .collect();
    let write_err = std::thread::scope(|s| {
        let handles: Vec<_> = inputs
            .iter()
            .zip(is_existing.iter())
            .map(|(inp, exist)| {
                let exist = *exist;
                s.spawn(move || {
                    if exist {
                        update_meta(
                            root.join(&inp.name),
                            inp.time_type,
                            &inp.sym,
                            &inp.time,
                        )
                        .map(|_| ())
                        .map_err(PartitionError::Table)
                    } else {
                        create_table(
                            root.join(&inp.name),
                            inp.time_type,
                            &inp.sym,
                            &inp.time,
                            &inp.columns,
                            inp.sorted,
                        )
                        .map(|_| ())
                        .map_err(PartitionError::Table)
                    }
                })
            })
            .collect();
        collect_results(handles)
    });

    if let Some(e) = write_err {
        return Err(e);
    }

    // 移除未保留的分区。
    let input_names: HashSet<&str> = inputs.iter().map(|i| i.name.as_str()).collect();
    for name in existing.iter().filter(|n| !input_names.contains(n.as_str())) {
        fs::remove_dir_all(root.join(name)).map_err(PartitionError::Io)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 写路径助手
// ---------------------------------------------------------------------------

/// 目录名是否 key=value 风格。
fn is_kv_name(name: &str) -> bool {
    name.contains('=')
}

/// 列（输入）↔ 全表 schema 字段（名→类型，忽略顺序）。
fn columns_match_schema(fields: &[(String, DataType)], columns: &[TableColumn]) -> bool {
    if fields.len() != columns.len() {
        return false;
    }
    let mut want: Vec<(&str, DataType)> = fields.iter().map(|(n, t)| (n.as_str(), *t)).collect();
    want.sort_by_key(|(n, _)| *n);
    let mut got: Vec<(&str, DataType)> = columns
        .iter()
        .map(|c| (c.name.as_str(), c.data_type))
        .collect();
    got.sort_by_key(|(n, _)| *n);
    want == got
}

fn columns_match(a: &[TableColumn], b: &[TableColumn]) -> bool {
    columns_match_schema(
        &a.iter().map(|c| (c.name.clone(), c.data_type)).collect::<Vec<_>>(),
        b,
    )
}

fn field_fingerprint(columns: &[TableColumn]) -> String {
    let mut v: Vec<(&str, DataType)> = columns.iter().map(|c| (c.name.as_str(), c.data_type)).collect();
    v.sort_by_key(|(n, _)| *n);
    format!("{v:?}")
}

fn fields_fingerprint(fields: &[(String, DataType)]) -> String {
    let mut v: Vec<(&str, DataType)> = fields.iter().map(|(n, t)| (n.as_str(), *t)).collect();
    v.sort_by_key(|(n, _)| *n);
    format!("{v:?}")
}

/// 分区 meta 是否包含 (SYM, TIME)。
fn meta_contains(meta: &MetaFile, sym: &str, time: i64) -> bool {
    let Some(si) = meta.find_symbol(sym) else {
        return false;
    };
    let rec = &meta.sym_index[si];
    let lo = rec.time_start as usize;
    let hi = lo + rec.time_count as usize;
    meta.time_axis[lo..hi].binary_search(&time).is_ok()
}

/// 收集线程结果，返回第一个错误。
fn collect_results<T>(
    handles: Vec<std::thread::ScopedJoinHandle<'_, Result<T, PartitionError>>>,
) -> Option<PartitionError> {
    let mut first: Option<PartitionError> = None;
    for h in handles {
        if first.is_some() {
            continue;
        }
        match h.join() {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => first = Some(e),
            Err(_) => first = Some(PartitionError::Scan(
                "partition write thread panicked".to_string(),
            )),
        }
    }
    first
}

/// 写路径通用收尾：并发错误优先返回；错误时清理 root 下新建产物。
fn first_err(
    err: Option<PartitionError>,
    first_dir: PathBuf,
) -> Result<(), PartitionError> {
    if let Some(e) = err {
        let _ = fs::remove_dir_all(first_dir);
        return Err(e);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

/// 判定一个值过滤是否被分区列 `declared` 值满足。
///
/// 值语义：Int64 字面量与数字值按 i64 比较；String 字面量按字符串相等。
/// 无法精确判定（未知字面量类型 / 非比较）一律返回 `true`（不漏剪裁）。
fn partition_filter_passes(f: &Filter, value: &str) -> bool {
    use crate::FilterValue as Fv;
    match f {
        Filter::Equal { value: Fv::Int64(v), .. } => value.parse::<i64>().map(|x| x == *v).unwrap_or(false),
        Filter::NotEqual { value: Fv::Int64(v), .. } => value
            .parse::<i64>()
            .map(|x| x != *v)
            .unwrap_or(true),
        Filter::GreaterThan { value: Fv::Int64(v), .. } => {
            value.parse::<i64>().map(|x| x > *v).unwrap_or(false)
        }
        Filter::GreaterOrEqual { value: Fv::Int64(v), .. } => {
            value.parse::<i64>().map(|x| x >= *v).unwrap_or(false)
        }
        Filter::LessThan { value: Fv::Int64(v), .. } => {
            value.parse::<i64>().map(|x| x < *v).unwrap_or(false)
        }
        Filter::LessOrEqual { value: Fv::Int64(v), .. } => {
            value.parse::<i64>().map(|x| x <= *v).unwrap_or(false)
        }
        Filter::Equal { value: Fv::String(s), .. } => value == s.as_str(),
        Filter::NotEqual { value: Fv::String(s), .. } => value != s.as_str(),
        Filter::IsNull { .. } => false,    // declared 值恒非空
        Filter::IsNotNull { .. } => true,
        // 其它字面量类型（Float/Date…）：保守不剪裁。
        _ => true,
    }
}

/// 发现分区目录：`dir/.meta` 存在 → 单分区（`[dir]`）；否则子目录中含 `.meta`
/// 者（按名升序）。
fn discover_partitions(dir: &Path) -> Result<Vec<PathBuf>, PartitionError> {
    if dir.join(META_FILE_NAME).exists() {
        return Ok(vec![dir.to_path_buf()]);
    }
    let mut out: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(dir).map_err(PartitionError::Io)? {
        let entry = entry.map_err(PartitionError::Io)?;
        let p = entry.path();
        if p.is_dir() && p.join(META_FILE_NAME).exists() {
            out.push(p);
        }
    }
    if out.is_empty() {
        return Err(PartitionError::NoPartition {
            dir: dir.to_path_buf(),
        });
    }
    out.sort();
    Ok(out)
}

/// 分区字段列表（名 + 类型，按名升序）：只读各 FIELD 头部 64 字节。
fn list_field_schema(dir: &Path) -> Result<Vec<(String, DataType)>, PartitionError> {
    let mut fields = Vec::new();
    for entry in fs::read_dir(dir).map_err(PartitionError::Io)? {
        let entry = entry.map_err(PartitionError::Io)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == META_FILE_NAME || entry.path().file_name().is_none() {
            continue;
        }
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        let mut buf = [0u8; 64];
        let mut f = fs::File::open(entry.path()).map_err(PartitionError::Io)?;
        use std::io::Read;
        if f.read_exact(&mut buf).is_err() {
            continue;
        }
        let header: FieldHeader = bytemuck::pod_read_unaligned(&buf);
        if header.validate().is_err() {
            continue; // 非 FIELD 文件（如临时文件）
        }
        let Ok(dt) = header.data_type() else { continue };
        fields.push((name, dt));
    }
    fields.retain(|(n, _)| !n.ends_with(".new") && !n.ends_with(".tmp"));
    fields.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(fields)
}