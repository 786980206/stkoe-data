//! 分区表（Hive 分区表的对位）——**引擎无关**，DataFusion / DuckDB / Polars
//! 共用这套实现。
//!
//! 模型：一个目录，其**直接子目录各含 `.meta`** 即一个分区（如 `2024/`、
//! `2025/`）；`dir/.meta` 直接存在时看作单分区表（向后兼容）。
//! 提供：发现 + schema 合并校验 + 三层剪裁（TIME / 符号 / 分区统计 min-max）
//! + 按分区名升序的流式合并扫描（产出 `CoreBatch`）。

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
            Self::Scan(s) => write!(f, "partition scan error: {s}"),
        }
    }
}
impl std::error::Error for PartitionError {}

/// 一个分区（元数据层面的视图；数据扫描惰性打开）。
#[derive(Debug, Clone)]
pub struct Partition {
    /// 分区名 = 子目录名（单分区表为 "."）。
    pub name: String,
    /// 分区目录（含 `.meta`）。
    pub path: PathBuf,
    /// 分区 meta（TIME AXIS / SYM INDEX 等，供剪裁）。
    pub meta: MetaFile,
}

/// 合并后的全表 schema（全分区一致）。
#[derive(Debug, Clone)]
pub struct PartitionSchema {
    pub time_type: TimeType,
    /// 有序字段（名, 类型）。
    pub fields: Vec<(String, DataType)>,
}

/// 分区表（引擎无关）。
#[derive(Debug)]
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
    pub filters: Vec<Filter>,
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

        for pd in &partition_dirs {
            let name = if pd == &dir {
                ".".to_string()
            } else {
                pd.file_name()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
            };
            let meta_bytes =
                fs::read(pd.join(META_FILE_NAME)).map_err(PartitionError::Io)?;
            let meta = MetaFile::deserialize(&meta_bytes).map_err(PartitionError::Meta)?;
            let fields = list_field_schema(pd)?;
            schemas.push(PartitionSchema {
                time_type: meta.time_type(),
                fields,
            });
            names.push(name.clone());
            partitions.push(Partition {
                name,
                path: pd.clone(),
                meta,
            });
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

        Ok(Self {
            dir,
            partitions,
            schema: schemas.remove(0),
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
// 内部工具
// ---------------------------------------------------------------------------

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