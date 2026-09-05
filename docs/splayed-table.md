# splayed-table：API 设计（V2.0 Draft）

本页定义 `splayed-table` 的 public API。splayed-table 位于 `splayed-core` 之上，把多个 Dataset 组织成一个逻辑 Table：负责 Partition 的组织、发现、裁剪与跨 Partition 的 scan / read 调度。

- 所有数据访问经 core API 完成；Table 层不定义新的物理格式，不重复定义 Dataset / META / Field 的物理细节。
- core 格式与 API 见 [splayed-format](splayed-format.md) / [splayed-core](splayed-core.md)。
- 各 API 按统一格式组织：接口定义 → 参数表 → 内部实现流程 → 说明。标注「未实现」的条目为设计契约，代码未落地。

```
Query Engine
     │
     ▼
splayed-table          Table / Partition 组织、发现、裁剪、跨 Partition 调度
     │
     ▼
splayed-core           单个 Dataset 的物理存储、Schema、scan / read / write
     │
     ▼
META / Field
```

## 1. Table 与 Partition

Partition 采用固定的 Hive-style 单层目录规范：不支持任意 partition expression，不支持多级组合。一个 Table 只能选择一种 scheme，四选一：

```
none                    // 不划分 Partition：Table 根目录即唯一 Dataset
year=2026/              // 整年
month=2026-09/          // 整月
date=2026-09-04/        // 单日
```

目录结构：

```
table/
├── .lock               // write_table 并发互斥
├── year=2024/          // 每个 Partition 目录 = 一个 Dataset
├── year=2025/
└── year=2026/
```

- Partition 不是新的物理格式，而是 Table 层对 Dataset 的逻辑组织单元；Partition : Dataset = 1:1。
- `none` 模式不存在 Partition 子目录，`PartitionRowRange` 中的 Partition 为隐式根 Dataset。

## 2. Partition Pruning

- 只使用 `time` 条件；不使用 sym。
- 只负责排除不可能命中的 Partition，不做行级过滤；`time` 条件同时继续传给 Dataset scan 做精确过滤。
- `none` 模式无 Partition pruning，直接访问唯一根 Dataset。

```
WHERE time >= 2026-09-01 AND time < 2026-10-01
        ↓ partition pruning
month=2026-09 → Dataset → splayed-core scan（精确过滤）
```

## 3. 公共数据结构

### 3.1 TableHandle / TableOptions

**接口定义**：
```rust
pub struct TableHandle { /* table_path, partition_scheme, partition 发现/缓存, mode */ }
impl TableHandle {
    pub fn path(&self) -> &Path
    pub fn scheme(&self) -> PartitionScheme
    pub fn mode(&self) -> Mode
}
pub struct TableOptions { /* max_parallelism: Option<usize> */ }
```

**说明**：
- `open_table` 只打开 Table 级元信息与 Partition 组织信息；Dataset 在实际 scan / read / write 时按需打开并可在内部缓存复用（实现细节，非 public API）。
- `max_parallelism`（缺省 = 逻辑核数）是 Table 内部并行**总预算**：create_table 按
  `P_part × P_field ≤ max_parallelism` 在「分区并行 × Field 并行」间切分（见 §4.1），
  并在打开每个 Partition Dataset 时下沉为 `DatasetHandle.max_parallelism`，驱动
  `write_dataset` / `scan_dataset` 的 Field 级并行分桶——多层并发有统一上界，
  避免与上层执行线程池形成不可控并发放大。

### 3.2 TableScanRequest

**接口定义**：
```rust
pub struct TableScanRequest {
    pub sym: Option<String>,        // Table-level symbol 条件（不参与 Partition pruning）
    pub time: Option<(i64, i64)>,   // Table-level 时间条件（参与 pruning，同时下传做精确过滤）
    pub predicate: Option<Predicate>, // Table 逻辑谓词（可提取 pruning 时间条件，其余 residual 下传）
    pub projection: Vec<String>,    // Table-level projection（跨 Partition 一致）
    pub limit: Option<u64>,         // Table-level 全局 limit
}
```

**说明**：
- 独立于 core `ScanRequest`；Table 层不把 sym / time 条件提前转换成 row ranges——row range 由 Dataset scan 产生。
- `sym`：不参与 Partition pruning，直接传给 Dataset scan。
- `time`：参与 Partition pruning，同时继续传给 Dataset scan。
- `predicate`：Table 逻辑条件；可从中提取可 pruning 的时间条件，其余作为 residual predicate 下传。
- `projection`：跨 Partition 保持一致。
- `limit`：Table 级**全局** limit，按 Partition 顺序扫描时以剩余量下推。

### 3.3 PartitionRowRange

**接口定义**：
```rust
pub struct PartitionRowRange {
    pub partition: String,      // "year=2024"；none 模式为隐式根 Dataset
    pub row_range: RowRange,    // 该 Dataset 的逻辑行范围（offset/length）
}
```

### 3.4 Scanner / Reader 契约

**接口定义**：
```rust
impl TableScanner {
    pub fn next(&mut self) -> Result<Option<PartitionRowRange>, CoreError>
    pub fn close(self) -> Result<(), CoreError>
}
impl<'t> TableReader<'t> {
    pub fn next(&mut self) -> Result<Option<DataView<'_>>, CoreError>
    pub fn close(self) -> Result<(), CoreError>
}
```

**说明**：
- `None` = 正常结束；`Err` = IO / 数据损坏 / 非法请求 / 资源错误。
- `close()` 在正常结束、LIMIT 提前结束、错误、取消后都可安全调用。
- 与 core 的 Scanner / Reader 契约一致（splayed-core §3.6）。

### 3.5 TableMetadata / TableStatistics

**接口定义**：
```rust
pub struct TableMetadata {
    pub ordering: Vec<&'static str>,        // [sym ASC, time ASC]
    pub partitioning: Partitioning,         // { kind: "time", scheme: none|year|month|date }
    pub capabilities: Capabilities,         // { projection_pushdown, predicate_pushdown, limit_pushdown }
    pub partitions: Vec<PartitionInfo>,     // { name, time_min, time_max }；等价于 list_table_partitions
}
pub struct TableStatistics {
    pub row_count: u64,            // 各 Partition 求和
    pub partition_count: u32,
    pub sym_min: Option<String>,   // 跨 Partition 聚合
    pub sym_max: Option<String>,
    pub time_min: i64,
    pub time_max: i64,
}
```

**说明**：
- `ordering` 表示 Table 输出与底层 Dataset 的自然排序，供上层判断是否需要额外 Sort；`capabilities` 表示 Table 可接受的下推能力；Table API 不暴露 DuckDB / DataFusion 专用接口。

## 4. API 设计

### 4.0 总览

| 类别 | API | 语义 |
| --- | --- | --- |
| Lifecycle | `create_table` | 创建完整 Table |
|  | `create_table_partition` | 新增单个 Partition / Dataset |
|  | `delete_table_partition` | 删除单个 Partition / Dataset |
|  | `delete_table` | 删除整个 Table |
|  | `rename_table` | 重命名 Table 根目录 |
|  | `open_table` / `close_table` | 打开 / 关闭 TableHandle |
| Field 结构 | `create_table_field` | 所有 Partition 新增字段（全 NULL） |
|  | `delete_table_field` | 删除所有 Partition 中的同名字段 |
|  | `update_table_field` | 更新所有 Partition 中该字段的 header 物理属性 |
|  | `rename_table_field` | 重命名所有 Partition 中的同名字段 |
|  | `cast_table_field` | 转换所有 Partition 中该字段的类型 |
|  | `compress_table_field` / `decompress_table_field` | 压缩 / 解压所有 Partition 中的同名字段 |
| Metadata | `read_table_schema` | 最后一个 Partition 的 Dataset Schema |
|  | `read_table_statistics` | 聚合各 Partition 统计 |
|  | `read_table_metadata` | Table 组织信息与执行能力 |
| Query | `query_table` | 组合入口：一次完成扫描定位与数据读取 |
|  | `scan_table` | 定位跨 Partition 的 `PartitionRowRange` |
|  | `read_table` | 消费 Scanner，输出批量 `DataView` |
| Write | `write_table` | 对已有 `(sym, time)` 行批量覆盖写入（**未实现**，见 §4.14） |

### 4.1 create_table

**接口定义**：
```rust
pub fn create_table(table_path: &Path, data: Data, scheme: PartitionScheme,
    options: TableOptions) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table_path` | `&Path` | 输入 | Table 根目录；必须不存在 |
| `data` | `Data` | 输入 | 完整 Table 数据（**接管列所有权**，`none` 路径零克隆）；必须含 `sym` 与 `time`，按 `(sym ASC, time ASC)` 排序 |
| `scheme` | `PartitionScheme` | 输入 | `none \| year \| month \| date` 四选一 |
| `options` | `TableOptions` | 输入 | `max_parallelism`：Table 内部并行总预算（缺省 = 逻辑核数） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = Table 创建完成 |

**内部实现流程**：
```
① 主线程校验（轻量）      →  路径不存在 / sym-time 在列 / time 类型可分区（infer_tt）
                            →  0 行：仅创建根目录并返回 Ok（无 META、无分区）
② none 快速路径           →  create_dataset 根目录（列所有权直接移交，Field 级并行拿满预算）
③ 一次线性扫描 time 列    →  每分区「连续行片段」RowSpan(start, len)：
                              sym run 内 time 单调 ⇒ 同名分区行连续成段（片段可跨 sym 边界，
                              拼接后分区内仍保持 (sym, time) 序）；
                              分区名字符串仅在换段时构造（段内用整数粗键判别，无逐行分配）
④ 并行创建分区（预算切分）→  P_part = min(max_parallelism, 分区数)
                              P_part = 1 → 串行（Field 级并行拿满 max_parallelism）
                              否则 P_field = max(1, max_parallelism / P_part)，
                              round-robin 分桶 thread::scope：
                              每工作线程 gather_runs（批量拼接）→ create_dataset（Field 级并行 P_field）
                              ⇒ P_part × P_field ≤ max_parallelism，多层并发有上界
⑤ 失败语义                →  单分区原子（tmp + sync_all + rename）；已创建分区保留、不回滚
```

**gather_runs（分区 gather 批量拼接原则）**：
- 定宽列：目标缓冲一次预分配，逐**片段** `copy_from_slice`（每片段一次 memcpy，不逐行）；
- Utf8 字典列（sym 等）：`remap` 表把全局字典 id 映射为分区局部 id（首现序），字符串仅
  首现时拷贝一次；逐行只做 u32 键读取 + 查表，无 String 分配、无哈希表；
- validity：`Bitmap::copy_bits_from` 逐片段 word 级位拼接（不逐 bit）；源无 validity → 目标无
  validity；拼接后全 1 → 收缩为 None（不落 validity 区）。

**核心原则**：
- **片段化 gather**：排序契约使分区行在输入中连续成段，gather 从「逐行散点拷贝」退化为
  「连续片段拼接」，内存带宽接近 memcpy。
- **并行预算单源**：分区级 × Field 级共享 `max_parallelism` 预算（`P_part × P_field ≤ max_parallelism`），
  消除 Table → Dataset 两层并发的无上界叠加；不引入 rayon，`std::thread::scope` 分桶。
- **分区名零逐行分配**：整数粗键判别换段（year → 年号；month → `y×12+m-1`；date → 天号），
  字符串仅在片段边界构造。
- **单分区原子、表级不回滚**：与 write_table 一致的失败契约——部分创建保留（调用方删除
  Table 目录即可清理）。

**说明**：
- 不改变 Partition 内数据顺序（time 切分天然保序，片段拼接保持源行序）。
- 分区目录由 create_dataset 的 tmp + rename 隐式创建根目录（首个分区落盘时父目录随之存在）。
- 0 行输入创建的 Table 无 `.meta`、无分区目录；`open_table` 后 `discover_partitions` 为空
  （`none` scheme 需走 create_table_partition / 重新 create_table 建立根 Dataset）。

### 4.2 create_table_partition / delete_table_partition

**接口定义**：
```rust
pub fn create_table_partition(table_path: &Path, partition_name: &str, data: Data) -> Result<(), CoreError>
pub fn delete_table_partition(table_path: &Path, partition_name: &str) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table_path` | `&Path` | 输入 | Table 根目录；create / delete 均要求已存在（不存在 → `NotFound`，不隐式引导建表） |
| `partition_name` | `&str` | 输入 | 必须符合当前 scheme；create 时必须不存在（已存在 → Error）；delete 时非 scheme 命名一律拒绝（防误删任意子目录） |
| `data`（create） | `Data` | 输入 | 该 Partition 的完整 Dataset 数据（**列所有权直接移交**，无 gather / 克隆）；必须含 sym / time；空数据拒绝（禁止空 Dataset） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = Partition 创建 / 删除完成 |

**内部实现流程**：
```
create  ① 主线程轻量校验：Table 根存在 → 分区名符合 scheme → 与既有分区 scheme 一致
              → 分区目录不存在 → 含 sym/time → 非空
        ② 直接委托 create_dataset（列所有权移交）——META 校验 + Field 并行创建
              全部由 Dataset 层管理，Table 层不嵌套并行
delete  ① 主线程轻量校验：Table 根存在 → 分区名符合 scheme → 分区目录存在
        ② 委托 delete_dataset 递归删除（不打开 Dataset、不扫描数据；删除非热路径，无需并行）
```

**核心原则**：
- **薄包装定位**：两者分别是 `create_dataset` / `delete_dataset` 的 Table 级包装，只负责
  路径 / 分区名管理，不执行任何额外数据扫描或 partition 重算。
- **信任调用者**：`data` 属于 `partition_name` 不做 O(N) 逐行归属校验；行序合法性由
  MetaBuilder 构建期校验（在任何盘上落痕之前失败，零残留）。
- **Table 层禁止自行并行**：所有并发由 Dataset 层内部策略统一控制；上层需一次操作
  多个分区时，由更上层以统一 `max_parallelism` 并行调用。
- **失败语义与 create_table 一致**：create 不回滚，已写入文件保留；delete 显式
  `NotFound`（不做幂等删除，避免掩盖逻辑错误）。
- **并发契约**：调用方保证目标 Partition 无打开的 Dataset 句柄（Windows 下打开的
  mmap 会阻止删除；由 Table 层生命周期保证）。Table 无中央 partition index，
  删除后 `discover_partitions` 自然不再列出。

**说明**：
- 这是向 Table 引入新数据的唯一入口（`write_table` 只覆盖已有行；向已有 Partition 追加数据暂缓，见 §6）。

### 4.3 delete_table

**接口定义**：
```rust
pub fn delete_table(table_path: &Path) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table_path` | `&Path` | 输入 | Table 根目录 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 根目录及全部 Partition 已删除 |

**说明**：
- 删除 Table 根目录及全部 Partition Dataset；不需要逐个删除 Partition。

### 4.4 rename_table

**接口定义**：
```rust
pub fn rename_table(table_path: &Path, new_name: &str) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table_path` | `&Path` | 输入 | Table 根目录 |
| `new_name` | `&str` | 输入 | 新 Table 名（同一父目录内）；对应目录已存在 → Error |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 原子重命名完成 |

**说明**：
- 纯目录级 rename：Partition 目录名、META、Field 全部原样，不涉及 core 调用与数据改写。
- 调用前 Table 必须没有任何打开的 Handle（Windows 不允许对打开中的目录 rename）；`TableHandle` 缓存的 `table_path` 在重新 `open_table` 后生效。
- 目录 rename 在同一文件系统内原子完成；跨文件系统视为非法（Error），不提供移动语义。

### 4.5 open_table / close_table

**接口定义**：
```rust
pub fn open_table(table_path: &Path, mode: Mode, options: TableOptions) -> Result<TableHandle, CoreError>
pub fn close_table(handle: TableHandle) -> Result<(), CoreError>
impl TableHandle { pub fn close(mut self) -> Result<(), CoreError> }
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table_path` | `&Path` | 输入 | Table 根目录（必须已存在） |
| `mode` | `Mode` | 输入 | 访问意图（read / write） |
| `options` | `TableOptions` | 输入 | Table 级选项；`max_parallelism` 下沉为各 Partition DatasetHandle 的 Field 级并行上限 |
| 返回 | `Result<TableHandle, CoreError>` | 输出 | Table 生命周期 Handle |

**说明**：
- 只打开 Table 元信息与 Partition 组织信息；不提前打开任何 Dataset。
- `partition_scheme` 记录在 `TableHandle` 中；`none` 模式记录为「Table 直接对应根目录 Dataset」。
- `close_table(handle)` 与 `handle.close()` 等价；close 后 Handle 不可再用。

### 4.6 Field 结构操作（create / delete / update / rename / cast / compress / decompress）

**接口定义**：
```rust
impl TableHandle {
    pub fn create_table_field(&mut self, field: &str, data_type: DataType) -> Result<(), CoreError>
    pub fn delete_table_field(&mut self, field: &str) -> Result<(), CoreError>
    pub fn update_table_field(&mut self, field: &str, header: splayed_format::FieldHeader) -> Result<(), CoreError>
    pub fn rename_table_field(&mut self, field: &str, new_name: &str) -> Result<(), CoreError>
    pub fn cast_table_field(&mut self, field: &str, target_type: DataType) -> Result<(), CoreError>
    pub fn compress_table_field(&mut self, field: &str) -> Result<(), CoreError>
    pub fn decompress_table_field(&mut self, field: &str) -> Result<(), CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `field` | `&str` | 输入 | 字段名；不得为 `sym` / `time` 保留名 |
| `data_type` / `target_type` | `DataType` | 输入 | 新增字段类型 / cast 目标类型 |
| `header`（update） | `FieldHeader` | 输入 | 新 header 物理属性；`data_type` / `row_count` 由 core 强制为现值（类型转换走 cast） |
| `new_name` | `&str` | 输入 | 新字段名 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 所有 Partition 的同名字段结构变化完成 |

**内部实现**：
```
1. discover_partitions（none 模式 = 根 Dataset）
2. 前置校验：所有 Partition 均满足操作前提（存在 / 不存在、无重名、非保留名）
   ——任一不满足 → Error，不做任何修改
3. 逐 Partition 调用对应 Dataset 级 API
```

各 API 语义：

- `create_table_field(field, data_type)`：所有 Partition 新增**全 NULL** 字段（`DatasetFieldInit::AllNull`）。带数据（data / stream）初始化形式为设计预留——要求总行数等于 `Σ L_p` 且按 Table 自然顺序排列、从第一个 Partition 顺序填充——**当前未实现**。
- `delete_table_field`：逐 Partition `delete_dataset_field`；META 与 sym / time 不受影响。
- `update_table_field`：逐 Partition `update_dataset_field_header`；可更新的是 encoding / compression / flags 等物理属性。
- `rename_table_field`：逐 Partition `rename_dataset_field`。
- `cast_table_field`：逐 Partition `cast_dataset_field`；中途失败会造成 Partition 间类型不一致，直接重试补齐即可（已为目标类型的 Partition 再次转换是无害 no-op）。
- `compress_table_field`：前置要求各 Partition 该 Field 均为 uncompressed；逐 Partition `compress_dataset_field`（sym 对齐边界由 Dataset 层生成）。
- `decompress_table_field`：前置要求各 Partition 该 Field 均为 compressed；逐 Partition `decompress_dataset_field`。

**说明**：
- 对 Table 的**所有 Partition** 执行同名字段的结构操作；不保证跨 Partition 原子：中途失败时已完成的 Partition 保留，返回 Error。
- `none` 模式即唯一根 Dataset 上的对应操作。
- 各 Partition Schema 同步更新；Table Schema（以最后 Partition 为准）随之更新。

### 4.7 read_table_schema

**接口定义**：
```rust
impl TableHandle {
    pub fn read_table_schema(&self) -> Result<Schema, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| 返回 | `Result<Schema, CoreError>` | 输出 | 逻辑 Schema（含 sym / time） |

**说明**：
- 返回**最后一个 Partition** 的 Dataset Schema（core `read_dataset_schema`）。
- 不扫描所有 Partition，不做多 Partition Schema merge——Schema 跨 Partition 一致是**写侧责任**，读侧以最后 Partition 为准。
- 逻辑 Schema，仅用于字段 / DataType 映射与查询规划。

### 4.8 read_table_statistics

**接口定义**：
```rust
impl TableHandle {
    pub fn read_table_statistics(&self) -> Result<TableStatistics, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| 返回 | `Result<TableStatistics, CoreError>` | 输出 | 跨 Partition 聚合统计 |

**说明**：
- 遍历全部 Partition 的 `read_dataset_statistics` 并聚合：`row_count` 求和，min / max 跨 Partition 聚合。
- 只访问各 Dataset 的 META 级统计，不扫 Field 数据。

### 4.9 read_table_metadata

**接口定义**：
```rust
impl TableHandle {
    pub fn read_table_metadata(&self) -> Result<TableMetadata, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| 返回 | `Result<TableMetadata, CoreError>` | 输出 | Table 组织信息（ordering / partitioning / capabilities / partitions） |

**内部实现**：
```
none 模式 →  partitions 为空，直接返回组织信息
分区模式  →  discover_partitions → 逐 Partition partition_range（scheme + time_type）
             →  PartitionInfo { name, time_min, time_max }
```

**说明**：
- 返回 Table 自身组织信息与执行能力；不读取实际字段数据。
- `partitions[]` 等价于 list_table_partitions 的结果，故不单独暴露该 API。

### 4.10 scan_table

**接口定义**：
```rust
pub fn scan_table(table: &TableHandle, request: TableScanRequest) -> Result<TableScanner, CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table` | `&TableHandle` | 输入 | 已打开的 Table Handle |
| `request` | `TableScanRequest` | 输入 | 扫描请求（见 §3.2） |
| 返回 | `Result<TableScanner, CoreError>` | 输出 | 定位器；`next()` 每次返回一个连续 `PartitionRowRange`，结束返回 `None` |

**内部实现**：
```
TableScanRequest
   ↓ partition pruning（仅 time 条件）
选中的 Partitions
   ↓ 逐 Partition：locate / open Dataset → scan_dataset(residual request)
DatasetScanner → 逻辑 RowRange
   ↓
TableScanner::next() -> PartitionRowRange
```
- `limit` 为全局 limit，Scanner 维护剩余量并按 Partition 递减下推：

```
remaining_limit → Partition 1 scan_dataset(limit=remaining)
                → remaining -= returned_rows
                → Partition 2 ...
```

**说明**：
- `next()` 每次返回**一个**连续 `PartitionRowRange`；结束返回 None；不返回 ranges 集合，不负责 batch。
- 输出顺序固定：Partition ASC；Partition 内保持 Dataset 的 `sym ASC, time ASC`。

### 4.11 read_table

**接口定义**：
```rust
pub fn read_table<'t>(table: &'t TableHandle, scanner: TableScanner,
    batch_size: Option<usize>) -> TableReader<'t>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table` | `&TableHandle` | 输入 | 已打开的 Table Handle |
| `scanner` | `TableScanner` | 输入 | `scan_table` 产出的定位器（按值消费） |
| `batch_size` | `Option<usize>` | 输入 | 输出聚合粒度；`None` = 逐 range 输出；最后一批允许小于 `batch_size` |
| 返回 | `TableReader` | 输出 | Reader；`next()` 每次返回一个批量 `DataView`，结束返回 `None` |

**内部实现**：
```
TableScanner.next() → PartitionRowRange
        → locate Dataset（none → 根 Dataset）→ read_dataset() → DataView
        → batch 聚合（ColumnView 多 segment 拼接，零拷贝）→ TableReader.next()
```

**说明**：
- Reader 从 Scanner 逐个取 `PartitionRowRange`，对其中单个 `row_range` 直接调用 core `read_dataset`。
- 不重新执行 predicate，不重新做 Partition pruning。
- `batch_size` 是读取 / 输出层语义，不改变 Scanner 的 `next()` primitive。
- 输出顺序遵循 Scanner：Partition ASC，Partition 内保持原序。

为什么不提供 `read_table_partition`：`Partition = Dataset`，且 `PartitionRowRange` 已同时包含 Partition 与行范围；该 API 本质只是 `locate Partition → read_dataset(...)`，没有独立语义，不作为 public API。同理不提供 `scan_table_partition`。

### 4.12 query_table

**接口定义**：
```rust
pub fn query_table(table: &TableHandle, request: TableScanRequest,
    batch_size: Option<usize>) -> Result<TableReader<'_>, CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table` | `&TableHandle` | 输入 | 已打开的 Table Handle |
| `request` | `TableScanRequest` | 输入 | 语义与 `scan_table` 完全一致 |
| `batch_size` | `Option<usize>` | 输入 | 语义与 `read_table` 完全一致 |
| 返回 | `Result<TableReader, CoreError>` | 输出 | 组合入口 Reader（内部持有 Scanner 状态） |

**内部实现**：
```
query_table(table, request, batch_size)
        ≡ read_table(table, scan_table(table, request)?, batch_size)
```

**说明**：
- 组合入口：一次完成 Table 级扫描定位与数据读取。
- `query_table` 自身只做 pruning 与组装：能立即确定的 Error（partition 发现失败、非法请求）在调用时返回；执行期错误（Dataset 打开、IO、数据损坏）通过 `reader.next()` 的 `Err` 返回。
- `close()` 同时释放 Reader 与 Scanner，在任何时刻（正常结束 / LIMIT 提前结束 / 错误 / 取消）都可安全调用。
- 输出顺序（Partition ASC + sym/time ASC）、batch 聚合、零拷贝多 segment 拼接等语义与分离使用时完全一致。
- `scan_table` + `read_table` 分离形式保留：供需要两阶段控制的上层使用（先检查扫描范围再读取、跨 Table 交错调度等）。

### 4.13 read_table_statistics 之外的辅助（peek_time_type）

**接口定义**：
```rust
impl TableHandle {
    pub(crate) fn peek_time_type(&self) -> TimeType   // 内部使用；来自任一 Partition META header
}
```

**说明**：
- 供 scan_table / read_table_metadata 做 pruning 与 partition_range 计算；不作为 public API 暴露。

### 4.14 write_table（**未实现**，设计契约）

**接口定义**（设计）：
```rust
pub fn write_table(table: &TableHandle, data: &DataView<'_>) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table` | `&TableHandle` | 输入 | 已打开的 Table Handle（write mode） |
| `data` | `&DataView<'_>` | 输入 | 必须含 `sym` 与 `time`（按行配对构成联合键，整体按 `(sym ASC, time ASC)` 排序，不得重复且必须已存在于目标 META）；其余列为要写入的 Fields（支持 projection write；`sym / time` 本身不作为 Field 写入） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 全部 Partition 覆盖写入完成（无事务） |

**内部实现**（设计流程）：
```
write_table(table, data)
    ↓ acquire table/.lock
校验输入（key 存在 / 唯一 / 有序）
    ↓ 按 partition_scheme 仅按 time 切分
Partition A | Partition B | ...
    ↓ locate_dataset_index(handle, partition_data) → RowRanges
    ↓ 对每个 RowRange：write_dataset(dataset, offset, input_slice)
release table/.lock
```

**说明**（设计契约）：
- 职责：对 Table 中**已存在**的 `(sym, time)` 行执行批量覆盖写入——不追加行、不创建 Partition / Dataset、不扩容、不新增 sym、不扩大 time 范围、不修改 META / Table Schema。
- **定位**：每个 Partition 调用 core `locate_dataset_index`（(sym, time) 联合键，双指针扫描）。定位成功的充要条件：`sum(RowRange.length) == data.length`；输入 key 重复或 META 中不存在 → **Error**，不允许静默跳过。
- **无事务**：不提供跨 Partition / Dataset / Field 回滚；中途失败时已完成的写入保留，返回 Error。
- **并发**：同一 Table 的写通过 `table/.lock` 互斥；锁等待 / 超时策略属实现层。不同 Partition / 不同 RowRange 的写入可并行（实现细节），不得改变最终语义。
- 输入属于不存在的 Partition → Error（`write_table` 不创建 Partition）。
- 实现状态：**未实现**——当前代码无 `write_table`；新数据走 `create_table_partition`。

## 5. 与 core 的调用关系

```
TABLE ──> DATASET ──> META / FIELD

scan_table  → scan_dataset   → scan_index_handle + scan_field_handle
read_table  → read_dataset   → read_field_handle
write_table → locate_dataset_index + write_dataset → write_field_handle / locate_index_handle
```

| Table API | core API |
| --- | --- |
| `create_table` / `create_table_partition` | `create_dataset`（间接 `create_meta_file` / `create_field_file`） |
| `delete_table` / `delete_table_partition` | `delete_dataset` |
| `create / delete / update / rename / cast_table_field` | 对应 `create_dataset_field` / `delete_dataset_field` / `update_dataset_field_header` / `rename_dataset_field` / `cast_dataset_field` |
| `read_table_schema` / `read_table_statistics` | `read_dataset_schema` / `read_dataset_statistics` |
| `scan_table` | `scan_dataset`（内部 `scan_index_handle` + `scan_field_handle`） |
| `read_table` | `read_dataset`（内部 `read_field_handle`） |
| `query_table` | `scan_table` + `read_table`（组合入口） |
| `write_table` | `locate_dataset_index` + `write_dataset`（内部 `write_field_handle` / `locate_index_handle`） |

Table 层只依赖 Dataset 级 API，不直接持有 MetaHandle / FieldHandle。

## 6. 暂缓 / 未定义

| 项 | 状态 | 说明 |
| --- | --- | --- |
| INSERT 追加到已有 Partition | 暂缓 | 扩大容量 / 新增 sym / 扩大 time 需要重建 Dataset（core 不提供原地 API）；新数据目前只能走 `create_table_partition` |
| 行级 DELETE | 暂缓 | 无行级删除 API；整 Partition 删除可用 `delete_table_partition` |
| UPSERT / MERGE INTO | 暂缓 | — |
| `write_table` | 未实现 | 设计契约见 §4.14；实现前新数据走 `create_table_partition` |
| `create_table_field` 的 data / stream 初始化 | 未实现 | 当前仅全 NULL 形态（`DatasetFieldInit::AllNull`） |

## 7. SQL 域映射（参考）

| SQL | Table API | 下沉 core API |
| --- | --- | --- |
| `CREATE TABLE` | `create_table` | `create_dataset` |
| `DROP TABLE` | `delete_table` | `delete_dataset` |
| `ALTER ... RENAME FIELD` | `rename_table_field` | `rename_dataset_field` → `rename_field_file` |
| `ALTER ... ADD FIELD` | `create_table_field` | `create_dataset_field`（全 NULL） |
| `ALTER ... DROP FIELD` | `delete_table_field` | `delete_dataset_field` |
| `ALTER ... CAST TYPE` | `cast_table_field` | `cast_dataset_field` |
| `RENAME TABLE` | `rename_table` | —（目录级原子 rename，不经 core） |
| `ALTER ... COMPRESS FIELD` | `compress_table_field` | `compress_dataset_field` |
| `ALTER ... DECOMPRESS FIELD` | `decompress_table_field` | `decompress_dataset_field` |
| `COPY TO`（初始化） | `create_table` | `create_dataset` |
| `INSERT`（新 Partition） | `create_table_partition` | `create_dataset` |
| `INSERT`（追加已有 Partition） | 暂缓 | 重建 Dataset |
| `UPDATE`（覆盖已有行） | `write_table`（未实现） | `locate_dataset_index` + `write_dataset` |
| `DELETE`（按 Partition） | `delete_table_partition` | `delete_dataset` |
| `DELETE`（行级） | 暂缓 | — |
| `SELECT` | `query_table`（等价 `scan_table` + `read_table`） | `scan_dataset` + `read_dataset` |
| `INFORMATION_SCHEMA` | `read_table_metadata` / `read_table_schema` / `read_table_statistics` | `read_dataset_*` |

## 8. 设计边界（当前 Table API 范围）

- Partition scheme 固定四选一；不支持多级 Partition 组合；Partition : Dataset = 1:1。
- `TableScanRequest` 与 core `ScanRequest` 相互独立；row range 只由 Dataset scan 产生。
- `TableScanner.next()` 返回单个 `PartitionRowRange`；batch 是 `TableReader` 的职责。
- 不提供 `scan_table_partition` / `read_table_partition` / `list_table_partitions`（后者并入 `read_table_metadata`）。
- Table 的写入扩展（INSERT 追加）待语义确定后单独整理；Field 结构操作见 §4.6。
