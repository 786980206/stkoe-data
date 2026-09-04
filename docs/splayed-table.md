# splayed-table：API 设计（V2.0 Draft）

本页定义 `splayed-table` 的 public API。splayed-table 位于 `splayed-core` 之上，把多个 Dataset 组织成一个逻辑 Table：负责 Partition 的组织、发现、裁剪与跨 Partition 的 scan / read 调度。

- 所有数据访问经 core API 完成；Table 层不定义新的物理格式，不重复定义 Dataset / META / Field 的物理细节。
- core 格式与 API 见 [splayed-format](splayed-format.md) / [splayed-core](splayed-core.md)。

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

```
TableHandle
├── table_path
├── partition_scheme            // none | year | month | date
└── partition discovery / metadata state

TableOptions
└── max_parallelism             // Table 内部并行上限
```

- `open_table` 只打开 Table 级元信息与 Partition 组织信息；Dataset 在实际 scan / read / write 时按需打开并可在内部缓存复用（实现细节，非 public API）。
- Table 内部并行不得突破 `max_parallelism`，避免与上层执行线程池形成不可控并发放大。

> 规范说明：草稿中 `open_table` 的返回类型混用 `Table / TableHandle`，统一为 `TableHandle`；`close_table(table: TableHandle)`。

### 3.2 TableScanRequest

```
TableScanRequest
├── sym?
├── time?
├── predicate?
├── projection?
└── limit?
```

- 独立于 core `ScanRequest`；Table 层不把 sym / time 条件提前转换成 row ranges——row range 由 Dataset scan 产生。
- `sym`：不参与 Partition pruning，直接传给 Dataset scan。
- `time`：参与 Partition pruning，同时继续传给 Dataset scan。
- `predicate`：Table 逻辑条件；可从中提取可 pruning 的时间条件，其余作为 residual predicate 下传。
- `projection`：跨 Partition 保持一致。
- `limit`：Table 级**全局** limit，按 Partition 顺序扫描时以剩余量下推。

### 3.3 PartitionRowRange

```
PartitionRowRange
├── partition: String           // "year=2024"；none 模式为隐式根 Dataset
└── row_range: RowRange         // 该 Dataset 的逻辑行范围（offset/length）
```

### 3.4 Scanner / Reader 契约

```
TableScanner { next() -> Result<PartitionRowRange?>, close() -> Result<()> }
TableReader  { next() -> Result<DataView?>,         close() -> Result<()> }
```

- `None` = 正常结束；`Err` = IO / 数据损坏 / 非法请求 / 资源错误。
- `close()` 在正常结束、LIMIT 提前结束、错误、取消后都可安全调用。
- 与 core 的 Scanner / Reader 契约一致（splayed-core §3.6）。

### 3.5 TableMetadata / TableStatistics

```
TableMetadata
├── ordering: [sym ASC, time ASC]
├── partitioning: { type: time, scheme: none | year | month | date }
├── capabilities: { projection_pushdown, predicate_pushdown, limit_pushdown }
└── partitions: PartitionInfo[]     // { name, time_min, time_max, ... }
    // partitions[] 等价于 list_table_partitions 的结果，故不单独暴露该 API

TableStatistics
├── row_count           // 各 Partition 求和
├── partition_count
├── sym_min / sym_max   // 跨 Partition 聚合
└── time_min / time_max
```

`ordering` 表示 Table 输出与底层 Dataset 的自然排序，供上层判断是否需要额外 Sort；`capabilities` 表示 Table 可接受的下推能力；Table API 不暴露 DuckDB / DataFusion 专用接口。

## 4. API 设计

| 类别 | API | 语义 |
| --- | --- | --- |
| Lifecycle | `create_table(table_path, data, partition_scheme)` | 创建完整 Table |
|  | `create_table_partition(table_path, partition_name, data)` | 新增单个 Partition / Dataset |
|  | `delete_table_partition(table_path, partition_name)` | 删除单个 Partition / Dataset |
|  | `delete_table(table_path)` | 删除整个 Table |
|  | `open_table(path, mode, options?)` | 打开 Table，返回 `TableHandle` |
|  | `close_table(table)` | 关闭 TableHandle |
| Metadata | `read_table_schema(table)` | 最后一个 Partition 的 Dataset Schema |
|  | `read_table_statistics(table)` | 聚合各 Partition 统计 |
|  | `read_table_metadata(table)` | Table 组织信息与执行能力 |
| Query | `scan_table(table, request)` | 定位跨 Partition 的 `PartitionRowRange` |
|  | `read_table(table, scanner, batch_size?)` | 消费 Scanner，输出批量 `DataView` |
| Write | `write_table(table, data)` | 对已有 `(sym, time)` 行批量覆盖写入 |

### 4.1 create_table

```
create_table(table_path, data, partition_scheme) -> Result<()>
```

- `data`：完整 Table 数据，必须包含 `sym` 与 `time`，按 `(sym ASC, time ASC)` 排序。
- `partition_scheme`：`none | year | month | date`。

流程：

1. `none`：直接在 Table 根目录创建单个 Dataset（core `create_dataset`）。
2. `year / month / date`：按 `time` 将输入切分为多个 Partition，生成 Hive-style `partition_name`，每个 Partition 通过 core `create_dataset` 创建。
3. 不同 Partition 可并行创建；不改变 Partition 内数据顺序（time 切分天然保序）。

### 4.2 create_table_partition / delete_table_partition

```
create_table_partition(table_path, partition_name, data) -> Result<()>
delete_table_partition(table_path, partition_name) -> Result<()>
```

- 新增：`partition_name` 必须符合当前 scheme 且不存在（已存在 → Error）；内部即 core `create_dataset`；数据要求与 `create_dataset` 一致。
- 删除：删除对应 Partition 目录（Dataset）；不影响其他 Partition。
- 这是向 Table 引入新数据的唯一入口（`write_table` 只覆盖已有行；向已有 Partition 追加数据暂缓，见 §6）。

### 4.3 delete_table

```
delete_table(table_path) -> Result<()>
```

删除 Table 根目录及全部 Partition Dataset；不需要逐个删除 Partition。

### 4.4 open_table / close_table

```
open_table(path, mode, options?) -> Result<TableHandle>
close_table(table) -> Result<()>
```

- `mode` 表示访问意图（read / write）。
- 只打开 Table 元信息与 Partition 组织信息；不提前打开任何 Dataset。
- `partition_scheme` 记录在 `TableHandle`中；`none` 模式记录为「Table 直接对应根目录 Dataset」。

### 4.5 read_table_schema

```
read_table_schema(table) -> Schema
```

- 返回**最后一个 Partition** 的 Dataset Schema（core `read_dataset_schema`）。
- 不扫描所有 Partition，不做多 Partition Schema merge——Schema 跨 Partition 一致是**写侧责任**，读侧以最后 Partition 为准。
- 逻辑 Schema，仅用于字段 / DataType 映射与查询规划。

### 4.6 read_table_statistics

```
read_table_statistics(table) -> TableStatistics
```

- 遍历全部 Partition 的 `read_dataset_statistics` 并聚合：`row_count` 求和，min / max 跨 Partition 聚合。
- 只访问各 Dataset 的 META 级统计，不扫 Field 数据。

### 4.7 read_table_metadata

```
read_table_metadata(table) -> TableMetadata
```

返回 Table 自身组织信息（ordering / partitioning / capabilities / partitions）；不读取实际字段数据。

### 4.8 scan_table

```
scan_table(table, request) -> Result<TableScanner>
```

流程：

```
TableScanRequest
   ↓ partition pruning（仅 time 条件）
选中的 Partitions
   ↓ 逐 Partition：locate / open Dataset → scan_dataset(residual request)
DatasetScanner → 逻辑 RowRange
   ↓
TableScanner::next() -> PartitionRowRange
```

- `next()` 每次返回**一个**连续 `PartitionRowRange`；结束返回 None；不返回 ranges 集合，不负责 batch。
- 输出顺序固定：Partition ASC；Partition 内保持 Dataset 的 `sym ASC, time ASC`。
- `limit` 为全局 limit，Scanner 维护剩余量并按 Partition 递减下推：

```
remaining_limit → Partition 1 scan_dataset(limit=remaining)
                → remaining -= returned_rows
                → Partition 2 ...
```

### 4.9 read_table

```
read_table(table, scanner, batch_size?) -> Result<TableReader>
```

- Reader 从 Scanner 逐个取 `PartitionRowRange`，按 scheme 定位 Dataset（`none` → 根 Dataset），对其中单个 `row_range` 直接调用 core `read_dataset`。
- 不重新执行 predicate，不重新做 Partition pruning。
- 多个 `PartitionRowRange` 可被聚合为 `batch_size` 大小的 `DataView` 输出；最后一批允许小于 `batch_size`。
- `batch_size` 是读取 / 输出层语义，不改变 Scanner 的 `next()` primitive。
- 输出顺序遵循 Scanner：Partition ASC，Partition 内保持原序。

```
TableScanner.next() → PartitionRowRange
        → locate Dataset → read_dataset() → DataView
        → batch 聚合 → TableReader.next()
```

为什么不提供 `read_table_partition`：`Partition = Dataset`，且 `PartitionRowRange` 已同时包含 Partition 与行范围；该 API 本质只是 `locate Partition → read_dataset(...)`，没有独立语义，不作为 public API。同理不提供 `scan_table_partition`。

### 4.10 write_table

```
write_table(table, data: DataView) -> Result<()>
```

职责：对 Table 中**已存在**的 `(sym, time)` 行执行批量覆盖写入。

输入 `data`：

```
DataView
├── sym          // 必需
├── time         // 必需
├── field A ...  // 要写入的 Dataset Fields（支持 projection write）
```

- 必须包含 `sym` 与 `time`，按行配对构成联合键；输入整体按 `(sym ASC, time ASC)` 排序。
- `(sym, time)` 不得重复；必须已存在于目标 META。
- 其余列为要写入的 Fields；`sym / time` 本身不作为 Field 写入；未提供的 Field 保持原值。

流程：

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

要点：

- **定位**：每个 Partition 调用 core `locate_dataset_index`（(sym, time) 联合键，双指针扫描）。定位成功的充要条件：`sum(RowRange.length) == data.length`；输入 key 重复或 META 中不存在 → **Error**，不允许静默跳过。
- **写入语义**：overwrite existing rows——不追加行、不创建 Partition / Dataset、不扩容、不新增 sym、不扩大 time 范围、不修改 META / Table Schema。
- **无事务**：不提供跨 Partition / Dataset / Field 回滚；中途失败时已完成的写入保留，返回 Error。
- **并发**：同一 Table 的写通过 `table/.lock` 互斥；锁等待 / 超时策略属实现层。不同 Partition / 不同 RowRange 的写入可并行（实现细节），不得改变最终语义。
- 输入属于不存在的 Partition → Error（`write_table` 不创建 Partition）。

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
| `read_table_schema` / `read_table_statistics` | `read_dataset_schema` / `read_dataset_statistics` |
| `scan_table` | `scan_dataset`（内部 `scan_index_handle` + `scan_field_handle`） |
| `read_table` | `read_dataset`（内部 `read_field_handle`） |
| `write_table` | `locate_dataset_index` + `write_dataset`（内部 `write_field_handle` / `locate_index_handle`） |

Table 层只依赖 Dataset 级 API，不直接持有 MetaHandle / FieldHandle。

## 6. 暂缓 / 未定义

| 项 | 状态 | 说明 |
| --- | --- | --- |
| INSERT 追加到已有 Partition | 暂缓 | 扩大容量 / 新增 sym / 扩大 time 需要重建 Dataset（core 不提供原地 API）；新数据目前只能走 `create_table_partition` |
| 行级 DELETE | 暂缓 | 无行级删除 API；整 Partition 删除可用 `delete_table_partition` |
| UPSERT / MERGE INTO | 暂缓 | — |
| rename_table | 未定义 | 待语义确定后单独整理 |
| create / delete / update / rename_table_field | 未定义 | Table 级 Schema 变更暂缓；Dataset 级已有对应 API，Table 级入口待定 |
| compress_table_fields | 暂缓 | 语义 = 遍历 Partition 调 `compress_dataset_field`；独立 API 待定 |

## 7. SQL 域映射（参考）

| SQL | Table API | 下沉 core API |
| --- | --- | --- |
| `CREATE TABLE` | `create_table` | `create_dataset` |
| `DROP TABLE` | `delete_table` | `delete_dataset` |
| `COPY TO`（初始化） | `create_table` | `create_dataset` |
| `INSERT`（新 Partition） | `create_table_partition` | `create_dataset` |
| `INSERT`（追加已有 Partition） | 暂缓 | 重建 Dataset |
| `UPDATE`（覆盖已有行） | `write_table` | `locate_dataset_index` + `write_dataset` |
| `DELETE` | 暂缓（整 Partition：`delete_table_partition`） | `delete_dataset` |
| `SELECT` | `scan_table` + `read_table` | `scan_dataset` + `read_dataset` |
| `INFORMATION_SCHEMA` | `read_table_metadata` / `read_table_schema` / `read_table_statistics` | `read_dataset_*` |

## 8. 设计边界（当前 Table API 范围）

- Partition scheme 固定四选一；不支持多级 Partition 组合；Partition : Dataset = 1:1。
- `TableScanRequest` 与 core `ScanRequest` 相互独立；row range 只由 Dataset scan 产生。
- `TableScanner.next()` 返回单个 `PartitionRowRange`；batch 是 `TableReader` 的职责。
- 不提供 `scan_table_partition` / `read_table_partition` / `list_table_partitions`（后者并入 `read_table_metadata`）。
- Table 的写入扩展（INSERT 追加、Schema 变更）待语义确定后单独整理。
