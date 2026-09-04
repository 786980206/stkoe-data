# Table API

## 定位

`splayed-table` 位于 `splayed-core` 之上，负责把多个 Dataset 组织成一个逻辑 Table。

```
Query Engine
     │
     ▼
splayed-table
     │
     ├── Table
     │    ├── Partition
     │    │     └── Dataset
     │    └── partition pruning
     │
     ▼
splayed-core
     │
     ▼
Dataset
```

职责边界：

- `splayed-table`：Table、Partition 的组织、发现、裁剪，以及跨 Partition 的 scan/read 调度。
- `splayed-core`：单个 Dataset 的物理存储、Schema、scan/read/write。
- Table 层不重复定义 Dataset / META / Field 的物理格式。

## Partition

Partition 采用固定的 Hive-style 单层目录规范，不支持任意 partition expression，也不支持多级 partition 组合。

支持四种固定模式，四选一：

```
none

year=2026/
month=2026-09/
date=2026-09-04/
```

一个 Table 只能选择一种 partition scheme。

其中 `none` 表示不做 Partition，整个 Table 只有一个位于 Table 根目录的 Dataset。

### Year

```
year=2026/
```

表示整个 2026 年的数据。

### Month

```
month=2026-09/
```

表示 2026 年 9 月的数据。

### Date

```
date=2026-09-04/
```

表示 2026-09-04 当天的数据。

## 目录结构

Table 由一个根目录以及其下的一组 Partition Dataset 构成：

```
table/
├── year=2024/
├── year=2025/
└── year=2026/
```

每个 Partition 目录内部直接对应一个 Dataset。具体 Dataset 的内部物理格式由 `splayed-core` 定义。

## Partition 与 Dataset

当前设计中，一个 Partition 对应一个 Dataset：

```
Partition 1 ─── Dataset 1
Partition 2 ─── Dataset 2
Partition 3 ─── Dataset 3
```

Partition 不是新的物理存储格式，而是 Table 层对 Dataset 的逻辑组织单元。

## Partition Pruning

Table 层根据 `partition_scheme` 处理查询：`none` 模式不做 Partition pruning，直接扫描唯一的根目录 Dataset；`year` / `month` / `date` 模式根据查询中的 `time` 条件做粗粒度 Partition pruning，再将选中的 Dataset 交给 Core 执行精确 scan。

```
WHERE time >= 2026-09-01
  AND time <  2026-10-01

        ↓
partition pruning
        ↓
month=2026-09
        ↓
Dataset
        ↓
splayed-core scan
```

Partition pruning：

- 只使用 `time` 条件。
- 不使用 `sym` 做 Partition pruning。
- 只负责排除不可能命中的 Partition，不负责 Dataset 内部的行级过滤。
- `time` 条件继续传递给 Dataset scan 做精确过滤。
- `none` 模式没有 Partition pruning，直接访问唯一的根目录 Dataset。
- 三种 partition scheme（`year` / `month` / `date`）统一采用同一类 Table-level pruning 语义。

## Table API 总览

| 类别 | API | 语义 |
| --- | --- | --- |
| File / Lifecycle | `create_table(table_path, data, partition_scheme)` | 创建完整 Table |
|  | `create_table_partition(table_path, partition_name, data)` | 创建单个 Partition / Dataset |
|  | `delete_table_partition(table_path, partition_name)` | 删除单个 Partition / Dataset |
|  | `delete_table(table_path)` | 删除整个 Table |
|  | `open_table(path, mode, options?)` | 打开 Table，按需访问 Dataset |
| Metadata | `read_table_schema(table)` | 返回最后一个 Partition 的 Dataset Schema，不做多 Partition merge |
|  | `read_table_statistics(table)` | 聚合多个 Partition 的 Dataset Statistics |
|  | `read_table_metadata(table)` | 返回 Table 的 ordering、partitioning、capabilities 以及 Partition 信息 |
| Query | `scan_table(table, request)` | 根据 Table 条件定位 `PartitionRowRange` |
|  | `read_table(table, scanner, batch_size?)` | 消费 Scanner 并读取实际 `DataView`，支持 batch 聚合 |
| Write | `write_table(table, data)` | 对已有 `(sym,time)` 行执行批量覆盖写入 |

Table 层不提供 `read_table_partition()`；`Partition = Dataset`，TableReader 定位到 Partition 后直接调用 Core 的 `read_dataset()`。

---

## create_table

```
create_table(table_path, data, partition_scheme) -> result
```

创建一个完整 Table，并根据指定的 partition scheme 创建 Dataset。`none` 模式不划分 Partition，直接在 Table 根目录创建单个 Dataset；`year` / `month` / `date` 模式才按 `time` 划分 Partition Dataset。

### 参数

- `table_path`：Table 根目录。
- `data`：完整 Table 数据，必须包含 `sym` 与 `time`。
- `partition_scheme`：固定为 `none` / `year` / `month` / `date` 四选一。

### 语义

- `create_table` 是完整 Table 的创建入口。
- `partition_scheme = none`：整个 Table 直接对应一个根目录 Dataset，不创建 Partition 子目录。
- `partition_scheme = year` / `month` / `date`：根据 `time` 将输入数据划分为多个 Partition，并生成对应的 Hive-style `partition_name`。
- 每个 Partition Dataset 直接通过 Core `create_dataset()` 创建。
- 不同 Partition 可以并行创建。
- 不改变 Partition 内数据顺序。
- 每个 Partition 数据仍必须满足 `sym ASC, time ASC`。

```
create_table(table_path, data, partition_scheme)
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
    partition_scheme=none   year/month/date
          │                   │
          ▼                   ▼
   root Dataset          partition by time
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
                 Dataset   Dataset   Dataset
```

## delete_table

```
delete_table(table_path) -> result
```

删除整个 Table。

- 删除 Table 根目录及其全部 Partition Dataset。
- 不需要逐个删除 Partition。

## Table Handle

```
open_table(path, mode, options?) -> TableHandle
```

打开已有 Table，返回 Table-level Handle。

`TableHandle` 保存 Table 级上下文：

```
TableHandle
├── table_path
├── partition_scheme
│   └── none | year | month | date
└── partition discovery / metadata state
```

### 语义

- `mode` 表示访问意图，例如 `read` / `write`。
- `open_table()` 只打开 Table 自身的元信息与 Partition 组织信息，并在 `TableHandle` 中记录 `partition_scheme`。
- `partition_scheme` 支持 `none` / `year` / `month` / `date`。
- `none` 模式下 TableHandle 记录 Table 直接对应一个根目录 Dataset，不存在 Partition 子目录。
- 不提前打开所有 Dataset。
- Dataset 在实际 scan/read/write 时按需打开。
- `TableHandle` 内部可以缓存、复用已打开的 `DatasetHandle`；这是实现细节，不作为额外 public API。

```
open_table(table_path, mode)
          │
          ▼
      TableHandle
          │
          ├── partition scheme
          ├── partition discovery
          └── Dataset handles: on demand
```

## TableScanRequest

Table 查询请求独立于 Core `ScanRequest`，Table API 不直接暴露 Core 的扫描请求类型。

```
TableScanRequest
├── sym
├── time
├── predicate
├── projection
└── limit
```

### 语义

- `sym`：Table-level symbol 条件；不参与 Partition pruning，继续传递给 Dataset scan。
- `time`：Table-level 时间条件；`none` 模式直接传递给唯一 Dataset scan 做精确过滤；`year` / `month` / `date` 模式用于 Partition pruning，同时继续传递给 Dataset scan 做精确过滤。
- `predicate`：Table logical predicate。Table 可从中提取可用于 Partition pruning 的时间条件，其余条件作为 residual predicate 继续下传。
- `projection`：Table-level projection，跨 Partition 保持一致。
- `limit`：Table-level global limit；按 Partition 顺序扫描时，当前剩余 limit 可以下推给 Dataset scan。
- Table 层不把 `sym/time` 查询条件提前转换成 Core row ranges；row range 由 Dataset scan 产生。

## scan_table

```
scan_table(table, request) -> TableScanner
```

执行 Table-level 条件扫描，返回跨 Partition 的扫描位置。

```
TableScanRequest
       │
       ▼
partition pruning
       │
       ▼
selected Partitions
       │
       ├── locate / open Dataset
       │
       └── scan_dataset(residual request)
                    │
                    ▼
                 RowRange
                    │
                    ▼
             PartitionRowRange
```

### TableScanner

```
TableScanner
├── next() -> Result<PartitionRowRange?>
└── close() -> Result
```

`next()` 每次只返回一个连续的 `PartitionRowRange`，结束时返回 `null`。

```
PartitionRowRange
├── partition
└── row_range
```

例如：

```
next() → (year=2024, [100, 30))
next() → (year=2024, [180, 20))
next() → (year=2025, [20, 50))
next() → null
```

语义：

- `row_range` 是对应 Dataset 的逻辑行范围。
- `none` 模式下不存在 Partition 子目录，`PartitionRowRange` 中的 Partition 为隐式根 Dataset。
- `TableScanner` 不返回 `RowRanges` 集合。
- `TableScanner` 不负责 batch。
- `TableScanner` 的 primitive 语义与 Core `DatasetScanner` 保持一致：一次 `next()` 一个连续范围。
- 输出顺序固定为 `Partition ASC`；Partition 内保持 Dataset 的 `sym ASC, time ASC` 顺序。

### Limit

`limit` 是 Table-level global limit，而不是每个 Partition 独立的 limit。

```
remaining_limit
      │
      ▼
Partition 1 → Dataset scan(limit = remaining)
      │
      ▼
remaining_limit -= returned_rows
      │
      ▼
Partition 2 → Dataset scan(limit = remaining)
```

Scanner 负责维护全局 limit 的剩余量，但不负责把结果组织成 batch。

## read_table

```
read_table(table, scanner, batch_size?) -> TableReader
```

消费 `TableScanner` 已经定位好的 `PartitionRowRange`，读取实际数据并向上层产生 `DataView`。

### TableReader

```
TableReader
├── next() -> Result<DataView?>
└── close() -> Result
```

`TableReader` 是 Table-level 的数据消费接口：

- 从 `TableScanner.next()` 取得一个 `PartitionRowRange`。
- 根据 `partition_scheme` 定位 Dataset：`none` 模式直接定位 Table 根目录 Dataset；其他模式根据 `partition` 定位对应 Dataset。
- 使用其中的单个 `row_range` 直接调用 `read_dataset()`。
- 不重新执行 predicate。
- 不重新进行 Partition pruning。
- 多个 `PartitionRowRange` 可以被 Reader 依次消费并聚合为 `batch_size` 指定大小的 `DataView`。
- `batch_size` 属于读取/输出层语义，不改变 Scanner 的 `next()` primitive。
- 输出顺序遵循 Scanner：Partition ASC，Partition 内保持 Dataset 的原有顺序。

```
scan_table(table, request)
        │
        ▼
   TableScanner
        │ next()
        ▼
PartitionRowRange
        │
        ├── partition
        └── row_range
               │
               ▼
        locate Dataset
               │
               ▼
        read_dataset()
               │
               ▼
            DataView
               │
               ▼
        batch aggregation
               │
               ▼
         TableReader.next()
```

### 为什么不提供 read_table_partition

`Partition = Dataset`，而 `PartitionRowRange` 已经同时包含 Partition 和该 Partition 内的单个 RowRange。因此单独提供：

```
read_table_partition(table, partition, row_range, columns?)
```

本质上只是：

```
locate Partition
      ↓
locate Dataset
      ↓
read_dataset(dataset, row_range.offset, row_range.length, columns?)
```

没有独立的 Table-level 数据读取语义，因此不作为 public API 暴露。

## Table Read / Scan 分层

```
TableScanRequest
      │
      ▼
scan_table()
      │
      ▼
TableScanner
      │
      │ next()
      ▼
PartitionRowRange
      │
      ▼
TableReader
      │
      │ locate Dataset
      ▼
read_dataset()
      │
      ▼
DataView
```

职责分别是：

- `scan_table`：决定访问哪些 Partition，以及每个 Partition 的哪些逻辑行。
- `TableScanner`：一次返回一个 `PartitionRowRange`，不负责数据读取和 batch。
- `TableReader`：消费扫描结果，调用 Core `read_dataset()` 获取实际数据，并负责 Table-level 输出 batch。
- `read_dataset`：负责单个 Dataset 内连续逻辑行的数据读取。

## Table Metadata

### read_table_schema

```
read_table_schema(table) -> Schema
```

返回 Table 当前最后一个 Partition 的 `read_dataset_schema()` 结果。

- 不扫描所有 Partition。
- 不做多 Partition Schema merge。
- 因此 Table 的 Partition Schema 应保持一致；Table Schema 以最后一个 Partition 为准。
- Table Schema 是逻辑 Schema，仅用于字段与 DataType 映射、查询规划等。

### read_table_statistics

```
read_table_statistics(table) -> TableStatistics
```

聚合多个 Partition 的 `read_dataset_statistics()` 结果。

```
Partition A ── read_dataset_statistics() ─┐
Partition B ── read_dataset_statistics() ─┼─→ merge → TableStatistics
Partition C ── read_dataset_statistics() ─┘
```

建议返回：

```
TableStatistics
├── row_count
├── partition_count
├── sym_min
├── sym_max
├── time_min
└── time_max
```

其中 `row_count` 求和，`partition_count` 为 Partition 数量，min/max 在所有 Partition Statistics 上聚合。

### read_table_metadata

```
read_table_metadata(table) -> TableMetadata
```

返回 Table 对外声明的物理组织与执行能力，并同时提供 Partition 信息。

```
TableMetadata
├── ordering
├── partitioning
├── capabilities
└── partitions[]
```

`partitions[]` 等价于 `list_table_partitions(table)` 的结果，因此不再单独暴露 `list_table_partitions()` public API。

#### Ordering

```
Ordering
└── [sym ASC, time ASC]
```

表示 Table 输出/底层 Dataset 的自然排序，可供上层查询引擎判断是否需要额外 Sort。

#### Partitioning

```
Partitioning
├── type: time
└── scheme: year | month | date
```

表示 Table 按时间进行单层 Partition，具体 scheme 与 Table 创建时一致。

#### Capabilities

```
Capabilities
├── projection_pushdown
├── predicate_pushdown
└── limit_pushdown
```

表示 Table 能够接受并下推这些查询能力。Table API 不暴露 DuckDB/DataFusion 专用接口。

#### Partitions

```
PartitionInfo
├── name
├── time_min
├── time_max
└── ...
```

用于上层查询引擎了解可执行 Partition、进行调度与进一步规划；不直接暴露 Dataset Handle。

## Table Options

```
TableOptions
└── max_parallelism
```

`max_parallelism` 用于限制 Table 内部并行执行的最大并行度。

- Table API 对外暴露并行度配置，但不暴露 `scan_table_parallel()` 等 engine-specific API。
- 上层 DuckDB / DataFusion 可以根据自身执行资源设置 `max_parallelism`。
- 具体 Partition / Dataset 的并行调度由 Table 内部负责。
- Table 内部不得突破该上限，以避免与上层执行线程池形成不可控的并发放大。

## Result / Error Semantics

`Scanner` / `Reader` 的迭代接口区分“正常结束”和“执行错误”：

```
TableScanner
├── next() -> Result<PartitionRowRange?>
└── close() -> Result

TableReader
├── next() -> Result<DataView?>
└── close() -> Result
```

- `null` / `None` 表示正常结束。
- `Err(...)` 表示 IO、数据损坏、非法请求、资源错误等执行失败。
- `close()` 应可安全用于正常结束、LIMIT 提前结束、错误以及上层取消后的资源释放。

## write_table

```
write_table(table, data: DataView) -> result
```

对已有 Table 中已经存在的 `(sym, time)` 行执行批量覆盖写入。

### 输入 DataView

```
DataView
├── sym
├── time
├── field A
├── field B
└── ...
```

语义：

- 必须包含 `sym` 与 `time`。
- `sym` 与 `time` 按行成对构成联合键 `(sym, time)`。
- 输入整体必须按 `(sym ASC, time ASC)` 排序。
- 其他列表示需要写入的 Dataset Fields。
- `sym` / `time` 本身不作为 Dataset 普通 Field 写入。
- 支持 projection write：一次只写部分 Field，未提供的 Field 保持不变。

### 写入流程

```
            write_table(table, data)
                     │
                     ▼
              获取 table/.lock
                     │
                     ▼
               校验输入 DataView
                     │
       ┌─────────────┼─────────────┐
       │             │             │
       ▼             ▼             ▼
 sym/time 存在    key 唯一      sym/time 有序
       │             │             │
       └─────────────┼─────────────┘
                     ▼
       按 partition scheme 切分 data
                     │
       ┌─────────────┼─────────────┐
       ▼             ▼             ▼
   Partition A   Partition B   Partition C
       │             │             │
       ▼             ▼             ▼
 locate_index   locate_index   locate_index
       │             │             │
       ▼             ▼             ▼
    RowRanges      RowRanges      RowRanges
       │             │             │
       ▼             ▼             ▼
write_dataset   write_dataset   write_dataset
       │             │             │
       └─────────────┼─────────────┘
                     ▼
              释放 table/.lock
                     │
                     ▼
                    done
```

### Partition 切分

首先根据 Table 固定的 `partition_scheme`，仅按照 `time` 将输入 `DataView` 切分到对应 Partition。

- `year`：按年份切分。
- `month`：按月份切分。
- `date`：按日期切分。
- 不根据 `sym` 切分 Partition。
- `write_table` 不负责创建不存在的 Partition。
- 输入数据属于不存在的 Partition 时直接报错。

### 定位已有 `(sym, time)` 行

对每个目标 Partition，调用 Core META 的：

```
locate_index_handle(
    meta_handle,
    partition_data
) -> RowRanges
```

`locate_index_handle()` 使用 `DataView` 中成对的 `(sym, time)` 作为联合键进行定位。

由于输入已经按 `(sym ASC, time ASC)` 排序，META 可以利用有序输入和 `SYM INDEX / TIME AXIS` 做双指针扫描，避免逐行 lookup。

返回的 `RowRanges` 与输入顺序对应，每个 `RowRange` 对应一个连续的输入数据段和 Dataset 中对应的连续逻辑行范围。

例如：

```
input:
AAPL 09:00
AAPL 09:01
AAPL 09:02
AAPL 09:05
MSFT 09:00
MSFT 09:01

RowRanges:
[100,3)
[105,1)
[500,2)
```

对应：

```
input [0,3) -> dataset [100,3)
input [3,1) -> dataset [105,1)
input [4,2) -> dataset [500,2)
```

### Key 校验

`write_table` 要求输入中的每一个 `(sym,time)` 都必须能够在目标 META 中定位。

- 输入 `(sym,time)` 重复：**Error**。
- META 中不存在 `(sym,time)`：**Error**。
- 因此成功定位时必须满足：

```
sum(RowRange.length) == data.length
```

- 不允许因为部分 key 找不到而静默跳过。

### 执行写入

定位完成后，将每个 `RowRange` 对应的输入 `DataView` slice 写入 Dataset：

```
write_dataset(
    dataset,
    row_range.offset,
    input_data_slice
)
```

不同 Partition、不同连续 RowRange 可以并行执行；并行执行属于实现细节，不能改变最终 Table 结果顺序或语义。

### 写入语义

`write_table` 是 **overwrite existing rows**：

- 只覆盖已有 Dataset 逻辑行。
- 不追加新逻辑行。
- 不创建 Partition。
- 不创建 Dataset。
- 不扩大 Dataset capacity。
- 不新增 `sym`。
- 不扩大 `time` 范围。
- 不修改 META 结构。
- 不负责 Table Schema 变更。

因此它本质上是：

```
(sym, time)
     │
     ▼
META locate
     │
     ▼
existing Dataset logical rows
     │
     ▼
write_dataset()
```

### 部分写入

`write_table` **不提供事务回滚**。

如果多个 Partition / Dataset / Field 的写入过程中途发生错误：

- 已经成功完成的写入保留。
- 后续尚未完成的写入不会继续或可能失败。
- `write_table` 返回 `Error`。

因此 `write_table` 不是跨 Partition、Dataset、Field 的原子事务。

### 并发控制

`write_table` 使用 Table 根目录下的 `.lock` 文件控制并发写入：

```
table/
├── .lock
├── year=2024/
├── year=2025/
└── year=2026/
```

执行流程：

```
write_table()
    │
    ▼
acquire table/.lock
    │
    ▼
perform validation + locate + write
    │
    ▼
release table/.lock
```

同一个 Table 的写入通过 `.lock` 进行互斥控制。锁的等待、超时等具体行为属于实现层策略，不改变 `write_table` 的 public API 语义。

## 当前 Table API 边界

- Partition scheme 固定为 `none` / `year` / `month` / `date` 四选一。
- 不支持多级 Partition 组合。
- Partition 与 Dataset 保持 1:1。
- Table 层负责 Partition discovery / pruning / Dataset 定位 / 跨 Partition scan-read 调度。
- Dataset 内部物理读写仍由 `splayed-core` 负责。
- `TableScanRequest` 与 Core `ScanRequest` 独立。
- `TableScanner.next()` 返回单个 `PartitionRowRange`，不返回 ranges 集合，也不负责 batch。
- `TableReader` 负责消费扫描结果和输出 batch。
- 不提供 `scan_table_partition`。
- 不提供 `read_table_partition`。
- Table 的写入、Schema 变更及其他结构化操作暂不在本页扩展，待语义确定后单独整理。

### read_table 已确认语义

- Scan / Read 分开：`scan_table` 只负责定位，`read_table` 负责读取实际数据。
- `TableReader` 可以消费多个 `PartitionRowRange`，按 `batch_size` 聚合为 `DataView`。
- 最后一批允许小于 `batch_size`，不要求补齐。