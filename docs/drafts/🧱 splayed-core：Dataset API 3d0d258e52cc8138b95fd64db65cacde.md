# 🧱 splayed-core：Dataset API

Dataset 是一个完整表数据集的物理存储实例，对应一个目录级数据单元。它由一个 META 文件和多个 Field 文件组成；Dataset 层负责将逻辑表操作映射到 META 与 Field。

Dataset 是 `splayed-core` 面向上层查询引擎暴露的完整数据单元。Dataset API 只描述 Dataset 级语义，不暴露 META / Field 的物理布局细节。

## API 总览

| API | 类型 | 作用 | 输入 |
| --- | --- | --- | --- |
| `create_dataset(path, data)` | File | 创建完整 Dataset | `Data` |
| `create_dataset_index(path, data)` | File | 创建 Dataset Index（即 `.meta`） | `Data` |
| `open_dataset(path, mode)` | File | 打开 Dataset | path, mode |
| `delete_dataset(path)` | File | 删除完整 Dataset | path |
| `create_dataset_fields(path, data)` | File | 一次新增多个 Dataset Field | `DataView` |
| `read_dataset_schema(dataset)` | Dataset | 读取 Dataset Schema | Dataset |
| `read_dataset_statistics(dataset)` | Dataset | 读取 Dataset 级统计信息 | Dataset |
| `read_dataset(dataset, offset, length, columns?)` | Dataset | 读取逻辑行 | row range, columns |
| `write_dataset(dataset, offset, data)` | Dataset | 覆盖已有逻辑行 | row offset, `DataView` |
| `scan_dataset(dataset, request)` | Dataset | 条件扫描 | `ScanRequest` |
| `close_dataset(dataset)` | Dataset | 关闭 Dataset | Dataset |

其中 `create_dataset_fields()` 与 `create_dataset_fields()` 是两个并列入口：前者用于新增单个 Field，后者用于一次新增多个 Field。两者语义一致，只是输入粒度不同。

## Dataset 的结构

```
Dataset
├── .meta
│   └── META：sym/time → logical row range
│
└── Fields
    ├── field A
    ├── field B
    └── field C
```

Dataset 的 `sym` / `time` 身份由 META 管理；普通字段分别存储在独立 Field 文件中。

Dataset 本身不增加独立的 Index 物理结构，Dataset Index 实际就是 META。

## create_dataset

```
create_dataset(path, data) -> result
```

创建一个完整 Dataset，是 Dataset 的一次性创建入口。

### 参数

- `path`：Dataset 根目录。
- `data`：拥有数据所有权的完整表数据，包含 Schema 与实际列值。
- `data` 必须包含 `sym` 与 `time` 两列。
- 输入数据必须按 `(sym ASC, time ASC)` 排序。
- 不要求不同 `sym` 具有相同的 `time` 集合。

`create_dataset` 从同一份输入 `Data` 同时建立 META、Field 文件以及 Dataset Schema，三者必须保持一致。

创建过程使用临时目录完成；全部成功后原子 rename 到目标路径。任何一步失败都不会留下不完整 Dataset，目标路径保持不变。

```
create_dataset(path, data)
        │
        ├── sym / time
        │      ↓
        │    META
        │
        └── fields
               ↓
          Field files
```

## create_dataset_index

```
create_dataset_index(path, data) -> result
```

创建 Dataset 的 Index 文件，即 `.meta`。

Dataset Index 没有独立于 META 的物理结构；`.meta` 本身就是 Dataset Index。该 API 是 Dataset 层对 META 创建能力的封装。

- `path`：Dataset 根目录。
- `data`：用于生成 META 的完整数据。
- 只负责创建 `.meta`，不创建 Field 文件。
- Dataset 的 sym/time 索引信息由 META 管理。

```
create_dataset_index(path, data)
          │
          └──→ create_meta_file(path/.meta, data)
```

## open_dataset

```
open_dataset(path, mode) -> Dataset
```

打开已有 Dataset 并返回 Dataset 对象。

- `mode` 表示访问意图，例如 `read` / `write`。
- `mode` 不是压缩状态。
- 打开时只打开 META，不预先打开任何 Field Handle。
- Dataset Schema 从 META 与 Field 元信息得到。
- Field 只有在实际读取或写入时才按需打开。

打开完成后，调用方可以直接从返回的 Dataset 对象获取 Schema；不提供额外的 `get_dataset_schema()` API。

```
open_dataset(path, mode)
        │
        ├── open META
        │
        ├── inspect Field metadata
        │      ↓
        │    Dataset Schema
        │
        └── return Dataset
```

首次实际打开某个 Field 时，再校验 Field 与 Dataset 的 generation / Schema 一致性。

## delete_dataset

```
delete_dataset(path) -> result
```

删除完整 Dataset。

删除对象是整个 Dataset 根目录，而不是单独删除 META 或 Field；内部递归删除 Dataset 目录。

## create_dataset_fields

```

```

在已有 Dataset 中新增一个 Field。

这是 Dataset 对外暴露的单 Field 新增入口，适用于只需要增加一个字段的场景。

- `path`：Dataset 根目录。
- `field`：待创建的 Field 数据及其字段信息。
- Field 名称不能与 Dataset 中已有 Field 重复。
- Field 的类型来自其字段定义 / `ColumnView` 的 `DataType`。
- Field 数据长度必须与 Dataset 当前逻辑长度一致。
- `sym` / `time` 不作为普通 Field 创建；Dataset 的 `sym` / `time` 身份仍由 META 管理。
- 新增 Field 不改变 Dataset 的 `sym` / `time` 范围。
- 新增 Field 后，Dataset Schema 同步增加该字段。
- Dataset 层负责将 Field 名称解析为 Dataset 内的实际 Field 文件路径；Field 文件的物理创建由 Field 层负责。

该 API 与 `create_dataset_fields()` 的区别仅在于新增数量：

```
create_dataset_fields(path, field)
                 │
                 ▼
              Field A
                 │
                 ▼
             Field file
```

## create_dataset_fields

```
create_dataset_fields(path, data) -> result
```

在已有 Dataset 中一次新增一个或多个 Field。

这是批量 Field 新增入口，输入使用 `DataView`。当需要同时增加多个字段时使用该 API；单个字段仍可使用 `create_dataset_fields()`。

- `path`：Dataset 根目录。
- `data`：包含待创建 Field 的 `DataView`。
- `DataView` 可以包含一个或多个 Field；每个 Field 对应创建一个 Field 文件。
- `DataView` 中所有列的长度必须与 Dataset 当前逻辑长度一致。
- 输入 Field 名称不能与 Dataset 中已有 Field 重复，且待创建字段之间也不能重名。
- 输入 Field 类型直接取自对应 `ColumnView` 的 `DataType`。
- `sym` / `time` 不作为待创建的普通 Field；Dataset 的 `sym` / `time` 身份仍由 META 管理。
- 新增 Field 不改变 Dataset 的 `sym` / `time` 范围。
- 新增 Field 后，Dataset Schema 同步增加对应字段。
- Dataset 层负责将 Field 名称解析为 Dataset 内的实际 Field 文件路径；Field 文件格式和物理创建由 Field 层负责。

```
create_dataset_fields(path, data)
                  │
                  ▼
               DataView
             ┌────┼────┐
             ▼    ▼    ▼
          Field A Field B Field C
             │    │      │
             ▼    ▼      ▼
          Field files
```

`create_dataset_fields()` 可以视为 `create_dataset_fields()` 的单字段形式；两者对 Dataset Schema 和 Dataset `sym` / `time` 身份的影响一致。

## 

```
(path, field) -> result
```

删除已有 Dataset 中指定的 Field。

这是 Dataset 对外暴露的删除 Field 入口，内部复用 Field 层的 `delete_field_file()`。

- `path`：Dataset 根目录。
- `field`：要删除的 Field 名称。
- `sym` / `time` 与 META 不受影响。
- 删除后该 Field 不再属于 Dataset Schema。

## cast_dataset_field

```

```

改变已有 Dataset Field 的数据类型。

这是 Dataset 对外暴露的 Field 类型转换入口，内部复用 Field 层的 `cast_field_file()`。

- `path`：Dataset 根目录。
- `field`：要转换的 Field 名称。
- `target_type`：目标数据类型。
- 转换只作用于指定 Field，不改变 Dataset 的 `sym` / `time` 范围。
- 转换完成后 Dataset Schema 中该 Field 的类型随之改变。

## Dataset 结构变化的边界

Dataset 对外提供两类结构操作：

1. **Field 级结构变化**：直接作用于已有 Dataset。
2. **Dataset 级整体变化**：改变 Dataset 的整体物理布局，需要重建。

```
Field 级变化
├── create_dataset_fields()
├── create_dataset_fields()
├── ()
└── cast_dataset_field()
        ↓
    单个 / 多个 Field 文件操作

Dataset 级变化
├── 扩大容量
├── 新增 sym
└── 扩大 time 范围
        ↓
    重建 Dataset
```

扩大 Dataset 容量、新增 `sym` 或扩大 `time` 范围都会改变 Dataset 整体的物理布局以及 META / Field 的对应关系，因此不作为原地 Dataset API 提供。

## read_dataset_schema

```
read_dataset_schema(dataset) -> Schema
```

读取当前 Dataset 的逻辑 Schema。

- 返回 Dataset 当前全部逻辑字段及其 `DataType`。
- 用于查询引擎进行字段解析、类型映射和查询规划。
- 不触发 Field 数据扫描。
- 不返回物理 encoding、compression、offset 等属性。
- 返回对象属于只读元数据视图，其生命周期不能超过 Dataset。

```
Schema
├── fields[]
│   ├── name
│   └── data_type
```

## read_dataset_statistics

```
read_dataset_statistics(dataset) -> DatasetStatistics
```

读取 Dataset 级统计信息，用于查询优化、范围检查、分区裁剪和成本估算。

统计信息来自 META，不扫描 Field 数据。

```
DatasetStatistics
├── row_count
├── sym_count
├── sym_min
├── sym_max
├── time_count
├── time_min
└── time_max
```

### 语义

- `row_count`：Dataset 逻辑行数。
- `sym_count`：META 中的 symbol 数量。
- `sym_min` / `sym_max`：symbol dictionary 的首尾 symbol。
- `time_count`：META TIME AXIS 中的时间数量。
- `time_min` / `time_max`：TIME AXIS 的首尾时间。
- 目前只提供 Dataset 级统计，不提供 Field min/max/null_count。
- 统计信息只读，不修改 Dataset。

## read_dataset

```
read_dataset(dataset, offset, length, columns?) -> DataView
```

按 Dataset 的逻辑 row range 读取多列数据。

### 参数

- `dataset`：已打开的 Dataset。
- `offset`：逻辑起始行。
- `length`：逻辑读取长度。
- `columns`：可选的 Field 名称集合。

### 语义

- `offset` / `length` 是 Dataset 的**逻辑行范围**，不是 Field 的物理 offset。
- 默认返回 `sym` 与 `time`。
- 其他 Field 由 `columns` 指定；不需要重复指定 `sym` / `time`。
- 返回 `DataView`，包含多列 `ColumnView`。
- META 将逻辑 row range 映射为各 Field 对应的物理 row ranges。
- 一个逻辑读取范围可以跨越多个 `sym`。
- 所需 Field 按需打开。
- values / validity 在可以保持零拷贝时保持零拷贝。
- 返回的 `DataView` 不拥有底层数据，其生命周期不能超过相关 Dataset / Field 资源。

META 本身不存在 NULL logical position。不同 `sym` 的实际时间集合不同导致的缺失位置，属于 Field 物理容量中的无效位置，并不构成 META 的逻辑行。

```
read_dataset(dataset, offset, length, columns?)
        │
        ├── META → sym / time + physical ranges
        │
        ├── Field A → ColumnView
        ├── Field B → ColumnView
        │
        └── assemble → DataView
```

## write_dataset

```
write_dataset(dataset, offset, data) -> result
```

对 Dataset 已有逻辑行进行 positional overwrite。

### 语义

- 只覆盖已有 Dataset 数据区域。
- 不改变 META layout。
- 不改变 Dataset 逻辑长度。
- 不改变 `sym` / `time` 的逻辑身份。
- 不作为追加接口使用。
- 不负责 Dataset 重建。
- 要求 `offset + data.length <= dataset.length`。
- 支持 projection write：输入 `DataView` 可以只包含 Dataset Schema 的部分字段。
- 输入字段必须属于 Dataset Schema，且类型兼容。
- 未提供的字段保持原值不变。
- 所有输入 `ColumnView` 必须具有相同长度。
- `length == 0` 是合法 no-op。
- `sym` / `time` 由 META 管理，不作为写入列。
- values 与 validity 一起写入。
- `validity == null` 表示本次写入范围全部有效。
- NULL 的物理 value 可以保留，但 validity 为无效时逻辑值为 NULL。

输入 `DataView` 的列顺序与 Dataset 的逻辑 row range 对应；Dataset 根据 Schema 将每一列定位到对应 Field，并调用 Field positional write。

```
write_dataset(dataset, offset, data)
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
       Field A   Field B   Field C
          │         │         │
          ▼         ▼         ▼
       positional overwrite
```

### 原子性

`write_dataset` 不保证跨 Field 原子性。

多个 Field 独立写入；如果部分 Field 已经写入成功而后续 Field 写入失败，已经成功写入的 Field 不回滚，Dataset 可能处于部分更新状态。Dataset API 不提供跨 Field transaction / rollback。

## scan_dataset

```
scan_dataset(dataset, request) -> Scanner
```

根据条件扫描 Dataset，并返回满足条件的逻辑 / 物理 row ranges，供上层继续调用 `read_dataset()` 获取数据。

`scan_dataset()` 本身不直接返回数组数据；Scanner 负责产生可读取的行范围。

### ScanRequest

```
ScanRequest
├── ranges
├── projection
├── predicate
├── limit
└── batch_size
```

- `ranges`：待扫描的逻辑 row ranges；为空时表示整个 Dataset。
- `projection`：需要读取的字段集合。
- `predicate`：过滤条件。
- `limit`：最多返回的逻辑行数；无值表示不限制。
- `batch_size`：Scanner 每次产生的范围 / 批次大小。

### 扫描路径

```
.meta
  │
  ▼
scan_index
  │
  ▼
row ranges
  │
  ├──────────────┐
  ▼              ▼
price.field   volume.field
  │              │
  ▼              ▼
ColumnView    ColumnView
  └──────┬───────┘
         ▼
      DataView
```

多个 predicate 可以逐步缩小待读取范围；predicate 的执行顺序由上层 planner / executor 决定，Dataset API 不规定具体优化顺序。

## close_dataset

```
close_dataset(dataset) -> result
```

关闭 Dataset 并释放 Dataset 持有的资源。

- 关闭 META Handle。
- 释放 Dataset 层维护的 Field Handle / 元数据资源。
- 已返回的非拥有型 `DataView` / `ColumnView` 在其依赖资源关闭后不再保证有效。
- Dataset 关闭后不能继续进行 Dataset 读写或扫描操作。

## Dataset 与下层 API 的关系

Dataset API 是对 META / Field 文件操作的组合与语义封装，而不是新的物理存储格式。

```
                 Dataset API
                      │
     ┌────────────────┼────────────────┐
     ▼                ▼                ▼
    META            Field A          Field B ...
     │                │                │
     ▼                ▼                ▼
sym/time index    Field API        Field API
```

Dataset 层负责：

- Dataset 目录组织。
- `sym` / `time` 与逻辑 row range 的关系。
- Dataset Schema。
- Dataset 级统计信息。
- Field 名称到实际 Field 文件的解析。
- Dataset 逻辑行到 Field 物理范围的映射。
- 多 Field 读写与扫描的组合。

META / Field 层负责各自文件的物理格式、Buffer / ColumnView 读写以及压缩等底层实现。

## Scanner

Dataset Scanner 的职责是将 META 与多个 Field 的扫描结果进行组合，得到最终可读取的 **Dataset 逻辑 RowRange**。

```
DatasetScanner
     │
     ├── META scan
     ├── Field A scan
     ├── Field B scan
     └── ...
          │
          ▼
      求交 / 裁剪 / 合并
          │
          ▼
   Dataset logical RowRanges
```

API：

```
scan_dataset(dataset, request) -> DatasetScanner

DatasetScanner
├── next() -> RowRange?
└── close()
```

- `next()` 每次返回一个连续的 `RowRange`；扫描结束返回 `null`。
- `RowRange` 是 Dataset 的**逻辑行范围**，不是某个 Field 的物理文件范围。
- DatasetScanner 的输出已经综合 META 和参与条件判断的多个 Field 的扫描结果。
- Scanner 只负责定位行范围，不直接读取数据；实际数据由 `read_dataset()` 根据 RowRange 读取。
- Scanner 本身不承担 batch 化语义；如需批量消费，由上层负责收集多个 `RowRange`。

关系：

```
scan_dataset()
      ↓
DatasetScanner
      ↓ next()
 RowRange
      ↓
read_dataset(dataset, offset, length, columns?)
      ↓
 DataView
```