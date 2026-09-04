# 🧱 splayed-core：META API

META 是 splayed-core 中承载元数据与索引能力的只读结构化文件。它不仅描述 META 自身的结构，还承担 SYM / TIME → FIELD row range 的 Index 职责。本文只讨论 META 的 public API、参数、返回值、生命周期与职责边界。

## API 总览

```
// File
create_meta_file(path, data)
open_meta_file(path, mode)
delete_meta_file(path)

// Handle
read_meta_handle()
read_index_handle()
scan_index_handle()
close_meta_handle()
```

| 接口 | 职责 | 类型 |
| --- | --- | --- |
| `create_meta_file` | 根据按 `(sym, time)` 排序的两列逻辑数据创建新的 META 文件 | File |
| `open_meta_file` | 打开已有 META 文件并返回可复用 Handle | File → Handle |
| `delete_meta_file` | 删除已有 META 的完整物理文件 | File |
| `read_meta_handle` | 读取 META 的结构化信息 | Handle |
| `read_index_handle` | 按逻辑位置读取 Index 中的 SYM / TIME 数据 | Handle |
| `scan_index_handle` | 根据 Index predicate 产生对应的 FIELD row ranges | Handle |
| `close_meta_handle` | 关闭 Handle 并释放资源 | Handle |

## 命名规则

统一采用：

```
action_object_handle
action_object_file
```

- `*_file`：直接操作物理文件，不依赖已经打开的 Handle。
- `*_handle`：操作已经打开的 Handle。

META 是只读结构，因此不提供 `write_meta_handle`、`update_meta_handle`、`compress_meta_file` 或 `decompress_meta_file`。

## META 文件职责

META 同时承担两个层次的职责：

```
META
├── Meta
│   └── 描述 META 文件本身的结构化信息
│
└── Index
    └── SYM / TIME → FIELD row range
```

其中 Index 是 META 对外最重要的查询能力：根据 SYM / TIME 条件定位实际 Field 数据所在的 row range。

## META 文件结构

META 文件采用固定结构布局，Header 为固定 64 bytes，之后依次包含 TIME AXIS、SYM Dictionary、SYM String Data 和 SYM INDEX。

```
HEADER
  │
  ├── magic
  ├── version
  ├── flags
  ├── time_type
  ├── generation
  ├── time_count
  ├── sym_count
  ├── sym_dict_offset
  ├── sym_index_offset
  └── file_size

DATA REGION
  │
  ├── TIME AXIS
  ├── SYM DICT INDEX
  ├── SYM STRING DATA
  └── SYM INDEX
```

### SYM INDEX

每个 SYM 对应一个固定 12 bytes 的记录：

```
SYM INDEX RECORD
├── time_start   uint32
├── time_count   uint32
└── row_start    uint32
```

`time_count` 同时表示：

- 该 SYM 在全局 TIME AXIS 中的连续区间长度；
- 该 SYM 在 Field 中预分配的 row capacity。

缺失的时间点通过 Field 中的特殊 NULL 值表示。

对于位于 SYM 时间区间内的 TIME：

```
time_index = TIME 在全局 TIME AXIS 中的 index
row = row_start + (time_index - time_start)
```

因此 META 可以将 SYM / TIME 条件直接转换成 Field row range。

## open_meta_file

```
open_meta_file(path, mode) -> meta_handle
```

其中：

```
mode = read
```

- `path`：已有 META 的物理文件位置。
- META 必须已经存在；`open_meta_file` 不负责创建 META。
- META 对外只读，不提供 write mode。
- core 从 META header 读取并校验其结构信息。
- 后续 Meta / Index 操作都通过返回的 Handle 执行。
- 同一个 Handle 可以被多次操作，直到 close。

## create_meta_file

```
create_meta_file(path, data) -> result
```

- `path`：新 META 文件的物理路径。
- `data`：按 `(sym ASC, time ASC)` 排序的两列逻辑数据，包含 `sym` 与 `time` 的 schema 和实际值。
- 不要求不同 SYM 具有相同的 TIME 集合；每个 SYM 可以拥有自己的 TIME 序列。
- core 从 `data` 中推断 SYM / TIME 的类型及 META 所需的统计与布局信息。
- core 内部负责将逻辑 `(sym, time)` 数据转换为 META 的 TIME AXIS、SYM Dictionary、SYM String Data 和 SYM INDEX。
- 缺失但位于某个 SYM 连续 TIME 区间内的时间点，由对应 Field 的 NULL 表示。
- 创建完成后，META layout 固定，并进入只读状态。

### data 与 Index Read 的关系

`create_meta_file` 的 `data` 与 `read_index_handle` 返回的 Index 数据在**逻辑 schema / 数据语义上保持一致**，都表示 `(sym, time)` 两列数据；区别在于：

- `create_meta_file` 接收创建 META 所需的逻辑输入数据。
- `read_index_handle` 返回从 META 物理布局解码得到的逻辑 view。
- 两者不要求使用完全相同的内存对象或所有权模型。

## read_meta_handle

```
read_meta_handle(meta_handle) -> MetaInfo
```

用于读取 META 本身的结构化信息，而不是执行 Index 查询。

`MetaInfo` 至少包含：

```
MetaInfo
├── version
├── time_type
├── generation
├── time_count
├── sym_count
└── ...
```

- 只读取结构化信息。
- 不修改 META。
- 不负责将 SYM / TIME 条件转换为 Field range。
- 不要求调用方了解 META 的物理布局细节。

## read_index_handle

```
read_index_handle(meta_handle, offset, length) -> view
```

参数语义与 `read_field_handle(field_handle, offset, length)` 保持一致：

- `offset`：Index 逻辑数据空间中的起始位置。
- `length`：读取的逻辑数据数量。
- `offset + length` 不得超过 Index 当前逻辑范围。
- `length = 0` 可以返回空 view。
- 返回 Index 对应的 SYM / TIME 数据 view。

Index 的物理布局不是普通连续 Field；core 内部根据 TIME AXIS、SYM Dictionary 和 SYM INDEX 组装逻辑数据。上层只依赖 `offset / length`，不依赖 META 的内部物理布局。

## scan_index_handle

```
scan_index_handle(meta_handle, request) -> scanner
```

使用与 Field Scan 对齐的 `ScanRequest`：

```
ScanRequest
├── ranges?
├── predicate?
├── limit?
└── batch_size?
```

### ranges

可选的已有 candidate ranges：

- 为空表示不限制扫描范围。
- 非空表示只在给定的 `[start, end)` ranges 内继续扫描。
- ranges 是上游 predicate 已经产生的候选位置集合，而不是要求调用方预先知道最终结果。

### predicate

用于对 META Index 中的 SYM / TIME 数据进行过滤。

例如：

```
sym = "AAPL"
AND time >= t1
AND time < t2
```

Predicate 可以由 DuckDB / DataFusion / Polars 等上层适配层转换为 core 定义的表达式。

### limit

限制最多产生的匹配位置数量；达到 limit 后 Scanner 可以提前结束。

### batch_size

控制 Scanner 每次返回的 range 数量。

### Scanner

```
scanner.next() -> ranges
```

返回满足当前 Index predicate 的 FIELD row ranges，而不是 materialize Field 数据。

例如：

```
[1000, 1250)
[5000, 5300)
```

这些 ranges 可以直接交给：

```
read_field_handle(field_handle, offset, length)
```

或作为下一阶段 `scan_field_handle` 的 `ranges` 输入，继续进行多字段 predicate pushdown。

## 多字段 Predicate Pushdown

META Index Scan 不负责决定多个 Field predicate 的执行顺序。它只负责根据自己的 SYM / TIME predicate 将候选位置缩小为 FIELD row ranges。

典型执行链：

```
SYM / TIME predicate
        │
        ▼
scan_index_handle(meta, request)
        │
        ▼
FIELD row ranges
        │
        ├───────────────┐
        ▼               ▼
scan_field_handle  scan_field_handle
  predicate A        predicate B
        │               │
        └───────┬───────┘
                ▼
          final selection
                │
                ▼
       read_field_handle()
```

`ranges` 因此既可以是 `scan_index_handle` 的输入，也可以是它的输出；具体过滤顺序由上层 query planner / execution engine 决定。

## close_meta_handle

```
close_meta_handle(meta_handle) -> result
```

- close 后 Handle 不再可用于任何 META / Index 操作。
- 释放 fd、mmap、缓存等内部资源。
- META 本身不会因 close 发生任何写回。
- 不提供 `commit` / `flush` / `dump` 等 public Handle API。

## META 更新

META 是 immutable / read-only 文件，不提供 `update_meta()`。

如果 META 内容或 layout 发生变化，直接重新构建新的 META 文件：

```
MetaBuilder
    │
    ▼
create_meta_file()
    │
    ▼
new.meta
```

实际文件替换应避免出现 META 短暂不存在的状态；可先构建临时文件，再通过原子 rename / replace 完成替换。

因此 META public API 不包含：

```
write_meta_handle()
update_meta_handle()
update_meta()
compress_meta_file()
decompress_meta_file()
```

## 与 Field API 的对应关系

```
Field                         META / Index
────────────────────────────────────────────────
open_field_file()             open_meta_file()
read_field_handle()           read_index_handle()
scan_field_handle()           scan_index_handle()
close_field_handle()          close_meta_handle()
```

`read` 的参数统一采用：

```
read_xxx_handle(handle, offset, length)
```

`scan` 统一采用：

```
scan_xxx_handle(handle, request)
```

两者内部实现不同，但 public API 尽量保持一致，使上层执行器可以使用统一的 range / predicate pushdown 模型。

## File API 与 Handle API 的边界

```
File API
├── create_meta_file()
├── open_meta_file(path, mode)
└── delete_meta_file()

Handle API
├── read_meta_handle()
├── read_index_handle()
├── scan_index_handle()
└── close_meta_handle()
```

File API 负责 META 物理文件的创建、打开与删除；Handle API 负责已经打开的 META 上的结构读取与 Index 查询。META 不提供写入和压缩能力。

## Data / DataView / ColumnView 参数统一

Field 与 META 的数据读写统一使用逻辑数据视图：

```
Field
  read  → ColumnView
  write ← ColumnView

META
  read  → DataView
  create ← DataView
```

Field API：

```
read_field_handle(field_handle, offset, length) -> ColumnView
write_field_handle(field_handle, offset, ColumnView) -> result
```

META API：

```
read_index_handle(meta_handle, offset, length) -> DataView
create_meta_file(path, DataView) -> result
```

`scan_*` 仍然返回 `ranges / Scanner`，因为扫描阶段只负责定位，不物化数据。

### `locate_index_handle`

```
locate_index_handle(
    handle,
    data: DataView
) -> RowRanges
```

用途：根据 `DataView` 中成对出现的 `(sym, time)`，批量定位这些联合 key 在 Dataset 中对应的逻辑行范围。

- `data` 至少包含 `sym` 和 `time` 两列，二者按行一一对应，组成联合 key `(sym, time)`。
- 输入数据按 `(sym ASC, time ASC)` 排序。
- META 内部可利用输入 `(sym, time)` 的有序性，与 SYM INDEX / TIME AXIS 进行双指针扫描，避免逐行查找。
- 返回 `RowRanges`，表示这些 `(sym, time)` 对应的 Dataset 行范围；连续命中的行可合并为一个 `RowRange`。
- 这是 Index / META 层的定位原语，不负责读取 Field 数据。
- 主要供上层 `write_table()` 等批量按 `(sym, time)` 定位已有 Dataset 行的场景使用。

定位语义与普通扫描的区别：

```
scan_index_handle()
    条件查询 → RowRanges

locate_index_handle()
    (sym, time) 联合 key → RowRanges
```