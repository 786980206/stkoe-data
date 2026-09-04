# 🧱 splayed-core：Field API

Field 是 splayed-core 面向实际字段数据文件提供的底层存储接口。本文只讨论 Field 的 public API、参数、返回值、生命周期与职责边界。

## API 总览

```
// File
create_field_file()
open_field_file(path, mode)
delete_field_file()
cast_field_file(source, target, target_type)
compress_field_file()
decompress_field_file()

// Handle
read_field_handle()
write_field_handle()
scan_field_handle()
update_field_handle()
close_field_handle()
```

| 接口 | 职责 | 类型 |
| --- | --- | --- |
| `create_field_file` | 创建新的 Field 文件并完成初始化 | File |
| `open_field_file` | 打开已有 Field 文件并返回可复用 Handle | File → Handle |
| `delete_field_file` | 删除已有 Field 的完整物理文件 | File |
| `cast_field_file` | 将 source Field 转换为 target Field 的目标数据类型 | File |
| `compress_field_file` | 将已有 Field 文件压缩为 compressed physical representation | File |
| `decompress_field_file` | 将已有 Field 文件解压为 uncompressed physical representation | File |
| `read_field_handle` | 按物理位置读取 data，返回 view | Handle |
| `write_field_handle` | 按物理位置覆盖写入 data | Handle |
| `scan_field_handle` | 根据 ranges / predicate 产生物理 selection | Handle |
| `update_field_handle` | 修改 Field header，不修改 data | Handle |
| `close_field_handle` | 关闭 Handle 并释放资源；必要时完成压缩写回 | Handle |

## 命名规则

统一采用：

```
action_object_handle
action_object_file
```

- `*_file`：直接操作物理文件，不依赖已经打开的 Handle。
- `*_handle`：操作已经打开的 Handle。

因此，`compress_field_file` / `decompress_field_file` 与 `open_field_file`、`delete_field_file` 属于同一层级的 File API；它们不依赖 `FieldHandle`。

## FieldHandle

`FieldHandle` 是 core 对外的不透明对象，用于管理一次打开的 Field 生命周期。它不是简单的 OS file descriptor wrapper。

内部可以根据实际实现持有：

```
FieldHandle
├── FileHandle / fd
│      └── 原始 Field 文件
├── FieldHeader
├── DataHandle
├── WorkingBuffer / MemoryHandle
│      └── compressed Field 解压后的工作数据
├── mode
│      ├── read
│      └── write
├── state
│      ├── unmodified
│      └── modified
└── resource / lifetime state
```

上述结构是实现模型，不是 public API 契约。调用方不依赖 fd、mmap、working buffer、compression state 等内部字段。

## open_field_file

```
open_field_file(path, mode) -> field_handle
```

其中：

```
mode = read | write
```

- `path`：已有 Field 的物理文件位置。
- Field 必须已经存在；`open_field_file` 不负责创建 Field。
- `mode` 表示上层访问意图，而不是文件的 compression 状态。
- core 从文件自身的 header / physical representation 识别 Field 当前是否 compressed。
- 后续 read / scan / write / update 都通过返回的 Handle 执行。
- 同一个 Handle 可以被多次操作，直到 close。

### mode 语义

`read`：

- 允许读取和扫描。
- 不修改原 Field。
- compressed Field 的解压过程对上层隐藏。

`write`：

- 允许读取、扫描、写入和更新。
- uncompressed Field 可直接原地修改。
- compressed Field 在内部进入可写的 uncompressed working representation。
- 如果发生修改，`close_field_handle` 时自动重新压缩并写回原 Field 文件。

## create_field_file

```
create_field_file(field, type, init) -> result
```

`init` 支持：

```
init
├── length(n)
├── data(buffer)
└── stream(reader)
```

- `field`：指定创建位置 / Field 文件。
- `type`：Field 数据类型，至少由上层指定。
- `length(n)`：创建指定逻辑长度的空占位 Field，由 core 生成占位空间。
- `data(buffer)`：创建并使用给定数据初始化 Field；长度等信息可由 core 从数据推断。
- `stream(reader)`：从流式数据源持续读取并完成初始化；最终长度无需预先知道。
- Header 不要求调用方完整构造；能够从 `field`、`type`、`init` 推断或生成的信息由 core 负责生成。
- 创建完成后，Field 才进入可被 `open_field_file` 打开的状态。

## read_field_handle

```
read_field_handle(field_handle, offset, length) -> view
```

- `field_handle` 必须有效且处于允许 read 的状态。
- `offset`：起始物理位置。
- `length`：读取元素数量。
- `offset + length` 不得超过 Field 当前逻辑 data length。
- `length = 0` 可以返回空 view。
- 返回 zero-copy view；对于 compressed Field，view 可以来自内部解压后的 working representation。
- view 生命周期不能超过其依赖的 Field Handle / 底层资源；Handle close 后不再保证有效。
- read 不提供 `parallel` 参数，并发由上层控制。

## write_field_handle

```
write_field_handle(field_handle, offset, data) -> result
```

`data` 支持：

```
data
├── buffer
└── stream(reader)
```

- Handle 必须由 `open_field_file(path, write)` 获得。
- 从 `offset` 开始覆盖写入。
- 只修改 data，不修改 header。
- 不改变 Field 的逻辑 data length。
- `offset + 实际写入长度` 不得超过现有 data length。
- `stream(reader)` 适合大数据写入，最终长度无需预先知道；core 持续写入并校验最终范围。
- uncompressed Field 可直接修改文件 / 映射。
- compressed Field 修改内部 uncompressed working representation；close 时如发生修改则自动重新压缩并写回。
- 同一 Field 可并发写入非重叠区域；重叠区域不允许。
- 并发度由上层控制，接口不提供 `parallel` 参数。
- 这是定位覆盖写，不承担追加、扩容或改变逻辑长度。

## update_field_handle

```
update_field_handle(field_handle, header) -> result
```

- Handle 必须处于允许写入 / 更新的状态。
- `header` 使用 core 定义的 `FieldHeader`。
- 只修改 header，不修改 data。
- core 负责校验 header 与现有 data 的一致性。
- 不需要 `offset` / `length` / `parallel` 参数。
- compressed Field 的 header 更新属于 write Handle 生命周期；最终物理文件由 close 流程保持一致。

区别：

- `write_field_handle`：修改 data。
- `update_field_handle`：修改 header。

## scan_field_handle

```
scan_field_handle(field_handle, request) -> scanner
```

`ScanRequest`：

```
ScanRequest
├── ranges
├── predicate
├── limit
└── batch_size
```

- `ranges`：多个不连续的 `[start, end)` 物理范围；为空表示扫描整个 Field。
- `predicate`：core 定义的可组合条件表达式，支持 `AND` / `OR` / `NOT` 以及基本比较；DuckDB / DataFusion / Polars 适配层负责转换。
- `limit`：限制扫描得到的匹配位置数量，可提前结束。
- `batch_size`：控制 Scanner 每次产生的 selection / ranges 数量。
- 不支持 `order` 下推。

Scanner：

```
scanner.next() -> ranges
```

- 每次返回一批满足条件的物理位置 ranges。
- 不 materialize Arrow Array。
- 多个不连续 ranges 是正常结果。
- 结果可继续交给 `read_field_handle` 消费。
- 多 Field 可以复用同一组 selection / ranges。

## close_field_handle

```
close_field_handle(field_handle) -> result
```

- close 后 Handle 不再可用于任何 Field 操作。
- 释放 fd、mmap、working memory 等内部资源。
- read handle：不修改原文件。
- uncompressed write handle：此前写入已经直接作用于文件，close 不需要额外压缩。
- compressed write handle：如果发生修改，close 时自动完成内部 compress + rewrite，使原文件继续保持 compressed 状态。
- compressed write handle 如果没有发生修改，则 close 不需要重新压缩或写回。
- 不提供 `commit` / `flush` / `dump` 等额外 public Handle API。

## cast_field_file

```
cast_field_file(source, target, target_type) -> result
```

将 `source` Field 的数据转换为 `target_type`，并写入指定的 `target` Field 文件。

语义：

- `source`：已有的源 Field 文件。
- `target`：目标 Field 文件位置；由调用方指定，不要求 `cast_field_file` 决定文件生命周期之外的上层替换策略。
- `target_type`：目标 Field 数据类型。
- `cast_field_file` 负责读取 source、执行数据类型转换并生成 target 的 Field 数据。
- 不修改 source Field 的数据或类型。
- 不属于 `write_field_handle` 的原地 positional overwrite；类型转换可能改变物理数据表示，因此通过独立的目标 Field 完成。
- source 为 compressed Field 时，压缩状态对调用方透明，由 core 内部完成必要的读取 / 解压 / 转换。
- target 的最终物理 representation 按 Field 创建与写入流程确定。
- 文件创建、替换旧 Field、原子切换等生命周期策略由上层负责。

典型流程：

```
source Field
    │
    │ cast
    ▼
target Field
```

## compress_field_file

```
compress_field_file(field) -> result
```

独立操作已有 Field 物理文件，将其从 uncompressed physical representation 转换为 compressed physical representation。

语义：

- 不依赖已经打开的 `FieldHandle`。
- `field` 指定已有 Field 文件。
- 操作对象是整个 Field 文件的 physical representation。
- 压缩完成后，Field 的逻辑 data 与 header 语义保持不变，只改变物理存储形式。
- 如果 Field 已经是 compressed 状态，应返回明确的状态结果，而不是重复压缩。
- 这是独立的 File API，可用于离线压缩、维护或在不通过 Handle 生命周期的情况下改变文件物理状态。
- 与 `close_field_handle` 内部触发的 compress 不同，它直接以 File API 形式暴露给上层。

## decompress_field_file

```
decompress_field_file(field) -> result
```

独立操作已有 Field 物理文件，将其从 compressed physical representation 转换为 uncompressed physical representation。

语义：

- 不依赖已经打开的 `FieldHandle`。
- `field` 指定已有 Field 文件。
- 操作对象是整个 Field 文件的 physical representation。
- 解压完成后，Field 的逻辑 data 与 header 语义保持不变，只改变物理存储形式。
- 如果 Field 已经是 uncompressed 状态，应返回明确的状态结果，而不是重复解压。
- 这是独立的 File API，可用于离线维护、调试或需要直接获得 uncompressed physical representation 的场景。

## File API 与 Handle API 的边界

```
File API
├── create_field_file()
├── open_field_file(path, mode)
├── delete_field_file()
├── compress_field_file()
└── decompress_field_file()

Handle API
├── read_field_handle()
├── write_field_handle()
├── scan_field_handle()
├── update_field_handle()
└── close_field_handle()
```

File API 负责物理文件级生命周期与物理 representation 转换；Handle API 负责已经打开的 Field 上的数据访问与 header 更新。

## 压缩相关的完整关系

```
             ┌──────────────────────┐
             │   Field File         │
             └──────────┬───────────┘
                        │
          ┌─────────────┴─────────────┐
          │                           │
compress_field_file()       decompress_field_file()
          │                           │
          ▼                           ▼
   compressed file            uncompressed file
          │                           │
          └──────────┬────────────────┘
                     │
             open_field_file()
                     │
                FieldHandle
                     │
         read / scan / write / update
                     │
              close_field_handle()
                     │
          compressed write 时
           内部自动 compress + rewrite
```

### Public / Private 边界

对外 public：

```
compress_field_file()
decompress_field_file()
```

对外不暴露、仅作为 Handle 内部实现：

```
compress_field_handle()
decompress_field_handle()
dump_field_file()
```

这样同时满足两种场景：

1. 正常访问时，上层无需关心 compression，由 Handle 自动处理。
2. 需要独立改变某个 Field 文件物理存储状态时，可以直接调用 File API。

## 与 `.meta` 的关系

`.meta` 不是普通 Field 的简单双列包装，而是数据布局中的固定元数据结构：它保存 `sym` 与 `time` 两个维度，并与物理数据位置建立对应关系。`.meta` 为只读结构，不参与压缩/解压，也不提供写入或更新操作。

## Data 参数与返回值

Field 是单列物理文件，因此数据读写统一使用 `ColumnView`，而不是 `DataView`：

```
read_field_handle(field_handle, offset, length) -> ColumnView
write_field_handle(field_handle, offset, ColumnView) -> result
```

`ColumnView` 为非拥有视图，包含 `type / values / validity / length`。Field 的 values 与可选 validity bitmap 可以直接暴露为 view，从而保持 0-copy；写入时使用 ColumnView 作为数据源，不改变 Field 的逻辑长度和 Header。

其他接口保持原有语义：

- `create_field_file(..., init)`：初始化阶段仍支持 `length(n)`、`data(buffer)`、`stream(reader)`。
- `scan_field_handle(..., ScanRequest) -> Scanner`：扫描只返回 ranges，不返回数据。
- `update_field_handle(..., Header) -> result`：只更新 Header。

## Scanner

Field Scanner 只负责单个 Field 的条件扫描。Field 不负责理解 `sym/time`，其 Scanner 输出的是该 Field 自身对应的**物理行 RowRange**。

API：

```
scan_field_handle(request) -> FieldScanner

FieldScanner
├── next() -> RowRange?
└── close()
```

- `next()` 每次返回一个连续的 `RowRange`；扫描结束返回 `null`。
- 输出的 RowRange 是 **Field 物理存储空间中的行范围**。
- FieldScanner 的结果由 DatasetScanner 在 Dataset 层进行组合、求交和裁剪。
- FieldScanner 只负责定位，不直接返回 `ColumnView`；实际数据由 `read_field_handle()` 读取。

关系：

```
Field predicate
      ↓
FieldScanner
      ↓ next()
physical RowRange
      ↓
read_field_handle(ranges)
      ↓
 ColumnView
```

与 DatasetScanner 的区别：

```
FieldScanner
  单 Field 条件
      ↓
  Field physical RowRange

DatasetScanner
  META + 多 Field 条件
      ↓
  Dataset logical RowRange
```