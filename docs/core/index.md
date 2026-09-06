# splayed-core：API 设计（V2.0 Draft）

本页定义 `splayed-core` 的 public API：每个接口的**职责、参数与返回结构、基本流程、注意事项**。不包含实现代码。

- 磁盘格式定义见 [splayed-format](splayed-format.md)（META / FIELD / 类型 / NULL / 编码压缩）。
- Table 层（多 Dataset 组织）见 [splayed-table](splayed-table.md)。
- core 保持引擎无关：不依赖 Arrow / DataFusion / DuckDB / Polars；列存取以 `ColumnView` / `DataView` 为交换结构，适配层负责与各引擎类型的零拷贝对接。

## 1. 对象与命名规则

核心对象直接围绕实际文件类型设计：

- `.meta`：元数据 + Index（只读）
- `field`：字段数据文件
- `.sub.xxx`：子数据 / 辅助数据（预留，API 后续单独定义）

命名规则统一为：

```
action_object_file      直接操作物理文件，不依赖已打开的 Handle
action_object_handle    操作已打开的 Handle
```

`*_file` 与 `*_handle` 明确区分「物理文件级操作」和「已打开资源上的操作」。

## 2. API 分层

```
File API                              Handle API
├── META:  create / open / delete     ├── read_meta / read_index / scan_index
│                                     │   locate_index / close
├── Field: create / open / delete     ├── read / write / update / scan / close
│         cast / compress / decompress│
└── Dataset（目录级封装，非文件格式）  └── open_dataset -> DatasetHandle
                                          ├── MetaHandle（.meta）
                                          └── FieldHandle × N（按需打开）
```

- META / Field 是文件级原语；Dataset 是目录级语义封装（META + 多 Field 的组合）。
- `FieldHandle` 是对外不透明对象，内部可持有 fd / header / mmap 或解压后的 working representation / mode / 修改状态等。内部结构是实现模型，不是 public 契约。
- Handle 内部的 `compress_field_handle` / `decompress_field_handle` / `dump_field_file` 属于 private 实现，不对外。

## 3. 公共数据结构（参数与返回值）

### 3.1 RowRange

```
RowRange
├── offset: u64
└── length: u64
```

连续行段统一用 `offset / length` 表示，不使用 `[start, end)`。

> 规范说明：草稿中两种表示混用（`[100,3)` 与 `[1000,1250)`），统一为 offset/length——与 SYM INDEX 的 `row_start + time_count`、`write` 的 slice 语义一致。

### 3.2 内存数据视图

`Buffer / BufferView / BitmapView / ColumnView / Schema / DataView / Data` 的定义见 [splayed-format](splayed-format.md) §3——零依赖的公共数据模型，codec 与各适配层共用同一套视图类型。

### 3.3 数据参数统一

```
Field    read / write          → ColumnView（单列）
META     create / read_index   → DataView（sym / time 两列）
Dataset  read / write          → DataView（多列）
```

### 3.4 ScanRequest

```
ScanRequest
├── ranges?: RowRange[]
├── projection?: string[]      // 仅 Dataset 级有效
├── predicate?: Predicate
└── limit?: u64
```

> 规范说明：相对草稿移除 `batch_size`——Scanner 一次 `next()` 只产出一个连续 RowRange，batch 聚合是 Reader 层职责（见 splayed-table 的 `TableReader`）。Field / META 是单列、两列扫描，无 projection；projection 仅 Dataset 级有意义。

### 3.5 Predicate

- core 定义的可组合条件表达式：`AND / OR / NOT` + 基本比较。
- DuckDB / DataFusion / Polars 等适配层负责转换为 core 表达式。
- predicate 在 ColumnView 的各 segment 内求值（段内 SIMD），段间结果按逻辑行序合并。
- 多字段 predicate 的执行顺序由上层 query planner 决定；core 只保证任一顺序下结果一致。

### 3.6 Scanner / Reader 契约

```
next()   -> Result<T?, Error>    // None = 正常结束；Err = 执行错误
close()  -> Result<()>
```

- `close()` 在正常结束、LIMIT 提前结束、错误、上层取消后都必须可安全调用。
- 该契约对 `FieldScanner / IndexScanner / DatasetScanner / TableScanner / TableReader` 统一适用。

### 3.7 mode

`read | write`。表示访问意图，不是文件的 compression 状态。

### 3.8 Handle 对象

Handle 是不透明的运行时对象，生命周期与内部状态由 core 管理。以下为实现模型，不是 public 契约；调用方不依赖内部字段。

```
FieldHandle
├── fd / mmap                     // 原始 Field 文件
├── header: FieldHeader           // header 明文缓存（字段定义见 splayed-format §8）
├── chunk_rows: Vec<u32>?         // compressed：打开时从 chunk 头读得的分组
├── working: values + validity    // compressed write：解压后的工作数据
├── mode: read | write
├── state: unmodified | modified
└── 资源 / 生命周期

MetaHandle
├── fd / mmap                     // .meta
├── header 缓存                   // version / time_type / generation / time_count / sym_count / row_count（splayed-format §7）
└── 资源 / 生命周期                // 只读对象，无修改状态

DatasetHandle
├── meta: MetaHandle              // 常驻
├── schema: Schema                // open 时由 META + Field 元信息得到
├── fields: FieldHandle 缓存      // 按需打开与复用
└── mode / 资源 / 生命周期
```

- `TableHandle` 属于 splayed-table 层，定义见 splayed-table §3.1。
- `FieldHeader` / `MetaInfo` 等结构的字段以 splayed-format §7 / §8 的 header 定义为准，core 不重复定义。

## 4. 公共语义：逻辑行空间

Dataset / META / Field 共享同一套**容量网格**逻辑行空间：

```
L = Σ_sym time_count(sym)                    // 逻辑总行数 = 每个 FIELD 的 row_count
row(sym_i, time_index) = row_start(i) + (time_index - time_start(i))
```

- 三层 API 的 offset / length 含义一致：META Index 的逻辑行、Dataset 逻辑行、Field 物理行一一对应，**无需换算**。
- sym 区间内缺失的时间点**仍然是逻辑行**（Field 预分配容量的一部分），值为 NULL（validity = 0）。
- 「实际有效行数」通过 validity / `null_count` 表达，不改变逻辑长度。

> 规范说明：草稿中「缺失位置不构成 META 的逻辑行」与「META 将逻辑 row range 映射为各 Field 物理 range」相互矛盾；且 META 只保存 sym 区间，无法得知区间内哪些时间点真实存在。故统一采用容量网格语义（零拷贝路径最短）。
