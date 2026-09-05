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

> 规范说明：草稿中「缺失位置不构成 META 的逻辑行」与「META 将逻辑 row range 映射为各 Field 物理 range」相互矛盾；且 META 只保存 sym 区间，无法得知区间内哪些时间点真实存在。故统一采用容量网格语义（与 V1.0 一致，零拷贝路径最短）。

## 5. Field API

### 5.0 总览

| 接口 | 职责 | 层 |
| --- | --- | --- |
| `create_field_file` | 创建 Field 文件并初始化 | File |
| `open_field_file` | 打开已有 Field，返回 `FieldHandle` | File |
| `delete_field_file` | 删除 Field 物理文件 | File |
| `rename_field_file` | 重命名 Field 文件 | File |
| `cast_field_file` | 将 Field 原地转换为 `target_type` | File |
| `compress_field_file` | uncompressed → compressed 物理表示 | File |
| `decompress_field_file` | compressed → uncompressed 物理表示 | File |
| `read_field_handle` | 按逻辑行读取，返回 `ColumnView` | Handle |
| `write_field_handle` | 按逻辑行覆盖写入 | Handle |
| `update_field_handle` | 修改 FieldHeader（不改 data） | Handle |
| `scan_field_handle` | 条件扫描 → `FieldScanner` | Handle |
| `close_field_handle` | 关闭 Handle；compressed write 收尾 | Handle |

### 5.1 create_field_file

```
create_field_file(path, data_type, init) -> Result<()>
init = length(n) | data(ColumnView) | stream(reader)
```

职责：创建并初始化一个 Field 文件。

- `length(n)`：创建指定逻辑长度的空占位 Field（全 NULL，validity 全 0）。
- `data(ColumnView)`：以给定数据初始化；长度由数据推断。
- `stream(reader)`：从流式数据源持续读取初始化；最终长度无需预先知道。
- header 不要求调用方完整构造；可从 `path / data_type / init` 推断的信息由 core 生成。

流程（按 `init` 分派，分配与写值一步完成，不做先预分配再写值的二次写入）：

- `length(n)`：写 header（`row_count = n`）→ 预分配 DATA（NULL）+ VALIDITY（全 0 位）。
- `data(ColumnView)`：`row_count` = 数据长度；header + values + validity 一次性顺序写出；数据全有效时不写 validity 区（`has_validity = 0`）。
- `stream(reader)`：写 header → 流式追加 values + validity → 结束时回填 `row_count` / `data_length` / `null_count`。

创建完成前 fsync，配合上层（Dataset / Table）的临时文件 + 原子 rename。

注意事项：

- 创建完成后才可被 `open_field_file` 打开；create 不返回 Handle。
- 带数据初始化时写真实 `null_count`。

### 5.2 open_field_file

```
open_field_file(path, mode) -> Result<FieldHandle>
```

- Field 必须已存在；open 不负责创建。
- 打开时校验 magic / version / generation。
- `read`：允许 read / scan；不修改原文件；compressed Field 的解压对上层隐藏。
- `write`：允许 read / scan / write / update。uncompressed Field 直接原地修改；compressed Field 内部进入解压后的 working representation，发生修改后由 close 自动重压缩写回。

### 5.3 rename_field_file

```
rename_field_file(path, new_name) -> Result<()>
```

- 同目录内重命名 Field 文件；文件名即字段名（沿用 Dataset 层的名称解析约定）。
- 原子完成；`new_name` 对应文件已存在时 Error，不覆盖。
- 只改文件名，不修改数据、header、generation。
- 字段名合法性与重复检查由上层负责。

### 5.4 read_field_handle

```
read_field_handle(handle, offset, length) -> Result<ColumnView>
```

- `offset / length` 为逻辑行（= 物理行）；`offset + length ≤ row_count`；`length = 0` 返回空 view。
- 返回 zero-copy ColumnView：`PLAIN + NONE` 为单段 mmap 切片；compressed Field 逐 chunk 物化，跨 chunk 的读取返回多段。
- view 生命周期不能超过 Handle / 底层资源；close 后失效。
- 不提供 `parallel` 参数，并发由上层控制。

### 5.5 write_field_handle

```
write_field_handle(handle, offset, data: ColumnView) -> Result<()>
```

职责：positional overwrite，按逻辑行覆盖写入。

- 需要 write mode；从 `offset` 起覆盖写入。
- values + validity 成对写入；`data` 含多个 segment 时按逻辑行序逐段写入，segment 的 `validity = null` 表示该段全部有效。
- 只改 data，不改 header；不改变逻辑长度；`offset + data.length ≤ row_count`。
- 这是覆盖写，不是追加 / 扩容接口。
- compressed Field 修改内部 working representation，close 时统一收尾。
- 成功后递增 `FIELD.generation`。
- 允许并发写非重叠区域；重叠区域不允许；并发度由上层控制。

> 规范说明：数据参数统一为 `ColumnView`（草稿中 buffer/stream 与 ColumnView 混用）。写路径长度有界，流式大数据 = 分块多次调用；`stream` 仅保留在 create 的 `init` 中（最终长度未知的场景）。

### 5.6 update_field_handle

```
update_field_handle(handle, header: FieldHeader) -> Result<()>
```

- 只修改 header，不修改 data；core 校验 header 与现有 data 的一致性（`row_count`、`data_type` 等）。
- 与 `write_field_handle` 的区别：write 改 data，update 改 header。
- compressed Field 的 header 更新随 close 流程保持文件一致。

### 5.7 scan_field_handle

```
scan_field_handle(handle, request) -> Result<FieldScanner>
FieldScanner::next() -> Result<RowRange?>
```

- `ranges`：候选物理范围（空 = 整个 Field）；`predicate` 在本 Field 的值上求值；`limit` 达到后提前结束。
- `next()` 每次返回一个连续 RowRange；扫描结束返回 None。
- 只定位，不物化数据；输出可交给 `read_field_handle`，或作为其他 Field scan 的 `ranges` 输入做多字段下推。
- Field 不理解 sym / time；只做值过滤。不支持 order 下推。

### 5.8 close_field_handle

```
close_field_handle(handle) -> Result<()>
```

- close 后 Handle 不可再用；释放 fd / mmap / working memory。
- read handle：无写回。
- uncompressed write handle：写入已直接生效，无需额外动作。
- compressed write handle：发生修改 → 自动 compress + rewrite，文件保持 compressed；重压缩沿用文件既有 chunk 分组（打开时从 chunk 头读得，自描述，不依赖 META；写路径不改 `row_count`，`Σ rows == row_count` 恒成立，分组可精确复用），写临时文件后原子替换；未修改 → 不写回。
- 不提供 `commit / flush / dump` public API。

### 5.9 cast_field_file

```
cast_field_file(path, target_type) -> Result<()>
```

- 读取 `path` 处 Field → 数据类型转换 → 写临时文件 → 原子 rename 替换原文件；对外表现为原地转换。
- 转换成功后该 Field 的 `data_type` 为 `target_type`，逻辑数据逐行完成类型转换。
- 转换失败时原文件保持不变。
- 原文件的压缩状态对调用方透明；转换通过临时文件完成，不属于 `write_field_handle` 的原地覆盖。

### 5.10 compress_field_file / decompress_field_file

```
compress_field_file(path, offsets?) -> Result<()>
decompress_field_file(path) -> Result<()>
```

- File 级物理表示转换；不依赖已打开 Handle；逻辑数据与 header 语义不变。
- `offsets`：可选的 chunk 起始行号，升序、`offsets[0] == 0`，隐含最后一块延伸到 `row_count`；省略时按固定 8192 行均匀分块（最后一块允许不足）。
- 分块策略是调用方的职责：Dataset 层按 META 网格生成 sym 对齐边界（见 7.7），裸调用可省略 `offsets`。
- 状态不符时返回明确错误（如 AlreadyCompressed / NotCompressed），不做静默 no-op。
- 与 close 的自动压缩互补：一个面向离线维护，一个面向写生命周期。

## 6. META API

### 6.0 总览

| 接口 | 职责 | 层 |
| --- | --- | --- |
| `create_meta_file` | 由 (sym, time) 两列数据构建 META | File |
| `open_meta_file` | 打开已有 META（mode = read） | File |
| `delete_meta_file` | 删除 META 物理文件 | File |
| `read_meta_handle` | 读取 META 结构化信息 | Handle |
| `read_index_handle` | 按逻辑行读取 (sym, time) 数据 | Handle |
| `scan_index_handle` | SYM / TIME 条件 → FIELD row ranges | Handle |
| `locate_index_handle` | (sym, time) 联合键批量定位 | Handle |
| `close_meta_handle` | 关闭 Handle | Handle |

META 是 immutable / read-only 文件：不提供 `write / update / compress / decompress`。

### 6.1 create_meta_file

```
create_meta_file(path, data: DataView) -> Result<()>
```

- `data`：按 `(sym ASC, time ASC)` 排序的 sym / time 两列逻辑数据。
- 不要求不同 SYM 具有相同 TIME 集合；每个 SYM 可以有自己的 TIME 序列。
- core 从数据推断 `time_type` 等信息，并构建 TIME AXIS / SYM DICT / SYM STRING DATA / SYM INDEX。
- 区间内缺失的时间点由对应 Field 的 NULL 表示。
- 完成后 layout 固定、进入只读状态。

### 6.2 open_meta_file

```
open_meta_file(path, mode) -> Result<MetaHandle>
```

- META 必须已存在；`mode = read`（无 write mode）。
- 打开时校验 header；后续所有 META / Index 操作经返回的 Handle 执行。

### 6.3 read_meta_handle

```
read_meta_handle(handle) -> MetaInfo
MetaInfo { version, time_type, generation, time_count, sym_count, row_count }
```

只读取结构化信息；不做 Index 定位；调用方无需了解物理布局。

### 6.4 read_index_handle

```
read_index_handle(handle, offset, length) -> Result<DataView>
```

- `offset / length` 是 Index 逻辑行空间（容量网格，§4）中的位置；`offset + length ≤ L`；`length = 0` 返回空 view。
- 返回该逻辑行段的 `(sym, time)` 两列 view：sym 以字典视图返回（指向 SYM DICT / STRING DATA），time 直接指向 TIME AXIS——两者零拷贝。
- 物理布局（TIME AXIS / DICT / INDEX 交错）由 core 内部组装，上层只见逻辑两列。

### 6.5 scan_index_handle

```
scan_index_handle(handle, request) -> Result<IndexScanner>
IndexScanner::next() -> Result<RowRange?>
```

- `predicate` 作用于 sym / time（如 `sym = "AAPL" AND time >= t1 AND time < t2`）。
- `ranges`：上游候选范围输入（空 = 不限制）；`limit` 达到后提前结束。
- 输出满足条件的 FIELD row ranges（逻辑 = 物理），可直接交给 `read_field_handle`，或作为 `scan_field_handle` 的 `ranges` 输入做多字段 predicate pushdown。

典型执行链：

```
SYM/TIME predicate → scan_index_handle → RowRanges
    → scan_field_handle (predicate A) → 更小 ranges
    → scan_field_handle (predicate B) → final ranges
    → read_field_handle / read_dataset
```

META / Field scan 不负责决定多 Field predicate 的执行顺序；顺序由上层 planner 决定。

### 6.6 locate_index_handle

```
locate_index_handle(handle, data: DataView) -> Result<RowRange[]>
```

- `data` 至少包含 sym / time 两列，按行一一配对组成联合键 `(sym, time)`；输入按 `(sym ASC, time ASC)` 排序。
- META 利用输入有序性 × SYM INDEX / TIME AXIS 有序性做双指针扫描，避免逐行查找。
- 返回与输入分段对应的 `RowRange[]`：每个 RowRange 对应一段连续输入与 Dataset 中一段连续逻辑行。
- 定位成功的充要条件：`sum(RowRange.length) == data.length`（key 全部存在且无重复）。
- 供 `write_table` 等批量「按 key 定位已有行」的场景；不读取 Field 数据。

与 `scan_index_handle` 的区别：

```
scan_index_handle    条件查询 → RowRanges
locate_index_handle  (sym, time) 联合键 → RowRanges
```

### 6.7 close_meta_handle

close 后 Handle 不可再用；释放 fd / mmap 等资源；META 无任何写回。

### 6.8 META 更新策略

META immutable，无 `update_meta()`。内容或 layout 变化（新增 sym、扩大 time 范围、布局重排）时由 `MetaBuilder` 构建新文件：写临时文件 → 原子 rename 替换旧 `.meta`；替换原子完成，不出现 META 短暂不存在的状态。

## 7. Dataset API

### 7.1 结构与对象

```
Dataset（目录）
├── .meta                     // Index：sym/time → 逻辑行
└── <name>.field × N          // 字段数据文件
```

- `open_dataset` 只打开 META；Dataset Schema 从 META 与 Field 元信息得到。
- Field Handle 按需打开；首次访问某个 Field 时校验其与 Dataset 的 generation / Schema 一致性。
- Field 名称 ↔ 文件名的映射由 Dataset 层定义（沿用 V1.0 的字段名文件名约定）。
- Dataset 不引入独立 Schema 文件、独立 Index 文件——Dataset Index 就是 `.meta`。

### 7.2 总览

| 接口 | 职责 | 层 |
| --- | --- | --- |
| `create_dataset` | 创建完整 Dataset（META + Fields） | File |
| `create_dataset_index` | 创建 / 重建 `.meta` | File |
| `open_dataset` | 打开 Dataset，返回 `DatasetHandle` | File |
| `delete_dataset` | 删除完整 Dataset 目录 | File |
| `create_dataset_field` | 新增单个 Field | Dataset |
| `create_dataset_fields` | 批量新增 Field | Dataset |
| `delete_dataset_field` | 删除指定 Field | Dataset |
| `rename_dataset_field` | 重命名指定 Field | Dataset |
| `cast_dataset_field` | 转换指定 Field 类型 | Dataset |
| `compress_dataset_field` / `decompress_dataset_field` | 压缩 / 解压指定 Field | Dataset |
| `read_dataset_schema` | 读取逻辑 Schema | Dataset |
| `read_dataset_statistics` | 读取 Dataset 级统计 | Dataset |
| `read_dataset` | 按逻辑行读取多列 | Dataset |
| `write_dataset` | 按逻辑行覆盖写入 | Dataset |
| `scan_dataset` | 条件扫描 → `DatasetScanner` | Dataset |
| `locate_dataset_index` | (sym, time) 联合键批量定位（转发 META） | Dataset |
| `close_dataset` | 关闭并释放资源 | Dataset |

> 规范说明：草稿中单字段入口与多字段入口重名（两处 `create_dataset_fields`），规范为 `create_dataset_field`（单）/ `create_dataset_fields`（批量）；`delete / cast / compress / decompress_dataset_field` 在草稿中签名缺失，此处补齐。新增 `locate_dataset_index` 作为 META `locate_index_handle` 的 Dataset 级封装，使 splayed-table 只依赖 Dataset API、不持有 MetaHandle。

### 7.3 create_dataset

```
create_dataset(path, data: Data) -> Result<()>
```

- `data`：拥有数据所有权的完整表数据（Schema + 全部列值）。
- 必须包含 `sym` 与 `time`；输入必须按 `(sym ASC, time ASC)` 排序；不要求各 SYM 时间集合相同。
- sym / time 转为 META（Index），其余列逐个转为 Field 文件；三者来自同一份输入，天然一致。

流程：临时目录中创建 META + 全部 Field → 全部成功后原子 rename 到 `path`；任何一步失败不留不完整 Dataset，目标路径保持不变。

### 7.4 create_dataset_index

```
create_dataset_index(path, data) -> Result<()>
```

Dataset 层对 `create_meta_file(path/.meta, data)` 的封装；只创建 / 重建 `.meta`，不创建 Field。用于 META 重建场景。

### 7.5 open_dataset

```
open_dataset(path, mode) -> Result<DatasetHandle>
```

- `mode = read | write`（访问意图，不是压缩状态）。
- 打开时只打开 META 并计算 Schema；不预先打开任何 Field Handle。
- Schema 直接从返回的 DatasetHandle 获取（`read_dataset_schema`），不另设 `get_dataset_schema()`。

### 7.6 delete_dataset

```
delete_dataset(path) -> Result<()>
```

删除整个 Dataset 根目录（META + 全部 Field），不逐个删除。

### 7.7 Field 结构操作

```
create_dataset_field(path, name, data_type, init?) -> Result<()>
create_dataset_fields(path, data: DataView) -> Result<()>
delete_dataset_field(path, name) -> Result<()>
rename_dataset_field(path, name, new_name) -> Result<()>
cast_dataset_field(path, name, target_type) -> Result<()>
compress_dataset_field(path, name) -> Result<()>
decompress_dataset_field(path, name) -> Result<()>
```

共同语义：

- `sym / time` 不作为普通 Field 操作；其身份由 META 管理。
- 新增：名称不得与已有 Field 重复（批量时彼此也不得重复）；字段长度必须等于当前逻辑长度 `L`；不改变 sym / time 范围；完成后 Schema 同步增长。`create_dataset_field` 的 `init` 对齐 core `create_field_file`：省略 = 全 NULL（`length(L)`）；`data(ColumnView)` / `stream(reader)` = 带数据初始化，行数必须恰为 `L`，分配与写值一步完成；`data_type` 显式传入并与数据一致。
- 删除：内部复用 `delete_field_file`；META 不受影响；Schema 同步移除。
- rename：内部复用 `rename_field_file`（同目录原子 rename）；`new_name` 不得与已有 Field 重复、不得为 `sym` / `time`；原子完成，失败时原字段名保持不变；完成后 Schema 同步更新。
- cast：内部复用 `cast_field_file`（临时文件 + 原子替换在其内部完成）；完成后 Schema 中该 Field 类型更新。
- compress：由 META 的 SYM INDEX 生成 chunk 边界（`k` 个连续 sym，默认 `k = 8`；单 sym 区间超过上限 64K 行时按行数劈开），以 `offsets` 传给 `compress_field_file`；decompress：直接调用 `decompress_field_file`。批量 = 多次调用。

### 7.8 read_dataset_schema

```
read_dataset_schema(handle) -> Schema
```

- 返回当前全部逻辑字段（含 sym / time）及 `DataType`；不触发数据扫描。
- 不返回物理属性（encoding / compression / offset）。
- 返回对象为只读元数据视图，生命周期不超过 Dataset。

### 7.9 read_dataset_statistics

```
read_dataset_statistics(handle) -> DatasetStatistics
DatasetStatistics
├── row_count        // 逻辑行数 = L（容量网格）
├── sym_count
├── sym_min / sym_max
├── time_count
└── time_min / time_max
```

- 统计来自 META，不扫描 Field 数据；只读。
- `sym_min / sym_max` 取字典首尾；`time_min / time_max` 取 TIME AXIS 首尾。
- 不含 Field 级 min / max / null_count。

### 7.10 read_dataset

```
read_dataset(handle, offset, length, columns?) -> Result<DataView>
```

- `offset / length` 是 Dataset 逻辑行范围；因逻辑 = 物理，各 Field 直接以相同 offset / length 读取，无需换算。
- 默认返回 `sym` 与 `time`（来自 META，零拷贝）；其余 Field 由 `columns` 指定，无需重复指定 sym / time。
- 一个范围可跨多个 sym；所需 Field 按需打开。
- 零拷贝优先；返回的 `DataView` 不拥有数据，生命周期不超过相关 Handle。

流程：

```
read_dataset(handle, offset, length, columns?)
        ├── META → sym/time view（TIME AXIS + SYM INDEX）
        ├── 各 Field → read_field_handle → ColumnView
        └── 组装 → DataView
```

### 7.11 write_dataset

```
write_dataset(handle, offset, data: DataView) -> Result<()>
```

职责：对已有逻辑行做 positional overwrite。

- 只覆盖已有数据区域；不改变 META layout、逻辑长度、sym / time 身份；不是追加接口。
- `offset + data.length ≤ L`；`length = 0` 合法 no-op。
- 支持 projection write：`data` 可只含 Schema 的部分字段；字段必须属于 Schema 且类型兼容；未提供字段保持原值。
- 所有输入列等长；`sym / time` 不作为写入列。
- 每列按名称定位到对应 Field，调用 `write_field_handle`（values + validity 成对写入；`validity = null` 表示本段全有效）。
- **不保证跨 Field 原子性**：多个 Field 独立写入，部分成功不回滚，Dataset 可能处于部分更新状态；不提供跨 Field transaction / rollback。

### 7.12 scan_dataset

```
scan_dataset(handle, request) -> Result<DatasetScanner>
DatasetScanner::next() -> Result<RowRange?>    // 逻辑行范围
```

- `ScanRequest.ranges`：逻辑行候选范围（空 = 整个 Dataset）；`projection`：需要读取的字段集合；`predicate` / `limit` 同公共契约。
- Scanner 组合 META 与各 Field 的扫描结果（求交 / 裁剪 / 合并），输出 **Dataset 逻辑 RowRange**。
- 只定位不读取；实际数据由 `read_dataset` 消费；batch 收集由上层负责。
- 多 predicate 的执行顺序由上层 planner 决定。

流程：

```
scan_index（sym/time 条件）→ candidate ranges
    → scan_field_handle（值条件，逐 Field）→ 求交 / 裁剪
    → DatasetScanner → 逻辑 RowRange → read_dataset → DataView
```

### 7.13 locate_dataset_index

```
locate_dataset_index(handle, data: DataView) -> Result<RowRange[]>
```

META `locate_index_handle` 的 Dataset 级封装（参数与语义一致）。供 splayed-table 的 `write_table` 批量定位使用；Table 层不直接持有 MetaHandle。

### 7.14 close_dataset

```
close_dataset(handle) -> Result<()>
```

- 关闭 META Handle，释放 Dataset 层维护的 Field Handle / 元数据。
- 已返回的 `DataView` / `ColumnView` 在依赖资源关闭后不再保证有效。
- 关闭后不可继续 Dataset 读写或扫描。

### 7.15 Dataset 结构变化的边界

```
Field 级变化（原地）
├── create_dataset_field(s) / delete_dataset_field / cast_dataset_field
└── compress / decompress_dataset_field

Dataset 级变化（重建）
├── 扩大容量 / 新增 sym / 扩大 time 范围
└── 改变整体物理布局与 META/Field 对应关系 → 重建 Dataset，不提供原地 API
```

## 8. 设计边界（V2.0 暂不引入）

- 独立 Schema 文件 / API。
- `commit / flush / dump` 等写入控制 API。
- Field Handle 的公开压缩 / 解压 API（用 File API）。
- META 原地 update API。
- stride / 非 contiguous Column。
- core 内部的全局 predicate reorder / query planner。
- 行级 DELETE、INSERT 追加、UPSERT、MERGE（见 splayed-table §6）。

设计原则：**core API 保持薄，只有上层真正需要的能力才下沉到 core。**

## 9. 性能审查记录（V2.0 首轮）

已优化：

- `MetaBuilder`：run-length 遍历——字符串分配/比较发生在 sym run 边界（O(sym 段数) 而非 O(行数)）；轴定位单调双指针；TIME AXIS 两遍全局构建（全局去重有序，非 first-appearance）。
- `sym_id_of`：字典二分查找 O(log S)（字典按首现序 = 排序序）。
- Dataset 扫描的 ranges 求交：双指针归并 O(a + b)。

已知优化项（当前实现为正确性优先的简化，行为符合本文档语义）：

- compressed Field 打开即**全量解压**为工作表示；chunk 级惰性解码（只解码覆盖请求范围的 chunk）待实现。
- 谓词求值逐行走 `read_row_scalar` 标量分发；类型特化的段内向量化（SIMD）待实现。
- Handle 的 scratch 缓冲逐次累积（视图生命周期契约要求），长生命周期高频读场景的回收策略待定。
- `read_index_handle` 的 sym keys 逐行物化；常量 run 可用 RLE 型段表示。

## 10. Benchmark vs Parquet（V2.0 首轮基线，2026-09）

数据：64 sym × 250 行 = 16K 行 × 4 列（sym/time/price/volume），按月分区。
基准：`crates/splayed-table/benches/vs_parquet.rs`（criterion；对照 arrow-rs parquet 56）。

| 路径 | splayed | parquet | 差距 |
| --- | --- | --- | --- |
| 写入（端到端建表） | ~83 ms | ~10 ms | ≈ 8× |
| 全表读取（Table 层） | ~6.5 ms | ~0.55 ms | ≈ 12× |
| 谓词扫描（price > 15） | ~6.9 ms | ~0.69 ms | ≈ 10× |

结论与定位：当前 V2.0 为**正确性优先**实现——写入开销主要在 MetaBuilder / 每分区
DataView 物化（gather），读取开销在三层 API 的逐分区打开 + 视图组装 + 谓词行级
求值。上述「已知优化项」（chunk 惰性解码、谓词向量化、scratch 回收、建表 gather
优化）是缩小差距的主要抓手；splayed 的目标优势场景（容量网格 O(1) 行定位、
零拷贝 sym/time 视图）在当前基准的全表读中尚未体现，因 Table 层端到端包含
schema 组装等固定开销。
