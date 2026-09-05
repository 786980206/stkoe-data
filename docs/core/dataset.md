# splayed-core / Dataset API

## 7. Dataset API

### 7.1 结构与对象

```
Dataset（目录）
├── .meta                     // Index：sym/time → 逻辑行
└── <name>.field × N          // 字段数据文件
```

- `open_dataset` 只打开 META；Dataset Schema 从 META 与 Field 元信息得到。
- Field Handle 按需打开；首次访问某个 Field 时校验其与 Dataset 的 generation / Schema 一致性。
- Field 名称 ↔ 文件名的映射由 Dataset 层定义（沿用字段名 = 文件名约定）。
- Dataset 不引入独立 Schema 文件、独立 Index 文件——Dataset Index 就是 `.meta`。

### 7.2 总览

| 接口 | 职责 | 层 |
| --- | --- | --- |
| `create_dataset` | 创建完整 Dataset（META + Fields） | File |
| `create_dataset_index` | 创建 / 重建 `.meta` | File |
| `open_dataset` | 打开 Dataset，返回 `DatasetHandle` | File |
| `delete_dataset` | 删除完整 Dataset 目录 | File |
| `create_dataset_field` | 新增单个 Field | Handle |
| `delete_dataset_field` | 删除指定 Field | Handle |
| `rename_dataset_field` | 重命名指定 Field | Handle |
| `update_dataset_field_header` | 更新指定 Field 的 header 物理属性 | Handle |
| `cast_dataset_field` | 转换指定 Field 类型 | Handle |
| `compress_dataset_field` / `decompress_dataset_field` | 压缩 / 解压指定 Field | Handle |
| `read_dataset_schema` | 读取逻辑 Schema | Handle |
| `read_dataset_statistics` | 读取 Dataset 级统计 | Handle |
| `read_dataset` | 按逻辑行读取多列 | Handle |
| `write_dataset` | 按逻辑行覆盖写入 | Handle |
| `scan_dataset` | 条件扫描 → `DatasetScanner` | Handle |
| `locate_dataset_index` | (sym, time) 联合键批量定位（转发 META） | Handle |
| `close_dataset` | 关闭并释放资源 | Handle |

> 规范说明：草稿中单字段入口与多字段入口重名（两处 `create_dataset_fields`），规范为 `create_dataset_field`（单字段）；批量新增由调用方多次调用（未设批量 API）。`delete / cast / compress / decompress_dataset_field` 在草稿中签名缺失，此处补齐。新增 `locate_dataset_index` 作为 META `locate_index_handle` 的 Dataset 级封装，使 splayed-table 只依赖 Dataset API、不持有 MetaHandle。新增 `update_dataset_field_header`（header 物理属性更新，供 Table 层转发）。

### 7.3 create_dataset

**接口定义**：
```rust
pub struct CreateDatasetOptions {
    pub max_parallelism: usize,   // Field 文件并行创建的线程上限（1 = 串行）；默认 = 逻辑核数
}
impl Default for CreateDatasetOptions { /* std::thread::available_parallelism() */ }

pub fn create_dataset(path: &Path, data: Data, options: CreateDatasetOptions) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Dataset 目录路径；必须不存在 |
| `data` | `Data` | 输入 | 拥有数据所有权的完整表数据（Schema + 全部列值）；必须含 `sym` 与 `time`，按 `(sym ASC, time ASC)` 排序 |
| `options` | `CreateDatasetOptions` | 输入 | 并行选项：`max_parallelism` 控制 Field 并行创建线程数 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = Dataset 创建完成 |

**内部实现**：
```
1. 校验 path 不存在、data 含 sym/time
2. ① META 单线程先行：MetaBuilder::build(data.as_view())
      一次扫描完成全局合法性校验（排序 / 连续子区间 / 容量网格）+ 构建；失败不落盘
3. ② 抽走非 sym/time 列（std::mem::take，列所有权零拷贝移动——不 clone）
4. ③ 临时目录（.{name}.tmp）→ META 字节直写 + fsync
5. ④ Field 并行创建：P = min(max_parallelism, 字段数)
      round-robin 分桶 → std::thread::scope：桶内顺序 create_field_file、桶间并行
6. 全部成功 → fs::rename(tmp, path) 原子发布；失败 → remove_dir_all(tmp) 清理
```

**核心原则**：先用 META 一次扫描完成全局合法性校验，再并行创建所有独立 Field；
Field 之间完全独立，是并行创建的基本单元；并行度由 `max_parallelism` 暴露给上层控制。
META 构建保持单线程（顺序扫描本身无并行点），这一层不重复扫描、不拷贝数据（列所有权移动）。

**说明**：
- sym / time 转为 META（Index），其余列逐个转为 Field 文件；三者来自同一份输入，天然一致。
- 不要求各 SYM 时间集合相同；缺失时间点由 Field 的 NULL 表示。
- META 文件在 tmp 内 fsync 后才做目录级 rename；Field 文件沿用 create_field_file 语义（无逐文件 fsync）。
- 无 rayon 依赖：并行用 `std::thread::scope` + 手工分桶实现，保持 core 依赖精简。

### 7.4 create_dataset_index

**接口定义**：
```rust
pub fn create_dataset_index(path: &Path, data: &DataView<'_>) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Dataset 目录路径 |
| `data` | `&DataView<'_>` | 输入 | sym / time 两列逻辑数据（要求同 `MetaBuilder::build`） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = `.meta` 创建 / 重建完成 |

**内部实现**：
```
create_meta_file(&path.join(".meta"), data)
    → MetaBuilder::build（单遍扫描 → TIME AXIS sort+dedup → SYM INDEX 连续子区间校验）
    → write_meta_atomic（tmp + sync_all + rename，失败清理）
```

**说明**：
- **薄封装原则**：本接口只是 Dataset → META 的一行委托，性能优化全部放在 `MetaBuilder`；
  这一层不重复扫描、不拷贝 sym/time、不引入并行（`data` 以借用 `&DataView` 直传，无所有权/拷贝要求）。
- 只创建 / 重建 `.meta`，不创建 Field。用于 META 重建场景。
- 重建期间不应持有该 `.meta` 的打开 Handle（含 open 着的 DatasetHandle）：原子 rename 替换时
  Windows 上会因文件被占用而失败——先 `close_dataset` 再重建。
- 路径校验委托 META 层：目录不存在时返回底层 IO/NotFound 错误，本层不重复校验。

### 7.5 open_dataset

**接口定义**：
```rust
pub fn open_dataset(path: &Path, mode: Mode) -> Result<DatasetHandle, CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Dataset 目录路径（必须已存在） |
| `mode` | `Mode` | 输入 | `read` / `write`（访问意图，不是压缩状态） |
| 返回 | `Result<DatasetHandle, CoreError>` | 输出 | Dataset 生命周期 Handle |

**内部实现**：
```
1. path.is_dir() 校验
2. MetaHandle::open(path/.meta)  →  File::open + Mmap::map + header 校验
3. build_schema(path, time_type)  →  read_dir 列出字段名（过滤 starts_with('.') 与 *.tmp）
                                    →  names.sort() → 逐 field File::open + read_exact 64B header → data_type
4. 构造 DatasetHandle { meta, schema, fields: RefCell<HashMap>（空）, mode }
```

**说明**（open_dataset 优化原则）：
- 只打开 META，不打开 Field Handle——Field 全部 lazy open：`fields` 缓存初始为空，
  `ensure_field` 首次 read/write/scan 访问时才 `open_field_file`。
- Schema 只读取 Field Header（逐字段 64B `read_exact`，无 mmap、不读数据区）。
- Schema 缓存到 `DatasetHandle`，open 后 `read_dataset_schema` 不再扫描目录；
  Field 结构操作（create / delete / rename / cast）同步更新缓存。
- 不引入并行参数：Field header I/O 很轻（N 次 64B 顺序读），并行没有收益。
- 过滤 `starts_with('.') || ends_with(".tmp")`：比原则更宽——覆盖 `.meta`、`.meta.tmp`、
  cast 的 `.cast.{pid}.{n}.tmp` 及一切隐藏文件；字段名排序保证 Schema 顺序确定。
- 保持轻量化：open 全程零数据 I/O，mmap 与数据读取延迟到首次 read / write / scan。
- 目录中混入非 Field 的非隐藏文件 / 子目录时 header 读取直接报错（fail-loud，符合目录布局契约）。

### 7.6 delete_dataset

**接口定义**：
```rust
pub fn delete_dataset(path: &Path) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Dataset 目录路径 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 目录已删除 |

**说明**：
- 删除整个 Dataset 根目录（META + 全部 Field），不逐个删除；调用方保证没有打开的 Handle。

### 7.7 Field 结构操作（create / delete / rename / update_header / cast / compress / decompress）

**接口定义**：
```rust
impl DatasetHandle {
    pub fn create_dataset_field(&mut self, name: &str, data_type: DataType,
        init: DatasetFieldInit) -> Result<(), CoreError>
    pub fn delete_dataset_field(&mut self, name: &str) -> Result<(), CoreError>
    pub fn rename_dataset_field(&mut self, name: &str, new_name: &str) -> Result<(), CoreError>
    pub fn update_dataset_field_header(&self, name: &str, header: FieldHeader) -> Result<(), CoreError>
    pub fn cast_dataset_field(&mut self, name: &str, target_type: DataType) -> Result<(), CoreError>
    pub fn compress_dataset_field(&mut self, name: &str) -> Result<(), CoreError>
    pub fn decompress_dataset_field(&mut self, name: &str) -> Result<(), CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `name` / `field` | `&str` | 输入 | 字段名；不得为 `sym` / `time` 保留名；文件名即字段名 |
| `data_type` / `target_type` | `DataType` | 输入 | 新增字段类型 / cast 目标类型（cast 涉及 Utf8 → Error） |
| `init` | `DatasetFieldInit` | 输入 | `AllNull`（全 NULL，长度 = L）/ `Data(Column)`（行数必须恰为 L）/ `Stream { reader }`（累计行数必须恰为 L） |
| `new_name` | `&str` | 输入 | 新字段名；不得与已有 Field 重复 |
| `header` | `FieldHeader` | 输入 | 新 header 物理属性；`data_type` / `row_count` 由 core 强制为现值 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 结构变化完成且 Schema 同步更新 |

**内部实现**（全部复用 Field 层 API；Handle 缓存中该字段的 Handle 先释放再操作）：
```
create   →  create_field_file(path/<name>, data_type, init 映射)（分配与写值一步完成）
delete   →  borrow_mut().remove(name)（释放缓存）→ delete_field_file
rename   →  释放缓存 → rename_field_file（同目录原子 rename）
update   →  ensure_field → update_field_handle（类型转换走 cast，不走 update）
cast     →  释放缓存 → cast_field_file（批次流式 + 原子替换在其内部完成）
compress →  释放缓存 → 由 META 的 SYM INDEX 生成 chunk 边界（k 个连续 sym，默认 k = 8；
            单 sym 区间超过上限 64K 行时按行数劈开）→ compress_field_file(path, Some(offsets))
decompress → 释放缓存 → decompress_field_file（chunk 流式）
```

**说明**（Dataset Field 结构操作九原则）：
1. **统一复用 Field 层 API**：Dataset 层只负责校验、缓存管理和 Schema 同步。
2. **操作前释放缓存 Handle**（`fields.borrow_mut().remove(name)`）：避免文件替换 / rename 后
   持有旧 mmap（Windows 关键）；update_header 例外——原地 header 写经缓存 Handle 直接生效。
3. **create / delete / rename / cast / compress / decompress 全部原子完成**，失败不破坏原文件。
4. **create 直接写入最终数据**：AllNull → set_len 零填充；Data → values 直写；Stream → 三阶段
   顺序写，无中间 Buffer 拷贝。
5. **cast / compress / decompress 全部流式处理**，内存控制在批次 / chunk 级。
6. **compress 的 chunk 边界优先复用 META 的 sym 边界**（`sym_aligned_offsets(k=8, cap=64K)`，
   从 SYM INDEX 累计 row_start，零数据扫描；超长 sym 按 cap 劈开）。
7. **update_header 只修改允许修改的物理属性**：`data_type / row_count` 由 Core 强制保持一致，
   `null_count` 为派生统计保持现值。
8. **结构操作完成后立即同步 Schema**（`reload_schema()`）；compress / decompress / update_header
   不改逻辑 Schema（data_type 不变），无需 reload。
9. **这些 API 不引入并行参数**；批量操作的并行由更高层（Table / 执行层）统一调度。

**核心原则：Dataset 层不重复做数据处理，只做"校验 → 调 Field → 更新 Schema"，性能优化全部下沉到 Field 层。**

- 共同语义：
  - `sym / time` 不作为普通 Field 操作；其身份由 META 管理。
  - 新增：名称不得与已有 Field 重复；字段长度必须等于当前逻辑长度 `L`；不改变 sym / time 范围；完成后 Schema 同步增长。
  - create 失败（如 Stream 源数据中断）立即删除半成品文件——直接写最终路径的残留会带零填充
    占位 header，污染后续 build_schema / open_dataset（Schema 未同步，文件必须不落痕）。
  - 删除：META 不受影响；Schema 同步移除。
  - rename：原子完成，失败时原字段名保持不变；完成后 Schema 同步更新。
  - cast：完成后 Schema 中该 Field 类型更新。

### 7.8 read_dataset_schema

**接口定义**：
```rust
impl DatasetHandle {
    pub fn read_dataset_schema(&self) -> Schema
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| 返回 | `Schema`（owned） | 输出 | 当前全部逻辑字段（含 sym / time）及 `DataType` |

**说明**（read_dataset_schema 优化原则）：
- 直接返回 `DatasetHandle` 中缓存的 Schema（`self.schema.clone()`），不重新扫描目录；
  缓存由 `open_dataset` 构建一次，此后本接口 O(1)（纯内存，O(字段数) clone）返回。
- 不触发 Field / META I/O。
- Schema 保持 owned：返回值是快照，不暴露内部生命周期；后续结构变化不影响已拿到的副本，close 后仍可用。
- 只描述逻辑字段（name + DataType），不混入 compression / offset 等物理信息。
- 无需并行——纯内存访问。
- 缓存一致性：Field 结构操作（create / delete / rename / cast / compress / decompress）
  变更后经 `reload_schema()` 重建缓存，读路径永远只读缓存。
- 另有 `peek_time_type(&self) -> TimeType` 访问器（来自 META header）。

### 7.9 read_dataset_statistics

**接口定义**：
```rust
pub struct DatasetStatistics {
    pub row_count: u64,        // 逻辑行数 = L（容量网格）
    pub sym_count: u32,
    pub sym_min: Option<String>,
    pub sym_max: Option<String>,
    pub time_count: u32,
    pub time_min: i64,
    pub time_max: i64,
}
impl DatasetHandle {
    pub fn read_dataset_statistics(&self) -> Result<DatasetStatistics, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| 返回 | `Result<DatasetStatistics, CoreError>` | 输出 | Dataset 级统计（来源见下） |

**说明**：
- 统计来自 META，不扫描 Field 数据；只读。
- `sym_min / sym_max` 取字典首尾；`time_min / time_max` 取 TIME AXIS 首尾。
- 不含 Field 级 min / max / null_count。

### 7.10 read_dataset

**接口定义**：
```rust
impl DatasetHandle {
    pub fn read_dataset(&self, offset: u64, length: u64,
        columns: Option<&[&str]>) -> Result<DataView<'_>, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&DatasetHandle` | 输入 | Handle（read / write mode 均可） |
| `offset` | `u64` | 输入 | 逻辑行起始 |
| `length` | `u64` | 输入 | 读取行数；`offset + length ≤ L` |
| `columns` | `Option<&[&str]>` | 输入 | 需要的 Field 集合（projection），必须显式列出；`sym` / `time` 恒返回，无需指定；`None` = 仅返回 sym / time |
| 返回 | `Result<DataView<'_>, CoreError>` | 输出 | 多列 zero-copy 视图；生命周期不超过相关 Handle |

**内部实现**（两阶段借用）：
```
阶段 1（&mut self.fields）  →  ensure_field：确保所有需要的 Field Handle 已打开
阶段 2（&self.meta + &self.fields）  →  共享借用创建视图
    base = meta.read_index_handle(offset, length)
        →  locate_row(offset) 二分 SYM INDEX
        →  sym 列 RepeatDict 段（零物化，无 scratch arena）
        →  time 列切片 TIME AXIS（零拷贝多段）
    各 Field → field_handle.read_field_handle(offset, length)
        →  mmap 切片（PLAIN+NONE 单段）或 working 切片（compressed 多段）
    组装 → DataView
```
- 两阶段借用避免 &mut self.fields 与 &self.meta 冲突

**说明**：
- `offset / length` 是 Dataset 逻辑行范围；因逻辑 = 物理，各 Field 直接以相同 offset / length 读取，无需换算。
- `sym` / `time` 恒返回（来自 META，零拷贝）；其余 Field 由 `columns` 显式指定（`None` = 仅 sym / time）。
- 一个范围可跨多个 sym；所需 Field 按需打开。
- 零拷贝优先；返回的 `DataView` 不拥有数据，生命周期不超过相关 Handle。

流程：

```
read_dataset(offset, length, columns?)
        ├── META → sym/time view（TIME AXIS + SYM INDEX RepeatDict）
        ├── 各 Field → read_field_handle → ColumnView
        └── 组装 → DataView
```

### 7.11 write_dataset

**接口定义**：
```rust
impl DatasetHandle {
    pub fn write_dataset(&self, offset: u64, data: &DataView<'_>) -> Result<(), CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&DatasetHandle` | 输入 | Handle（需 write mode） |
| `offset` | `u64` | 输入 | 逻辑行起始；`offset + data.length() ≤ L` |
| `data` | `&DataView<'_>` | 输入 | 待写入列（支持 projection write）；`sym` / `time` 不作为写入列；所有输入列等长 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 各列覆盖写入完成（跨列无原子性） |

**内部实现**：
```
1. mode.require_write + 边界校验（offset + len ≤ L）+ 类型校验
2. 逐列（按 data.schema 顺序）：
     ensure_field（&self.fields）
     self.fields.borrow_mut().get_mut(name).write_field_handle(offset, col_view)
3. 不保证跨 Field 原子性：第 N 列写入失败时前 N-1 列已生效
```
- write_field_handle 内部按 Field 的物理表示分派（mmap 直写或 working 修改）
- 每次 write 成功后 field generation += 1

**说明**：
- 职责：对已有逻辑行做 positional overwrite；只覆盖已有数据区域；不改变 META layout、逻辑长度、sym / time 身份；不是追加接口。
- `length = 0` 合法 no-op。
- 支持 projection write：`data` 可只含 Schema 的部分字段；字段必须属于 Schema 且类型兼容；未提供字段保持原值。
- 每列按名称定位到对应 Field，调用 `write_field_handle`（values + validity 成对写入；`validity = null` 表示本段全有效）。
- **不保证跨 Field 原子性**：多个 Field 独立写入，部分成功不回滚，Dataset 可能处于部分更新状态；不提供跨 Field transaction / rollback。

### 7.12 scan_dataset

**接口定义**：
```rust
impl DatasetHandle {
    pub fn scan_dataset(&self, request: &ScanRequest) -> Result<DatasetScanner, CoreError>
}
impl DatasetScanner {
    pub fn next(&mut self) -> Result<Option<RowRange>, CoreError>   // 逻辑行范围
    pub fn close(self) -> Result<(), CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&DatasetHandle` | 输入 | Handle（read / write mode 均可） |
| `request.ranges` | `&[RowRange]` | 输入 | 逻辑行候选范围（空 = 整个 Dataset） |
| `request.projection` | `&[Arc<str>]` | 输入 | 需要读取的字段集合（决定哪些 Field 参与谓词求值） |
| `request.predicate` | `Option<Predicate>` | 输入 | 字段值 / sym / time 条件；跨字段的 Or / Not 不支持（返回 Invalid）——行级过滤由上层兜底 |
| `request.limit` | `Option<u64>` | 输入 | 最多产生的命中行数 |
| 返回 scanner | `Result<DatasetScanner, CoreError>` | 输出 | 定位器；`next()` 每次返回一个逻辑 RowRange，结束返回 `None` |

**内部实现**：
```
1. clamp_ranges(request.ranges, L)  →  裁剪到 [0, L)
2. collect_for_fields(predicate, ["sym","time"])  →  提取 sym/time 子谓词
     →  meta.scan_index_handle(sym_time_req)  →  narrowed ranges
3. predicate_groups(predicate)  →  按字段分组（跳过 sym/time 保留名）
     →  按名称排序 → 逐 Field：ensure_field → scan_field_handle(sub_req)
     →  逐 Field 收窄 ranges（intersect_range_lists 归并求交）
4. DatasetScanner { ranges: VecDeque(current), remaining: request.limit }
```
- 求交用双指针归并 O(a + b)，非 O(a × b)

**说明**：
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

**接口定义**：
```rust
impl DatasetHandle {
    pub fn locate_dataset_index(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `pairs` | `&[(String, i64)]` | 输入 | `(sym, time)` 联合键序列；必须按 `(sym ASC, time ASC)` 排序且唯一 |
| 返回 | `Result<Vec<RowRange>, CoreError>` | 输出 | 合并后的连续 RowRanges；`sum(length) == pairs.len()` 为定位成功充要条件 |

**说明**：
- META `locate_index_handle` 的 Dataset 级封装（参数与语义一致）。供 splayed-table 的 `write_table` 批量定位使用；Table 层不直接持有 MetaHandle。

### 7.14 close_dataset

**接口定义**：
```rust
impl DatasetHandle {
    pub fn close_dataset(mut self) -> Result<(), CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `self` | `DatasetHandle` | 输入 | 按值消费 Handle |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 资源释放完成 |

**说明**：
- 关闭 META Handle，释放 Dataset 层维护的 Field Handle / 元数据。
- 已返回的 `DataView` / `ColumnView` 在依赖资源关闭后不再保证有效。
- 关闭后不可继续 Dataset 读写或扫描。

### 7.15 Dataset 结构变化的边界

```
Field 级变化（原地）
├── create_dataset_field / delete_dataset_field / rename_dataset_field
├── update_dataset_field_header / cast_dataset_field
└── compress / decompress_dataset_field

Dataset 级变化（重建）
├── 扩大容量 / 新增 sym / 扩大 time 范围
└── 改变整体物理布局与 META/Field 对应关系 → 重建 Dataset，不提供原地 API
```
