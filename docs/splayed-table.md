# splayed-table：API 设计与规格

本页定义 `splayed-table` 的 public API。splayed-table 位于 `splayed-core` 之上，把多个 Dataset 组织成一个逻辑 Table：负责 Partition 的组织、发现、裁剪与跨 Partition 的 scan / read 调度。

- 所有数据访问经 core API 完成；Table 层不定义新的物理格式，不重复定义 Dataset / META / Field 的物理细节。
- core 格式与 API 见 [splayed-format](splayed-format.md) / [splayed-core](splayed-core.md)。
- 各 API 按统一格式组织：函数名称 → 函数签名 → 参数与返回表 → 内部实现流程 → 其他说明。

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
Index / Field
```

---

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
├── .lock               // write_table 并发互斥锁
├── year=2024/          // 每个 Partition 目录 = 一个 Dataset
├── year=2025/
└── year=2026/
```

- Partition 不是新的物理格式，而是 Table 层对 Dataset 的逻辑组织单元；Partition : Dataset = 1:1。
- `none` 模式不存在 Partition 子目录，`PartitionRowRange` 中的 Partition 为隐式根 Dataset。

---

## 2. Partition Pruning

- 只使用 `time` 条件；不使用 sym。
- 只负责排除不可能命中的 Partition，不做行级过滤；`time` 条件同时继续传给 Dataset scan 做精确过滤。
- `none` 模式无 Partition pruning，直接访问唯一根 Dataset。

```
WHERE time >= 2026-09-01 AND time < 2026-10-01
        ↓ partition pruning
month=2026-09 → Dataset → splayed-core scan（精确过滤）
```

---

## 3. 公共数据结构

### 3.1 TableOptions
```rust
pub struct TableOptions {
    pub max_parallelism: Option<usize>,
    pub compression: Option<Compression>,
    pub chunk_target_rows: Option<usize>,
}
```

### 3.2 TableScanRequest
```rust
pub struct TableScanRequest {
    pub sym: Option<String>,
    pub time: Option<(i64, i64)>,
    pub predicate: Option<Predicate>,
    pub projection: Vec<String>,
    pub limit: Option<u64>,
    pub max_parallelism: Option<usize>,
}
```

### 3.3 并发调度管理
```rust
impl TableHandle {
    /// 获取当前 Table 的最大并发预算
    pub fn max_parallelism(&self) -> usize;

    /// 动态调节全局最大并发预算，并级联更新所有已打开的 Dataset 句柄
    pub fn set_max_parallelism(&mut self, max_parallelism: usize);
}
```

---

## 4. Table API 详细设计

### 4.0 总览

| 类别 | API | 语义 |
| --- | --- | --- |
| Lifecycle | `create_table` | 创建空表根目录骨架 |
|  | `init_table` | 连带数据初始化整张表（跨分区切分与并行创建） |
|  | `open_table` | 打开 TableHandle（惰性加载分区与缓存） |
|  | `close_table` | 关闭 TableHandle，刷盘各分区 |
|  | `drop_table` | 销毁并物理删除整张表目录 |
|  | `rename_table` | 重命名表根目录 |
|  | `delete_table` | 物理删除指定分区的数据目录 |
| Field 结构 | `TableHandle::create_table_field` | 最新分区或全分区新增字段（64B Header-Only） |
|  | `TableHandle::delete_table_field` | 删除所有分区中的同名字段 |
|  | `TableHandle::rename_table_field` | 重命名所有分区中的同名字段 |
|  | `TableHandle::cast_table_field` | 转换所有分区中该字段的数据类型 |
|  | `TableHandle::compress_table_field` | 压缩所有分区中的同名字段 |
|  | `TableHandle::decompress_table_field`| 解压所有分区中的同名字段 |
| Metadata | `TableHandle::read_table_schema` | 读取全表统合 Schema |
| 流式写入与追加 | `TableStreamWriter` | 超大数据冷启动 Out-of-Core 逐分区流式落盘构建 |
|  | `TableHandle::create_partition` | 运行时向打开的表流式追加新分区 |
| Query | `scan_table` | 惰性定位跨 Partition 的 `PartitionRowRange` |
|  | `read_table` | 跨分区流式批量读取（缺失列自动补齐虚拟 NULL） |
| Write | `write_table` | 逐分区覆盖写入（自动补建缺失列，自愈） |
|  | `update_table` | 逐分区全量替换更新数据 |

---

### 4.1 create_table

#### 函数签名
```rust
pub fn create_table(
    table_path: &Path,
    schema: &Schema,
    scheme: PartitionScheme,
    initial_partition: Option<&str>,
    options: Option<TableOptions>,
) -> Result<TableHandle, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table_path` | `&Path` | 输入 | Table 根目录；必须不存在或为空 |
| `schema` | `&Schema` | 输入 | 表的初始逻辑 Schema（必须含 sym 与 time） |
| `scheme` | `PartitionScheme` | 输入 | `none \| year \| month \| date` 四选一 |
| `initial_partition` | `Option<&str>` | 输入 | 初始预建的最新分区名；None 自动根据当前公历推断（如 `month=2026-03`） |
| `options` | `Option<TableOptions>` | 输入 | 并行度与压缩等配置项 |
| 返回 | `Result<TableHandle, CoreError>` | 输出 | 创建成功的空表句柄 |

#### 内部实现流程
```
1. 校验 table_path 不存在或为空，创建根目录 fs::create_dir_all(table_path)；
2. 校验 schema 包含合法的 sym 与 time 字段；
3. 若 scheme == PartitionScheme::None：
   → 直接调用 create_dataset(table_path, schema) 在根目录创建单一数据集；
4. 若为分区模式（Year / Month / Date）：
   → 提取指定或默认推断的最新分区名（如 month=2026-03）；
   → 在对应分区路径下调用 create_dataset(&part_path, schema) 完整初始化最新分区的 64B Header-Only 空骨架；
5. 调用 open_table(table_path, Mode::Write, options) 打开并返回句柄。
```

#### 其他说明
- 物理磁盘上仅生成各字段 64B Header-Only 空文件，0 实际数据 I/O。
- 表创建完成后立即拥有完整的 Schema 上下文，`table.read_table_schema()` 可立即读取，无需等待任何数据落盘。

---

### 4.2 init_table

#### 函数签名
```rust
pub fn init_table(
    path: &Path,
    partition_scheme: PartitionScheme,
    data: &DataView<'_>,
    options: Option<TableOptions>,
) -> Result<TableHandle, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Table 根目录；必须不存在 |
| `partition_scheme` | `PartitionScheme` | 输入 | 分区方案 |
| `data` | `&DataView<'_>` | 输入 | 包含 sym, time 及数据字段的完整初始数据 |
| `options` | `Option<TableOptions>` | 输入 | 并行度与压缩分块配置 |
| 返回 | `Result<TableHandle, CoreError>` | 输出 | 构建完成的表句柄 |

#### 内部实现流程
```
1. 校验 path 不存在且 data 包含按 (sym ASC, time ASC) 排序的有效数据；
2. 若 partition_scheme == PartitionScheme::None：
   → 调用 init_dataset(path, data, ds_options) 直写根目录；
3. 若为时间分区：
   - 主线程单遍线性扫描 time 列：计算连续行片段 RowSpan，分组为各分区 run；
   - 预算切分：P_part = min(max_parallelism, 分区数)，P_field = max(1, max_parallelism / P_part)；
   - std::thread::scope 启动并行线程：每个线程执行 gather_runs 批量拼接后调用 init_dataset；
4. open_table 打开并返回 TableHandle。
```

#### 其他说明
- 片段化 gather 确保内存带宽接近 memcpy；分区级与字段级严格共享并行预算。

---

### 4.3 open_table

#### 函数签名
```rust
pub fn open_table(
    path: &Path,
    mode: Mode,
    options: Option<TableOptions>,
) -> Result<TableHandle, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 已存在的 Table 目录路径 |
| `mode` | `Mode` | 输入 | `Mode::Read` 或 `Mode::Write` |
| `options` | `Option<TableOptions>` | 输入 | 配置项 |
| 返回 | `Result<TableHandle, CoreError>` | 输出 | 表生命周期句柄 |

#### 内部实现流程
```
1. 校验 path 存在且为目录；
2. 发现分区：fs::read_dir 扫描子目录名推断 partition_scheme；若根目录含有 .meta 则为 None scheme；
3. 初始化 TableHandle：持有 scheme、mode、空的 Dataset 缓存池 datasets 与统计缓存 stats_cache；
4. 返回 TableHandle。
```

#### 其他说明
- 极速打开（微秒级），不提前打开任何子分区 Dataset。

---

### 4.4 close_table

#### 函数签名
```rust
pub fn close_table(handle: TableHandle) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `handle` | `TableHandle` | 输入 | 待关闭的表句柄 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 释放完成 |

#### 内部实现流程
```
1. 获取 datasets 缓存中所有已加载的 DatasetHandle；
2. 逐个调用 close_dataset(ds) 进行刷盘和释放 mmap；
3. 释放锁与内存缓存。
```

#### 其他说明
- 确保所有写入和重压缩操作安全刷盘并解除 Windows 句柄锁定。

---

### 4.5 drop_table

#### 函数签名
```rust
pub fn drop_table(handle: TableHandle) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `handle` | `TableHandle` | 输入 | 待删除的表句柄 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 整张表物理目录已删除 |

#### 内部实现流程
```
1. 记录根路径 root；
2. 显式调用 close_table 释放所有子分区句柄与 mmap 映射；
3. 调用 fs::remove_dir_all(root) 物理删除整个表目录。
```

#### 其他说明
- 彻底规避 Windows 文件句柄占用导致无法删除的问题。

---

### 4.6 rename_table

#### 函数签名
```rust
pub fn rename_table(path: &Path, new_name: &str) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 表根目录路径 |
| `new_name` | `&str` | 输入 | 同级目录下的新表名 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 重命名完成 |

#### 内部实现流程
```
1. 校验 new_name 合法性；
2. 构造新路径 path.parent().join(new_name)；
3. 检查目标是否存在；
4. 调用 fs::rename(path, new_path) 原子替换。
```

#### 其他说明
- 原子重命名整个表目录。

---

### 4.7 delete_table (按分区删除)

#### 函数签名
```rust
impl TableHandle {
    pub fn delete_table(&self, partition_name: &str) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&TableHandle` | 输入 | 表句柄（需具备写权限） |
| `partition_name` | `&str` | 输入 | 待删除的分区名（如 "month=2026-08"） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 分区物理删除完成 |

#### 内部实现流程
```
1. 校验 partition_name 格式是否与当前 scheme 匹配；
2. 若 datasets 缓存中持有该分区句柄，从中移除并显式 drop 解除 mmap；
3. stats_cache 移除该分区缓存条目；
4. 调用 fs::remove_dir_all(self.root.join(partition_name)) 删除物理分区目录。
```

#### 其他说明
- 精确安全删除单分区，对齐用户表格中的 `delete_table(tablehandle, partition)` 需求。

---

### 4.8 TableHandle::read_table_schema

#### 函数签名
```rust
impl TableHandle {
    pub fn read_table_schema(&self) -> Result<Schema, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&TableHandle` | 输入 | 表句柄 |
| 返回 | `Result<Schema, CoreError>` | 输出 | 表的统合 Schema |

#### 内部实现流程
```
1. 发现所有分区名（升序排列）；
2. 优先取最后一个最新分区，调用 dataset_for(last).read_dataset_schema()；
3. 缓存 Schema 结果，避免重复扫描。
```

#### 其他说明
- 纯内存或单次元数据读取，保证全表 Schema 查询在微秒级完成。

---

### 4.9 TableHandle::create_table_field

#### 函数签名
```rust
impl TableHandle {
    pub fn create_table_field(&self, name: &str, field_type: DataType) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&TableHandle` | 输入 | 表句柄（Write 模式） |
| `name` | `&str` | 输入 | 新增字段名（支持 dot 语法） |
| `field_type` | `DataType` | 输入 | 字段数据类型 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 新字段已创建 |

#### 内部实现流程
```
1. 检查字段名合法性与保留名；
2. 遍历已发现的全部或最新分区：
   - 调用 ds.create_dataset_field(name, field_type)；
   - 底层各自分区生成 64B Header-Only 文件；
3. 刷新 Schema 缓存。
```

#### 其他说明
- 物理开销极低（每个分区仅 64B Header），秒级完成全表新增列。

---

### 4.10 TableHandle 其它字段结构操作 (delete / rename / cast / compress / decompress)

#### 函数签名
```rust
impl TableHandle {
    pub fn delete_table_field(&self, name: &str) -> Result<(), CoreError>;
    pub fn rename_table_field(&self, old_name: &str, new_name: &str) -> Result<(), CoreError>;
    pub fn cast_table_field(&self, name: &str, target_type: DataType) -> Result<(), CoreError>;
    pub fn compress_table_field(&self, name: &str) -> Result<(), CoreError>;
    pub fn decompress_table_field(&self, name: &str) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&TableHandle` | 输入 | 表句柄 |
| `name` / `old_name` / `new_name` | `&str` | 输入 | 目标字段名称 |
| `target_type` | `DataType` | 输入 | 目标转换类型 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 跨分区操作完成 |

#### 内部实现流程
```
1. 获取所有分区列表；
2. 逐分区调用 DatasetHandle 对应的字段管理接口；
3. 失败立即返回首个错误；成功后刷新全表 Schema 缓存。
```

#### 其他说明
- 跨所有分区原子保持结构与数据类型的一致性。

---

### 4.11 scan_table

#### 函数签名
```rust
pub fn scan_table<'t>(
    table: &'t TableHandle,
    request: &TableScanRequest,
) -> Result<TableScanner<'t>, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table` | `&'t TableHandle` | 输入 | 表句柄 |
| `request` | `&TableScanRequest` | 输入 | 包含 sym、time、predicate、projection、limit、max_parallelism 的查询请求 |
| 返回 | `Result<TableScanner<'t>, CoreError>` | 输出 | 跨分区惰性扫描器 |

#### 内部实现流程
```
1. 分区裁剪（Partition Pruning）：
   - 基于 request.time 范围，二分筛选满足条件的分区子集（纯推导，零 META I/O）；
2. 构造 TableScanner：
   - 保持裁剪后的有序分区列表；
   - 携带 request.max_parallelism 并发度预算；
   - next() 时惰性打开当前分区，下推该并发配置至 DatasetHandle，调用 scan_dataset 返回 PartitionRowRange；
   - 保证任意时刻内存中至多打开一个分区的扫描器。
```

#### 其他说明
- 纯惰性流式迭代，支持全局 limit 提前早停（Early Termination）。

---

### 4.12 read_table

#### 函数签名
```rust
pub fn read_table<'t>(
    table: &'t TableHandle,
    scanner: TableScanner<'t>,
    batch_size: Option<usize>,
) -> Result<TableReader<'t>, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table` | `&'t TableHandle` | 输入 | 表句柄 |
| `scanner` | `TableScanner<'t>` | 输入 | 由 scan_table 产出的扫描器 |
| `batch_size` | `Option<usize>` | 输入 | 每批最大行数（None 为一 range 一批） |
| 返回 | `Result<TableReader<'t>, CoreError>` | 输出 | 跨分区流式数据批次读取器 |

#### 内部实现流程
```
1. 解析全局统一的 projection 列列表；
2. TableReader::next() 消费 scanner 产出的 PartitionRowRange：
   - 调用目标分区的 read_dataset(offset, length, projection)；
   - 【核心容错】：若某分区缺少某列，read_dataset 自动注入虚拟全 NULL 视图！
   - 保证输出的每个批次 DataView 具有完全一致的全局 Schema！
3. 若指定了 batch_size，按行截断与多段 ColumnView 零拷贝拼接聚合后返回。
```

#### 其他说明
- 彻底解决由于历史分区缺少新字段导致跨分区流式读取报错的问题，提供严格统一的 Arrow / Polars 友好视图。

---

### 4.13 write_table

#### 函数签名
```rust
pub fn write_table(table: &TableHandle, data: &DataView<'_>) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table` | `&TableHandle` | 输入 | 表句柄（Write 模式） |
| `data` | `&DataView<'_>` | 输入 | 待覆盖写入的数据视图（必须包含排好序的 sym 与 time） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 全部分区写入完成 |

#### 内部实现流程
```
1. 获取 .lock 文件互斥写锁；
2. 单遍线性扫描 data.time 列，按分区连续划分 run；
3. 主线程定位：针对各目标分区调用 locate_index 映射逻辑行区间，前置校验全部 key 存在；
4. 【自愈补建】：检查各目标分区是否缺失 data 中包含的列，若缺失则调用 create_field 补建 64B 占位文件；
5. 多线程分桶调度各分区调用 write_dataset 进行并行覆盖写；
6. 释放写锁。
```

#### 其他说明
- 自愈写入：自动识别并补齐各分区的缺失列，随后无缝执行覆盖写入。

---

### 4.14 update_table

#### 函数签名
```rust
pub fn update_table(table: &TableHandle, data: &DataView<'_>) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table` | `&TableHandle` | 输入 | 表句柄 |
| `data` | `&DataView<'_>` | 输入 | 替换后的完整数据视图 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 全表数据原子更新替换完成 |

#### 内部实现流程
```
1. 获取全表写锁；
2. 按时间分区切分输入数据；
3. 逐分区调用 update_dataset 原子替换数据；
4. 若产生全新分区，调用 init_dataset 自动创建；
5. 刷新全表缓存与元数据，释放写锁。
```

#### 其他说明
- 全量表数据滚动更新替换，具备崩溃安全性；局部行情打补丁/修改推荐使用微秒级覆盖写接口 `write_table`。

---

### 4.15 流式写入与追加（TableStreamWriter / create_partition）

#### 函数签名
```rust
pub struct TableStreamWriter { ... }

impl TableStreamWriter {
    pub fn new(table_path: &Path, scheme: PartitionScheme, options: Option<TableOptions>) -> Result<Self, CoreError>;
    pub fn write_partition(&mut self, partition_name: &str, data: &DataView<'_>) -> Result<(), CoreError>;
    pub fn write_batch(&mut self, batch: &DataView<'_>) -> Result<(), CoreError>;
    pub fn finish(self) -> Result<TableHandle, CoreError>;
}

impl TableHandle {
    pub fn create_partition(&self, partition_name: &str, data: &DataView<'_>, options: Option<CreateDatasetOptions>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `table_path` | `&Path` | 输入 | 表根目录路径 |
| `scheme` | `PartitionScheme` | 输入 | 表分区方案（None / Year / Month / Date） |
| `data` | `&DataView<'_>` | 输入 | 流式批次或单分区数据切片（零拷贝入参） |
| 返回（`finish`） | `Result<TableHandle, CoreError>` | 输出 | 构建完毕并打开的 TableHandle |

#### 内部实现流程
```
1. TableStreamWriter 初始化时获取 .lock 独占写锁；
2. write_partition 接收单分区切片，直接调用底层 create_dataset_from_view 落盘；
3. write_batch 自动根据时间键划分为分区连续片段，逐分区流式落盘；
4. 内存开销仅恒定在 O(单批次 / 单分区)，完备支持数百 GB 历史数据的 Out-of-Core 冷启动大灌库；
5. finish() 刷盘收尾并释放写锁，返回只读 TableHandle。
```
