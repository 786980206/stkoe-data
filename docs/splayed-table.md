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

## 4. Table API 详细设计（面向对象 Reader / Writer 体系）

### 4.0 总览与对象模型

Splayed 表层采用严格对称的面向对象体系：
- **`TableReader`**：只读表对象，负责多分区惰性扫描（`scan`）、细粒度点查（`read_range`）、一步式流式批次读取（`read`）与全局元数据查询；
- **`TableWriter`**：可写表对象，负责骨架创建（`create`）、数据初始化（`init`）、已有分区覆盖写与分发（`write`，支持自动建列缺列自愈）、全量替换更新（`update`）、分区管理与跨分区 DDL；
- **`TableScanner` 与 `TableBatchReader`**：`scanner.into_reader(batch_size)` 作为核心管道转换接口，将惰性剪枝后的行范围无缝转为流式批次数据。

| 核心操作 | 对象 / 方法 | 语义说明 |
| --- | --- | --- |
| **只读打开** | `TableReader::open(path)` / `open_with_options` | 打开只读表句柄，惰性缓存分区 |
| **创建骨架** | `TableWriter::create(path, schema, scheme, init_p, opts)` | 纯 Schema 驱动建立表骨架（64B Header-Only） |
| **数据初始化** | `TableWriter::init(path, scheme, data, opts)` | 连带数据初始化整张表（跨分区切分与多线程创建） |
| **可写打开** | `TableWriter::open(path)` / `open_with_options` | 打开可写表句柄，支持写入与结构操作 |
| **惰性扫描** | `reader.scan(request)` | 惰性分区时间剪枝，返回 `TableScanner` 迭代器 |
| **扫描转流** | `scanner.into_reader(batch_size)` | 将 `TableScanner` 转换为 `TableBatchReader` 流式批次读取器 |
| **一步式流读** | `reader.read(request, batch_size)` | 组合 scan 与 into_reader，直接返回流式批次读取器 |
| **细粒度点读** | `reader.read_range(range, projection)` | 零拷贝直接读取单个 `PartitionRowRange` 的 `DataView` |
| **覆盖写 / 批写** | `writer.write(data)` | 跨分区切分并分发写入（全表 `.lock` 互斥保护）；自动为缺失列建列对齐并写入数据 |
| **全量替换更新** | `writer.update(data)` | 跨分区全量替换更新数据，自动追加缺失新分区 |
| **物理完整性修复** | `writer.fix()` | 清理分区空文件夹，并以 `init_field` 将中间分区独有字段补齐到最新分区 |
| **分区生命周期** | `writer.delete_partition` | 物理删除指定分区目录（新分区由 write/update 自动创建） |
| **跨分区 DDL** | `writer.init_field` / `delete_field` / `rename_field` / `cast_field` / `compress_field` / `decompress_field` / `update_field` | 跨全部分区调度执行结构变更 |
| **转换为只读** | `writer.as_reader()` | 转换为 `TableReader` 读取当前已提交数据 |
| **安全关闭** | `reader.close()`, `writer.close()` | 安全释放 Mmap 句柄并刷盘缓存 |
| **物理销毁** | `writer.remove()` | 安全释放句柄并在磁盘物理递归删除整表目录 |

---

### 4.1 TableReader 接口定义

```rust
pub struct TableReader { ... }

impl TableReader {
    pub fn open(path: &Path) -> Result<Self, CoreError>;
    pub fn open_with_options(path: &Path, options: TableOptions) -> Result<Self, CoreError>;

    pub fn scan(&self, request: TableScanRequest) -> Result<TableScanner<'_>, CoreError>;
    pub fn read<'t>(&'t self, request: TableScanRequest, batch_size: Option<usize>) -> Result<TableBatchReader<'t>, CoreError>;
    pub fn read_range(&self, range: &PartitionRowRange, projection: Option<&[&str]>) -> Result<DataView<'_>, CoreError>;

    pub fn schema(&self) -> Result<Schema, CoreError>;
    pub fn metadata(&self) -> Result<TableMetadata, CoreError>;
    pub fn statistics(&self) -> Result<TableStatistics, CoreError>;

    pub fn max_parallelism(&self) -> usize;
    pub fn set_max_parallelism(&mut self, max_parallelism: usize);

    pub fn path(&self) -> &Path;
    pub fn scheme(&self) -> PartitionScheme;
    pub fn close(self) -> Result<(), CoreError>;
}
```

### 4.2 TableWriter 接口定义

```rust
pub struct TableWriter { ... }

impl TableWriter {
    pub fn create(
        path: &Path,
        schema: &Schema,
        scheme: PartitionScheme,
        initial_partition: Option<&str>,
        options: Option<TableOptions>,
    ) -> Result<Self, CoreError>;

    pub fn init(
        path: &Path,
        scheme: PartitionScheme,
        data: &DataView<'_>,
        options: Option<TableOptions>,
    ) -> Result<Self, CoreError>;

    pub fn open(path: &Path) -> Result<Self, CoreError>;
    pub fn open_with_options(path: &Path, options: TableOptions) -> Result<Self, CoreError>;

    pub fn write(&self, data: &DataView<'_>) -> Result<(), CoreError>;
    pub fn update(&self, data: &DataView<'_>) -> Result<(), CoreError>;

    pub fn delete_partition(&self, partition_name: &str) -> Result<(), CoreError>;

    pub fn init_field(&self, field_name: &str, data_type: DataType, opts: Option<CreateFieldOptions>) -> Result<(), CoreError>;
    pub fn delete_field(&self, field: &str) -> Result<(), CoreError>;
    pub fn rename_field(&self, field: &str, new_name: &str) -> Result<(), CoreError>;
    pub fn cast_field(&self, field: &str, target_type: DataType) -> Result<(), CoreError>;
    pub fn compress_field(&self, field: &str) -> Result<(), CoreError>;
    pub fn decompress_field(&self, field: &str) -> Result<(), CoreError>;
    pub fn update_field(&self, field: &str, header: &FieldHeader) -> Result<(), CoreError>;
    pub fn fix(&self) -> Result<(), CoreError>;

    pub fn as_reader(&self) -> Result<TableReader, CoreError>;

    pub fn schema(&self) -> Result<Schema, CoreError>;
    pub fn metadata(&self) -> Result<TableMetadata, CoreError>;
    pub fn statistics(&self) -> Result<TableStatistics, CoreError>;
    pub fn close(self) -> Result<(), CoreError>;
    pub fn remove(self) -> Result<(), CoreError>;
}
```

---

### 4.1 TableWriter::create (纯 Schema 骨架创建)

#### 函数签名
```rust
impl TableWriter {
    pub fn create(
        path: &Path,
        schema: &Schema,
        scheme: PartitionScheme,
        initial_partition: Option<&str>,
        options: Option<TableOptions>,
    ) -> Result<Self, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Table 根目录；必须不存在或为空 |
| `schema` | `&Schema` | 输入 | 表的初始逻辑 Schema（必须含 sym 与 time） |
| `scheme` | `PartitionScheme` | 输入 | `none | year | month | date` 四选一 |
| `initial_partition` | `Option<&str>` | 输入 | 初始预建的最新分区名；None 自动根据当前公历推断（如 `month=2026-03`） |
| `options` | `Option<TableOptions>` | 输入 | 并行度与压缩等配置项 |
| 返回 | `Result<TableWriter, CoreError>` | 输出 | 创建成功的可写表对象 |

#### 内部实现流程
```
1. 校验 path 不存在或为空，创建根目录 fs::create_dir_all(path)；
2. 校验 schema 包含合法的 sym 与 time 字段；
3. 若 scheme == PartitionScheme::None：
   → 直接在根目录创建单一 Dataset 空骨架；
4. 若为分区模式（Year / Month / Date）：
   → 提取指定或默认推断的最新分区名（如 month=2026-03）；
   → 在对应分区路径下完整初始化最新分区的 64B Header-Only 空骨架；
5. 打开并返回 TableWriter 对象。
```

#### 其他说明
- 物理磁盘上仅生成各字段 64B Header-Only 空文件，0 实际数据 I/O。
- 表创建完成后立即拥有完整的 Schema 上下文，`writer.schema()` 可立即读取，无需等待任何数据落盘。

---

### 4.2 TableWriter::init (连带数据初始化)

#### 函数签名
```rust
impl TableWriter {
    pub fn init(
        path: &Path,
        scheme: PartitionScheme,
        data: &DataView<'_>,
        options: Option<TableOptions>,
    ) -> Result<Self, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Table 根目录；必须不存在 |
| `scheme` | `PartitionScheme` | 输入 | 分区方案 |
| `data` | `&DataView<'_>` | 输入 | 包含 sym, time 及数据字段的完整初始数据 |
| `options` | `Option<TableOptions>` | 输入 | 并行度与压缩分块配置 |
| 返回 | `Result<TableWriter, CoreError>` | 输出 | 构建完成的可写表对象 |

#### 内部实现流程
```
1. 校验 path 不存在且 data 包含按 (sym ASC, time ASC) 排序的有效数据；
2. 若 scheme == PartitionScheme::None：
   → 调用底层 Dataset 直写根目录；
3. 若为时间分区：
   - 主线程单遍线性扫描 time 列：计算连续行片段 RowSpan，分组为各分区 run；
   - 预算切分：P_part = min(max_parallelism, 分区数)，P_field = max(1, max_parallelism / P_part)；
   - std::thread::scope 启动并行线程：每个线程执行 gather_runs 批量拼接后落盘；
4. 打开并返回 TableWriter 对象。
```

---

### 4.3 TableReader::open 与 TableWriter::open

#### 函数签名
```rust
impl TableReader {
    pub fn open(path: &Path) -> Result<Self, CoreError>;
    pub fn open_with_options(path: &Path, options: TableOptions) -> Result<Self, CoreError>;
}

impl TableWriter {
    pub fn open(path: &Path) -> Result<Self, CoreError>;
    pub fn open_with_options(path: &Path, options: TableOptions) -> Result<Self, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 已存在的 Table 目录路径 |
| `options` | `TableOptions` | 输入 | 并行度、压缩模式等配置项 |
| 返回 | `Result<TableReader / TableWriter, CoreError>` | 输出 | 只读或可写表生命周期对象 |

#### 内部实现流程
```
1. 校验 path 存在且为目录；
2. 发现分区：扫描子目录名推断 partition_scheme；若根目录含有 .meta 则为 None scheme；
3. 初始化底层句柄，构建惰性 Dataset 缓存池与统计缓存；
4. 返回对应 TableReader 或 TableWriter 实例。
```

---

### 4.4 TableReader::scan (惰性分区剪枝扫描)

#### 函数签名
```rust
impl TableReader {
    pub fn scan(&self, request: TableScanRequest) -> Result<TableScanner<'_>, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&TableReader` | 输入 | 只读表对象借用 |
| `request` | `TableScanRequest` | 输入 | 包含 sym、time、predicate、projection、limit、max_parallelism 的查询请求 |
| 返回 | `Result<TableScanner<'_>, CoreError>` | 输出 | 跨分区惰性扫描器 |

#### 内部实现流程
```
1. 分区裁剪（Partition Pruning）：基于 request.time 范围，二分筛选满足条件的分区子集（纯推导，零 META I/O）；
2. 构造 TableScanner：持有裁剪后的有序分区列表；
3. TableScanner::next() 时惰性打开当前分区并下推并发配置，调用 ds.scan 返回 PartitionRowRange；
4. 保证任意时刻内存中至多打开一个分区的扫描器，支持全局 limit 提前早停。
```

---

### 4.5 TableScanner::into_reader 与 TableReader::read (流式读取消费)

#### 函数签名
```rust
impl<'t> TableScanner<'t> {
    /// 核心流式转换接口：将 scanner 直接无缝转换为流式批次读取器
    pub fn into_reader(self, batch_size: Option<usize>) -> TableBatchReader<'t>;
}

impl TableReader {
    /// 一步式流式读取便捷方法
    pub fn read<'t>(&'t self, request: TableScanRequest, batch_size: Option<usize>) -> Result<TableBatchReader<'t>, CoreError>;
}

impl<'t> TableBatchReader<'t> {
    pub fn next(&mut self) -> Result<Option<DataView<'_>>, CoreError>;
    pub fn close(self) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `batch_size` | `Option<usize>` | 输入 | 每批最大行数（None 为一 range 一批） |
| 返回 | `TableBatchReader<'t>` | 输出 | 跨分区流式数据批次读取器 |

#### 内部实现流程
```
1. TableScanner::into_reader 将已裁剪的扫描计划移交给 TableBatchReader；
2. 批次读取器按需消费扫描出的行范围：
   - 逐段读取底层各分区数据视图；
   - 遇到历史冷分区缺少新列时，自动补齐零开销虚拟全 NULL 视图，确保全批次 Schema 统一；
3. 支持按 batch_size 跨 range 零拷贝聚合成整批。
```

---

### 4.6 TableReader::read_range (细粒度单范围点读)

#### 函数签名
```rust
impl TableReader {
    pub fn read_range(
        &self,
        range: &PartitionRowRange,
        projection: Option<&[&str]>,
    ) -> Result<DataView<'_>, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&TableReader` | 输入 | 只读表对象 |
| `range` | `&PartitionRowRange` | 输入 | 指定分区的精确逻辑行范围 |
| `projection` | `Option<&[&str]>` | 输入 | 选定读取的字段列表 |
| 返回 | `Result<DataView<'_>, CoreError>` | 输出 | 借用底层持久 Mmap 的零拷贝数据视图 |

---

### 4.7 TableWriter::write (统一步长与流式批次覆盖写)

#### 函数签名
```rust
impl TableWriter {
    pub fn write(&self, data: &DataView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&TableWriter` | 输入 | 可写表对象（内部并发加锁保护） |
| `data` | `&DataView<'_>` | 输入 | 待写入数据视图（自动支持单分区或跨多时区流式批次） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 写入完成 |

#### 内部实现流程
```
1. 获取全表 .lock 互斥写锁；
2. 单遍扫描 data.time 列，按分区连续切分数据；
3. 前置二分校验所有分区与时序键存在；
4. 自动自愈补建各分区缺失的字段（生成 64B 占位文件）；
5. 多线程调度各分区调用底层 ds.write 执行原地覆盖写；
6. 释放写锁。
```

---

### 4.8 TableWriter::update (全量替换更新表数据)

#### 函数签名
```rust
impl TableWriter {
    pub fn update(&self, data: &DataView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&TableWriter` | 输入 | 可写表对象 |
| `data` | `&DataView<'_>` | 输入 | 替换后的完整数据视图 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 全表数据原子更新替换完成 |

#### 内部实现流程
```
1. 获取全表写锁；
2. 按时间分区切分输入数据；
3. 逐分区执行原子更新替换；若发现全新时间分区，自动创建并落盘；
4. 刷新全表缓存与元数据，释放写锁。
```

---

### 4.9 TableWriter 分区生命周期管理

#### 函数签名
```rust
impl TableWriter {
    pub fn delete_partition(&self, partition_name: &str) -> Result<(), CoreError>;
}
```

- **`delete_partition`**：物理删除指定分区的数据目录（自动安全解除 Windows Mmap 占用；新分区由 `write` 或 `update` 根据时间自动创建）。

---

### 4.10 TableWriter 跨分区 DDL 结构变更

#### 函数签名
```rust
impl TableWriter {
    pub fn init_field(&self, field_name: &str, data_type: DataType, opts: Option<CreateFieldOptions>) -> Result<(), CoreError>;
    pub fn delete_field(&self, field: &str) -> Result<(), CoreError>;
    pub fn rename_field(&self, field: &str, new_name: &str) -> Result<(), CoreError>;
    pub fn cast_field(&self, field: &str, target_type: DataType) -> Result<(), CoreError>;
    pub fn compress_field(&self, field: &str) -> Result<(), CoreError>;
    pub fn decompress_field(&self, field: &str) -> Result<(), CoreError>;
    pub fn update_field(&self, field: &str, header: &FieldHeader) -> Result<(), CoreError>;
}
```

- **`init_field`**：在已有表中新增单列，只在自然排序的最新分区创建物理字段文件，无需指定 rows，自动与最新分区的 index 行数（row_count）对齐；产出 64B Header-Only 延迟展开全 NULL 骨架，零磁盘数据页分配；
- 跨所有已发现分区级联调度执行 `delete_field` / `rename_field` / `cast_field` 等结构变更，操作前全面释放 mmap，变更完成后原子刷新全表统合 Schema。

---

### 4.11 物理完整性修复 (fix)

#### 函数签名
```rust
impl TableWriter {
    pub fn fix(&self) -> Result<(), CoreError>;
}
```

- **空子目录清理**：自底向上递归扫描每个分区目录，清理因删除嵌套字段（如 `factor.alpha`）后遗留的无用空文件夹；
- **历史分区字段向前对齐补齐**：检查是否存在历史/中间分区中存在、但最新分区缺失的字段；若存在，自动在最新分区中通过 `init_field` 以最新分区的当前逻辑行数创建全 NULL 延迟展开骨架（64B Header-Only，零数据页分配），并刷新元数据缓存，使全表统合 Schema 恢复一致。

---

### 4.12 安全关闭与物理销毁 (close 与 remove)

#### 函数签名
```rust
impl TableReader {
    pub fn close(self) -> Result<(), CoreError>;
}

impl TableWriter {
    pub fn close(self) -> Result<(), CoreError>;
    pub fn remove(self) -> Result<(), CoreError>;
}
```

- **`close(self)`**：消费自身所有权，安全刷盘并解除底层全部分区与字段的 Mmap 映射；
- **`remove(self)`**：消费自身所有权，显式 `close` 释放全部句柄后，从物理磁盘递归删除整张表目录。
