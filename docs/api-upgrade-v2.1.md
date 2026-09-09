# Splayed V2.1 统一四层接口升级架构设计与实施规格书

> **文档性质**：Splayed V2.1 核心公共 API 升级权威技术设计方案。
> **适用范围**：`splayed-format`、`splayed-codec`、`splayed-core`、`splayed-table`、`splayed-arrow`、`splayed-polars`。

---

## 一、 背景与架构目标

### 1.1 现状与升级动因
当前 Splayed V2.0 确立了高性能时序存储的核心体系（容量网格、LSB-first Bitmap、零拷贝 mmap 切片、SIMD 向量化求值、Header-Only 全 NULL 字段延迟展开）。但在 API 组织层面存在以下可改进点：
1. **层次对称性不完整**：`Field`、`Dataset`、`Table` 拥有 Handle 抽象，但 `.meta`（主键网格）仅作为内部细节通过 `meta_file` 提供，外部无法独立操作网格。
2. **骨架与数据绑定过紧**：创建接口（如 `create_table`、`create_dataset`）大多要求必须传入初始全量数据 `Data`，缺少纯元数据骨架创建能力。
3. **扁平单级字段名限制**：字段名禁止包含路径分隔符，无法自然支持多因子空间（如 `factor.momentum.ret20`）的多级目录分类。
4. **缺少缺失列容错机制**：多分区表中新增列后，历史冷分区未落盘时读取会抛出 `NotFound` 或 `Invalid`，破坏全表统一读取体验。

### 1.2 核心架构原则（Invariants）
1. **四层严格对称（Table → Dataset → Index → Field）**：
   - **Table（表级）**：跨分区统一门面、Hive 分区发现与调度。
   - **Dataset（分区级）**：单分区容量网格与多字段协调器。
   - **Index（网格级）**：独立的 `.meta` 索引网格句柄 `IndexHandle`。
   - **Field（字段级）**：单字段物理文件句柄 `FieldHandle`。
2. **性能第一（Zero-Cost Abstraction）**：
   - **零数据复制**：读路径全程为 mmap 切片引用，返回 `ColumnView` / `DataView`。
   - **零物化虚拟列**：`sym` 列在 Index 层以 `RepeatDict` 零物化提供；缺失列以共享静态零页与全 0 `BitmapView` 零 I/O、零堆分配提供。
   - **写入高吞吐**：定位走数学网格二分（$O(\log S + \log T)$），写入走跨分区/跨字段多线程无锁分桶并行。
3. **延迟物理展开（Lazy Expansion）**：
   - `create_field`、`create_index` 与全 NULL 字段物理磁盘仅占用 64 字节 Header，首次执行覆盖写入时就地扩展。
4. **Windows 平台句柄安全释放规则**：
   - 所有文件替换（`update_*`、`cast`、`compress`、`drop_*`）在 Windows 下执行 `rename` 或 `remove` 前，必须主动释放原有 `Mmap` / `MmapMut` 和文件句柄。

---

## 二、 完整四层面向对象 API 对照矩阵

| 业务分类 | Table 表级 (`splayed-table`) | Dataset 分区级 (`splayed-core`) | Index 网格级 (`splayed-core`) | Field 字段级 (`splayed-core`) |
| :--- | :--- | :--- | :--- | :--- |
| **只读对象** | `TableReader` | `DatasetReader` | `IndexReader` | `FieldReader` |
| **可写对象** | `TableWriter` | `DatasetWriter` | `IndexWriter` | `FieldWriter` |
| **创建骨架** | `TableWriter::create(...)` | `DatasetWriter::create(...)` | `IndexWriter::create(...)` | `FieldWriter::create(...)` |
| **初始化数据**| `TableWriter::init(...)` | `DatasetWriter::init(...)` | `IndexWriter::init(...)` | `FieldWriter::init(...)` |
| **打开只读** | `TableReader::open(...)` | `DatasetReader::open(...)` | `IndexReader::open(...)` | `FieldReader::open(...)` |
| **打开可写** | `TableWriter::open(...)` | `DatasetWriter::open(...)` | `IndexWriter::open(...)` | `FieldWriter::open(...)` |
| **关闭对象** | `reader.close()`, `writer.close()` | `reader.close()`, `writer.close()` | `reader.close()`, `writer.close()` | `reader.close()`, `writer.close()` |
| **物理销毁** | `writer.remove()` | `writer.remove()` | `writer.remove()` | `writer.remove()` |
| **结构查询** | `reader.schema()` / `writer.schema()` | `reader.schema()` / `writer.schema()` | `reader.schema()` / `writer.schema()` | `reader.schema()` / `writer.schema()` |
| **新增字段** | `writer.init_field(...)` | `writer.create_field(...)` | — | — |
| **删除字段** | `writer.delete_field(...)` | `writer.delete_field(...)` | — | — |
| **重命名字段**| `writer.rename_field(...)` | `writer.rename_field(...)` | — | `writer.rename(...)` |
| **类型转换** | `writer.cast_field(...)` | `writer.cast_field(...)` | — | `writer.cast(...)` |
| **压缩管理** | `writer.compress_field` / `decompress` | `writer.compress_field` / `decompress` | — | `writer.compress` / `decompress` |
| **条件扫描** | `reader.scan(request)` | `reader.scan(request)` | `reader.scan(request)` | `reader.scan(request)` |
| **扫描转流** | `scanner.into_reader(batch_size)` | — | — | — |
| **范围读取** | `reader.read(request, batch_size)` | `reader.read(offset, len, cols)` | `reader.read(offset, len)` | `reader.read(offset, len)` |
| **单范围点读**| `reader.read_range(range, proj)` | — | — | — |
| **主键批量定位**| — | `reader.locate(pairs)` | `reader.locate(pairs)` | — |
| **位置覆盖写**| `writer.write(dataview)` | `writer.write(offset, dataview)` | — | `writer.write(offset, colview)` |
| **全量替换更新**| `writer.update(dataview)` | `writer.update(dataview)` | `writer.update(dataview)` | `writer.update(colview)` |
| **写入数据（自动建列）** | `writer.write(dataview)` | — | — | — |
| **分区管理** | `writer.delete_partition` | — | — | — |

---

## 三、 详细类型与函数签名设计

### 3.1 字段层：`Field` API (`splayed-core::field_file`)

```rust
// 1. 只读字段对象
pub struct FieldReader { ... }

impl FieldReader {
    pub fn open(path: &Path) -> Result<Self, CoreError>;
    pub fn read(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError>;
    pub fn scan(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError>;
    pub fn schema(&self) -> FieldSchema;
    pub fn row_count(&self) -> u64;
    pub fn close(self) -> Result<(), CoreError>;
}

// 2. 可写字段对象
pub struct FieldWriter { ... }

impl FieldWriter {
    pub fn create(path: &Path, field_type: DataType) -> Result<Self, CoreError>;
    pub fn init(path: &Path, data: &ColumnView<'_>, options: Option<CreateFieldOptions>) -> Result<Self, CoreError>;
    pub fn open(path: &Path) -> Result<Self, CoreError>;

    pub fn write(&mut self, offset: u64, data: &ColumnView<'_>) -> Result<(), CoreError>;
    pub fn update(&mut self, data: &ColumnView<'_>) -> Result<(), CoreError>;
    pub fn rename(&mut self, new_name: &str) -> Result<(), CoreError>;
    pub fn cast(&mut self, target_type: DataType) -> Result<(), CoreError>;
    pub fn compress(&mut self) -> Result<(), CoreError>;
    pub fn decompress(&mut self) -> Result<(), CoreError>;
    pub fn update_header(&mut self, header: FieldHeader) -> Result<(), CoreError>;

    pub fn schema(&self) -> FieldSchema;
    pub fn as_reader(&self) -> Result<FieldReader, CoreError>;
    pub fn close(self) -> Result<(), CoreError>;
    pub fn remove(self) -> Result<(), CoreError>;
}
```

### 3.2 索引层：`Index` API (`splayed-core::meta_file`)

```rust
// 1. 只读索引对象
pub struct IndexReader { ... }

impl IndexReader {
    pub fn open(path: &Path) -> Result<Self, CoreError>;
    pub fn read(&self, offset: u64, length: u64) -> Result<DataView<'_>, CoreError>;
    pub fn scan(&self, request: &ScanRequest) -> Result<IndexScanner, CoreError>;
    pub fn locate(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError>;
    pub fn locate_borrowed(&self, pairs: &[(&str, i64)]) -> Result<Vec<RowRange>, CoreError>;
    pub fn schema(&self) -> Schema;
    pub fn info(&self) -> MetaInfo;
    pub fn close(self) -> Result<(), CoreError>;
}

// 2. 可写索引对象
pub struct IndexWriter { ... }

impl IndexWriter {
    pub fn create(path: &Path, time_type: TimeType) -> Result<Self, CoreError>;
    pub fn init(path: &Path, data: &DataView<'_>) -> Result<Self, CoreError>;
    pub fn open(path: &Path) -> Result<Self, CoreError>;

    pub fn update(&mut self, data: &DataView<'_>) -> Result<(), CoreError>;
    pub fn schema(&self) -> Schema;
    pub fn as_reader(&self) -> Result<IndexReader, CoreError>;
    pub fn close(self) -> Result<(), CoreError>;
    pub fn remove(self) -> Result<(), CoreError>;
}
```

### 3.3 数据集层：`Dataset` API (`splayed-core::dataset`)

```rust
// 1. 只读数据集对象
pub struct DatasetReader { ... }

impl DatasetReader {
    pub fn open(path: &Path) -> Result<Self, CoreError>;
    pub fn read(&self, offset: u64, length: u64, projection: Option<&[&str]>) -> Result<DataView<'_>, CoreError>;
    pub fn scan(&self, request: &ScanRequest) -> Result<DatasetScanner, CoreError>;
    pub fn locate(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError>;
    pub fn locate_borrowed(&self, pairs: &[(&str, i64)]) -> Result<Vec<RowRange>, CoreError>;
    pub fn schema(&self) -> Schema;
    pub fn statistics(&self) -> Result<DatasetStatistics, CoreError>;
    pub fn max_parallelism(&self) -> usize;
    pub fn set_max_parallelism(&self, max_parallelism: usize);
    pub fn close(self) -> Result<(), CoreError>;
}

// 2. 可写数据集对象
pub struct DatasetWriter { ... }

impl DatasetWriter {
    pub fn create(path: &Path, schema: &Schema) -> Result<Self, CoreError>;
    pub fn init(path: &Path, data: &DataView<'_>, options: Option<CreateDatasetOptions>) -> Result<Self, CoreError>;
    pub fn open(path: &Path) -> Result<Self, CoreError>;

    pub fn write(&self, offset: u64, data: &DataView<'_>) -> Result<(), CoreError>;
    pub fn update(&mut self, data: &DataView<'_>) -> Result<(), CoreError>;

    pub fn create_field(&mut self, name: &str, data_type: DataType, init: DatasetFieldInit, options: CreateFieldOptions) -> Result<(), CoreError>;
    pub fn delete_field(&mut self, name: &str) -> Result<(), CoreError>;
    pub fn rename_field(&mut self, name: &str, new_name: &str) -> Result<(), CoreError>;
    pub fn cast_field(&mut self, name: &str, target_type: DataType) -> Result<(), CoreError>;
    pub fn compress_field(&mut self, name: &str) -> Result<(), CoreError>;
    pub fn decompress_field(&mut self, name: &str) -> Result<(), CoreError>;
    pub fn update_field(&self, name: &str, header: &FieldHeader) -> Result<(), CoreError>;

    pub fn schema(&self) -> Schema;
    pub fn statistics(&self) -> Result<DatasetStatistics, CoreError>;
    pub fn as_reader(&self) -> Result<DatasetReader, CoreError>;
    pub fn close(self) -> Result<(), CoreError>;
    pub fn remove(self) -> Result<(), CoreError>;
}
```

### 3.4 表层：`Table` API (`splayed-table`)

```rust
// 1. 只读表对象
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
    pub fn close(self) -> Result<(), CoreError>;
}

// 2. 扫描器与流式批次转换
pub struct TableScanner<'t> { ... }
impl<'t> TableScanner<'t> {
    pub fn next(&mut self) -> Result<Option<PartitionRowRange>, CoreError>;
    pub fn into_reader(self, batch_size: Option<usize>) -> TableBatchReader<'t>;
    pub fn close(self) -> Result<(), CoreError>;
}

// 3. 可写表对象（统一支持单分区/跨分区流式批次写与 DDL）
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

    pub fn init_field(&self, field: &str, data_type: DataType, init: TableFieldInit) -> Result<(), CoreError>;
    pub fn delete_field(&self, field: &str) -> Result<(), CoreError>;
    pub fn rename_field(&self, field: &str, new_name: &str) -> Result<(), CoreError>;
    pub fn cast_field(&self, field: &str, target_type: DataType) -> Result<(), CoreError>;
    pub fn compress_field(&self, field: &str) -> Result<(), CoreError>;
    pub fn decompress_field(&self, field: &str) -> Result<(), CoreError>;
    pub fn update_field(&self, field: &str, header: &FieldHeader) -> Result<(), CoreError>;

    pub fn as_reader(&self) -> Result<TableReader, CoreError>;
    pub fn schema(&self) -> Result<Schema, CoreError>;
    pub fn metadata(&self) -> Result<TableMetadata, CoreError>;
    pub fn statistics(&self) -> Result<TableStatistics, CoreError>;
    pub fn close(self) -> Result<(), CoreError>;
    pub fn remove(self) -> Result<(), CoreError>;
}

---

## 四、 关键新特性底层高性能技术实现方案

### 4.1 多级嵌套字段（Dot-separated fields）
- **路径双向映射**：
  - 字段名 `"factor.momentum.ret20"` 映射到 `<root>/factor/momentum/ret20`。
  - 创建时提取父目录并通过 `fs::create_dir_all` 预建。
- **Schema 递归发现（DFS）**：
  - 遍历所有子目录，跳过 `.` 开头（如 `.meta`、`.tmp`、`.lock`）和 `.tmp` 结尾项。
  - 常规文件且大小 $\ge 64$ 字节且前 8 字节匹配 `SPLAYFLD` 的视为字段文件。
  - 将相对路径以 `.` 连接还原为字段全名，结果按字典序排列并缓存。

### 4.2 缺失列零开销虚拟补 NULL（Zero-Cost Null Projection）
- **实现机制**：
  - 在 `splayed-format` 中提供共享零页与全 0 位图构造器：
    `ColumnSegment::new_virtual_null(data_type, rows)`。
  - 内部 `values` 引用全局只读零页 `STATIC_ZERO_BUFFER`（无需堆分配）；`validity` 构造全 0 `BitmapView`。
  - 当 Dataset/Field 的 `read` 发现请求列在当前物理磁盘上不存在时，直接生成虚拟 NULL 段，耗时 $<10\text{ns}$，不产生磁盘 I/O。

### 4.3 写入与更新时缺失列自愈补建（Auto-Creation）
- **流水线**：
  1. 单线程规划阶段比对 `data.schema` 与目标分区的物理存在状态。
  2. 对缺失的列调用 `create_field(&path, dtype)` 写入 64B 占位文件，并注入当前 Dataset 的字段句柄缓存。
  3. 后续多线程并发写入时，因字段为 64B Header-Only 状态，会自动触发 `expand_header_only_file()` 就地扩展磁盘并覆盖写入。

### 4.4 `update_*` 全量数据替换与原子提交
- **安全提交协议**：
  1. 写入临时文件 `<path>.update.<pid>.<seq>.tmp`。
  2. `sync_all()` 确保落盘。
  3. **Windows 句柄解绑**：原 Handle 将 `Backing` 置为 `Empty`，主动 drop 掉 mmap 引用。
  4. `fs::rename(&tmp, &path)` 完成物理原子替换。
  5. 重新建立 mmap 映射更新 Handle 内部状态。

---

## 五、 实施计划与平滑迁移路线图

```
┌─────────────────────────────────────────────────────────────┐
│ 阶段一：底层独立与对称完善（Field & Index）                 │
│ • 新建 crates/splayed-core/src/index.rs，暴露 IndexHandle   │
│ • 扩充 create_field (64B 空头)、init_field、open_field、drop│
│ • 保持现有 open_field_file、close_field_handle 别名兼容     │
│ • 补齐单元测试与边界测试                                    │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段二：Dataset 增强（Dot 多级目录 + 缺失列补 NULL）        │
│ • 升级 field_path 支持 "a.b.c" 路径映射                      │
│ • schema (read_dataset_schema) 递归 DFS 发现并还原嵌套字段   │
│ • create_dataset 骨架构建与 init_dataset 带数据构建         │
│ • read (read_dataset) 缺列自动补 NULL 虚拟列                │
│ • write (write_dataset) 缺列自愈建列与 update 全量替换      │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段三：Table 表级升级（Schema 一致性与分区自愈）           │
│ • 实现 create_table 骨架初始化与 init_table 数据建表        │
│ • read_table 跨分区自动补 NULL 列，保证 Schema 严格对齐     │
│ • write_table 缺列自愈建列与 update_table 全表替换          │
│ • 整合 delete_table(handle, partition) 与 drop_table        │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 阶段四：适配器同步、弃用标记与全面验证                      │
│ • splayed-arrow / splayed-polars 适配新接口                 │
│ • 旧 API 标注 #[deprecated(since = "0.3.0")] 内部桥接       │
│ • 全工作区 cargo test --workspace 确保零警告、100% 通过     │
│ • 性能基准复测，确保核心读写零退化                          │
└─────────────────────────────────────────────────────────────┘
```
