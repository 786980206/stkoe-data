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

## 二、 完整四层 API 对照矩阵

| 业务分类 | Table 表级 (`splayed-table`) | Dataset 分区级 (`splayed-core`) | Index 网格级 (`splayed-core`) | Field 字段级 (`splayed-core`) |
| :--- | :--- | :--- | :--- | :--- |
| **创建骨架** | `create_table(path, schema, partition_scheme, options)` | `create_dataset(path, schema)` | `create_index(path, time_type)` | `create_field(path, field_type)` |
| **初始化数据**| `init_table(path, partition_scheme, dataview, options)` | `init_dataset(path, dataview, options)` | `init_index(path, dataview)` | `init_field(path, columnview, options)` |
| **打开句柄** | `open_table(path, mode, options)` | `open_dataset(path, mode)` | `open_index(path)` | `open_field(path, mode)` |
| **关闭句柄** | `close_table(handle)` | `close_dataset(handle)` | `close_index(handle)` | `close_field(handle)` |
| **销毁删除** | `drop_table(handle)` | `drop_dataset(handle)` | `drop_index(handle)` | `drop_field(handle)` |
| **结构查询** | `read_table_schema(tablehandle)` | `read_dataset_schema(path / handle)` | `read_index_schema(indexhandle)` | `read_field_schema(fieldhandle)` |
| **新增字段** | `create_table_field(table, name, type)` | `create_dataset_field(ds, name, type)`| — | — |
| **删除字段** | `delete_table_field(table, name)` | `delete_dataset_field(ds, name)` | — | `delete_field_file(handle / path)` |
| **重命名字段**| `rename_table_field(table, old, new)` | `rename_dataset_field(ds, old, new)`| — | `rename_field(path, new_name)` |
| **类型转换** | `cast_table_field(table, name, type)` | `cast_dataset_field(ds, name, type)` | — | `cast_field(handle, type)` |
| **压缩管理** | `compress_table_field` / `decompress` | `compress_dataset_field` / `decompress` | — | `compress_field` / `decompress` |
| **条件扫描** | `scan_table(table, request)` | `scan_dataset(ds, request)` | `scan_index(index, request)` | `scan_field(field, request)` |
| **范围读取** | `read_table(table, scanner, batch_size)`| `read_dataset(ds, offset, len, cols)`| `read_index(index, offset, len)` | `read_field(field, offset, len)` |
| **位置覆盖写**| `write_table(table, dataview)` | `write_dataset(ds, offset, dataview)`| — | `write_field(field, offset, colview)` |
| **全量替换更新**| `update_table(table, dataview)` | `update_dataset(ds, dataview)` | `update_index(index, dataview)` | `update_field(field, columnview)` |
| **分区数据删除**| `delete_table(table, partition)` | — | — | — |
| **重命名对象**| `rename_table(path/handle, new_name)` | `rename_dataset(path/handle, new_name)`| — | — |

---

## 三、 详细类型与函数签名设计

### 3.1 字段层：`Field` API (`splayed-core::field_file`)

```rust
// 1. 创建与初始化
/// 创建仅包含 64 字节文件头的空字段文件（row_count = 0, null_count = 0, data_length = 0）。
pub fn create_field(path: &Path, field_type: DataType) -> Result<(), CoreError>;

/// 连带数据直接初始化创建字段文件（带数据一步直写，全 NULL 产出 64B Header-Only 文件）。
pub fn init_field(
    path: &Path,
    column: &ColumnView<'_>,
    options: Option<CreateFieldOptions>,
) -> Result<(), CoreError>;

// 2. 生命周期与句柄操作
pub fn open_field(path: &Path, mode: Mode) -> Result<FieldHandle, CoreError>;
pub fn close_field(handle: FieldHandle) -> Result<(), CoreError>;
pub fn drop_field(path: &Path) -> Result<(), CoreError>;
pub fn drop_field_handle(handle: FieldHandle) -> Result<(), CoreError>;

// 3. 元数据与物理转换（基于物理路径，规避 Mmap 共享冲突与陈旧句柄）
pub fn read_field_schema(path: &Path) -> Result<FieldSchema, CoreError>;
pub fn rename_field(path: &Path, new_name: &str) -> Result<(), CoreError>;
pub fn cast_field(path: &Path, target_type: DataType) -> Result<(), CoreError>;
pub fn compress_field(path: &Path, offsets: Option<Vec<u64>>) -> Result<(), CoreError>;
pub fn decompress_field(path: &Path) -> Result<(), CoreError>;

// 4. FieldHandle 方法
impl FieldHandle {
    pub fn read_field_schema(&self) -> FieldSchema;
    pub fn read_field(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError>;
    pub fn write_field(&mut self, offset: u64, data: &ColumnView<'_>) -> Result<(), CoreError>;
    pub fn update_field(&mut self, data: &ColumnView<'_>) -> Result<(), CoreError>;
    pub fn scan_field(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError>;
}
```

### 3.2 索引层：`Index` API (`splayed-core::index`)

```rust
pub struct IndexHandle {
    path: PathBuf,
    mmap: memmap2::Mmap,
    header: MetaHeader,
}

// 1. 自由函数
pub fn create_index(path: &Path, time_type: TimeType) -> Result<IndexHandle, CoreError>;
pub fn init_index(path: &Path, data: &DataView<'_>) -> Result<IndexHandle, CoreError>;
pub fn open_index(path: &Path) -> Result<IndexHandle, CoreError>;
pub fn close_index(handle: IndexHandle) -> Result<(), CoreError>;
pub fn drop_index(handle: IndexHandle) -> Result<(), CoreError>;

// 2. IndexHandle 方法
impl IndexHandle {
    pub fn path(&self) -> &Path;
    pub fn read_index_schema(&self) -> Schema;
    pub fn read_index(&self, offset: u64, length: u64) -> Result<DataView<'_>, CoreError>;
    pub fn scan_index(&self, request: &ScanRequest) -> Result<IndexScanner, CoreError>;
    pub fn update_index(&mut self, data: &DataView<'_>) -> Result<(), CoreError>;
    pub fn locate_index(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError>;
}
```

### 3.3 数据集层：`Dataset` API (`splayed-core::dataset`)

```rust
// 1. 自由函数
pub fn create_dataset(path: &Path, schema: &Schema) -> Result<DatasetHandle, CoreError>;
pub fn init_dataset(
    path: &Path,
    data: &DataView<'_>,
    options: Option<CreateDatasetOptions>,
) -> Result<DatasetHandle, CoreError>;
pub fn open_dataset(path: &Path, mode: Mode) -> Result<DatasetHandle, CoreError>;
pub fn close_dataset(handle: DatasetHandle) -> Result<(), CoreError>;
pub fn drop_dataset(handle: DatasetHandle) -> Result<(), CoreError>;
pub fn read_dataset_schema(path: &Path) -> Result<Schema, CoreError>;

// 2. DatasetHandle 方法
impl DatasetHandle {
    pub fn read_dataset_schema(&self) -> &Schema;
    pub fn create_dataset_field(&mut self, name: &str, field_type: DataType) -> Result<(), CoreError>;
    pub fn delete_dataset_field(&mut self, name: &str) -> Result<(), CoreError>;
    pub fn rename_dataset_field(&mut self, old_name: &str, new_name: &str) -> Result<(), CoreError>;
    pub fn cast_dataset_field(&mut self, name: &str, target_type: DataType) -> Result<(), CoreError>;
    pub fn compress_dataset_field(&mut self, name: &str) -> Result<(), CoreError>;
    pub fn decompress_dataset_field(&mut self, name: &str) -> Result<(), CoreError>;

    pub fn read_dataset(
        &self,
        offset: u64,
        length: u64,
        columns: Option<&[&str]>,
    ) -> Result<DataView<'_>, CoreError>;
    pub fn write_dataset(&self, offset: u64, data: &DataView<'_>) -> Result<(), CoreError>;
    pub fn update_dataset(&mut self, data: &DataView<'_>) -> Result<(), CoreError>;
    pub fn scan_dataset(&self, request: &ScanRequest) -> Result<DatasetScanner, CoreError>;
    pub fn max_parallelism(&self) -> usize;
    pub fn set_max_parallelism(&self, max_parallelism: usize);
}
```

### 3.4 表层：`Table` API (`splayed-table`)

```rust
// 1. 自由函数
pub fn create_table(
    path: &Path,
    schema: &Schema,
    partition_scheme: PartitionScheme,
    initial_partition: Option<&str>,
    options: Option<TableOptions>,
) -> Result<TableHandle, CoreError>;

pub fn init_table(
    path: &Path,
    partition_scheme: PartitionScheme,
    data: &DataView<'_>,
    options: Option<TableOptions>,
) -> Result<TableHandle, CoreError>;

pub fn open_table(
    path: &Path,
    mode: Mode,
    options: Option<TableOptions>,
) -> Result<TableHandle, CoreError>;

pub fn close_table(handle: TableHandle) -> Result<(), CoreError>;
pub fn drop_table(handle: TableHandle) -> Result<(), CoreError>;
pub fn rename_table(path: &Path, new_name: &str) -> Result<(), CoreError>;

pub fn scan_table<'t>(
    table: &'t TableHandle,
    request: &TableScanRequest,
) -> Result<TableScanner<'t>, CoreError>;

pub fn read_table<'t>(
    table: &'t TableHandle,
    scanner: TableScanner<'t>,
    batch_size: Option<usize>,
) -> Result<TableReader<'t>, CoreError>;

pub fn write_table(table: &TableHandle, data: &DataView<'_>) -> Result<(), CoreError>;
pub fn update_table(table: &TableHandle, data: &DataView<'_>) -> Result<(), CoreError>;

// 2. 流式表写入器（Out-of-Core Streaming Ingestion）
pub struct TableStreamWriter { ... }
impl TableStreamWriter {
    pub fn new(path: &Path, scheme: PartitionScheme, options: Option<TableOptions>) -> Result<Self, CoreError>;
    pub fn write_partition(&mut self, partition_name: &str, data: &DataView<'_>) -> Result<(), CoreError>;
    pub fn write_batch(&mut self, batch: &DataView<'_>) -> Result<(), CoreError>;
    pub fn finish(self) -> Result<TableHandle, CoreError>;
}

// 3. TableHandle 方法
impl TableHandle {
    pub fn read_table_schema(&self) -> Result<Schema, CoreError>;
    pub fn create_partition(&self, partition_name: &str, data: &DataView<'_>, options: Option<CreateDatasetOptions>) -> Result<(), CoreError>;
    pub fn create_table_field(&self, name: &str, field_type: DataType) -> Result<(), CoreError>;
    pub fn delete_table_field(&self, name: &str) -> Result<(), CoreError>;
    pub fn rename_table_field(&self, old_name: &str, new_name: &str) -> Result<(), CoreError>;
    pub fn cast_table_field(&self, name: &str, target_type: DataType) -> Result<(), CoreError>;
    pub fn compress_table_field(&self, name: &str) -> Result<(), CoreError>;
    pub fn decompress_table_field(&self, name: &str) -> Result<(), CoreError>;
    pub fn delete_table(&self, partition_name: &str) -> Result<(), CoreError>;
    pub fn max_parallelism(&self) -> usize;
    pub fn set_max_parallelism(&mut self, max_parallelism: usize);
}
```

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
  - 当 `read_dataset` 发现请求列在当前物理磁盘上不存在时，直接生成虚拟 NULL 段，耗时 $<10\text{ns}$，不产生磁盘 I/O。

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
│ • read_dataset_schema 递归 DFS 发现并还原嵌套字段           │
│ • create_dataset 骨架构建与 init_dataset 带数据构建         │
│ • read_dataset 缺列自动补 NULL 虚拟列                       │
│ • write_dataset 缺列自愈建列与 update_dataset 全量替换      │
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
