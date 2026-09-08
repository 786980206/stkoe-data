# splayed-core：公共架构与设计契约

本页定义 `splayed-core` 的公共架构、命名规范、核心数据结构与公共语义契约。

- 磁盘格式定义见 [splayed-format](../splayed-format.md)（META / FIELD / 类型 / NULL / 编码压缩）。
- 表级组织（Hive 分区 / TableHandle）见 [splayed-table](../splayed-table.md)。
- core 保持引擎无关：不依赖 Arrow / DataFusion / DuckDB / Polars；列存取以 `ColumnView` / `DataView` 为交换结构，适配层负责与各引擎类型的零拷贝对接。

---

## 1. 架构分层与命名规则

Splayed V2.1 构建了严格对称的四层体系：

```
Table (表级)      跨分区 Hive 组织、流式查询、缺失列补齐、自愈写入
    │
    ▼
Dataset (分区级)  单分区多列协调、多级嵌套目录字段 (Dot 语法)、虚拟 NULL 容错
    │
    ▼
Index (索引级)    主索引 .meta 句柄 IndexHandle，维护 (sym, time) 容量网格
    │
    ▼
Field (字段级)    单列物理文件句柄 FieldHandle (支持 64B 延迟展开、Chunk 压缩)
```

统一动词命名规则：
- `create_*`：纯元数据骨架创建（仅创建 64 字节 Header 或空目录）。
- `init_*`：连带全量数据一步到位构建物理文件/数据集/表。
- `open_*` / `close_*`：打开与显式释放生命周期句柄（Windows 句柄安全释放）。
- `drop_*`：释放句柄后彻底物理删除对应文件或目录。
- `read_*` / `write_*` / `update_*` / `scan_*`：零拷贝读取、位置覆盖写、全量原子替换、谓词扫描。

---

## 2. 公共数据结构

### 2.1 RowRange
```rust
pub struct RowRange {
    pub offset: u64,
    pub length: u64,
}
```
连续行段统一用 `offset`（起始逻辑行）与 `length`（连续行数）表示，不使用 `[start, end)`。与 SYM INDEX 的 `row_start + time_count` 及覆盖写切片语义严格一致。

### 2.2 内存数据视图
- `Buffer / BufferView / BitmapView / ColumnSegment / ColumnView / Schema / DataView / Data` 构成统一公共数据模型（见 `splayed-format` §3）。
- 读路径统一输出 non-owning 视图类型，生命周期受限于 Handle。

### 2.3 交换数据参数
```
Field    read / write          → ColumnView（单列）
Index    read / scan           → DataView（sym / time 两列，sym 零物化）
Dataset  read / write          → DataView（多列）
Table    read / write          → DataView（跨分区多列）
```

### 2.4 ScanRequest
```rust
pub struct ScanRequest {
    pub ranges: Vec<RowRange>,
    pub projection: Vec<Arc<str>>,
    pub predicate: Option<Predicate>,
    pub limit: Option<u64>,
}
```
- `ranges`：候选行区间（空表示全表空间）。
- `projection`：列投影列表。
- `predicate`：可组合的过滤条件。
- `limit`：最大匹配行数（满足后提前早停）。

### 2.5 Predicate 条件表达式
- core 定义的可组合条件表达式：`And / Or / Not` + 基本比较操作（`CmpOp::Eq, Ne, Lt, Le, Gt, Ge`）。
- 算子在 ColumnView 的段内批量求值（SIMD 优化），结合 validity 位图进行整 word 快速求交。

### 2.6 Scanner 契约
```rust
pub trait Scanner {
    fn next(&mut self) -> Result<Option<RowRange>, CoreError>;
    fn close(self) -> Result<(), CoreError>;
}
```
- `next()` 每次返回一个连续命中的行区间，遍历完毕返回 `None`。
- `close()` 无论正常结束、提前终止还是发生错误，均可安全调用。

---

## 3. Handle 对象模型

Handle 是各层公开的不透明运行时对象，生命周期与内部资源由 core / table 管理：

```
FieldHandle
├── path: PathBuf
├── header: FieldHeader           // Header 内存缓存
├── backing: Backing              // Mmap / MmapMut / Empty
├── working: Option<Working>      // 延迟展开或解压后的 working 内存
└── mode: Mode (Read / Write)

IndexHandle
├── path: PathBuf
├── header: MetaHeader            // MetaHeader 内存缓存
└── mmap: Mmap                    // .meta 只读内存映射

DatasetHandle
├── root: PathBuf
├── meta: IndexHandle             // 常驻索引网格
├── schema: Schema                // 包含多级嵌套字段的全表 Schema 缓存
├── fields: RefCell<HashMap<...>> // 按需懒加载的 FieldHandle 池
└── mode: Mode

TableHandle
├── root: PathBuf
├── scheme: PartitionScheme       // None | Year | Month | Date
├── datasets: RefCell<HashMap>    // 按需懒加载的分区 DatasetHandle 池
└── stats_cache: RefCell<HashMap> // 分区统计信息缓存
```

---

## 4. 公共语义：容量网格与逻辑行空间

Dataset / Index / Field 共享同一套**容量网格**逻辑行空间：

```
L = Σ_sym time_count(sym)                    // 逻辑总行数 = 每个 Field 的 row_count
row(sym_i, time_index) = row_start(i) + (time_index - time_start(i))
```

- 各层 API 的 `offset / length` 含义完全一致，内部行号一一映射，无需坐标折算。
- 某标的区间内缺失的时间点仍然是合法的逻辑行位，值为 NULL（validity = 0）。
- **缺失列虚拟 NULL 补齐**：冷分区物理上不存在的列，由 `read_dataset` 在内存中生成虚拟全 NULL 视图，不影响容量网格长度与下游读取一致性。
