# splayed-core / Dataset API

## 7. Dataset API

### 7.1 结构与对象

Dataset 处于四层架构的第二层，管理单个分区/数据集，协调 `.meta` 容量网格与各物理数据列：

```
Dataset（目录）
├── .meta                           // Index：sym/time → 逻辑行网格
├── price                           // 单级字段文件
└── factor                          // 多级目录嵌套字段（Dot 语法）
    └── momentum
        └── ret20                   // 对应字段 "factor.momentum.ret20"
```

- `open_dataset` 只打开 `.meta` 索引；字段按需懒加载入句柄池。
- **多级嵌套字段（Dot 语法）**：字段名 `"a.b.c"` 物理映射为子目录 `a/b/c`。
- **缺失列容错**：`read` 遇到请求了物理未创建的字段时，自动填充零开销虚拟全 NULL 视图，不抛出异常。
- **写入自愈补建**：`write` 遇到 Schema 中存在但文件未创建的字段时，自动通过 `create_field` 补建。

---

### 7.2 面向对象模型与总览

Dataset 采用成对的对称对象模型：
- **`DatasetReader`**：只读数据集对象，负责 `read`（流式/按需投影）、`scan`（谓词二分过滤）、`locate`（批量点查）以及元数据统计查询；
- **`DatasetWriter`**：可写数据集对象，负责骨架创建（`create`）、数据初始化（`init`）、局部位置覆盖写（`write`）、全量原子更新（`update`）、字段 DDL 与目录管理。

| 核心操作 | 对象 / 方法 | 职责说明 |
| --- | --- | --- |
| **创建骨架** | `DatasetWriter::create(path, schema)` | 解析 Schema，建立各字段 64B 骨架文件（0 数据 I/O） |
| **数据初始化** | `DatasetWriter::init(path, data, opts)` | 连带数据构建完整 Dataset（构建 .meta 并多线程分桶落盘） |
| **只读打开** | `DatasetReader::open(path)` | 打开只读数据集，按需懒加载字段句柄 |
| **可写打开** | `DatasetWriter::open(path)` | 打开可写数据集，支持写入与结构操作 |
| **流式切片读取** | `reader.read(offset, len, proj)` | 零拷贝切片读取数据视图（自动补齐缺失列虚拟 NULL） |
| **条件谓词扫描** | `reader.scan(request)` | 惰性条件扫描，输出 `DatasetScanner` |
| **主键批量定位** | `reader.locate(pairs)` | 快速二分定位行范围 |
| **位置覆盖写** | `writer.write(offset, data)` | 原地覆盖写入（缺失列自动自愈补建） |
| **全量原子替换** | `writer.update(data)` | 原子全量替换数据集内容（同步重构 .meta 索引） |
| **字段 DDL** | `writer.create_field` / `delete_field` / `rename_field` / `cast_field` / `compress_field` / `decompress_field` / `update_field` | 单字段生命周期管理与结构转换 |
| **转换为只读** | `writer.as_reader()` | 转换为 `DatasetReader` 读取已提交数据 |
| **安全关闭** | `reader.close()`, `writer.close()` | 关闭句柄并释放全部内存映射 |
| **物理销毁** | `writer.remove()` | 安全关闭句柄并在磁盘物理递归删除整目录 |
| `DatasetHandle::read` | 按逻辑行零拷贝读取多列（支持缺失列自动补 NULL） | Handle |
| `DatasetHandle::write` | 按逻辑行覆盖写入多列（多线程分桶并行，缺列自愈） | Handle |
| `DatasetHandle::update` | 全量替换包括 sym, time 在内的所有数据 | Handle |
| `DatasetHandle::scan` | 索引先行 + 字段并行条件扫描 | Handle |

---

### 7.3 DatasetWriter::create (纯 Schema 骨架创建)

#### 函数签名
```rust
impl DatasetWriter {
    pub fn create(path: &Path, schema: &Schema) -> Result<Self, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Dataset 根目录路径；已存在则报错 |
| `schema` | `&Schema` | 输入 | 必须包含 sym 与 time 及所需的所有普通字段 |
| 返回 | `Result<DatasetWriter, CoreError>` | 输出 | 打开的空数据集写对象 |

#### 内部实现流程
```
1. 校验 path 不存在，创建根目录 fs::create_dir_all(path)；
2. 校验 schema 必须包含 sym 与 time，提取 time 的 TimeType；
3. 调用 IndexWriter::create(path.join(".meta"), time_type) 创建空索引；
4. 遍历普通字段：
   - 解析字段名（例如 "factor.vol.ret" 映射为 path.join("factor/vol/ret")）；
   - fs::create_dir_all 预建父目录；
   - 调用 FieldWriter::create(&field_path, field.data_type) 写入 64 字节空文件头；
5. 打开并返回 DatasetWriter 实例。
```

---

### 7.4 DatasetWriter::init (连带数据初始化)

#### 函数签名
```rust
pub struct CreateDatasetOptions {
    pub max_parallelism: usize,
    pub compression: Compression,
    pub chunk_target_rows: usize,
}

impl DatasetWriter {
    pub fn init(
        path: &Path,
        data: &DataView<'_>,
        options: Option<CreateDatasetOptions>,
    ) -> Result<Self, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Dataset 根目录路径；已存在则报错 |
| `data` | `&DataView<'_>` | 输入 | 包含 sym, time 及各列的完整初始数据 |
| `options` | `Option<CreateDatasetOptions>` | 输入 | 并行度与压缩分块配置 |
| 返回 | `Result<DatasetWriter, CoreError>` | 输出 | 构建完成并打开的数据集写对象 |

#### 内部实现流程
```
1. 创建根目录；
2. 单线程先行构建索引：调用 IndexWriter::init(path.join(".meta"), data) 构建并原子提交 .meta；
3. 从索引提取 sym 对齐的分块边界（sym_aligned_chunk_offsets）；
4. 多线程分桶并行创建各普通字段：
   - 任务数 P = min(max_parallelism, fields.len())；
   - std::thread::scope 启动并行工作线程；
   - 每个工作线程负责一组字段：支持嵌套目录创建，调用 FieldWriter::init 直写物理文件；
5. 全部字段刷盘完成，返回 DatasetWriter。
```

---

### 7.5 DatasetReader::open 与 DatasetWriter::open

#### 函数签名
```rust
impl DatasetReader {
    pub fn open(path: &Path) -> Result<Self, CoreError>;
}

impl DatasetWriter {
    pub fn open(path: &Path) -> Result<Self, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 已存在的 Dataset 目录路径 |
| 返回 | `Result<DatasetReader / DatasetWriter, CoreError>` | 输出 | 只读或可写数据集对象 |

#### 内部实现流程
```
1. 校验 path 存在且为目录；
2. IndexReader/IndexWriter 打开主索引，读取 time_type 与 row_count；
3. 递归扫描文件系统构建统一 Schema 并缓存；
4. 初始化空的字段句柄缓存池；
5. 返回对应 DatasetReader 或 DatasetWriter 对象。
```

---

### 7.6 DatasetReader::read (零拷贝流式/投影读取)

#### 函数签名
```rust
impl DatasetReader {
    pub fn read(
        &self,
        offset: u64,
        length: u64,
        projection: Option<&[&str]>,
    ) -> Result<DataView<'_>, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&DatasetReader` | 输入 | 数据集只读对象 |
| `offset` | `u64` | 输入 | 起始逻辑行号 |
| `length` | `u64` | 输入 | 读取行数 |
| `projection` | `Option<&[&str]>` | 输入 | 投影字段列表（None 为全列） |
| 返回 | `Result<DataView<'_>, CoreError>` | 输出 | 包含多列的零拷贝 `DataView` |

#### 内部实现流程
```
1. 边界校验：offset + length <= logical_length()；
2. 获取 sym 与 time 列（sym 为 RepeatDict 零物化）；
3. 遍历 requested columns：
   - 若列在磁盘物理存在：按需载入字段句柄，调用 field.read(offset, length)；
   - 若列在磁盘物理不存在（冷分区缺失列）：
     → 零开销构造虚拟全 NULL 切片 ColumnSegment::new_virtual_null(dt, length)；
     → 引用只读共享零页，有效位图全 0，0 磁盘 I/O、0 堆内存分配！
4. 组装为 DataView 并返回。
```

---

### 7.7 DatasetReader::scan 与 locate (条件扫描与批量定位)

#### 函数签名
```rust
impl DatasetReader {
    pub fn scan(&self, request: &ScanRequest) -> Result<DatasetScanner, CoreError>;
    pub fn locate(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError>;
    pub fn locate_borrowed(&self, pairs: &[(&str, i64)]) -> Result<Vec<RowRange>, CoreError>;
}
```

- **`scan`**：提取 sym 与 time 谓词由索引裁剪出候选行范围，各普通字段并行求值 SIMD 位图过滤并位与求交，产出 `DatasetScanner`；
- **`locate`**：直接在容量网格上二分定位指定的 `(sym, time)` 复合键对应的行范围。

---

### 7.8 DatasetWriter::write (位置覆盖写)

#### 函数签名
```rust
impl DatasetWriter {
    pub fn write(&self, offset: u64, data: &DataView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&DatasetWriter` | 输入 | 数据集写对象 |
| `offset` | `u64` | 输入 | 写入起始逻辑行 |
| `data` | `&DataView<'_>` | 输入 | 待写入的多列数据视图（跳过 sym/time） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 写入完成 |

#### 内部实现流程
```
1. 缺失列探测与自动补建（Auto-Creation）：
   - 检查 data 中各普通列是否已在磁盘落盘；
   - 若未落盘但属于合法列：调用 create_field 生成 64B 占位文件并载入缓存池；
2. 将待写入的字段按并行预算分桶；
3. std::thread::scope 启动工作线程并行覆盖写（桶内串行、桶间并行，无全局锁）；
4. 64B Header-Only 字段在首次写入时自动触发扩展；维护各字段 generation 与 null_count。
```

---

### 7.9 DatasetWriter::update (全量原子替换更新)

#### 函数签名
```rust
impl DatasetWriter {
    pub fn update(&mut self, data: &DataView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&mut self` | `&mut DatasetWriter` | 输入 | 数据集写对象 |
| `data` | `&DataView<'_>` | 输入 | 包含 sym, time 及全量字段的新数据视图 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 全量数据原子替换完成 |

#### 内部实现流程
```
1. 原子更新主索引网格（.meta）；
2. 并行调用各个普通列原子替换文件；
3. 若新 data 中包含新列，直接初始化创建；
4. 刷新内存中缓存的 Schema 与句柄池。
```

---

### 7.10 DatasetWriter 字段 DDL 操作

#### 函数签名
```rust
impl DatasetWriter {
    pub fn create_field(&mut self, name: &str, data_type: DataType, init: DatasetFieldInit, options: CreateFieldOptions) -> Result<(), CoreError>;
    pub fn delete_field(&mut self, name: &str) -> Result<(), CoreError>;
    pub fn rename_field(&mut self, name: &str, new_name: &str) -> Result<(), CoreError>;
    pub fn cast_field(&mut self, name: &str, target_type: DataType) -> Result<(), CoreError>;
    pub fn compress_field(&mut self, name: &str) -> Result<(), CoreError>;
    pub fn decompress_field(&mut self, name: &str) -> Result<(), CoreError>;
    pub fn update_field(&self, name: &str, header: &FieldHeader) -> Result<(), CoreError>;
}
```

- 支持嵌套 Dot 语法字段，各 DDL 操作均在独立释放 mmap 句柄后执行原子替换，确保 Windows 下安全。

---

### 7.11 安全关闭与物理销毁 (close 与 remove)

#### 函数签名
```rust
impl DatasetReader {
    pub fn close(self) -> Result<(), CoreError>;
}

impl DatasetWriter {
    pub fn close(self) -> Result<(), CoreError>;
    pub fn remove(self) -> Result<(), CoreError>;
}
```

- **`close(self)`**：释放所有字段和索引的 Mmap 映射并安全持久化；
- **`remove(self)`**：显式调用 `close` 释放句柄后，物理递归删除整个 Dataset 目录。
