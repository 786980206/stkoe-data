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
- **缺失列容错**：`read_dataset` 遇到请求了物理未创建的字段时，自动填充零开销虚拟全 NULL 视图，不抛出异常。
- **写入自愈补建**：`write_dataset` 遇到 Schema 中存在但文件未创建的字段时，自动通过 `init_field` 补建。

---

### 7.2 总览

| 接口 | 职责 | 层次 |
| --- | --- | --- |
| `create_dataset` | 解析 Schema，递归创建目录与空字段骨架 | File |
| `init_dataset` | 连带数据构建完整 Dataset（构建 .meta 并多线程分桶创建各列） | File |
| `open_dataset` | 打开 Dataset，返回 `DatasetHandle`（按需懒加载字段） | File |
| `close_dataset` | 关闭 Dataset，刷盘并释放全部资源 | File / Handle |
| `drop_dataset` | 销毁并物理删除完整 Dataset 目录 | File / Handle |
| `read_dataset_schema` | 递归扫描目录发现所有字段（还原 '.' 嵌套名称） | File / Handle |
| `DatasetHandle::create_dataset_field` | 新增单个字段（支持多级目录与 64B 延迟展开） | Handle |
| `DatasetHandle::delete_dataset_field` | 删除指定字段文件 | Handle |
| `DatasetHandle::rename_dataset_field` | 原子重命名指定字段 | Handle |
| `DatasetHandle::cast_dataset_field` | 转换指定字段类型 | Handle |
| `DatasetHandle::compress_dataset_field` | 压缩指定字段 | Handle |
| `DatasetHandle::decompress_dataset_field`| 解压指定字段 | Handle |
| `DatasetHandle::read_dataset` | 按逻辑行零拷贝读取多列（支持缺失列自动补 NULL） | Handle |
| `DatasetHandle::write_dataset` | 按逻辑行覆盖写入多列（多线程分桶并行，缺列自愈） | Handle |
| `DatasetHandle::update_dataset` | 全量替换包括 sym, time 在内的所有数据 | Handle |
| `DatasetHandle::scan_dataset` | 索引先行 + 字段并行条件扫描 | Handle |

---

### 7.3 create_dataset

#### 函数签名
```rust
pub fn create_dataset(path: &Path, schema: &Schema) -> Result<DatasetHandle, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Dataset 根目录路径；已存在则报错 |
| `schema` | `&Schema` | 输入 | 必须包含 sym 与 time 及所需的所有普通字段 |
| 返回 | `Result<DatasetHandle, CoreError>` | 输出 | 打开的空数据集句柄 |

#### 内部实现流程
```
1. 校验 path 不存在，创建根目录 fs::create_dir_all(path)；
2. 校验 schema 必须包含 sym 与 time，提取 time 的 TimeType；
3. 调用 create_index(path.join(".meta"), time_type) 创建空索引；
4. 遍历普通字段：
   - 解析字段名（例如 "factor.vol.ret" 映射为 path.join("factor/vol/ret")）；
   - fs::create_dir_all 预建父目录；
   - 调用 create_field(&field_path, field.data_type) 写入 64 字节空文件头；
5. 调用 open_dataset(path, Mode::Write) 返回 DatasetHandle。
```

#### 其他说明
- 纯元数据骨架创建，耗时在毫秒级以内，0 实际数据 I/O。

---

### 7.4 init_dataset

#### 函数签名
```rust
pub struct CreateDatasetOptions {
    pub max_parallelism: usize,
    pub compression: Compression,
    pub chunk_target_rows: usize,
}

pub fn init_dataset(
    path: &Path,
    data: &DataView<'_>,
    options: Option<CreateDatasetOptions>,
) -> Result<DatasetHandle, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Dataset 根目录路径；已存在则报错 |
| `data` | `&DataView<'_>` | 输入 | 包含 sym, time 及各列的完整初始数据 |
| `options` | `Option<CreateDatasetOptions>` | 输入 | 并行度与压缩分块配置 |
| 返回 | `Result<DatasetHandle, CoreError>` | 输出 | 构建完成并打开的数据集句柄 |

#### 内部实现流程
```
1. 创建根目录；
2. 单线程先行构建索引：调用 init_index(path.join(".meta"), data) 构建并原子提交 .meta；
3. 从索引提取 sym 对齐的分块边界（sym_aligned_chunk_offsets）；
4. 多线程分桶并行创建各普通字段：
   - 任务数 P = min(max_parallelism, fields.len())；
   - std::thread::scope 启动并行工作线程；
   - 每个工作线程负责一组字段：支持嵌套目录创建，调用 init_field 直写物理文件；
5. 全部字段刷盘完成，open_dataset(path, Mode::Write) 返回句柄。
```

#### 其他说明
- 一步到位高性能构建，充分利用多核带宽；全有效列不写 validity 区，全 NULL 列产出 64B 延迟展开文件。

---

### 7.5 open_dataset

#### 函数签名
```rust
pub fn open_dataset(path: &Path, mode: Mode) -> Result<DatasetHandle, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 已存在的 Dataset 目录路径 |
| `mode` | `Mode` | 输入 | 访问模式（`Read` / `Write`） |
| 返回 | `Result<DatasetHandle, CoreError>` | 输出 | 数据集句柄 |

#### 内部实现流程
```
1. 校验 path 存在且为目录；
2. open_index(path.join(".meta")) 打开主索引，读取 time_type 与 row_count；
3. 调用 read_dataset_schema(path) 递归扫描文件系统构建统一 Schema 并缓存；
4. 初始化空的字段句柄缓存池 RefCell<HashMap<String, Box<FieldHandle>>>；
5. 返回 DatasetHandle。
```

#### 其他说明
- 字段文件句柄采用按需懒加载（Lazy Loading），未访问的列不占用打开句柄与虚拟地址空间。

---

### 7.6 close_dataset

#### 函数签名
```rust
pub fn close_dataset(handle: DatasetHandle) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `handle` | `DatasetHandle` | 输入 | 待关闭的数据集句柄 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 全部资源安全释放 |

#### 内部实现流程
```
1. 获取 fields 缓存的所有 FieldHandle；
2. 逐个调用 close_field(handle)（若是 compressed 写入则重压缩落盘并刷盘）；
3. 调用 close_index 释放 .meta 的 mmap 映射。
```

#### 其他说明
- 确保所有写入和重压缩操作安全持久化，彻底释放所有 Windows 句柄。

---

### 7.7 drop_dataset

#### 函数签名
```rust
pub fn drop_dataset(handle: DatasetHandle) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `handle` | `DatasetHandle` | 输入 | 待销毁的数据集句柄 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 物理目录已彻底删除 |

#### 内部实现流程
```
1. 记录根路径 root；
2. 显式调用 close_dataset 释放所有文件句柄与 mmap 映射；
3. 调用 fs::remove_dir_all(root) 递归删除整个目录。
```

#### 其他说明
- 严格遵循先 drop 句柄再物理删除原则。

---

### 7.8 read_dataset_schema

#### 函数签名
```rust
pub fn read_dataset_schema(path: &Path) -> Result<Schema, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 数据集目录路径 |
| 返回 | `Result<Schema, CoreError>` | 输出 | 解析还原出的完整 Schema |

#### 内部实现流程
```
1. 从 .meta 读取前 64 字节获取 time_type，放入 [sym: Utf8, time: <time_type>]；
2. 深度优先遍历（DFS）扫描数据集子目录：
   - 过滤以 '.' 开头的隐藏文件/目录（.meta, .tmp 等）和以 '.tmp' 结尾的文件；
   - 若遇到常规文件且大小 >= 64 且 magic == FIELD_MAGIC：
     → 将相对路径组件通过 '.' 连接（例如 factor/vol/ret -> "factor.vol.ret"）；
     → 读取 header 中的 data_type；
3. 将发现的字段按名称字母序排序，组装并返回 Schema。
```

#### 其他说明
- 支持任意深度的嵌套子目录；过滤任何临时文件。

---

### 7.9 DatasetHandle::read_dataset

#### 函数签名
```rust
impl DatasetHandle {
    pub fn read_dataset(
        &self,
        offset: u64,
        length: u64,
        columns: Option<&[&str]>,
    ) -> Result<DataView<'_>, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&DatasetHandle` | 输入 | 数据集句柄 |
| `offset` | `u64` | 输入 | 起始逻辑行号 |
| `length` | `u64` | 输入 | 读取行数 |
| `columns` | `Option<&[&str]>` | 输入 | 投影字段列表（None 为全列） |
| 返回 | `Result<DataView<'_>, CoreError>` | 输出 | 包含多列的零拷贝 `DataView` |

#### 内部实现流程
```
1. 边界校验：offset + length <= logical_length()；
2. 调用 self.meta.read_index(offset, length) 获取 sym 与 time 列（sym 为 RepeatDict 零物化）；
3. 遍历 requested columns：
   - 若列在磁盘物理存在：按需载入 ensure_field，调用 field.read_field(offset, length)；
   - 若列在磁盘物理不存在（冷分区缺失列）：
     → 零开销构造虚拟全 NULL 切片 ColumnSegment::new_virtual_null(dt, length)；
     → 引用只读共享零页，有效位图全 0，0 磁盘 I/O、0 堆内存分配！
4. 组装为 DataView 并返回。
```

#### 其他说明
- 具备强大的缺失列透明容错能力，保证下游接收到的批次 Schema 始终完整一致。

---

### 7.10 DatasetHandle::write_dataset

#### 函数签名
```rust
impl DatasetHandle {
    pub fn write_dataset(&self, offset: u64, data: &DataView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&DatasetHandle` | 输入 | 数据集句柄（必须以 Write 模式打开） |
| `offset` | `u64` | 输入 | 写入起始逻辑行 |
| `data` | `&DataView<'_>` | 输入 | 待写入的多列数据视图（跳过 sym/time） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 写入完成 |

#### 内部实现流程
```
1. self.mode.require_write("write_dataset") 校验；
2. 缺失列探测与自动补建（Auto-Creation）：
   - 检查 data 中各普通列是否已在磁盘落盘；
   - 若未落盘但属于合法列：调用 create_field 生成 64B 占位文件并载入缓存池；
3. 将待写入的字段按并行预算分桶；
4. std::thread::scope 启动工作线程并行覆盖写（桶内串行、桶间并行，无全局锁）；
5. 64B Header-Only 字段在首次写入时自动触发扩展；维护各字段 generation 与 null_count。
```

#### 其他说明
- 位置覆盖写入，不改变数据集总行数；各字段并发写入，吞吐极高。

---

### 7.11 DatasetHandle::update_dataset

#### 函数签名
```rust
impl DatasetHandle {
    pub fn update_dataset(&mut self, data: &DataView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&mut self` | `&mut DatasetHandle` | 输入 | 数据集句柄 |
| `data` | `&DataView<'_>` | 输入 | 包含 sym, time 及全量字段的新数据视图 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 全量数据原子替换完成 |

#### 内部实现流程
```
1. 调用 self.meta.update_index(data) 原子更新主索引网格；
2. 并行调用各个普通列的 update_field(data.column(name)) 原子替换文件；
3. 若新 data 中包含新列，调用 init_field 直接初始化；
4. 刷新内存中缓存的 Schema 与句柄池。
```

#### 其他说明
- 支持数据集全量重建与行数变更，全程原子替换，崩溃安全。

---

### 7.12 DatasetHandle::scan_dataset

#### 函数签名
```rust
impl DatasetHandle {
    pub fn scan_dataset(&self, request: &ScanRequest) -> Result<DatasetScanner, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&DatasetHandle` | 输入 | 数据集句柄 |
| `request` | `&ScanRequest` | 输入 | 扫描请求（谓词、候选范围、限制行数） |
| 返回 | `Result<DatasetScanner, CoreError>` | 输出 | 数据集条件扫描器 |

#### 内部实现流程
```
1. 提取针对 sym 与 time 的谓词，调用 self.meta.scan_index 获取候选物理 RowRange 列表；
2. 提取针对各普通字段的谓词，按字段名称排序；
3. 多线程并行调用各涉及字段的 scan_field，对候选 RowRange 进行向量化过滤；
4. 对各字段产出的匹配位图进行位与求交（Bitwise AND）；
5. 构造 DatasetScanner 返回。
```

#### 其他说明
- 索引先行裁剪，字段下推求交，最大限度避免无效 I/O。

---

### 7.13 并发控制 (set_max_parallelism / max_parallelism)

#### 函数签名
```rust
impl DatasetHandle {
    /// 获取当前数据集的 Field 级最大并行度
    pub fn max_parallelism(&self) -> usize;

    /// 动态设置 Field 级最大并行度（影响 write_dataset 与 scan_dataset）
    pub fn set_max_parallelism(&self, max_parallelism: usize);
}
```

#### 其他说明
- 支持内部可变性（`Cell`），允许在持有不可变借用时无锁调节并发度，供上层 `Table` 级调度器灵活管控。
