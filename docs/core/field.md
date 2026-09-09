# splayed-core / Field API

## 5. Field API

### 5.0 面向对象模型与总览

Field 层采用成对的对称对象模型：
- **`FieldReader`**：只读字段对象，负责 `read`（零拷贝切片 ColumnView）、`scan`（SIMD 向量化条件扫描）与元数据读取；
- **`FieldWriter`**：可写字段对象，负责骨架创建（`create`）、数据初始化（`init`）、局部位置覆盖写（`write`）、全量原子更新（`update`）、就地转换（`cast`）、压缩（`compress`）、解压（`decompress`）与删除。

| 核心操作 | 对象 / 方法 | 职责说明 |
| --- | --- | --- |
| **创建骨架** | `FieldWriter::create(path, type)` | 创建空字段文件（仅写 64B Header，`row_count = 0`） |
| **数据初始化** | `FieldWriter::init(path, data, opts)`| 带数据初始化创建字段文件（支持全 NULL 延迟展开与 chunk 压缩） |
| **只读打开** | `FieldReader::open(path)` | 打开已有 Field，返回只读句柄（零拷贝 Mmap 映射） |
| **可写打开** | `FieldWriter::open(path)` | 打开已有 Field，返回可写句柄（支持 Mmap 写入与扩展） |
| **零拷贝切片读取**| `reader.read(offset, len)` | 按逻辑行读取，返回零拷贝 `ColumnView` |
| **向量化扫描** | `reader.scan(request)` | SIMD 向量化条件扫描 → `FieldScanner` |
| **位置覆盖写** | `writer.write(offset, data)` | 按逻辑行覆盖写入（Header-Only 自动就地扩展） |
| **全量原子替换** | `writer.update(data)` | 全量替换字段数据并更新文件头 |
| **同目录重命名** | `writer.rename(new_name)` | 原子重命名 Field 物理文件 |
| **原地类型转换** | `writer.cast(target_type)` | 将 Field 原地转换为目标数据类型 |
| **分块压缩** | `writer.compress()` | 将 uncompressed 文件转换为分块压缩物理表示 |
| **原地解压** | `writer.decompress()` | 将 compressed 文件还原为 uncompressed PLAIN 物理表示 |
| **转换为只读** | `writer.as_reader()` | 转换为 `FieldReader` 读取当前已提交数据 |
| **安全关闭** | `reader.close()`, `writer.close()` | 关闭 Handle；compressed write 收尾重压缩刷盘 |
| **物理销毁** | `writer.remove()` | 安全关闭句柄并在磁盘物理删除该字段文件 |

---

### 5.1 FieldWriter::create (纯元数据骨架创建)

#### 函数签名
```rust
impl FieldWriter {
    pub fn create(path: &Path, field_type: DataType) -> Result<Self, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 待创建字段文件路径；已存在则报错 |
| `field_type` | `DataType` | 输入 | 字段的数据类型 |
| 返回 | `Result<FieldWriter, CoreError>` | 输出 | Ok = 创建完成的可写字段对象（物理文件恰好 64 字节） |

#### 内部实现流程
```
1. 校验 path 存在性（已存在 → CoreError::AlreadyExists）；
2. 构造空 FieldHeader：
   magic: FIELD_MAGIC, version: FORMAT_VERSION,
   data_type: field_type.id(), encoding: PLAIN, compression: NONE,
   row_count: 0, null_count: 0, data_length: 0, generation: 0, file_size: 64;
3. 创建文件并 write_all(&header.to_bytes())，保证磁盘占用恰为 64B Header-Only；
4. 打开并返回 FieldWriter 实例。
```

---

### 5.2 FieldWriter::init (连带数据初始化)

#### 函数签名
```rust
pub struct CreateFieldOptions {
    pub compression: Compression,
    pub chunk_offsets: Option<Vec<u64>>,
}

impl FieldWriter {
    pub fn init(
        path: &Path,
        data: &ColumnView<'_>,
        options: Option<CreateFieldOptions>,
    ) -> Result<Self, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Field 目标文件路径；已存在则报错 |
| `data` | `&ColumnView<'_>` | 输入 | 初始单列数据视图 |
| `options` | `Option<CreateFieldOptions>` | 输入 | 压缩算法与 sym 对齐分块边界（None 默认为未压缩） |
| 返回 | `Result<FieldWriter, CoreError>` | 输出 | Ok = 带数据初始化完成的可写对象 |

#### 内部实现流程
```
1. 检查全 NULL 特判：若 data.null_count() == data.length()：
   → 写入 64 字节 Header-Only 文件（row_count = n, null_count = n, data_length = 0）；
   → 延迟物理分配，磁盘物理仅 64B！
2. 若 compression 为 None（未压缩）：
   → 三阶段顺序直写：占位 HEADER(64B) → 顺序写入 values 字节 → 顺序写入 packed validity 位流；
   → 校验字长并回填 HEADER（更新 generation=0, null_count, data_length, file_size）；
3. 若 compression != None（分块压缩）：
   → 单遍分块流水线：按 chunk_offsets 切片，逐块执行 encode_chunk 并顺序落盘；
4. 打开并返回 FieldWriter。
```

---

### 5.3 FieldReader::open 与 FieldWriter::open

#### 函数签名
```rust
impl FieldReader {
    pub fn open(path: &Path) -> Result<Self, CoreError>;
}

impl FieldWriter {
    pub fn open(path: &Path) -> Result<Self, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 已存在的 Field 物理文件路径 |
| 返回 | `Result<FieldReader / FieldWriter, CoreError>` | 输出 | 只读或可写字段对象 |

#### 内部实现流程
```
1. 打开文件并读取前 64 字节 Header，校验 magic == FIELD_MAGIC；
2. Header-Only 检查：若 file_size == 64 且 row_count > 0 且 null_count == row_count，启用延迟展开（零开销返回全 0 位图零页）；
3. 未压缩文件：分别挂载只读 Mmap 或可写 MmapMut；
4. 压缩文件（chunked）：挂载 chunk catalog 与解压内存 working 视图。
```

---

### 5.4 FieldReader::read (零拷贝切片读取)

#### 函数签名
```rust
impl FieldReader {
    pub fn read(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&FieldReader` | 输入 | 只读字段对象 |
| `offset` | `u64` | 输入 | 起始逻辑行 |
| `length` | `u64` | 输入 | 读取行数（0 返回空视图） |
| 返回 | `Result<ColumnView<'_>, CoreError>` | 输出 | 零拷贝列视图（生命周期绑定 reader） |

#### 内部实现流程
```
1. 边界校验：offset + length <= row_count；
2. 若处于 Header-Only 延迟展开模式：
   → 直接切片静态只读零页，构造全 0 BitmapView，返回 ColumnView（耗时 < 10ns，0 磁盘 I/O）；
3. 未压缩模式：
   → 切片 mmap[64 + offset * size .. 64 + (offset + length) * size]；
   → 若存在 validity 区，构造 BitmapView::new(validity_bytes, offset, length)；
4. 压缩模式：二分定位起始 chunk，顺序切出连续段。
```

---

### 5.5 FieldReader::scan (SIMD 向量化条件扫描)

#### 函数签名
```rust
impl FieldReader {
    pub fn scan(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&FieldReader` | 输入 | 只读字段对象 |
| `request` | `&ScanRequest` | 输入 | 扫描候选范围与谓词条件 |
| 返回 | `Result<FieldScanner<'_>, CoreError>` | 输出 | 单字段条件扫描器 |

#### 内部实现流程
```
1. 校验 predicate 兼容性；
2. 在每个段内按 64 字节块使用 SIMD 比较指令进行批量谓词求值；
3. 结合 validity bitmap 进行位与（&），过滤掉 NULL 值；
4. 利用 trailing_zeros 提取连续的命中物理行区间 RowRange。
```

---

### 5.6 FieldWriter::write (原地位置覆盖写)

#### 函数签名
```rust
impl FieldWriter {
    pub fn write(&mut self, offset: u64, data: &ColumnView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&mut self` | `&mut FieldWriter` | 输入 | 可写字段对象 |
| `offset` | `u64` | 输入 | 覆盖写入的起始逻辑行 |
| `data` | `&ColumnView<'_>` | 输入 | 待写入的数据视图（类型必须匹配） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 写入完成 |

#### 内部实现流程
```
1. 若当前为 64B Header-Only 状态：自动触发就地延迟扩展，扩展文件物理尺寸并挂载 MmapMut；
2. 未压缩写入：
   → memcpy 写入目标区间的数据值；
   → 增量维护 validity：对比新旧位图，利用 64 位 word 级 XOR popcount 统计增量变化；
   → 更新 header.null_count，header.generation += 1，刷入 header 映射区；
3. 压缩写入：写入内存 working buffer 并标记 dirty，延迟到关闭时重压缩。
```

---

### 5.7 FieldWriter::update (全量替换更新)

#### 函数签名
```rust
impl FieldWriter {
    pub fn update(&mut self, data: &ColumnView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&mut self` | `&mut FieldWriter` | 输入 | 可写字段对象 |
| `data` | `&ColumnView<'_>` | 输入 | 替换后的完整列数据 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 全量数据原子替换完成 |

#### 内部实现流程
```
1. 写入临时文件 <path>.update.<pid>.tmp 并 sync_all；
2. 显式释放原句柄的 mmap 映射（Windows 安全要求）；
3. fs::rename 原子覆盖目标文件；
4. 重新挂载 mmap 映射，更新内部状态。
```

---

### 5.8 FieldWriter 结构转换与 DDL (rename / cast / compress / decompress)

#### 函数签名
```rust
impl FieldWriter {
    pub fn rename(&mut self, new_name: &str) -> Result<(), CoreError>;
    pub fn cast(&mut self, target_type: DataType) -> Result<(), CoreError>;
    pub fn compress(&mut self) -> Result<(), CoreError>;
    pub fn decompress(&mut self) -> Result<(), CoreError>;
}
```

- **`rename`**：原子重命名物理文件名，自动重建内部写句柄；
- **`cast`**：原地类型转换（64B 全 NULL 文件原地改写 Header；普通文件流式逐批转换替换）；
- **`compress` / `decompress`**：在 uncompressed PLAIN 与 compressed chunk 布局间原地流式转换。

---

### 5.9 安全关闭与物理销毁 (close 与 remove)

#### 函数签名
```rust
impl FieldReader {
    pub fn close(self) -> Result<(), CoreError>;
}

impl FieldWriter {
    pub fn close(self) -> Result<(), CoreError>;
    pub fn remove(self) -> Result<(), CoreError>;
}
```

- **`close(self)`**：消费自身，解除底层 Mmap 映射（若发生过压缩写入则重压缩落盘）；
- **`remove(self)`**：显式调用 `close` 释放 Mmap 后，从磁盘彻底删除物理字段文件。
