# splayed-core / Field API

## 5. Field API

### 5.0 总览

Field 层管理单列物理文件（`FIELD_MAGIC`，64 字节 Header）。V2.1 提供纯元数据骨架创建（`create_field`）、带数据初始化（`init_field`）、统一 Handle 操作（`open_field` / `close_field` / `drop_field` / `read_field` / `write_field` / `update_field` / `scan_field`）以及物理转换操作。

| 接口 | 职责 | 层次 |
| --- | --- | --- |
| `create_field` | 创建空字段文件（仅写 64B Header，`row_count = 0`） | File |
| `init_field` | 带数据初始化创建字段文件（支持全 NULL 延迟展开与 chunk 压缩） | File |
| `open_field` | 打开已有 Field，返回 `FieldHandle`（透明兼容 64B 文件） | File |
| `close_field` | 关闭 Handle；compressed write 收尾重压缩刷盘 | File / Handle |
| `drop_field` / `drop_field_path` | 销毁并物理删除 Field 文件（释放 mmap 后 unlink） | File / Handle |
| `read_field_schema` | 读取字段的 `FieldSchema` | Handle |
| `rename_field` | 重命名 Field 文件 | File |
| `cast_field` | 将 Field 原地转换为 `target_type` | File / Handle |
| `compress_field` | uncompressed → compressed 分块压缩物理表示 | File / Handle |
| `decompress_field` | compressed → uncompressed PLAIN 物理表示 | File / Handle |
| `FieldHandle::read_field` | 按逻辑行读取，返回零拷贝 `ColumnView` | Handle |
| `FieldHandle::write_field` | 按逻辑行覆盖写入（Header-Only 自动就地扩展） | Handle |
| `FieldHandle::update_field` | 全量替换字段数据并更新文件头 | Handle |
| `FieldHandle::scan_field` | SIMD 向量化条件扫描 → `FieldScanner` | Handle |

---

### 5.1 Handle 属性访问器

#### 函数签名
```rust
impl FieldHandle {
    pub fn path(&self) -> &Path;
    pub fn header(&self) -> &FieldHeader;
    pub fn mode(&self) -> Mode;
    pub fn data_type(&self) -> DataType;
    pub fn row_count(&self) -> u64;
    pub fn is_chunked(&self) -> bool;
}
```

#### 参数与返回
| 方法 | 返回类型 | 说明 |
| --- | --- | --- |
| `path()` | `&Path` | Field 文件路径 |
| `header()` | `&FieldHeader` | 当前 Header（含 generation / null_count 等最新值） |
| `mode()` | `Mode` | 访问模式（`Read` / `Write`） |
| `data_type()` | `DataType` | 列数据类型 |
| `row_count()` | `u64` | 逻辑行数（= 物理行数） |
| `is_chunked()` | `bool` | 是否为 compressed chunk 布局 |

#### 内部实现流程
```
只读访问 Handle 内部结构体字段，直接返回引用或基本类型复制，无系统调用与 I/O。
```

#### 其他说明
- 全部为 $O(1)$ 常数时间只读访问器；不触发磁盘 I/O。

---

### 5.2 create_field

#### 函数签名
```rust
pub fn create_field(path: &Path, field_type: DataType) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 待创建字段文件路径；已存在则报错 |
| `field_type` | `DataType` | 输入 | 字段的数据类型 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 创建完成（物理文件恰好 64 字节） |

#### 内部实现流程
```
1. 校验 path 存在性（已存在 → CoreError::AlreadyExists）；
2. 构造空 FieldHeader：
   magic: FIELD_MAGIC, version: FORMAT_VERSION,
   data_type: field_type.id(), encoding: PLAIN, compression: NONE,
   row_count: 0, null_count: 0, data_length: 0, generation: 0, file_size: 64;
3. 创建文件并 write_all(&header.to_bytes())；
4. 保证磁盘占用恰为 64B Header-Only，0 数据 I/O。
```

#### 其他说明
- 极速轻量骨架创建，耗时 $< 1\mu\text{s}$。
- 用于 `create_table` / `create_dataset` 时预先声明字段结构，无需等待数据到位。

---

### 5.3 init_field

#### 函数签名
```rust
pub struct CreateFieldOptions {
    pub compression: Compression,
    pub chunk_offsets: Option<Vec<u64>>,
}

pub fn init_field(
    path: &Path,
    column: &ColumnView<'_>,
    options: Option<CreateFieldOptions>,
) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Field 目标文件路径；已存在则报错 |
| `column` | `&ColumnView<'_>` | 输入 | 初始单列数据视图 |
| `options` | `Option<CreateFieldOptions>` | 输入 | 压缩算法与 sym 对齐分块边界（None 默认为未压缩） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 带数据初始化完成 |

#### 内部实现流程
```
1. 检查全 NULL 特判：若 column.null_count() == column.length()：
   → 写入 64 字节 Header-Only 文件（row_count = n, null_count = n, data_length = 0）；
   → 延迟物理分配，磁盘物理仅 64B！
2. 若 compression 为 None（未压缩）：
   → 三阶段顺序直写：占位 HEADER(64B) → 顺序写入 values 字节 → 顺序写入 packed validity 位流；
   → 校验字长并回填 HEADER（更新 generation=0, null_count, data_length, file_size）；
3. 若 compression != None（分块压缩）：
   → 单遍分块流水线：按 chunk_offsets 切片，逐块执行 encode_chunk；
   → 顺序直写各 chunk 数据，内存占用严格限制在 O(单个 chunk)；
   → 回填 chunked HEADER。
```

#### 其他说明
- 统一编码出口：压缩创建与 `compress_field` 复用同一 `encode_chunk`，输出二进制字节级一致。
- validity 位流采用 64 位 word 级位操作打包（`copy_bits_into`），null_count 统计通过 CPU 硬件指令 `popcnt` 加速。

---

### 5.4 open_field

#### 函数签名
```rust
pub fn open_field(path: &Path, mode: Mode) -> Result<FieldHandle, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 已存在的 Field 物理文件路径 |
| `mode` | `Mode` | 输入 | `Mode::Read`（只读）或 `Mode::Write`（读写） |
| 返回 | `Result<FieldHandle, CoreError>` | 输出 | 字段生命周期句柄 |

#### 内部实现流程
```
1. File::open(path) 并读取前 64 字节 Header；
2. 校验 magic == FIELD_MAGIC 与 format version；
3. Header-Only 检查：若 file_size == 64 且 row_count > 0 且 null_count == row_count：
   → 进入延迟展开模式（Lazy-expansion representation）；
   → 读模式返回全局共享只读零页与全 0 BitmapView，无需磁盘扩展；
4. 未压缩文件：
   → mode == Read: unsafe { Mmap::map(&file) }；
   → mode == Write: unsafe { MmapMut::map_mut(&file) }；
5. 压缩文件（chunked）：
   → 解析 chunk catalog，解压为内存 working 表示。
```

#### 其他说明
- 对上层完全透明隐藏 64B Header-Only 文件的物理细节。

---

### 5.5 close_field

#### 函数签名
```rust
pub fn close_field(handle: FieldHandle) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `handle` | `FieldHandle` | 输入 | 待关闭的字段句柄（转移所有权） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 资源安全释放与脏数据刷盘完成 |

#### 内部实现流程
```
1. 若为 compressed 且发生过写入（dirty == true）：
   → 流式重新压缩各 chunk 到临时文件 <path>.tmp；
   → sync_all 确保落盘；
   → 主动 drop 原句柄的 mmap 映射（解除 Windows 句柄占用）；
   → fs::rename 原子覆盖原文件；
2. 若为 uncompressed 且 mode == Write：
   → 调用 mmap.flush()；
3. 释放 mmap 与文件句柄。
```

#### 其他说明
- 显式生命周期收尾，保证 Windows 平台下原子重命名的安全性。

---

### 5.6 drop_field / drop_field_path
 
#### 函数签名
```rust
pub fn drop_field(path: &Path) -> Result<(), CoreError>;
pub fn drop_field_handle(handle: FieldHandle) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 待销毁的字段物理路径（调用方确保无打开句柄占用） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 物理文件已被删除 |

#### 内部实现流程
```
1. 对于 drop_field(path)：直接调用 fs::remove_file(path) 删除物理文件；
2. 对于 drop_field_handle(handle)：先显式 close_field 关闭并释放底层 Mmap，再彻底删除物理文件。
```

#### 其他说明
- 基于物理路径操作，严格避免 Windows 平台下的句柄占用冲突（Sharing Violation）。

---

### 5.7 read_field_schema

#### 函数签名
```rust
pub fn read_field_schema(path: &Path) -> Result<FieldSchema, CoreError>;
impl FieldHandle {
    pub fn read_field_schema(&self) -> FieldSchema;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` / `&self` | `&Path` / `&FieldHandle` | 输入 | 字段路径或字段句柄 |
| 返回 | `Result<FieldSchema, CoreError>` | 输出 | 该字段的名称与数据类型 |

#### 内部实现流程
```
1. 路径读取：仅读取文件前 64 字节 HEADER，解析提取 data_type 与文件名，无需建立任何 Mmap 映射（0 句柄占用）；
2. 句柄读取：直接自 FieldHandle 内存缓存提取，耗时 < 10ns。
```

#### 其他说明
- 纯元数据读取，不产生实际数据 I/O 开销。

---

### 5.8 rename_field

#### 函数签名
```rust
pub fn rename_field(path: &Path, new_name: &str) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 原字段文件路径 |
| `new_name` | `&str` | 输入 | 新字段名（同级目录下） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 重命名完成 |

#### 内部实现流程
```
1. 校验 new_name 合法性（非空、不含非法字符、不以 '.' 开头）；
2. 构造目标路径 path.parent().join(new_name)；
3. 检查目标文件是否已存在（已存在则报错）；
4. 调用 fs::rename(path, new_path) 原子替换。
```

#### 其他说明
- 只改变文件名，不修改文件内部数据与 Header。

---

### 5.9 cast_field

#### 函数签名
```rust
pub fn cast_field(path: &Path, target_type: DataType) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 待转换字段的文件路径 |
| `target_type` | `DataType` | 输入 | 目标转换类型 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 类型转换成功并原子替换磁盘物理文件 |

#### 内部实现流程
```
1. Header-Only 特判：若为 64B 全 NULL 文件：
   → 直接原地修改 Header 中的 data_type，写回 64 字节并更新 generation，耗时 < 100ns！
2. 普通文件：
   → 逐批流式读取 values（每批 8192 行）执行 as 类型转换；
   → validity 位流原样保留；
   → 写入临时文件 <path>.cast.<pid>.tmp 并 fsync；
   → fs::rename 原子覆盖替换原路径。
```

#### 其他说明
- 基于物理路径执行，避免持有 Mmap 时发生 Windows 共享锁冲突与陈旧句柄（Stale Handles）。
- 流式转换，内存恒定在 $O(\text{batch})$；NULL 值保持不变。

---

### 5.10 compress_field / decompress_field

#### 函数签名
```rust
pub fn compress_field(path: &Path, offsets: Option<Vec<u64>>) -> Result<(), CoreError>;
pub fn decompress_field(path: &Path) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 待压缩/解压字段的文件路径 |
| `offsets`（compress） | `Option<Vec<u64>>` | 输入 | chunk 起始行（None 默认 8192 行） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 物理压缩/解压转换完成 |

#### 内部实现流程
```
compress_field:
1. 校验当前是否已压缩（已压缩则返回 CoreError::InvalidState）；
2. 逐 chunk 零拷贝切片 → encode_chunk → 写入临时文件；
3. 临时文件落盘后释放原句柄映射，原子 rename 并重新加载。

decompress_field:
1. 校验当前是否为压缩状态；
2. 逐 chunk 流式 decode_chunk，直接写入未压缩临时文件 DATA 与 VALIDITY 区；
3. 回填未压缩 Header，drop handles，原子 rename 并重新加载。
```

#### 其他说明
- chunk 流式处理，内存上限为单个 chunk 大小。

---

### 5.11 FieldHandle::read_field

#### 函数签名
```rust
impl FieldHandle {
    pub fn read_field(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&FieldHandle` | 输入 | 字段句柄 |
| `offset` | `u64` | 输入 | 起始逻辑行 |
| `length` | `u64` | 输入 | 读取行数（0 返回空视图） |
| 返回 | `Result<ColumnView<'_>, CoreError>` | 输出 | 零拷贝列视图（生命周期绑定 Handle） |

#### 内部实现流程
```
1. 边界校验：offset + length <= row_count；
2. 若处于 Header-Only 延迟展开模式：
   → 直接切片静态只读零页 STATIC_ZERO_BUFFER；
   → 构造全 0 BitmapView（zeros）；
   → 返回单段 ColumnView，耗时 < 10ns，0 磁盘 I/O！
3. 未压缩模式：
   → 切片 mmap[64 + offset * size .. 64 + (offset + length) * size]；
   → 若存在 validity 区，构造 BitmapView::new(validity_bytes, offset, length)；
   → 返回 ColumnView::from_one；
4. 压缩模式：
   → 二分定位 chunk_ends 起始块，顺序切出多个连续段。
```

#### 其他说明
- 绝对零拷贝切片，生命周期不超过 Handle。

---

### 5.12 FieldHandle::write_field

#### 函数签名
```rust
impl FieldHandle {
    pub fn write_field(&mut self, offset: u64, data: &ColumnView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&mut self` | `&mut FieldHandle` | 输入 | 具有写权限的字段句柄 |
| `offset` | `u64` | 输入 | 覆盖写入的起始逻辑行 |
| `data` | `&ColumnView<'_>` | 输入 | 待写入的数据视图（类型必须匹配） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 写入完成 |

#### 内部实现流程
```
1. 写权限与类型一致性检查；
2. 若当前为 64B Header-Only 状态：
   → 自动触发就地延迟扩展：文件 set_len(64 + row_count * size + validity_size)；
   → 重新挂载 MmapMut，初始化全 0 validity；
3. 未压缩写入：
   → memcpy 写入目标区间的数据值；
   → 增量维护 validity：对比旧位图与新位图，利用 word 级 XOR popcount 统计增量变化；
   → 更新 header.null_count，header.generation += 1，刷入 header 映射区；
4. 压缩写入：
   → 写入内存 working buffer，标记 dirty = true，延迟到 close 时重压缩。
```

#### 其他说明
- 原地覆盖写入（Positional Overwrite），不改变字段总行数。

---

### 5.13 FieldHandle::update_field

#### 函数签名
```rust
impl FieldHandle {
    pub fn update_field(&mut self, data: &ColumnView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&mut self` | `&mut FieldHandle` | 输入 | 字段句柄 |
| `data` | `&ColumnView<'_>` | 输入 | 替换后的完整列数据 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 全量数据原子替换完成 |

#### 内部实现流程
```
1. 写入唯一临时文件 <path>.update.<pid>.tmp；
2. sync_all 确保数据完全落盘；
3. 显式释放原 Handle 的 mmap 映射（Windows 安全要求）；
4. fs::rename 原子覆盖目标文件；
5. 重新打开并建立 mmap 映射，更新 Handle 内部 header 与状态。
```

#### 其他说明
- 全量数据替换接口，允许修改 row_count，具有强原子性和崩溃安全性。

---

### 5.14 FieldHandle::scan_field

#### 函数签名
```rust
impl FieldHandle {
    pub fn scan_field(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&FieldHandle` | 输入 | 字段句柄 |
| `request` | `&ScanRequest` | 输入 | 扫描候选范围与谓词条件 |
| 返回 | `Result<FieldScanner<'_>, CoreError>` | 输出 | 单字段条件扫描器 |

#### 内部实现流程
```
1. 校验 predicate 兼容性；
2. 在每个段内按 64 字节块使用 SIMD 比较指令进行批量谓词求值；
3. 结合 validity bitmap 进行位与（&），过滤掉 NULL 值；
4. 利用 trailing_zeros 提取连续的命中物理行区间 RowRange。
```

#### 其他说明
- 向量化批量求值，每行耗时小于 1 个 CPU 时钟周期。
