# splayed-core / Field API

## 5. Field API

### 5.0 总览

| 接口 | 职责 | 层 |
| --- | --- | --- |
| `create_field_file` | 创建 Field 文件并初始化 | File |
| `open_field_file` | 打开已有 Field，返回 `FieldHandle` | File |
| `delete_field_file` | 删除 Field 物理文件 | File |
| `rename_field_file` | 重命名 Field 文件 | File |
| `cast_field_file` | 将 Field 原地转换为 `target_type` | File |
| `compress_field_file` | uncompressed → compressed 物理表示 | File |
| `decompress_field_file` | compressed → uncompressed 物理表示 | File |
| `read_field_handle` | 按逻辑行读取，返回 `ColumnView` | Handle |
| `write_field_handle` | 按逻辑行覆盖写入 | Handle |
| `update_field_handle` | 修改 FieldHeader（不改 data） | Handle |
| `scan_field_handle` | 条件扫描 → `FieldScanner` | Handle |
| `close_field_handle` | 关闭 Handle；compressed write 收尾 | Handle |

### 5.1 Handle 属性访问器

**接口定义**：
```rust
impl FieldHandle {
    pub fn path(&self) -> &Path
    pub fn header(&self) -> &FieldHeader
    pub fn mode(&self) -> Mode
    pub fn data_type(&self) -> DataType
    pub fn row_count(&self) -> u64
    pub fn is_chunked(&self) -> bool
}
```

**参数与返回**：

| 方法 | 返回类型 | 说明 |
| --- | --- | --- |
| `path()` | `&Path` | Field 文件路径 |
| `header()` | `&FieldHeader` | 当前 header（含 generation / null_count 等，写路径维护后的最新值） |
| `mode()` | `Mode` | 访问模式（`read` / `write`），不是压缩状态 |
| `data_type()` | `DataType` | 列数据类型（open 时校验过） |
| `row_count()` | `u64` | 逻辑行数（= 物理行数） |
| `is_chunked()` | `bool` | 是否为 compressed chunk 布局 |

**说明**：
- 全部只读访问器；不触发 IO（数据来自 open 时读取的 header）。

### 5.2 create_field_file

**接口定义**：
```rust
pub struct CreateFieldOptions {
    pub compression: Compression,        // chunk 压缩算法（默认 None = PLAIN + NONE）
    pub chunk_offsets: Option<Vec<u64>>, // chunk 起始行（升序、首项 0）；None = 均匀 8192 行
}
impl Default for CreateFieldOptions { /* None + None */ }

pub fn create_field_file(path: &Path, data_type: DataType, init: FieldInit,
    options: CreateFieldOptions) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | Field 文件路径（文件名即字段名）；已存在 → Error |
| `data_type` | `DataType` | 输入 | 列数据类型 |
| `init` | `FieldInit` | 输入 | 初始化方式，见下表 |
| `options` | `CreateFieldOptions` | 输入 | 创建选项：`compression`（默认 None）+ `chunk_offsets`（**sym 对齐边界**，由上层从 META 网格 / 输入 sym run 推导；None = 均匀 8192 行；仅压缩时生效） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 创建完成；此后才可被 `open_field_file` 打开（create 不返回 Handle） |

`FieldInit` 变体：

| 变体 | 载荷 | 说明 |
| --- | --- | --- |
| `Length(u64)` | 行数 `n` | 全 NULL 占位 Field（validity 全 0）；**压缩时**产出 chunked 全 NULL 字段（每 chunk values 零填充 + validity 全 0 位，后续 write 生命周期保持压缩） |
| `Data(Column)` | 拥有型列 | 带数据初始化；`row_count` = 数据长度；数据全有效时不写 validity 区 |
| `Stream { reader: Box<dyn FieldChunkReader> }` | 流式读取器 | 两相迭代：先 `next_values()` 消费全部 values，再 `next_validity()` 消费 validity 位；最终长度无需预先知道 |

**内部实现流程（按 options 分派）**：
```
compression = None            →  未压缩路径（PLAIN + NONE；三阶段顺序写：
                                 HEADER 占位 → DATA/VALIDITY 顺序写 → HEADER 回填；
                                 length(n) 走 set_len 稀疏零填充，O(1) 不触碰数据页）
compression != None：
  Length(n)                   →  chunked 全 NULL：逐 chunk encode_chunk（values 零填充 +
                                 validity 全 0 位）——后续 write 生命周期保持压缩
  Data(col)                   →  **单遍 chunked 直接创建**（主路径）：
                                 占位 header → 按边界切 chunk、逐 chunk encode_chunk
                                 顺序直写（values 零拷贝切片、validity 按位切片重打包）
                                 → 回填 header；内存 O(单 chunk)，无 tmp / 无二次读
  Stream(reader)              →  组合路径：未压缩流式创建（O(1) 内存，两相协议）
                                 + compress_field_file_encoded 原地压缩（O(单 chunk)）；
                                 两相协议（先全部 values 后全部 validity）使单遍
                                 chunked 编码需物化全列，故不走直接路径
chunked header 回填约定       →  与 compress_field_file 输出一致：chunked、
                                 has_validity = false（validity 在 chunk 内自描述）、
                                 data_length = 逻辑字节数、null_count 精确
                                 （打开时会从解压位图重建，写值仅为一致性）
```

**说明**：
- **统一编码出口**：压缩创建与 `compress_field_file` 复用同一 `encode_chunk` 与 chunk
  格式，二者对同一输入 + 同一边界的输出**字节级一致**（测试锁定）。
- `header` 不要求调用方完整构造；可从 `path / data_type / init / options` 推断的信息由 core 生成。
- 必须包含 `sym` 与 `time` 的约定属于 Dataset / Table 层；Field 层不关心字段名语义。
- 创建完成前由调用方决定是否 fsync；配合上层（Dataset / Table）的临时文件 + 原子 rename。
- 带数据初始化时写真实 `null_count`；stream 初始化由 validity 位 word 批量 popcount 精确统计。
- `create_field_file` 内部不开启线程；并行度由上层（Dataset / Table）控制。

### 5.3 open_field_file

**接口定义**：
```rust
pub fn open_field_file(path: &Path, mode: Mode) -> Result<FieldHandle, CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 已存在的 Field 文件路径 |
| `mode` | `Mode` | 输入 | `read` / `write`（访问意图，不是压缩状态） |
| 返回 | `Result<FieldHandle, CoreError>` | 输出 | Field 生命周期 Handle |

**内部实现**：
```
File::open → 读 64B header → validate
chunked   →  遍历 chunk 头得 chunk_rows / chunk_ends（累积行末）+ 全量解压 working 表示
             （null_count 基线从解压位图 word 批量 popcount 精确重建）
read      →  只读 mmap 挂载
write     →  MmapMut 挂载
```

**说明**：
- Field 必须已存在；open 不负责创建。
- 打开时校验 magic / version。
- `read`：允许 read / scan；不修改原文件；compressed Field 的解压对上层隐藏。
- `write`：允许 read / scan / write / update。uncompressed 直接原地修改；compressed 内部进入解压后的 working representation，发生修改后由 close 自动重压缩写回。
- compressed 打开即全量解压是当前实现策略；chunk 级惰性解码为后续优化项（见 design-boundary）。

### 5.4 delete_field_file

**接口定义**：
```rust
pub fn delete_field_file(path: &Path) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 待删除的 Field 文件路径 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 文件已删除 |

**说明**：
- 直接删除物理文件；调用方保证没有打开的 Handle（Windows 上文件被打开时无法删除）。
- 不涉及 META / Schema——那是 Dataset / Table 层的职责。

### 5.5 rename_field_file

**接口定义**：
```rust
pub fn rename_field_file(path: &Path, new_name: &str) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 待重命名的 Field 文件路径 |
| `new_name` | `&str` | 输入 | 新字段名（= 新文件名） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 原子重命名完成 |

**说明**：
- 同目录内重命名 Field 文件；文件名即字段名（沿用 Dataset 层的名称解析约定）。
- 原子完成；`new_name` 对应文件已存在时 Error，不覆盖。
- 只改文件名，不修改数据、header、generation。
- 字段名合法性与重复检查由上层负责；`new_name` 为空 / 以 `.` 开头 / 含路径分隔符 → Error。

### 5.6 read_field_handle

**接口定义**：
```rust
impl FieldHandle {
    pub fn read_field_handle(&self, offset: u64, length: u64) -> Result<ColumnView<'_>, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&FieldHandle` | 输入 | Handle（read / write mode 均可读） |
| `offset` | `u64` | 输入 | 起始逻辑行（= 物理行） |
| `length` | `u64` | 输入 | 读取行数；`length = 0` 返回空 view |
| 返回 | `Result<ColumnView<'_>, CoreError>` | 输出 | zero-copy 列视图；生命周期不超过 Handle / 底层资源 |

**内部实现**：
```
bounds        →  end = offset.checked_add(length)（溢出安全）→ end ≤ row_count
length = 0    →  ColumnView::empty（单零行段，满足 segments 非空不变式）
uncompressed  →  mmap 切片 values[offset×size .. (offset+length)×size]
                  + BitmapView::new(validity_bytes, offset, length)
                  → 单段 ColumnView::from_one
compressed    →  chunk_ends（open 时累积行末）上 partition_point 二分首个 chunk
                  → 自首个 chunk 顺序切段，cstart ≥ end 即 break
                  → 每段 ColumnSegment::new（values 字节切片 + validity.slice 位视图，均零拷贝）
                  → ColumnView::new(多段；段容量按平均 chunk 行数预分配)
```
- 职责边界：read 只负责"逻辑行范围 → ColumnView"转换 —— 解压在 open 一次性完成；
  不在此做数据复制、段合并或谓词求值（归 Scanner / Dataset 层）
- PLAIN+NONE 返回的值指针直接指向 mmap 区域（零拷贝）
- compressed Field 打开时全量解压到 `Working { values: Buffer, validity: Option<Bitmap> }`，
  后续读取从 working 上切片（chunk 级惰性解码为优化项）
- compressed 定位复杂度 O(log C + 交叠 chunk 数)：二分定位 + 交叠段连续产出，
  不从头遍历 chunk、不做逐 chunk 前缀和；validity 只建位视图（`BitmapView::slice`），不复制位图
- `offset / length` 为逻辑行（= 物理行）；`offset + length ≤ row_count`（`checked_add` 溢出安全）；`length = 0` 返回空 view。

**说明**：
- 返回 zero-copy ColumnView：`PLAIN + NONE` 为单段 mmap 切片；compressed 逐 chunk 物化，跨 chunk 的读取返回多段。
- view 生命周期不能超过 Handle / 底层资源；close 后失效。
- 不提供 `parallel` 参数，并发由上层控制。

### 5.7 write_field_handle

**接口定义**：
```rust
impl FieldHandle {
    pub fn write_field_handle(&mut self, offset: u64, data: &ColumnView) -> Result<(), CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&mut self` | `&mut FieldHandle` | 输入 | Handle（需 write mode） |
| `offset` | `u64` | 输入 | 起始逻辑行；`offset + data.length() ≤ row_count` |
| `data` | `&ColumnView` | 输入 | 待写入数据（values + validity 成对）；`data_type` 必须与 Field 一致 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 覆盖写入完成，generation 已递增 |

**内部实现**（按物理表示分派；写入三原则：values 逐段 memcpy、validity 字节/word 批量、null_count 增量）：
```
bounds        →  end = offset.checked_add(data.length)（溢出安全）→ end ≤ row_count
length = 0    →  no-op（不改数据、不递增 generation）
uncompressed  →  先校验后写入（无 validity 区的字段拒绝含 NULL 的段）
                  → 逐段：values copy_from_slice（一次连续 memcpy）
                  + validity：BitmapView::copy_bits_into / bitmap_fill_bits（按字节批量：
                    头尾掩码 RMW，中间同相位 memcpy / 异相位逐字节移位，不逐 bit）
                  + null_count 按（覆盖前 1 位数 − 覆盖后 1 位数）增量修正
                  → generation += 1 → header 回写 mmap[0..64]（落盘即含新 generation / null_count）
compressed    →  working.values 同上逐段 copy_from_slice
                  + 若段含 NULL 且 working 无位图 → 先物化全 1 位图
                  + Bitmap::copy_bits_from / set_range（字节批量）
                  + null_count 增量修正（基线 open 时由解压位图精确重建）→ modified = true
```
- 复杂度 O(data.length)，实际执行以连续内存复制为主（接近 memcpy）；`generation` 每次 write 调用恰好 +1，不按段递增
- write 路径不做 fsync（uncompressed 写入 mmap 即生效；compressed 在 close 时统一落盘）
- 并发写非重叠区域安全：MmapMut 或 working 上按 offset 切片互不干扰

**说明**：
- 需要 write mode；从 `offset` 起覆盖写入（positional overwrite），不改变逻辑长度；不是追加 / 扩容接口。
- `data` 含多个 segment 时按逻辑行序逐段写入；segment 的 `validity = null` 表示该段全部有效。
- 只改 data，不改 header 布局字段；成功后递增 `FIELD.generation`。
- 允许并发写非重叠区域；重叠区域不允许；并发度由上层控制。

> 规范说明：数据参数统一为 `ColumnView`（草稿中 buffer/stream 与 ColumnView 混用）。写路径长度有界，流式大数据 = 分块多次调用；`stream` 仅保留在 create 的 `init` 中（最终长度未知的场景）。

### 5.8 update_field_handle

**接口定义**：
```rust
impl FieldHandle {
    pub fn update_field_handle(&mut self, header: FieldHeader) -> Result<(), CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&mut self` | `&mut FieldHandle` | 输入 | Handle（需 write mode） |
| `header` | `FieldHeader` | 输入 | 新 header；`row_count` / `data_type` 必须与现值一致；`null_count` 被忽略 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = header 更新完成，generation 已递增 |

**内部实现**：
```
校验     →  magic / version / data_type / row_count 强制为现值；generation = 现 + 1
            null_count 为派生统计，保持现值（由写路径增量维护，不接受调用方改写）
布局派生 →  非 chunked：data_length / validity_offset 按现值重算，防布局描述不一致
落盘     →  uncompressed：立即回写 mmap[0..64]；compressed：随 close 收尾
```

**说明**：
- 只修改 header，不修改 data；与 `write_field_handle` 的区别：write 改 data，update 改 header。
- 可更新的是 encoding / compression / flags 等物理属性；类型转换走 `cast_field_file`。
- compressed Field 的 header 更新随 close 流程保持文件一致。

### 5.9 scan_field_handle

**接口定义**：
```rust
impl FieldHandle {
    pub fn scan_field_handle(&self, request: &ScanRequest) -> Result<FieldScanner<'_>, CoreError>
}
impl<'h> FieldScanner<'h> {
    pub fn next(&mut self) -> Result<Option<RowRange>, CoreError>
    pub fn close(self) -> Result<(), CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&FieldHandle` | 输入 | Handle（read / write mode 均可） |
| `request.ranges` | `&[RowRange]` | 输入 | 候选物理范围；空 = 整个 Field；仅与 `[0, row_count)` 求交防越界（保序，不排序不合并） |
| `request.predicate` | `Option<Predicate>` | 输入 | 作用于本 Field 值的谓词（`field = None`）；Utf8 字段不支持值谓词 |
| `request.limit` | `Option<u64>` | 输入 | 最多产生的命中行数，达到后提前结束 |
| 返回 scanner | `Result<FieldScanner, CoreError>` | 输出 | 定位器；`next()` 每次返回一个连续命中 RowRange，结束返回 `None` |

**内部实现**（顺序批量管线，不物化数据）：
```
构造      →  ranges 直接顺序消费：仅与 [0, row_count) 求交防越界（保序、不排序、不合并；
              有序不重叠由上游保证）；空 ranges = 整个 Field
next()    →  消费当前段命中位图：word 级 next_true_run 找下一连续命中区 → RowRange
              （word 跳零字 / 满字扩展；limit 达到即截断并结束）
段求值    →  段 = [row, min(range.end, row + 1M))：整段连续 values（PLAIN mmap 切片 /
              compressed working 切片，零拷贝）批量求谓词
              → Cmp：类型化切片比较循环（算子分派在循环外，LLVM 自动向量化；
                跨域加宽语义保持 compare_scalar 规则：有符号 ↔ 无符号 ↔ 浮点）
              → And / Or / Not：字节级位运算（AND 全零短路）
              → 根部统一与 validity 求交：NULL 行不命中任何条件（含 NOT / OR）
              → 0/1 字节掩码 → branchless 打包位图（缓冲跨 next() 重用）
```
- 不复制 values、不物化数据、不创建 DataView、不逐行生成 RowRange、不做 ranges merge、不处理 sym / time
- 输出为连续命中区：段内相邻命中行合并为单个 RowRange；跨段不合并
- 无谓词 = 只输出有效行（validity 过滤）
- 浮点比较遵循 IEEE 语义（NaN 行 / NaN 目标按 IEEE 求值，仅 Ne 命中；不再逐行报错）
- 段上限 1M 行仅为限定掩码内存；掩码缓冲跨 `next()` 重用
- 只定位，不物化数据；输出可交给 `read_field_handle`，或作为其他 Field scan 的 `ranges` 输入做多字段下推。
- Field 不理解 sym / time；只做值过滤。不支持 order 下推。

### 5.10 close_field_handle

**接口定义**：
```rust
pub fn close_field_handle(handle: FieldHandle) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `handle` | `FieldHandle` | 输入 | 按值消费 Handle（close 后不可再用） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 资源释放 / 写回完成 |

**内部实现**（按 mode × 是否 chunked 分派）：
```
read                            → 直接 Ok（Mmap 随 Drop 释放）
write + uncompressed            → MmapMut::flush → Ok
write + compressed + 未修改      → Ok（不写回）
write + compressed + 已修改      → 流式：header（编码前即完全确定，无需占位回填）直写 tmp
                                    → 逐 chunk：从 working 切段 → extract_bits → encode_chunk
                                      → 直写 tmp（内存 O(working + 一个 chunk)，不拼接整个重压缩文件）
                                    → tmp sync_all（rename 生效时新文件内容已持久）
                                    → fs::rename(tmp, path) 原子替换；失败清理 tmp，原文件保持不变
```
- compressed 重压缩沿用文件既有 chunk 分组（打开时从 chunk 头读得，写路径不改 row_count）
- 临时文件路径 = `{field_path}.tmp`，rename 原子替换

**说明**：
- close 后 Handle 不可再用；释放 fd / mmap / working memory。
- read handle：无写回。
- uncompressed write handle：写入已直接生效（mmap flush 即落盘），无需额外动作。
- compressed write handle：发生修改 → 自动 compress + rewrite，文件保持 compressed；重压缩沿用文件既有 chunk 分组（打开时从 chunk 头读得，自描述，不依赖 META；写路径不改 `row_count`，`Σ rows == row_count` 恒成立，分组可精确复用），写临时文件后原子替换；未修改 → 不写回。
- 不提供 `commit / flush / dump` public API。

### 5.11 cast_field_file

**接口定义**：
```rust
pub fn cast_field_file(path: &Path, target_type: DataType) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 待转换的 Field 文件路径 |
| `target_type` | `DataType` | 输入 | 目标数据类型；与现类型相同 → 直接返回；涉及 Utf8 → Error |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 原地转换完成（`data_type` 变为 `target_type`） |

**内部实现**（read → 逐批 cast → create tmp → sync_all → rename）：
```
open(read)   →  target_type == src_type 直接返回；涉及 Utf8 → Error
stream cast  →  CastReader 批次读取（CAST_BATCH_ROWS = 256K 行/批）：
                  逐段 read_field_handle → 类型转换（逐行 as 语义）拼接
                  → validity 逐段位级拼接为全局字节对齐位流（批尾填充位不计，
                    O(total/8) 内存，远小于 values；段边界非字节对齐安全）
                create_field_file(tmp, Stream) 三阶段顺序写出（不构造完整目标 Field）
压缩状态     →  保持原 Field 压缩状态：uncompressed → uncompressed；
                compressed → 按原 encoding / compression / chunk 分组重新编码
收尾         →  header 仅更新 data_type（generation 保持源值）
                → tmp sync_all → rename 原子替换（rename 生效时新文件内容已持久）
                → 失败清理 tmp（含 compress 步 tmp），原文件保持不变
```
- 临时文件名唯一（`{path}.cast.{pid}.{n}.tmp`），并发 cast 互不覆盖。
- uncompressed 源全程 O(一个批次) 内存；compressed 源受限于「open 即全量解压」（chunk 级惰性解码为后续优化项）。

**说明**：
- 转换成功后该 Field 的 `data_type` 为 `target_type`，逻辑数据逐行完成类型转换（逐行 `as` 语义：bool ↔ 数值 ↔ 浮点；Utf8 不支持）。
- validity / NULL 原样保留，不参与类型转换。
- 转换失败时原文件保持不变；转换通过临时文件完成，不属于 `write_field_handle` 的原地覆盖。

### 5.12 compress_field_file / decompress_field_file

**接口定义**：
```rust
pub fn compress_field_file(path: &Path, offsets: Option<Vec<u64>>) -> Result<(), CoreError>
pub fn decompress_field_file(path: &Path) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 待转换物理表示的 Field 文件路径 |
| `offsets`（compress） | `Option<Vec<u64>>` | 输入 | chunk 起始行号：升序、`offsets[0] == 0`，隐含最后一块延伸到 `row_count`；省略时按固定 8192 行均匀分块（最后一块允许不足） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 物理表示转换完成；状态不符 → 明确 Error（已压缩再压缩 / 未压缩再解压） |

**compress 内部实现**（chunk 流式，内存 O(一个编码 chunk)）：
```
open(read) → 未压缩校验；header（编码前即完全确定，无需占位回填）直写 tmp
    → 逐 chunk：range_views 零拷贝切片 → encode_chunk → 直写 tmp
    → tmp sync_all → drop handles → rename（失败清理 tmp，原文件保持不变）
```

**decompress 内部实现**（chunk 流式，内存 O(一个解码 chunk)，不全量解压）：
```
mmap 原文件 → 直接解析 header + chunk 位置（不经 open_field_file，避免 open 全量解压）
    → tmp set_len 预分配 DATA + VALIDITY（零填充即占位 HEADER）
    → 逐 chunk：decode_chunk → values 顺序直写 DATA（游标 64 起）
      → validity 位跨 chunk 拼接（无 validity 的 chunk 补全 1 位）顺序直写 VALIDITY
        （游标 64+data_len 起；单句柄双游标，两区域各自顺序 IO）
    → header 回填：has_validity = 是否有 chunk 携带 validity（全无则收缩 validity 区）；
      data_type / generation / row_count / null_count 保持源值
    → tmp sync_all → drop handles → rename（失败清理 tmp，原文件保持不变）
```

**说明**：
- File 级物理表示转换；逻辑数据与 header 语义不变（只改物理表示：data_type / row_count / null_count / generation 不变）。
- 分块策略是调用方的职责：Dataset 层按 META 网格生成 sym 对齐边界（见 7.7），裸调用可省略 `offsets`。
- compress 编码配置固定 PLAIN + ZSTD（cast 经 `compress_field_file_encoded` 保持源配置）；decompress 目标恒为 PLAIN + NONE。
- 与 close 的自动压缩互补：一个面向离线维护，一个面向写生命周期。
