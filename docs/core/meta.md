# splayed-core / Index (META) API

## 6. Index (META) API

### 6.0 总览

Index 层负责管理 `.meta` 文件，维护二维时序坐标容量网格：全局有序唯一的 `TIME AXIS`、标的字典 `SYM DICT` 以及 `SYM INDEX` 记录表。V2.1 将其全面统一为四层对称体系中的第三层——**Index 索引层**，提供首级公开对象 `IndexHandle`（与 `MetaHandle` 兼容并存）。

| 接口 | 职责 | 层次 |
| --- | --- | --- |
| `create_index` | 创建空索引骨架文件（仅写 64B Header，`row_count = 0`） | File |
| `init_index` | 连带 (sym, time) 数据初始化构建完整索引网格 | File |
| `open_index` | 打开 `.meta` 文件，返回 `IndexHandle` | File |
| `close_index` | 关闭索引句柄，释放 mmap 映射 | File / Handle |
| `drop_index` | 销毁并物理删除 `.meta` 索引文件 | File / Handle |
| `IndexHandle::read_index_schema` | 返回固定主键 Schema `[sym: Utf8, time: <time_type>]` | Handle |
| `IndexHandle::read_index` | 零拷贝读取 (sym, time) 两列构成的 `DataView`（sym 零物化） | Handle |
| `IndexHandle::scan_index` | 谓词二分快速扫描，产出命中行区间 `RowRange` | Handle |
| `IndexHandle::locate_index` | (sym, time) 联合键双指针单调批量定位 | Handle |
| `IndexHandle::update_index` | 全量原子替换主索引网格并刷新映射 | Handle |

---

### 6.1 create_index

#### 函数签名
```rust
pub fn create_index(path: &Path, time_type: TimeType) -> Result<IndexHandle, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 目标 `.meta` 文件路径；已存在则报错 |
| `time_type` | `TimeType` | 输入 | 时间轴的整数或时间戳类型 |
| 返回 | `Result<IndexHandle, CoreError>` | 输出 | 初始化的空索引句柄（物理大小恰为 64B） |

#### 内部实现流程
```
1. 校验 path 存在性（已存在 → CoreError::AlreadyExists）；
2. 构建空 MetaHeader：
   magic: META_MAGIC, version: FORMAT_VERSION, time_type: time_type.id(),
   time_count: 0, sym_count: 0, row_count: 0, generation: 0,
   sym_dict_offset: 64, sym_index_offset: 64, file_size: 64；
3. File::create 并 write_all 写入 64 字节 Header；
4. 挂载只读 mmap，返回 IndexHandle，耗时 < 1μs，0 数据 I/O。
```

#### 其他说明
- 供 `create_dataset` / `create_table` 纯元数据骨架创建时使用。

---

### 6.2 init_index

#### 函数签名
```rust
pub fn init_index(path: &Path, data: &DataView<'_>) -> Result<IndexHandle, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 目标 `.meta` 文件路径 |
| `data` | `&DataView<'_>` | 输入 | 必须包含按 `(sym ASC, time ASC)` 排序的 sym 与 time 两列 |
| 返回 | `Result<IndexHandle, CoreError>` | 输出 | 构建完成并已打开的 `IndexHandle` |

#### 内部实现流程
```
1. 批量类型转换：sym 字典 keys 整段转换为 &[u32]，time 列整段转换为 &[u64]（无逐行调用）；
2. 单遍扫描：收集全部 time 值并提取 sym run 边界（row_start / time_first / time_last / rows）；
3. TIME AXIS 构建：sort_unstable + dedup，生成全局时间轴；
4. SYM INDEX 构建：二分定位 time_first / time_last 在全局轴上的下标（O(2R log T)）；
5. 连续子区间硬约束校验：end >= start 且 (end - start + 1) == run.rows（违规报 NonContiguousTime）；
6. 顺序拼接序列化为单块 Buffer：HEADER(64B) + TIME AXIS + DICT OFFSETS + STRING DATA + SYM INDEX；
7. 原子提交：写入临时文件 <path>.tmp → sync_all → rename 原文件；
8. 重新以 mmap 打开，返回 IndexHandle。
```

#### 其他说明
- 极速构建：无 HashMap，无逐行对象分配；网格与数据行严格数学对齐。

---

### 6.3 open_index

#### 函数签名
```rust
pub fn open_index(path: &Path) -> Result<IndexHandle, CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | 已存在的 `.meta` 文件路径 |
| 返回 | `Result<IndexHandle, CoreError>` | 输出 | 索引生命周期句柄 |

#### 内部实现流程
```
1. File::open 打开物理文件并挂载 unsafe { Mmap::map(&file) }；
2. 解析前 64 字节 MetaHeader 并验证 magic、version 及 file_size 一致性；
3. 返回封装了 mmap 与 Header 的 IndexHandle。
```

#### 其他说明
- 零拷贝挂载，打开耗时仅几微秒。

---

### 6.4 close_index

#### 函数签名
```rust
pub fn close_index(handle: IndexHandle) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `handle` | `IndexHandle` | 输入 | 待释放的索引句柄（消费所有权） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 释放完成 |

#### 内部实现流程
```
消费 IndexHandle，释放内部持有的 Mmap 映射与文件描述符。
```

#### 其他说明
- 显式生命周期收尾，在重命名或删除前必须调用。

---

### 6.5 drop_index

#### 函数签名
```rust
pub fn drop_index(handle: IndexHandle) -> Result<(), CoreError>;
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `handle` | `IndexHandle` | 输入 | 待删除的索引句柄 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 物理文件已被删除 |

#### 内部实现流程
```
1. 记录文件路径 path；
2. 显式释放 mmap 映射与文件句柄；
3. 调用 fs::remove_file(path) 彻底删除物理文件。
```

#### 其他说明
- 解除 mmap 后执行 unlink，避免 Windows 下句柄占用报错。

---

### 6.6 IndexHandle::read_index_schema

#### 函数签名
```rust
impl IndexHandle {
    pub fn read_index_schema(&self) -> Schema;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&IndexHandle` | 输入 | 索引句柄 |
| 返回 | `Schema` | 输出 | 包含 `sym`（Utf8）与 `time`（TimeType）两列的 Schema |

#### 内部实现流程
```
直接返回基于 self.header.time_type() 构成的双字段 Schema 结构。
```

#### 其他说明
- 纯内存构建，无 I/O。

---

### 6.7 IndexHandle::read_index

#### 函数签名
```rust
impl IndexHandle {
    pub fn read_index(&self, offset: u64, length: u64) -> Result<DataView<'_>, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&IndexHandle` | 输入 | 索引句柄 |
| `offset` | `u64` | 输入 | 起始逻辑行（容量网格偏移） |
| `length` | `u64` | 输入 | 读取行数（0 返回空视图） |
| 返回 | `Result<DataView<'_>, CoreError>` | 输出 | 包含 sym 与 time 两列的零拷贝 `DataView` |

#### 内部实现流程
```
1. 边界校验：offset + length <= row_count；
2. 二分 SYM INDEX 表（row_start 单调递增），找到第一个重叠的 sym_id；
3. 顺序遍历重叠的 sym 区间：
   - 每段 sym 列：构造 ColumnSegment::new_repeat_dict(...)
     （零内存分配、零存储物化，仅记录 dict_index 与行数，O(1) 内存）；
   - 每段 time 列：直接从 mmap 的 TIME AXIS 区域按字节切片（零拷贝）；
4. 组装为包含 sym 和 time 两个 ColumnView 的 DataView 并返回。
```

#### 其他说明
- 绝对零拷贝：sym 字典 keys 不逐行展开，time 轴零内存分配。

---

### 6.8 IndexHandle::scan_index

#### 函数签名
```rust
impl IndexHandle {
    pub fn scan_index(&self, request: &ScanRequest) -> Result<IndexScanner, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&IndexHandle` | 输入 | 索引句柄 |
| `request` | `&ScanRequest` | 输入 | 包含 ranges、predicate 与 limit 的请求 |
| 返回 | `Result<IndexScanner, CoreError>` | 输出 | 索引扫描器，迭代返回物理 RowRange |

#### 内部实现流程
```
1. 编译谓词：将针对 sym 和 time 的条件表达式编译为 (sym_ids, time_lo, time_hi) 窗口；
2. 线性扫描 SYM INDEX 表（通常仅几千行标的）：
   - 根据 sym_id 过滤目标标的；
   - 将 time 窗口二分映射为局部行偏移，直接数学计算命中区间 RowRange；
3. 将计算得到的 RowRange 与 request.ranges 做双指针求交；
4. 封装为 IndexScanner 返回。
```

#### 其他说明
- 极速定位：完全无需扫描任何数据列文件，纯元数据二分在微秒级完成全表行区间裁剪。

---

### 6.9 IndexHandle::locate_index

#### 函数签名
```rust
impl IndexHandle {
    pub fn locate_index(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&IndexHandle` | 输入 | 索引句柄 |
| `pairs` | `&[(String, i64)]` | 输入 | 输入的 (sym, time) 键序列，必须按 (sym ASC, time ASC) 排序 |
| 返回 | `Result<Vec<RowRange>, CoreError>` | 输出 | 合并后的内部逻辑行物理区间列表 |

#### 内部实现流程
```
1. 双指针单调推进（sym_cursor 与 time_cursor），结合有序性避免逐行哈希查找；
2. 根据 sym 记录二分时间轴，直接计算网格行号 row = row_start + (time_cursor - time_start)；
3. 边推进边连续合并（last.end() == row 则 length += 1）；
4. 校验总长度 sum(length) == pairs.len()。
```

#### 其他说明
- 为 `write_table` 与 `write_dataset` 提供高效定位基础设施。

---

### 6.10 IndexHandle::update_index

#### 函数签名
```rust
impl IndexHandle {
    pub fn update_index(&mut self, data: &DataView<'_>) -> Result<(), CoreError>;
}
```

#### 参数与返回
| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&mut self` | `&mut IndexHandle` | 输入 | 索引句柄 |
| `data` | `&DataView<'_>` | 输入 | 替换后的全新 (sym, time) 完整数据视图 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 主索引全量更新替换完成 |

#### 内部实现流程
```
1. 基于新 data 调用 MetaBuilder::build 构建新索引的完整字节流；
2. 写入临时文件 <path>.update.<pid>.tmp 并 sync_all；
3. 显式释放原 Handle 的 mmap 映射；
4. fs::rename 原子替换原有 .meta 文件；
5. 重新建立 mmap 映射并更新 Handle 内部 header 与状态。
```

#### 其他说明
- 崩溃安全，具备强原子性，无中间损坏状态。
