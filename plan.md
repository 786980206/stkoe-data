# Splayed V1 — 设计文档

> 目标：**针对 SYM × TIME 的金融时序数据，优先做到极低 I/O、O(1) 定位、mmap/零拷贝读取、高吞吐追加写入，并通过 Arrow 连接 DataFusion / DuckDB。**

---

# 1. 概述与设计目标

Splayed V1 是一个针对 **SYM × TIME × FIELD** 金融时序数据的专用列式存储引擎。

核心目标：

| 目标 | 说明 |
| --- | --- |
| 极低 I/O | 按需读取，只触碰被请求的 FIELD 与行范围 |
| O(1) 定位 | META 索引直接给出目标 row range，无扫描 |
| mmap / 零拷贝 | PLAIN + NONE 路径直接指针切片，不复制 |
| 高吞吐写入 | 全量预分配 + 原地更新，无需重整整个文件 |
| 生态连接 | Arrow 交换层 → DataFusion（原生 Adapter）/ DuckDB（Extension） |

---

# 2. 总体设计决策表

| 原则 | 决策 | 理由 |
| --- | --- | --- |
| 数据模型 | `SYM × TIME × FIELD` | 金融时序的自然建模 |
| 数据布局 | Splayed，**一个 FIELD 一个文件** | 列级独立读/并行，投影裁剪天然成立 |
| SYM | 连续排列 | row range 连续，mmap 友好 |
| TIME | 每个 SYM 有自己的时间范围/偏移 | 不同 SYM 时间粒度/起点可不同 |
| TIME 类型 | **仅 `DATE32` / `TIMESTAMP_US`** | 二值简化，不做通用 time unit |
| FIELD | 只存固定宽度类型 | 保证 O(1) 行定位与零拷贝 |
| STRING | **不支持** | 破坏固定宽度布局 |
| FIELD Block / Index | **不做** | 保持连续、简化 mmap |
| Block Index | **不做** | 同上 |
| NULL bitmap | **不做** | 用类型内置特殊值编码 NULL |
| NULL | 类型内置特殊值 | 无额外位图，保持固定宽度 |
| META | 唯一定位索引 | 承载 SYM/TIME → row range 映射 |
| FIELD | 纯数据 | 不含 SYM/TIME/索引/NULL bitmap |
| 读取 | mmap + range read | 零拷贝优先路径 |
| 写入 | 全量预分配 + 原地更新 | `create_field` 预分配，`update_field` 原地填值，不重整 |
| 压缩 | 可选 | NONE 为最快路径，ZSTD/LZ4 用于冷数据 |
| Arrow | 交换层，不作为底层格式 | 解耦底层布局 |
| DataFusion | 原生 Rust Adapter | 通过 TableProvider 接入 |
| DuckDB | Extension | 两阶段：先 Arrow，后 Native ColumnView |
| Scanner | 自己实现的核心扫描器 | 不被生态库绑架 |
| 并行 | Reader/Writer 都支持 | Field/SYM/range 层并行读，encode/compress 层并行写 |
| 核心语言 | **Rust** | 内存安全 + 高性能 + 生态 |

---

# 3. 系统架构

```text
                         +---------------------+
                         |      User / SQL     |
                         +----------+----------+
                                    |
                       +------------+------------+
                       |                         |
                 +-----+-----+             +----+-----+
                 | DataFusion |             |  DuckDB   |
                 +-----+-----+             +----+------+
                       |                         |
                       |                         |
                 Arrow Adapter             DuckDB Extension
                       |                         |
                       +----------+--------------+
                                  |
                           +------+------+
                           |   Scanner   |
                           +------+------+
                                  |
                    +-------------+-------------+
                    |                           |
              +-----+-----+               +----+-----+
              |    META    |               |  FIELDs  |
              |  indexing  |               |  values  |
              +-----+-----+               +----+-----+
                    |                            |
                    +-----------+----------------+
                                v
                         mmap / filesystem
```

**最重要的边界：**

> **META 决定「读哪里」，FIELD 决定「读什么」。**

任何上层（DataFusion / DuckDB / 自定义 SQL）都必须通过 Scanner 接入，不直接理解 META/FIELD 的内部布局。

---

# 4. 数据模型与物理布局

## 4.1 布局原则

- 一个 FIELD 一个文件（`close`、`open`、`high`、`low`、`volume`…）。
- 一个 SYM 的数据在 FIELD 文件中**连续排列**（`row_start` + `time_count`）。
- META 是唯一索引，指向每个 SYM 的 row range 与 time range。
- FIELD 文件采用**全量预分配**：`create_field` 时按 `sum(time_count)` 预写全 NULL 占位数据，后续通过 `update_field` 原地填入实际值。全量预声明模型下 `time_count` 既是「该 SYM 在全局 TIME AXIS 中占据的连续区间长度」，也是「该 SYM 在 FIELD 中预分配的行容量」，二者恒等，无需单独的 `row_capacity` 字段。

## 4.2 Partition 组织策略

Partition **不需要写死在格式里**，是数据组织策略，不是 FIELD/META 格式的一部分。

```text
dataset/
+-- 2024/
|   +-- .meta
|   +-- close
|   +-- open
|   +-- high
|   +-- low
|   +-- volume
+-- 2025/
|   +-- .meta
|   +-- ...
+-- 2026/
    +-- .meta
    +-- ...
```

也可自由选择粒度：

```text
2026-Q1
2026-Q2
2026-01
...
```

> **决策理由（resolve #2）**：TIME AXIS 采用**全局一条**——所有 SYM 共享同一份「去重后按序排列」的时间轴（META header 的 `time_count` 为该轴元素总数）；每个 SYM 通过 `time_start` / `time_count` 指向该轴中的一段。这样既能表达「每个 SYM 自己的时间范围」，又使 TIME → row range 的映射成为一次 O(1) 下标运算。若将来需要不同 SYM 拥有完全不同粒度的时间，可由上层 Partition 策略承担（如按 SYM 分组为多个 dataset 目录），格式层保持单一全局轴。
---

# 5. 文件格式规范（权威定义）

## 5.1 META 格式

### Header（固定 64 字节）

| Offset | 长度 | 字段 | 类型 | 说明 |
| ---: | ---: | --- | --- | --- |
| 0 | 8 | `magic` | uint64 | 文件格式标识，如 `SPLAYMETA` |
| 8 | 2 | `version` | uint16 | META 格式版本 |
| 10 | 2 | `flags` | uint16 | 预留 |
| 12 | 1 | `time_type` | uint8 | 0 = DATE32，1 = TIMESTAMP_US |
| 13 | 3 | reserved | bytes | 预留 |
| 16 | 8 | `generation` | uint64 | 当前 META 数据版本，用于校验 FIELD 是否匹配 |
| 24 | 4 | `time_count` | uint32 | TIME AXIS 元素数量 |
| 28 | 4 | `sym_count` | uint32 | SYM 数量 |
| 32 | 8 | `sym_dict_offset` | uint64 | SYM Dictionary 起始位置 |
| 40 | 8 | `sym_index_offset` | uint64 | SYM INDEX 起始位置 |
| 48 | 8 | `file_size` | uint64 | META 文件总大小 |
| 64 |  | HEADER END |  | 固定 64 字节 |

### 数据区

| 区段 | 布局 | 说明 |
| --- | --- | --- |
| TIME AXIS | `time_count × (4 或 8)` bytes | DATE32 = 4B，TIMESTAMP_US = 8B；全局去重按序 |
| SYM DICT INDEX | `(sym_count + 1) × 8` bytes | 每个 SYM 字符串在 STRING DATA 中的 offset |
| SYM STRING DATA | variable | 所有 SYM 字符串连续存储 |
| SYM INDEX | `sym_count × 12` bytes | 每个 SYM 一个固定 12B record |

### 偏移字段语义

- `sym_dict_offset` → SYM Dictionary 起始（含 DICT INDEX + STRING DATA）
- `sym_index_offset` → SYM INDEX 数组起始
- `file_size` → META 文件总大小（用于完整性/截断校验）
- `TIME AXIS` 固定偏移 64 字节

### SYM INDEX record（固定 12 字节）

| Offset | 长度 | 字段 | 类型 | 说明 |
| ---: | ---: | ---: | --- | --- |
| 0 | 4 | `time_start` | uint32 | 该 SYM 在全局 TIME AXIS 中的起始下标 |
| 4 | 4 | `time_count` | uint32 | 该 SYM 的时间点数量 = **行容量**（全量预声明） |
| 8 | 4 | `row_start` | uint32 | 该 SYM 在 FIELD 文件中的起始 row = **累计 sum(前序所有 SYM 的 time_count)** |

> 在全量预声明模型下，`time_count` 同时表示「该 SYM 在全局 TIME AXIS 中占据的连续区间长度」和「该 SYM 在 FIELD 中预分配的行容量」。二者恒等，无需单独的 `row_capacity` 字段。每个 SYM 的区间为 `[time_start, time_start + time_count)`，区间内缺失的时间点在 FIELD 中以 NULL 特殊值填充。

**核心映射：**

| SYM | 时间起点 | 时间数量 | 数据起点 |
| --- | ---: | ---: | ---: |
| SYM01 | 0 | 250 | 0 |
| SYM02 | 0 | 250 | 250 |
| SYM03 | 2 | 248 | 500 |
| SYM04 | 0 | 250 | 748 |

`row_start` + `time_count` 决定 FIELD 中该 SYM 的预分配行范围 `[row_start, row_start + time_count)`。全局 row 定位公式：

```text
row_start(SYM_n) = sum(time_count(SYM_0 .. SYM_{n-1}))
global_row = row_start + (time_index - time_start)
```

## 5.2 FIELD 格式

一个字段一个文件，物理布局：

```text
+------------------------------+
| HEADER             64 bytes  |
+------------------------------+
| DATA                         |
| value[0]                     |
| value[1]                     |
| ...                          |
| value[N-1]                   |
+------------------------------+
```

**FIELD 不保存：** SYM、TIME、STRING、FIELD Index、Block Index、NULL bitmap。

### Header（固定 64 字节）

| Offset | 长度 | 字段 | 类型 | 说明 |
| ---: | ---: | --- | --- | --- |
| 0 | 8 | `magic` | uint64 | FIELD 文件标识 |
| 8 | 2 | `version` | uint16 | FIELD 格式版本 |
| 10 | 2 | `flags` | uint16 | 属性 |
| 12 | 1 | `data_type` | uint8 | 数据类型 |
| 13 | 1 | `encoding` | uint8 | 编码 |
| 14 | 1 | `compression` | uint8 | 压缩 |
| 15 | 1 | reserved | uint8 | 预留 |
| 16 | 8 | `generation` | uint64 | 与 META 的 generation 校验 |
| 24 | 4 | `row_count` | uint32 | 数据总行数 |
| 28 | 4 | `null_count` | uint32 | NULL 数量 |
| 32 | 4 | reserved | uint32 | 预留 |
| 36 | 4 | reserved | uint32 | 预留 |
| 40 | 8 | `data_length` | uint64 | DATA 区字节长度 |
| 48 | 16 | reserved | bytes | 未来扩展 |
| 64 |  | HEADER END |  | 固定 64 字节 |

```text
data_offset = 64
```

### 5.2.1 预分配与原地更新模型

FIELD 文件的生命周期由函数接口驱动：

| 阶段 | 函数 | FIELD 状态 |
| --- | --- | --- |
| 创建 | `create_field` | 按 `total_rows = sum(time_count)` 预分配，全部填 NULL 特殊值 |
| 填值 | `update_field` | 按绝对 row 偏移原地写入实际值，覆盖 NULL |
| 压缩 | `compact_field` | 压缩为只读，退出原地更新路径 |

**PLAIN + NONE 下的 `update_field` 写入公式：**

```text
offset = 64 + start_row × sizeof(type)

update_info = [start_row, values[]]
  → 逐元素写入 FIELD[offset, offset + len(values) × sizeof(type))
```

**约束：**

- `update_field` 不改变 `row_count`、不扩展文件长度。
- 写入超出 `[0, total_rows)` 的 row 报错。
- `compact_field` 后 FIELD 转为只读，`update_field` 拒绝写入。
- `generation` 在 `update_field` 成功后递增，保持与 META 校验一致。



| ID | 类型 | 单值大小 | NULL 表示 |
| --: | --- | ---: | --- |
| 0 | `BOOL` | 1 B | 保留值（如 `0x02`） |
| 1 | `INT32` | 4 B | `INT32_MIN` |
| 2 | `INT64` | 8 B | `INT64_MIN` |
| 3 | `FLOAT32` | 4 B | canonical NaN |
| 4 | `FLOAT64` | 8 B | canonical NaN |
| 5 | `DATE32` | 4 B | `INT32_MIN` |
| 6 | `TIMESTAMP_US` | 8 B | `INT64_MIN` |

Arrow / Parquet 对应关系：

| TIME 类型 | Arrow | Parquet |
| --- | --- | --- |
| `DATE32` | `date32` | DATE |
| `TIMESTAMP_US` | `timestamp[us]` | TIMESTAMP_MICROS |

META Header 的 `time_type` 直接声明其一，不再拆 `time_type` / `time_unit`。

## 5.4 NULL 编码

不使用 validity bitmap，用类型内置特殊值。判断 NULL 时**比较 bit pattern**，而非 `value == NaN`。

| 类型 | NULL bit pattern |
| --- | --- |
| INT32 | `0x80000000` |
| INT64 | `0x8000000000000000` |
| FLOAT32 | `0x7FC00000` |
| FLOAT64 | `0x7FF8000000000000` |
| DATE32 | `0x80000000` |
| TIMESTAMP_US | `0x8000000000000000` |
| BOOL | 保留值，例如 `0x02` |

FLOAT 语义：

```text
canonical NaN = NULL
其他 NaN     = 普通 NaN
```

## 5.5 Encoding

| ID | 编码 | 说明 |
| --: | --- | --- |
| 0 | `PLAIN` | 原始固定宽度数组（默认） |
| 1 | `DELTA` | Delta 编码（时间/整数） |
| 2 | `RLE` | Run Length Encoding（重复值） |
| 3 | `BITPACK` | 位打包（小整数） |

**优先级：PLAIN 是第一优先级实现。** 先把 `mmap + PLAIN` 做到极致，再加其他 encoding。

## 5.6 Compression

| ID | 压缩 | 特点 |
| --: | --- | --- |
| 0 | `NONE` | mmap / O(1) 随机访问 |
| 1 | `ZSTD` | 高压缩率 |
| 2 | `LZ4` | 高解压速度 |

### NONE

```text
offset = 64 + row × sizeof(type)
```

直接定位。

### 压缩（ZSTD/LZ4）

不做 FIELD Block Index，因此压缩 FIELD 更适合：

```text
大范围扫描 / 冷数据 / 归档
```

而非随机单点查询。

---

# 6. Generation 版本一致性

用于判断 META 与 FIELD 是否属于同一个数据版本：

```text
META    generation = 12345
close   generation = 12345
volume  generation = 12345
```

若 FIELD generation 与 META 不匹配（如 `close = 12344`），Reader 拒绝使用。

**决策理由（resolve #1）**：V1 使用**单独的 uint64 generation 序列**（严格单调递增），而非 wall-clock timestamp，因为 timestamp 不是严格单调的（时钟回拨）。

---

# 7. Reader 设计

实现：

```text
mmap
SYM lookup
TIME lookup
row range
column read
```

目标：**O(1) 定位**。

## 7.1 mmap（V1 最重要性能路径）

对于 `PLAIN + NONE`，优先 mmap：

```text
META
 |
 v
row range
 |
 v
pointer / slice
```

不复制整个 FIELD。Reader 通过 mmap 获得指针后直接切片返回。

## 7.2 并行读取

### Field 并行

```text
Thread 1 -> close
Thread 2 -> volume
Thread 3 -> high
Thread 4 -> low
```

### SYM 并行

```text
Thread 1 -> SYM 0~999
Thread 2 -> SYM 1000~1999
Thread 3 -> SYM 2000~2999
Thread 4 -> SYM 3000~3999
```

实际通过 Thread Pool 调度，不创建与 SYM 数量相等的线程。

---

# 8. Writer 设计

## 8.1 写入架构

写入**不要**多线程同时 `update_field` 同一个 FIELD（单 FIELD 内串行，多 FIELD 间可并行）。

```text
Input (update_table)
 |
 +-- 按 (SYM, TIME) 查 META 得到 global_row
 |
 +-- 整理为 update_info[start_row, values[]]
 |
 +-- Worker 1  -- update_field(field_a)
 +-- Worker 2  -- update_field(field_b)   <- 不同 FIELD 可并行
 +-- Worker 3  -- update_field(field_c)
 +-- Worker 4  -- update_field(field_d)
       |
       v
 FIELD (原地写入，不扩展长度)
       |
       v
 generation 递增 + fsync
```

## 8.2 写入策略

> **函数接口模型下的写入语义**：在全量预声明模型下，FIELD 文件在 `create_field` 时已按 `sum(time_count)` 全量预分配。热数据的「追加」不再是扩展文件长度，而是通过 `update_table` 将新值写入预分配的 NULL 占位位置——即「原地填值」。`compact_field` 是唯一改变文件长度的写操作（压缩重写）。

热数据：

```text
FIELD (create_field 后全量预分配)
  +-- [NULL NULL NULL ... NULL]   ← 初始占位
  +-- update_field 填值           ← 覆盖 NULL，不扩展文件
  +-- update_field 填值
  +-- update_field 填值
```

不要求重整整个文件。

`.meta` 更新采用原子提交：

```text
.meta.new
   |
   v
fsync
   |
   v
atomic rename
   |
   v
.meta
```

保证 Reader 看到完整 generation。

## 8.3 Crash recovery

V1 需要覆盖：FIELD 原地更新未完成、META 原子替换失败等场景。以 META 的 `generation` + `file_size` 为一致性锚点，启动时校验 FIELD 的 generation 与数据长度，拒绝不匹配部分。

## 8.4 函数接口规范

以下七个函数是 Writer 层的权威 API 定义。Arrow 相关函数（`create_meta` / `create_table` / `update_table`）位于 `splayed-arrow` crate，纯核心函数位于 `splayed-core` crate。

### create_meta(folder, data, sorted)

**所在 crate：** `splayed-arrow`

**输入：**

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `folder` | path | 目录路径 |
| `data` | ARROW BATCHRECORD | 含两列：`TIME`（DATE32 或 TIMESTAMP_US）、`SYM`（STRING） |
| `sorted` | bool | 标识输入是否已按 (SYM, TIME) 升序排列 |

**处理：**

1. 提取 `TIME` 列去重升序 → 全局 TIME AXIS
2. 提取 `SYM` 列去重升序 → SYM DICT + SYM INDEX
3. 对每个 SYM 计算 `time_start` / `time_count`：
   - `time_start` = 该 SYM 数据点在 TIME AXIS 中的最小下标
   - `time_count` = `max_index - min_index + 1`（连续区间，缺失为 NULL）
4. `row_start` = 累计 `sum(前序 time_count)`
5. 写入 `.meta`（原子提交）

**优化路径：** `sorted=true` 时跳过排序步骤。

**约束：** META 创建后不可变，不支持扩展 `time_count` 或新增 TIME AXIS 元素。

### create_table(folder, data, sorted)

**所在 crate：** `splayed-arrow`

**语义：** 一步完成 `create_meta` + `create_field` + `update_field`。本质上是将三步合一，可优化执行路径减少多次扫描。

**输入：**

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `folder` | path | 目录路径 |
| `data` | ARROW BATCHRECORD | 含 `TIME`（DATE32 / TIMESTAMP_US）+ `SYM`（STRING）+ 若干 FIELD 列 |
| `sorted` | bool | 标识输入是否已按 (SYM, TIME) 升序排列 |

**处理：**

1. 提取 `TIME` / `SYM` 列，计算 `.meta` 全部信息（同 `create_meta` 步骤 1–4）
2. 从 `data` 的 FIELD 列推断各列 `data_type`
3. 单次遍历 `data`，同时完成：
   - 写入 `.meta`
   - 为每个 FIELD 列预分配 FIELD 文件（`create_field`）
   - 将每行 FIELD 值按 `(SYM, TIME) → global_row` 映射整理为 `update_info`，原地写入（`update_field`）
4. `sorted=true` 时跳过排序步骤，且 row 映射可按流式计算（无需构建完整哈希表）

**优化路径：** `sorted=true` 时，数据已按 (SYM, TIME) 排列，`global_row` 随行单调递增，可避免：
- 全局排序
- 构建 `(SYM, TIME) → global_row` 哈希表
- 对 FIELD 值的二次重排

直接流式写入 FIELD DATA 区，等价于一次顺序写。

**约束：**

- 目录须为空或不存在（不允许多次 `create_table` 同一目录）。
- 各 FIELD 列的 `data_type` 由 Arrow schema 推断，须为 §5.3 支持的类型。
- 创建后 META 不可变，后续更新须使用 `update_table`。

### create_field(field_path, data_type)

**所在 crate：** `splayed-core`

**输入：**

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `field_path` | path | 字段文件路径（与 `.meta` 同目录） |
| `data_type` | uint8 | 数据类型 ID（见 §5.3） |

**处理：**

1. 读取同级目录 `.meta` 的 SYM INDEX
2. `total_rows = sum(time_count)`
3. 自动确定 `encoding = PLAIN`，`compression = NONE`
4. 预分配 FIELD 文件：`header(64B) + total_rows × sizeof(type)`
5. DATA 区全部填入该类型的 NULL 特殊值
6. 写入 FIELD header

### delete_field(field_path)

**所在 crate：** `splayed-core`

删除字段文件。若 FIELD 不存在则 no-op。

### update_field(field_path, update_info[])

**所在 crate：** `splayed-core`

**输入：**

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `field_path` | path | 字段文件路径 |
| `update_info[]` | array | 更新项数组，每项 = `[start_row: u32, values: Vec<value>]` |

**处理：**

1. 校验 FIELD：`compression = NONE`（压缩后只读，拒绝更新）
2. 对每项：`offset = 64 + start_row × sizeof(type)`
3. 校验 `[start_row, start_row + len(values)) ⊂ [0, total_rows)`
4. 原地写入 `values`
5. 更新 FIELD header 的 `generation`（递增）
6. `fsync`

**约束：** 不改变 `row_count` / `null_count`（全量预声明下二者固定）。

### compact_field(field_path, compression)

**所在 crate：** `splayed-codec`

**输入：**

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `field_path` | path | 字段文件路径 |
| `compression` | uint8 | 目标压缩 ID（ZSTD / LZ4） |

**处理：**

1. 读取当前 FIELD 数据（必须 `compression = NONE`）
2. 按指定算法压缩
3. 重写 FIELD 文件，更新 header 的 `compression` 字段
4. 压缩后 FIELD 转为只读

### update_table(folder, data, sorted)

**所在 crate：** `splayed-arrow`

**输入：**

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `folder` | path | 表目录路径（含 `.meta` 和若干 FIELD 文件） |
| `data` | ARROW BATCHRECORD | 含 `TIME` + `SYM` + 若干 FIELD 列 |
| `sorted` | bool | 标识输入是否已排序 |

**处理：**

1. 读取 `.meta`，构建 `(SYM, TIME) → global_row` 映射
2. 扫描当前目录下所有 FIELD 文件，确定可更新的列集合
3. 对 `data` 中每个 FIELD 列：
   1. 按 `(SYM, TIME)` 查 META 得到每个数据点的 `global_row`
   2. 整理为 `update_info[start_row, values[]]` 格式
   3. 调用 `update_field`
4. `sorted=true` 时可优化映射查找路径

**约束：**

- 仅更新已存在的 `(SYM, TIME)` 位置。
- `data` 中 `(SYM, TIME)` 不在 META 中则报错（不扩展）。
- `data` 中包含 META 中不存在的 FIELD 列则报错或跳过。

### 失败模式

| 场景 | 处理 |
| --- | --- |
| `update_field` 写入已压缩 FIELD | 拒绝，报错（只读） |
| `update_field` row 超出 `[0, total_rows)` | 拒绝，报错 |
| `update_table` 传入 META 中不存在的 `(SYM, TIME)` | 报错，不静默扩展 |
| `update_table` 传入 .meta 不存在的 FIELD 列 | 报错或跳过（实现选择） |
| `create_field` 时 `.meta` 不存在 | 报错 |
| `compact_field` 时 `compression != NONE` | 拒绝（不允许二次压缩） |
| crash 后 FIELD generation 与 META 不匹配 | Reader 拒绝使用该 FIELD |

---

# 9. Scanner API 与执行流程

## 9.1 Scanner API

Scanner 是自研库，不是现成库。核心接口：

```text
Scanner
```

接收参数：

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `columns` | FieldId[] | 要读取的字段 |
| `symbols` | SymbolSelection | SYM 过滤 |
| `time_range` | TimeRange | 时间过滤 |
| `filter` | Filter | FIELD 值过滤 |
| `batch_size` | usize | Batch 大小 |
| `parallelism` | usize | 并行度 |

示例：

```text
columns:     [close, volume]
symbols:     [AAPL, MSFT]
time_range:  [2026-01-01, 2026-03-01)
filter:      close > 100
batch_size:  65536
parallelism: auto
```

## 9.2 执行流程

```text
ScanRequest
      |
      v
 META lookup
      |
      +-- SYM pruning
      |
      +-- TIME pruning
      |
      v
 row ranges
      |
      v
 projection pruning
      |
      v
 FIELD Reader
      |
      v
 mmap / read
      |
      v
 filter
      |
      v
 Batch
```

关键：`WHERE sym = ...` 不是扫描后过滤，而是：

```text
META -> 直接定位
```

这是性能关键。

## 9.3 Pushdown

### Projection（必须支持）

```sql
SELECT close FROM ...
```

只打开 `close`，不读 `open/high/low/volume`。

### Predicate（必须支持）

```sql
WHERE sym = 'AAPL' AND time >= ...
```

转换：SYM → SymbolId，TIME → row range，只读目标范围。

### Filter

```sql
WHERE close > 100
```

不能提前靠 META 定位，流程：

```text
META pruning -> 读取 close -> close > 100
```

可用 SIMD 加速。

## 9.4 GROUP BY / Aggregate 预留

V1 不把 SQL 执行引擎塞进 Core，但预留：COUNT / SUM / MIN / MAX / AVG。

尤其 `GROUP BY sym` 非常适合本布局：

```text
SYM01 -> local aggregate
SYM02 -> local aggregate
SYM03 -> local aggregate
...
```

然后上层 merge。

优先级：

```text
Projection        *****
Predicate         *****
SYM pruning       *****
TIME pruning      *****
Aggregation       ****
GROUP BY SYM      ****
复杂 GROUP BY     *
```

## 9.5 Batch API

Scanner 不一次返回全部数据：

```text
Scanner
 |
 +-- Batch 64K
 +-- Batch 64K
 +-- Batch 64K
 +-- Batch 20K
```

`next_batch()` 是连接 Arrow/DataFusion/DuckDB 的关键。

---

# 10. 对外集成

## 10.1 Arrow 转换

Arrow 只是交换层。

### Splayed → Arrow

```text
Splayed Field
      |
      v
 native array
      |
      v
 Arrow Array
      |
      v
 RecordBatch
```

### NULL 语义保留

```text
canonical NaN    ->  Arrow validity bitmap = 0（NULL）
其他普通 NaN     ->  Arrow validity = 1, value = NaN
```

这样语义不会丢失。

## 10.2 Native ColumnView（核心接口）

为了同时服务 DataFusion 和 DuckDB，Core 不应该只暴露 Arrow。Core 有：

```text
ColumnView
```

概念：

```text
ColumnView
+-- data_type
+-- ptr / slice
+-- length
+-- null information
```

于是：

```text
Splayed Core
       |
       v
 ColumnView
    /     \
   /       \
 Arrow     DuckDB
```

这是整个系统未来性能优化的关键接口。

## 10.3 DataFusion 集成

实现：

```text
SplayedTableProvider
```

链路：

```text
DataFusion
    |
    v
TableProvider
    |
    v
SplayedExecutionPlan
    |
    v
SplayedScanner
    |
    v
RecordBatch
```

重点支持：Projection Pushdown、Predicate Pushdown、SYM pruning、TIME pruning、Batch scan。

## 10.4 DuckDB 集成

实现 DuckDB Extension，例如：

```sql
SELECT * FROM read_splayed('2026');
```

链路：

```text
DuckDB
  |
  v
Splayed Extension
  |
  v
Splayed Scanner
```

初期可以：

```text
Splayed -> Arrow -> DuckDB
```

性能成熟后：

```text
Splayed -> Native ColumnView -> DuckDB DataChunk
```

减少 Arrow 转换。

---

# 11. Rust Crate 结构

```text
splayed/
|
+-- splayed-format/          # 无依赖：格式定义
|   +-- meta.rs
|   +-- field.rs
|   +-- header.rs
|   +-- types.rs
|
+-- splayed-codec/           # format ← codec：编码/压缩
|   +-- plain.rs
|   +-- delta.rs
|   +-- rle.rs
|   +-- compression.rs
|
+-- splayed-core/            # format + codec ← core：不含 Arrow
|   +-- dataset.rs
|   +-- reader.rs
|   +-- field_writer.rs      # create_field / update_field / delete_field
|   +-- compact.rs           # compact_field
|   +-- scanner.rs
|   +-- mmap.rs
|   +-- parallel.rs
|
+-- splayed-arrow/           # core + arrow ← arrow：Arrow 交换层
|   +-- to_arrow.rs
|   +-- from_arrow.rs
|   +-- meta_writer.rs       # create_meta / create_table
|   +-- table_writer.rs      # update_table
|
+-- splayed-datafusion/      # arrow → datafusion
|   +-- provider.rs
|
+-- splayed-duckdb/
    +-- extension/
```

依赖方向：`format`（无依赖）← `codec` ← `core` → `arrow` → `datafusion` / `duckdb`。

> **分层原则**：`splayed-core` 不依赖 Arrow。`create_meta` / `create_table` / `update_table` 因需 Arrow BATCHRECORD 输入，位于 `splayed-arrow`；`create_field` / `update_field` / `delete_field` / `compact_field` 为纯核心操作，位于 `splayed-core`。`compact_field` 的压缩实现位于 `splayed-codec`，由 `splayed-core` 调用。

---

# 12. 开发阶段（Phase 1–9）

## Phase 1：Format

完成 META + FIELD，支持 DATE32 / TIMESTAMP_US / INT32 / INT64 / FLOAT32 / FLOAT64 / BOOL，只支持 PLAIN + NONE。META 由 `create_meta` 从 Arrow 数据驱动生成（见 §8.4），不再是手动 schema 声明。

目标：**先把最简单路径做到极致。**

## Phase 2：Reader

实现 mmap、SYM lookup、TIME lookup、row range、column read。目标：**O(1) 定位**。

## Phase 3：Writer

实现函数接口（见 §8.4）：
- `create_field`（预分配 placeholder，`splayed-core`）
- `update_field`（原地更新，`splayed-core`）
- `delete_field`（`splayed-core`）
- `compact_field`（NONE → ZSTD/LZ4，`splayed-codec`）
- `create_meta` / `create_table` / `update_table`（Arrow 输入，`splayed-arrow`）
- generation、crash recovery

## Phase 4：Scanner ✅ Done

实现 Projection、SYM filter、TIME filter、FIELD filter、batch、parallelism。

## Phase 5：性能优化 ✅ Done (SIMD filter + prefetch + parallel scan)

加入 SIMD、parallel scan、parallel field read、page-cache friendly access、prefetch。

## Phase 6：Compression ✅ Done

加入 ZSTD、LZ4、DELTA、RLE、BITPACK，但不破坏 PLAIN + NONE 这条最快路径。

## Phase 7：Arrow ✅ Done

实现 Splayed → Arrow 与 Arrow → Splayed，确保 NULL / NaN / DATE32 / TIMESTAMP_US 正确映射。

## Phase 8：DataFusion ✅ Done

实现 TableProvider、ExecutionPlan、Projection Pushdown、Predicate Pushdown。

## Phase 9：DuckDB ✅ Done (Arrow IPC bridge)

先 DuckDB → Arrow → Splayed 与 Splayed → Arrow → DuckDB，再优化为 Native ColumnView。

---

# 13. 性能基准（从第一天开始）

不要等做完才 benchmark。至少建立：

| Benchmark | 指标 |
| --- | --- |
| META SYM lookup | ns/op |
| META TIME lookup | ns/op |
| single row read | ns/op |
| 1K row read | GB/s |
| 64K row scan | GB/s |
| full FIELD scan | GB/s |
| 1 SYM query | latency |
| 100 SYM query | latency |
| 5000 SYM query | throughput |
| create_field (预分配) | rows/s |
| update_field (原地更新) | rows/s |
| update_table | rows/s |
| multi-thread update | rows/s |
| compression | GB/s |
| decompression | GB/s |
| Arrow conversion | GB/s |
| DataFusion query | latency |
| DuckDB query | latency |

### vs Parquet 对比矩阵

```text
SYM 维度：single SYM / 10 SYM / 100 SYM / all SYM
TIME 维度：1 day / 1 month / 1 year / all years
FIELD 维度：1 field / 5 fields / 20 fields
```

这样才能知道格式在哪些 workload 上赢。

---

# 14. V1 最终物理模型

```text
                  DATASET
                     |
       +-------------+-------------+
       |                           |
     META                     FIELD files
       |                           |
       |                 +---------+---------+
       |                 |         |         |
       |               close      open      volume
       |                 |         |         |
       v                 v         v         v
 SYM -> row range      FLOAT64   FLOAT64    INT64
 TIME -> row range
       |
       v
    Scanner
       |
       v
   ColumnView
      /    \
     /      \
 Arrow     Native
   |          |
   v          v
DataFusion  DuckDB
```

---

# 15. 最重要的性能原则（8 条规则）

1. **META 负责定位，FIELD 只负责数据。**
2. **FIELD 永远保持连续，不做逻辑 Block。**
3. **不存 STRING，不存 FIELD Index，不存 Block Index。**
4. **SYM/TIME 是 Scanner 的一等过滤条件。**
5. **PLAIN + NONE 是最高性能路径，直接 mmap。**
6. **Arrow 是交换层，不是底层存储格式。**
7. **读并行化在 FIELD / SYM / range 层面，写并行化在 encode/compress 层面。**
8. **DataFusion/DuckDB 通过 Scanner 接入，而不是让它们理解 META/FIELD。**

> 第一版甚至不需要先做复杂压缩、GROUP BY pushdown、DuckDB Native DataChunk。先把 `META → row range → mmap FIELD → ColumnView → Arrow` 这条链做到极致，就是整个系统性能的地基。