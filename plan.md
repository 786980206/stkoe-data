# Splayed V1 — 设计文档

> 目标：**针对 SYM × TIME 的金融时序数据，优先做到极低 I/O、O(1) 定位、mmap/零拷贝读取、高吞吐追加写入，并通过 Arrow 连接 DataFusion / DuckDB。**

> **读者指引**：本文件是**权威设计文档**（源码注释按 §号引用）。用户文档见 MkDocs 站点 `docs/`（快速上手/架构/格式规范/Crate 参考/集成指南，构建方式见 `docs/development/build-test.md`）。
> **状态标注约定**：文中带 ⚠ 的段落表示「未实现 / 预留 / 规划」，其余均指**当前已实现**状态；实现进度以 `docs/development/phases.md` 为准。

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
- **字段文件名规则**：字段名**可以包含 `.`**（如 `close.bid`、`price.usd`）；但**以 `.` 开头的文件/目录一律忽略**——`list_fields` / `update_meta` 字段枚举 / 分区 schema 发现都跳过，创建侧（`create_field` / `create_field_with_data` / `create_table` / `update_table`）拒绝以 `.` 开头的字段名（`.meta` 及隐藏/元数据文件，如 `.DS_Store`，不属于字段）。
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

### 5.2.2 统计 Footer（min/max）

可选、定长 28 字节、追加在数据区之后（`splayed_format::field_footer`）：

```text
[0..4)   magic   u32 LE = "SFTF"
[4)      version u8 = 1
[5)      flags   u8 = bit0: min/max 有效
[6..8)   reserved u16
[8..16)  min     [u8;8]（原始 LE 槽位，宽度 = DataType::size_of()）
[16..24) max     [u8;8]
[24..28) total   u32 LE = 28
```

规则与使用：

- **min/max 计算跳过 NULL 哨兵**（浮点 canonical NaN、INT_MIN、无符号全 1）；
  全 NULL 列 flags=0（或无 footer）。`FieldReader::stats() -> Option<FieldStats>`。
- **写入时机**：`create_field_with_data`（含 `create_table`）写值后追加并写**真实 `null_count`**；
  `compact_field` 按原始数据**重算**（compacted 只读，统计永久有效）。
- **`update_field` 增量维护（O(k)，不失效）**：
  写前快照被覆盖行段旧值 → `null_count` 精确增量（header，写回）；min/max =
  `min(旧 min, 写入批次 min)` / `max(旧 max, 写入批次 max)`（**保守上界**，
  永远安全）；仅当被覆盖行**命中旧极值**才全列重扫精确重算（罕见路径）。
  无有效 footer（从未建立/已失效）时保持无统计（归档时重算）。
- **读侧**：`field_length` 仍指纯数据字节；读取按末尾 magic 检测 footer，
  数据区按 `[header, data)` 界定（压缩文件同理，payload 排除 footer）。
- **消费**：`Scanner::plan` 用 filter 列的 min/max 做**整数据集剪裁**——任一
  filter 与统计不相交（如 `close > 204` 而 max=204）→ 空计划，任何 FIELD
  都不读；DataFusion `statistics()` 对 FIELD 列上报 min/max（`Precision::Exact`）
  与 TIME 列的 `[time_axis[0], time_axis[-1]]`。
- **注意**：header `null_count` 自 `create_field_with_data`/`update_field` 起为
  **真实值**（精确增量维护），预分配占位语义仅剩 `create_field`（全 NULL）；
  `IsNull/IsNotNull` 剪裁仍交由行级过滤保证。

### 5.2.3 读取期限值（LIMIT 下推）

`ScanRequest.limit: Option<usize>`：三个扫描入口（`scan` / `scan_owned` /
`scan_owned_parallel`）与 `scan_all_parallel` 在**产出通过过滤的第 N 行后停止**
（末批 `CoreBatch::slice` 截断，不再拉取/解码后续批次）。DataFusion 的
`scan(limit)` 直接透传（多分区仍由引擎 LIMIT 节点保证全局语义）；Polars 的
`n_rows` 也映射到该字段。

## 5.3 数据类型

| ID | 类型 | 单值大小 | NULL 表示 |
| --: | --- | ---: | --- |
| 0 | `BOOL` | 1 B | 保留值（如 `0x02`） |
| 1 | `INT32` | 4 B | `INT32_MIN` |
| 2 | `INT64` | 8 B | `INT64_MIN` |
| 3 | `FLOAT32` | 4 B | canonical NaN |
| 4 | `FLOAT64` | 8 B | canonical NaN |
| 5 | `DATE32` | 4 B | `INT32_MIN` |
| 6 | `TIMESTAMP_US` | 8 B | `INT64_MIN` |
| 7 | `INT8` | 1 B | `INT8_MIN`（`0x80`） |
| 8 | `INT16` | 2 B | `INT16_MIN`（`0x8000`） |
| 9 | `UINT8` | 1 B | 全 1（`0xFF`） |
| 10 | `UINT16` | 2 B | 全 1（`0xFFFF`） |
| 11 | `UINT32` | 4 B | 全 1（`0xFFFFFFFF`） |
| 12 | `UINT64` | 8 B | 全 1（`0xFFFF…`） |
| 13 | `DATE64` | 8 B | `INT64_MIN` |

> 7–13 为**增量扩展**：只新增 ID 取值，不改动 ID 0–6 的语义，旧文件零迁移。
> 无符号类型的 NULL 借用全 1（MAX）位型——比较仍是 bit pattern。

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
| INT8 / INT16 | `0x80` / `0x8000` |
| UINT8 / UINT16 / UINT32 / UINT64 | 全 1（`0xFF…`） |
| DATE64 | `0x8000000000000000` |

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

# 5.7 SUBSET 文件（`.sub.xxx`）

## 5.7.1 定位

`.sub.xxx` 与 `.meta` 同目录，是**父 `.meta` 的 `SYM × TIME` 网格子集**的索引文件：
记录选中 `(SYM, TIME)` 对应的**父全局行空间区间**，指向 FIELD 文件的 data 区。
典型场景："全市场 A 股"为 `.meta`，`".sub.hs300"` 为沪深300 成分的时序子集。
**文件本身不存值**，只有索引（同 `.meta` 的"只记录索引"语义）。

文件名：`name` 如 `hs300` → `.sub.hs300`（`SUBSET_PREFIX = ".sub."`）。以 `.` 开头
→ 天然被 `Dataset::list_fields` / `update_meta::list_field_names` 忽略，**不会被当
成 FIELD**；`create_field*` 也拒绝点文件名。

## 5.7.2 与 META 的相同/不同

**相同**（"相似接口"）：

- 64B header + 同款 SYM 字典编码（`(sym_count+1) × 8` 偏移表 + 字符串数据）；
- 区间记录**复用 12B `SymIndexRecord`**（`time_start(4) + time_count(4) + row_start(4)`），
  且 `time_start`/`time_count` 索引**父 `.meta` 的全局 TIME AXIS**、`row_start` 为
  **父全局行空间**（即父 SYM INDEX 的 `row = sym_row_start + (time_idx - time_start)`）。

**不同**（用户点名的差异）：

- META `sym_index`：每 SYM **一条**连续区间（一个 `time_start`+`time_count`）；
- SUBSET：每 SYM **可多条不连续区间**（股票进出指数多次，各自成段）。

## 5.7.3 二进制布局

```text
[Header 64B]
[SYM DICT INDEX: (sym_count + 1) × 8 bytes]   ← u64 offsets into STRING DATA
[SYM STRING DATA: variable]
[RANGE INDEX: sym_count × (u32 count + count × 12B SymIndexRecord)]
```

### Header（64B，magic `SPLAYSUB`，version 1）

| 偏移 | 大小 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 8 | `magic` | `SPLAYSUB`（LE u64） |
| 8 | 2 | `version` | 1 |
| 10 | 2 | `flags` | 0 |
| 12 | 1 | `time_type` | 必须与父 `.meta` 一致 |
| 13 | 3 | `reserved` | 0 |
| 16 | 8 | `generation` | 子集自身代数（重写时递增） |
| 24 | 8 | `parent_generation` | **父 `.meta` 的 generation**（失效检测） |
| 32 | 4 | `sym_count` | 子集内符号数 |
| 36 | 4 | `range_count` | 全符号区间总数 |
| 40 | 4 | `total_rows` | 子集覆盖行数 = Σ 区间 `time_count` |
| 44 | 20 | `reserved2` | 0 |

### RANGE INDEX（每符号）

```
[count: u32] 后跟 count × SymIndexRecord{time_start, time_count, row_start}
```

相邻/重叠段在建文件时已合并；每符号区间按 `time_start` 升序；符号按字典序。

## 5.7.4 写入与校验（`create_subset(dir, name, inputs)`）

输入：`inputs: Vec<SubsetInput{sym, segments: Vec<(time_value, count)>}>`，时间值为父
`time_type` 域（Date32=天数 / TimestampUs=µs）。对每个段：

1. `sym` 须在父 `.meta`（否则 `SymNotFound`）；
2. `time_value` 须在父 TIME AXIS（否则 `TimeNotFound`）；
3. 段须落在该 SYM 的 `[time_start, time_start + time_count)` 连续块内（否则
   `SegmentOutOfRange`）；
4. 解析 `row_start = sym.row_start + (time_idx - sym.time_start)`，喂给 `SubsetBuilder`
   （同 SYM 相邻/重叠段自动合并）；
5. 序列化后**原子落盘**（`.sub.{name}.tmp` → fsync → rename），header 记
   `parent_generation = 父 generation`。

## 5.7.5 读取（`SubsetReader`）

- `open(dir, name)`：仅解析文件；
- `open_with_parent(dir, name, &MetaFile)`：另校验 `parent_generation == 父
  generation`、`time_type` 一致——父被 `update_meta` 重排后 generation 递增 →
  **`StaleParent`**，须重建 `.sub.xxx`（重排会改变行布局，旧索引失效）；
- `symbols() / contains(sym) / ranges(sym) / total_rows()`；
- `iter_ranges()`：按**父全局行序**产出 `(sym_idx, &SymIndexRecord)`；
- `iter_entries(&MetaFile)`：按父全局行序产出 `(sym, time_value, global_row)`；
- `read_field_values(&FieldReader)`：把某字段在子集内的值按父全局行序拼接
  （每区间 `read_range_raw` 行段读取，NONE 零拷贝；压缩字段自动解压）——
  长度 = `total_rows × sizeof(type)`。

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

> ✅ **现状**：机制已实现——以 META 的 `generation` + `file_size` 为一致性锚点，Reader 拒绝 generation 不匹配的 FIELD（§6 屏障）；`update_meta` 的原子提交（`.meta.new`→fsync→rename）保证崩溃后重跑幂等；`compact_field` 用「写新文件再 rename」保证崩溃后原文件完好。⚠ 尚无专门的故障注入 / 崩溃恢复测试套件。

V1 需要覆盖：FIELD 原地更新未完成、META 原子替换失败等场景。以 META 的 `generation` + `file_size` 为一致性锚点，启动时校验 FIELD 的 generation 与数据长度，拒绝不匹配部分。

## 8.4 函数接口规范

以下七个函数是 Writer 层的权威 API 定义。Arrow 相关函数（`create_meta` / `create_table` / `update_table`）位于 `splayed-arrow` crate（仅做 Arrow→原生转换），同名的原生版本位于 `splayed-core` crate（见后文「原生表写接口」），供 DuckDB 原生 DataChunk 等交换层直写；`create_field` / `create_field_with_data` / `update_field` / `delete_field` / `compact_field` 位于 `splayed-core` / `splayed-codec`。

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

### create_field_with_data(field_path, data_type, values)

**所在 crate：** `splayed-core`

**输入：**

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `field_path` | path | 字段文件路径（与 `.meta` 同目录） |
| `data_type` | uint8 | 数据类型 ID（见 §5.3） |
| `values` | bytes | 原始小端字节，长度必须 = `total_rows × sizeof(type)` |

**处理：**

1. 读取同级目录 `.meta`（权威 `total_rows` / `generation`）
2. 校验 `values.len() == total_rows × sizeof(type)`，否则 `LengthMismatch` 报错
3. **一次写入**：`header(64B) + data`（单一写通，没有预分配全 NULL 再回填的两遍写）

**约束：** 值必须覆盖全部 `total_rows`；稀疏/部分填充走 `create_field` + `update_field`。

### create_field_with_data_encoded(field_path, data_type, values, encoding, compression)

**所在 crate：** `splayed-core`

**语义：** 与 `create_field_with_data` 同契约（读同级 `.meta` 取权威
`total_rows`/`generation`、`values` 须覆盖全部 `total_rows`），但数据区在**写入
时直接编码/压缩**（一步到位，无需先写 PLAIN 再 `compact_field`）。

**输入：** 在 `create_field_with_data` 基础上增加：

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `encoding` | uint8 | PLAIN / DELTA / RLE / BITPACK（§5.2 header） |
| `compression` | uint8 | NONE / ZSTD / LZ4（§5.2 header） |

**数据区布局（与 `compact_field_with_encoding` 完全一致）：**

- `PLAIN + NONE` → 原样原始数据（无前缀，mmap 零拷贝快路径；与
  `create_field_with_data` 输出**字节一致**）；
- 其它 → `[u64 编码后字节数][payload]`，payload = 编码结果，`compression !=
  NONE` 时再压缩该编码结果。

**写后只读：** `compression != NONE`（或编码非 PLAIN）的字段 `update_field`
拒绝写入。适合"一次写入、多次读取"的稀疏因子列（90%~99% NULL 直接写
`RLE + ZSTD`），header `null_count` 与统计 footer 均按原始值计算（永久有效）。

**字段文件名规则：** 与 `create_field_with_data` 相同——以 `.` 开头的字段名
拒绝（`HiddenFileName`）。

### FieldWriteOptions（表级新字段写选项）

**所在 crate：** `splayed-core`（`splayed-arrow` 重导出）

```text
FieldWriteOptions { encoding: Encoding, compression: Compression }
// Default = { Plain, None }（可写，mmap 零拷贝，与既有行为一致）
```

作用于 `create_table_with_options` / `update_table_with_options` 及分区写
`_with_options` 变体中**新创建**的字段（已存在字段不受影响，仍可写 PLAIN 原地
更新）。非默认选项时新字段直接以编码/压缩形式落盘（写后只读）。

### create_subset(dir, name, inputs) / SubsetReader

**所在 crate：** `splayed-core`（格式定义在 `splayed-format` §5.7）

创建 `.sub.{name}`：父 `.meta` 网格的**行区间子集索引**（每 SYM 可多条不连续
区间）。格式与 `.meta` 相似（64B header、同款 SYM 字典、12B `SymIndexRecord`），
差异仅在 `sym_index` 一条区间 → RANGE INDEX 每 SYM 多条。

```text
create_subset(dir, name, inputs: &[SubsetInput]) -> Result<(), SubsetError>
SubsetInput { sym: String, segments: Vec<(i64, u32)> }   // 时间值(父 time_type 域)+连续段
SubsetReader::open(dir, name) -> Result<Self, SubsetError>
SubsetReader::open_with_parent(dir, name, &MetaFile)      // + parent_generation/time_type 校验
  symbols() / contains(sym) / ranges(sym) / total_rows()
  iter_ranges()   -> (sym_idx, &SymIndexRecord)   // 父全局行序
  iter_entries(&MetaFile) -> (sym, time_value, global_row)
  read_field_values(&FieldReader) -> Vec<u8>      // 字段在子集内的值（父行序拼接）
```

父被 `update_meta` 重排后 generation 递增 → `open_with_parent` 报 `StaleParent`
（须重建 `.sub.xxx`）；`.sub.*` 以点开头，`list_fields` / `update_meta` 均忽略，
不会被当成 FIELD。

**下游消费（读侧，已实现）：**
- `splayed-arrow::read_subset(dir, sub_name, &[String]) -> RecordBatch`：物化
  `[time, sym, ...columns]`（父全局行序；`columns` 空 = 全部字段；`StaleParent` 检测）。
- `splayed-polars::splayed_lazyframe_subset(dir, sub_name) -> LazyFrame`：物化视图转惰性帧。
- `splayed-datafusion::SplayedSubsetFunction`：`read_splayed_subset('dir', 'hs300')` 表函数
  （`MemTable` 物化，`register_udtf`）。
- `splayed-python`（pyo3 扩展 `splayed`）：`create_subset(dir, name, [SubsetInput])` /
  `read_subset(dir, name) -> pyarrow.RecordBatch`（pyarrow 交换层，见 `docs/crates/python.md`）。

### 原生表写接口（`splayed-core`）——交换层直写，无 Arrow

上述 Arrow 函数（`create_meta` / `create_table` / `update_table`）只是转换器：
真正的引擎实现在 core 的原生接口（未来 DuckDB 原生 DataChunk 等交换层可直接调用，无需经 Arrow）：

```text
create_meta(dir, time_type, sym: Vec<String>, time: Vec<i64>) -> MetaFile
create_table(dir, time_type, sym, time, columns: Vec<TableColumn>, sorted: bool) -> MetaFile
create_table_with_options(dir, time_type, sym, time, columns, sorted, opts: FieldWriteOptions) -> MetaFile
update_table(dir, sym, time, columns: Vec<TableColumn>, create_missing_fields: bool) -> ()
update_table_with_options(dir, sym, time, columns, create_missing_fields, opts: FieldWriteOptions) -> ()
update_meta(dir, time_type, sym, time) -> MetaFile
TableColumn { name, data_type, values: Vec<u8> }   // 输入行序原始小端字节
```

- `create_table`：建 `.meta` 后，每个 FIELD 先在**全局行序**缓冲（缺失时间点保留 NULL 哨兵），再 `create_field_with_data` 一次写入；`create_table_with_options` 在 `opts` 非默认时改用 `create_field_with_data_encoded`（新字段直接编码/压缩、写后只读）；
- `sorted`（**性能提示**）：输入已按 (SYM, TIME) 升序时走快速路径——`MetaBuilder`
  跳过重复排序，FIELD 散列改为按符号分组 + TIME AXIS 窗口游标（每行均摊
  O(1)，免去每行的两次二分查找）。传入前会做 O(n) 顺序校验：若实际乱序
  自动回退普通路径，结果始终正确；
- `update_table`：仅更新 META 中已存在的 (SYM, TIME)，未知位置 / 缺失 FIELD / 类型不匹配均报错；`create_missing_fields=true` 时自动创建缺失 FIELD（新列历史全 NULL）；`update_table_with_options` 在 `opts` 非默认时，**新创建**的 FIELD 用 `create_field_with_data_encoded` 直接编码/压缩落盘（写后只读），已存在 FIELD 仍原地更新；
- 分区写 `_with_options` 变体：`create_partitioned_table_with_options` /
  `append_partition_with_options` / `update_partition_table_with_options` /
  `update_partition_meta_with_options` ——语义与同名非 `_with_options` 函数一致，
  仅将 `opts` 透传给新分区/新字段的写入（非默认时新字段直接编码/压缩、写后
  只读）；既有分区经 `update_partition_meta` 重写时仍为 PLAIN+NONE 可写；
- `update_meta`：**以新 (SYM, TIME) 布局重建 `.meta` 并把全部现有 FIELD
  并发重散布到新布局**（区别于 `update_table` 的「格子内更新」）：
  - 参数与 `create_meta` 一致（`sym`/`time` 为逐行数组）；
  - gather：新布局第 g 行 = (新 sym, 新 time)，旧数据中同 (SYM, TIME) 的值
    被搬运（旧 meta 二分定位，连续段一次 memcpy），旧数据没有的格子填
    NULL；FIELD 保持同名，重写为 PLAIN+NONE 并重算真实 `null_count` 与
    统计 footer；
  - 并发：`std::thread::scope` 每字段一线程（字段彼此独立）；
  - 原子性：先写 `.meta.new`（fsync 暂不改名）→ 各字段 `name.tmp` 写入+
    fsync+rename → 最后 rename `.meta.new`→`.meta`（提交点）。中间态由
    **generation 屏障**挡住（§6 不变量：generation 不匹配的 FIELD 被
    Reader 拒绝）；失败重跑本函数即完成提交（重散布确定性、幂等）；
  - `time_type` 允许变化（TIME 不落 FIELD）；
  - ⚠ Windows：被 mmap 的字段无法 rename——调用方须先 drop 所有打开的
    FieldReader / provider / 连接再调用。
- 目录须为空或不存在（仅 `create_table`）。

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
- `data` 中包含 META 中不存在的 FIELD 列：默认报错；`create_missing_fields=true`
  时自动创建该列（新列历史槽位全 NULL，本次输入值按 `(SYM, TIME) → global_row`
  一次写入），用于 schema 演进（晚到列）。

### 失败模式

| 场景 | 处理 |
| --- | --- |
| `update_field` 写入已压缩 FIELD | 拒绝，报错（只读） |
| `update_field` row 超出 `[0, total_rows)` | 拒绝，报错 |
| `update_table` 传入 `(SYM, TIME)` 不在 META 中 | 报错，不静默扩展 |
| `update_table` 传入 .meta 不存在的 FIELD 列 | 默认报错；`create_missing_fields=true` 时自动创建 |
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
| `filters` | Filter[] | FIELD 值过滤（**合取**语义，全部生效） |
| `batch_size` | usize | Batch 大小 |
| `parallelism` | usize | 并行度 |

示例：

```text
columns:     [close, volume]
symbols:     [AAPL, MSFT]
time_range:  [2026-01-01, 2026-03-01)
filters:     [close > 100, volume > 5000]
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

> ⚠ **现状**：core 层聚合**未实现**（仍为预留）。当前聚合在**上层**执行——DataFusion 经 `SplayedStatsAggRule` 优化规则对**无过滤**的 MIN/MAX/COUNT 直接命中列统计（footer min/max + null_count）返回单行常量，其余聚合由 DataFusion/Polars 的物理计划完成；`GROUP BY sym` 的 per-SYM local aggregate → merge 尚未下沉 core。

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

### 内存列式表示：CoreBatch（引擎无关核心层）

扫描输出由 **CoreBatch** 承载（`splayed-core::batch`，零 Arrow 依赖）：

```text
CoreBatch
+-- schema: Arc<CoreSchema>          [time, sym, fields...]
+-- columns: Vec<CoreColumn>
|   +-- Primitive { ty, data: Buffer }         定长数值/日期/时间戳（LE 连续字节）
|   +-- Dictionary { indices: u32, values }    SYM 列（共享字典，零拷贝）
|   +-- Varlen { offsets, data }               Utf8/Binary（内存支持，磁盘本轮不存）
|   +-- nulls: Option<Bitmap>                  validity（无 NULL 时 None，零分配）
+-- num_rows
```

- **NULL**：磁盘仍是哨兵位型；读时哨兵 → validity 位图（**每批次每列一次**
  O(n) 判空），数据缓冲原样保留；`FieldHeader.null_count == 0` 时跳过扫描
  （无位图、零开销）。写回时位图 → 哨兵。
- **TIME 列按类型产出**（Date32 直接 i32、TimestampUs 直接 i64），消除转换端收窄拷贝。
- **SYM 列字典化**（u32 索引 + 共享字典），取代逐行字符串展开。

### Splayed → Arrow（零拷贝）

`splayed-arrow::corebatch_into_record_batch(batch, projected, fields, sym_dict)`
利用 CoreBatch 原始缓冲**移交**给 Arrow（`Buffer::from_vec` 零拷贝）：

```text
CoreBatch buffer → Arrow Buffer（数据不复制）
Bitmap           → Arrow NullBuffer（无 NULL 列不产生）
Dictionary (sym) → 展开为 Utf8（小拷贝，schema 为 Utf8）
```

### NULL 语义保留

```text
canonical NaN    ->  Arrow validity bitmap = 0（NULL）
其他普通 NaN     ->  Arrow validity = 1, value = NaN
```

这样语义不会丢失。

## 10.2 Native ColumnView（核心接口）

> ⚠ **定位更新**：本节的 ColumnView 是**遗留路径**——当前引擎无关的主内存模型已由 **CoreBatch**（`splayed-core::batch`，见 §10.1）取代；`ColumnView` 仍在（供 `column_view_to_arrow` 与 scalar filter 使用），但「未来性能优化的关键接口」的角色已转移给 CoreBatch。

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

三层对接，每层职责单一：

```text
Layer 3   register             register_splayed_table / SplayedTableFactory
                              (CREATE EXTERNAL TABLE ... STORED AS SPLAYED) /
                              SplayedTableFunction (read_splayed('dir'))
Layer 2   SplayedTableProvider  分区表（对标 Hive 分区表）：自动探测分区、
                              schema 校验、TIME 分区裁剪
Layer 1   SplayedDatasetProvider 单个 dataset 目录（对标一个 parquet 文件）：
                              自带 schema / statistics / 严格谓词下推
```

- **Layer 1**：一个含 `.meta` 的目录 = 一个分区（自包含、可直接查询）。实现
  `schema`、`table_type`、`statistics`（精确行数/字节数/各 FIELD null_count、
  TIME min/max、SYM distinct）、`get_table_definition`、`supports_filters_pushdown`
  （严格 `Exact`）、`scan`。
- **Layer 2**：顶层目录 = 表，直接子目录（各含 `.meta`）= 分区。所有分区 schema
  必须一致（否则构造报错）；基于各分区 META 的 TIME AXIS `[min,max]` 做分区裁剪。
  自动探测：目录本身含 `.meta` 时退化为单分区表（向后兼容旧 API）。
- **Layer 3**：`register_splayed_table(ctx, name, dir)`（自动探测）、
  `SplayedTableFactory`（`STORED AS SPLAYED`）、`read_splayed('dir')` 表函数。

谓词下推（Layer 1/2 共用同一套解析，保证一致性 = 正确性）：

| 谓词 | 处理 | 标记 |
| --- | --- | --- |
| `sym = 'X'`（单个已知 SYM） | → `SymbolSelection` | `Exact` |
| `sym IN ('A','B',...)`（非取反） | 已知 SYM 并集 → `SymbolSelection`；字面量全未知 → 该分区 0 行 | `Exact` |
| `sym = 'X'` / `sym IN (...)`，同一合取中出现 ≥2 个 SYM 过滤表达式 | 选择只能表达并集，交集交由 DF 重滤 | `Inexact` |
| `sym = '<未知>'` | 该分区返回 0 行（不报错） | `Exact` |
| `time` 比较 | → `TimeRange`（半开区间，溢出用 saturating） | `Exact` |
| FIELD 值比较（可转换） | → `Filter`，**合取全部生效** | `Exact` |
| `field IS [NOT] NULL` | → `Filter::IsNull / IsNotNull`（NULL 哨兵匹配） | `Exact` |
| 恒等 `CAST(col AS 同型)` 包裹的列比较 | 剥除 cast 后按普通比较下推；非恒等 cast 不下推 | `Exact` / `Unsupported` |
| 其它（`IS NULL` 于非空列、非列比较、`NOT IN`） | 不下推，DF 自行过滤 | `Unsupported` |

执行计划：`SplayedScanExec`（单分区叶子，`UnknownPartitioning(1)`，有界 mpsc
通道 + `spawn_blocking` 流式产出，`LIMIT` 提前截断）与 `SplayedTableScanExec`
（`UnknownPartitioning(plans.len())`，每个物理分区 = 一个输出分区）。
没有命中分区时返回 `EmptyExec`（如 `COUNT(*)` → 0）。

### Layer 2 分区管理在 core（引擎无关，DataFusion / DuckDB 共用）

分区管理**下沉到 `splayed-core::partition`**（`SplayedTableProvider` 不再自带
`discover_partitions`）：

```rust
PartitionedTable::open(dir)              // dir/.meta 存在→单分区(".")；否则子目录含 .meta 者为分区（按名升序）
PartitionSchema { time_type, fields }    // 合并校验：字段名+类型一致、time_type 一致；符号并集
plan(&PartitionScanRequest)              // 三层剪裁：
  //  TIME：分区 meta time_axis 与半开区间 [start,end) 相交性
  //  符号：Symbols 选择剔除分区不存在的符号；一个都不存在→跳过分区
  //  统计：值 filter 与分区字段 footer [min,max] 不相交 → 跳过分区（整分区文件级跳过）
  //  分区列：key=value 目录名解析的声明式列（PartitionColumn，Int64/String）
  //          → 列上过滤条件与分区 declared 值不相交 → 跳过分区
scan(&plan, &req) -> PartitionScanBatches// 按分区名升序流式合并 CoreBatch
```

- 分区列：单层 `key=value/` 目录（如 `year=2024/.meta`）解析为**声明式
  虚拟列**（全值可解析 i64 → Int64，否则 String）；DataFusion 表 schema
  追加该列（`ProjectionExec` 常量列补回值），DuckDB 原生经
  `PartitionScanRequest.partition_filters` 剪裁；
- DataFusion：`SplayedTableProvider::scan` 用 core 分区计划的 tasks 选分区执行；
- DuckDB：`splayed_duckdb::native::scan_table_to_chunks` 走同一实现；
- Polars/其它引擎可直接调用 `PartitionedTable`（core 依赖即可）。

并行能力：

- **core**：`scan_all_parallel`（按 `ScanRequest.parallelism` 分块 + 线程）与
  **`scan_owned_parallel`**（有序**流式**并行：`split_ranges` 把 row ranges 按
  行数拆分/连续打包成 ≤n 组保序切片，每组一个 `std::thread` 生产者 + 有界
  通道，消费按组序取回 → 与串行扫描逐行等价）。`split_ranges` 对超大单
  SYM 区间先按目标行数切分，天然抗偏斜。
- **DataFusion**：`SplayedDatasetProvider::with_scan_parallelism(n)` /
  `SplayedTableProvider::with_scan_parallelism(n)`（默认 1）把单个 dataset 的
  扫描切成 n 个**行均衡、保序**的 output partition，由 DataFusion 多线程
  runtime 并行执行；`SplayedTableScanExec` 展平各子计划的输出分区数。
  LIMIT 安全性：DF 对多分区扫描保留 `GlobalLimitExec`（`partition_count()!=1`
  时 `limit_satisfied_by_input` 恒 false），每分区提前截断只是优化。
- **DuckDB 接入点**：`ScanRequest.parallelism` 直接对表 DuckDB 线程数；
  native 直写无需改 core（见 §8.4 原生表写接口）。

有序性：单个 dataset 的 Scanner 按 `(SYM, TIME)` 升序产出（META 的 symbols 与
time_axis 均升序，range 依 SYM INDEX 顺序生成）。因此 Layer 1 在
`SymbolSelection::All`（无 SYM 过滤，避免过滤字面量顺序打乱）且投影保留
`sym`/`time` 两列时，通过 `EquivalenceProperties` 声明 `(sym, time)` 有序，
使 `ORDER BY sym, time`（如 TOP-N）可免排序；SYM 过滤与非全序多分区情形不声明。

重点支持：Projection Pushdown、Predicate Pushdown、SYM pruning、TIME pruning、
Batch scan、LIMIT。

未来（暂缓）：`INSERT INTO` 写回（`update_table`）。

## 10.3b 分层组件架构（非并列）

组件按**层次**组织，引擎无关的核心在最底，ADBC 驱动在最上：

```text
外部应用
  │
  ▼
splayed-adbc          上层 ADBC 驱动：内部经 DataFusion 执行 SQL → Arrow 结果
  │（调用执行 SQL；DuckDB 可作平替后端）
  ├▶ splayed-datafusion   DataFusion TableProvider（查询自定义格式）+ SQL 执行
  └▶ splayed-duckdb       DuckDB 扩展 / Arrow IPC 桥
  │
splayed-core           引擎无关核心：数据扫描与写入（Scanner / CoreBatch / Filter）
  ▲
  │ 共享转换工具（可选用）
splayed-arrow          CoreBatch → Arrow 零拷贝转换
```

- **`splayed-core`**：引擎无关，提供数据扫描与写入；不依赖 Arrow。
- **`splayed-datafusion`**：实现 DataFusion `TableProvider`，使 DataFusion 能
  查询自定义格式；同时提供 SQL 执行能力（`SessionContext`）。
- **`splayed-duckdb`**：实现 DuckDB 扩展能力（当前为 Arrow IPC 桥）。
- **`splayed-adbc`**：**上层**接口，面向外部应用；**内部使用 DataFusion 执行
  SQL**、返回 Arrow `RecordBatch` 流——不是与 DataFusion/DuckDB 并列的适配层，
  也**不复实现 SQL 解析**（ADBC C ABI / `adbc.h` FFI 可在
  `Connection`/`Statement` 上加薄层）。
- **`splayed-arrow`**：**可选的共享转换工具库**，位于核心层之上，被需要
  Arrow 的适配层（DataFusion、ADBC）复用；解决重复转换代码问题，同时保持
  核心层独立。仅一个 Arrow 消费者时也可把转换逻辑直接放进对应适配层。

umbrella crate `splayed` 用 features 开关（`arrow` 默认开、可关；`adbc` 隐式
拉入 datafusion）。

依赖矩阵：

| 组件 | 依赖 | 说明 |
| --- | --- | --- |
| splayed-core | format + codec | 引擎无关核心 |
| splayed-arrow | core | 共享转换工具（可选） |
| splayed-datafusion | core + arrow + datafusion | TableProvider + SQL |
| splayed-duckdb | core + arrow + arrow-ipc | DuckDB 桥 |
| splayed-adbc | datafusion + arrow | 上层 ADBC 驱动（无独立 SQL 解析） |

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

两条路径（`splayed-duckdb`）：

```text
Phase 1（feature "arrow"，默认开）：
  Splayed -> CoreBatch -> Arrow(零拷贝) -> IPC 文件 -> DuckDB read_arrow
  （外部文件交换；代价 = IPC 序列化 + DuckDB 重解析一段拷贝）

Phase 2（原生 DataChunk，no arrow）：
  Splayed -> CoreBatch -> scan_to_chunks() / C ABI -> DuckDB Extension 逐批消费
```

### Phase 2 工程结构（Rust 集成层 + C++ 壳）

```text
splayed-duckdb（Rust crate，rlib；可按需 cdylib/staticlib）
  └─ src/ffi.rs   C ABI（零 Arrow）：
       splayed_dataset_open/close · schema_count/schema_field
       splayed_scan_open/next/dict_value/close · last_error
       句柄=裸指针（谁创建谁释放）；列视图借用当前批（下次调用前有效）
splayed-duckdb-extension（C++ 工程，非 cargo 成员）
  └─ extension.cpp 扩展壳：Load/注册 read_splayed(dir) 表函数，
     消费 C ABI 列视图填充 DataChunk（定长列 memcpy、validity→NullMask、
     SYM 字典逐行取串）
```

- 内存管理：句柄即 `Box::into_raw` 指针，每对象有 `*_close`；错误=返回码 +
  `splayed_last_error()`（线程本地，Rust 持有）。
- 列布局：`[0]=time`（原生 DATE/TIMESTAMP 类型）、`[1]=sym`（字典列
  type_id=200）、`[2..]=FIELD`（splayed `DataType` id 0..=13）。
- 数据合同：定长列缓冲与 DuckDB `Vector` 布局同构——C API 路径 memcpy 进
  向量缓冲；扩展工程若改用 C++ `Vector(LogicalType, data_ptr)` 构造可零拷贝。
- 演进顺序（按实施建议）：先扫描（已做）→ 分区谓词转换 → 写入 →
  ADBC/DuckDB 平替后端。
  ⚠ **现状**：扫描已完成；**分区谓词转换**在 native 侧经
  `PartitionScanRequest.partition_filters` 已部分支持，但 C++ 扩展壳
  （`splayed-duckdb-extension`）的谓词下推、**写入、物化视图**仍为骨架/未来项；
  **ADBC/DuckDB 平替后端**未做（当前 ADBC 走 DataFusion 执行 SQL）。

构建产物（`--config "lib.crate-type=['cdylib','staticlib']"`）：
`target/release/{splayed_duckdb.dll, splayed_duckdb.lib, splayed_duckdb.dll.lib}`。

## 10.5 Polars 集成（splayed-polars）

通过 Polars 官方 **`AnonymousScan`**（`LazyFrame::anonymous_scan`）实现惰性
扫描源 `splayed_lazyframe(dir)`，深度优化：

- **谓词下推**：`allows_predicate_pushdown` 开启——pushdown 的过滤条件经
  `predicate` 模块翻译到核心层（`sym` 等值 → `SymbolSelection`、`time` →
  `TimeRange`、FIELD 值比较 → `Filter`，AND 链合并），减少读取与转换；
  未翻译部分（复杂/表达式比较）在扫描结果上交给 polars 物理评估兜底
  （`df.lazy().filter(pred).collect()`），结果恒正确。
- **列裁剪**：`allows_projection_pushdown` 开启——只扫描 `with_columns`
  涉及的列（投影下推）。
- **转换**：arrow-rs → polars 走 **Arrow C data interface**（`FFI_ArrowArray`
  与 polars `ArrowArray` 布局逐字段一致，所有权移交）；时间列按物理类型
  （Date=Int32、Datetime=Int64）导入，字符串列按值构造（polars 0.45 的
  newest-compat 将 String 映射为 Utf8View，标准 C data 无法直接灌入）。
- 已知约束（polars 0.45）：无显式 `select` 的完整收集会触发其 anonymous-scan
  投影优化中的 `reader_schema=None` unwrap bug——pe 链上建议显式
  `select([...])`（等价语义）。

依赖：`polars = "=0.45.1"`（features: lazy/fmt/dtype-*），`polars-arrow = "=0.45.1"`。

---

# 11. Rust Crate 结构

```text
splayed/
|
+-- splayed-format/          # 无依赖：格式定义
|   +-- meta.rs, field.rs, header.rs, types.rs   # DataType 0–13（定长 + 哨兵 NULL）
|
+-- splayed-codec/           # format ← codec：编码/压缩
|   +-- plain.rs, delta.rs, rle.rs, compression.rs
|
+-- splayed-core/            # format + codec ← core：不含 Arrow
|   +-- dataset.rs, reader.rs, field_writer.rs, scanner.rs, simd_filter.rs
|   +-- batch.rs             # CoreBatch：引擎无关内存列式（Buffer/Validity/字典列/CoreType）
|   +-- table_writer.rs      # 原生 create_meta/create_table/update_table（TableColumn）
|   +-- 并行流式：split_ranges / scan_owned_parallel（有序多线程）
|
+-- splayed-arrow/           # core + arrow ← arrow：共享转换工具（可选用）
|   +-- arrow_conv.rs        # 类型映射 / ColumnView→Arrow（遗留）
|   +-- corebatch_to_arrow.rs# CoreBatch→Arrow 零拷贝（corebatch_into_record_batch）
|   +-- meta_writer.rs / table_writer.rs   # create_meta / create_table / update_table
|
+-- splayed-datafusion/      # core + arrow + datafusion：TableProvider 三层对接 + SQL
|   +-- dataset.rs           # Layer1：一个 .meta 目录 = 一个分区
|   +-- table.rs             # Layer2：分区表（自动探测 / 分区裁剪 / with_scan_parallelism）
|   +-- register.rs          # Layer3：register_splayed_table / STORED AS SPLAYED / read_splayed
|   +-- filter.rs / convert.rs / exec.rs   # 谓词下推 / CoreBatch→RecordBatch / 执行计划
|
+-- splayed-duckdb/          # core（+ 可选 arrow）：DuckDB 集成层
|   +-- native.rs            # scan_to_chunks（原生 DataChunk 路由，零 Arrow）
|   +-- ffi.rs               # C ABI（cdylib/staticlib 按需生成）：句柄/schema/扫描/列视图/错误
|   +-- arrow_bridge.rs      # feature "arrow"：Splayed→Arrow IPC 导出
|
+-- splayed-polars/          # core + arrow + polars 0.45：惰性扫描
|   +-- anonymous.rs         # AnonymousScan（谓词下推 + 列裁剪 + 兜底过滤）
|   +-- predicate.rs         # polars Expr → 核心层剪裁翻译（尽力）
|   +-- arrowconv.rs         # arrow-rs → polars（C data interface，时间列物理化）
|
+-- splayed-adbc/            # datafusion + arrow：上层 ADBC 驱动（内部 DataFusion 执行 SQL）
|
+-- splayed/                 # umbrella：核心恒有；arrow/adbc/datafusion/duckdb/polars features
|
+-- splayed-cli/             # CLI：init, update, compact, read, sql, export, export-arrow
splayed-duckdb-extension/    # C++ DuckDB 扩展壳（非 cargo 成员，加载 splayed-duckdb C ABI）
example/                     # Python demo scripts
```

依赖方向：`format`（无依赖）← `codec` ← `core` → `arrow`（共享转换工具，
被 need Arrow 的适配层复用）→ `{datafusion, duckdb, polars}` →（上层）
`adbc`。

> **分层原则**：`splayed-core` 不依赖 Arrow，提供引擎无关的扫描/写入与
> `CoreBatch`；`splayed-arrow` 是**可选共享转换工具**；各引擎绑定（DataFusion
> / DuckDB / Polars）与上层 ADBC 驱动各自成 crate（umbrella features 开关）。

---

# 12. 开发阶段（Phase 1–9，均已 ✅ Done）

> 后续阶段（Phase 10–20：CoreBatch、类型扩展、并行、适配层组件化、统计 footer、update_meta、分区管理/分区列/分区写、stats-agg 规则等）已全部完成，完整列表见 **`docs/development/phases.md`**。

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

三层对接（见 §10.3）：`SplayedDatasetProvider`（单目录=一个分区，对标一个
parquet 文件）→ `SplayedTableProvider`（分区表，对标 Hive 分区表，含自动
探测/schema 校验/TIME 分区裁剪）→ `register`（`register_splayed_table` /
`SplayedTableFactory` / `read_splayed`）。支持 Projection/Predicate Pushdown、
SYM/TIME pruning、合取值过滤、LIMIT、统计信息、`get_table_definition`。

## Phase 9：DuckDB ✅ Done (Arrow IPC bridge)

先 DuckDB → Arrow → Splayed 与 Splayed → Arrow → DuckDB，再优化为 Native ColumnView。

---

# 13. 性能基准（从第一天开始）

> ⚠ **未实现（目标）**：仓库暂无任何 bench 基础设施（无 `benches/`，Cargo.lock 无 criterion/iai/divan）。下表与「vs Parquet 对比矩阵」均为**规划目标**，尚未落地；实现时可接入 criterion 并在 CI 中跑 benchmark job。

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