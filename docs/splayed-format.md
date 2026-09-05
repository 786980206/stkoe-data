# splayed-format：数据格式定义

splayed 磁盘数据格式定义。只定义物理布局与二进制语义；API 见 [splayed-core](splayed-core.md)、[splayed-table](splayed-table.md) 与 [splayed-codec](splayed-codec.md)。

## 1. 文件类型

| 文件 | magic | 职责 |
| --- | --- | --- |
| `.meta` | `SPLAYMTA` | 元数据 + Index：SYM / TIME → FIELD row range；只读 |
| `field` | `SPLAYFLD` | 单字段数据文件：values + validity |
| `.sub.xxx` | `SPLAYSUB` | 子数据 / 辅助数据（API 后续单独定义） |

## 2. 数据类型（DataType）

| ID | 类型 | 单值大小 |
| --: | --- | ---: |
| 0 | `BOOL` | 1 B |
| 1 | `INT32` | 4 B |
| 2 | `INT64` | 8 B |
| 3 | `FLOAT32` | 4 B |
| 4 | `FLOAT64` | 8 B |
| 5 | `DATE32` | 4 B |
| 6 | `TIMESTAMP_US` | 8 B |
| 7 | `INT8` | 1 B |
| 8 | `INT16` | 2 B |
| 9 | `UINT8` | 1 B |
| 10 | `UINT16` | 2 B |
| 11 | `UINT32` | 4 B |
| 12 | `UINT64` | 8 B |
| 13 | `DATE64` | 8 B |

TIME 类型为 `DATE32` / `TIMESTAMP_US`；META header 的 `time_type` 声明其一。

## 3. 内存数据表示

core / codec / 适配层共用的零依赖内存数据模型。

```
Buffer        { ptr, size, alignment, ownership }    // Owned / Borrowed / Mmap，负责底层内存与生命周期
BufferView    { ptr, size }                          // non-owning；可经 offset + size 切片构造，不复制
BitmapView    { data: BufferView, offset, length }   // 1 bit ↔ 1 行，LSB-first
ColumnSegment { values: BufferView, validity: Option<BitmapView> }
ColumnView    { data_type, segments: Vec<ColumnSegment>, length }
FieldSchema   { name, data_type }
Schema        { fields: FieldSchema[] }
DataView      { schema, columns: ColumnView[], length }
```

- BufferView / BitmapView / ColumnSegment / ColumnView / DataView 均为 non-owning；生命周期不能超过底层 Buffer。
- Bitmap / BitmapView 提供字节/word 批量位操作原语（`copy_bits_into` / `Bitmap::copy_bits_from` / `set_range` /
  `bitmap_count_ones` / `bitmap_fill_bits`：头尾掩码 RMW、中间 word popcount、同相位整字节 memcpy）；
  validity 写路径按字节批量操作，不逐 bit；`copy_bits` 返回覆盖前后 1 位数，供 null_count 增量维护。
- ColumnSegment：一段连续行区间；段内连续是硬约束（SIMD 逐段求值的前提）；`validity = null` 表示该段全部有效。
- ColumnView：单列视图，由一个或多个 segment 按逻辑行序组成。不变式：
  - `segments` 非空、按逻辑行序排列；
  - `Σ segments 行数 == length`；
  - `PLAIN + NONE` 路径恒为单段（mmap 切片）；多段仅出现在 compressed 跨 chunk 读取与跨 Partition batch 聚合。
  - 空视图由 `ColumnView::empty(data_type)` 构造（单零行段，`length = 0`），同样满足非空段不变式。
- Schema：纯逻辑描述，不携带 encoding / compression / offset 等物理属性；不独立持久化（无 Schema 文件）。
- DataView：read-only 优先；可只含部分字段（projection）；所有列统一 `length`；不同列可来自不同 Buffer；列内多段对 DataView 透明。
- `Data` 为 owning / materialized 表示；仅需要独立拥有数据时才发生 `DataView → Data` 复制。

与 Arrow 的转化：segment 与 Arrow Array 一一对应、零拷贝（values / validity 直接包装底层 Buffer，LSB-first 位序一致）；Arrow Array 要求单张连续 values buffer，多段列转单个 Array 需显式 rechunk（一次拷贝）。chunk 边界天然是分 batch 边界。

## 4. NULL 语义

- NULL 由 validity bitmap 表示，与 DataType 无关。
- 每个 FIELD 携带可选 validity bitmap：1 bit 对应 1 行，`1 = 有效，0 = NULL`，位序 LSB-first（bit i ↔ row i）。
- 全有效列没有 validity 区（header `flags.has_validity = 0`）；内存表示中 segment 的 `validity = null` 等价于该段全部有效。
- NULL 单元的 value 位未定义，读侧忽略。
- 写入总是 values + validity 成对提交。

## 5. Encoding

| ID | 编码 | 说明 |
| --: | --- | --- |
| 0 | `PLAIN` | 原始固定宽度数组（默认） |
| 1 | `DELTA` | Delta 编码（时间 / 整数） |
| 2 | `RLE` | Run Length Encoding（重复值） |
| 3 | `BITPACK` | 位打包（小整数） |

## 6. Compression

| ID | 压缩 | 特点 |
| --: | --- | --- |
| 0 | `NONE` | mmap / O(1) 随机访问 |
| 1 | `ZSTD` | 高压缩率 |
| 2 | `LZ4` | 高解压速度 |

- `PLAIN + NONE` 下定位公式：`offset = 64 + row × sizeof(type)`。
- compressed FIELD 的 DATA / VALIDITY 整体作为 payload 参与编码压缩，header 恒为明文；分块与块头布局由 splayed-codec 定义。
- compressed FIELD 可通过 write mode Handle 修改：内部解压为 working representation，close 时如有修改自动重压缩写回。

## 7. META 格式（`.meta`）

### Header（固定 64 字节）

| Offset | 长度 | 字段 | 类型 | 说明 |
| ---: | ---: | --- | --- | --- |
| 0 | 8 | `magic` | uint64 | `SPLAYMTA` |
| 8 | 2 | `version` | uint16 | META 格式版本 |
| 10 | 2 | `flags` | uint16 | 预留 |
| 12 | 1 | `time_type` | uint8 | 0 = `DATE32`，1 = `TIMESTAMP_US` |
| 13 | 3 | reserved | bytes | 预留 |
| 16 | 8 | `generation` | uint64 | 当前 META 数据版本，用于校验 FIELD 是否匹配 |
| 24 | 4 | `time_count` | uint32 | TIME AXIS 元素数量 |
| 28 | 4 | `sym_count` | uint32 | SYM 数量 |
| 32 | 4 | `row_count` | uint32 | 逻辑总行数 = `Σ time_count` = 每个 FIELD 的 `row_count` |
| 36 | 4 | reserved | uint32 | 预留 |
| 40 | 8 | `sym_dict_offset` | uint64 | SYM Dictionary 起始位置 |
| 48 | 8 | `sym_index_offset` | uint64 | SYM INDEX 起始位置 |
| 56 | 8 | `file_size` | uint64 | META 文件总大小 |
| 64 |  | HEADER END |  | 固定 64 字节 |

### 数据区

| 区段 | 布局 | 说明 |
| --- | --- | --- |
| TIME AXIS | `time_count × (4 或 8)` bytes | `DATE32` = 4B，`TIMESTAMP_US` = 8B；全局去重按序 |
| SYM DICT INDEX | `(sym_count + 1) × 8` bytes | 每个 SYM 字符串在 STRING DATA 中的 offset |
| SYM STRING DATA | variable | 所有 SYM 字符串连续存储 |
| SYM INDEX | `sym_count × 12` bytes | 每个 SYM 一个固定 12B record |

偏移语义：`sym_dict_offset` 指向 SYM Dictionary（DICT INDEX + STRING DATA）起始；`sym_index_offset` 指向 SYM INDEX 数组起始；TIME AXIS 固定起始于 offset 64；`file_size` 用于完整性 / 截断校验。`row_count` 恒等于 `Σ time_count`，打开时校验；有了它，逻辑总行数从 header 直接读取，无需遍历 SYM INDEX。

### SYM INDEX record（固定 12 字节）

| Offset | 长度 | 字段 | 类型 | 说明 |
| ---: | ---: | --- | --- | --- |
| 0 | 4 | `time_start` | uint32 | 该 SYM 在全局 TIME AXIS 中的起始下标 |
| 4 | 4 | `time_count` | uint32 | 该 SYM 的时间点数量 = 行容量（全量预声明） |
| 8 | 4 | `row_start` | uint32 | 该 SYM 在 FIELD 中的起始 row = `sum(前序所有 SYM 的 time_count)` |

`time_count` 双重语义：该 SYM 在全局 TIME AXIS 中的连续区间长度，同时是该 SYM 在 FIELD 中预分配的行容量。全局行定位：

```text
global_row = row_start + (time_index - time_start)
```

区间内缺失的时间点是有效的逻辑行位，在 FIELD 中以 NULL（validity = 0）表示。

**连续子区间原则**：每个 SYM 的 time 序列必须严格等于 TIME AXIS 的一个连续子区间
（`[time_start, time_start + time_count)`）。因此 SYM INDEX 仅凭 `row_start + time_start + time_count`
三个字段即可完整恢复该 SYM 的行空间与时间定位，不保存任何 symbol 内的 time 信息；META 构建时校验
`time_count == 该 SYM 数据行数`，不满足即拒绝构建（保证容量网格与数据行严格对齐）。

### META 不变性

META 创建后 layout 固定、进入只读状态；不提供 update / compress / decompress。

## 8. FIELD 格式

### 物理布局

下图为 `PLAIN + NONE` 表示的布局；compressed FIELD 的 DATA 区为 chunk 序列，见 [splayed-codec](splayed-codec.md) §3。

```text
+------------------------------+
| HEADER             64 bytes  |
+------------------------------+
| DATA                         |
| value[0..row_count]          |
+------------------------------+
| VALIDITY（可选）              |
| ceil(row_count / 8) bytes    |
+------------------------------+
```

### Header（固定 64 字节）

| Offset | 长度 | 字段 | 类型 | 说明 |
| ---: | ---: | --- | --- | --- |
| 0 | 8 | `magic` | uint64 | `SPLAYFLD` |
| 8 | 2 | `version` | uint16 | FIELD 格式版本 |
| 10 | 2 | `flags` | uint16 | bit0 = `has_validity`；其余预留 |
| 12 | 1 | `data_type` | uint8 | DataType ID |
| 13 | 1 | `encoding` | uint8 | Encoding ID |
| 14 | 1 | `compression` | uint8 | Compression ID |
| 15 | 1 | reserved | uint8 | 预留 |
| 16 | 8 | `generation` | uint64 | 与 META 的 generation 校验 |
| 24 | 4 | `row_count` | uint32 | 逻辑行数 |
| 28 | 4 | `null_count` | uint32 | NULL 数；无 validity 区时为 0 |
| 32 | 4 | reserved | uint32 | 预留 |
| 36 | 4 | reserved | uint32 | 预留 |
| 40 | 8 | `data_length` | uint64 | DATA 区字节长度 |
| 48 | 8 | `validity_offset` | uint64 | VALIDITY 区起始；0 表示无 validity 区 |
| 56 | 8 | reserved | uint64 | 预留 |
| 64 |  | HEADER END |  | 固定 64 字节 |

### 规则

- `data_offset = 64`。
- `row_count` 在创建时确定，写路径不改变。
- VALIDITY 区大小 = `ceil(row_count / 8)`，`has_validity = 0` 时不存在。
- `null_count` 为真实 NULL 计数，创建（带数据）与每次成功写入后维护。

## 9. Generation 与原子提交

- `generation`：uint64，严格单调递增。
- `META.generation` 是 Dataset 的数据版本；`FIELD.generation` 必须与之匹配，打开 / 首次访问时校验，不一致按过期或损坏处理。
- `write_field_handle` / `update_field_handle` 成功后递增 `FIELD.generation`。
- 文件级替换（META 重建、cast 产物切换）：写临时文件 → fsync → 原子 rename。
- FIELD data / validity 原地写以 generation 屏障 + 读侧校验保证一致性。
