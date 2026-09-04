# META 格式

META（`.meta`）是**唯一定位索引**：承载 SYM/TIME → row range 映射。生成由 `MetaBuilder` 完成；创建后 META 布局不可变（布局演进走 `update_meta` 重建）。

## Header（固定 64 字节）

| Offset | 长度 | 字段 | 类型 | 说明 |
| ---: | ---: | --- | --- | --- |
| 0 | 8 | `magic` | uint64 | `SPLAYMTA` |
| 8 | 2 | `version` | uint16 | META 格式版本 |
| 10 | 2 | `flags` | uint16 | 预留 |
| 12 | 1 | `time_type` | uint8 | 0 = DATE32，1 = TIMESTAMP_US |
| 13 | 3 | reserved | bytes | 预留 |
| 16 | 8 | `generation` | uint64 | 当前 META 数据版本，用于校验 FIELD 是否匹配 |
| 24 | 4 | `time_count` | uint32 | TIME AXIS 元素数量 |
| 28 | 4 | `sym_count` | uint32 | SYM 数量 |
| 32 | 8 | `sym_dict_offset` | uint64 | SYM Dictionary 起始位置 |
| 40 | 8 | `sym_index_offset` | uint64 | SYM INDEX 起始位置 |
| 48 | 8 | `file_size` | uint64 | META 文件总大小（完整性/截断校验） |
| 64 |  | HEADER END |  | 固定 64 字节 |

## 数据区

| 区段 | 布局 | 说明 |
| --- | --- | --- |
| TIME AXIS | `time_count × (4 或 8)` bytes | DATE32 = 4B，TIMESTAMP_US = 8B；全局去重按序 |
| SYM DICT INDEX | `(sym_count + 1) × 8` bytes | 每个 SYM 字符串在 STRING DATA 中的 offset |
| SYM STRING DATA | variable | 所有 SYM 字符串连续存储 |
| SYM INDEX | `sym_count × 12` bytes | 每个 SYM 一个固定 12B record |

**TIME AXIS 为全局一条**：所有 SYM 共享同一份「去重后按序排列」的时间轴；每个 SYM 通过 `time_start` / `time_count` 指向该轴中的一段，使 TIME → row range 的映射成为一次 O(1) 下标运算。

## SYM INDEX record（固定 12 字节）

| Offset | 长度 | 字段 | 类型 | 说明 |
| ---: | ---: | --- | --- | --- |
| 0 | 4 | `time_start` | uint32 | 该 SYM 在全局 TIME AXIS 中的起始下标 |
| 4 | 4 | `time_count` | uint32 | 该 SYM 的时间点数量 = **行容量**（全量预声明） |
| 8 | 4 | `row_start` | uint32 | 该 SYM 在 FIELD 文件中的起始 row = **累计 sum(前序所有 SYM 的 time_count)** |

> 在全量预声明模型下，`time_count` 同时是「该 SYM 在全局 TIME AXIS 中占据的连续区间长度」和「该 SYM 在 FIELD 中预分配的行容量」，二者恒等，**无需单独 `row_capacity` 字段**。区间 `[time_start, time_start + time_count)` 内缺失的时间点在 FIELD 中以 NULL 特殊值填充。

## 核心映射示例

| SYM | 时间起点 | 时间数量 | 数据起点 |
| --- | ---: | ---: | ---: |
| SYM01 | 0 | 250 | 0 |
| SYM02 | 0 | 250 | 250 |
| SYM03 | 2 | 248 | 500 |
| SYM04 | 0 | 250 | 748 |
