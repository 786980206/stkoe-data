# splayed-format（磁盘格式）

**依赖**：无（仅 `bytemuck` 用于 header 结构零拷贝 reinterpret）。

## 接口

| 接口 | 说明 |
|---|---|
| `DataType`（ID 0–13） | BOOL/INT32/64/FLOAT32/64/DATE32/TIMESTAMP_US + 扩展 INT8/16、UINT8/16/32/64、DATE64；`size_of/null_bytes/as_str/from_id` |
| `RawValue` | 定长值：`read_le/write_le`、全类型构造/访问器、`null_bytes_slice` |
| `fill_null` | 按类型填 NULL 哨兵 |
| `MetaFile / MetaBuilder / SymIndexRecord` | meta 序列化/反序列化；builder 按 (sym,time) 建 TIME AXIS/SYM DICT/SYM INDEX；`total_rows` |
| `SubsetFile / SubsetBuilder / SubsetHeader` + `SUBSET_MAGIC/SUBSET_PREFIX` | `.sub.xxx` 子集索引：64B header（含 `parent_generation`）、同款 SYM 字典、每 SYM **多条** 12B `SymIndexRecord`（区间复用同一类型） |
| `FieldHeader / MetaHeader / TimeType / Compression / Encoding` + 常量 | 64B 头结构、magic、`HEADER_SIZE/DATA_OFFSET` |
| `field::row_byte_offset/data_length/field_file_size/validate_field_header/new_plain_field_header` | 偏移与长度计算、头部校验 |
| `field_footer::{FOOTER_SIZE, compute_stats, encode_footer, parse_footer}` | 统计 footer：min/max 槽（跳过 NULL 哨兵）、编码/解析 |

## 关键格式事实

- META 用 `.meta.new → fsync → rename` 原子提交。
- FIELD `data_length` = 纯数据字节。
- 压缩布局 `[64B head][u64 len][payload][28B footer]`。
- NULL = 哨兵位型（浮点 canonical NaN、有符号 INT_MIN、无符号全 1、BOOL `0x02`）。

详见 [磁盘格式规范](../format/index.md)。
