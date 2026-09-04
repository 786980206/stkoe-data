# FIELD 格式

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
| [STATS FOOTER 28 bytes]      |   # 可选，见下文
+------------------------------+
```

**FIELD 不保存：** SYM、TIME、STRING、FIELD Index、Block Index、NULL bitmap。

## Header（固定 64 字节）

| Offset | 长度 | 字段 | 类型 | 说明 |
| ---: | ---: | --- | --- | --- |
| 0 | 8 | `magic` | uint64 | `SPLAYFLD` |
| 8 | 2 | `version` | uint16 | FIELD 格式版本 |
| 10 | 2 | `flags` | uint16 | 属性 |
| 12 | 1 | `data_type` | uint8 | 数据类型 |
| 13 | 1 | `encoding` | uint8 | 编码（PLAIN/DELTA/RLE/BITPACK） |
| 14 | 1 | `compression` | uint8 | 压缩（NONE/ZSTD/LZ4） |
| 15 | 1 | reserved | uint8 | 预留 |
| 16 | 8 | `generation` | uint64 | 与 META 的 generation 校验 |
| 24 | 4 | `row_count` | uint32 | 数据总行数 |
| 28 | 4 | `null_count` | uint32 | NULL 数量（真实值，精确增量维护） |
| 32 | 4 | reserved | uint32 | 预留 |
| 36 | 4 | reserved | uint32 | 预留 |
| 40 | 8 | `data_length` | uint64 | DATA 区字节长度（纯数据） |
| 48 | 16 | reserved | bytes | 未来扩展 |
| 64 |  | HEADER END |  | 固定 64 字节 |

`data_offset = 64`。

## 预分配与原地更新模型

| 阶段 | 函数 | FIELD 状态 |
| --- | --- | --- |
| 创建 | `create_field` | 按 `total_rows = sum(time_count)` 预分配，全部填 NULL 特殊值 |
| 填值 | `update_field` | 按绝对 row 偏移原地写入实际值，覆盖 NULL |
| 压缩 | `compact_field` | 压缩为只读，退出原地更新路径 |

PLAIN + NONE 下的写入公式：

```text
offset = 64 + start_row × sizeof(type)
```

**约束：**

- `update_field` 不改变 `row_count`、不扩展文件长度；写入超出 `[0, total_rows)` 报错。
- `compact_field` 后 FIELD 只读，`update_field` 拒绝。
- `generation` 在 `update_field` 成功后递增。

## 统计 Footer（min/max，可选）

定长 **28 字节**，追加在数据区之后（`splayed_format::field_footer`）：

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

- **min/max 计算跳过 NULL 哨兵**（浮点 canonical NaN、INT_MIN、无符号全 1）；全 NULL 列 flags=0（或无 footer）。`FieldReader::stats() -> Option<FieldStats>`。
- **写入时机**：`create_field_with_data`（含 `create_table`）写值后追加并写**真实 `null_count`**；`compact_field` 按原始数据**重算**（compacted 只读，统计永久有效）。
- **`update_field` 增量维护（O(k)，不失效）**：写前快照被覆盖行段旧值 → `null_count` 精确增量（header，写回）；min/max = `min(旧 min, 写入批次 min)` / `max(旧 max, 写入批次 max)`（**保守上界**，永远安全）；仅当被覆盖行**命中旧极值**才全列重扫精确重算（罕见路径）。无有效 footer（从未建立/已失效）时保持无统计（归档时重算）。
- **读侧**：`field_length` 仍指纯数据字节；读取按末尾 magic 检测 footer，数据区按 `[header, data)` 界定（压缩文件同理，payload 排除 footer）。
- **消费**：`Scanner::plan` 用 filter 列的 min/max 做**整数据集剪裁**；DataFusion `statistics()` 对 FIELD 列上报 min/max（`Precision::Exact`）。

## 压缩 / 编码布局

```text
压缩:   [64B head][u64 len][payload][28B footer]   # footer 统计保留
编码:   [64B head][u64 编码长][payload][28B footer] # 读侧按 header.encoding 自动解码恢复
```

详见[编码与压缩](codec.md)。
