# 编码与压缩

## Encoding

| ID | 编码 | 说明 |
| --: | --- | --- |
| 0 | `PLAIN` | 原始固定宽度数组（默认） |
| 1 | `DELTA` | Delta 编码（时间/整数） |
| 2 | `RLE` | Run Length Encoding（重复值） |
| 3 | `BITPACK` | 位打包（小整数） |

**优先级：PLAIN 是第一优先级实现**——`mmap + PLAIN` 是最快路径。

## Compression

| ID | 压缩 | 特点 |
| --: | --- | --- |
| 0 | `NONE` | mmap / O(1) 随机访问 |
| 1 | `ZSTD` | 高压缩率 |
| 2 | `LZ4` | 高解压速度 |

**NONE** 直接定位：

```text
offset = 64 + row × sizeof(type)
```

**压缩（ZSTD/LZ4）**：不做 FIELD Block Index，因此压缩 FIELD 更适合**大范围扫描 / 冷数据 / 归档**，而非随机单点查询。

## 接线（compact / 编码）

`splayed-codec` 提供：

| 接口 | 说明 |
| --- | --- |
| `compact_field(path, compression)` | NONE→ZSTD/LZ4 压缩（`[u64 len][payload]`），成功后**只读**；**重算统计 footer**（压缩后统计永久有效） |
| `compact_field_with_encoding(path, encoding, compression)` | **编码接线**：DELTA/RLE/BITPACK 编码（`[u64 编码长][payload]`）后可选 ZSTD/LZ4；只读；读侧按 `header.encoding` 自动解码恢复原始布局（统计 footer 保留） |
| `decompress_field_data(path)` | 压缩字段解压为原始字节 |
| `encode_encoding / decode_encoding` | 编码器（按需暴露） |
| `PlainCodec` | PLAIN 编解码 |

```text
压缩:   [64B head][u64 len][payload][28B footer]
编码:   [64B head][u64 编码长][payload][28B footer]
```

## 只读语义

- `compression != NONE` → `update_field` 被拒绝（只读）。
- `compact_field` 对已压缩字段拒绝（不允许二次压缩）。
- `compaction`/编码重写都会**重算统计 footer**，保证压缩后统计永久有效。
