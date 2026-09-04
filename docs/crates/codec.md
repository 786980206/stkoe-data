# splayed-codec（编码/压缩）

**依赖**：format。

## 接口

| 接口 | 说明 |
|---|---|
| `compact_field(path, compression)` | NONE→ZSTD/LZ4 压缩（`[u64 len][payload]`），成功后只读；**重算统计 footer** |
| `compact_field_with_encoding(path, encoding, compression)` | **编码接线**：DELTA/RLE/BITPACK 编码（`[u64 编码长][payload]`）后可选 ZSTD/LZ4；只读；读侧按 `header.encoding` 自动解码恢复原始布局（统计 footer 保留） |
| `decompress_field_data(path)` | 压缩字段解压为原始字节 |
| `encode_encoding / decode_encoding` | 编码器（按需暴露） |
| `PlainCodec` | PLAIN 编解码 |
| `CompactError / DecompressError / CodecError` | 错误类型 |

## 模块

- `plain` — PLAIN（原始固定宽度）
- `delta` — DELTA（时间/整数）
- `rle` — RLE（重复值）
- `bitpack` — BITPACK（小整数）
- `compact` — 压缩 + 接线入口

详见 [编码与压缩](../format/codec.md)。
