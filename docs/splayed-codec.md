# splayed-codec：API 设计（V2.0 Draft）

`splayed-codec` 提供 Encoding（`PLAIN / DELTA / RLE / BITPACK`）与 Compression（`NONE / ZSTD / LZ4`）的内存编解码原语。Encoding / Compression 的 ID 枚举见 [splayed-format](splayed-format.md) §5 / §6；内存视图类型（BufferView / BitmapView）定义见 splayed-format §3。本页定义编码语义、compressed FIELD 的物理表示与 public API。

依赖：`splayed-format`（DataType / Encoding / Compression ID）。不依赖 core / Arrow；不做文件 IO、不感知文件 header——那是 `splayed-core` 的职责。

## 1. 职责边界

```
splayed-core（FieldHandle / Field 文件）
        │  调用
        ▼
splayed-codec            内存字节 ↔ 编码/压缩 payload 的纯变换
        │
        ▼
splayed-format           ID 枚举与物理布局定义（不提供实现）
```

- codec 的输入输出都是内存 buffer / payload；不接触文件、mmap、header。
- 编解码必须逐位可逆：`decode(encode(x)) == x`，对任意输入（含 NULL 单元的未定义位）成立。

## 2. 两种物理表示

一个 FIELD 处于且仅处于两种物理表示之一，由 header 的 `encoding` / `compression` 字段区分：

| 表示 | 条件 | DATA / VALIDITY 布局 |
| --- | --- | --- |
| uncompressed | `encoding = PLAIN` 且 `compression = NONE` | splayed-format §8：连续 values + 可选 validity 区 |
| compressed | 其余任何组合 | HEADER 64B + chunk 序列（§3）；values 编码与 validity 打包进 chunk payload，无独立 VALIDITY 区 |

## 3. Compressed FIELD 的 chunk 布局

```text
FIELD 文件（compressed）
+------------------------------+
| HEADER             64 bytes  |
+------------------------------+
| chunk[0]                     |
| chunk[1]                     |
| ...                          |
| chunk[N-1]                   |
+------------------------------+

chunk
+--------------------+--------------------+------------------+------------------+
| rows       u32 LE  | values_len u32 LE  | payload_len u32  | payload          |
+--------------------+--------------------+------------------+------------------+

payload = compression( encode_encoding(values[rows]) || validity_bits[ceil(rows/8)] )
```

- chunk 行数由写入侧决定，chunk 头自描述（`rows` 明文），读侧不假设固定大小；最后一块允许不足。
- 常见策略：缺省固定 8192 行均匀分块；Dataset 层压缩时按 `k × time_count`（k 个连续 sym，默认 k = 8）生成 sym 对齐边界。
- chunk 头（rows / values_len / payload_len）恒为明文小端。
- `payload` 先按 Encoding 编码 values，再拼接该 chunk 的 validity 位，最后整段按 Compression 压缩；`values_len` 记录解压后 encoded values 的长度，validity 位在解压结果中按该偏移分离。
- 无 Block Index：定位第 k 个 chunk 需顺序跳过前 k 个 chunk 头（每头 12 字节）；随机单点读场景应使用 `PLAIN + NONE`。
- 完整性约束：`Σ chunk.rows == row_count`；`payload_len` 与实际 payload 长度一致。

## 4. API 设计

### 4.1 单列原语

```
encode_values(encoding, data_type, values: BufferView, row_count) -> Result<Vec<u8>>
decode_values(encoding, data_type, payload, row_count) -> Result<Buffer>       // owned

compress(compression, payload) -> Result<Vec<u8>>
decompress(compression, payload) -> Result<Vec<u8>>
```

- `encode_values` / `decode_values` 只作用于 values 数组；`PLAIN` 为直通。
- `decode_values` 产出自有 Buffer（解压/解码必然物化）。
- ZSTD / LZ4 的压缩级别取 codec 内置默认常量，不写入 header。

### 4.2 chunk 组合原语

```
encode_chunk(encoding, compression, data_type,
             values: BufferView, validity: BitmapView?, rows) -> Result<ChunkPayload>

decode_chunk(encoding, compression, data_type,
             chunk: ChunkPayload) -> Result<(Buffer, BitmapView?)>            // owned
```

- `validity = null` 表示该 chunk 全有效；`decode_chunk` 返回的 validity 相应为 null。
- 这是 `splayed-core` 写回（compress / close 收尾）与读取（逐 chunk 物化）的实际调用单元。

## 5. Encoding 语义

| Encoding | 语义 |
| --- | --- |
| `PLAIN` | 原始定宽连续数组；encode = 直通 |
| `DELTA` | 首值原样存储，其余存相邻差分；适用于 time / 有序整数列 |
| `RLE` | `(run_length, value)` 序列；适用于重复值列 |
| `BITPACK` | 按块统计最大位宽并打包；适用于小整数列 |

- 除 `PLAIN` 外的编码按 chunk 内数据生效；跨 chunk 不共享状态。
- 差分位宽、RLE 计数宽度、BITPACK 分块大小等实现参数在 codec 内定稿，不进文件 header。

## 6. NULL 与 validity

- Encoding 只作用于 values，不感知 NULL；validity 独立按位拼接、随 chunk 压缩。
- NULL 单元的 value 位未定义：参与编码（按位参与差分/打包），解码按位原样还原；只影响压缩率，不影响语义。
- 读侧以 validity 判定 NULL，不检查 value 位。

## 7. 与 splayed-core 的关系

| core 场景 | codec 调用 |
| --- | --- |
| `compress_field_file` / compressed write handle 的 close 收尾 | 逐 chunk `encode_chunk` |
| compressed Field 的 `read_field_handle` | 定位覆盖的 chunks → `decode_chunk` 物化到 working representation → 返回 view |
| compressed Field 的 `scan_field_handle` | 逐 chunk 物化后求值 predicate（值过滤发生在解压后数据上） |
| `decompress_field_file` | 全量 `decode_chunk` → 按 format §7 布局重写为 uncompressed |

## 8. 注意事项

- 无 Block Index：compressed FIELD 面向大范围扫描 / 冷数据 / 归档；随机单点查询走 `PLAIN + NONE`。
- 分块对齐（如 sym 对齐）由调用方以 `offsets` 参数表达，codec 不感知 META；compressed write 的 close 收尾沿用文件既有 chunk 分组。
- `decode_chunk` 必须校验 `rows`、`values_len`、`payload_len` 与 payload 实际内容一致，不一致按数据损坏处理（Err）。
- 编解码逐位可逆是硬性约束；`PLAIN` 之外每种 Encoding 都必须有覆盖 NULL 位型的 roundtrip 测试。
