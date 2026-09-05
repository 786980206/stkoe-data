# splayed-arrow：V2.0 设计文档（Arrow 转换层）

## 1. 定位

`splayed-arrow` 是共享转换工具：把 core 的内存数据模型（`Column` / `ColumnView` /
`Data` / `DataView`）映射为 Arrow 数组与 RecordBatch，供 DataFusion / DuckDB / Polars /
ADBC 等适配层复用。依赖 `splayed-format`、`splayed-core` 与 arrow-rs（与 bench 的
parquet 56 对齐，arrow-array/buffer/schema 56）。

- 依赖方向：`format ← codec ← core ← arrow`；arrow 不反向依赖任何适配层。
- 不触碰磁盘格式；一切输入来自 core 的内存表示。

## 2. 类型映射（权威表）

| DataType（V2.0） | Arrow 类型 | 说明 |
| --- | --- | --- |
| `BOOL` | `Boolean` | |
| `INT8/16/32/64` | `Int8/16/32/64` | |
| `UINT8/16/32/64` | `UInt8/16/32/64` | |
| `FLOAT32/64` | `Float32/64` | |
| `DATE32` | `Date32` | 天序号直接映射 |
| `TIMESTAMP_US` | `Timestamp(µs, None)` | 无时区 |
| `DATE64` | `Date64` | |
| `UTF8` | `Dictionary(Int32, Utf8)` | 字典段直接映射（keys u32→i32 位宽转换，strings 零拷贝字节） |

反向映射（RecordBatch → Data，供 create/write 路径）：一一对应；Arrow 字典 → V2 Utf8
字典列；不支持类型返回错误。

## 3. NULL 语义

- V2 validity bitmap（LSB-first，1=有效）与 Arrow null buffer 位序一致：
  直接按字节拷贝；`validity = None`（全有效）→ Arrow 侧不设 null buffer。
- Arrow → V2：Arrow null buffer 拷回 V2 Bitmap；无 null buffer → `validity = None`。

## 4. 零拷贝边界

- **Utf8 字典列**：keys 位宽转换（u32→i32）为一次字节级重解释拷贝；字典
  strings 零拷贝字节引用后拷入 `StringArray`。
- **定宽列**：v1 通过 typed `Vec` 拷贝构造 PrimitiveArray（一次 memcpy）；
  零拷贝需要 format `Buffer` 提供 Arc 共享所有权（优化路径：`Buffer` 增加
  Arc 变体后，`arrow_buffer::Buffer::from_custom_allocation` 直接包装）。
- **多段 ColumnView**：段间拼接使用逐段拷贝合并（RecordBatch 单列单数组约束）；
  跨段零拷贝依赖上条 Arc 变体。

> 本版为正确性优先；上表拷贝点均为 `docs/splayed-core.md` §9 记录的已知优化项。

## 5. API 设计

```
// Column / Data → Arrow
column_to_arrow(col: &Column) -> Result<ArrayRef>
data_to_record_batch(data: &Data) -> Result<RecordBatch>
data_view_to_batch<'a>(view: &DataView<'a>) -> Result<RecordBatch>   // 多段合并

// Arrow → core（create / write 路径）
record_batch_to_data(&RecordBatch) -> Result<Data>

// Table 端到端
scan_to_arrow(table: &TableHandle, request: TableScanRequest, batch_size) 
    -> Result<Vec<RecordBatch>>     // 逐批查询并转换
```

职责与约束：

- `data_view_to_batch`：多段合并按行序拼接；所有列行数必须一致（继承 DataView 校验）。
- `record_batch_to_data`：无 sym/time 约束（由 create/write 调用方校验）；
  Arrow nullable → validity bitmap；字典键位宽校验。
- `scan_to_arrow`：语义 = `query_table` + 逐批 `data_view_to_batch`；不改扫描语义。

## 6. 与 V1.0 能力对照

| V1.0 能力 | V2.0 对应 | 状态 |
| --- | --- | --- |
| CoreBatch → Arrow 零拷贝 | `data_to_record_batch`（v1 拷贝 + Arc 优化路径） | ✅（拷贝级） |
| NULL / NaN 语义保留 | validity bitmap 直接映射（无哨兵概念） | ✅ |
| 类型映射 0–13 | 类型映射表（+UTF8 字典） | ✅ |
| ArrayRef/RecordBatch 输出 | 同 | ✅ |
| Arrow → core（写路径） | `record_batch_to_data` | ✅ 新增 |
