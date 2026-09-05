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
| `UTF8` | `Dictionary(Int32, Utf8)` | 字典段直接映射（keys u32→i32 为 **checked cast**：`i32::try_from`，超出 i32 值域 → Error，不是 reinterpret；strings 按字节拷入 `StringArray`） |

反向映射（RecordBatch → Data，供 create/write 路径）：一一对应；Arrow 字典 → V2 Utf8
字典列；不支持类型返回错误。

## 3. NULL 语义

- V2 validity bitmap（LSB-first，1=有效）与 Arrow null buffer 位序一致：
  直接按字节拷贝；`validity = None`（全有效）→ Arrow 侧不设 null buffer。
- Arrow → V2：Arrow null buffer 拷回 V2 Bitmap；无 null buffer → `validity = None`。

## 4. 零拷贝边界

- **Utf8 字典列**：keys u32→i32 为 **checked cast**（逐元素 `i32::try_from`，
  值域校验，超出 → Error——u32::MAX 无法表示为 i32，不能称单纯 reinterpret）；
  字典 strings 按字节拷入 `StringArray`（引用 `&str` 后拷贝）。
- **定宽列**：typed `Vec` 拷贝构造 PrimitiveArray（一次 memcpy）；零拷贝需要
  format `Buffer` 提供 Arc 共享所有权，且需通过
  `arrow_buffer::Buffer::from_custom_allocation` 包装——该优化**依赖 core buffer
  ownership 与 Arrow buffer layout 的兼容设计**（对齐 / 偏移 / 长度约定），不能
  简单承诺「Arc 变体后即零拷贝」。
- **多段 ColumnView**：RecordBatch 单列单数组约束下，多段合并为单一 Arrow Array
  时**当前发生一次合并拷贝**（逐段构造 + arrow concat）；后续零拷贝同样依赖上述
  ownership / layout 兼容设计。

> 本版为正确性优先；上表拷贝点均为 `docs/splayed-core.md` §9 记录的已知优化项。

## 5. API 设计（三层）

### Layer 1：Array 转换（单列）

```rust
pub fn column_to_array(col: &Column) -> Result<ArrayRef>            // 拥有型列
pub fn column_view_to_array(view: &ColumnView) -> Result<ArrayRef>  // 多段视图（段间合并拷贝）
```

### Layer 2：Batch 转换（core ↔ Arrow 纯内存转换）

```rust
pub fn data_to_record_batch(data: &Data) -> Result<RecordBatch>                 // 逐列复用 column_to_array
pub fn data_view_to_record_batch<'a>(view: &DataView<'a>) -> Result<RecordBatch> // 逐列视图直转，不物化为 Data
pub fn record_batch_to_data(batch: &RecordBatch) -> Result<Data>                 // Arrow → core（create / write 路径）
```

### Layer 3：Table 流式适配

```rust
pub fn read_table_as_arrow<'t>(table: &'t TableHandle, scanner: TableScanner<'t>,
    batch_size: Option<usize>) -> TableArrowReader<'t>

pub struct TableArrowReader<'t> { /* inner: TableReader<'t> */ }
impl<'t> TableArrowReader<'t> {
    pub fn next(&mut self) -> Result<Option<RecordBatch>>   // 逐批流式，不物化整个结果集
    pub fn close(self) -> Result<()>
}
```

职责与约束：

- **调用链**：`TableScanRequest → scan_table → TableScanner → read_table_as_arrow
  → TableArrowReader → next() → RecordBatch`。Arrow 层直接接收 `TableScanner`
  （而非 `TableScanRequest`）——**查询语义（裁剪 / 谓词 / limit）归 Table 层**，
  Arrow 层不重新 scan、不做 pruning。
- **流式**：`TableArrowReader` 逐批输出；构造无 I/O，Dataset 打开 / META 错误延迟
  到 `next()`；1 TB 级结果不物化 `Vec<RecordBatch>`。
- `data_view_to_record_batch`：多段合并按行序拼接（发生一次合并拷贝，见 §4）；
  所有列行数必须一致（继承 DataView 校验）；**不物化为 `Data`**（否则引入多余拷贝）。
- `record_batch_to_data`：无 sym/time 约束（由 create/write 调用方校验）；
  Arrow nullable → validity bitmap；`Dictionary(Int32, Utf8)` → V2 字典表示
  （keys + DictBuffers，天然映射，不展开为普通字符串）；字典键位宽校验。
- **nullable 恒为 true**：V2 不建模 NOT NULL 约束——schema 跨批次稳定，不随当批
  是否含 NULL 变化。
- 类型映射统一入口：`to_arrow_type` / `from_arrow_type`（Layer 1 / 2 / 反向共用，
  不复制映射逻辑）。

## 6. 与 V1.0 能力对照

| V1.0 能力 | V2.0 对应 | 状态 |
| --- | --- | --- |
| CoreBatch → Arrow 零拷贝 | `data_to_record_batch`（拷贝级 + Arc 兼容设计为优化路径） | ✅（拷贝级） |
| NULL / NaN 语义保留 | validity bitmap 直接映射（无哨兵概念） | ✅ |
| 类型映射 0–13 | 类型映射表（+UTF8 字典） | ✅ |
| ArrayRef/RecordBatch 输出 | 同 | ✅ |
| Arrow → core（写路径） | `record_batch_to_data` | ✅ 新增 |
| Table 端到端 Arrow 流 | `read_table_as_arrow` → `TableArrowReader`（流式，不物化） | ✅ 新增 |
