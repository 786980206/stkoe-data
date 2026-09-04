# CoreBatch 内存模型

扫描输出由 **CoreBatch** 承载（`splayed-core::batch`，**零 Arrow 依赖**）——它是引擎无关的内存列式表示，也是零拷贝的桥梁。

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

## 核心类型

| 类型 | 说明 |
| --- | --- |
| `CoreType` | 内存列类型：含 Varlen/List/Struct/Dictionary/时间单位（仅内存，不落盘） |
| `CoreSchema` / `CoreField` | 列式 schema（`[time(0), sym(1), fields(2+)]` 布局） |
| `CoreColumn` | `Primitive{ty, data}` / `Dictionary{indices(u32), values: Arc<CoreStringDict>}` / `Varlen{offsets, data}` + `nulls: Option<Bitmap>` |
| `Buffer` | 连续字节缓冲：`take()` 零拷贝移交、`typed_slice<T>` 类型化切片 |
| `Bitmap` | validity 位图：`slice(offset, len)`、`contains_null`；无 NULL 时 `None`（零分配） |
| `CoreStringDict` | 字符串共享字典（SYM 列） |

## 关键语义

- **NULL**：磁盘仍是哨兵位型；读时哨兵 → validity 位图（每批次每列一次 O(n) 判空），数据缓冲原样保留；`FieldHeader.null_count == 0` 时跳过扫描（无位图、零开销）。写回时位图 → 哨兵。
- **TIME 列按类型产出**：Date32 直接 `i32`、TimestampUs 直接 `i64`，消除转换端收窄拷贝。
- **SYM 列字典化**：u32 索引 + 共享字典，取代逐行字符串展开。
- **截断**：`CoreBatch::slice(offset, len)` 用于 limit/窗口截断；`Buffer::take()` 零拷贝移交。

## 去向（零拷贝）

- **Arrow**：`splayed_arrow::corebatch_into_record_batch` 把缓冲直接交给 Arrow（FIELD/TIME 不拷贝、0 NULL 列不产 validity、字典→Utf8 展开）。
- **DuckDB**：`splayed-duckdb` 原生路由 / `ffi` 直接消费原始字节 + validity（定长列缓冲与 DuckDB `Vector` 布局同构）。
- **Polars**：经 Arrow C data interface 零拷贝进 polars。
