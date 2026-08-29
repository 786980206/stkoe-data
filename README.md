# Splayed V1

A specialized **columnar storage engine** for `SYM × TIME × FIELD` financial time-series data.

Designed for **extremely low I/O, O(1) row location, mmap/zero-copy reads, and high-throughput pre-allocated writes**. The engine core is Arrow-agnostic; Arrow is a shared conversion tool used by the engine adapters (DataFusion / Polars / DuckDB / ADBC).

> 本文档只描述**当前已实现**的状态（与 `plan.md` 的已落地部分一致；未实现/暂缓项不在此列）。

---

## Design Principles（已实现）

> **META decides *where* to read. FIELD decides *what* to read.**

| Principle | Decision |
|---|---|
| Data model | `SYM × TIME × FIELD`；一个 `.meta` 文件夹 = 一个 dataset（≈ Parquet 文件），一堆 dataset 目录 = 分区表（≈ Hive 分区表） |
| Layout | Splayed — 每 FIELD 一个文件 |
| SYM | 每个符号连续行，mmap 友好；`SymbolSelection` 直接裁剪 SYM INDEX 块 |
| TIME | 全局去重升序 TIME AXIS；每 SYM 引用 `[time_start, time_start+time_count)` 区间 |
| FIELD | 磁盘定长类型（无变长列、无 Block Index、无磁盘 validity 位图）；`plain.rs/delta.rs/rle.rs/bitpack.rs` + `NONE/ZSTD/LZ4` |
| NULL | 类型内**哨兵位型**（浮点 canonical NaN、有符号 INT_MIN、无符号全 1、BOOL `0x02`）；比较按 **bit pattern** |
| Write | 全预分配 + 原地更新（`create_field` → `update_field`）；`compact_field` 是唯一重写文件体的操作 |
| Compression | `NONE` = 最快 mmap 路径；`ZSTD`/`LZ4` 冷数据；**压缩后只读** |
| Generation | u64 严格单调；META/FIELD 同版本；Reader 拒绝 generation 不匹配的 FIELD |
| Commit | `.meta.new` 写入 → fsync → 原子 rename 提交；`update_meta` 用 generation 屏障保证重散布中间态不被读到 |

---

## Architecture & Crates

```text
外部应用 → splayed-adbc（上层 ADBC 驱动，内部经 DataFusion 执行 SQL → Arrow）
             ├→ splayed-datafusion（DataFusion TableProvider 三层对接 + SQL 执行）
             ├→ splayed-polars（Polars AnonymousScan 惰性扫描）
             └→ splayed-duckdb（DuckDB 集成层：IPC 桥 / 原生 DataChunk / C ABI）
splayed-core（引擎无关核心：数据扫描与写入、CoreBatch；零 Arrow）
splayed-arrow（共享转换工具：CoreBatch → Arrow 零拷贝；被需要 Arrow 的适配层复用，可选用）
```

| Crate | 职责 |
|---|---|
| `splayed-format` | 磁盘格式：META/FIELD header、类型（0–13）、NULL 编码、统计 footer（无依赖） |
| `splayed-codec` | 编码（PLAIN/DELTA/RLE/BITPACK）+ 压缩（NONE/ZSTD/LZ4） |
| `splayed-core` | mmap 读取、字段/表写入（`create_field*`/`update_field`/`delete_field`、`create_table`/`update_table`/`update_meta`）、Dataset、Scanner（CoreBatch 流，含并行/限值/统计剪裁） |
| `splayed-arrow` | Arrow 交换层：`create_meta`/`create_table`/`update_table`（Arrow 输入）+ `corebatch_into_record_batch`（零拷贝） |
| `splayed-datafusion` | 三层：`SplayedDatasetProvider`（单 dataset）→ `SplayedTableProvider`（分区表，时间裁剪）→ `register_splayed_table` / `read_splayed` / STORED AS SPLAYED |
| `splayed-polars` | `AnonymousScan` 惰性扫描：谓词下推 + 列裁剪 + 兜底过滤（polars =0.45） |
| `splayed-duckdb` | Arrow IPC 导出（feature "arrow"）、原生 `scan_to_chunks`（零 Arrow）、DuckDB 扩展 C ABI（`ffi`） |
| `splayed-adbc` | 上层 ADBC 驱动（DataFusion 后端），`Connection`/`Statement`/`refresh` |
| `splayed` | Umbrella：核心恒有；`arrow`(默认)/`datafusion`/`duckdb`/`polars`/`adbc` features 开关 |
| `splayed-cli` | CLI：init/update/compact/read/sql/export/export-arrow |
| `splayed-duckdb-extension/` | C++ DuckDB 扩展壳骨架（非 cargo 成员，消费 `splayed-duckdb` 的 C ABI） |

依赖方向：`format ← codec ← core → arrow → {datafusion, duckdb, polars} → adbc`。
Umbrella 用法：`splayed = { path = "crates/splayed", features = ["arrow", "adbc", "duckdb", "polars"] }`。

---

## Quick Start

### Build & Test

```bash
cargo build        # 零警告
cargo test         # 全量（Windows/MSVC 下建议：cargo test --config "profile.test.debug=false"）
```

### 建表与读取（Arrow 入口）

```rust
use arrow_array::{Date32Array, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDT, Field, Schema};
use std::sync::Arc;
use splayed_arrow::create_table;
use splayed_core::{open_dataset, FieldReader};

let schema = Arc::new(Schema::new(vec![
    Field::new("time", ArrowDT::Date32, false),
    Field::new("sym", ArrowDT::Utf8, false),
    Field::new("close", ArrowDT::Float64, true),
]));
let batch = RecordBatch::try_new(schema, vec![
    Arc::new(Date32Array::from(vec![0, 1, 2])),
    Arc::new(StringArray::from(vec![Some("AAPL"); 3])),
    Arc::new(Float64Array::from(vec![Some(100.0), Some(101.0), Some(102.0)])),
]).unwrap();

create_table("my_dataset", &batch, true).unwrap();   // .meta + FIELD + 统计 footer

let dataset = open_dataset("my_dataset").unwrap();
let reader = FieldReader::open("my_dataset/close").unwrap();
assert_eq!(reader.read_row(0).unwrap().as_f64(), Some(100.0));
```

### 原生扫描（CoreBatch 流）

```rust
use splayed_core::{open_dataset, Scanner, ScanRequest, Filter, FilterValue,
                   SymbolSelection, TimeRange, scan_owned_parallel};
use std::sync::Arc;

let dataset = Arc::new(open_dataset("my_dataset").unwrap());
let req = ScanRequest {
    columns: vec!["close".into()],                    // 投影（列裁剪）
    symbols: SymbolSelection::All,
    time_range: TimeRange::all(),                     // TIME 裁剪（半开区间）
    filters: vec![Filter::GreaterThan { field: "close".into(),
                                        value: FilterValue::Float64(100.5) }], // AND 语义
    batch_size: 65536,
    parallelism: 4,                                   // 并行（有序流式）
    limit: Some(100),                                 // 读取期截断（第 N 个通过过滤的行即停）
};
let scanner = Scanner::new(&dataset);
let plan = scanner.plan(&req).unwrap();               // SYM/TIME 剪裁 + 统计(min/max)整表剪裁
let mut stream = scan_owned_parallel(dataset, &plan, &req, 4).unwrap();
while let Some(batch) = stream.next_batch().unwrap() {
    // CoreBatch：typed columns `[time(0), sym(1), fields(2+)]`
}
```

### DataFusion SQL（三层：dataset → 分区表 → 绑定）

```rust
use datafusion::prelude::SessionContext;
use splayed_arrow::create_table;
use splayed_datafusion::{register_splayed_table, SplayedDatasetProvider};

let ctx = SessionContext::new();
// 单 dataset 或分区表目录均自动探测：
register_splayed_table(&ctx, "splayed", "my_dataset").unwrap();

// 谓词下推：sym 等值 → SymbolSelection、time → TimeRange、
// FIELD 值比较 → 扫描期 Filter；LIMIT → 读取期截断；与列统计不相交 → 空扫描
let batches = ctx.sql("SELECT close FROM splayed WHERE sym = 'AAPL' AND time >= 1 LIMIT 3")
    .await.unwrap().collect().await.unwrap();

// core::update_meta 重排布局后，刷新 provider 即可看到新数据：
let mut provider = SplayedDatasetProvider::new("my_dataset").unwrap();
splayed_core::update_meta("my_dataset", splayed_format::TimeType::Date32, &["AAPL".into()], &[0,1,2]).unwrap();
provider.reload().unwrap();
```

分区表（每个带 `.meta` 的子目录 = 一个分区，扫描按时间/分区目录裁剪）：

```rust
register_splayed_table(&ctx, "prices", "my_table").unwrap(); // my_table/2024/.meta, ...
let batches = ctx.sql("SELECT COUNT(*) FROM prices WHERE time < TO_DATE('2025-01-01')")
    .await.unwrap().collect().await.unwrap();
```

另有 `read_splayed('/path')` 表函数与 `CREATE EXTERNAL TABLE ... STORED AS SPLAYED LOCATION 'dir'`（`SplayedTableFactory`）。

### Polars 惰性扫描

```rust
use polars::prelude::*;
use splayed_polars::splayed_lazyframe;

let lf = splayed_lazyframe("my_dataset").unwrap();
let out = lf.filter(col("close").gt(lit(100.5)))
             .select([col("sym"), col("close")])
             .collect().unwrap();   // 谓词下推进核心层 + 只读涉及列
```

### ADBC（上层统一接口）

```rust
use splayed_adbc::Connection;

let conn = Connection::open("my_dataset").unwrap();
let rows = conn.execute("SELECT close FROM splayed WHERE sym = 'AAPL'").unwrap();
// core::update_meta(...) 之后：
conn.refresh().unwrap();            // 重新注册新 provider，后续 SQL 看到新布局
```

### DuckDB（三条路径）

1. **Arrow IPC 桥**（feature "arrow"，默认开）：`splayed_duckdb::export_to_arrow_ipc(dir, out.arrow)` → DuckDB `read_arrow('out.arrow')`；
2. **原生 DataChunk 路由**（零 Arrow）：`splayed_duckdb::native::scan_to_chunks(Arc<Dataset>, &req, n, |core_batch| …)` — 扩展层逐批消费 `CoreBatch`（定长 LE 缓冲 + validity，与 DuckDB `Vector` 布局同构）；
3. **C ABI**（`splayed_duckdb::ffi`，cdylib/staticlib 按需生成）：`splayed_dataset_open` / `splayed_scan_open` / `splayed_scan_next`（返回扁平列视图 `SplayedColumnFFI`：kind/type_id/data/data_len/validity(位图)/dict_count，SYM 字典串经 `splayed_scan_dict_value` 取回；句柄=裸指针、谁创建谁释放）→ 供 `splayed-duckdb-extension` C++ 扩展壳消费（壳为骨架，需接真实 DuckDB SDK 编译）。

---

## Function API

Writer（原生在 `splayed-core`；Arrow 输入版在 `splayed-arrow`）：

| Function | Crate | Description |
|---|---|---|
| `create_meta(folder, data, sorted)` | splayed-arrow | 由 Arrow RecordBatch（TIME+SYM）建 `.meta` |
| `create_table(folder, data, sorted)` | splayed-arrow | Arrow 一步建表（.meta + 各 FIELD 一次写入） |
| `update_table(folder, data, sorted, create_missing_fields)` | splayed-arrow | 按已有 (SYM, TIME) 格子原地更新（Arrow 输入） |
| `create_meta(dir, time_type, sym, time)` | splayed-core | 原生建 `.meta`（逐行 sym/time） |
| `create_table(dir, time_type, sym, time, columns, sorted)` | splayed-core | 原生一步建表；`sorted=true` 走 O(n) 快路径（输入已按 (SYM,TIME) 升序） |
| `update_meta(dir, time_type, sym, time)` | splayed-core | **以新 (SYM, TIME) 布局重建 `.meta` + 并发重散布全部现有 FIELD**（gather / NULL 补齐 / 真实 null_count / 统计 footer 重算；generation 屏障 + 最后 rename 原子提交；失败重跑幂等；Windows 下需先关闭所有打开中的 reader） |
| `update_table(dir, sym, time, columns, create_missing_fields)` | splayed-core | 原生格子更新（未知位置/缺失字段/类型不匹配报错；`true` 自动建缺失 FIELD） |
| `create_field(path, data_type)` | splayed-core | 预分配全 NULL 字段文件 |
| `create_field_with_data(path, data_type, values)` | splayed-core | 建字段并一次写入数据 + 统计 footer |
| `update_field(path, &[UpdateItem])` | splayed-core | 原地写；成功后使统计 footer 失效（magic 归零） |
| `delete_field(path)` | splayed-core | 删除字段文件（幂等） |
| `compact_field(path, compression)` | splayed-codec | 压缩为只读（重新计算统计 footer，压缩后统计永久有效） |

Read-side：

| Function | Crate | Description |
|---|---|---|
| `open_dataset(dir)` → `Dataset` | splayed-core | 打开 dataset；`field_path/list_fields/existing_fields` |
| `FieldReader::open(path)` | splayed-core | mmap/解压读；`read_row/read_range(raw)`、`stats() -> Option<FieldStats>` |
| `Scanner::plan(&ScanRequest)` | splayed-core | SYM 裁剪 + TIME 裁剪 + 统计整表剪裁 |
| `scan` / `scan_owned` / `scan_owned_parallel` / `scan_all_parallel` | splayed-core | CoreBatch 流（借用/自有/并行保序/全收集；均支持 `limit`） |
| `split_ranges(plan, n)` | splayed-core | 行均衡切片（并行入口） |
| `corebatch_into_record_batch(batch, proj, fields, dict)` | splayed-arrow | CoreBatch → Arrow **零拷贝**（缓冲移交；无 NULL 列不产 validity） |
| `splayed_lazyframe(dir)` | splayed-polars | Polars `LazyFrame` 惰性数据源 |
| `scan_to_chunks(dataset, req, n, sink)` | splayed-duckdb | 原生 DataChunk 路由（零 Arrow） |
| `export_to_arrow_ipc(dir, out)` | splayed-duckdb | Splayed → Arrow IPC 文件 |
| `ffi::splayed_*` | splayed-duckdb | DuckDB 扩展 C ABI（句柄 + 列视图 + 错误消息） |
| `Connection::open/refresh` | splayed-adbc | 上层 ADBC：SQL → Arrow 批次流 |

---

## On-disk Format

```text
dataset/
├── .meta          # 索引：TIME AXIS + SYM DICT + SYM INDEX（+ 可选统计）
├── close          # FIELD: [64B header] [data…] [28B stats footer(可选)]
├── open
└── ...
```

### META Header（64 字节）

| Offset | Field | Type |
|---:|---|---|
| 0 | `magic` | u64 (`SPLAYMTA`) |
| 8 | `version` | u16 |
| 12 | `time_type` | u8 (0=DATE32, 1=TIMESTAMP_US) |
| 16 | `generation` | u64 |
| 24 | `time_count` | u32 |
| 28 | `sym_count` | u32 |
| 32 | `sym_dict_offset` | u64 |
| 40 | `sym_index_offset` | u64 |
| 48 | `file_size` | u64 |

### SYM INDEX Record（12 字节）

| Offset | Field | Type | Description |
|---:|---|---|---|
| 0 | `time_start` | u32 | 该 SYM 在全局 TIME AXIS 的起始下标 |
| 4 | `time_count` | u32 | 该 SYM 覆盖的连续区间长度（= 行容量，全预声明） |
| 8 | `row_start` | u32 | 该 SYM 在 FIELD 文件中的起始行（前序 time_count 累加） |

### FIELD Header（64 字节）

| Offset | Field | Type |
|---:|---|---|
| 0 | `magic` | u64 (`SPLAYFLD`) |
| 12 | `data_type` | u8 |
| 13 | `encoding` | u8 |
| 14 | `compression` | u8 |
| 16 | `generation` | u64 |
| 24 | `row_count` | u32 |
| 28 | `null_count` | u32 |
| 40 | `data_length` | u64 |

> `data_length` = 纯数据字节数（行数 × 类型宽）；`null_count` 在 `update_meta`
> 重写的字段中是**真实** NULL 计数（预分配语义下不保证）。

### 统计 Footer（28 字节，可选）

数据区之后：`magic "SFTF" + version + flags + min[8槽] + max[8槽] + total`。
min/max **跳过 NULL 哨兵**；写入时机：`create_field_with_data`（含 `create_table`）
与 `compact_field`（重算）；`update_field` 使失效（magic 归零，文件大小不变）。
消费：`Scanner::plan` 整数据集剪裁（filter 与统计不相交 → 空计划，任何 FIELD
都不读）；DataFusion `statistics()` 上报 FIELD 列 min/max 与 TIME 列
`[time_axis[0], time_axis[-1]]`（`Precision::Exact`）。

### 压缩布局

```text
[64B header][u64 uncompressed_len][zstd/lz4 payload][28B stats footer(可选)]
```

读取时按尾部 magic 检测 footer 并从 payload 区排除；压缩 = 只读。

### Generation 版本一致性

- u64 严格单调；`update_field` / `update_meta` 成功后递增；
- Reader 打开 FIELD 时校验与 META generation 一致，不一致拒绝；
- `.meta` 提交 = 写 `.meta.new` → fsync → 原子 rename（`update_meta` 在
  字段全部 rename 完成后才 rename meta，提交前中间态被 generation 屏障挡住）。

### NULL 编码（哨兵位型，无磁盘 validity 位图）

| ID | Type | Size | NULL pattern |
|---:|---|---:|---|
| 0 | `BOOL` | 1 B | `0x02` |
| 1 | `INT32` | 4 B | `0x80000000` |
| 2 | `INT64` | 8 B | `0x8000000000000000` |
| 3 | `FLOAT32` | 4 B | `0x7FC00000` (canonical NaN) |
| 4 | `FLOAT64` | 8 B | `0x7FF8000000000000` (canonical NaN) |
| 5 | `DATE32` | 4 B | `0x80000000` |
| 6 | `TIMESTAMP_US` | 8 B | `0x8000000000000000` |
| 7 / 8 | `INT8` / `INT16` | 1 / 2 B | `0x80` / `0x8000` |
| 9–12 | `UINT8/16/32/64` | 1/2/4/8 B | 全 1（MAX 位型） |
| 13 | `DATE64` | 8 B | `0x8000000000000000` |

> ID 0–6 为原始类型；7–13 为增量扩展（旧文件零迁移）。比较按 bit pattern。

---

## Memory Model & Scanner

**CoreBatch**（`splayed_core::batch`，引擎无关、零 Arrow）：`CoreSchema` +
`CoreColumn`（`Primitive{ty, data: Buffer}` / `Dictionary{indices, values}` /
`Varlen{offsets, data}` + `nulls: Option<Bitmap>`）+ `CoreStringDict`（SYM
共享字典）。扫描输出布局固定 `[time(0), sym(1), fields(2+)]`。

**零拷贝**：`corebatch_into_record_batch` 把数据缓冲直接交给 Arrow
（FIELD/TIME 不拷贝；字段 header 报 0 NULL 时不建 validity）。

**ScanRequest / 下推（现状）**：

| 项 | 状态 |
|---|---|
| 投影（列裁剪） | ✅ `columns` 只读/解列需要的 FIELD |
| SYM 裁剪 | ✅ `SymbolSelection::Symbols`（等值/IN）直接切 SYM INDEX 块 |
| TIME 裁剪 | ✅ `TimeRange` 半开区间二分切行段；分区表按各 `.meta` 时间元数据跳过分区 |
| 值过滤 | ✅ 8 种（`= ≠ > ≥ < ≤ IS NULL IS NOT NULL`，AND 语义），f64/i64 SIMD 快路径 |
| 统计剪裁 | ✅ footer min/max 与 filter 不相交 → 整数据集空计划 |
| 读取期限值 | ✅ `limit` 三个流入口 + `scan_all_parallel`（第 N 个通过过滤的行即停，末批 `CoreBatch::slice`） |

**并行**：`split_ranges`（目标行数切片 → 连续合并 ≤n 组）→ 每组分线程
`std::sync::mpsc` 流式，`ParallelScanBatches` 按 (sym, time) 全局保序；DataFusion
`with_scan_parallelism(n)` 把一个 dataset 切成 n 个输出分区。

---

## Integration Status（现状）

| 引擎 | 接入 | 能力 |
|---|---|---|
| DataFusion 55 | `register_splayed_table`（自动探测 dataset/分区表）、`read_splayed()`、STORED AS SPLAYED | 投影/SYM/TIME/值过滤下推、LIMIT、统计上报（num_rows/byte_size/null_count/min/max）、`ORDER BY sym, time` 免排序声明（等值属性）、单数据集多分区并行、`provider.reload()` |
| Polars 0.45 | `AnonymousScan`（`splayed_lazyframe`） | 谓词下推（`列 op 字面量` AND 链 → SYM/TIME/值过滤；未翻译部分 `lazy().filter(pred)` 兜底）、投影裁剪、`n_rows → limit`、C data interface 转换（时间列物理化导入）。已知约束：无显式 `select` 的完整收集会触发 polars 0.45 anonymous-scan 投影优化 bug（文档建议显式 `select([...])`） |
| DuckDB | IPC 导出 / 原生 `scan_to_chunks` / C ABI | DuckDB 进程内扩展壳（C++ 骨架）消费 CoreBatch 列视图；`--config "lib.crate-type=['cdylib','staticlib']"` 产出 dll/lib 供链接 |
| ADBC | `Connection`/`Statement`（SQL）→ Arrow 流 | SQL 解析/执行由 DataFusion 承担；`refresh()` 在 `update_meta` 后刷新表视图 |

---

## Development Phases（现状）

| Phase | Status | Description |
|---:|---|---|
| 1 | ✅ Done | Format: META + FIELD binary read/write, types, NULL encoding |
| 2 | ✅ Done | Reader: mmap, SYM/TIME lookup, row range, column read |
| 3 | ✅ Done | Writer: 函数接口（create_table/update_table/update_meta/字段级），generation, crash recovery |
| 4 | ✅ Done | Scanner: projection/predicate/filter pushdown, batch API, ColumnView |
| 5 | ✅ Done | Performance: parallel scan, SIMD filter, inline prefetch |
| 6 | ✅ Done | Compression: ZSTD, LZ4, DELTA, RLE, BITPACK |
| 7 | ✅ Done | Arrow: ColumnView→Arrow, NULL/NaN semantics, type mapping |
| 8 | ✅ Done | DataFusion: TableProvider 三层对接, pushdown, 表函数/排序声明 |
| 9 | ✅ Done | DuckDB: Arrow IPC bridge + 原生 DataChunk 路由 + C ABI 集成层 |
| 10 | ✅ Done | CoreBatch 内存模型：引擎无关列式（Buffer/Validity/字典列），扫描/适配零拷贝 |
| 11 | ✅ Done | 磁盘定长类型扩展（INT8/16、UINT*、DATE64）+ 新类型过滤下推 |
| 12 | ✅ Done | 并行能力：`scan_owned_parallel` 有序流式 + DataFusion 单数据集多分区 |
| 13 | ✅ Done | 适配层组件化（umbrella features）：adbc（上层 ADBC，内部 DataFusion）、duckdb C ABI、polars 惰性扫描 |
| 14 | ✅ Done | Polars AnonymousScan：谓词下推 + 列裁剪 + C data interface 转换 |
| 15 | ✅ Done | FIELD 统计 footer（min/max）→ 扫描期整数据集剪裁 + DataFusion 统计上报；ScanRequest.limit 读取期截断 |
| 16 | ✅ Done | core `update_meta`：布局重排 + 并发字段重散布 + 原子提交；DataFusion `reload` / ADBC `refresh` 联动 |

---

## License

MIT