# Splayed V1

面向金融时序数据 `SYM × TIME × FIELD` 的专用列式存储引擎：**O(1) 行定位、mmap/零拷贝读取、全预分配写入**。引擎核心（`splayed-core`）不依赖任何 Arrow；Arrow 只是被各引擎适配层复用的共享转换工具。

> 本文档只描述**当前已实现**的状态；未实现/暂缓项不在此列。

> **完整文档**：使用 MkDocs 整理，位于 [`docs/`](docs/index.md)，包含快速上手、架构设计、磁盘格式规范、Crate 参考、集成指南与开发指南。构建：`pip install mkdocs-material && mkdocs build`；GitHub Actions（`.github/workflows/`）负责文档部署与 CI。

---

## 1. 代码架构总览

```text
外部应用（SQL / DataFrame / Python）
   │
   ▼
splayed-python ──────── pyo3 扩展（`import splayed`）：pyarrow 表格交换层（C data interface）
   │
   ├── splayed-adbc ──────── 上层 ADBC 驱动（内部经 DataFusion 执行 SQL → Arrow 流）
   ├── splayed-datafusion ── DataFusion TableProvider 三层对接 + SQL 执行
   ├── splayed-polars    ── Polars AnonymousScan 惰性扫描（谓词/投影下推）
   └── splayed-duckdb    ── DuckDB 集成层（IPC 桥 / 原生 DataChunk / C ABI）
   │
splayed-arrow ──────── 共享转换工具：CoreBatch → Arrow 零拷贝（可选用）
   │
splayed-core ───────── 引擎无关核心：数据扫描与写入、CoreBatch（零 Arrow）
   │
splayed-codec ──────── 编码（PLAIN/DELTA/RLE/BITPACK）+ 压缩（NONE/ZSTD/LZ4）
   │
splayed-format ─────── 磁盘格式：META/FIELD header、类型、NULL 编码、统计 footer（无依赖）
```

### 模块职责表

| Crate | 位置 | 依赖 | 职责 |
|---|---|---|---|
| `splayed-format` | `crates/splayed-format` | 无 | 磁盘格式权威定义：META/FIELD header、`DataType`(0–13)、NULL 哨兵、`MetaFile/MetaBuilder`、统计 footer（`field_footer`） |
| `splayed-codec` | `crates/splayed-codec` | format | `compact_field`（NONE→ZSTD/LZ4）+ 编码器（PLAIN/DELTA/RLE/BITPACK） |
| `splayed-core` | `crates/splayed-core` | format + codec | **引擎核心**：mmap 读取、字段/表写入（含 `update_meta` 布局重排）、Dataset、Scanner（CoreBatch 流）、并行/限值/统计剪裁、`batch`（CoreBatch 内存模型）、子集 `.sub.xxx`（`create_subset`/`SubsetReader`） |
| `splayed-arrow` | `crates/splayed-arrow` | core | Arrow 交换：`create_meta/create_table/update_table/update_meta`（Arrow 输入）+ `scan_dataset/scan_partitioned`（→ Arrow）+ 分区写（`PartitionWriteInputArrow`）+ `corebatch_into_record_batch`（零拷贝） |
| `splayed-datafusion` | `crates/splayed-datafusion` | core + arrow + datafusion | 三层：单 dataset provider → 分区表 provider → `register_splayed_table`/`read_splayed`/STORED AS SPLAYED |
| `splayed-polars` | `crates/splayed-polars` | core + arrow + polars 0.45 | `AnonymousScan` 惰性数据源：谓词下推 + 列裁剪 + 兜底过滤 |
| `splayed-python` | `crates/splayed-python` | core + arrow(pyarrow) + pyo3 | **Python 绑定**（`import splayed`，pyo3 0.29）：镜像 `splayed-arrow` 写/读/分区/子集接口；pyarrow 表格交换层（Arrow C data interface 零拷贝） |
| `splayed-duckdb` | `crates/splayed-duckdb` | core（+可选用 arrow） | Arrow IPC 导出（feature "arrow"）、原生 `scan_to_chunks`（零 Arrow）、DuckDB 扩展 C ABI（`ffi`） |
| `splayed-adbc` | `crates/splayed-adbc` | datafusion + arrow | 上层 ADBC：`Connection`/`Statement`/Arrow 流（SQL 由 DataFusion 执行） |
| `splayed` | `crates/splayed` | 全部（features 开关） | Umbrella：`arrow`(默认)/`datafusion`/`duckdb`/`polars`/`adbc` |
| `splayed-cli` | `crates/splayed-cli` | core + arrow | CLI：init/update/compact/read/sql/export/export-arrow |
| `splayed-duckdb-extension` | `splayed-duckdb-extension/` | C++（非 cargo 成员） | DuckDB 扩展壳骨架：消费 `splayed-duckdb` C ABI（`extension.cpp`） |

依赖方向：`format ← codec ← core → arrow → {datafusion, duckdb, polars} → adbc`。
Umbrella 用法：`splayed = { path = "crates/splayed", features = ["arrow", "adbc", "duckdb", "polars"] }`。

### 磁盘形态

```text
dataset/                        一个 folder = 一个 dataset（≈ Parquet 文件）
├── .meta        # TIME AXIS + SYM DICT + SYM INDEX（generation + 原子提交）
├── .sub.hs300   # 行区间子集索引（父网格上的成分股视图，可选）
├── close        # FIELD: [64B header][data…][28B stats footer(可选)]
├── open
└── ...
表 = 一堆 dataset 目录（≈ Hive 分区表，分区按时间/目录裁剪）
```

---

## 2. splayed-core（引擎核心）

### 2.1 接口汇总

| 分组 | 接口 | 职责 |
|---|---|---|
| 打开/元数据 | `open_dataset(dir) -> Dataset` | 打开 dataset（读 `.meta`）；`Dataset{dir, meta}` |
| | `Dataset::meta_path/field_path/list_fields/existing_fields` | 目录与字段元数据 |
| 字段读取 | `FieldReader::open(path)` | mmap（NONE）或解压（ZSTD/LZ4）打开字段 |
| | `data_type/row_count/compression/header` | 头部元数据 |
| | `read_row(row)` / `read_range_raw(start, count)` | 单行 / 零拷贝行段 |
| | `as_column_view()/column_view_range()` | 遗留列视图（兼容保留） |
| | `stats() -> Option<FieldStats>` | footer min/max（无 footer/失效 → None） |
| 字段写入 | `create_field(path, dt)` | 预分配全 NULL 字段 |
| | `create_field_with_data(path, dt, values)` | 建字段 + 一次写值 + 统计 footer |
| | `create_field_with_data_encoded(path, dt, values, encoding, compression)` | 建字段 + 一次写值，**数据区直接编码/压缩**（写后只读） |
| | `update_field(path, &[UpdateItem])` | 原地写；**统计增量维护**：null_count 精确、min/max 保守上界（O(k)；命中旧极值才重扫） |
| | `delete_field(path)` | 删除字段（幂等） |
| 表写入（原生） | `create_meta(dir, time_type, sym, time)` | 仅建 `.meta` |
| | `create_table(dir, time_type, sym, time, columns, sorted)` | 一步建表（`sorted=true` 走 O(n) 快路径） |
| | `create_table_with_options(..., opts: FieldWriteOptions)` | 同上，新字段直接编码/压缩（写后只读） |
| | `update_table(dir, sym, time, columns, create_missing_fields)` | 按已有格子原地更新 |
| | `update_table_with_options(..., create_missing_fields, opts)` | 同上；**新创建**字段直接编码/压缩（既有字段仍可写） |
| | `update_meta(dir, time_type, sym, time)` | **布局重排**：重建 `.meta` + 并发重散布全部 FIELD（见 2.4） |
| 子集（`.sub.xxx`） | `create_subset(dir, name, &[SubsetInput])` | 建父 `.meta` 网格的行区间子集索引（每 SYM 可多条不连续区间；原子落盘） |
| | `SubsetReader::open / open_with_parent` | 读子集；`open_with_parent` 校验父 generation/time_type（`StaleParent` 检测） |
| | `symbols / contains / ranges / total_rows` | 子集元数据 |
| | `iter_ranges / iter_entries(&MetaFile)` | 父全局行序迭代（区间 / `(sym, time, row)` 三元组） |
| | `read_field_values(&FieldReader)` | 某字段在子集内的值（父行序拼接，`total_rows × sizeof(type)` 字节） |
| 扫描 | `Scanner::new(&Dataset)` / `plan(&ScanRequest)` | 规划：SYM 剪裁 + TIME 剪裁 + 统计剪裁 → `ScanPlan` |
| | `scan(&plan, &req) -> ScanBatches` | 借用式 CoreBatch 迭代 |
| | `scan_owned(...) / scan_owned_parallel(..., n)` | 自有 / 有序并行流 |
| | `scan_all_parallel(...)` | 全收集（并行，支持 limit） |
| | `split_ranges(plan, n)` | 行均衡切片（并行分组） |
| 分区表 | `PartitionedTable::open(dir)` | 发现（单 dataset 兼容）+ schema 合并校验 + 符号并集 + **key=value 目录名解析为声明式分区列**（`PartitionColumn`，Int64/String） |
| | `plan(&PartitionScanRequest)` | 四层剪裁：TIME（分区时间轴）/ 符号（分区缺失剔除）/ 统计（footer min-max 不相交→跳过分区）/ **分区列**（`partition_filters` 与 declared 值不相交→跳过） |
| | `scan(&plan, &req) -> PartitionScanBatches` | 按分区名升序流式合并 CoreBatch（DataFusion / DuckDB 共用此实现） |
| | `create_partitioned_table(root, tt, &[PartitionWriteInput])` | 一次建整表（root + N 分区，并发 create_table；schema/命名风格预检） |
| | `append_partition(root, input)` | 追加分区（校验与既有 schema/命名/分区列一致；重复→`PartitionExists`） |
| | `drop_partition(root, name)` | 删除分区目录（不存在→`PartitionNotFound`） |
| | `update_partition_table(root, sym, time, columns, create_missing, target?)` | **表级格子写入**：跨分区路由（存在性自动定位或显式 target；无命中→`SymTimeNotFound`）后逐分区 `update_table` |
| | `update_partition_meta(root, &[PartitionWriteInput])` | **表级布局重排**：既有分区 `update_meta`（gather 重散布）+ 新增 `create_table` + 移除未保留分区（并发） |
| 请求类型 | `ScanRequest{columns, symbols, time_range, filters, batch_size, parallelism, limit}` | 一次扫描的全部下推条件 |
| | `SymbolSelection::All \| Symbols` / `TimeRange::new`, `Filter`(8 种) / `FilterValue`(全定长类型) | 下推值类型 |
| 内存模型 | `CoreBatch/CoreColumn/CoreSchema/CoreType/CoreStringDict/Buffer/Bitmap` | 引擎无关列式表示（见 2.5） |
| 压缩 | `compact_field(path, compression)` / `decompress_field_data(path)` | 压缩为只读 / 解压（codec 透传） |
| 错误 | `DatasetError/ReaderError/ScannerError/CreateFieldError/UpdateError/DeleteFieldError/TableError/CompactError` | 分层错误类型 |

### 2.2 `FieldReader`（mmap / 解压 / 统计）

- **流程**：打开文件 → mmap → 校验 header → 尾部检测统计 footer（magic `"SFTF"`，28 字节）→ `NONE` 走 mmap 零拷贝（数据区 = `[header, data)`，footer 前），压缩则解压 payload（排除 footer）后入内存缓冲。
- **依赖**：`splayed-format`（header/类型/哨兵/footer 解析）、`memmap2`、`zstd`/`lz4_flex`。
- **效果**：`read_range_raw` 对 `NONE` 是 mmap 切片零拷贝；`stats()` 供扫描期整表剪裁与 DataFusion `statistics()` 上报。

### 2.3 表写入：`create_table` / `update_table` / `update_meta`

- **入参**：`sym`/`time` 为**逐行对齐**数组（`sym[i] ↔ time[i]`）；`TableColumn{name, data_type, values}` 为输入行序原始 LE 字节。
- **create_table 流程**：`MetaBuilder` 区分 (SYM, TIME) → 全局行序 → 每个 FIELD 按全局行缓冲（缺失时间点填 NULL 哨兵）→ `create_field_with_data` 一次写入（含统计 footer）；`sorted=true` 且输入已按 (SYM,TIME) 升序时走 O(n) 窗口游标快路径（未排序自动回退，结果恒正确；非排序路径用符号 HashMap + 块内时间二分免逐行字符串二分）。
- **update_table 流程**：逐 (SYM, TIME) 定位全局行（符号经一次 HashMap + 块内时间二分）→ 连续行合并为 `UpdateItem` → `update_field` 原地写；未知位置/缺失字段/类型不匹配报错。
- **编码写**：`create_table_with_options` / `update_table_with_options` / `create_field_with_data_encoded` 传入 `FieldWriteOptions{encoding, compression}`（默认 `PLAIN+NONE` 可写）→ 新字段**一次落盘即编码/压缩**（写后只读，布局同 `compact_field_with_encoding`）。
- **update_meta 流程**：① 读旧 meta → ② `MetaBuilder` 建新布局（generation+1）→ ③ 构建 **gather 映射**（新行 (sym,time) → 旧行号，旧 meta 符号哈希 + 区间二分；旧数据没有 → NULL）→ ④ 写 `.meta.new`（fsync 不改名）→ ⑤ **每字段一线程**并发重写 `name.tmp`（新 generation、真实 `null_count`、重算统计 footer；gather 连续段一次 memcpy）→ fsync + rename → ⑥ 最后 rename `.meta.new → .meta`（提交点）。
- **原子性**：提交点之前，任何 Reader 因 FIELD/META **generation 不匹配而拒绝**；中途失败重跑本函数即完成提交（重散布确定性、幂等）。
- **依赖**：`std::thread::scope`（字段级并行）、`field_footer::compute_stats/encode_footer`。
- **效果**：`DataFusion::SplayedDatasetProvider::reload()` / ADBC `Connection::refresh()` 之后上层立即看到新布局。

### 2.4 Scanner 扫描管线

- **`plan(&ScanRequest) -> ScanPlan{ranges, columns, total_rows}`**（三层剪裁）：
  1. **SYM 剪裁**：`SymbolSelection::Symbols` 只保留命中的 SYM INDEX 记录；
  2. **TIME 剪裁**：`TimeRange` 半开区间在各 SYM 的 time_axis 区间内二分 → 行段；
  3. **统计剪裁**：任一值 filter 与该列 footer `[min,max]` 不相交（如 `close > 204` 且 max=204）→ 整个计划 `ranges=[]`（任何 FIELD 都不读）。
- **执行**：`scan`/`scan_owned`/`scan_owned_parallel` 逐批产出 `CoreBatch`（布局 `[time(0), sym(1), fields(2+)]`）；值过滤（8 种，AND 语义）在解码后按行应用，f64/i64 有 SIMD 快路径；`limit` 在**第 N 个通过过滤的行**处截断（末批 `CoreBatch::slice`，不再拉取后续批次）。
- **并行**：`split_ranges` 把范围按目标行数切片并连续合并成 ≤n 组 → 每组一个 `std::thread` + `sync_channel(2)` 流式，`ParallelScanBatches` 按 (sym, time) 全局保序。
- **依赖**：`FieldReader`（mmap/解压）、`batch`（CoreBatch 构造）、`field_footer`（统计剪裁）。
- **效果**：上层的投影/SYM/TIME/值过滤四类下推 + 统计剪裁 + 限值全部在读取期生效。

### 2.5 CoreBatch 内存模型

- `CoreType`（含 Varlen/List/Struct/Dictionary/时间单位——仅内存）/ `CoreSchema` / `CoreField`；
- `CoreColumn`：`Primitive{ty, data: Buffer}` / `Dictionary{indices(u32), values: Arc<CoreStringDict>}` / `Varlen{offsets, data}` + `nulls: Option<Bitmap>`（无 NULL 时 None）；
- `Buffer::take()`（零拷贝移交）、`Bitmap::slice(offset,len)`、`CoreBatch::slice(offset,len)`（limit/窗口截断）；
- **去向**：`splayed_arrow::corebatch_into_record_batch` 把缓冲直接交给 Arrow（FIELD/TIME 不拷贝、0 NULL 列不产 validity）；`splayed-duckdb` 原生路由/`ffi` 直接消费原始字节 + validity。

### 2.6 子集（`.sub.xxx`）

`.sub.{name}` 是父 `.meta` 网格的**行区间子集索引**（如 `.sub.hs300` = 沪深300 成分股在父表上的时间区间视图），与 `.meta` 同目录并存、以 `.` 开头故不被当作字段（详见 [SUBSET 格式](docs/format/subset-format.md)）。

- **写**：`create_subset(dir, name, &[SubsetInput{sym, segments: Vec<(time, count)>}])`——每段在校验后定位到父 (SYM, TIME) 行区间，相邻/重叠区间自动合并；原子落盘（`.sub.{name}.tmp` → fsync → rename）。与 `.meta` 唯一的结构差异：**一个 SYM 可对应多条连续区间**（`.meta` 的 SYM INDEX 恒为单条）。
- **读**：`SubsetReader::open(dir, name)` 直接读索引；`open_with_parent(dir, name, &meta)` 额外校验父 `generation`/`time_type`（父表 `update_meta` 后旧子集报 `StaleParent`，避免错位）。
- **取数**：`read_field_values(&FieldReader)` 按父全局行序拼接目标字段各区间值 → 得到子集视角的一列（`total_rows × sizeof(type)` 字节）；`iter_entries(&MetaFile)` 展开为 `(sym, time, row)` 三元组。

---

## 3. splayed-format（磁盘格式）

| 接口 | 说明 |
|---|---|
| `DataType`（ID 0–13） | BOOL/INT32/64/FLOAT32/64/DATE32/TIMESTAMP_US + 扩展 INT8/16、UINT8/16/32/64、DATE64；`size_of/null_bytes/as_str/from_id` |
| `RawValue` | 定长值：`read_le/write_le`、全类型构造/访问器、`null_bytes_slice` |
| `fill_null` | 按类型填 NULL 哨兵 |
| `MetaFile / MetaBuilder / SymIndexRecord` | meta 序列化/反序列化；builder 按 (sym,time) 建 TIME AXIS/SYM DICT/SYM INDEX；`total_rows` |
| `FieldHeader / MetaHeader / TimeType / Compression / Encoding` + 常量 | 64B 头结构、magic、`HEADER_SIZE/DATA_OFFSET` |
| `field::row_byte_offset/data_length/field_file_size/validate_field_header/new_plain_field_header` | 偏移与长度计算、头部校验 |
| `field_footer::{FOOTER_SIZE, compute_stats, encode_footer, parse_footer}` | 统计 footer：min/max 槽（跳过 NULL 哨兵）、编码/解析 |

**关键格式事实**：META 用 `.meta.new → fsync → rename` 原子提交；FIELD `data_length` = 纯数据字节；压缩布局 `[64B head][u64 len][payload][28B footer]`；NULL = 哨兵位型（浮点 canonical NaN、有符号 INT_MIN、无符号全 1、BOOL `0x02`）。

## 4. splayed-codec（编码/压缩）

| 接口 | 说明 |
|---|---|
| `compact_field(path, compression)` | NONE→ZSTD/LZ4 压缩（`[u64 len][payload]`），成功后只读；**重算统计 footer**（压缩后统计永久有效） |
| `compact_field_with_encoding(path, encoding, compression)` | **编码接线**：DELTA/RLE/BITPACK 编码（`[u64 编码长][payload]`）后可选 ZSTD/LZ4；只读；读侧按 header.encoding 自动解码恢复原始布局（统计 footer 保留） |
| `decompress_field_data(path)` | 压缩字段解压为原始字节 |
| `PlainCodec` / `delta` / `rle` / `bitpack` 模块 | 编码器（按需暴露） |
| `CompactError / DecompressError / CodecError` | 错误类型 |

## 5. splayed-arrow（Arrow 交换 + 零拷贝）

| 接口 | 说明 |
|---|---|
| `create_meta(folder, data, sorted)` | 由 Arrow RecordBatch（TIME+SYM）建 `.meta` |
| `create_table(folder, data, sorted)` / `update_table(folder, data, sorted, create_missing_fields)` | 一步建表 / 格子更新（Arrow 输入 → 原生 core） |
| `create_table_with_options(folder, data, sorted, FieldWriteOptions)` / `update_table_with_options(...)` | 同上，新字段直接编码/压缩（写后只读） |
| `update_meta(folder, time_type, data, sorted)` | Arrow 输入布局重排（透传 `core::update_meta`） |
| `scan_dataset(dir, data, columns, n_parallel, batch_rows, limit) -> Vec<RecordBatch>` | **整数据集 → Arrow**：全列/列子集扫描，可选并行与 limit（便捷入口） |
| `scan_partitioned(root, data, columns, ...) -> Vec<RecordBatch>` | **分区表 → Arrow**：key=value 分区列自动加回结果（同上便捷入口） |
| `create_partitioned_table(root, tt, &[PartitionWriteInputArrow])` / `append_partition` / `drop_partition` | **分区写**（Arrow 输入）：一次建整表 / 追加 / 删除 |
| `update_partition_table(root, sym, time, columns, create_missing, target?)` / `update_partition_meta(root, &[PartitionWriteInputArrow])` | 表级格子写入 / 表级布局重排（Arrow 输入） |
| `corebatch_into_record_batch(batch, indices, fields, sym_dict)` | **零拷贝**：CoreBatch 缓冲移交 Arrow（indices 须升序；字典→Utf8 展开；BOOL 位压缩） |
| `corebatch_to_record_batch(...)` | 同名（借用版） |
| `splayed_to_arrow_type / arrow_to_splayed_type / arrow_time_type / arrow_value_to_raw` | 类型/值映射（含扩展类型） |
| `column_view_to_arrow` | 遗留 ColumnView → Arrow（兼容保留） |

## 6. splayed-datafusion（三层对接）

| 接口 | 说明 |
|---|---|
| `SplayedDatasetProvider::new(dir)` | **Layer 1**：单 dataset provider；`with_scan_parallelism(n)`（单数据集多输出分区）、**`reload()`**（`update_meta` 后刷新）、`statistics()`（num_rows/byte_size/null_count/min/max 上报） |
| `SplayedTableProvider::new(dir)` | **Layer 2**：分区表（基于 `core::partition` 发现/剪裁；每个子目录一个分区，时间/符号/统计剪裁，schema 合并校验） |
| `SplayedStatsAggRule` / `with_splayed_optimizer_rules(builder)` | **聚合下推**：无过滤的 MIN/MAX/COUNT 命中列统计（footer min/max + null_count）→ 单行常量计划，整扫描跳过（`SessionStateBuilder` 挂规则） |
| `register_splayed_table(ctx, name, dir)` / `auto_provider(dir)` | **Layer 3**：自动探测 dataset/分区表并注册 |
| `SplayedTableFunction` | `read_splayed('dir')` 表函数（`register_udtf`） |
| `SplayedSubsetFunction` | `read_splayed_subset('dir', 'hs300')` 表函数：子集 `.sub.hs300` → 内存表（物化视图，`register_udtf`） |
| `SplayedTableFactory` | `CREATE EXTERNAL TABLE ... STORED AS SPLAYED LOCATION 'dir'` |

**下推现状**：投影 / SYM 等值 / TIME 范围 / 值过滤（`classify` 三分类：Exact/Inexact/NotPushdown，8 种 Filter）全透传到 core；`LIMIT` → `ScanRequest.limit` 读取期截断；`ORDER BY sym, time` 通过单调等值属性免排序。

## 7. splayed-polars（惰性扫描）

| 接口 | 说明 |
|---|---|
| `splayed_lazyframe(dir) -> LazyFrame` | 注册 `AnonymousScan` 惰性数据源（`LazyFrame::anonymous_scan`） |
| `splayed_lazyframe_table(dir) -> LazyFrame` | **分区表**惰性数据源：复用 `core::partition` 剪裁/合并；key=value 分区列进 schema + 常量列 |
| `splayed_lazyframe_subset(dir, sub_name) -> LazyFrame` | 子集 `.sub.{name}` → 惰性帧（经 `splayed_arrow::read_subset` 物化视图） |
| `SplayedScan` | `AnonymousScan` 实现：`allows_predicate/projection_pushdown=true` |
| `predicate::translate(expr, dtype_of)` | polars `Expr`（`列 op 字面量` AND 链）→ core `SymbolSelection/TimeRange/Filter` |
| `arrowconv::{to_polars_dtype, to_polars_arrow_dtype, record_batch_to_dataframe}` | arrow-rs → polars：C data interface（时间列物理化），字符串按值构造 |

**流程**：`n_rows`/`with_columns`/谓词 → core 扫描（投影 + SYM/TIME/值过滤 + limit）→ CoreBatch → Arrow（零拷贝）→ polars DataFrame；未翻译的复杂谓词用 `df.lazy().filter(pred).collect()` 兜底（结果恒正确，下推只是裁剪）。
**已知约束**（polars 0.45）：无显式 `select` 的完整收集会触发其 anonymous-scan 投影优化 bug——链上显式 `select([...])`。

## 8. splayed-duckdb（DuckDB 集成层）

| 接口 | 说明 |
|---|---|
| `export_to_arrow_ipc(dir, out.arrow)`（feature "arrow"） | Splayed → Arrow IPC 文件（DuckDB `read_arrow`） |
| `build_arrow_schema(dataset)` | 导出 schema（time/sym/fields） |
| `native::scan_to_chunks(dataset, req, n, sink)` | **零 Arrow**：CoreBatch 流直出（扩展层逐批消费，数据/validity 与 DuckDB `Vector` 布局同构） |
| `native::scan_table_to_chunks(dir, preq, n, sink)` | **零 Arrow 分区表**：走 `core::partition`（与 DataFusion 共用剪裁/合并） |
| `ffi::splayed_dataset_open/close`、`splayed_scan_open/next/close`、`splayed_scan_dict_value`、`splayed_last_error` | **C ABI**：句柄=裸指针（谁创建谁释放）、扁平列视图（`SplayedColumnFFI{kind, type_id, data, data_len, validity, dict_count}`，SYM 字典串经 dict_value 取回）、错误消息线程本地 |

**产物**：`cargo build -p splayed-duckdb --release --config "lib.crate-type=['cdylib','staticlib']"` → dll/import-lib/static-lib；引擎内扩展由 `splayed-duckdb-extension`（C++ 壳骨架）消费。

## 9. splayed-python（Python 绑定，pyo3）

pyo3 0.29 扩展模块 `splayed`（`import splayed`），镜像 `splayed-arrow` 接口，以 **pyarrow** 作为表格交换层（Arrow C data interface，零拷贝）。

| 接口 | 说明 |
|---|---|
| `create_meta(dir, t)` / `create_table(dir, t, sorted=True)` / `create_table_with_options(dir, t, sorted, encoding, compression)` | 写：`t` 为 `pyarrow.RecordBatch`（TIME + SYM + 字段列） |
| `update_table(dir, t, create_missing_fields=False)` / `update_table_with_options(...)` / `update_meta(dir, t)` | 原地更新 / 新增字段 / 布局重排 |
| `create_partitioned_table(root, time_type, partitions)` / `append_partition(root, p)` / `update_partition_table(root, t, ...)` / `update_partition_meta(root, ps)` / `drop_partition(root, name)` | 分区写（`PartitionWriteInput(name, table, sorted)`） |
| `create_subset(dir, name, inputs)` | 子集写（`SubsetInput(sym, [(time, count), ...])`） |
| `scan_dataset(dir, columns, symbols, time_range, batch_size, parallelism, limit)` | → `list[pyarrow.RecordBatch]`（列 = time/sym/...columns） |
| `scan_partitioned(dir, ...)` | 分区表扫描（按分区名升序合并） |
| `read_subset(dir, name, columns=None)` | 子集 `.sub.{name}` → `pyarrow.RecordBatch`（父全局行序） |

**错误**：所有 splayed 侧错误抛 `splayed.SplayedError`（`RuntimeError` 子类）。
**构建**：`pwsh -File crates/splayed-python/build_pyd.ps1` → 产物 `crates/splayed-python/splayed.pyd`。验证：`python example/demo_splayed_python.py`。详见 [docs/crates/python.md](docs/crates/python.md)。

## 10. splayed-adbc（上层 ADBC 驱动）

| 接口 | 说明 |
|---|---|
| `Connection::open(dir)` / `open_dataset(dir)` | 打开并注册 `splayed` 表（内部 DataFusion `SessionContext` + tokio runtime） |
| `Connection::refresh()` | `update_meta` 后重新注册新 provider（先注销再注册） |
| `Connection::statement(sql)` / `execute(sql)` | 执行 SQL → `Vec<RecordBatch>` |
| `Statement::with_sql/execute/execute_all` | 流式 / 全量执行 |
| `ArrowRecordBatchStream` | 同步 `Iterator<Item=Result<RecordBatch>>`（内部 block_on 驱动 DataFusion 流） |

**定位**：不复实现 SQL 解析——解析/优化/执行全部由 DataFusion 承担；DuckDB 可作平替后端。

## 11. splayed（Umbrella）

`pub use` 各 crate：`codec/core/format` 恒有；`arrow`（默认 feature）、`datafusion`、`duckdb`、`polars`、`adbc` 按 features 开启。

## 12. splayed-cli

`init` / `update` / `compact` / `read` / `sql` / `export` / `export-arrow`（基于 core + arrow）。

---

## 13. Development Phases（现状）

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
| 17 | ✅ Done | 可写压缩字段：`FieldWriteOptions`（编码+压缩）+ `create_field_with_data_encoded` / `*_with_options`（写后只读） |
| 18 | ✅ Done | 子集索引 `.sub.{name}`：`create_subset` / `SubsetReader`（每 SYM 多区间；父 generation 失效检测） |
| 19 | ✅ Done | splayed-python：pyo3 扩展 `splayed`（pyarrow 表格交换层）——写/读/分区/子集接口镜像 `splayed-arrow` |

---

## License

MIT