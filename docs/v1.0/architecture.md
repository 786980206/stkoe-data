# 架构总览

## 分层组件（非并列）

组件按**层次**组织：引擎无关的核心在最底，ADBC 驱动在最上。

```text
外部应用
  │
  ▼
splayed-adbc          上层 ADBC 驱动：内部经 DataFusion 执行 SQL → Arrow 结果
  │（调用执行 SQL；DuckDB 可作平替后端）
  ├▶ splayed-datafusion   DataFusion TableProvider（查询自定义格式）+ SQL 执行
  └▶ splayed-duckdb       DuckDB 扩展 / Arrow IPC 桥
  │
splayed-core           引擎无关核心：数据扫描与写入（Scanner / CoreBatch / Filter）
  ▲
  │ 共享转换工具（可选用）
splayed-arrow          CoreBatch → Arrow 零拷贝转换
```

## 模块职责表

| Crate | 位置 | 依赖 | 职责 |
|---|---|---|---|
| `splayed-format` | `crates/splayed-format` | 无 | 磁盘格式权威定义：META/FIELD header、`DataType`(0–13)、NULL 哨兵、`MetaFile/MetaBuilder`、统计 footer（`field_footer`） |
| `splayed-codec` | `crates/splayed-codec` | format | `compact_field`（NONE→ZSTD/LZ4）+ `compact_field_with_encoding`（DELTA/RLE/BITPACK）+ 编码器 |
| `splayed-core` | `crates/splayed-core` | format + codec | **引擎核心**：mmap 读取、字段/表写入（含 `update_meta` 布局重排）、Dataset、Scanner（CoreBatch 流）、并行/限值/统计剪裁、`batch`（CoreBatch 内存模型）、`partition`（分区管理） |
| `splayed-arrow` | `crates/splayed-arrow` | core | Arrow 交换：`create_meta/create_table/update_table`（Arrow 输入）+ `corebatch_into_record_batch`（零拷贝） |
| `splayed-datafusion` | `crates/splayed-datafusion` | core + arrow + datafusion | 三层：单 dataset provider → 分区表 provider → `register_splayed_table`/`read_splayed`/STORED AS SPLAYED；聚合下推规则 |
| `splayed-polars` | `crates/splayed-polars` | core + arrow + polars 0.45 | `AnonymousScan` 惰性数据源：谓词下推 + 列裁剪 + 兜底过滤 |
| `splayed-duckdb` | `crates/splayed-duckdb` | core（+可选用 arrow） | Arrow IPC 导出（feature "arrow"）、原生 `scan_to_chunks`（零 Arrow）、DuckDB 扩展 C ABI（`ffi`） |
| `splayed-adbc` | `crates/splayed-adbc` | datafusion + arrow | 上层 ADBC：`Connection`/`Statement`/Arrow 流（SQL 由 DataFusion 执行） |
| `splayed` | `crates/splayed` | 全部（features 开关） | Umbrella：`arrow`(默认)/`datafusion`/`duckdb`/`polars`/`adbc` |
| `splayed-cli` | `crates/splayed-cli` | core + arrow | CLI：init/update/compact/read/sql/export/export-arrow |
| `splayed-duckdb-extension` | `splayed-duckdb-extension/` | C++（非 cargo 成员） | DuckDB 扩展壳骨架：消费 `splayed-duckdb` C ABI（`extension.cpp`） |

**依赖方向**：`format ← codec ← core → arrow → {datafusion, duckdb, polars} → adbc`。

**依赖不变式**：`splayed-core` 必须**永不依赖 Arrow**。Arrow 相关函数在 `splayed-arrow`；引擎绑定各自成适配层。

## 磁盘形态

```text
dataset/                        一个 folder = 一个 dataset（≈ Parquet 文件）
├── .meta        # TIME AXIS + SYM DICT + SYM INDEX（generation + 原子提交）
├── close        # FIELD: [64B header][data…][28B stats footer(可选)]
├── open
└── ...
表 = 一堆 dataset 目录（≈ Hive 分区表，分区按时间/目录裁剪）
```

## 设计决策要点

| 原则 | 决策 | 理由 |
| --- | --- | --- |
| 数据模型 | `SYM × TIME × FIELD` | 金融时序的自然建模 |
| 数据布局 | Splayed，一个 FIELD 一个文件 | 列级独立读/并行，投影裁剪天然成立 |
| TIME 类型 | 仅 `DATE32` / `TIMESTAMP_US` | 二值简化 |
| FIELD | 只存固定宽度类型 | O(1) 行定位与零拷贝 |
| STRING | 不支持（磁盘 FIELD） | 破坏固定宽度布局 |
| NULL bitmap | 不做 | 用类型内置特殊值编码 NULL |
| 压缩 | 可选 | NONE 最快路径，ZSTD/LZ4 冷数据 |
| Arrow | 交换层，不作为底层格式 | 解耦底层布局 |

## 最重要的边界

> **META 决定「读哪里」，FIELD 决定「读什么」。**

任何上层（DataFusion / DuckDB / 自定义 SQL）都必须通过 Scanner 接入，不直接理解 META/FIELD 的内部布局。

## 性能原则（8 条规则）

1. **META 负责定位，FIELD 只负责数据。**
2. **FIELD 永远保持连续，不做逻辑 Block。**
3. **不存 STRING，不存 FIELD Index，不存 Block Index。**
4. **SYM/TIME 是 Scanner 的一等过滤条件。**
5. **PLAIN + NONE 是最高性能路径，直接 mmap。**
6. **Arrow 是交换层，不是底层存储格式。**
7. **读并行化在 FIELD / SYM / range 层面，写并行化在 encode/compress 层面。**
8. **DataFusion/DuckDB 通过 Scanner 接入，而不是让它们理解 META/FIELD。**

## 并行能力

- **core**：`scan_all_parallel`（按 `parallelism` 分块 + 线程）与 `scan_owned_parallel`（有序**流式**并行：`split_ranges` 按行数拆分/连续打包成 ≤n 组保序切片，每组一个 `std::thread` 生产者 + 有界通道，消费按组序取回，与串行扫描逐行等价）。
- **DataFusion**：`SplayedDatasetProvider::with_scan_parallelism(n)` / `SplayedTableProvider::with_scan_parallelism(n)`（默认 1）把单 dataset 扫描切成 n 个行均衡、保序的 output partition，由 DataFusion 多线程 runtime 并行执行。
- **DuckDB**：`ScanRequest.parallelism` 直接对应 DuckDB 线程数。
- **写**：`update_meta` 布局重排时每 FIELD 一线程并发重写；`create_partitioned_table` 并发 `create_table`。
