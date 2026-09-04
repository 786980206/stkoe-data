# Splayed V1

面向金融时序数据 **SYM × TIME × FIELD** 的专用列式存储引擎：**O(1) 行定位、mmap/零拷贝读取、全预分配写入**。引擎核心（`splayed-core`）不依赖任何 Arrow；Arrow 只是被各引擎适配层复用的共享转换工具。

> 本文档只描述**当前已实现**的状态；未实现/暂缓项不在此列。

## 核心特性

| 特性 | 说明 |
| --- | --- |
| O(1) 行定位 | META 索引直接给出目标 row range，无扫描 |
| mmap / 零拷贝 | PLAIN + NONE 路径直接指针切片，不复制 |
| 全预分配写入 | 一个 FIELD 一个文件，全量预分配 + 原地更新，不重整文件 |
| 分层生态对接 | Arrow 交换层 → DataFusion / DuckDB / Polars / ADBC |
| 下推完备 | 投影 / SYM / TIME / 值过滤 / LIMIT / 统计剪裁 / 分区剪裁 |
| 只读压缩 | ZSTD / LZ4 压缩 + DELTA / RLE / BITPACK 编码，压缩后只读 |

## 架构一图流

```text
外部应用（SQL / DataFrame）
   │
   ▼
splayed-adbc ──────── 上层 ADBC 驱动（内部经 DataFusion 执行 SQL → Arrow 流）
   │
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

## 文档导航

- [快速上手](getting-started.md) —— 构建、CLI、Python 示例
- [架构总览](architecture.md) —— 分层组件、模块职责、设计决策
- [磁盘格式规范](format/index.md) —— META / FIELD / 类型 / 编码 / 统计 footer（权威）
- [Crate 参考](crates/index.md) —— 各 crate 公开接口一览
- [集成指南](integrations/datafusion.md) —— DataFusion / DuckDB / Polars / ADBC 用法
- [开发指南](development/build-test.md) —— 构建与测试、开发流程、阶段状态

## 磁盘形态

```text
dataset/                        一个 folder = 一个 dataset（≈ Parquet 文件）
├── .meta        # TIME AXIS + SYM DICT + SYM INDEX（generation + 原子提交）
├── close        # FIELD: [64B header][data…][28B stats footer(可选)]
├── open
└── ...
表 = 一堆 dataset 目录（≈ Hive 分区表，分区按时间/目录裁剪）
```

## 关键设计决策

1. **全预声明**：`time_count` = 行容量，SYM INDEX 无独立 `row_capacity` 字段。
2. **NULL 走哨兵位型**：无 validity bitmap，比较按 bit pattern。
3. **原地更新**：`create_field` 预分配全 NULL，`update_field` 原地覆盖，不扩展文件。
4. **压缩即只读**：`compression != NONE` 后拒绝更新。
5. **Generation**：u64 严格单调，META/FIELD 不匹配即拒绝。
6. **原子 META 提交**：`.meta.new` → fsync → rename。

## License

MIT
