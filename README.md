# Splayed V2.0

Rust 列式存储引擎：`SYM × TIME × FIELD` 金融时序数据。容量网格逻辑行空间、validity bitmap NULL 语义、PLAIN/DELTA/RLE/BITPACK + ZSTD/LZ4 chunk 压缩、零拷贝 `ColumnView`/`DataView` 交换结构。

> V1.0 归档于 git 分支（快照 `f572500`，其完整 README 随分支保留）。V2.0 为重写实现，本 README 只做导航。

## 文档（权威）

| 文档 | 内容 |
| --- | --- |
| [docs/splayed-format.md](docs/splayed-format.md) | 磁盘格式：META/FIELD header、SYM INDEX（连续子区间原则）、内存数据模型、NULL 语义、原子提交 |
| [docs/splayed-core.md](docs/splayed-core.md) | core 概览 + 实现原则摘要 |
| [docs/core/index.md](docs/core/index.md) | 公共语义：命名规则、RowRange / ScanRequest / Predicate、Handle 对象 |
| [docs/core/field.md](docs/core/field.md) | Field 层 API 与内部实现流程（create/open/read/write/scan/close/cast/compress/decompress） |
| [docs/core/meta.md](docs/core/meta.md) | META 层 API 与内部实现流程（MetaBuilder、index 读取/扫描/定位） |
| [docs/core/dataset.md](docs/core/dataset.md) | Dataset 层 API 与内部实现流程 |
| [docs/benchmark.md](docs/benchmark.md) | 基准方法论与结果（Splayed vs Parquet / DuckDB / Polars） |
| [docs/splayed-table.md](docs/splayed-table.md) | 表层设计（Partition = Dataset 1:1、Hive 式分区） |
| [docs/splayed-arrow.md](docs/splayed-arrow.md) | Arrow 类型映射与零拷贝边界 |
| [docs/splayed-codec.md](docs/splayed-codec.md) | 编码/压缩设计与 chunk 布局 |

## Workspace（V2 活跃成员）

```
crates/
  splayed-format/   # 零依赖：格式定义 + 内存数据模型（Buffer/Bitmap/Column/DataView）
  splayed-codec/    # PLAIN/DELTA/RLE/BITPACK + ZSTD/LZ4，chunk 编解码
  splayed-core/     # Field / META / Dataset 三层 API + Scanner（不依赖 Arrow）
  splayed-table/    # 表层 API（分区表、query/write/结构操作）
  splayed-arrow/    # Arrow 类型映射与零拷贝转换
  splayed-polars/   # Polars AnonymousScan 惰性扫描
```

V1 遗留 crate（umbrella / adbc / datafusion / duckdb / cli / python）在 `Cargo.toml` exclude 中，待模块对齐阶段回归。

## Build & Test

```bash
cargo build   # 零 warning
cargo test    # 全部通过
```

Windows 下测试建议：`cargo test --workspace --config "profile.test.debug=false"`（规避 MSVC PDB 并发抖动）。

## 状态

- format / codec / core / table / arrow / polars 实现 + 对齐审查 + 性能优化批次：完成。
- splayed-duckdb（IPC 桥）/ splayed-adbc（DataFusion provider）模块对齐：进行中。
- Benchmark 复测（首轮基线后的优化批次未计入）：待统一执行。
