# 适配层设计（V2.0）：Polars / DuckDB / ADBC

本文档定义三个引擎适配层的能力对齐方案（对照 V1.0），作为实现前置的权威设计。
依赖方向：`format ← codec ← core ← table ← arrow ← {polars | duckdb | adbc}`。

## 1. splayed-polars（AnonymousScan 惰性扫描）

### API

```rust
pub struct SplayedTable { root: PathBuf }          // 扫描对象包装
impl AnonymousScan for SplayedTable { ... }
pub fn scan_polars(path: impl AsRef<Path>) -> PolarsResult<LazyFrame>   // 入口
```

### 能力对齐（V1.0 → V2.0）

| V1.0 | V2.0 |
| --- | --- |
| AnonymousScan + 谓词/列裁剪 | 同（via `AnonymousScanOptions`） |
| 分区表惰性扫描（分区列过滤/常量列） | `scan_polars` 内部做分区发现 + time 条件 pruning |
| arrowconv（arrow-rs ↔ polars-arrow） | v1 为**拷贝级**转换（列→Series），零拷贝依赖 polars-arrow 桥（优化项） |

### 语义

- `schema`：由 `read_table_schema()` 提供（最后 Partition，含 sym/time）。
- 谓词：polars 传来的 `Expr` 谓词 **v1 不下推**（行级由 polars 自己过滤）；
  sym/time 等值与范围条件在 v2.1 经 `TableScanRequest.sym/time` 下推（优化项）。
- 投影：v1 **不下推**（polars 0.45 匿名扫描的投影下推优化器存在 unwrap panic；
  关闭后 polars 在返回的 DataFrame 上自行 select，语义不变）。
- 输出：`DataFrame`，列序 = schema；Utf8 字典解码为 String。

## 2. splayed-duckdb

三层能力（对照 V1.0）：Arrow IPC 桥、原生 DataChunk 路由、C ABI 扩展。

### V2.0 范围（分两步）

1. **Arrow IPC 桥**（本迭代）：`scan_duckdb(path) -> arrow::ArrayReader` 语义——
   将 `scan_to_arrow` 的批次经 Arrow IPC 序列化，供 DuckDB `read_arrow` /
   replacement scan 消费；写入方向 `write_duckdb_arrow(con, table_path)`。
2. **原生 DataChunk + C ABI**（后续迭代）：`splayed-duckdb-extension` C++ 壳
   调用 Rust 导出的 C ABI（`splayed_scan_meta` / `splayed_scan_field`），
   实现谓词下推与分区裁剪（对齐 V1.0 扩展骨架）。

### 注意事项

- DuckDB 的表函数签名需静态 schema → 打开时读 `read_table_schema()`。
- Windows 下 DuckDB 扩展为独立 cdylib，不在 workspace members。

## 3. splayed-adbc（ADBC 驱动，DataFusion 执行）

### V2.0 范围

- `SplayedDriver`：注册 path → DataFusion `ListingTable` 式 provider（实现
  `TableProvider`：schema()/scan()），SQL 经 DataFusion 执行 → Arrow 流（ADBC
  标准输出）。
- 谓词/投影/limit 下推：DataFusion 优化器裁剪后映射到 `TableScanRequest`
  （predicate → `Predicate`，limit → 全局 limit）。
- 统计：`read_table_statistics()` 上报（row_count 精确，min/max 精确）。

### 对照 V1.0

| V1.0 | V2.0 |
| --- | --- |
| DataFusion TableProvider 三层对接 | Table 层单 provider（pruning 在 scan_table 内） |
| stats-agg 优化规则（MIN/MAX/COUNT 命中） | 后续迭代（需 DataFusion rule 定制） |
| refresh/reload 联动 | `TableHandle` 重开语义 |

## 4. 实现顺序与验收

1. splayed-polars：`scan_polars` 返回 LazyFrame；谓词执行正确；分区表可扫。
2. splayed-duckdb：IPC 桥 + DuckDB SQL 查询端到端。
3. splayed-adbc：DataFusion SQL → Arrow 流端到端 + 下推验证。
每步先对照本文档审查实现 PR 级差异，再合入。
