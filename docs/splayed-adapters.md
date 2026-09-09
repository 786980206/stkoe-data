# 适配层设计（V2.1）：Polars / DuckDB / ADBC

本文档定义三个引擎适配层的能力对齐方案，作为实现前置的权威设计。
依赖方向：`format ← codec ← core ← table ← arrow ← {polars | duckdb | adbc}`。

## 1. splayed-polars（Rust & Python 原生 Polars 深度适配）

### 核心能力与 API 对齐
同时支持 Rust 原生与 Python 两端生态：

#### 1.1 惰性扫描（scan_splayed）
- **Rust API**: `pub fn scan_splayed(path: impl AsRef<Path>) -> PolarsResult<LazyFrame>`（保留 `scan_polars` 别名）
- **Python API**: `splayed.scan_splayed(path, allow_filter=True, batch_size=None)`，同时注入为 `polars.scan_splayed(path)`
- **Lazy 下推优化**：
  - **Projection Pushdown（列裁剪）**：仅读取并解码查询请求所需要的列（包含过滤谓词依赖列）；
  - **Predicate Pushdown（谓词下推）**：自动将 Polars 过滤表达式下推并转换为 PyArrow 计算表达式，在内存流式切片批次层即时求值过滤；
  - **Limit Pushdown（行数裁剪）**：自动下推 limit 参数，提前截断批次并及早终止跨分区扫描。

#### 1.2 导出与写入（sink_splayed）
- **Rust API**: `pub fn sink_splayed(df: &DataFrame, path: impl AsRef<Path>, scheme: PartitionScheme, options: Option<TableOptions>) -> PolarsResult<()>`
- **Python API**:
  - `splayed.sink_splayed(df_or_lf, path, scheme="none", max_parallelism=None)`
  - 原生链式方法：`df.sink_splayed(path, ...)` 与 `lf.sink_splayed(path, ...)`
  - 扩展命名空间：`df.splayed.sink(path, ...)` 与 `lf.splayed.sink(path, ...)`
- **智能自愈与双路径路由**：
  - 若目标路径尚无表物理元数据（`.meta` 缺失）：自动调用 `TableWriter::init` 进行建表初始化与首批数据写入；
  - 若目标表已存在：自动调用 `TableWriter::open` 并通过 `.write()` 执行覆盖写入。

### 能力对齐表

| 能力 | V2.1 实现 |
| --- | --- |
| `scan_splayed` 惰性扫描 | 支持 Rust 与 Python，返回 Polars `LazyFrame` |
| Projection 下推 | 支持：仅拉取所需列，通过 `splayed-arrow` 零拷贝转换 |
| Predicate 下推 | 支持：通过 PyArrow/Arrow 表达式求值层实现 |
| Limit 下推 | 支持：批次提前早停截断 |
| `sink_splayed` 自动建表与写入 | 支持：自动判别不存在走 `init`，已存在走 `open` + `write` |
| 跨分区与时序切分 | 支持：根据 `PartitionScheme` 自动切分并写入各分区 |

### 语义

- `schema`：由 `table.schema()` 提供（最后 Partition，含 sym/time）。
- 谓词：polars 传来的 `Expr` 谓词 **暂不下推**（行级由 polars 自己过滤）；
  sym/time 等值与范围条件在 v2.1 经 `TableScanRequest.sym/time` 下推（优化项）。
- 投影：**暂不下推**（polars 0.45 匿名扫描的投影下推优化器存在 unwrap panic；
  关闭后 polars 在返回的 DataFrame 上自行 select，语义不变）。
- 输出：`DataFrame`，列序 = schema；Utf8 字典解码为 String。

## 2. splayed-duckdb

三层能力：Arrow IPC 桥、原生 DataChunk 路由、C ABI 扩展。

### V2.0 范围（分两步）

1. **Arrow IPC 桥**（本迭代）：`scan_duckdb(path) -> arrow::ArrayReader` 语义——
   将 `scan_to_arrow` 的批次经 Arrow IPC 序列化，供 DuckDB `read_arrow` /
   replacement scan 消费；写入方向 `write_duckdb_arrow(con, table_path)`。
2. **原生 DataChunk + C ABI**（后续迭代）：`splayed-duckdb-extension` C++ 壳
   调用 Rust 导出的 C ABI（`splayed_scan_meta` / `splayed_scan_field`），
   实现谓词下推与分区裁剪。

### 注意事项

- DuckDB 的表函数签名需静态 schema → 打开时读 `table.schema()`。
- Windows 下 DuckDB 扩展为独立 cdylib，不在 workspace members。

## 3. splayed-adbc（ADBC 驱动，DataFusion 执行）

### V2.0 范围

- `SplayedDriver`：注册 path → DataFusion `ListingTable` 式 provider（实现
  `TableProvider`：schema()/scan()），SQL 经 DataFusion 执行 → Arrow 流（ADBC
  标准输出）。
### 实现要点

- 谓词/投影/limit 下推：DataFusion 优化器裁剪后映射到 `TableScanRequest`
  （predicate → `Predicate`，limit → 全局 limit）。
- 统计：`table.statistics()` 上报（row_count 精确，min/max 精确）。
- stats-agg 优化规则（MIN/MAX/COUNT 命中）：后续迭代（需 DataFusion rule 定制）。
- refresh/reload 联动：`TableHandle` 重开语义。

## 4. 实现顺序与验收

1. splayed-polars：`scan_polars` 返回 LazyFrame；谓词执行正确；分区表可扫。
2. splayed-duckdb：IPC 桥 + DuckDB SQL 查询端到端。
3. splayed-adbc：DataFusion SQL → Arrow 流端到端 + 下推验证。
每步先对照本文档审查实现 PR 级差异，再合入。
