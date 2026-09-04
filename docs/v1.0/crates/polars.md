# splayed-polars（惰性扫描）

**依赖**：core + arrow + polars 0.45（`polars = "=0.45.1"`，features: lazy/fmt/dtype-*；`polars-arrow = "=0.45.1"`）。

通过 Polars 官方 **`AnonymousScan`**（`LazyFrame::anonymous_scan`）实现惰性扫描源，深度优化：

## 接口

| 接口 | 说明 |
|---|---|
| `splayed_lazyframe(dir) -> LazyFrame` | 注册 `AnonymousScan` 惰性数据源（单 dataset） |
| `splayed_lazyframe_table(dir) -> LazyFrame` | **分区表**惰性数据源：复用 `core::partition` 剪裁/合并；key=value 分区列进 schema + 常量列 |
| `splayed_lazyframe_subset(dir, sub_name) -> LazyFrame` | 子集 `.sub.{name}` 惰性帧（经 `splayed_arrow::read_subset` 物化视图，父全局行序） |
| `SplayedScan` / `SplayedTableScan` | `AnonymousScan` 实现：`allows_predicate/projection_pushdown=true` |
| `predicate::translate(expr, dtype_of)` | polars `Expr`（`列 op 字面量` AND 链）→ core `SymbolSelection/TimeRange/Filter` |
| `arrowconv::{to_polars_dtype, to_polars_arrow_dtype, record_batch_to_dataframe}` | arrow-rs → polars：C data interface（时间列物理化），字符串按值构造 |

## 优化点

- **谓词下推**：pushdown 的过滤条件经 `predicate` 模块翻译到核心层（`sym` 等值 → `SymbolSelection`、`time` → `TimeRange`、FIELD 值比较 → `Filter`，AND 链合并）；未翻译部分（复杂/表达式比较）在扫描结果上交给 polars 物理评估兜底（`df.lazy().filter(pred).collect()`），**结果恒正确**。
- **列裁剪**：只扫描 `with_columns` 涉及的列（投影下推）。
- **转换**：arrow-rs → polars 走 **Arrow C data interface**（`FFI_ArrowArray` 与 polars `ArrowArray` 布局逐字段一致，所有权移交）；时间列按物理类型（Date=Int32、Datetime=Int64）导入，字符串列按值构造（polars 0.45 的 newest-compat 将 String 映射为 Utf8View，标准 C data 无法直接灌入）。

## 已知约束（polars 0.45）

无显式 `select` 的完整收集会触发其 anonymous-scan 投影优化中的 `reader_schema=None` unwrap bug——pe 链上建议显式 `select([...])`（等价语义）。

使用指南见 [Polars 集成](../integrations/polars.md)。
