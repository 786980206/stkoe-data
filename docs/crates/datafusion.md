# splayed-datafusion（三层对接）

**依赖**：core + arrow + datafusion。实现 DataFusion `TableProvider`，使 DataFusion 能查询自定义格式；同时提供 SQL 执行能力（`SessionContext`）。

## 三层结构

| 层 | 接口 | 职责 |
|---|---|---|
| Layer 3 | `register_splayed_table(ctx, name, dir)` / `auto_provider(dir)` | 自动探测 dataset/分区表并注册 |
| | `SplayedTableFactory` | `CREATE EXTERNAL TABLE ... STORED AS SPLAYED LOCATION 'dir'` |
| | `SplayedTableFunction` | `read_splayed('dir')` 表函数（`register_udtf`） |
| | `SplayedSubsetFunction` | `read_splayed_subset('dir', 'hs300')` 表函数：子集 `.sub.hs300` 物化为 `MemTable`（`register_udtf`） |
| Layer 2 | `SplayedTableProvider::new(dir)` | 分区表（基于 `core::partition` 发现/剪裁；每个子目录一个分区，时间/符号/统计剪裁，schema 合并校验）；`reload()` |
| Layer 1 | `SplayedDatasetProvider::new(dir)` | 单 dataset provider；`with_scan_parallelism(n)`（单数据集多输出分区）、`reload()`（`update_meta` 后刷新）、`statistics()`（num_rows/byte_size/null_count/min/max 上报） |

## 聚合下推

- `SplayedStatsAggRule` / `with_splayed_optimizer_rules(builder)`：无过滤的 MIN/MAX/COUNT 命中列统计（footer min/max + null_count）→ 单行常量计划，**整扫描跳过**（`SessionStateBuilder` 挂规则）。

## 谓词下推（Layer 1/2 共用同一套解析）

| 谓词 | 处理 | 标记 |
| --- | --- | --- |
| `sym = 'X'`（单个已知 SYM） | → `SymbolSelection` | `Exact` |
| `sym IN ('A','B',...)`（非取反） | 已知 SYM 并集 → `SymbolSelection`；字面量全未知 → 该分区 0 行 | `Exact` |
| `sym = 'X'` / `sym IN (...)`，同一合取中 ≥2 个 SYM 过滤表达式 | 选择只能表达并集，交集交由 DF 重滤 | `Inexact` |
| `sym = '<未知>'` | 该分区返回 0 行（不报错） | `Exact` |
| `time` 比较 | → `TimeRange`（半开区间，溢出用 saturating） | `Exact` |
| FIELD 值比较（可转换） | → `Filter`，**合取全部生效** | `Exact` |
| `field IS [NOT] NULL` | → `Filter::IsNull / IsNotNull`（NULL 哨兵匹配） | `Exact` |
| 恒等 `CAST(col AS 同型)` 包裹的列比较 | 剥除 cast 后按普通比较下推；非恒等 cast 不下推 | `Exact` / `Unsupported` |
| 其它（`IS NULL` 于非空列、非列比较、`NOT IN`） | 不下推，DF 自行过滤 | `Unsupported` |

**下推现状**：投影 / SYM 等值 / TIME 范围 / 值过滤（`classify` 三分类：Exact/Inexact/NotPushdown，8 种 Filter）全透传到 core；`LIMIT` → `ScanRequest.limit` 读取期截断；`ORDER BY sym, time` 通过单调等值属性免排序（仅 `SymbolSelection::All` 且投影保留 `sym`/`time` 时声明）。

## 执行计划

- `SplayedScanExec`（单分区叶子，`UnknownPartitioning(1)`，有界 mpsc 通道 + `spawn_blocking` 流式产出，`LIMIT` 提前截断）。
- `SplayedTableScanExec`（`UnknownPartitioning(plans.len())`，每个物理分区 = 一个输出分区）。
- 没有命中分区时返回 `EmptyExec`（如 `COUNT(*)` → 0）。

使用指南见 [DataFusion 集成](../integrations/datafusion.md)。
