# 未实现 / Roadmap

本文档集中列出**当前尚未实现 / 预留 / 规划**的能力，与 `plan.md` 中的 ⚠ 标注一一对应。其余能力均为**已实现**状态（见[阶段状态](phases.md)）。

## 尚未实现 / 预留

| 能力 | 出处 | 状态 | 说明 |
| --- | --- | --- | --- |
| 性能基准（Benchmark） | `plan.md` §13 | ⚠ **未实现** | 仓库无任何 bench 基础设施（无 `benches/`、无 criterion/iai/divan）；指标矩阵与 vs Parquet 对比为**规划目标** |
| core 层聚合（GROUP BY pushdown） | `plan.md` §9.4 | ⚠ **预留** | `COUNT/SUM/MIN/MAX/AVG` 的 per-SYM local aggregate → merge 未下沉 core。当前聚合在上层执行：DataFusion 仅对**无过滤** MIN/MAX/COUNT 走 stats-agg 优化规则（命中 footer 统计，整扫描跳过）；其余由 DataFusion/Polars 物理计划完成 |
| DuckDB 扩展：谓词下推 / 写入 / 物化视图 | `plan.md` §10.4 | ⚠ **骨架/未来** | C++ 扩展壳（`splayed-duckdb-extension`）当前「全列 + 全时间」扫描；分区谓词转换在 native 侧经 `partition_filters` 部分支持，扩展层下推、写入、物化视图待 Rust 层 C ABI 逐步扩展 |
| ADBC/DuckDB 平替后端 | `plan.md` §10.4 | ⚠ **未做** | 当前 ADBC 经 DataFusion 执行 SQL；DuckDB 执行路径（Extension）为平替后端规划项 |
| 崩溃恢复测试套件 | `plan.md` §8.3 | ⚠ **机制有、测试无** | 崩溃安全机制已实现（generation 屏障 + 原子提交 + compact 写后 rename），但无专门的故障注入 / 崩溃恢复测试 |
| `INSERT INTO` 写回接口 | `plan.md` §10.3 | ⚠ **暂缓** | `update_table` 的上层 SQL 写回未做 |

## 过时/遗留（已由新机制取代，保留仅向后兼容）

| 项 | 出处 | 说明 |
| --- | --- | --- |
| `ColumnView`（Native ColumnView） | `plan.md` §10.2 | 遗留路径：主内存模型已由 **CoreBatch** 取代；`ColumnView` 仍供 `column_view_to_arrow` 与 scalar filter 使用 |

## 当前已实现能力速览

> 供对照——以下均为 **✅ 已实现**（详见[阶段状态](phases.md)）：

- 磁盘格式：META/FIELD 64B 头、类型 0–13、哨兵 NULL、统计 footer（min/max + 增量维护）、generation 原子提交。
- 读写：mmap 零拷贝读取、全预分配 + 原地更新、`update_meta` 布局重排（并发 gather + 原子提交）。
- 扫描：三层剪裁（SYM/TIME/统计）+ 8 种值过滤 + SIMD + 并行流式 + LIMIT 读取期截断。
- 编码压缩：PLAIN/DELTA/RLE/BITPACK + NONE/ZSTD/LZ4（`compact_field_with_encoding`，只读）。
- 分区：`core::partition` 四层剪裁（TIME/符号/统计/分区列）+ 分区写（建/追加/删/表级格子写/表级重排）。
- 生态：DataFusion 三层对接 + stats-agg 规则、DuckDB（Arrow IPC + 原生 DataChunk + C ABI）、Polars AnonymousScan（含分区表）、ADBC 上层驱动。
