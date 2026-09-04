# 分区策略

Partition **不需要写死在格式里**，是**数据组织策略**，不是 FIELD/META 格式的一部分。分区管理实现**下沉到 `splayed-core::partition`**（引擎无关，DataFusion 与 DuckDB 共用）。

```text
table/                          # 顶层 = 表
├── year=2024/                  # 单层 key=value 目录 = 分区
│   └── .meta + close + ...
├── year=2025/
│   └── .meta + ...
└── year=2026/
    └── .meta + ...
```

也可自由选择粒度：`2026-Q1` / `2026-01` / 按 SYM 分组等。

## 发现与校验

- `PartitionedTable::open(dir)`：
  - `dir/.meta` 存在 → 退化为**单分区**（`.`，向后兼容旧 API）；
  - 否则子目录含 `.meta` 者为分区（按名升序）。
- `PartitionSchema { time_type, fields }`：合并校验——所有分区**字段名 + 类型一致**、`time_type` 一致；符号取并集。

## 四层剪裁

`plan(&PartitionScanRequest) -> PartitionPlan`：

| 层 | 剪裁逻辑 |
| --- | --- |
| 1. TIME | 分区 meta `time_axis` 与半开区间 `[start, end)` 相交性 |
| 2. 符号 | `Symbols` 选择剔除分区不存在的符号；一个都不存在 → 跳过分区 |
| 3. 统计 | 值 filter 与分区字段 footer `[min,max]` 不相交 → 跳过分区（整分区文件级跳过） |
| 4. 分区列 | `key=value` 目录名解析的声明式列（`PartitionColumn`）——列上过滤条件与分区 declared 值不相交 → 跳过分区 |

`scan(&plan, &req) -> PartitionScanBatches`：按分区名**升序流式合并** CoreBatch（DataFusion / DuckDB 共用此实现）。

## 分区列（key=value 虚拟列）

- 单层 `key=value/` 目录（如 `year=2024/.meta`）解析为**声明式虚拟列**：全值可解析 i64 → `Int64`，否则 `String`。
- **DataFusion**：表 schema 追加该列（`ProjectionExec` 常量列补回值）。
- **DuckDB**：原生经 `PartitionScanRequest.partition_filters` 剪裁。
- **Polars**：`splayed_lazyframe_table` 让分区列进 schema + 常量列，分区列过滤直接路由。

## 写能力

| 接口 | 说明 |
| --- | --- |
| `create_partitioned_table(root, tt, &[PartitionWriteInput])` | 一次建整表（root + N 分区，并发 `create_table`；schema/命名风格预检） |
| `append_partition(root, input)` | 追加分区（校验与既有 schema/命名/分区列一致；重复 → `PartitionExists`） |
| `drop_partition(root, name)` | 删除分区目录（不存在 → `PartitionNotFound`） |
| `update_partition_table(root, sym, time, columns, create_missing, target?)` | **表级格子写入**：跨分区路由（存在性自动定位或显式 target；无命中 → `SymTimeNotFound`）后逐分区 `update_table` |
| `update_partition_meta(root, &[PartitionWriteInput])` | **表级布局重排**：既有分区 `update_meta`（gather 重散布）+ 新增 `create_table` + 移除未保留分区（并发） |

## 各引擎接入点

- **DataFusion**：`SplayedTableProvider::scan` 用 core 分区计划的 tasks 选分区执行；`SplayedTableProvider::reload()` 刷新。
- **DuckDB**：`splayed_duckdb::native::scan_table_to_chunks` 走同一实现。
- **Polars / 其它**：可直接调用 `PartitionedTable`（仅依赖 core）。
