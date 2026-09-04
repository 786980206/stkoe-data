# 阶段状态

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
| 17 | ✅ Done | 分区管理下沉 core（`splayed-core::partition`：发现/schema 合并/三层剪裁/流式合并），DataFusion 与 DuckDB 共用 |
| 18 | ✅ Done | 分区列（key=value 虚拟列）+ 分区级符号剪裁透传 + polars 分区表 + 编码器接线 + 聚合下推 rule（rust-version 升 1.86） |
| 19 | ✅ Done | 分区写能力：`create_partitioned_table` / `append_partition` / `drop_partition` / `update_partition_table`（跨分区路由格子写）/ `update_partition_meta`（表级布局重排）+ DataFusion `SplayedTableProvider::reload` |
| 20 | ✅ Done | `update_field` 统计增量维护：null_count 精确增量 + min/max 保守上界（O(k)），命中旧极值才全列重扫；`create_field_with_data` 写真实 null_count（基线） |
| 21 | ✅ Done | splayed-python：pyo3 0.29 扩展 `splayed`（`import splayed`），pyarrow 表格交换层（Arrow C data interface）——写/读/分区/子集接口镜像 `splayed-arrow` |

## 未来（暂缓，不在本文档范围）

- `INSERT INTO` 写回（`update_table` 的上层接口）。
- DuckDB 扩展：谓词下推（时间范围/分区列）、写入、物化视图等（逐步在 Rust 层 C ABI 扩展）。
