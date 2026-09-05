# Splayed vs Parquet 基准测试方案（V2.0 正式基准）

> 状态：**方案定稿，执行待确认**。本文档定义 Splayed folder 与单个 Parquet file 的
> 对比基准方法论；场景执行器实现于 `crates/splayed-bench`（bin，一键可重现）。

## 1. 目标与原则

- 比较 Splayed folder 与单个 Parquet file 在相同 Arrow 数据上的写入、扫描和选择性读取性能。
- 输入输出均为 Arrow，不经过中间格式。
- 两者均不包含追加或更新逻辑（写入 = 一次性全量建表）。
- 公平性：相同数据、相同硬件、相同预热与测量方法；关键参数（压缩算法与级别）显式对齐。

> **引擎内部并行语义**：Splayed 的建表 / 写入路径内部分区级并行（`max_parallelism`
> 预算切分）是引擎能力的一部分，基准如实报告默认配置下的表现；同时提供一组
> `max_parallelism = 1` 的 Splayed-serial 对照，用于分离「格式开销」与「并行收益」。
> Parquet 侧使用 arrow-rs 默认单线程 writer。

## 2. 数据生成规范

| 参数 | 规定 |
| --- | --- |
| 行数 | 10M / 100M / 1B（按磁盘与内存可行性分阶段执行，见 §9） |
| 字段数 | 10 / 50 |
| 字段组合 | 固定：`symbol`（低基数 Utf8）+ `time`（i64 unix µs）+ `description`（Utf8）+ 其余数值列（i64 / f64 混合） |
| Symbol 基数 | 1000 唯一 symbol，均匀分布 |
| 时间范围 | 2020-01-01 … 2023-12-31 均匀分布（i64 unix µs） |
| 排序 | **按 `(symbol ASC, time ASC)` 排序**（Splayed 自然顺序；Parquet 同序写入） |
| 数据分布 | 数值列随机（均匀分布，固定随机种子保证可重现） |
| 压缩前字节数 | 固定（Arrow 内存字节数），用于压缩率与吞吐计算 |

- 随机种子固定：所有运行可逐字节重现同一数据集。
- `symbol`：1000 值字典列（V2 Utf8 = 字典编码，天然契合）。

## 3. 存储格式参数固定

### Parquet

- arrow-rs `parquet` crate（与 bench 依赖同版本系）。
- 行组大小：1M 行（`WriterProperties::set_max_row_group_size`）。
- 压缩：ZSTD level 3（`set_compression(Zstd)`，arrow-rs 默认级别 3）。
- 编码：库自动选择（字典 / RLE / Delta 默认开启）。
- row group 统计保持默认（谓词扫描对照可用）。

### Splayed

- 本项目 `splayed-core` / `splayed-table` 默认路径。
- chunk 边界：sym 对齐（每 chunk_syms=8 个连续 sym 一组，行数超 64K 的 sym 按 cap 劈开）。
- 压缩：`Compression::Zstd`（codec `DEFAULT_ZSTD_LEVEL = 3`，与 Parquet 对齐）。
- 索引：META（sym + time）与 per-chunk 自描述头；无额外索引结构。
- 不启用手动调优的额外缓存。

## 4. 基准执行细节

- **预热与测量**：每用例预热 1 次（不计时），正式测量 3–5 次取中位数（附标准差）。
- **写入前清空目标目录**；读取前保证 OS 缓存状态一致（见下）。
- **缓存控制（平台差异）**：
  - Linux：`echo 3 > /proc/sys/vm/drop_caches`（冷启动）。
  - **Windows（当前执行环境）无等价 drop_caches**：仅报告 **Warm** 结果；
    近似冷启动可用「预读大于内存的填充文件驱逐 standby list」近似（非严格，标注后可选）。
- **计时**：`std::time::Instant` 单调时钟；写入 = 首字节到 flush + fsync 完成；
  读取 = 开始扫描到返回完整 Arrow 数据（不含校验）。
- **资源监控（可选）**：峰值 RSS / 磁盘 I/O 字节数——Linux 经 `/proc/self/io`；
  Windows 侧暂缺（记录为局限）。

## 5. 场景（W1 + R1–R6）

| 场景 | 行数 | 字段数 | 压缩 | Projection | Sym Sel. | Time Sel. | 说明 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| W1 | 100M | 50 | ZSTD-3 | - | - | - | 全量写入 |
| R1 | 100M | 50 | ZSTD-3 | All | 100% | 100% | 全量扫描 |
| R2 | 100M | 50 | ZSTD-3 | 1 数值列 | 100% | 100% | 单列投影 |
| R3 | 100M | 50 | ZSTD-3 | 5 列（3 数值+2 字符串） | 1% | 1% | 双重过滤 |
| R4 | 100M | 50 | ZSTD-3 | 5 列 | 1% | 100% | 仅 symbol 过滤 |
| R5 | 100M | 50 | ZSTD-3 | 5 列 | 100% | 1% | 仅 time 范围过滤 |
| R6 | 100M | 50 | ZSTD-3 | 1 列 | 0.01% | 0.01% | 极高选择性 |

- 选择性定义：Sym Sel. = 选中 symbol 数 / 1000；Time Sel. = 选中时间跨度 / 总跨度。
- 过滤实现：`symbol IN (…)` = Or(Eq) 组合（Splayed 经 META `scan_index_handle`
  sym 过滤 union 下推）；`time BETWEEN lo AND hi` = 时间范围（Splayed 经分区裁剪 +
  META time 窗口下推）。Parquet 侧逐批行级过滤对照。
- Symbol 1% = 1000 中固定种子随机取 10 个；Time 1% = 总跨度 1% 连续区间。

## 6. 输出指标

`write_time (s)`、`read_time (s)`、`physical_size (bytes)`、
`compression_ratio = physical_size / uncompressed_arrow_size`、`output_rows`、
`output_bytes (Arrow 内存字节)`、`write_throughput (MB/s) = uncompressed_bytes / write_time`、
`read_throughput (MB/s) = output_bytes / read_time`、`cold/warm` 标签、
`peak_rss`（可选）、`disk_io_bytes`（可选）。

输出：CSV（每行一个用例，含全部参数与指标），追加 JSON 供程序化分析。

## 7. 正确性校验

- 所有读取用例：Splayed 与 Parquet 返回的 Arrow 数据**完全一致**（行序、列值、NULL）。
- 选择性查询结果 = 全量扫描 + 同条件过滤的对照结果（抽样 / 全量比对）。
- 校验不计入性能时间。

## 8. 其他考虑

- 同一文件系统（NTFS，D: 盘）；记录文件系统与磁盘类型。
- 记录 CPU / 内存 / 磁盘型号。
- 执行器为单客户端顺序调用；引擎内部并行语义见 §1 注记。
- 每用例 ≥ 3 次；中位数 + 标准差。
- 一键脚本：`crates/splayed-bench`（bin）+ `scripts/run_bench.ps1`，可重现全流程。

## 9. 规模可行性与执行阶段（按执行环境核定）

执行环境（2026-09-05 核定）：D: 盘 112GB（**空闲 12GB**）、RAM 32GB、Windows。

| 规模 | 未压缩 Arrow | Splayed ZSTD（估） | Parquet ZSTD（估） | 双引擎 + 生成内存 | 可行性 |
| --- | --- | --- | --- | --- | --- |
| 10M × 50 | ≈ 3.9 GB | ≈ 1.5 GB | ≈ 1.5 GB | ≈ 6 GB | ✅ 现有 12GB 空闲可执行 |
| 100M × 50 | ≈ 39 GB | ≈ 15 GB | ≈ 15 GB | ≈ 35–45 GB | ⚠️ 需清理磁盘至 ≥ 50GB 空闲 |
| 1B × 50 | ≈ 390 GB | ≈ 150 GB | ≈ 150 GB | ≈ 400 GB+ | ❌ 本机 112GB 盘不可行 |

**执行阶段**：

- **Phase 0**：`crates/splayed-bench` 执行器实现 + 10M × 50 全场景试运行（验证数据
  生成 / 正确性校验 / 指标输出闭环）。
- **Phase 1**：100M × 50 全场景（前置：磁盘清理至 ≥ 50GB 空闲；生成与写入按分区
  递增执行——逐月 `create_table_partition`，单分区内存 ≈ 1GB，规避全量物化）。
- **Phase 2**：1B 视磁盘扩容 / 多盘情况另行评估（当前环境不排期）。

## 10. 待确认决策（阻塞 Phase 1）

1. **执行规模**：Phase 0（10M）是否直接续跑 100M（取决于磁盘清理结果）。
2. **冷缓存维度**：Windows 无 drop_caches——仅 Warm，还是接受「填充文件驱逐」近似冷启动。
3. **`description` 高基数字符串**：V2 Utf8 为字典编码，逐行唯一的高基数会使字典随行数
   膨胀（存储 / 内存代价）——降为中低基数（如 1 万模板短语）符合 V2 模型；保留高基数
   需接受代价。
4. **并行语义**：Splayed 内部并行如实报告（默认），另加 Splayed-serial（max_parallelism=1）
   对照组。
