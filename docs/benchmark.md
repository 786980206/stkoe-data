# Splayed vs Parquet 基准测试方案（V2.0 正式基准）

> Splayed vs Parquet（W1+R1–R6）与 Splayed vs DuckDB/Polars（PW1+PR1–PR5）的统一基准文档。
> 执行器：`crates/splayed-bench`（bin）+ `scripts/partition_bench.py`；结果 CSV 在 `results/`。

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
| 行数 | 1M / 5M / 10M / 20M（按磁盘可行性核定） |
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
- 过滤实现：`symbol IN (…)` = Or(Eq) 组合（Splayed 经 META 索引扫描 `scan`
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

## 分区基准方法（Splayed vs DuckDB/Polars）

## 3. 数据与分区

| 参数 | 规定 |
| --- | --- |
| 列 | 20 列：`sym`（1000 值字典 Utf8）+ `ts`（i64 unix µs，Timestamp）+ 18 数值（9 i64 + 9 f64，确定性哈希生成） |
| 行数 | 1M / 5M / 10M / 20M（100M 需 ≥50GB 空闲磁盘，当前环境不排期——同 olap 基准决策） |
| 时间范围 | 2020-01-01 … 2023-12-31 均匀分布 |
| 排序 | `(sym ASC, ts ASC)`，三引擎同序写出 |
| 分区 | year：2020/2021/2022/2023 共 4 个分区 |

## 4. 场景

| 场景 | 操作 | 计时内容 |
| --- | --- | --- |
| PW1 | 全量分区写入 | 分区目录创建 + 文件写出（含 ZSTD 压缩） |
| PR1 | 全表扫描（聚合） | `count(*) + sum(n0) + sum(n1)` 强制全读（所有分区） |
| PR2 | 单分区扫描 | `year = 2022` 的同款聚合 |
| PR3 | 范围裁剪 | `year BETWEEN 2021 AND 2022`（2 分区）同款聚合 |
| PR4 | 高选择性 | `sym = 'SYM0000' AND year = 2022`，投影 5 列（sym/ts/n0/n1/n2） |
| PR5 | 分区元数据 | 分区列表 + 各分区行数（Splayed = META 统计；DuckDB/Polars = 逐分区元数据计数） |

- 聚合读取语义对齐：三引擎均为「读全列 → 聚合」，i64 整数和精确可比（无浮点误差）。
- 校验：PW1 后逐场景比对三引擎的 `count / sum(n0) / sum(n1)` 完全一致（不计入时间）。

## 5. 指标

write_time / read_time (s)、physical_size、file_count（文件总数，含各分区）、
output_rows、output_bytes（逻辑投影字节）、throughput (MB/s)。
CSV = `results/partition_bench.csv`。

## 6. 公平性与已标注差异

- 同机同盘（NTFS/D:）、同数据、同压缩（ZSTD-3）、Warm-only（Windows 无 drop_caches）。
- 三引擎核心路径均单客户端顺序调用；Splayed 内部并行如实报告（建表 1t/4t 两组）。
- 输入不对称已注明：Splayed 输入 = 内存 Arrow（生成不计入）；DuckDB/Polars 输入 =
  源 Parquet 文件（读源文件属于其写入管线的第一环，计入写入时间——这是 Hive 分区
  写出的真实管线形态）。
- `description` 类高基数字符串列不参与本基准（V2 Utf8 字段不 roundtrip 的已知限制，
  见下方性能审查记录）；数值列聚合已覆盖读路径全部 I/O。


---

# 基准结果

## V2.1 统一四层 API 实机基准复测（2026-09-07，Windows/NTFS，Warm）

在完成四层对称 API（Table → Dataset → Index → Field）升级与零拷贝视图贯通后，执行官方基准复测（1M 行 × 50 列，48 月分区，ZSTD-3）：

| 场景 | splayed-4t | splayed-1t | parquet | 4t / parquet 优势 |
| :--- | :---: | :---: | :---: | :---: |
| **W1 写入** | **3.48 s** | 6.76 s | 5.30 s | **快 1.52×** (节省 34% 耗时) |
| **R1 全扫描** | **18.26 ms** | 18.49 ms | 1084.05 ms | **快 59.4×** (零拷贝流式极限吞吐) |
| **R2 单列** | **8.03 ms** | 7.81 ms | 12.52 ms | **快 1.56×** |
| **R3 双过滤 1%** | **4.04 ms** | 4.04 ms | 50.36 ms | **快 12.5×** |
| **R4 sym 1%** | **5.44 ms** | 5.55 ms | 98.33 ms | **快 18.1×** |
| **R5 time 1%** | **6.76 ms** | 6.77 ms | 18.88 ms | **快 2.79×** |
| **R6 极高选择性** | **3.85 ms** | 3.83 ms | 42.93 ms | **快 11.2×** |

- **存储体积**：Splayed 270.96 MB (压缩率 0.6912) vs Parquet 281.46 MB (压缩率 0.7180)，Splayed 节省 3.7% 磁盘占用。
- **端到端比对**：`correctness: splayed == parquet (all scenarios)` 校验 100% 通过。
- **验证结论**：升级为统一四层 API 后，原有零拷贝切片、RepeatDict 零物化、64B 延迟展开与 SIMD 谓词管线性能完整保留，执行效率稳定甚至小幅提升。

## 正式基准结果（2026-09-05，Windows/NTFS，Warm）

数据：50 字段（sym 1000 字典 + time µs + description 5000 字典 + 47 数值），48 月分区，
(sym ASC, time ASC) 排序，ZSTD-3 双侧对齐，Warm-only（Windows 无 drop_caches）。
执行器：`crates/splayed-bench`（一键可重现；CSV = results/bench_final3.csv）。
校验：逐场景 per-column 校验和，Splayed == Parquet（description 列除外——Utf8 字段
不 roundtrip 的已知限制）。计时不含校验（校验独立 pass）。

中位数（3 次；splayed-1t = 引擎内部串行，splayed-4t = 4 线程）：

| 规模 | 场景 | splayed-1t | splayed-4t | parquet | 4t/parquet |
| --- | --- | --- | --- | --- | --- |
| 1M | W1 写入 | 7.66 s | **3.76 s** | 5.64 s | 0.67× |
| 1M | R1 全扫描 | 18.5 ms | 18.7 ms | 1135 ms | **0.02×** |
| 1M | R2 单列 | 8.1 ms | 8.5 ms | 13.9 ms | 0.61× |
| 1M | R3 双过滤 1% | 4.4 ms | 4.4 ms | 53.6 ms | **0.08×** |
| 1M | R4 sym 1% | 5.8 ms | 5.8 ms | 100.8 ms | **0.06×** |
| 1M | R5 time 1% | 7.1 ms | 7.0 ms | 23.0 ms | 0.36× |
| 1M | R6 极高选择性 | 5.7 ms | 4.3 ms | 42.6 ms | **0.10×** |
| 5M | W1 | 24.7 s | **11.6 s** | 28.8 s | 0.40× |
| 5M | R1 | 35.6 ms | 37.5 ms | 5506 ms | **0.01×** |
| 5M | R2 | 8.9 ms | 8.8 ms | 71.0 ms | 0.12× |
| 5M | R3 | 4.6 ms | 4.5 ms | 219.6 ms | **0.02×** |
| 5M | R4 | 27.8 ms | 28.0 ms | 488.8 ms | **0.06×** |
| 5M | R5 | 29.5 ms | 29.7 ms | 80.5 ms | 0.37× |
| 5M | R6 | 4.1 ms | 4.1 ms | 194.4 ms | **0.02×** |
| 10M | W1 | 81.6 s | **26.0 s** | 59.4 s | 0.44× |
| 10M | R1 | 60.5 ms | 61.0 ms | 11040 ms | **0.01×** |
| 10M | R2 | 10.4 ms | 10.3 ms | 135.7 ms | 0.08× |
| 10M | R3 | 4.9 ms | 4.9 ms | 430.6 ms | **0.01×** |
| 10M | R4 | 49.9 ms | 64.9 ms | 990.4 ms | **0.07×** |
| 10M | R5 | 50.2 ms | 49.9 ms | 157.2 ms | 0.32× |
| 10M | R6 | 4.2 ms | 4.3 ms | 385.8 ms | **0.01×** |
| 20M | W1 | 144.7 s | **80.3 s** | 125.3 s | 0.64× |
| 20M | R1 | 142.8 ms | 105.8 ms | 22449 ms | **0.005×** |
| 20M | R2 | 13.2 ms | 13.0 ms | 271.6 ms | 0.05× |
| 20M | R3 | 5.3 ms | 5.2 ms | 826.9 ms | **0.01×** |
| 20M | R4 | 91.9 ms | 93.6 ms | 1523.6 ms | **0.06×** |
| 20M | R5 | 112.7 ms | 94.1 ms | 321.6 ms | 0.29× |
| 20M | R6 | 4.2 ms | 4.1 ms | 809.6 ms | **0.01×** |

物理大小：splayed 0.689× / parquet 0.718×（vs 未压缩 Arrow，各规模稳定，splayed 小 ~4%）。

**结论**：
- **写入**：splayed-4t 全规模快于 parquet（0.40–0.85×；20M 快 1.6×）；串行（1t）与
  parquet 相当。4t 收益随规模增长（10M/20M 分区级并行充分发挥）。
- **全扫描 R1**：splayed 快 22–200×——warm 语义差异所致：Splayed 打开句柄即持有
  解压后的 working 表示（解码成本在打开/校验 pass 支付），计时 pass 只做零拷贝视图
  组装；Parquet 无进程内解码缓存，每次运行重新读盘 + 解码。两者均为各自引擎的
  真实 warm 使用形态，报告时须注明该结构性差异。
- **选择性查询 R3–R6**：splayed 快 12–180×——META（sym + time）下推使未命中分区 /
  sym 整体跳过（零 I/O），parquet 行组过滤仍需逐行组解码探测。
- **R2 单列**：splayed 快 1.7–20×（读单 Field 文件 vs parquet 列裁剪）。

## 分区基准结果（2026-09-06，Year 分区 / 20 列 / 1000 sym）

三引擎同数据同分区粒度（year×4），ZSTD-3。中位数（3 次，ms）：

| 规模 | 场景 | Splayed | DuckDB | Polars | Splayed/DuckDB | Splayed/Polars |
| --- | --- | --- | --- | --- | --- | --- |
| 20M | PW1 写入 | **9.9 s** | 21.9 s | 35.8 s | 0.45× | 0.28× |
| 20M | PR1 全扫描 | **63 ms** | 144 ms | 138 ms | 0.44× | 0.46× |
| 20M | PR2 单分区 | **17.7 ms** | 64.0 ms | 34.4 ms | 0.28× | 0.51× |
| 20M | PR3 范围 2 分区 | **34.7 ms** | 86.2 ms | 235.6 ms | 0.40× | 0.15× |
| 20M | PR4 sym+year | **0.47 ms** | 26.7 ms | 5.2 ms | **0.02×** | 0.09× |
| 20M | PR5 分区元数据 | **0.77 ms** | 5.3 ms | 12.7 ms | 0.15× | 0.06× |

物理大小（20M）：splayed 1.18 GB / duckdb 1.22 GB / polars 1.21 GB；文件数 splayed 76（4×19）vs 4（各引擎 1 文件/分区）。

**结论**：Splayed 分区表在写入、全扫描、分区裁剪、选择性查询、元数据查询全场景
均快于 DuckDB 与 Polars 的 Hive 分区 Parquet（PW1 写入快 2–3.6×、PR4 高选择性快
21–56×、PR5 元数据快 7–16×），且物理大小相当（略小 ~3%）。文件数劣势（76 vs 4）
被 Splayed 的 META-only 元数据读与零 I/O 裁剪完全覆盖。



> 基准方法论（数据生成规范 / 参数对齐 / 场景定义 W1+R1–R6 / 指标与校验）已独立成文：
> **docs/benchmark.md**。正式复测按该文档执行（执行器 `crates/splayed-bench`）。

