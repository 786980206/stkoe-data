# Splayed vs DuckDB/Polars 分区 Parquet 基准方案（V2.0）

> 状态：方案定稿并执行（执行器 = `crates/splayed-bench --mode partition` +
> `scripts/partition_bench.py`；CSV = `results/partition_bench.csv`）。
> 上游方法论（数据生成 / 计时 / 缓存 / 指标定义）沿用 [benchmark.md](benchmark.md)。

## 1. 目标

比较 Splayed 原生分区表与 DuckDB / Polars 的 Hive 风格分区 Parquet，在相同数据、
相同分区粒度（year）下的：写入性能（分区目录 + 文件写出）、读取性能（全表扫描 /
分区裁剪 / 选择性查询）、存储开销（物理大小 / 文件数量）。

## 2. 对比对象与驱动方式

| 系统 | 分区方式 | 驱动 |
| --- | --- | --- |
| Splayed | 内置 `PartitionScheme::Year`（ts → year，无需冗余列） | `crates/splayed-bench`（Rust） |
| DuckDB | `COPY … TO dir (FORMAT PARQUET, PARTITION_BY (year))`（Hive 目录，分区列不入文件） | 官方 Python API（duckdb 1.5.4，进程内精确计时；SQL 与 CLI 完全一致） |
| Polars | `partition_by("year")` + 逐分区 `write_parquet`（ZSTD） | 官方 Python API（polars 1.32.3，进程内计时） |

- 外部引擎输入 = 源 Parquet 单文件（20 列全量数据，由 splayed-bench 生成并写出，
  **不计入任何引擎写入时间**——它是三引擎的共同输入）。
- 结构性差异（方案 §2 已注明）：DuckDB/Polars 分区目录内文件**不含分区列**
  （列值编码在目录名）；Splayed 每分区 Dataset 保留完整数据（分区列 ts 由 META
  网格管理）。读回对齐：外部引擎 hive_partitioning 从目录名还原 year；
  Splayed 从 ts 直接返回。

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
  见 design-boundary §9）；数值列聚合已覆盖读路径全部 I/O。
