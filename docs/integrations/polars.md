# Polars 集成

`crates/splayed-polars` 通过 Polars 官方 **`AnonymousScan`** 提供惰性数据源，深度优化谓词下推与列裁剪。

## 单 dataset

```rust
use splayed_polars::splayed_lazyframe;

let lf = splayed_lazyframe("data/2024")?;

let df = lf
    .filter(col("sym").eq(lit("AAPL")))
    .select([col("time"), col("close")])
    .limit(100)
    .collect()?;
```

## 分区表

```rust
use splayed_polars::splayed_lazyframe_table;

let lf = splayed_lazyframe_table("data/")?;   // 顶层目录 = 表

// key=value 分区列进 schema + 常量列，分区列过滤直接路由
let df = lf
    .filter(col("year").eq(lit(2024)))
    .select([col("sym"), col("close")])
    .collect()?;
```

## 下推行为

| 能力 | 说明 |
| --- | --- |
| 谓词下推 | `sym` 等值 → `SymbolSelection`、`time` → `TimeRange`、FIELD 值比较 → `Filter`（AND 链合并） |
| 列裁剪 | 只扫描 `with_columns` 涉及的列 |
| 兜底过滤 | 未翻译的复杂谓词由 polars 物理评估（`df.lazy().filter(pred).collect()`），**结果恒正确** |
| 转换 | Arrow C data interface（零拷贝）；时间列物理化（Date=Int32、Datetime=Int64），字符串按值构造 |

## 已知约束（polars 0.45）

无显式 `select` 的完整收集会触发 anonymous-scan 投影优化 bug——链上显式 `select([...])`：

```rust
// 完整收集请带显式 select（等价语义）
let df = lf.select([col("time"), col("sym"), col("close")]).collect()?;
```

## 依赖

```toml
polars = "=0.45.1"        # features: lazy/fmt/dtype-*
polars-arrow = "=0.45.1"
```
