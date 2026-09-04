# DataFusion 集成

`crates/splayed-datafusion` 实现三层 TableProvider 对接，使 DataFusion 能查询 Splayed 格式，并提供 SQL 执行能力。

## 注册方式（Layer 3）

```rust
use datafusion::prelude::SessionContext;
use splayed_datafusion::register_splayed_table;

let ctx = SessionContext::new();
// 自动探测：dir 含 .meta → 单 dataset；否则子目录含 .meta → 分区表
register_splayed_table(&ctx, "splayed", "data/2024")?;
```

另外两种等价注册：

```sql
-- CREATE EXTERNAL TABLE（SplayedTableFactory）
CREATE EXTERNAL TABLE splayed STORED AS SPLAYED LOCATION 'data/2024';

-- 表函数（SplayedTableFunction）
SELECT * FROM read_splayed('data/2024');
```

## 三层结构

| 层 | Provider | 对标 |
| --- | --- | --- |
| Layer 1 | `SplayedDatasetProvider` | 一个含 `.meta` 的目录 = 一个分区（≈ 一个 parquet 文件） |
| Layer 2 | `SplayedTableProvider` | 顶层目录 = 表，子目录 = 分区（≈ Hive 分区表） |
| Layer 3 | `register_splayed_table` / Factory / UDTF | 注册入口 |

## SQL 示例

```sql
-- 投影下推：只打开 close，不读 open/high/low/volume
SELECT close FROM splayed;

-- SYM/TIME 下推：直接定位 row range
SELECT * FROM splayed WHERE sym = 'AAPL' AND time >= 0;

-- 值过滤下推 + 合取
SELECT * FROM splayed WHERE close > 100 AND volume > 5000;

-- LIMIT 读取期截断
SELECT * FROM splayed LIMIT 100;

-- ORDER BY sym, time 免排序（单调等值属性）
SELECT * FROM splayed ORDER BY sym, time LIMIT 10;
```

## 聚合下推（stats-agg rule）

无过滤的 `MIN/MAX/COUNT` 命中列统计（footer min/max + null_count）时，整扫描被跳过，改为单行常量计划：

```rust
use datafusion::prelude::SessionStateBuilder;
use splayed_datafusion::with_splayed_optimizer_rules;

let builder = SessionStateBuilder::new()
    .with_config(SessionConfig::new());
let state = with_splayed_optimizer_rules(builder).build();
```

## 并行扫描

```rust
use splayed_datafusion::SplayedDatasetProvider;

// 单个 dataset 切成 n 个行均衡、保序的 output partition
let provider = SplayedDatasetProvider::new("data/2024")?.with_scan_parallelism(4);
```

## update_meta 后刷新

```rust
// core::update_meta 布局重排后：
provider.reload()?;   // SplayedDatasetProvider / SplayedTableProvider
```

## 分区表 + 分区列

```sql
-- year=2024/ 等 key=value 目录解析为声明式分区列
SELECT * FROM splayed WHERE year = 2024;   -- 分区列剪裁 + 常量列补回
```

## 下推矩阵

| 能力 | 支持 |
| --- | --- |
| 投影下推 | ✅ |
| SYM 等值 / IN | ✅（`Exact`/`Inexact`） |
| TIME 范围 | ✅（半开区间） |
| 值过滤（8 种） | ✅（合取全生效） |
| IS [NOT] NULL | ✅ |
| LIMIT | ✅（读取期截断） |
| ORDER BY sym, time 免排序 | ✅（条件性） |
| 统计剪裁 | ✅（footer min/max） |
| 聚合下推（MIN/MAX/COUNT） | ✅（stats-agg rule） |
