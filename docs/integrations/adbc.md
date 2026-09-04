# ADBC 集成

`crates/splayed-adbc` 面向**外部应用**提供统一数据库连接接口：打开表 → 执行 SQL → 返回 Arrow `RecordBatch` 流。内部由 DataFusion 执行 SQL。

## 基本用法

```rust
use splayed::adbc::Connection;
use std::iter::Iterator;

// 打开并注册 `splayed` 表（自动探测单 dataset / 分区表）
let conn = Connection::open("data/2024")?;

// 流式执行：同步 Iterator<Item = Result<RecordBatch, AdbcError>>
let mut stream = conn.statement("SELECT sym, avg(close) FROM splayed GROUP BY sym")?.execute()?;
while let Some(batch) = stream.next() {
    let batch = batch?;
    // 消费 RecordBatch（投影/聚合结果）
}

// 便捷全量收集
let batches = conn.execute("SELECT * FROM splayed WHERE close > 150 LIMIT 10")?;

// 更新布局后刷新（core::update_meta / 分区写入后）
conn.refresh()?;
```

## Statement API

```rust
// builder 风格覆盖 SQL
let stmt = conn.statement("SELECT 1").with_sql("SELECT * FROM splayed WHERE sym = 'AAPL'");
let stream = stmt.execute()?;          // 流式
let all = stmt.execute_all()?;         // 全量 Vec<RecordBatch>
```

## 错误处理

`AdbcError` 三分类：

| 变体 | 含义 |
| --- | --- |
| `OpenDatabase(String)` | 打开数据库 / 建连接失败 |
| `Execution(String)` | SQL 规划 / 执行失败 |
| `ResultStream(String)` | 结果流读取出错 |

## 定位

- **不复实现 SQL 解析**——解析/优化/执行全部由 DataFusion 承担。
- DuckDB 执行路径（DuckDB Extension）可作为平替后端。
- ADBC C ABI（`adbc.h`）FFI 可在 `Connection`/`Statement` 之上加薄层。
