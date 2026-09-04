# splayed-adbc（上层 ADBC 驱动）

**依赖**：datafusion + arrow。面向**外部应用**的统一数据库连接接口。

**定位**：不复实现 SQL 解析——解析/优化/执行全部由 DataFusion 承担；DuckDB 可作平替后端。ADBC C ABI（`adbc.h`）FFI 可在 `Connection`/`Statement` 上加薄层。

## 接口

| 接口 | 说明 |
|---|---|
| `Connection::open(dir)` / `open_dataset(dir)` | 打开并注册 `splayed` 表（内部 DataFusion `SessionContext` + tokio runtime；自动探测单 dataset / 分区表） |
| `Connection::refresh()` | `update_meta` 后重新注册新 provider（先注销再注册） |
| `Connection::statement(sql)` / `execute(sql)` | 执行 SQL → `Vec<RecordBatch>` |
| `Statement::with_sql/execute/execute_all` | 流式 / 全量执行 |
| `ArrowRecordBatchStream` | 同步 `Iterator<Item=Result<RecordBatch>>`（内部 block_on 驱动 DataFusion 流） |
| `AdbcError` | `OpenDatabase / Execution / ResultStream` |

## 示例

```rust
use splayed::adbc::Connection;

let conn = Connection::open("data/2024")?;

// 流式执行
let mut stream = conn.statement("SELECT sym, avg(close) FROM splayed GROUP BY sym")?.execute()?;
while let Some(batch) = stream.next() {
    let batch = batch?;
    // 消费 RecordBatch
}

// 便捷全量收集
let batches = conn.execute("SELECT * FROM splayed WHERE time >= 0 LIMIT 10")?;

// update_meta 后刷新
conn.refresh()?;
```

## 架构位置

```text
外部应用
   │
   ▼
splayed-adbc ── ADBC 驱动（本 crate）
   │  内部调用（执行 SQL）
   ▼
splayed-datafusion ── DataFusion TableProvider + SQL 执行
   │
   ▼
splayed-core ── 引擎无关核心（扫描/写入/CoreBatch）
   ▲
   │ 共享转换工具（可选）
splayed-arrow ── CoreBatch → Arrow 零拷贝
```
