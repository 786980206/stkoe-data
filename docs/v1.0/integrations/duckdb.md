# DuckDB 集成

`crates/splayed-duckdb` 提供两条与 DuckDB 对接的路径：

## Path 1：Arrow IPC 桥（feature "arrow"，默认开）

**外部文件交换**：Splayed → CoreBatch → Arrow（零拷贝）→ IPC 文件，DuckDB 用 `read_arrow` 读取。

```rust
use splayed_duckdb::export_to_arrow_ipc;

let rows = export_to_arrow_ipc("data/2024", "data/2024.arrow")?;
```

```sql
SELECT * FROM read_arrow('data/2024.arrow');
```

**代价**：IPC 序列化 + DuckDB 重解析一段拷贝。

## Path 2：原生 DataChunk 路由（零 Arrow）

`native::scan_to_chunks` / `scan_table_to_chunks` 直接把 `CoreBatch` 流喂给 DuckDB 扩展层——CoreBatch 的定长小端缓冲 + validity 位图与 DuckDB `Vector` 的 `data_ptr + NullMask` 布局**同构**，扩展内可用 `Vector(LogicalType, data_ptr)` 零拷贝借用：

- 数值/日期/时间戳列：数据缓冲原样（无拷贝）；
- NULL：CoreBatch validity 位图 → DuckDB `NullMask`（n/8 字节小拷贝）；
- SYM（字典列）：扩展侧展开为 DuckDB 字典/字符串向量。

```rust
use splayed_duckdb::native::scan_to_chunks;
use splayed_core::{ScanRequest, SymbolSelection};

let mut req = ScanRequest::new(vec!["close".into()]);
req.symbols = SymbolSelection::Symbols(vec!["AAPL".into()]); // 字段公开，直接赋值
let n_threads = 4;
scan_to_chunks(&dataset, &req, n_threads, |batch| {
    // 逐批消费 CoreBatch，填充 DuckDB DataChunk
    Ok(())
})?;
```

## C ABI + 扩展壳

Rust 集成层暴露 C ABI（`ffi.rs`），C++ 壳工程（`splayed-duckdb-extension`）消费它注册 `read_splayed(dir)` 表函数：

```sql
LOAD 'splayed_read';
SELECT sym, avg(close) FROM read_splayed('data/2024')
  WHERE time >= DATE '2024-06-01' GROUP BY sym;
```

### 构建扩展库

```bash
cargo build -p splayed-duckdb --release --config "lib.crate-type=['cdylib','staticlib']"
# → target/release/splayed_duckdb.dll / .lib / .dll.lib
```

## C ABI 数据合同

- 句柄 = 裸指针（谁创建谁释放）；错误 = 返回码 + `splayed_last_error()`。
- 列布局：`[0]=time`（DATE/TIMESTAMP）、`[1]=sym`（字典列，type_id=200）、`[2..]=FIELD`。
- 列视图：`SplayedColumnFFI{kind, type_id, data, data_len, validity, dict_count}`；SYM 字典串经 `splayed_scan_dict_value` 取回。

详见 [splayed-duckdb 参考](../crates/duckdb.md)。
