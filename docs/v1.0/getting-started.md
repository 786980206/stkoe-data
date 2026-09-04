# 快速上手

## 环境要求

- Rust 工具链 **1.86+**（workspace `rust-version` 已升至 1.86，`stats-agg` 优化规则需要 trait upcasting）
- （可选）Python 3.10+ 运行示例脚本；DuckDB 相关脚本需要 `pip install duckdb pyarrow`

## 构建

```bash
cargo build        # 必须零警告
cargo test         # 全部测试通过
```

workspace 成员一览：

| Crate | 说明 |
| --- | --- |
| `splayed-format` | 磁盘格式定义（无依赖） |
| `splayed-codec` | 编码 + 压缩 |
| `splayed-core` | 引擎无关核心 |
| `splayed-arrow` | Arrow 交换 + 零拷贝转换 |
| `splayed-datafusion` | DataFusion TableProvider 三层对接 |
| `splayed-duckdb` | DuckDB 集成（IPC 桥 / 原生 / C ABI） |
| `splayed-polars` | Polars 惰性扫描 |
| `splayed-adbc` | 上层 ADBC 驱动 |
| `splayed` | Umbrella（features 开关） |
| `splayed-cli` | CLI 工具 |

## CLI 快速开始

```bash
# 构建 CLI
cargo build -p splayed-cli

# 创建一个示例股票数据集（3 symbols × 5 days）
splayed init <dataset_dir>

# 原地更新一个格子
splayed update <dataset_dir> --field close --sym AAPL --time 0 --value 999.99

# 压缩字段（ZSTD/LZ4），压缩后只读
splayed compact <dataset_dir> --field close --algo zstd

# 读取字段原始值
splayed read <dataset_dir> --field close

# 通过 DataFusion 执行 SQL
splayed sql <dataset_dir> --query "SELECT sym, avg(close) FROM splayed GROUP BY sym"

# 导出 CSV / Arrow IPC
splayed export <dataset_dir> --output out.csv
splayed export-arrow <dataset_dir> --output out.arrow
```

详见 [splayed-cli](crates/cli.md)。

## Python 示例

```bash
# 编译一次 CLI 后：
python example/demo_datafusion.py       # 原生 DataFusion 集成（无 Python 三方库）
python example/demo_duckdb.py           # DuckDB（CSV 交换）
python example/demo_duckdb_arrow.py     # DuckDB（Arrow IPC 交换，需 duckdb + pyarrow）
```

详见 [Python 示例](integrations/examples.md)。

## 作为库使用（Umbrella）

```toml
[dependencies]
splayed = { path = "crates/splayed", features = ["arrow", "adbc", "duckdb", "polars"] }
```

```rust
// ADBC 连接：打开表 → 执行 SQL → Arrow 流
use splayed::adbc::Connection;

let conn = Connection::open("data/2024")?;
for batch in conn.statement("SELECT sym, avg(close) FROM splayed GROUP BY sym")?.execute()? {
    println!("{batch:?}");
}
```

`arrow`（默认开）为共享转换工具；`datafusion` / `duckdb` / `polars` / `adbc` 按需开启。`adbc` 隐式引入 `datafusion`。

## 下一步

- 了解[架构总览](architecture.md)
- 阅读[磁盘格式规范](format/index.md)
- 深入各[集成指南](integrations/datafusion.md)
