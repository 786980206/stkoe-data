# splayed-cli

CLI 工具（基于 core + arrow + datafusion），提供完整的生命周期演示与运维能力。

## 子命令

| 子命令 | 说明 |
| --- | --- |
| `init <dir>` | 创建示例股票数据集（3 symbols × 5 days：AAPL/GOOG/MSFT，close + volume） |
| `update <dir> --field <f> --sym <s> --time <t> --value <v>` | 单格原地更新（按类型编码 f64 值） |
| `compact <dir> --field <f> --algo zstd\|lz4` | 压缩字段（ZSTD/LZ4），压缩后只读 |
| `read <dir> --field <f> [--sym <s>]` | 读取字段原始值（含时间标签） |
| `sql <dir> --query "SQL"` | 通过 DataFusion TableProvider 执行 SQL（自动探测单 dataset/分区表） |
| `export <dir> --output <csv>` | 导出全表为 CSV |
| `export-arrow <dir> --output <arrow>` | 导出全表为 Arrow IPC（供 DuckDB `read_arrow`） |

## 快速开始

```bash
cargo build -p splayed-cli

splayed init data/2024
splayed update data/2024 --field close --sym AAPL --time 0 --value 999.99
splayed compact data/2024 --field close --algo zstd
splayed read data/2024 --field close
splayed sql data/2024 --query "SELECT sym, count(*), avg(close) FROM splayed GROUP BY sym"
splayed export data/2024 --output out.csv
splayed export-arrow data/2024 --output out.arrow
```

!!! note "Windows mmap 注意"
    `update` 在写入前先 drop 已打开的 `FieldReader`（Windows 下 mmap 文件不可写）。

## 依赖

- `splayed-arrow::create_table`（init）
- `splayed-core`（open_dataset / update_field / FieldReader / compact_field）
- `splayed-datafusion::register_splayed_table`（sql / export）
- `splayed-duckdb::export_to_arrow_ipc`（export-arrow）
