# Splayed V1 示例脚本

本目录包含三个 Python 脚本，展示 Splayed V1 列式存储引擎的完整生命周期。

## 前置准备

```bash
# 1. 构建 Rust CLI 工具（编译一次即可）
cargo build -p splayed-cli

# 2. （DuckDB 脚本需要）安装 Python 依赖
pip install duckdb pyarrow
```

## 脚本说明

### `demo_datafusion.py` — 基于 DataFusion

展示 Splayed 原生的 DataFusion 集成：
- 创建数据集 → 读取 → 原地更新 → SQL 查询 → 压缩
- SQL 直接在 Splayed 存储上执行（TableProvider + 下推）
- **无需 Python 第三方库**，仅用标准库 `subprocess`

```bash
python example/demo_datafusion.py
```

### `demo_duckdb.py` — 基于 DuckDB (CSV 交换)

展示 Splayed → CSV → DuckDB 的数据分析管道：
- 创建数据集 → 读取 → 原地更新 → 导出 CSV → DuckDB 复杂分析
- 包含窗口函数、CTE、跨股票比较、滚动聚合
- 需要 `pip install duckdb`

```bash
python example/demo_duckdb.py
```

### `demo_duckdb_arrow.py` — 基于 DuckDB (Arrow IPC 交换)

展示 Phase 9 的 Arrow IPC 桥接方案：
- 创建数据集 → 原地更新 → 导出 Arrow IPC → DuckDB 分析
- Arrow IPC 是强类型列式格式，比 CSV 快 5-10 倍
- 需要 `pip install duckdb pyarrow`

```bash
python example/demo_duckdb_arrow.py
```

## 三个脚本的区别

| | demo_datafusion.py | demo_duckdb.py | demo_duckdb_arrow.py |
|---|---|---|---|
| 查询引擎 | DataFusion（原生集成） | DuckDB（CSV 交换） | DuckDB（Arrow IPC 交换） |
| 下推支持 | 投影 + 谓词下推到 Scanner | 无（CSV 全量导入） | 无（Arrow 全量导入） |
| SQL 执行 | 直接在 FIELD 文件上 | 在 DuckDB 内存表中 | 在 DuckDB 内存表中 |
| 交换格式 | 无（直接查询） | CSV（弱类型） | Arrow IPC（强类型列式） |
| 适合场景 | 低延迟点查 + 简单聚合 | 简单演示 + 无依赖 | 高性能分析 + 窗口函数 |
| Python 依赖 | 无 | duckdb | duckdb, pyarrow |

## 数据集结构

所有脚本使用相同的数据：
- 3 只股票（AAPL / GOOG / MSFT）× 5 天
- 2 个字段（close / volume）
- 共 15 行 × 2 个 FIELD 文件
