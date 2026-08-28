#!/usr/bin/env python3
"""
Splayed V1 — DuckDB 示例脚本
=============================

本脚本演示如何将 Splayed V1 的数据导入 DuckDB 并进行高性能 SQL 分析。

工作流程：
  1. init    — 用 Rust CLI 创建 Splayed 数据集（SYM × TIME × FIELD）
  2. read    — 读取原始字段值（验证数据正确性）
  3. update  — 原地更新某个 cell（in-place update）
  4. export  — 将数据集导出为 CSV 文件
  5. DuckDB  — 用 DuckDB 读取 CSV 并执行复杂 SQL 分析
                包括：窗口函数、CTE、跨符号比较、滚动聚合

运行方式：
    python example/demo_duckdb.py

前置条件：
    1. 已安装 Rust + cargo
    2. 在项目根目录执行过: cargo build -p splayed-cli
    3. pip install duckdb        # DuckDB Python 包

为什么用 CSV 交换？
    Splayed V1 使用自定义二进制格式（FIELD 文件），DuckDB 无法直接读取。
    通过 CSV 作为中间格式，DuckDB 可以用 read_csv_auto() 自动推断类型。
    在生产环境中，可以使用 Arrow IPC 或 Parquet 作为更高性能的交换格式。
    但 CSV 的优势是零额外依赖、可读性强、适合演示全流程。

Splayed vs DuckDB 的角色分工：
    - Splayed：负责高效写入（in-place update）和低延迟随机读取（mmap 零拷贝）
    - DuckDB ：负责复杂分析查询（窗口函数、跨表 JOIN、列式聚合）
    两者互补：Splayed 是存储层，DuckDB 是分析引擎。
"""

import os
import subprocess
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("✗ 需要安装 duckdb: pip install duckdb")
    sys.exit(1)

# ─── 路径配置 ──────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLI_BIN = PROJECT_ROOT / "target" / "debug" / "splayed.exe"
DATASET_DIR = PROJECT_ROOT / "target" / "demo_dataset"
CSV_PATH = PROJECT_ROOT / "target" / "demo_dataset.csv"


def run_cli(*args: str) -> str:
    """调用 splayed CLI 工具，返回 stdout。使用 UTF-8 编码。"""
    cmd = [str(CLI_BIN)] + list(args)
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        print(f"  ✗ ERROR: {result.stderr}")
        sys.exit(1)
    if result.stdout.strip():
        print(f"  {result.stdout.rstrip()}")
    return result.stdout


def main():
    # ─── 0. 前置检查 ──────────────────────────────────────────────────
    if not CLI_BIN.exists():
        print(f"✗ CLI 二进制不存在: {CLI_BIN}")
        print("  请先运行: cargo build -p splayed-cli")
        sys.exit(1)

    print("=" * 70)
    print("  Splayed V1 — DuckDB 全流程演示")
    print("=" * 70)

    # ─── 1. 创建示例数据集 ────────────────────────────────────────────
    # Splayed 的 create_table 一次性完成:
    #   create_meta → create_field → update_field
    # 生成 3 只股票 × 5 天 × 2 个字段 (close, volume)
    print("\n━━━ Step 1: 创建数据集 (Splayed create_table) ━━━━━━━━━━━━━━━━━")
    run_cli("init", str(DATASET_DIR))

    # ─── 2. 读取原始字段值 ────────────────────────────────────────────
    # FieldReader 通过 mmap 直接读取 FIELD 文件（零拷贝）
    print("\n━━━ Step 2: 读取字段值 (Splayed mmap 零拷贝) ━━━━━━━━━━━━━━━━━━")
    run_cli("read", str(DATASET_DIR), "--field", "close")
    run_cli("read", str(DATASET_DIR), "--field", "volume")

    # ─── 3. 原地更新 ──────────────────────────────────────────────────
    # update_field 在绝对行偏移处覆写，不扩展文件
    print("\n━━━ Step 3: 原地更新 (in-place update) ━━━━━━━━━━━━━━━━━━━━━━")
    run_cli("update", str(DATASET_DIR),
            "--field", "close", "--sym", "GOOG", "--time", "3", "--value", "888.88")
    run_cli("update", str(DATASET_DIR),
            "--field", "close", "--sym", "MSFT", "--time", "1", "--value", "777.77")
    print("  → 验证更新结果:")
    run_cli("read", str(DATASET_DIR), "--field", "close", "--sym", "GOOG")

    # ─── 4. 导出为 CSV ─────────────────────────────────────────────────
    # 通过 DataFusion TableProvider 读取 Splayed 数据，导出为 CSV
    # 这一步展示了 Splayed → DataFusion → CSV 的数据管道
    print("\n━━━ Step 4: 导出为 CSV (Splayed → DataFusion → CSV) ━━━━━━━━━━━━")
    run_cli("export", str(DATASET_DIR), "--output", str(CSV_PATH))
    print(f"  CSV 文件: {CSV_PATH}")

    # ─── 5. DuckDB 分析 ────────────────────────────────────────────────
    # 将 CSV 导入 DuckDB，执行复杂分析查询
    print("\n━━━ Step 5: DuckDB SQL 分析 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # 创建 DuckDB 内存数据库
    con = duckdb.connect(":memory:")

    # 5a. 导入 CSV 到 DuckDB 表
    # read_csv_auto 自动推断列类型
    con.execute(f"""
        CREATE TABLE stocks AS
        SELECT * FROM read_csv_auto('{CSV_PATH.as_posix()}')
    """)
    print("  ▸ 导入 CSV → DuckDB 表 'stocks'")
    row_count = con.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    print(f"  → 行数: {row_count}")

    # 5b. 查看原始数据
    print("\n  ▸ 原始数据:")
    result = con.execute("SELECT * FROM stocks ORDER BY sym, time").fetchdf()
    print(result.to_string(index=False))

    # 5c. 窗口函数：每日收盘价的 3 日移动平均
    print("\n  ▸ 窗口函数 — 3 日移动平均 (OVER + ROWS):")
    result = con.execute("""
        SELECT
            sym, time, close,
            AVG(close) OVER (
                PARTITION BY sym
                ORDER BY time
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            ) AS ma_3day
        FROM stocks
        ORDER BY sym, time
    """).fetchdf()
    print(result.to_string(index=False))

    # 5d. CTE：计算每只股票的日收益率
    print("\n  ▸ CTE — 日收益率 (close / LAG(close) - 1):")
    result = con.execute("""
        WITH daily_returns AS (
            SELECT
                sym, time, close,
                close / LAG(close) OVER (PARTITION BY sym ORDER BY time) - 1 AS ret
            FROM stocks
        )
        SELECT
            sym,
            time,
            close,
            ROUND(ret * 100, 2) AS return_pct
        FROM daily_returns
        ORDER BY sym, time
    """).fetchdf()
    print(result.to_string(index=False))

    # 5e. 聚合：每只股票的统计摘要
    print("\n  ▸ 聚合 — 统计摘要:")
    result = con.execute("""
        SELECT
            sym,
            MIN(close) AS min_close,
            MAX(close) AS max_close,
            ROUND(AVG(close), 2) AS avg_close,
            SUM(volume) AS total_volume
        FROM stocks
        GROUP BY sym
        ORDER BY sym
    """).fetchdf()
    print(result.to_string(index=False))

    # 5f. 跨股票比较：哪天涨幅最大？
    print("\n  ▸ 跨股票比较 — 每日涨幅最大的股票:")
    result = con.execute("""
        WITH ranked AS (
            SELECT
                time, sym, close,
                RANK() OVER (PARTITION BY time ORDER BY close DESC) AS rank_close
            FROM stocks
        )
        SELECT time, sym, close
        FROM ranked
        WHERE rank_close = 1
        ORDER BY time
    """).fetchdf()
    print(result.to_string(index=False))

    # ─── 6. 总结 ──────────────────────────────────────────────────────
    print("\n━━━ 演示完成 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  全流程: Splayed init → read → update → export CSV → DuckDB 分析")
    print()
    print("  关键点:")
    print("    · Splayed 负责高效存储: mmap 零拷贝读 + in-place 原地写")
    print("    · CSV 作为交换格式 → DuckDB 负责复杂分析")
    print("    · 生产环境可用 Arrow IPC / Parquet 替代 CSV 提升性能")
    print()
    print("  数据集目录:", DATASET_DIR)
    print("  CSV 文件:  ", CSV_PATH)


if __name__ == "__main__":
    main()
