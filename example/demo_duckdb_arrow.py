#!/usr/bin/env python3
"""
Splayed V1 — DuckDB Arrow IPC 演示脚本
======================================

本脚本演示 Phase 9 的 Arrow IPC 桥接方案：
  Splayed → Arrow IPC → DuckDB

工作流程：
  1. init        — 用 Rust CLI 创建 Splayed 数据集
  2. update      — 原地更新数据（in-place update）
  3. export-arrow — 导出为 Arrow IPC 格式（零拷贝 Arrow 交换）
  4. DuckDB     — 用 DuckDB + pyarrow 读取 Arrow IPC 并执行复杂分析

为什么用 Arrow IPC 而非 CSV？
  - Arrow IPC 是列式二进制格式，保留了精确类型（date32、double、int64）
  - 零序列化/反序列化开销（Arrow → DuckDB 直接内存映射）
  - CSV 需要类型推断，Arrow IPC 是强类型的
  - 比 CSV 快 5-10 倍，尤其对大数据集

运行方式：
    python example/demo_duckdb_arrow.py

前置条件：
    1. 已安装 Rust + cargo
    2. cargo build -p splayed-cli
    3. pip install duckdb pyarrow
"""

import subprocess
import sys
from pathlib import Path
import shutil

import duckdb
import pyarrow.ipc as ipc

# ─── 路径配置 ────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLI_BIN = PROJECT_ROOT / "target" / "debug" / "splayed.exe"
DATASET_DIR = PROJECT_ROOT / "target" / "demo_arrow_dataset"
ARROW_FILE = PROJECT_ROOT / "target" / "demo_arrow_dataset.arrow"


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
    print("=" * 70)
    print("  Splayed V1 — DuckDB Arrow IPC 全流程演示")
    print("=" * 70)

    # ─── 1. 创建数据集 ─────────────────────────────────────────────
    if DATASET_DIR.exists():
        shutil.rmtree(DATASET_DIR)
    print("\n━━━ Step 1: 创建数据集 (Splayed create_table) ━━━━━━━━━━━━━━━━━")
    run_cli("init", str(DATASET_DIR))

    # ─── 2. 原地更新 ───────────────────────────────────────────────
    print("\n━━━ Step 2: 原地更新 (in-place update) ━━━━━━━━━━━━━━━━━━━━━━")
    run_cli("update", str(DATASET_DIR), "--field", "close", "--sym", "AAPL", "--time", "2", "--value", "199.99")
    run_cli("update", str(DATASET_DIR), "--field", "close", "--sym", "MSFT", "--time", "4", "--value", "555.55")
    print("  → AAPL[time=2] = 199.99, MSFT[time=4] = 555.55")

    # ─── 3. 导出 Arrow IPC ─────────────────────────────────────────
    print("\n━━━ Step 3: 导出 Arrow IPC (Splayed → Arrow → DuckDB) ━━━━━━━━")
    run_cli("export-arrow", str(DATASET_DIR), "--output", str(ARROW_FILE))
    print(f"  Arrow IPC 文件: {ARROW_FILE}")

    # ─── 4. DuckDB 分析 ────────────────────────────────────────────
    print("\n━━━ Step 4: DuckDB SQL 分析 (Arrow IPC → DuckDB) ━━━━━━━━━━━━━━")

    # 用 pyarrow 读取 Arrow IPC 流格式
    reader = ipc.open_stream(str(ARROW_FILE))
    table = reader.read_all()
    print(f"  ▸ Arrow 表加载: {table.num_rows} 行, {table.num_columns} 列")
    print(f"  Schema: {table.schema}")

    # 注册到 DuckDB
    con = duckdb.connect()
    con.register("splayed", table)
    print(f"\n  ▸ 原始数据 (SELECT * FROM splayed ORDER BY sym, time):")
    rows = con.execute("SELECT * FROM splayed ORDER BY sym, time").fetchall()
    for row in rows:
        print(f"    {row}")

    # ─── 复杂分析 ─────────────────────────────────────────────────
    print("\n  ▸ 窗口函数 — 3 日移动平均:")
    result = con.execute("""
        SELECT sym, time, close,
               ROUND(AVG(close) OVER (PARTITION BY sym ORDER BY time ROWS 2 PRECEDING), 2) as ma_3
        FROM splayed ORDER BY sym, time
    """).fetchall()
    for r in result:
        print(f"    {r}")

    print("\n  ▸ CTE — 日收益率 (close / LAG(close) - 1):")
    result = con.execute("""
        WITH returns AS (
            SELECT sym, time, close,
                   close / LAG(close) OVER (PARTITION BY sym ORDER BY time) - 1 AS ret
            FROM splayed
        )
        SELECT sym, time, close, ROUND(ret * 100, 2) as return_pct
        FROM returns ORDER BY sym, time
    """).fetchall()
    for r in result:
        print(f"    {r}")

    print("\n  ▸ 聚合 — 统计摘要:")
    result = con.execute("""
        SELECT sym, MIN(close) as min_close, MAX(close) as max_close,
               AVG(close) as avg_close, SUM(volume) as total_volume
        FROM splayed GROUP BY sym ORDER BY sym
    """).fetchall()
    for r in result:
        print(f"    {r}")

    print("\n  ▸ 跨股票比较 — 每日收盘价最高的股票:")
    result = con.execute("""
        WITH ranked AS (
            SELECT time, sym, close,
                   RANK() OVER (PARTITION BY time ORDER BY close DESC) as rk
            FROM splayed
        )
        SELECT time, sym, close FROM ranked WHERE rk = 1 ORDER BY time
    """).fetchall()
    for r in result:
        print(f"    {r}")

    print("\n  ▸ RANK + 过滤 — 收盘价超过 200 的股票排名:")
    result = con.execute("""
        SELECT sym, time, close,
               RANK() OVER (ORDER BY close DESC) as price_rank
        FROM splayed
        WHERE close > 200
        ORDER BY price_rank
    """).fetchall()
    for r in result:
        print(f"    {r}")

    # ─── 完成 ─────────────────────────────────────────────────────
    print("\n━━━ 演示完成 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  全流程: Splayed init → update → export-arrow → DuckDB 分析")
    print()
    print("  Arrow IPC 桥接 (Phase 9 Stage 1):")
    print("    · Splayed 负责高效存储: mmap 零拷贝读 + in-place 原地写")
    print("    · Arrow IPC 作为零拷贝交换格式（强类型、列式）")
    print("    · DuckDB 通过 pyarrow 读取 Arrow IPC，直接注册为内存表")
    print("    · 比 CSV 快 5-10 倍，保留精确类型（date32、double、int64）")
    print()
    print("  数据集目录:    ", DATASET_DIR)
    print("  Arrow IPC 文件:", ARROW_FILE)


if __name__ == "__main__":
    main()
