#!/usr/bin/env python3
"""
Splayed V1 — DataFusion 示例脚本
=================================

本脚本通过调用 Splayed V1 的 Rust CLI 工具 (splayed-cli)，完整演示
一个 SYM × TIME × FIELD 金融时序数据库的全生命周期：

  1. init     — 创建示例数据集（3 只股票 × 5 天 × 2 个字段）
  2. read     — 读取原始字段值（close / volume）
  3. update   — 原地更新某个 cell（in-place update，不扩展文件）
  4. compact  — 用 ZSTD 压缩字段文件（压缩后变为只读）
  5. sql      — 通过 DataFusion TableProvider 执行 SQL 查询
                 包括：SELECT / WHERE / GROUP BY / 聚合函数

运行方式：
    python example/demo_datafusion.py

前置条件：
    1. 已安装 Rust + cargo
    2. 在项目根目录执行过: cargo build -p splayed-cli
    3. Python 3.8+（无需第三方库，仅用标准库 subprocess）

设计理念（参见 plan.md）：
    - META 决定 "去哪里读"（SYM INDEX 定位行范围）
    - FIELD 决定 "读什么"（一个字段一个文件，mmap 零拷贝）
    - 全量预声明：time_count = 行容量，update_field 原地覆写
    - NULL 用哨兵位模式编码（float 用 canonical NaN），无 bitmap
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

# ─── 路径配置 ──────────────────────────────────────────────────────────
# CLI 二进制路径（cargo build 后生成）
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLI_BIN = PROJECT_ROOT / "target" / "debug" / "splayed.exe"

# 临时数据集目录
DATASET_DIR = PROJECT_ROOT / "target" / "demo_dataset"


def run_cli(*args: str) -> str:
    """
    调用 splayed CLI 工具，返回 stdout 输出。
    如果命令失败则打印 stderr 并退出。
    使用 UTF-8 编码读取输出（避免 Windows GBK 编码问题）。
    """
    cmd = [str(CLI_BIN)] + list(args)
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        print(f"  ✗ ERROR (exit {result.returncode}): {result.stderr}")
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
    print("  Splayed V1 — DataFusion 全流程演示")
    print("=" * 70)

    # ─── 1. 创建示例数据集 ────────────────────────────────────────────
    # create_table 一次性完成: create_meta + create_field + update_field
    # 生成 3 只股票 (AAPL/GOOG/MSFT) × 5 天 × 2 个字段 (close/volume)
    print("\n━━━ Step 1: 创建数据集 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    run_cli("init", str(DATASET_DIR))

    # ─── 2. 读取原始字段值 ────────────────────────────────────────────
    # FieldReader 通过 mmap 直接读取 FIELD 文件的数据区域（零拷贝）
    print("\n━━━ Step 2: 读取字段值 (mmap 零拷贝) ━━━━━━━━━━━━━━━━━━━━━━━━")
    run_cli("read", str(DATASET_DIR), "--field", "close")
    run_cli("read", str(DATASET_DIR), "--field", "volume")

    # ─── 3. 原地更新 ──────────────────────────────────────────────────
    # update_field 在绝对行偏移处覆写数据，不扩展文件
    # generation 递增，mmap 中的旧数据因 generation 校验而失效
    print("\n━━━ Step 3: 原地更新 (in-place update) ━━━━━━━━━━━━━━━━━━━━━")
    run_cli("update", str(DATASET_DIR),
            "--field", "close", "--sym", "AAPL", "--time", "0", "--value", "199.99")

    # 验证更新结果
    print("\n  → 验证更新后的 close 字段:")
    run_cli("read", str(DATASET_DIR), "--field", "close", "--sym", "AAPL")

    # ─── 4. SQL 查询 (DataFusion TableProvider) ───────────────────────
    # SplayedTableProvider 实现了 DataFusion 的 TableProvider trait
    # 支持: 投影下推 (只读请求的字段) + 谓词下推 (SYM→SymbolSelection, TIME→TimeRange)
    print("\n━━━ Step 4: DataFusion SQL 查询 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # 4a. 全表查询
    print("\n  ▸ SELECT * — 全表扫描 (投影下推: 所有字段)")
    run_cli("sql", str(DATASET_DIR), "--query", "SELECT * FROM splayed LIMIT 10")

    # 4b. 投影 + 过滤
    print("  ▸ SELECT close WHERE sym='GOOG' — 投影下推 + SYM 过滤下推")
    run_cli("sql", str(DATASET_DIR),
            "--query", "SELECT close FROM splayed WHERE sym = 'GOOG'")

    # 4c. 时间范围过滤
    print("  ▸ SELECT * WHERE time >= 2 — TIME 范围下推")
    run_cli("sql", str(DATASET_DIR),
            "--query", "SELECT sym, time, close FROM splayed WHERE time >= 2")

    # 4d. 聚合查询
    print("  ▸ GROUP BY + AVG — 聚合函数")
    run_cli("sql", str(DATASET_DIR),
            "--query", "SELECT sym, AVG(close) AS avg_close, SUM(volume) AS total_vol "
                       "FROM splayed GROUP BY sym ORDER BY sym")

    # 4e. 值过滤 (close > 200)
    print("  ▸ WHERE close > 200 — 值过滤下推到 Scanner")
    run_cli("sql", str(DATASET_DIR),
            "--query", "SELECT sym, time, close FROM splayed WHERE close > 200 ORDER BY close")

    # ─── 5. 压缩字段 (ZSTD) ──────────────────────────────────────────
    # compact_field 将 PLAIN+NONE 转为 PLAIN+ZSTD
    # 压缩后字段变为只读，update_field 会被拒绝
    # 注意：压缩后的 SQL 查询需要通过 decompress_field_data 路径，
    # 目前 Scanner 的 mmap 直接读取路径不支持压缩数据解压。
    # 压缩主要用于冷数据归档场景。
    print("\n━━━ Step 5: 压缩字段 (compact_field, ZSTD) ━━━━━━━━━━━━━━━━━━")
    run_cli("compact", str(DATASET_DIR), "--field", "close", "--algo", "zstd")
    print("  (压缩后字段变为只读，适合冷数据归档)")

    # ─── 6. 总结 ──────────────────────────────────────────────────────
    print("\n━━━ 演示完成 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"  数据集目录: {DATASET_DIR}")
    print("  核心流程:")
    print("    create_table → read → update_field → sql → compact_field → sql")
    print()
    print("  你可以手动探索:")
    print(f"    {CLI_BIN} read {DATASET_DIR} --field volume")
    print(f"    {CLI_BIN} sql  {DATASET_DIR} --query \"SELECT MAX(close) FROM splayed\"")


if __name__ == "__main__":
    main()
