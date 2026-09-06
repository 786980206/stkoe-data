"""分区基准编排器（docs/benchmark-partition.md）。

驱动三引擎同规模执行分区基准：
  1. Splayed（Rust 执行器，子进程）——写 Year 分区表 + 读场景 + 源 Parquet 生成
  2. DuckDB（py-duckdb 1.5.x，进程内精确计时）——COPY TO PARTITION_BY + read_parquet
  3. Polars（python polars 1.32，进程内精确计时）——partition_by 写出 + scan_parquet

结果统一追加到 results/partition_bench.csv；三引擎 count/sum 交叉校验。
"""

import argparse
import glob
import os
import shutil
import subprocess
import sys
import time

import duckdb
import polars as pl

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH = os.path.join(REPO, "target", "release", "splayed-bench")
CSV = os.path.join(REPO, "results", "partition_bench.csv")
RUNS = 3

HEADER = ("engine,scenario,scale_rows,run_idx,time_ms,output_rows,output_bytes,"
          "physical_size,file_count")


def append_rows(rows):
    new = not os.path.exists(CSV)
    with open(CSV, "a") as f:
        if new:
            f.write(HEADER + "\n")
        for r in rows:
            f.write(",".join(str(x) for x in r) + "\n")


def dir_stats(root):
    files = glob.glob(os.path.join(root, "**", "*"), recursive=True)
    files = [f for f in files if os.path.isfile(f)]
    return sum(os.path.getsize(f) for f in files), len(files)


def splayed_side(scale, runs):
    subprocess.run([BENCH, "--mode", "partition", "--scale", str(scale),
                    "--runs", str(runs), "--out", CSV], check=True, cwd=REPO)


def read_splayed_aggs():
    aggs = {}
    p = os.path.join(REPO, "bench_data", "splayed_aggs.txt")
    with open(p) as f:
        for line in f:
            parts = line.split()
            aggs[parts[0]] = (int(parts[1]), int(parts[2]), int(parts[3]))
    return aggs


def duckdb_side(src, d, scale, runs):
    if os.path.exists(d):
        shutil.rmtree(d)
    con = duckdb.connect()

    # PW1: 分区写出（读源 Parquet + 写 Hive 分区目录，ZSTD，行组 1M）
    t0 = time.perf_counter()
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{src}')) TO '{d}' "
        f"(FORMAT PARQUET, PARTITION_BY (year), COMPRESSION ZSTD, "
        f"ROW_GROUP_SIZE 1000000)"
    )
    write_s = time.perf_counter() - t0
    physical, files = dir_stats(d)
    duck_rows = [("duckdb", "PW1", scale, 0, write_s * 1000, scale,
                  scale * 156, physical, files)]

    # PR1–PR4：聚合读（count + sum(n0) + sum(n2)）
    hive = f"read_parquet('{d}/**/*.parquet', hive_partitioning=1)"
    aggs = {}
    scenarios = [
        ("PR1", hive, None),
        ("PR2", f"read_parquet('{d}/year=2022/*.parquet')", None),
        ("PR3", hive, "\"year\" BETWEEN 2021 AND 2022"),
        ("PR4", hive, "sym = 'SYM0000' AND \"year\" = 2022"),
    ]
    for sc, src_expr, where in scenarios:
        where_sql = f" WHERE {where}" if where else ""
        # 预热
        con.execute(f"SELECT count(*), sum(n0), sum(n2) FROM {src_expr}{where_sql}").fetchall()
        best = None
        for run in range(runs):
            t0 = time.perf_counter()
            res = con.execute(
                f"SELECT count(*), sum(n0), sum(n2) FROM {src_expr}{where_sql}"
            ).fetchall()
            t = (time.perf_counter() - t0) * 1000
            if best is None or t < best:
                best = t
            cnt, s0, s2 = res[0]
        rows_ = int(cnt)
        aggs[sc] = (rows_, int(s0), int(s2))
        duck_rows.append(("duckdb", sc, scale, 0, best, rows_,
                          rows_ * 156 if sc != "PR4" else rows_ * 36, physical, files))

    # PR5: 分区元数据（逐分区元数据计数）
    year_dirs = sorted(glob.glob(os.path.join(d, "year=*")))
    t0 = time.perf_counter()
    for yd in year_dirs:
        con.execute(
            f"SELECT count(*) FROM read_parquet('{yd}/*.parquet')"
        ).fetchall()
    t = (time.perf_counter() - t0) * 1000
    duck_rows.append(("duckdb", "PR5", scale, 0, t, len(year_dirs), 0, physical, files))
    con.close()
    append_rows(duck_rows)
    return aggs


def polars_side(src, d, scale, runs):
    if os.path.exists(d):
        shutil.rmtree(d)

    # PW1: 分区写出
    t0 = time.perf_counter()
    df = pl.read_parquet(src)
    df = df.with_columns(pl.col("time").dt.year().alias("year"))
    parts = df.partition_by("year", as_dict=True)
    for key, part in parts.items():
        y = key[0]
        yd = os.path.join(d, f"year={y}")
        os.makedirs(yd, exist_ok=True)
        part.drop("year").write_parquet(os.path.join(yd, "data.parquet"),
                                        compression="zstd")
    write_s = time.perf_counter() - t0
    physical, files = dir_stats(d)
    pol_rows = [("polars", "PW1", scale, 0, write_s * 1000, scale,
                 scale * 156, physical, files)]

    # PR1–PR4
    hive = os.path.join(d, "**", "*.parquet")
    aggs = {}
    scenarios = [
        ("PR1", hive, None),
        ("PR2", os.path.join(d, "year=2022", "**", "*.parquet"), None),
        ("PR3", hive, ((pl.col("year") >= 2021) & (pl.col("year") <= 2022))),
        ("PR4", hive, (pl.col("sym") == "SYM0000") & (pl.col("year") == 2022)),
    ]
    for sc, src_expr, filt in scenarios:
        lf = pl.scan_parquet(src_expr, hive_partitioning=True)
        if filt is not None:
            lf = lf.filter(filt)
        # 预热
        lf.select(pl.len().alias("rows"), pl.col("n0").sum().alias("s0"), pl.col("n2").sum().alias("s2")).collect()
        best = None
        result = None
        for run in range(runs):
            t0 = time.perf_counter()
            result = lf.select(pl.len().alias("rows"), pl.col("n0").sum().alias("s0"), pl.col("n2").sum().alias("s2")).collect()
            t = (time.perf_counter() - t0) * 1000
            if best is None or t < best:
                best = t
        rows_ = int(result["rows"][0])
        s0 = int(result["s0"][0])
        s2 = int(result["s2"][0])
        aggs[sc] = (rows_, s0, s2)
        pol_rows.append(("polars", sc, scale, 0, best, rows_,
                         rows_ * 156 if sc != "PR4" else rows_ * 36, physical, files))

    # PR5: 分区元数据
    year_dirs = sorted(glob.glob(os.path.join(d, "year=*")))
    t0 = time.perf_counter()
    for yd in year_dirs:
        pl.scan_parquet(os.path.join(yd, "**", "*.parquet")).select(
            pl.len()).collect()
    t = (time.perf_counter() - t0) * 1000
    pol_rows.append(("polars", "PR5", scale, 0, t, len(year_dirs), 0, physical, files))
    append_rows(pol_rows)
    return aggs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scales", default="1,5,10,20")
    ap.add_argument("--runs", type=int, default=3)
    args = ap.parse_args()
    runs = args.runs
    scales = [int(s) for s in args.scales.split(",")]

    if os.path.exists(CSV):
        os.remove(CSV)

    for millions in scales:
        scale = millions * 1_000_000
        print(f"=== partition scale {millions}M rows ===")
        splayed_side(scale, runs)
        sp_aggs = read_splayed_aggs()

        src = os.path.join(REPO, "bench_data", "source.parquet")
        db_aggs = duckdb_side(src, os.path.join(REPO, "bench_data", "duckdb_partition"),
                              scale, runs)
        pl_aggs = polars_side(src, os.path.join(REPO, "bench_data", "polars_partition"),
                              scale, runs)

        # 交叉校验：count + sum(n0) + sum(n2) 三引擎一致
        for sc in sp_aggs:
            r_sp, s0_sp, s2_sp = sp_aggs[sc]
            r_db, s0_db, s2_db = db_aggs[sc]
            r_pl, s0_pl, s2_pl = pl_aggs[sc]
            assert r_sp == r_db == r_pl, f"{sc} rows: {r_sp}/{r_db}/{r_pl}"
            assert s0_sp == s0_db == s0_pl, f"{sc} sum(n0): {s0_sp}/{s0_db}/{s0_pl}"
            assert s2_sp == s2_db == s2_pl, f"{sc} sum(n2): {s2_sp}/{s2_db}/{s2_pl}"
        print("correctness: splayed == duckdb == polars (all scenarios)")

        # 清理：下一规模前释放磁盘
        for d in ["bench_data/duckdb_partition", "bench_data/polars_partition",
                  "bench_data/source.parquet"]:
            p = os.path.join(REPO, d)
            if os.path.isdir(p):
                shutil.rmtree(p)
            elif os.path.exists(p):
                os.remove(p)
    print("done")


if __name__ == "__main__":
    main()
