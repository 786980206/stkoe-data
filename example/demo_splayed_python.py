"""splayed-python 演示 + 冒烟验证：在真实 CPython + pyarrow 下走通全部绑定接口。

运行（先构建 .pyd，见 docs/crates/python.md）：
    pwsh -File crates/splayed-python/build_pyd.ps1
    $env:PYTHONPATH = "$PWD\crates\splayed-python"
    python example/demo_splayed_python.py
"""
import os
import tempfile
import shutil
import pyarrow as pa

import splayed

print("version =", splayed.__version__)
print("exports =", sorted(
    n for n in dir(splayed)
    if not n.startswith("_")
))

ROOT = os.path.join(tempfile.gettempdir(), "splayed_smoke_data")


def table() -> pa.Table:
    return pa.table({
        "time": pa.array([0, 1, 0, 1], type=pa.date32()),
        "sym": pa.array(["SYM01", "SYM01", "SYM02", "SYM02"]),
        "close": pa.array([100.0, 101.0, 200.0, 201.0]),
        "vol": pa.array([1, None, 4, 5], type=pa.int64()),
    })


def batch() -> pa.RecordBatch:
    """绑定以 `pyarrow.RecordBatch` 为数据契约；Table 转 batch 用 `to_batches()[0]`。"""
    return table().to_batches()[0]


def table2() -> pa.RecordBatch:
    return pa.table({
        "time": pa.array([10, 11, 10, 11], type=pa.date32()),
        "sym": pa.array(["SYM01", "SYM01", "SYM02", "SYM02"]),
        "close": pa.array([500.0, 501.0, 600.0, 601.0]),
        "vol": pa.array([1, 2, 3, 4], type=pa.int64()),
    }).to_batches()[0]


def main() -> None:
    shutil.rmtree(ROOT, ignore_errors=True)

    # 1) 写 / 扫
    splayed.create_table(os.path.join(ROOT, "t1"), batch(), True)
    out = splayed.scan_dataset(os.path.join(ROOT, "t1"))
    assert len(out) == 1 and out[0].num_rows == 4, out
    names = out[0].schema.names
    assert names == ["time", "sym", "close", "vol"], names
    close = out[0].column("close").to_pylist()
    assert close == [100.0, 101.0, 200.0, 201.0], close
    assert out[0].column("vol").to_pylist() == [1, None, 4, 5]
    print("1) create_table + scan_dataset: OK")

    # 2) 选择 / 裁剪 / limit
    out = splayed.scan_dataset(os.path.join(ROOT, "t1"),
                               columns=["close"], symbols=["SYM02"])
    assert out[0].num_rows == 2 and out[0].schema.names == ["time", "sym", "close"], out[0]
    out = splayed.scan_dataset(os.path.join(ROOT, "t1"),
                               time_range=(0, 1), limit=1)
    assert out[0].num_rows == 1, out[0].num_rows
    print("2) selection / projection / limit: OK")

    # 3) 子集写读
    splayed.create_subset(os.path.join(ROOT, "t1"), "hs300", [
        splayed.SubsetInput("SYM01", [(0, 2)]),
        splayed.SubsetInput("SYM02", [(0, 1)]),
    ])
    sub = splayed.read_subset(os.path.join(ROOT, "t1"), "hs300")
    assert sub.num_rows == 3, sub.num_rows
    assert sub.column("close").to_pylist() == [100.0, 101.0, 200.0], sub.column("close").to_pylist()
    print("3) create_subset + read_subset: OK")

    # 4) 分区表
    splayed.create_partitioned_table(os.path.join(ROOT, "p"), "date32", [
        splayed.PartitionWriteInput("p1", batch(), True),
        splayed.PartitionWriteInput("p2", table2(), True),
    ])
    out = splayed.scan_partitioned(os.path.join(ROOT, "p"))
    total = sum(b.num_rows for b in out)
    assert total == 8, total
    closes = [v for b in out for v in b.column("close").to_pylist()]
    assert 500.0 in closes, closes
    print("4) create_partitioned_table + scan_partitioned: OK")

    # 4b) 分区追加 / 删除
    splayed.append_partition(os.path.join(ROOT, "p"), splayed.PartitionWriteInput("p3", batch(), True))
    out = splayed.scan_partitioned(os.path.join(ROOT, "p"))
    assert sum(b.num_rows for b in out) == 12, sum(b.num_rows for b in out)
    splayed.drop_partition(os.path.join(ROOT, "p"), "p2")
    out = splayed.scan_partitioned(os.path.join(ROOT, "p"))
    names = sorted({b.schema.names[0] for b in out})  # 触发合并读取
    assert sum(b.num_rows for b in out) == 8, sum(b.num_rows for b in out)
    print("4b) append_partition + drop_partition: OK")

    # 5) 错误路径
    try:
        splayed.scan_dataset(os.path.join(ROOT, "nope"))
        raise SystemExit("scan_dataset should have raised")
    except splayed.SplayedError as e:
        print("5) SplayedError raised:", str(e)[:60], "...")

    # 6) 参数错误 → ValueError
    try:
        splayed.create_table_with_options(os.path.join(ROOT, "t1"), batch(), True,
                                          encoding="bogus")
        raise SystemExit("should have raised ValueError")
    except ValueError as e:
        print("6) ValueError raised:", str(e)[:60], "...")

    shutil.rmtree(ROOT, ignore_errors=True)
    print("\nALL SMOKE TESTS PASSED")


if __name__ == "__main__":
    main()
