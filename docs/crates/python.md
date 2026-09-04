# splayed-python（Python 绑定，pyo3）

**依赖**：core + arrow（`pyarrow` feature）+ pyo3 0.29 + splayed-arrow。以 **pyarrow** 作为
表格交换层（Arrow C data interface，零拷贝），镜像 `splayed-arrow` 的写 / 读 / 分区 / 子集接口。

## 定位

让 Python 直接读写 Splayed V1 数据集：写接口接受 `pyarrow.RecordBatch`
（TIME + SYM + 字段列），读接口返回 `pyarrow.RecordBatch`，可直接 `t.to_pandas()` 或进
polars / datafusion。与 `splayed-polars` / `splayed-datafusion` 不同，这里不引入 DataFrame
依赖——只做「pyarrow ⇄ Splayed」的交换，SQL/DataFrame 由 Python 生态自己选。

## 构建

```powershell
# 一键构建 crates/splayed-python/splayed.pyd（release + extension-module）
pwsh -File crates/splayed-python/build_pyd.ps1

# 手动等价命令（crate-type 默认只出 rlib，用 --config 按需覆盖为 cdylib+rlib）：
#   $env:PYO3_PYTHON = "C:\...\python.exe"   # 可选：指定解释器（默认用 PATH 上的 python）
#   $cfg = "lib.crate-type=['cdylib','rlib']"            # 经变量传，避免 PowerShell 剥引号
#   cargo build -p splayed-python --release --features extension-module --config $cfg
#   Copy-Item target\release\splayed.dll crates\splayed-python\splayed.pyd
```

- 产物 `crates/splayed-python/splayed.pyd` 需位于 `sys.path`（如把 `crates/splayed-python`
  加入 `PYTHONPATH`）才能 `import splayed`。
- `--features extension-module`：.pyd 不链接 libpython（可移植性更好）；默认（测试/开发）
  链接本机 Python，便于 `cargo test` 内嵌解释器跑绑定测试。
- 本机要求：Python 3.x（含头文件 + import lib）+ `pip install pyarrow`。
- 快速验证：`python example/demo_splayed_python.py`（先构建 .pyd，并设
  `PYTHONPATH=crates/splayed-python`）。

## 接口

所有写接口的 `t` 参数为 `pyarrow.RecordBatch`；`time` 列须为 `date32` 或
`timestamp[us]`，`sym` 列为 `utf8`，其余为字段列。`encoding`：`plain|delta|rle|bitpack`；
`compression`：`none|zstd|lz4`；`time_type`：`date32|timestamp_us`。

### 写：单 dataset

| 接口 | 说明 |
|---|---|
| `create_meta(dir, t, sorted=True)` | 建 `.meta`（只取 TIME + SYM 列） |
| `create_table(dir, t, sorted=True)` | 一次性建 `.meta` + 全部字段 |
| `create_table_with_options(dir, t, sorted=True, encoding="plain", compression="none")` | 新字段直接以编码/压缩落盘（写后只读） |
| `update_table(dir, t, create_missing_fields=False)` | 原地更新既有字段 |
| `update_table_with_options(dir, t, create_missing_fields=False, encoding=..., compression=...)` | 新字段编码/压缩；已存在字段仍原地更新 |
| `update_meta(dir, t)` | 用新 (SYM, TIME) 布局重写 `.meta` 并重散布全部字段 |

### 写：分区表

| 接口 | 说明 |
|---|---|
| `create_partitioned_table(root, time_type, partitions, encoding="plain", compression="none")` | 一次建整个分区表；`partitions: list[PartitionWriteInput]` |
| `append_partition(root, partition, ...)` | 追加一个分区（schema/命名风格/分区列校验） |
| `update_partition_table(root, t, create_missing_fields=False, target_partition=None, ...)` | 表级格子写入（跨分区路由） |
| `update_partition_meta(root, partitions, ...)` | 表级布局重排（新增/重排/删除分区） |
| `drop_partition(root, name)` | 删除一个分区目录 |

`PartitionWriteInput(name: str, table: RecordBatch, sorted: bool = True)`：`name` = 目录名，
可含 `key=value` 分区列（如 `"month=2026-07"`）。

### 写：子集

`create_subset(dir, name, inputs)`：`inputs: list[SubsetInput]`；
`SubsetInput(sym: str, segments: list[tuple[int, int]])`——每段为 `(time_value, count)`
连续时间点（`time_value` 值域 = 父 `.meta` 的 `time_type`）。

### 读

| 接口 | 说明 |
|---|---|
| `scan_dataset(dir, columns=None, symbols=None, time_range=None, batch_size=65536, parallelism=1, limit=None)` | → `list[pyarrow.RecordBatch]`；列 = time/sym/...columns；`symbols`/`time_range=[start,end)`/`limit` 下推 |
| `scan_partitioned(dir, ...)` | 分区表扫描（按分区名升序合并，同 `scan_dataset` 下推） |
| `read_subset(dir, name, columns=None)` | `.sub.{name}` → `pyarrow.RecordBatch`（父全局行序） |

## 错误

所有 splayed 侧错误抛 `splayed.SplayedError`（`RuntimeError` 子类，`str(e)` 含底层信息）。
参数类错误（未知 encoding/compression/time_type）抛 `ValueError`。

## 示例

```python
import pyarrow as pa
import splayed

t = pa.table({
    "time": pa.array([0, 1, 0, 1], type=pa.date32()),
    "sym":  pa.array(["SYM01", "SYM01", "SYM02", "SYM02"]),
    "close": pa.array([100.0, 101.0, 200.0, 201.0]),
    "vol":  pa.array([1, None, 4, 5], type=pa.int64()),
})

splayed.create_table("/tmp/ds", t.to_batches()[0], sorted=True)

# 读：全字段扫描
batches = splayed.scan_dataset("/tmp/ds")
df = pa.Table.from_batches(batches).to_pandas()          # 4 行

# 读：只取 SYM02 + 列裁剪 + 前 1 行
batches = splayed.scan_dataset("/tmp/ds", columns=["close"],
                              symbols=["SYM02"], limit=1)

# 子集：SYM01 两天 + SYM02 只取 day0
splayed.create_subset("/tmp/ds", "hs300", [
    splayed.SubsetInput("SYM01", [(0, 2)]),
    splayed.SubsetInput("SYM02", [(0, 1)]),
])
sub = splayed.read_subset("/tmp/ds", "hs300")            # 3 行（父全局行序）

# 分区表
splayed.create_partitioned_table("/tmp/part", "date32", [
    splayed.PartitionWriteInput("2026-07", t.to_batches()[0], True),
])
batches = splayed.scan_partitioned("/tmp/part")
```

使用指南见 [Python 示例](../integrations/examples.md)。
