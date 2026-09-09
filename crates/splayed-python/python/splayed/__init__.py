from __future__ import annotations

import os
from typing import Any, Sequence, Optional, Union
from pathlib import Path

from ._splayed import (
    TableReader,
    TableWriter,
    TableBatchReader,
    PartitionScheme,
)

__all__ = [
    "TableReader",
    "TableWriter",
    "TableBatchReader",
    "PartitionScheme",
    "scan_splayed",
    "sink_splayed",
]


def scan_splayed(
    path: str | Path,
    *,
    allow_filter: bool = True,
    batch_size: int | None = None,
) -> Any:
    """
    以惰性方式扫描 Splayed 表，返回 Polars LazyFrame。

    支持 Polars 原生 Lazy 下推优化：
    - Projection Pushdown：仅解包和读取被请求的列；
    - Predicate Pushdown：自动解析并下推过滤表达式；
    - Limit Pushdown：自动提前截断批次并尽早终止扫描。

    参数:
        path: Splayed 表目录路径。
        allow_filter: 是否允许将谓词表达式下推至存储层求值（默认 True）。
        batch_size: 读取每个 RecordBatch 的最大行数。
    """
    try:
        import polars as pl
        import pyarrow as pa
        import pyarrow.compute
    except ImportError as err:
        raise ImportError("scan_splayed 需要安装 polars 和 pyarrow: pip install polars pyarrow") from err

    path_str = str(path)
    reader = TableReader.open(path_str)
    arrow_schema = reader.schema()

    def _scan_fn(
        columns: list[str] | None,
        predicate: str | None,
        n_rows: int | None,
    ) -> pl.DataFrame:
        filter_expr = None
        if predicate and allow_filter:
            try:
                from polars._utils.convert import to_py_date, to_py_datetime, to_py_time, to_py_timedelta
                from polars.datatypes import Date, Datetime, Duration
                env = {
                    "pa": pa,
                    "Date": Date,
                    "Datetime": Datetime,
                    "Duration": Duration,
                    "to_py_date": to_py_date,
                    "to_py_datetime": to_py_datetime,
                    "to_py_time": to_py_time,
                    "to_py_timedelta": to_py_timedelta,
                }
                filter_expr = eval(predicate, env)
            except Exception:
                filter_expr = None

        # 投影下推：仅提取请求的列（若包含过滤谓词所需的列，则合并）
        requested_cols = list(columns) if columns is not None else None
        read_cols = None
        if requested_cols is not None:
            read_cols_set = set(requested_cols)
            if filter_expr is not None:
                # 确保过滤涉及的列也在读取范围内
                for name in arrow_schema.names:
                    if name in str(predicate):
                        read_cols_set.add(name)
            read_cols = list(read_cols_set)

        # 执行底层的流式读取
        batch_reader = reader.read(columns=read_cols, limit=n_rows, batch_size=batch_size)
        batches = list(batch_reader)
        if not batches:
            tbl = pa.Table.from_batches([], schema=arrow_schema)
        else:
            tbl = pa.Table.from_batches(batches)

        # 谓词过滤求值
        if filter_expr is not None and tbl.num_rows > 0:
            tbl = tbl.filter(filter_expr)

        # 最终投影裁剪
        if requested_cols is not None:
            # 过滤掉不存在的列以防异常
            valid_cols = [c for c in requested_cols if c in tbl.column_names]
            tbl = tbl.select(valid_cols)

        return pl.from_arrow(tbl)

    return pl.LazyFrame._scan_python_function(arrow_schema, _scan_fn, pyarrow=allow_filter)


def sink_splayed(
    data: Any,
    path: str | Path,
    *,
    scheme: str = "none",
    max_parallelism: int | None = None,
) -> None:
    """
    将 Polars DataFrame 或 LazyFrame 导出写入 Splayed 存储。

    自动判定表物理存储状态：
    - 若目标表不存在：自动调用 `TableWriter.init` 执行建表并写入首批数据；
    - 若目标表已存在：自动调用 `TableWriter.open` 并通过 `.write()` 执行覆盖写入。

    参数:
        data: polars.DataFrame 或 polars.LazyFrame 对象。
        path: 目标表根目录。
        scheme: 分区策略，可选 'none' | 'day' | 'month' | 'year'（仅建表时生效，默认 'none'）。
        max_parallelism: 并发执行度。
    """
    try:
        import polars as pl
        import pyarrow as pa
    except ImportError as err:
        raise ImportError("sink_splayed 需要安装 polars 和 pyarrow: pip install polars pyarrow") from err

    if isinstance(data, pl.LazyFrame):
        df = data.collect()
    elif isinstance(data, pl.DataFrame):
        df = data
    else:
        raise TypeError(f"sink_splayed 需要 polars DataFrame 或 LazyFrame，实际收到: {type(data)}")

    arrow_table = df.to_arrow()
    batches = arrow_table.to_batches()
    if not batches:
        return

    path_str = str(path)
    path_obj = Path(path_str)

    # 判定表物理元数据是否存在
    meta_file = path_obj / ".meta"
    has_meta = meta_file.exists()
    if not has_meta and path_obj.is_dir():
        # 检查是否已有分区子目录
        for child in path_obj.iterdir():
            if child.is_dir() and (child / ".meta").exists():
                has_meta = True
                break

    if not has_meta:
        # 首次初始化建表并写入首个批次
        writer = TableWriter.init(path_str, batches[0], scheme=scheme, max_parallelism=max_parallelism)
        for b in batches[1:]:
            writer.write(b)
    else:
        # 已存在表：打开并覆盖写
        writer = TableWriter.open(path_str, max_parallelism=max_parallelism)
        for b in batches:
            writer.write(b)


# 注册 Polars 扩展命名空间与链式调用方法
def _register_polars_extensions():
    try:
        import polars as pl

        # 1. 注册扩展命名空间 lf.splayed.sink(...) 与 df.splayed.sink(...)
        @pl.api.register_lazyframe_namespace("splayed")
        class _SplayedLazyNamespace:
            def __init__(self, ldf: pl.LazyFrame):
                self._ldf = ldf

            def sink(self, path: str | Path, scheme: str = "none", max_parallelism: int | None = None) -> None:
                sink_splayed(self._ldf, path, scheme=scheme, max_parallelism=max_parallelism)

        @pl.api.register_dataframe_namespace("splayed")
        class _SplayedDataNamespace:
            def __init__(self, df: pl.DataFrame):
                self._df = df

            def sink(self, path: str | Path, scheme: str = "none", max_parallelism: int | None = None) -> None:
                sink_splayed(self._df, path, scheme=scheme, max_parallelism=max_parallelism)

        # 2. 直接为 LazyFrame 和 DataFrame 打上原生链式方法 .sink_splayed(...)
        if not hasattr(pl.LazyFrame, "sink_splayed"):
            pl.LazyFrame.sink_splayed = lambda self, path, scheme="none", max_parallelism=None: sink_splayed(
                self, path, scheme=scheme, max_parallelism=max_parallelism
            )
        if not hasattr(pl.DataFrame, "sink_splayed"):
            pl.DataFrame.sink_splayed = lambda self, path, scheme="none", max_parallelism=None: sink_splayed(
                self, path, scheme=scheme, max_parallelism=max_parallelism
            )
        if not hasattr(pl, "scan_splayed"):
            pl.scan_splayed = scan_splayed

    except Exception:
        pass


_register_polars_extensions()
