# splayed-python：V2.1 设计文档与 Python 绑定使用指南

## 1. 定位与架构

`splayed-python`（Python 包名为 `splayed`）是基于 `splayed-arrow` 和 PyO3 构建的高性能 Python 绑定扩展库。
它使得 Python / 数据科学 / 量化投研生态（Pandas、Polars、PyArrow、DuckDB 等）能够以**极高吞吐、零开销、全流式**的方式读写 Splayed 列式时序存储引擎。

### 核心特性

- **跨多个 Python 版本支持（C-ABI3 兼容）**：采用 PyO3 `abi3-py38` 规范编译，单个 wheel 包直接兼容 **Python 3.8、3.9、3.10、3.11、3.12、3.13 及以上**版本，无需针对每个小版本重新编译。
- **与 PyArrow 生态零成本互通**：底层直接通过 Arrow C Data Interface 和 PyCapsule / PyArrow 原生对象完成转换；输入输出自动对接 `pyarrow.RecordBatch` 与 `pyarrow.Table`。
- **纯面向对象对称设计**：与 Rust 核心层完全对齐，提供 **`TableWriter`**、**`TableReader`** 和流式迭代器 **`TableBatchReader`**。
- **Out-of-Core 流式读写**：支持设定 `batch_size` 逐批迭代，避免将数十 GB/TB 的大规模时序数据集一次性载入内存。
- **写入自动建列与原生 DDL**：`write` 写入时若包含全新因子列将自动建列自愈对齐，并支持 `init_field`（最新分区新增单列骨架）/ `rename_field` / `delete_field` / `cast_field` / `compress_field` 原生结构调整。

---

## 2. API 规范总览

### 2.1 基础常量：`PartitionScheme`

| 常量 | 字符串值 | 说明 |
| --- | --- | --- |
| `PartitionScheme.NONE` | `"none"` | 单目录无分区（单 Partition） |
| `PartitionScheme.YEAR` | `"year"` | 按年分区（如 `year=2026/`） |
| `PartitionScheme.MONTH` | `"month"` | 按月分区（如 `month=2026-03/`） |
| `PartitionScheme.DAY` | `"date"` | 按日分区（如 `date=2026-03-31/`） |

---

### 2.2 写对象：`TableWriter`

```python
class TableWriter:
    @classmethod
    def create(
        cls,
        path: str,
        schema: pyarrow.Schema,
        scheme: str = PartitionScheme.NONE,
        initial_partition: Optional[str] = None,
        max_parallelism: Optional[int] = None,
    ) -> TableWriter:
        """Schema 驱动创建表骨架（不写数据）。"""

    @classmethod
    def init(
        cls,
        path: str,
        batch: Union[pyarrow.RecordBatch, pyarrow.Table],
        scheme: str = PartitionScheme.NONE,
        max_parallelism: Optional[int] = None,
    ) -> TableWriter:
        """连带数据初始化整表。"""

    @classmethod
    def open(cls, path: str, max_parallelism: Optional[int] = None) -> TableWriter:
        """打开已有表以进行写操作。"""

    def write(self, data: Union[pyarrow.RecordBatch, pyarrow.Table]) -> None:
        """写入数据：自动按分区切分并分发；若包含表中尚不存在的新列，自动为各分区建列自愈对齐并写入数据。"""

    def update(self, data: Union[pyarrow.RecordBatch, pyarrow.Table]) -> None:
        """全量原子替换表数据（自动新建缺失分区）。"""

    def delete_partition(self, partition_name: str) -> None:
        """彻底物理删除指定分区。"""

    def init_field(self, field_name: str, data_type: str, opts: Optional[dict] = None) -> None:
        """在已有表中初始化新增单列（只在最新分区创建对应字段物理文件，无需指定 rows，自动与最新分区的 index 行数对齐，产出 64B 全 NULL 延迟展开骨架）。"""

    def delete_field(self, name: str) -> None:
        """删除字段物理文件。"""

    def rename_field(self, name: str, new_name: str) -> None:
        """原子重命名物理字段。"""

    def cast_field(self, name: str, target_type: str) -> None:
        """原地类型转换。"""

    def compress_field(self, name: str) -> None:
        """物理压缩字段。"""

    def decompress_field(self, name: str) -> None:
        """物理反压缩字段。"""

    def fix(self) -> None:
        """检查并修复表物理存储与元数据完整性：清理分区空文件夹，并自动将历史中间分区字段对齐补齐至最新分区。"""

    def as_reader(self) -> TableReader:
        """转为只读 TableReader 对象。"""

    def schema(self) -> pyarrow.Schema:
        """获取 Arrow Schema。"""

    def statistics(self) -> dict:
        """获取统计信息（包含 row_count, partition_count, time_min, time_max 等）。"""

    def metadata(self) -> dict:
        """获取元数据信息。"""

    def path(self) -> str:
        """返回存储路径。"""

    def scheme(self) -> str:
        """返回分区方案。"""

    def close(self) -> None:
        """关闭写对象。"""

    def remove(self) -> None:
        """彻底销毁整张表及其底层物理目录。"""
```

---

### 2.3 读对象：`TableReader` 与 `TableBatchReader`

```python
class TableReader:
    @classmethod
    def open(cls, path: str, max_parallelism: Optional[int] = None) -> TableReader:
        """只读模式打开表。"""

    def read(
        self,
        symbols: Optional[List[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> TableBatchReader:
        """带下推谓词与流式批次切分的迭代器。"""

    def read_all(
        self,
        symbols: Optional[List[str]] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> pyarrow.Table:
        """一步式便捷读取为完整的 pyarrow.Table。"""

    def schema(self) -> pyarrow.Schema:
        """获取 Arrow Schema。"""

    def statistics(self) -> dict:
        """获取统计信息。"""

    def metadata(self) -> dict:
        """获取元数据信息。"""

    def path(self) -> str: ...
    def scheme(self) -> str: ...
    def close(self) -> None: ...


class TableBatchReader:
    def __iter__(self) -> Iterator[pyarrow.RecordBatch]: ...
    def __next__(self) -> pyarrow.RecordBatch: ...
    def next(self) -> Optional[pyarrow.RecordBatch]: ...
    def read_all(self) -> pyarrow.Table: ...
    def close(self) -> None: ...
```

---

## 3. 全 API 完整使用示例 (Full API Example)

下面提供了一个涵盖表创建、数据写入与更新、缺列自愈、DDL 变更、流式读取、下推过滤、转换为 Pandas DataFrame 及生命周期销毁的完整可执行代码：

```python
import os
import pyarrow as pa
import splayed

def main():
    table_path = "./data/stock_market_table"

    # =========================================================================
    # 1. 定义 Schema 并通过 TableWriter::create 建立 Schema 驱动骨架
    # =========================================================================
    schema = pa.schema([
        ("sym", pa.dictionary(pa.int32(), pa.utf8())),   # 标的代码（自动利用字典编码）
        ("time", pa.timestamp("us")),                     # 全局单调时间戳
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
    ])

    # 支持分区方案：PartitionScheme.NONE / YEAR / MONTH / DAY
    writer = splayed.TableWriter.create(
        table_path,
        schema=schema,
        scheme=splayed.PartitionScheme.MONTH,
        max_parallelism=4,
    )
    print("TableWriter created at:", writer.path())
    print("Initial row count:", writer.statistics()["row_count"])

    # =========================================================================
    # 2. 构造 PyArrow 数据并通过 writer.update() 写入
    # =========================================================================
    # 构造 2 只标的各 2 个时间戳，共 4 行数据
    sym_indices = pa.array([0, 0, 1, 1], type=pa.int32())
    sym_dict = pa.array(["AAPL", "MSFT"])
    sym_array = pa.DictionaryArray.from_arrays(sym_indices, sym_dict)
    time_array = pa.array([1_000_000, 2_000_000, 1_000_000, 2_000_000], type=pa.timestamp("us"))
    open_array = pa.array([150.0, 151.0, 300.0, 302.0], type=pa.float64())
    high_array = pa.array([152.0, 153.0, 304.0, 305.0], type=pa.float64())
    low_array  = pa.array([149.0, 150.5, 298.0, 301.0], type=pa.float64())
    close_array= pa.array([151.5, 152.5, 303.0, 304.5], type=pa.float64())

    batch = pa.record_batch(
        [sym_array, time_array, open_array, high_array, low_array, close_array],
        schema=schema,
    )

    # 写入数据（支持传入 RecordBatch 或 Table）
    writer.update(batch)
    print("Statistics after update:", writer.statistics())

    # =========================================================================
    # 3. 写入与自动建列自愈：直接通过 writer.write 灌入含全新因子字段的数据
    # =========================================================================
    extended_cols = list(batch.columns)
    # 新增一个多级嵌套命名的因子列 "factor.alpha001"
    extended_cols.append(pa.array([0.88, 0.92, 0.45, 0.51], type=pa.float64()))
    extended_schema = batch.schema.append(pa.field("factor.alpha001", pa.float64()))
    extended_batch = pa.record_batch(extended_cols, schema=extended_schema)

    # writer.write 会自动检测缺失列并就地创建对应的物理存储列
    writer.write(extended_batch)
    print("Fields after write auto-columns:", writer.schema().names)

    # =========================================================================
    # 4. 字段级 DDL：动态初始化新字段、重命名与删除字段
    # =========================================================================
    writer.init_field("volume", "int64")
    print("Fields after init volume:", writer.schema().names)

    writer.rename_field("volume", "vol")
    print("Fields after rename volume -> vol:", writer.schema().names)

    writer.delete_field("vol")
    print("Fields after delete vol:", writer.schema().names)

    # =========================================================================
    # 5. TableReader 读取：支持一步式全量与 Out-of-Core 批次流式
    # =========================================================================
    # 可以直接由 writer.as_reader() 获取，或通过 TableReader.open(table_path) 打开
    reader = writer.as_reader()

    # 5.1 一步式全量读取为 pyarrow.Table 并转为 Pandas DataFrame
    full_table = reader.read_all()
    df = full_table.to_pandas()
    print("\n--- Pandas DataFrame Output ---\n", df)

    # 5.2 带谓词下推与字段投影读取
    projected = reader.read_all(
        symbols=["AAPL"],
        columns=["sym", "close", "factor.alpha001"],
        limit=1,
    )
    print("\n--- Projected AAPL Output ---\n", projected.to_pandas())

    # 5.3 Out-of-Core 逐批流式迭代读取（内存恒定）
    print("\n--- Streaming RecordBatches (batch_size=2) ---")
    stream = reader.read(batch_size=2)
    for i, chunk in enumerate(stream):
        print(f"Batch {i + 1}: rows={len(chunk)}, cols={chunk.num_columns}")
    stream.close()

    # =========================================================================
    # 6. 生命周期管理与物理清理
    # =========================================================================
    reader.close()
    writer.remove()  # 彻底删除整个物理表及其磁盘文件
    print("\nTable successfully removed from disk!")

if __name__ == "__main__":
    main()
```
