# Crate 一览

```text
splayed/
|
+-- splayed-format/          # 无依赖：格式定义
|   +-- meta.rs, field.rs, header.rs, types.rs, field_footer.rs
|
+-- splayed-codec/           # format ← codec：编码/压缩
|   +-- compact.rs, plain.rs, delta.rs, rle.rs, bitpack.rs
|
+-- splayed-core/            # format + codec ← core：不含 Arrow
|   +-- dataset.rs, reader.rs, field_writer.rs, scanner.rs, simd_filter.rs
|   +-- batch.rs             # CoreBatch：引擎无关内存列式
|   +-- table_writer.rs      # 原生 create_meta/create_table/update_table/update_meta
|   +-- partition.rs         # 分区管理（发现/剪裁/合并/写）
|
+-- splayed-arrow/           # core + arrow ← arrow：共享转换工具（可选用）
|   +-- arrow_conv.rs, corebatch_to_arrow.rs, meta_writer.rs, table_writer.rs
|
+-- splayed-datafusion/      # core + arrow + datafusion：TableProvider 三层对接 + SQL
|   +-- dataset.rs, table.rs, register.rs, filter.rs, convert.rs, exec.rs, stats_agg_rule.rs
|
+-- splayed-duckdb/          # core（+ 可选 arrow）：DuckDB 集成层
|   +-- native.rs, ffi.rs, arrow_bridge.rs
|
+-- splayed-polars/          # core + arrow + polars 0.45：惰性扫描
|   +-- anonymous.rs, predicate.rs, arrowconv.rs
|
+-- splayed-python/          # core + arrow(pyarrow) + pyo3：Python 扩展（import splayed）
|   +-- lib.rs, tests.rs
|
+-- splayed-adbc/            # datafusion + arrow：上层 ADBC 驱动
|
+-- splayed/                 # umbrella：核心恒有；features 开关
|
+-- splayed-cli/             # CLI：init/update/compact/read/sql/export/export-arrow
splayed-duckdb-extension/    # C++ DuckDB 扩展壳（非 cargo 成员）
example/                     # Python demo scripts
```

**依赖方向**：`format`（无依赖）← `codec` ← `core` → `arrow`（共享转换工具，被需 Arrow 的适配层复用）→ `{datafusion, duckdb, polars, python}` →（上层）`adbc`。

| Crate | 页面 |
| --- | --- |
| `splayed-core` | [核心](core.md) |
| `splayed-format` | [格式](format.md) |
| `splayed-codec` | [编码压缩](codec.md) |
| `splayed-arrow` | [Arrow 交换](arrow.md) |
| `splayed-datafusion` | [DataFusion](datafusion.md) |
| `splayed-duckdb` | [DuckDB](duckdb.md) |
| `splayed-duckdb-extension` | [DuckDB 扩展壳](duckdb-extension.md) |
| `splayed-polars` | [Polars](polars.md) |
| `splayed-python` | [Python 绑定](python.md) |
| `splayed-adbc` | [ADBC](adbc.md) |
| `splayed-cli` | [CLI](cli.md) |
| `splayed`（umbrella） | [Umbrella](umbrella.md) |
