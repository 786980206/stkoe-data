# splayed-core

引擎无关存储核心：Field（单列文件）/ META（元数据 + Index）/ Dataset（目录级封装）三层 API。

- 引擎无关：不依赖 Arrow / DataFusion / DuckDB / Polars。
- 交换结构：`ColumnView`（单列）/ `DataView`（多列），适配层负责与各引擎的零拷贝对接。
- 逻辑行空间：容量网格（三层 API 的 offset / length 含义一致，无需换算）。

## 详细文档

| 文档 | 内容 |
| --- | --- |
| [公共语义](core/index.md) | 命名规则、API 分层、RowRange / ScanRequest / Predicate / Handle 对象、逻辑行空间 |
| [Field API](core/field.md) | 文件生命周期（create/open/rename/cast/compress/decompress）+ Handle 读写扫描（read/write/update/scan/close）+ 内部实现流程 |
| [META API](core/meta.md) | MetaBuilder（run-length 构建）、read_index_handle、scan_index_handle（谓词提取）、locate_index_handle（双指针定位）+ 内部实现流程 |
| [Dataset API](core/dataset.md) | 生命周期、读写、扫描、结构操作（create/delete/rename/cast/compress field）、统计 + 内部实现流程 |
| [设计边界与性能](core/design-boundary.md) | V2.0 暂不引入的能力、性能审查记录、Benchmark vs Parquet 基线 |

## 核心语义摘要

**逻辑行空间（容量网格）**：

```
L = Σ_sym time_count(sym)
row(sym_i, time_index) = row_start(i) + (time_index - time_start(i))
```

三层 API 的 offset / length 一一对应，无需换算；sym 区间内缺失时间 = NULL 逻辑行。

**File / Handle 两层**：

```
*_file    直接操作物理文件（create / open / delete / rename / cast / compress）
*_handle  操作已打开的 Handle（read / write / scan / update / close）
```

**Scanner 契约**：

```
next()   -> Result<T?, Error>    // None = 正常结束；Err = 执行错误
close()  -> Result<()>           // 任何时刻可安全调用
```

**设计原则**：core API 保持薄，只有上层真正需要的能力才下沉到 core。
