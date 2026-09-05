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

**实现原则**（各 API 的内部流程见对应文档；性能审查记录见 [design-boundary](core/design-boundary.md)）：

- 读零拷贝：`read_field_handle` 只做逻辑行范围 → `ColumnView` 转换（PLAIN mmap 切片 / compressed working 切片），
  不复制、不合并、不求谓词；compressed 定位为 chunk_ends 二分。
- 写批量位操作：values 逐段 memcpy；validity 按字节/word 批量（头尾掩码 RMW、中间同相位 memcpy、
  word popcount），不逐 bit；null_count 按覆盖区前后 1 位数增量维护。
- 扫描批量管线：候选 ranges 顺序直接消费（不 merge）；整段连续 values 类型化比较循环（可自动向量化）
  → 0/1 字节掩码根部统一 validity 求交 → 打包位图 → word 级连续命中区输出。
- META 构建连续子区间原则：每个 sym 的 time 必须是 TIME AXIS 的连续子区间，SYM INDEX 只存
  `row_start + time_start + time_count`；轴定位二分（无哈希表），span == 数据行数构建期校验。
- 结构操作流式：close（compressed 收尾）/ cast / compress / decompress 均 tmp 顺序写 + `sync_all`
  后原子 rename，内存 O(批次 / 单个 chunk)，不全量物化、不全量解压；失败清理 tmp，原文件保持不变。
- Dataset 三阶段并发模型：主线程完成校验 + `ensure_field`（缓存管理永不进入并行热路径）
  → Field 级并行只做纯 I/O（`std::thread::scope` round-robin 分桶：write_dataset 总字节 ≥ 1 MiB、
  scan_dataset 候选 ≥ 64K 行才并行，小负载走串行快路径）→ 主线程收尾（求交 / 合并 / 组装）；
  `read_dataset` 为纯零拷贝切片恒单线程；并行度 `max_parallelism` 由最上层控制。
