# splayed-core（引擎核心）

**依赖**：format + codec（**不依赖 Arrow**）。提供引擎无关的扫描/写入与 `CoreBatch`。

## 接口汇总

| 分组 | 接口 | 职责 |
|---|---|---|
| 打开/元数据 | `open_dataset(dir) -> Dataset` | 打开 dataset（读 `.meta`）；`Dataset{dir, meta}` |
| | `Dataset::meta_path/field_path/list_fields/existing_fields` | 目录与字段元数据 |
| 字段读取 | `FieldReader::open(path)` | mmap（NONE）或解压（ZSTD/LZ4）打开字段 |
| | `data_type/row_count/compression/header` | 头部元数据 |
| | `read_row(row)` / `read_range_raw(start, count)` | 单行 / 零拷贝行段 |
| | `as_column_view()/column_view_range()` | 遗留列视图 |
| | `stats() -> Option<FieldStats>` | footer min/max（无 footer/失效 → None） |
| 字段写入 | `create_field(path, dt)` | 预分配全 NULL 字段 |
| | `create_field_with_data(path, dt, values)` | 建字段 + 一次写值 + 统计 footer |
| | `create_field_with_data_encoded(path, dt, values, encoding, compression)` | 建字段 + 一次写值，**数据区直接编码/压缩**（写后只读） |
| | `update_field(path, &[UpdateItem])` | 原地写；**统计增量维护**：null_count 精确、min/max 保守上界（O(k)；命中旧极值才重扫） |
| | `delete_field(path)` | 删除字段（幂等） |
| 表写入（原生） | `create_meta(dir, time_type, sym, time)` | 仅建 `.meta` |
| | `create_table(dir, time_type, sym, time, columns, sorted)` | 一步建表（`sorted=true` 走 O(n) 快路径） |
| | `create_table_with_options(..., sorted, opts: FieldWriteOptions)` | 同上，新字段直接编码/压缩（写后只读） |
| | `update_table(dir, sym, time, columns, create_missing_fields)` | 按已有格子原地更新 |
| | `update_table_with_options(..., create_missing_fields, opts)` | 同上；**新创建**字段直接编码/压缩（既有字段仍可写） |
| | `update_meta(dir, time_type, sym, time)` | **布局重排**：重建 `.meta` + 并发重散布全部 FIELD |
| 扫描 | `Scanner::new(&Dataset)` / `plan(&ScanRequest)` | 规划：SYM 剪裁 + TIME 剪裁 + 统计剪裁 → `ScanPlan` |
| | `scan(&plan, &req) -> ScanBatches` | 借用式 CoreBatch 迭代 |
| | `scan_owned(...) / scan_owned_parallel(..., n)` | 自有 / 有序并行流 |
| | `scan_all_parallel(...)` | 全收集（并行，支持 limit） |
| | `split_ranges(plan, n)` | 行均衡切片（并行分组） |
| 分区表 | `PartitionedTable::open(dir)` | 发现（单 dataset 兼容）+ schema 合并校验 + 符号并集 + key=value 目录名解析为声明式分区列 |
| | `plan(&PartitionScanRequest)` | 四层剪裁：TIME / 符号 / 统计 / 分区列 |
| | `scan(&plan, &req) -> PartitionScanBatches` | 按分区名升序流式合并 CoreBatch |
| | `create_partitioned_table / append_partition / drop_partition` | 分区写：建整表 / 追加 / 删除 |
| | `create_partitioned_table_with_options / append_partition_with_options` | 同上，新分区字段直接编码/压缩 |
| | `update_partition_table / update_partition_meta` | 表级格子写入 / 表级布局重排 |
| | `update_partition_table_with_options / update_partition_meta_with_options` | 同上，新创建字段直接编码/压缩 |
| 子集（`.sub.xxx`） | `create_subset(dir, name, &[SubsetInput])` | 建父 `.meta` 网格的行区间子集索引（每 SYM 可多条不连续区间；原子落盘） |
| | `SubsetReader::open / open_with_parent` | 读子集；`open_with_parent` 校验父 generation/time_type（`StaleParent` 检测） |
| | `symbols / contains / ranges / total_rows` | 子集元数据 |
| | `iter_ranges / iter_entries(&MetaFile)` | 父全局行序迭代（区间 / `(sym, time, row)` 三元组） |
| | `read_field_values(&FieldReader)` | 某字段在子集内的值（父行序拼接，`total_rows × sizeof(type)` 字节） |
| 请求类型 | `ScanRequest{columns, symbols, time_range, filters, batch_size, parallelism, limit}` | 一次扫描的全部下推条件 |
| | `SymbolSelection::All \| Symbols` / `TimeRange::new`, `Filter`(8 种) / `FilterValue`(全定长类型) | 下推值类型 |
| 内存模型 | `CoreBatch/CoreColumn/CoreSchema/CoreType/CoreStringDict/Buffer/Bitmap` | 引擎无关列式表示 |
| 压缩 | `compact_field(path, compression)` / `decompress_field_data(path)` | 压缩为只读 / 解压（codec 透传） |
| 错误 | `DatasetError/ReaderError/ScannerError/CreateFieldError/UpdateError/DeleteFieldError/TableError/CompactError` | 分层错误类型 |

!!! note "字段文件名规则"
    字段文件名**可以包含 `.`**（如 `close.bid`、`price.usd`），正常建表/读取/列出；但**以 `.` 开头**的文件（`.meta` 及任意隐藏/元数据文件，如 `.DS_Store`）一律**不作为字段**：

    - `Dataset::list_fields()` 忽略所有以 `.` 开头的文件（`update_meta` 的字段枚举、分区 schema 发现同规则）；
    - `create_field` / `create_field_with_data` / `create_table` / `update_table`（`create_missing_fields`）**拒绝**以 `.` 开头的字段名（`CreateFieldError::HiddenFileName`）；
    - `create_table` 的「目录必须为空」检查忽略点文件（`.gitkeep`、`.DS_Store` 等）；
    - 以 `.` 开头的目录（如 `.cache/.meta`）不作为分区。
    - `.sub.xxx`（见 [SUBSET 格式](../format/subset-format.md)）以 `.` 开头 → 同样不会被当作字段；它是父 `.meta` 网格的子集索引，与 `.meta` 并存。

## FieldReader（mmap / 解压 / 统计）

- **流程**：打开文件 → mmap → 校验 header → 尾部检测统计 footer（magic `"SFTF"`，28 字节）→ `NONE` 走 mmap 零拷贝（数据区 = `[header, data)`，footer 前），压缩则解压 payload（排除 footer）后入内存缓冲。
- **效果**：`read_range_raw` 对 `NONE` 是 mmap 切片零拷贝；`stats()` 供扫描期整表剪裁与 DataFusion `statistics()` 上报。

## 表写入：create_table / update_table / update_meta

- **入参**：`sym`/`time` 为**逐行对齐**数组（`sym[i] ↔ time[i]`）；`TableColumn{name, data_type, values}` 为输入行序原始 LE 字节。
- **create_table**：`MetaBuilder` 区分 (SYM, TIME) → 全局行序 → 每个 FIELD 按全局行缓冲（缺失时间点填 NULL 哨兵）→ `create_field_with_data` 一次写入（含统计 footer）；`sorted=true` 且输入已按 (SYM,TIME) 升序时走 O(n) 窗口游标快路径（未排序自动回退，结果恒正确）。
- **update_table**：逐 (SYM, TIME) 定位全局行 → 连续行合并为 `UpdateItem` → `update_field` 原地写；未知位置/缺失字段/类型不匹配报错；`create_missing_fields=true` 时自动创建缺失 FIELD（新列历史全 NULL）。
- **update_meta**：见 [Generation 与原子提交](../format/generation.md)。

## 直接写成压缩字段（FieldWriteOptions / create_field_with_data_encoded）

稀疏/NULL-heavy 因子列（90%~99% NULL）在 splayed 上默认定宽存储会浪费空间。两套
「**一次写入即压缩（写后只读）**」入口，磁盘布局与 `compact_field_with_encoding`
完全一致（`PLAIN+NONE` → 原样；其它 → `[u64 编码后字节数][payload]`）：

- **字段级**：`create_field_with_data_encoded(path, dt, values, encoding, compression)`
  —— 读同级 `.meta` 取权威 `total_rows`/`generation`，`values` 须覆盖全部行；
  header `null_count` 与统计 footer 均按原始值计算（压缩字段永久有效）。
- **表级**：`FieldWriteOptions{encoding, compression}`（`Default = Plain+None`，与
  既有行为一致）；`create_table_with_options` / `update_table_with_options` 及分区
  写 `_with_options` 变体，在 `opts` 非默认时**新创建**的字段直接编码/压缩。
  `update_table_with_options` 中已存在字段不受影响（仍可写 PLAIN 原地更新）。

> 写后只读：`compression != NONE`（或编码非 PLAIN）的字段 `update_field` 拒绝
> （`UpdateError::ReadOnlyAfterCompress`），适合"一次写入、多次读取"。
> 读侧无需任何改动——`FieldReader` 自动解压/解码。
> 注意：分区表要求**全分区字段集合一致**；某分区缺某组字段时，需在该分区写入
> 该字段的全 NULL 数据（而非不建该字段），否则 `PartitionedTable::open` 报
> `SchemaMismatch`。

## Scanner 扫描管线

- **`plan(&ScanRequest) -> ScanPlan{ranges, columns, total_rows}`**（三层剪裁）：
  1. **SYM 剪裁**：`SymbolSelection::Symbols` 只保留命中的 SYM INDEX 记录；
  2. **TIME 剪裁**：`TimeRange` 半开区间在各 SYM 的 time_axis 区间内二分 → 行段；
  3. **统计剪裁**：任一值 filter 与该列 footer `[min,max]` 不相交（如 `close > 204` 且 max=204）→ 整个计划 `ranges=[]`（任何 FIELD 都不读）。
- **执行**：`scan`/`scan_owned`/`scan_owned_parallel` 逐批产出 `CoreBatch`（布局 `[time(0), sym(1), fields(2+)]`）；值过滤（8 种，AND 语义）在解码后按行应用，f64/i64 有 SIMD 快路径；`limit` 在**第 N 个通过过滤的行**处截断（末批 `CoreBatch::slice`，不再拉取后续批次）。
- **并行**：`split_ranges` 把范围按目标行数切片并连续合并成 ≤n 组 → 每组一个 `std::thread` + `sync_channel(2)` 流式，`ParallelScanBatches` 按 (sym, time) 全局保序。

## 分区管理

见 [分区策略](../partitioning.md)。

## 子集（`.sub.xxx`）

`.sub.xxx` 是父 `.meta` 网格的**行区间子集索引**（"全市场 + 沪深300"场景）：
只记录选中 `(SYM, TIME)` 在父全局行空间的区间，指向 FIELD data 区；文件本身不存值。

- **写**：`create_subset(dir, name, inputs)`——输入为每符号若干连续 TIME 段（父
  `time_type` 值域）；校验符号 / TIME / 段边界，相邻段自动合并，原子落盘
  （`.sub.{name}.tmp` → fsync → rename）。
- **读**：`SubsetReader`——`open_with_parent` 校验父 `generation`/`time_type`；
  父被 `update_meta` 重排后 generation 递增 → `StaleParent`，须重建。
- **读值**：`read_field_values(&FieldReader)` 按父全局行序把某字段在子集内的值
  拼接返回（每区间 `read_range_raw` 行段读；NONE 零拷贝、压缩自动解压）。
- 与 `.meta` 的唯一结构性差异：每 SYM **可多条不连续区间**（区间记录复用 12B
  `SymIndexRecord`，格式详见 [SUBSET 格式](../format/subset-format.md) 与
  `plan.md` §5.7）。

## 内存模型

见 [CoreBatch 内存模型](../corebatch.md)。
