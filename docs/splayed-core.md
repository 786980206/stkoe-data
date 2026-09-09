# splayed-core

引擎无关存储核心：Field（单列文件）/ META（元数据 + Index）/ Dataset（目录级封装）三层 API。

- 引擎无关：不依赖 Arrow / DataFusion / DuckDB / Polars。
- 交换结构：`ColumnView`（单列）/ `DataView`（多列），适配层负责与各引擎的零拷贝对接。
- 逻辑行空间：容量网格（三层 API 的 offset / length 含义一致，无需换算）。

## 详细文档

| 文档 | 内容 |
| --- | --- |
| [公共语义](core/index.md) | 命名规则、四层架构、RowRange / ScanRequest / Predicate / Handle 对象、逻辑行空间 |
| [Field API](core/field.md) | 骨架与初始化（create_field/init_field）、Handle 操作（read/write/update/scan/close/drop）、物理转换（cast/compress/decompress）+ 内部实现流程 |
| [Index (META) API](core/meta.md) | 索引骨架与初始化（create_index/init_index）、IndexHandle（read/scan/locate/update/close/drop）+ 内部实现流程 |
| [Dataset API](core/dataset.md) | 数据集骨架与初始化（create_dataset/init_dataset）、多级嵌套目录字段（Dot 语法）、缺列补 NULL 容错、写自愈、全量更新 + 内部实现流程 |

## 核心语义摘要

**逻辑行空间（容量网格）**：

```
L = Σ_sym time_count(sym)
row(sym_i, time_index) = row_start(i) + (time_index - time_start(i))
```

三层 API 的 offset / length 一一对应，无需换算；sym 区间内缺失时间 = NULL 逻辑行。

**四层对称面向对象架构**：

```
Table    TableReader (open / scan / read / read_range / close)
         TableWriter (create / init / open / write / update / DDL / remove)
Dataset  DatasetReader (open / scan / read / locate / close)
         DatasetWriter (create / init / open / write / update / DDL / remove)
Index    IndexReader (open / scan / read / locate / close)
         IndexWriter (create / init / open / update / remove)
Field    FieldReader (open / scan / read / close)
         FieldWriter (create / init / open / write / update / cast / compress / decompress / rename / remove)
```

**Scanner 契约**：

```
next()   -> Result<T?, Error>    // None = 正常结束；Err = 执行错误
close()  -> Result<()>           // 任何时刻可安全调用
```

**设计原则**：core API 保持薄，只有上层真正需要的能力才下沉到 core。

**实现原则**（各 API 的内部流程见对应文档）：

- 读零拷贝：`FieldHandle::read` 只做逻辑行范围 → `ColumnView` 转换（PLAIN mmap 切片 / compressed working 切片），
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
  → Field 级并行只做纯 I/O（`std::thread::scope` round-robin 分桶：write 总字节 ≥ 1 MiB、
  scan 候选 ≥ 64K 行才并行，小负载走串行快路径）→ 主线程收尾（求交 / 合并 / 组装）；
  `read` 为纯零拷贝切片恒单线程；并行度 `max_parallelism` 由最上层控制。

## 设计边界（V2.0 暂不引入）

- 独立 Schema 文件 / API。
- `commit / flush / dump` 等写入控制 API。
- Field Handle 的公开压缩 / 解压 API（用 File API）。
- META 原地 update API。
- stride / 非 contiguous Column。
- core 内部的全局 predicate reorder / query planner。
- 行级 DELETE、INSERT 追加、UPSERT、MERGE（见 splayed-table §6）。

设计原则：**core API 保持薄，只有上层真正需要的能力才下沉到 core。**

---

## 性能审查记录

已优化：

- `MetaBuilder`：run-length 遍历——字符串分配/比较发生在 sym run 边界（O(sym 段数) 而非 O(行数)）；轴定位为排序去重后二分（2R·logT，无哈希表）；连续子区间校验保证 time_count 与数据行对齐；TIME AXIS 全局构建（全局去重有序，非 first-appearance）。
- `sym_id_of`：字典二分查找 O(log S)（字典按首现序 = 排序序）。
- Dataset 扫描的 ranges 求交：双指针归并 O(a + b)。
- `FieldScanner`：顺序批量管线——ranges 直接消费（不 merge）；整段连续 values 类型化比较循环（算子分派在循环外，LLVM 自动向量化）；0/1 字节掩码根部统一 validity 求交；branchless 打包位图 + word 级 `next_true_run` 输出连续命中区。
- `close_field_handle`（compressed 收尾）：逐 chunk 编码直写 tmp（内存 O(working + 一个 chunk)，不拼接整个重压缩文件）；tmp `sync_all` 后再原子替换，rename 生效时新文件内容已持久。
- `cast_field_file`：批次流式转换（256K 行/批，uncompressed 源全程 O(一个批次) 内存，validity 位流 O(total/8)）；tmp `sync_all` 后原子替换 + 唯一临时文件名；保持原压缩状态与 generation。
- `compress_field_file` / `decompress_field_file`：chunk 流式——compress 逐 chunk 零拷贝切片编码直写 tmp（header 编码前即确定）；decompress 直接解析 chunk 位置逐 chunk 解码（不经 open 全量解压），DATA / VALIDITY 双游标顺序写，validity 位跨 chunk 拼接；内存 O(单个 chunk)；tmp `sync_all` 后原子替换。
- Dataset 的 `read` / `write` / `scan` 统一三阶段并发模型（主线程校验 + `ensure_field` 串行 → Field 级并行只做纯 I/O → 主线程收尾；并行度 `max_parallelism` 由最上层控制，`std::thread::scope` 分桶，不自建线程池）：write 按 `values_mut` 收集互不相交 `&mut FieldHandle`（列名唯一校验）分桶并行写，总字节 < 1 MiB 走串行快路径；scan 各 Field 以相同候选范围独立并行扫描后主线程顺序求交 + 相邻合并（与逐字段串行收窄等价：谓词逐行性质 + `∩` 交换/结合），候选 < 64K 行走串行，`limit` 不下推子扫描（截断后求交会漏行）；read **保持单线程**——读路径是 O(1) 零拷贝切片、不触碰数据页，并行调度开销为负收益，并行插入点在 chunk 惰性解码落地后。
- Table 层 Field 结构操作统一执行器 `structural_for_each`（主线程严格前置校验 → `values_mut` 互不相交 `&mut DatasetHandle` 分桶并行 → 返回首个错误；并行只跨 Partition、不嵌套 Field 级并发）；compress / decompress 增加物理状态前置校验（`dataset_field_is_chunked` 只读 64B header，不经打开——compressed 打开会全量解压）；cast 增加跨分区类型一致性校验；create 的全 NULL 字段经 `FieldInit::Length` 的 Header-Only（仅 64 字节 Header，不落磁盘数据页，按需首次写扩展），成本 O(header)/分区。
- Table Reader（read_table）双路径：`None` = 原始路径（一 range 一批，零聚合）；
  `Some(n)` = 聚合路径（range 级截断 + pending 剩余，恰好 n 行不超发——修正旧实现
  「拉满为止」的批次超发；多段 ColumnView 拼接保持零拷贝）；`Some(0)` 显式拒绝。
  pending 存 range 而非 DataView（不可变 → 延迟读等价，避免跨 next() 持有数据借用）。
- Table 层收尾微优化：structural_for_each 按当前分区集过滤缓存句柄（排除外部删除后
  的过期句柄——正确性边界）；Scanner/Reader 的 projection 贯通 `Vec<Arc<str>>`（消除
  Reader 每次 next() 的 Vec<String> 克隆与 Scanner 每分区的 Arc 重建）；statistics
  归并按缓存引用（无逐分区 DatasetStatistics clone，仅最终 sym 界各 clone 一次）；
  write_table 分区存在性检查改 HashSet（O(P×B) → O(P+B)）。
  已记录未做：write_table 的 pairs 逐行 String 分配需 core 提供按 sym-run 的
  locate 变体方可消除（core API 保持薄，暂不引入）。
- write_table 三阶段重写：主线程一次扫描（相邻 key 零分配校验 + 粗键分区 run 划分）
  → 全部定位与校验先于任何写入（key 缺失不产生部分写入——强于旧实现的逐分区
  先写后验）→ 分区级并行写（P_part × P_field ≤ max_parallelism 预算切分，Field 级
  预算临时下调 join 后恢复；per-range 字段 Schema 复用 + slice_rows 零拷贝切片）。
- **创建即压缩**（create_field_file + CreateFieldOptions{compression, chunk_offsets}）：
  Data → 单遍 chunked 直接创建（逐 chunk encode_chunk 顺序直写，内存 O(单 chunk)，
  无 tmp / 无二次读；与 compress_field_file 输出字节级一致——测试锁定）；Length →
  Header-Only 全 NULL 字段（仅 64 字节 Header，后续 write 生命周期保持压缩）；Stream → 组合路径（两阶段 reader
  协议使单遍编码需物化全列：流式写未压缩 + 原地压缩）。分层透传：CreateDatasetOptions
  {compression, chunk_syms}（sym 对齐边界由输入 sym run 推导，全 Field 复用）、
  TableOptions{compression, chunk_syms}、create_table_partition 按分区覆盖（冷热分层）、
  DatasetHandle::sym_aligned_chunk_offsets 公开。
- Table 元数据读 API 统一缓存模型（另见 splayed-table §3.1）：scan_table 惰性化——
  scan_table 只做裁剪与构造（不打开 Dataset，tt 经 64B META header 直读），分区在
  next() 时按序惰性打开；时间裁剪按 time_min 排序后二分（不依赖分区名字典序——年号
  位数不同时字典序 ≠ 时间序）；完整谓词下传（裁剪是粗筛，边界分区需 Dataset 内时间
  精确过滤）；limit 逐分区下推 + 返回前防御性裁剪；无时间条件的扫描不读任何 META。
  串行扫描（保持顺序 / limit 早停 / 无嵌套并行）。
- 统一缓存模型：逐分区 `DatasetStatistics` 按名 memo（不可变——META
  immutable + positional overwrite 不改 row_count / TIME AXIS / sym 字典，命中后聚合纯内存）；
  分区 time 界由分区名 `partition_range` 纯推导（零 I/O）；**分区列表不缓存**（read_dir 微秒级，
  保证路径式 create / delete_table_partition 与外部变更的正确性——修正伪代码中"open 时缓存
  分区列表"的方案：会静默漏掉新建分区）；**修复 TableHandle::statistics 的 time_min 聚合
  bug**（从 default 0 起归并，`0.min(实际值)` 恒为 0——改 Option 起始归并 + checked_add）。
- **修复 convert_values 的 f64→f32 字节截断 bug**：原实现对所有浮点目标用 f64 的 8 字节 LE 表示截断到目标宽度——f64 小端低 4 字节不是 f32 位型，小整数值（低 32 位全零）被清成 0.0、大值成乱码；现按目标宽度直接生成对应位型（`as f32` 后 `to_le_bytes`）。既有 cast 测试只覆盖整型互转与 f32→f64 方向，未覆盖 f64→f32——补 f64→f32→f64 往返回归测试。

已知优化项（当前实现为正确性优先的简化，行为符合本文档语义）：

- compressed Field 打开即**全量解压**为工作表示；chunk 级惰性解码（只解码覆盖请求范围的 chunk）待实现——它同时是 `read` 引入 Field 级并行的前提（见 §9 read 单线程结论）。
- ~~谓词求值逐行 `read_row_scalar` 标量分发~~ → 已解决：类型化批量比较循环 + 字节掩码 + word 级命中区提取（显式 SIMD intrinsics 仍为可选后续项）。
- ~~Handle 的 scratch 缓冲逐次累积~~ → 已消除：读路径不再存在 scratch——uncompressed 直接切 mmap、compressed 切 working、META sym 列为 RepeatDict 零物化段，全部视图直接指向底层缓冲。
- ~~IndexHandle 的 `read` (原 `read_index_handle`) 的 sym keys 逐行物化~~ → 已解决：`RepeatDict` 段（零存储）替代 keys 物化，scratch arena 已移除。
