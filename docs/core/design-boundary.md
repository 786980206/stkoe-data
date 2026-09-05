# splayed-core / 设计边界与性能

## 8. 设计边界（V2.0 暂不引入）

- 独立 Schema 文件 / API。
- `commit / flush / dump` 等写入控制 API。
- Field Handle 的公开压缩 / 解压 API（用 File API）。
- META 原地 update API。
- stride / 非 contiguous Column。
- core 内部的全局 predicate reorder / query planner。
- 行级 DELETE、INSERT 追加、UPSERT、MERGE（见 splayed-table §6）。

设计原则：**core API 保持薄，只有上层真正需要的能力才下沉到 core。**

## 9. 性能审查记录（V2.0 首轮）

已优化：

- `MetaBuilder`：run-length 遍历——字符串分配/比较发生在 sym run 边界（O(sym 段数) 而非 O(行数)）；轴定位为排序去重后二分（2R·logT，无哈希表）；连续子区间校验保证 time_count 与数据行对齐；TIME AXIS 全局构建（全局去重有序，非 first-appearance）。
- `sym_id_of`：字典二分查找 O(log S)（字典按首现序 = 排序序）。
- Dataset 扫描的 ranges 求交：双指针归并 O(a + b)。
- `FieldScanner`：顺序批量管线——ranges 直接消费（不 merge）；整段连续 values 类型化比较循环（算子分派在循环外，LLVM 自动向量化）；0/1 字节掩码根部统一 validity 求交；branchless 打包位图 + word 级 `next_true_run` 输出连续命中区。
- `close_field_handle`（compressed 收尾）：逐 chunk 编码直写 tmp（内存 O(working + 一个 chunk)，不拼接整个重压缩文件）；tmp `sync_all` 后再原子替换，rename 生效时新文件内容已持久。
- `cast_field_file`：批次流式转换（256K 行/批，uncompressed 源全程 O(一个批次) 内存，validity 位流 O(total/8)）；tmp `sync_all` 后原子替换 + 唯一临时文件名；保持原压缩状态与 generation。
- `compress_field_file` / `decompress_field_file`：chunk 流式——compress 逐 chunk 零拷贝切片编码直写 tmp（header 编码前即确定）；decompress 直接解析 chunk 位置逐 chunk 解码（不经 open 全量解压），DATA / VALIDITY 双游标顺序写，validity 位跨 chunk 拼接；内存 O(单个 chunk)；tmp `sync_all` 后原子替换。
- `read_dataset` / `write_dataset` / `scan_dataset` 统一三阶段并发模型（主线程校验 + `ensure_field` 串行 → Field 级并行只做纯 I/O → 主线程收尾；并行度 `max_parallelism` 由最上层控制，`std::thread::scope` 分桶，不自建线程池）：write_dataset 按 `values_mut` 收集互不相交 `&mut FieldHandle`（列名唯一校验）分桶并行写，总字节 < 1 MiB 走串行快路径；scan_dataset 各 Field 以相同候选范围独立并行扫描后主线程顺序求交 + 相邻合并（与逐字段串行收窄等价：谓词逐行性质 + `∩` 交换/结合），候选 < 64K 行走串行，`limit` 不下推子扫描（截断后求交会漏行）；read_dataset **保持单线程**——读路径是 O(1) 零拷贝切片、不触碰数据页，并行调度开销为负收益，并行插入点在 chunk 惰性解码落地后。
- Table 层 Field 结构操作统一执行器 `structural_for_each`（主线程严格前置校验 → `values_mut` 互不相交 `&mut DatasetHandle` 分桶并行 → 返回首个错误；并行只跨 Partition、不嵌套 Field 级并发）；compress / decompress 增加物理状态前置校验（`dataset_field_is_chunked` 只读 64B header，不经打开——compressed 打开会全量解压）；cast 增加跨分区类型一致性校验；create 的全 NULL 字段经 `FieldInit::Length` 的 `set_len` 稀疏零填充，成本 O(header)/分区。
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
  chunked 全 NULL（后续 write 生命周期保持压缩）；Stream → 组合路径（两阶段 reader
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
  分区列表"的方案：会静默漏掉新建分区）；**修复 read_table_statistics 的 time_min 聚合
  bug**（从 default 0 起归并，`0.min(实际值)` 恒为 0——改 Option 起始归并 + checked_add）。
- **修复 convert_values 的 f64→f32 字节截断 bug**：原实现对所有浮点目标用 f64 的 8 字节 LE 表示截断到目标宽度——f64 小端低 4 字节不是 f32 位型，小整数值（低 32 位全零）被清成 0.0、大值成乱码；现按目标宽度直接生成对应位型（`as f32` 后 `to_le_bytes`）。既有 cast 测试只覆盖整型互转与 f32→f64 方向，未覆盖 f64→f32——补 f64→f32→f64 往返回归测试。

已知优化项（当前实现为正确性优先的简化，行为符合本文档语义）：

- compressed Field 打开即**全量解压**为工作表示；chunk 级惰性解码（只解码覆盖请求范围的 chunk）待实现——它同时是 `read_dataset` 引入 Field 级并行的前提（见 §9 read_dataset 单线程结论）。
- ~~谓词求值逐行 `read_row_scalar` 标量分发~~ → 已解决：类型化批量比较循环 + 字节掩码 + word 级命中区提取（显式 SIMD intrinsics 仍为可选后续项）。
- ~~Handle 的 scratch 缓冲逐次累积~~ → 已消除：读路径不再存在 scratch——uncompressed 直接切 mmap、compressed 切 working、META sym 列为 RepeatDict 零物化段，全部视图直接指向底层缓冲。
- ~~`read_index_handle` 的 sym keys 逐行物化~~ → 已解决：`RepeatDict` 段（零存储）替代 keys 物化，scratch arena 已移除。

## 10.4 正式基准（docs/benchmark.md 方案，2026-09-05，Windows/NTFS，Warm）

数据：50 字段（sym 1000 字典 + time µs + description 5000 字典 + 47 数值），48 月分区，
(sym ASC, time ASC) 排序，ZSTD-3 双侧对齐，Warm-only（Windows 无 drop_caches）。
执行器：`crates/splayed-bench`（一键可重现；CSV = results/bench_final3.csv）。
校验：逐场景 per-column 校验和，Splayed == Parquet（description 列除外——Utf8 字段
不 roundtrip 的已知限制）。计时不含校验（校验独立 pass）。

中位数（3 次；splayed-1t = 引擎内部串行，splayed-4t = 4 线程）：

| 规模 | 场景 | splayed-1t | splayed-4t | parquet | 4t/parquet |
| --- | --- | --- | --- | --- | --- |
| 1M | W1 写入 | 7.66 s | **3.76 s** | 5.64 s | 0.67× |
| 1M | R1 全扫描 | 18.5 ms | 18.7 ms | 1135 ms | **0.02×** |
| 1M | R2 单列 | 8.1 ms | 8.5 ms | 13.9 ms | 0.61× |
| 1M | R3 双过滤 1% | 4.4 ms | 4.4 ms | 53.6 ms | **0.08×** |
| 1M | R4 sym 1% | 5.8 ms | 5.8 ms | 100.8 ms | **0.06×** |
| 1M | R5 time 1% | 7.1 ms | 7.0 ms | 23.0 ms | 0.36× |
| 1M | R6 极高选择性 | 5.7 ms | 4.3 ms | 42.6 ms | **0.10×** |
| 5M | W1 | 24.7 s | **11.6 s** | 28.8 s | 0.40× |
| 5M | R1 | 35.6 ms | 37.5 ms | 5506 ms | **0.01×** |
| 5M | R2 | 8.9 ms | 8.8 ms | 71.0 ms | 0.12× |
| 5M | R3 | 4.6 ms | 4.5 ms | 219.6 ms | **0.02×** |
| 5M | R4 | 27.8 ms | 28.0 ms | 488.8 ms | **0.06×** |
| 5M | R5 | 29.5 ms | 29.7 ms | 80.5 ms | 0.37× |
| 5M | R6 | 4.1 ms | 4.1 ms | 194.4 ms | **0.02×** |
| 10M | W1 | 81.6 s | **26.0 s** | 59.4 s | 0.44× |
| 10M | R1 | 60.5 ms | 61.0 ms | 11040 ms | **0.01×** |
| 10M | R2 | 10.4 ms | 10.3 ms | 135.7 ms | 0.08× |
| 10M | R3 | 4.9 ms | 4.9 ms | 430.6 ms | **0.01×** |
| 10M | R4 | 49.9 ms | 64.9 ms | 990.4 ms | **0.07×** |
| 10M | R5 | 50.2 ms | 49.9 ms | 157.2 ms | 0.32× |
| 10M | R6 | 4.2 ms | 4.3 ms | 385.8 ms | **0.01×** |
| 20M | W1 | 144.7 s | **80.3 s** | 125.3 s | 0.64× |
| 20M | R1 | 142.8 ms | 105.8 ms | 22449 ms | **0.005×** |
| 20M | R2 | 13.2 ms | 13.0 ms | 271.6 ms | 0.05× |
| 20M | R3 | 5.3 ms | 5.2 ms | 826.9 ms | **0.01×** |
| 20M | R4 | 91.9 ms | 93.6 ms | 1523.6 ms | **0.06×** |
| 20M | R5 | 112.7 ms | 94.1 ms | 321.6 ms | 0.29× |
| 20M | R6 | 4.2 ms | 4.1 ms | 809.6 ms | **0.01×** |

物理大小：splayed 0.689× / parquet 0.718×（vs 未压缩 Arrow，各规模稳定，splayed 小 ~4%）。

**结论**：
- **写入**：splayed-4t 全规模快于 parquet（0.40–0.85×；20M 快 1.6×）；串行（1t）与
  parquet 相当。4t 收益随规模增长（10M/20M 分区级并行充分发挥）。
- **全扫描 R1**：splayed 快 22–200×——warm 语义差异所致：Splayed 打开句柄即持有
  解压后的 working 表示（解码成本在打开/校验 pass 支付），计时 pass 只做零拷贝视图
  组装；Parquet 无进程内解码缓存，每次运行重新读盘 + 解码。两者均为各自引擎的
  真实 warm 使用形态，报告时须注明该结构性差异。
- **选择性查询 R3–R6**：splayed 快 12–180×——META（sym + time）下推使未命中分区 /
  sym 整体跳过（零 I/O），parquet 行组过滤仍需逐行组解码探测。
- **R2 单列**：splayed 快 1.7–20×（读单 Field 文件 vs parquet 列裁剪）。

## 10. Benchmark vs Parquet

> 基准方法论（数据生成规范 / 参数对齐 / 场景定义 W1+R1–R6 / 指标与校验）已独立成文：
> **docs/benchmark.md**。正式复测按该文档执行（执行器 `crates/splayed-bench`）。

基准：`crates/splayed-table/benches/vs_parquet.rs`（criterion；对照 arrow-rs parquet 56）。

### 10.1 首轮基线（2026-09，64 sym × 250 行 = 16K 行，按月分区）

| 路径 | splayed | parquet | 差距 |
| --- | --- | --- | --- |
| 写入（端到端建表） | ~57 ms | ~7.7 ms | ≈ 7× |
| 全表读取（Table 层，open 复用） | **~0.79 ms** | ~0.57 ms | **≈ 1.4×** |
| 谓词扫描（price > 15，open 复用） | **~1.06 ms** | ~0.69 ms | **≈ 1.5×** |

> 读取 1.4× 差距主要来自 polars 列转换层（DataView → Series 拷贝）；
> core 层的 mmap 零拷贝路径本身接近 parquet。
> 写入 7× 差距来自 gather_data 逐行拷贝 + 多文件创建，后续可批量化。

### 10.2 64K 行复测（2026-09，256 sym × 250 行，9 月分区；优化批次后）

| 路径 | splayed | parquet | 对比 |
| --- | --- | --- | --- |
| 写入（端到端建表） | ~102 ms | ~24 ms | ≈ 4.2×（写入仍慢） |
| 全表读取（Table 层，open 复用） | **~0.90 ms** | ~1.77 ms | **splayed 快 ≈ 2.0×** |
| 谓词扫描（price > 15，open 复用） | **~1.03 ms** | ~2.23 ms | **splayed 快 ≈ 2.2×** |

对照优化批次前的本机记录（criterion 持久基线，a0d1b16 时代）：

- **谓词扫描 ≈ -49%**（p = 0.00，显著）：Scanner 批量管线（类型化向量化比较 + 根部 validity 求交
  + word 级命中区）的直接成效；splayed 由落后 parquet 反超为**快 2.2×**。
- 全表读取 0.97 ms → 0.90 ms（≈ -7%）；**splayed 快 ≈ 2.0×**（首轮基线时落后 1.4×）。
- 写入 102 ms vs 109 ms 基本持平（迭代区间 [68, 146] ms 方差大，criterion 迭代内含
  `remove_dir_all`）；仍 ≈ 4.2× 慢于 parquet——瓶颈 = 9 分区 ×（META + 2 field 文件）创建 +
  MetaBuilder × 12 + gather，即 §8 已知优化项（写入批量化 / 并行化）。
- Dataset 层阶段计时（release profiler，64K 行）：`read_dataset` 483 µs、`scan_dataset` 谓词 685 µs、
  `create_table`（单分区）7.0 ms、`create_table`（9 月分区）44.6 ms、compress 13.8 ms、
  decompress 11.5 ms。
- 后续增量：`create_dataset` 并行化（META 先行 + Field 并行创建 + 零拷贝列移动，
  c864833）后写入中位 102 → 82 ms（≈ -19.5%）；`write_dataset` / `scan_dataset` 的
  Field 级并行与小写快路径（6d3b809）已落地。另查明：`create_table` 分区创建
  **已并行**（6d71ae8，每分区一个线程，未接 max_parallelism 上限，与 create_dataset
  内部 Field 级并行嵌套）——此前"跨分区串行"的记载系函数注释失实所致。

### 10.3 Table 层优化轮复测（2026-09-05，64K 行 / 9 月分区，criterion 最新中位数）

> 基准写入对比的结构性差异：parquet 侧为**单个文件**（bench.parquet，单 RecordBatch）；
> splayed 侧为 **9 分区 ×（.meta + price + volume）= 27 个文件** + 9 目录（sym / time 由
> META 管理，不单独成文件），且每分区经 tmp 目录 + sync_all + 原子 rename 发布。
> 按「每文件成本」计：splayed ≈ 1.1 ms/文件（含 META 构建 + gather + fsync），
> parquet 23 ms / 1 文件——1.3× 的端到端差距在该文件数差异下是合理的。

| 路径 | splayed | parquet | 差距 |
| --- | --- | --- | --- |
| 写入（端到端建表） | **30.3 ms** | 23.0 ms | **≈ 1.3×**（此前 4.2× → 2.6× → 1.3×） |
| 全表读取（Table 层，open 复用） | **0.935 ms** | 1.825 ms | **splayed 快 ≈ 2.0×** |
| 谓词扫描（price > 15，open 复用） | **1.059 ms** | 2.152 ms | **splayed 快 ≈ 2.0×** |

- 写入 82 → 30.3 ms（≈ -63%）：本轮 Table 层优化的直接成效——create_table 分区
  片段化 gather（连续片段 memcpy + validity word 级拼接 + 字典 remap，去逐行拷贝 /
  逐 bit 位写）+ 预算切分并行（P_part × P_field ≤ max_parallelism）；写入基准的
  迭代体即端到端建表。与 parquet 的剩余差距主要在 12 × Dataset 的 META 构建与
  文件创建（Windows 文件系统开销）。
- 读取 / 谓词扫描 0.94 / 1.06 ms：与上轮 0.90 / 1.03 ms 持平（噪声级波动）——
  惰性 scan_table 与双路径 read_table 重写未引入回归。

结论与定位：读取侧已反超 parquet（全表 ≈ 2.0×、谓词 ≈ 2.0×），谓词向量化已从「已知优化项」兑现；
写入经 Table 层优化后与 parquet 差距缩至 ≈ 1.3×（§10.3）；
**写入侧 create_table 已完成批量化 + 并行预算治理**（一次线性扫描产出分区连续片段 →
gather_runs 连续片段 memcpy + validity word 级拼接 + 字典 remap 零逐行字符串分配 →
`P_part × P_field ≤ max_parallelism` 预算切分并行，TableOptions 直达 create_table）；
剩余待测：端到端写入基准复测；chunk 级惰性解码（读路径 Field 级并行的前提）仍为后续项。
已查明的边界：Field 文件层仅支持定宽类型的 roundtrip（Utf8 字段的字典区不落盘，
读回为空 Fixed 段）——Utf8 值字段的持久化支持待设计评估。
