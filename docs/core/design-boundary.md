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

已知优化项（当前实现为正确性优先的简化，行为符合本文档语义）：

- compressed Field 打开即**全量解压**为工作表示；chunk 级惰性解码（只解码覆盖请求范围的 chunk）待实现。
- ~~谓词求值逐行 `read_row_scalar` 标量分发~~ → 已解决：类型化批量比较循环 + 字节掩码 + word 级命中区提取（显式 SIMD intrinsics 仍为可选后续项）。
- Handle 的 scratch 缓冲逐次累积（视图生命周期契约要求），长生命周期高频读场景的回收策略待定。
- ~~`read_index_handle` 的 sym keys 逐行物化~~ → 已解决：`RepeatDict` 段（零存储）替代 keys 物化，scratch arena 已移除。

## 10. Benchmark vs Parquet

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

### 10.2 64K 行复测（2026-09，256 sym × 250 行，12 月分区；优化批次后）

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
  `remove_dir_all`）；仍 ≈ 4.2× 慢于 parquet——瓶颈 = 12 分区 × 4 field 文件创建 +
  MetaBuilder × 12 + gather，即 §8 已知优化项（写入批量化 / 并行化）。
- Dataset 层阶段计时（release profiler，64K 行）：`read_dataset` 483 µs、`scan_dataset` 谓词 685 µs、
  `create_table`（单分区）7.0 ms、`create_table`（12 月分区）44.6 ms、compress 13.8 ms、
  decompress 11.5 ms。

结论与定位：读取侧已反超 parquet（全表 2.0×、谓词 2.2×），谓词向量化已从「已知优化项」兑现；
**写入是当前唯一显著落后项（≈ 4.2×）**，开销集中在每分区文件创建 / MetaBuilder / gather——
下一优先级是写入路径批量化与并行化；chunk 级惰性解码与 scratch 回收仍为后续项。
