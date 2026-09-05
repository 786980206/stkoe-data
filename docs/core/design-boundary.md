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

- `MetaBuilder`：run-length 遍历——字符串分配/比较发生在 sym run 边界（O(sym 段数) 而非 O(行数)）；轴定位单调双指针；TIME AXIS 两遍全局构建（全局去重有序，非 first-appearance）。
- `sym_id_of`：字典二分查找 O(log S)（字典按首现序 = 排序序）。
- Dataset 扫描的 ranges 求交：双指针归并 O(a + b)。

已知优化项（当前实现为正确性优先的简化，行为符合本文档语义）：

- compressed Field 打开即**全量解压**为工作表示；chunk 级惰性解码（只解码覆盖请求范围的 chunk）待实现。
- 谓词求值逐行走 `read_row_scalar` 标量分发；类型特化的段内向量化（SIMD）待实现。
- Handle 的 scratch 缓冲逐次累积（视图生命周期契约要求），长生命周期高频读场景的回收策略待定。
- ~~`read_index_handle` 的 sym keys 逐行物化~~ → 已解决：`RepeatDict` 段（零存储）替代 keys 物化，scratch arena 已移除。

## 10. Benchmark vs Parquet（V2.0 首轮基线，2026-09）

数据：64 sym × 250 行 = 16K 行 × 4 列（sym/time/price/volume），按月分区。
基准：`crates/splayed-table/benches/vs_parquet.rs`（criterion；对照 arrow-rs parquet 56）。

| 路径 | splayed | parquet | 差距 |
| --- | --- | --- | --- |
| 写入（端到端建表） | ~57 ms | ~7.7 ms | ≈ 7× |
| 全表读取（Table 层，open 复用） | **~0.79 ms** | ~0.57 ms | **≈ 1.4×** |
| 谓词扫描（price > 15，open 复用） | **~1.06 ms** | ~0.69 ms | **≈ 1.5×** |

> 读取 1.4× 差距主要来自 polars 列转换层（DataView → Series 拷贝）；
> core 层  的 mmap 零拷贝路径本身接近 parquet。
> 写入 7× 差距来自 gather_data 逐行拷贝 + 多文件创建，后续可批量化。

结论与定位：当前 V2.0 为**正确性优先**实现——写入开销主要在 MetaBuilder / 每分区
DataView 物化（gather），读取开销在三层 API 的逐分区打开 + 视图组装 + 谓词行级
求值。上述「已知优化项」（chunk 惰性解码、谓词向量化、scratch 回收、建表 gather
优化）是缩小差距的主要抓手；splayed 的目标优势场景（容量网格 O(1) 行定位、
零拷贝 sym/time 视图）在当前基准的全表读中尚未体现，因 Table 层端到端包含
schema 组装等固定开销。
