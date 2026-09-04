# splayed-arrow（Arrow 交换 + 零拷贝）

**依赖**：core + codec + format（**可选的共享转换工具库**，位于核心层之上，
被需要 Arrow 的适配层复用；也是 Python / polars 消费端的直接入口）。

## 接口

| 接口 | 说明 |
|---|---|
| `create_meta(folder, data, sorted)` | 由 Arrow RecordBatch（TIME+SYM）建 `.meta` |
| `create_table(folder, data, sorted)` | 一步建表（Arrow 输入 → 原生 core） |
| `create_table_with_options(folder, data, sorted, opts: FieldWriteOptions)` | 同上，新字段直接编码/压缩（写后只读） |
| `update_table(folder, data, sorted, create_missing_fields)` | 格子更新（Arrow 输入） |
| `update_table_with_options(folder, data, create_missing_fields, opts)` | 同上；**新创建**字段直接编码/压缩 |
| `update_meta(folder, data, sorted)` | **布局重排**（Arrow 输入 TIME+SYM，新增 (SYM,TIME) + 并发重散布全部 FIELD） |
| `create_partitioned_table(root, time_type, &[PartitionWriteInputArrow], opts)` | 一次建分区表（每分区一个 RecordBatch） |
| `append_partition(root, &PartitionWriteInputArrow, opts)` / `drop_partition(root, name)` | 追加 / 删除分区 |
| `update_partition_table(root, data, create_missing_fields, target_partition, opts)` | 表级格子写入（跨分区路由） |
| `update_partition_meta(root, &[PartitionWriteInputArrow], opts)` | 表级布局重排（新增/重排/删除分区） |
| `scan_dataset(dir, &ScanRequest) -> Vec<RecordBatch>` | 便捷扫描：单 dataset → Arrow 批次（投影/过滤/LIMIT 下推） |
| `scan_partitioned(dir, &PartitionScanRequest) -> Vec<RecordBatch>` | 便捷扫描：分区表 → Arrow 批次（按分区名升序合并） |
| `read_subset(dir, sub_name, &[String]) -> RecordBatch` | 子集 `.sub.{sub_name}` → Arrow（父全局行序；`columns` 空 = 全部字段；`StaleParent` 检测） |
| `corebatch_into_record_batch(batch, indices, fields, sym_dict)` | **零拷贝**：CoreBatch 缓冲移交 Arrow（indices 须升序；字典→Utf8 展开；BOOL 位压缩） |
| `corebatch_to_record_batch(...)` | 同名（借用版） |
| `core_type_to_arrow_type` | CoreType → Arrow 类型映射 |
| `splayed_to_arrow_type / arrow_to_splayed_type / arrow_value_to_raw / column_view_to_arrow` | 类型/值映射（含扩展类型） |
| `PartitionWriteInputArrow{name, data, sorted}` | 分区写输入（RecordBatch 含 TIME+SYM+全表字段列） |
| `FieldWriteOptions{encoding, compression}` | 新字段写选项（重导出 core；`Default = Plain+None` 可写） |

## 定位

- **写接口只是转换器**：真正的引擎实现在 core 的原生接口（`TableColumn` 原始
  字节，无 Arrow）——本 crate 把 RecordBatch 转成原生输入后委托 core，故
  `splayed-arrow` 与 `splayed-core` 的写能力一一对应（输入/输出换成 Arrow）。
- **`scan_dataset` / `scan_partitioned`**：把 `Scanner` / `PartitionedTable` 的
  扫描结果直接转成 Arrow `RecordBatch`（经 `corebatch_into_record_batch` 零拷贝），
  输出列固定 `[time, sym, ...columns]`——Python / polars 消费端的推荐读入口
  （polars 可把 `Vec<RecordBatch>` 直接建 DataFrame）。
- **`read_subset`**：把子集 `.sub.{name}`（见 [SUBSET 格式](../format/subset-format.md)）
   物化成单个 `RecordBatch`。行序 = 子集的父全局行序；时间/符号来自父 `.meta`
   （TIME AXIS / SYM DICT），字段值经 `read_field_values` 按区间拼接后由
   `column_view_to_arrow` 转 Arrow（NULL 哨兵 → validity）。子集本身是**父网格
   的视图**——先建父表、再 `create_subset` 圈定成分股/区间，即可用
   `read_subset` 直接得到「该子集 + 任一父字段」的表格，供 Python / polars 消费。
- **直接写成压缩**：`*_with_options` 传 `FieldWriteOptions{encoding, compression}`
  （如 `Rle + Zstd`）即可让新字段一步落盘为压缩（写后只读），适合稀疏因子列。
- `corebatch_into_record_batch` 利用 CoreBatch 原始缓冲**移交**给 Arrow
  （`Buffer::from_vec` 零拷贝）：FIELD/TIME 不拷贝、0 NULL 列不产 validity、
  字典→Utf8 展开（小拷贝）。
