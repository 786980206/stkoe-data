# SUBSET 格式（`.sub.xxx`）

`.sub.xxx` 与 `.meta` 同目录，是**父 `.meta` 的 `SYM × TIME` 网格子集**的索引文件：
只记录选中 `(SYM, TIME)` 对应的**父全局行空间区间**，指向 FIELD 文件的 data 区。
**文件本身不存值**（与 `.meta` 一样"只记录索引"）。

典型场景：`.meta` = 全市场 A 股；`.sub.hs300` = 沪深300 成分的时序子集。

## 与 META 的相同 / 不同

| | META（§5.1） | SUBSET（`.sub.xxx`） |
| --- | --- | --- |
| Header | 64B，magic `SPLAYMTA` | 64B，magic `SPLAYSUB` |
| SYM 字典 | `(sym_count+1)×8` 偏移表 + 字符串 | **同款** |
| 区间记录 | 12B `SymIndexRecord` | **同款**（`time_start`/`time_count` 索引父 TIME AXIS，`row_start` 为父全局行） |
| 每 SYM 区间数 | **恰好 1 条**（连续） | **可多条不连续**（进出指数多次） |
| TIME AXIS | 自带（全局去重升序） | **无**（引用父的） |
| 生命周期 | 建/重排（`update_meta`） | 派生快照；父重排后失效（`StaleParent`） |

## 二进制布局

```text
[Header 64B]
[SYM DICT INDEX: (sym_count + 1) × 8 bytes]   ← u64 offsets into STRING DATA
[SYM STRING DATA: variable]
[RANGE INDEX: sym_count × (u32 count + count × 12B SymIndexRecord)]
```

### Header（64B）

| 偏移 | 大小 | 字段 | 说明 |
| --- | --- | --- | --- |
| 0 | 8 | `magic` | `SPLAYSUB`（LE u64） |
| 8 | 2 | `version` | 1 |
| 10 | 2 | `flags` | 0 |
| 12 | 1 | `time_type` | 必须与父 `.meta` 一致 |
| 13 | 3 | `reserved` | 0 |
| 16 | 8 | `generation` | 子集自身代数 |
| 24 | 8 | `parent_generation` | **父 `.meta` 的 generation**（失效检测） |
| 32 | 4 | `sym_count` | 子集符号数 |
| 36 | 4 | `range_count` | 全符号区间总数 |
| 40 | 4 | `total_rows` | 子集覆盖行数 = Σ 区间 `time_count` |
| 44 | 20 | `reserved2` | 0 |

### RANGE INDEX

每符号（字典序）：`[count: u32]` + `count × SymIndexRecord{time_start, time_count, row_start}`。
区间按 `time_start` 升序；相邻/重叠段在创建时已合并。

## 语义要点

- **区间指向父字段**：`row_start` 直接落在父 `.meta` 的全局行空间（`row = sym_row_start +
  (time_idx - sym.time_start)`），所以读值 = 对 FIELD 按区间 `read_range_raw`。
- **多区间**：一个 SYM 在子集内可有多个不相邻区间（如股票在指数里的多个任期），
  这也是与 META"一个 SYM 一条连续区间"的唯一结构性差异。
- **失效检测**：父被 `update_meta` 重排后 generation 递增 → `SubsetReader::open_with_parent`
  报 `StaleParent`，须重建 `.sub.xxx`。
- **不是字段**：文件以 `.` 开头，`Dataset::list_fields` / `update_meta::list_field_names`
  一律忽略，不会被当成 FIELD。

## 读写接口

见 `plan.md` §5.7 / §8.4：

- 写：`splayed_core::create_subset(dir, name, &[SubsetInput])`（`SubsetInput{sym, segments}`，
  校验符号/TIME/段边界，相邻段自动合并，原子落盘）；
- 读：`splayed_core::SubsetReader`——`open` / `open_with_parent`（父绑定校验）、
  `symbols / contains / ranges / total_rows`、`iter_ranges`（父全局行序）、
  `iter_entries(&MetaFile)`（`(sym, time, row)`）、`read_field_values(&FieldReader)`
  （字段在子集内的值，父行序拼接，长度 = `total_rows × sizeof(type)`）。

## 下游消费

- **Arrow**：`splayed_arrow::read_subset(dir, sub_name, &columns) -> RecordBatch`
  直接物化 `[time, sym, ...columns]`（父全局行序；`columns` 空 = 全部字段），
  时间/符号来自父 `.meta`，字段经 `column_view_to_arrow` 转 Arrow（NULL 哨兵 →
  validity）。Python / polars 可用 `Vec<RecordBatch>` 直接建 DataFrame。
