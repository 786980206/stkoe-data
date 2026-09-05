# splayed-core / META API

## 6. META API

### 6.0 总览

| 接口 | 职责 | 层 |
| --- | --- | --- |
| `create_meta_file` | 由 (sym, time) 两列数据构建 META | File |
| `open_meta_file` | 打开已有 META（mode = read） | File |
| `delete_meta_file` | 删除 META 物理文件 | File |
| `read_meta_handle` | 读取 META 结构化信息 | Handle |
| `read_index_handle` | 按逻辑行读取 (sym, time) 数据 | Handle |
| `scan_index_handle` | SYM / TIME 条件 → FIELD row ranges | Handle |
| `locate_index_handle` | (sym, time) 联合键批量定位 | Handle |
| `close_meta_handle` | 关闭 Handle | Handle |

META 是 immutable / read-only 文件：不提供 `write / update / compress / decompress`。

### 6.1 create_meta_file

```
create_meta_file(path, data: DataView) -> Result<()>
```

**MetaBuilder::build 内部实现**（单遍扫描 + 轴二分定位，无 HashMap）：
```
批量 cast  →  sym keys 整段 cast 为 &[u32]、time 列整段 cast 后归一为 &[u64]（零逐行函数调用）
单遍扫描   →  收集全部 time 值 + sym run 边界（row_start / time_first / time_last / rows）
              → 仅 run 切换时解析一次 string_at（字典解码，O(sym 段数) 字符串分配）
TIME AXIS  →  sort_unstable + dedup
SYM INDEX  →  每 run 二分定位 time_first / time_last 的轴下标（O(2R·log T)，R=run 数、T=轴长）
              → 连续子区间校验：end ≥ start 且 (end − start + 1) == run 行数
                （未按 (sym ASC, time ASC) 排序 / sym 内 time 重复或跳空 → NonContiguousTime）
              → time_count = span = run 行数；row_start 累计 span
serialize  →  header(64B) + TIME AXIS + DICT OFFSETS(n+1)×u64
              + STRING DATA + SYM INDEX(n×12B)
```

**核心原则**：每个 symbol 的 time 必须严格等于 TIME AXIS 的一个连续子区间，因此 SYM INDEX 只需保存
`row_start + time_start + time_count`，不保存任何 symbol 内的 time 信息。连续性校验使 `time_count`
（轴跨度）与 run 实际行数强一致，容量网格 `row_start = Σ 前序 time_count` 与数据行严格对齐；
轴定位从 HashMap 换为二分（`2R·log T` 次比较，无哈希表构建与随机访问），输入规模越大收益越明显。

**write_meta_atomic 内部实现**：
```
File::create(path.tmp) → write_all(bytes) → drop → fs::rename(tmp, path)
```

- `data`：按 `(sym ASC, time ASC)` 排序的 sym / time 两列逻辑数据。
- 违反连续子区间原则的输入（sym 内 time 跳空 / 重复 / 未排序）在构建期以 `NonContiguousTime` 拒绝，
  不会生成网格与数据错位的 META。
- 不要求不同 SYM 具有相同 TIME 集合；每个 SYM 可以有自己的 TIME 序列（只要各自是轴的连续子区间）。
- core 从数据推断 `time_type` 等信息，并构建 TIME AXIS / SYM DICT / SYM STRING DATA / SYM INDEX。
- 区间内缺失的时间点由对应 Field 的 NULL 表示。
- 完成后 layout 固定、进入只读状态。

### 6.2 open_meta_file

```
open_meta_file(path, mode) -> Result<MetaHandle>
```

- META 必须已存在；`mode = read`（无 write mode）。
- 打开时校验 header；后续所有 META / Index 操作经返回的 Handle 执行。

### 6.3 read_meta_handle

```
read_meta_handle(handle) -> MetaInfo
MetaInfo { version, time_type, generation, time_count, sym_count, row_count }
```

只读取结构化信息；不做 Index 定位；调用方无需了解物理布局。

### 6.4 read_index_handle

```
read_index_handle(handle, offset, length) -> Result<DataView>
```

**内部实现**：
```
1. 二分 SYM INDEX（row_start 单调递增）→ 找到起始 sym_id
2. 从 sym_id 起逐 sym 遍历：
     每段重叠区间 [max(offset,row_start), min(end,row_end))
     → sym: ColumnSegment::new_repeat_dict(dict_offsets, dict_strings, sym_id, n)
       （零存储——不物化 keys，O(sym_count) 而非 O(length)）
     → time: TIME AXIS 直接切片（零拷贝多段）
3. 组装 DataView { schema: [sym Utf8, time]，columns: [RepeatDict segments, time segments] }
```
- sym 列零物化：RepeatDict 段只记录 (dict_offsets, dict_strings, dict_index)，消费方按需解析
- time 列零拷贝：直接引用 mmap 的 TIME AXIS 区域
- 无 scratch arena：不再需要 keys 物化缓冲

- `offset / length` 是 Index 逻辑行空间（容量网格，§4）中的位置；`offset + length ≤ L`；`length = 0` 返回空 view。
- 返回该逻辑行段的 `(sym, time)` 两列 view：sym 以字典视图返回（指向 SYM DICT / STRING DATA），time 直接指向 TIME AXIS——两者零拷贝。
- 物理布局（TIME AXIS / DICT / INDEX 交错）由 core 内部组装，上层只见逻辑两列。

### 6.5 scan_index_handle

```
scan_index_handle(handle, request) -> Result<IndexScanner>
IndexScanner::next() -> Result<RowRange?>
```

**内部实现**：
```
1. extract_sym_filter(predicate)   →  SymFilter::All | Ids(Vec<u32>)
     And → 各子过滤交集；Or → 并集；Not → All 回退
     sym = "X" → sym_id_of 二分 → Ids([id])；不存在 → Ids([])
2. extract_time_window(predicate)  →  轴 index 窗口 [lo, hi)
     And → 各子条件窗口收窄；Or/Not → 全轴回退
     time >= t1 → axis_lower_bound(t1)（二分）
     time < t2  → axis_lower_bound(t2)
3. 遍历候选 sym → 与 time 窗口求交 → 与 request.ranges 求交
     → merge_ranges → IndexScanner
```
- 无法静态求值的谓词（嵌套 NOT 等）回退 All → 行级过滤由上层兜底

- `predicate` 作用于 sym / time（如 `sym = "AAPL" AND time >= t1 AND time < t2`）。
- `ranges`：上游候选范围输入（空 = 不限制）；`limit` 达到后提前结束。
- 输出满足条件的 FIELD row ranges（逻辑 = 物理），可直接交给 `read_field_handle`，或作为 `scan_field_handle` 的 `ranges` 输入做多字段 predicate pushdown。

典型执行链：

```
SYM/TIME predicate → scan_index_handle → RowRanges
    → scan_field_handle (predicate A) → 更小 ranges
    → scan_field_handle (predicate B) → final ranges
    → read_field_handle / read_dataset
```

META / Field scan 不负责决定多 Field predicate 的执行顺序；顺序由上层 planner 决定。

### 6.6 locate_index_handle

```
locate_index_handle(handle, data: DataView) -> Result<RowRange[]>
```

**内部实现**（双指针单调推进，O(S + N)）：
```
sym_cursor（SYM INDEX 游标）──→ 单调前进，O(S) 总计
time_cursor（TIME AXIS 游标）──→ sym 切换时重置到 time_start，run 内单调前进
        │
        ↓
row = row_start + (time_cursor - time_start)
        │
        ↓
边走边合并连续行（last.end() == row → length += 1）
```
- 排序校验内联（每行与前行比较，无额外遍历）
- sym 不存在 / time 不在区间内 / time 不在轴上 → Error
- 总量校验 sum(length) == input.len()

- `data` 至少包含 sym / time 两列，按行一一配对组成联合键 `(sym, time)`；输入按 `(sym ASC, time ASC)` 排序。
- META 利用输入有序性 × SYM INDEX / TIME AXIS 有序性做双指针扫描，避免逐行查找。
- 返回与输入分段对应的 `RowRange[]`：每个 RowRange 对应一段连续输入与 Dataset 中一段连续逻辑行。
- 定位成功的充要条件：`sum(RowRange.length) == data.length`（key 全部存在且无重复）。
- 供 `write_table` 等批量「按 key 定位已有行」的场景；不读取 Field 数据。

与 `scan_index_handle` 的区别：

```
scan_index_handle    条件查询 → RowRanges
locate_index_handle  (sym, time) 联合键 → RowRanges
```

### 6.7 close_meta_handle

close 后 Handle 不可再用；释放 fd / mmap 等资源；META 无任何写回。

### 6.8 META 更新策略

META immutable，无 `update_meta()`。内容或 layout 变化（新增 sym、扩大 time 范围、布局重排）时由 `MetaBuilder` 构建新文件：写临时文件 → 原子 rename 替换旧 `.meta`；替换原子完成，不出现 META 短暂不存在的状态。
