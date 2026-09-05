# splayed-core / META API

## 6. META API

### 6.0 总览

| 接口 | 职责 | 层 |
| --- | --- | --- |
| `MetaBuilder::build` | 从 (sym, time) 两列逻辑数据构建 META 文件字节 | File |
| `create_meta_file` | 创建 `.meta`（build + 原子写出） | File |
| `delete_meta_file` | 删除 `.meta` 物理文件 | File |
| `MetaHandle::open` | 打开 `.meta`（只读 mmap），返回 `MetaHandle` | File |
| `read_meta_handle` | 读取结构化元信息 `MetaInfo` | Handle |
| `read_index_handle` | 按逻辑行读取 (sym, time) 两列 view | Handle |
| `scan_index_handle` | 条件扫描 → `IndexScanner`（FIELD row ranges） | Handle |
| `locate_index_handle` | (sym, time) 联合键批量定位 | Handle |
| `close_meta_handle` | 关闭 Handle（`MetaHandle::close`） | Handle |

布局：`HEADER 64 | TIME AXIS | SYM DICT INDEX (n+1)×u64 | SYM STRING DATA | SYM INDEX n×12`。

### 6.1 MetaBuilder::build

**接口定义**：
```rust
pub struct MetaBuilder;
impl MetaBuilder {
    pub fn build(data: &DataView<'_>) -> Result<Vec<u8>, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `data` | `&DataView<'_>` | 输入 | 必须包含 `sym`（Utf8 字典视图）与 `time`（定宽整数列），按 `(sym ASC, time ASC)` 排序 |
| 返回 | `Result<Vec<u8>, CoreError>` | 输出 | 完整 META 文件字节（header + TIME AXIS + DICT + STRING DATA + SYM INDEX） |

**内部实现**（单遍扫描 + 轴二分定位，无 HashMap）：
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

**说明**：
- 违反连续子区间原则的输入（sym 内 time 跳空 / 重复 / 未排序）在构建期以 `NonContiguousTime` 拒绝，
  不会生成网格与数据错位的 META。
- 不要求不同 SYM 具有相同 TIME 集合；每个 SYM 可以有自己的 TIME 序列（只要各自是轴的连续子区间）。
- `time_type` 从 time 列类型推断（Date32 / TimestampUs）；缺失时间点由对应 Field 的 NULL 表示。
- 构建结果不可变：layout 固定、进入只读状态。

### 6.2 create_meta_file

**接口定义**：
```rust
pub fn create_meta_file(path: &Path, data: &DataView<'_>) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | `.meta` 文件路径 |
| `data` | `&DataView<'_>` | 输入 | sym / time 两列逻辑数据（要求同 `MetaBuilder::build`） |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = `.meta` 创建 / 替换完成 |

**内部实现**：
```
MetaBuilder::build(data) → write_meta_atomic(path, bytes)
write_meta_atomic: 写 path.tmp（create + truncate）→ write_all(bytes) → sync_all（tmp 完整落盘）
→ fs::rename(tmp, path) 原子替换；失败清理 tmp，旧 META 保持不变
```

**说明**：
- 原子提交规则见 splayed-format §9：写临时文件 → fsync → 原子 rename。

### 6.3 delete_meta_file

**接口定义**：
```rust
pub fn delete_meta_file(path: &Path) -> Result<(), CoreError>
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | `.meta` 文件路径 |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 文件已删除 |

**说明**：
- 直接删除物理文件；调用方保证没有打开的 MetaHandle。

### 6.4 MetaHandle::open

**接口定义**：
```rust
impl MetaHandle {
    pub fn open(path: &Path) -> Result<Self, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `path` | `&Path` | 输入 | `.meta` 文件路径（必须已存在） |
| 返回 | `Result<MetaHandle, CoreError>` | 输出 | 只读 Handle |

**内部实现**：
```
File::open → Mmap::map（只读）→ MetaHeader::from_bytes + validate
（chunk 信息按需经 SYM INDEX / TIME AXIS 视图访问，无预物化）
```

**说明**：
- META 无 write mode：META immutable，内容或 layout 变化时由 `MetaBuilder` 构建新文件原子替换。
- open 后所有 META / Index 操作经 Handle 执行；无逐字段打开开销。

### 6.5 read_meta_handle（与属性访问器）

**接口定义**：
```rust
pub struct MetaInfo {
    pub version: u16,
    pub time_type: TimeType,
    pub generation: u64,
    pub time_count: u32,
    pub sym_count: u32,
    pub row_count: u32,
}
impl MetaHandle {
    pub fn read_meta_handle(&self) -> MetaInfo
    pub fn header(&self) -> &MetaHeader
    pub fn time_type(&self) -> TimeType
    pub fn sym_record(&self, sym_id: u32) -> Result<SymIndexRecord, CoreError>
    pub fn sym_str(&self, sym_id: u32) -> Result<&str, CoreError>
    pub fn sym_id_of(&self, name: &str) -> Result<Option<u32>, CoreError>
    pub fn time_at(&self, index: u32) -> Result<u64, CoreError>
    pub fn locate_row(&self, row: u64) -> Result<(u32, u32), CoreError>
    pub fn axis_index_of(&self, value: u64) -> Option<u32>
}
```

**参数与返回**：

| 方法 | 返回类型 | 说明 |
| --- | --- | --- |
| `read_meta_handle()` | `MetaInfo`（owned） | 结构化元信息：version / time_type / generation / time_count / sym_count / row_count |
| `header()` | `&MetaHeader` | 原始 header 视图 |
| `time_type()` | `TimeType` | 时间类型（Date32 / TimestampUs） |
| `sym_record(sym_id)` | `Result<SymIndexRecord>` | 第 `sym_id` 个 SYM INDEX record（time_start / time_count / row_start） |
| `sym_str(sym_id)` | `Result<&str>` | 字典中第 `sym_id` 个符号字符串（零拷贝，指向 mmap） |
| `sym_id_of(name)` | `Result<Option<u32>>` | 符号 → 字典 id（字典二分 O(log S)，按首现序 = 排序序；不存在 → `None`） |
| `time_at(index)` | `Result<u64>` | TIME AXIS 第 `index` 个时间值（零拷贝读取） |
| `locate_row(row)` | `Result<(u32, u32)>` | 逻辑行 → `(sym_id, 轴下标)`（SYM INDEX 二分；`row ≥ row_count` → Error） |
| `axis_index_of(value)` | `Option<u32>` | 时间值 → TIME AXIS 下标（精确匹配；不存在 → `None`） |

**说明**：
- 只读取结构化信息与单点定位；不做批量 Index 扫描；调用方无需了解物理布局。
- `sym_id_of` 的字典二分要求字典有序——META 构建按首现序写入字典，输入有序保证首现序 = 排序序。

### 6.6 read_index_handle

**接口定义**：
```rust
impl MetaHandle {
    pub fn read_index_handle(&self, offset: u64, length: u64) -> Result<DataView<'_>, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&MetaHandle` | 输入 | 只读 Handle |
| `offset` | `u64` | 输入 | Index 逻辑行空间（容量网格）中的起始行 |
| `length` | `u64` | 输入 | 读取行数；`offset + length ≤ row_count`；`length = 0` 返回空 view |
| 返回 | `Result<DataView<'_>, CoreError>` | 输出 | 该逻辑行段的 (sym, time) 两列 view（零拷贝） |

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

**说明**：
- 返回该逻辑行段的 `(sym, time)` 两列 view：sym 以字典视图返回（指向 SYM DICT / STRING DATA），time 直接指向 TIME AXIS——两者零拷贝。
- 物理布局（TIME AXIS / DICT / INDEX 交错）由 core 内部组装，上层只见逻辑两列。
- view 生命周期不超过 Handle。

### 6.7 scan_index_handle

**接口定义**：
```rust
impl MetaHandle {
    pub fn scan_index_handle(&self, request: &ScanRequest) -> Result<IndexScanner, CoreError>
}
impl IndexScanner {
    pub fn next(&mut self) -> Result<Option<RowRange>, CoreError>
    pub fn close(self) -> Result<(), CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&MetaHandle` | 输入 | 只读 Handle |
| `request.ranges` | `&[RowRange]` | 输入 | 上游候选范围（空 = 不限制），与候选求交 |
| `request.predicate` | `Option<Predicate>` | 输入 | 作用于 sym / time 的条件（如 `sym = "AAPL" AND time >= t1 AND time < t2`） |
| `request.limit` | `Option<u64>` | 输入 | 达到后提前结束 |
| 返回 scanner | `Result<IndexScanner, CoreError>` | 输出 | 定位器；`next()` 每次返回一个 FIELD row range（逻辑 = 物理），结束返回 `None` |

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

**说明**：
- 输出满足条件的 FIELD row ranges（逻辑 = 物理），可直接交给 `read_field_handle`，或作为 `scan_field_handle` 的 `ranges` 输入做多字段 predicate pushdown。

典型执行链：

```
SYM/TIME predicate → scan_index_handle → RowRanges
    → scan_field_handle (predicate A) → 更小 ranges
    → scan_field_handle (predicate B) → final ranges
    → read_field_handle / read_dataset
```

META / Field scan 不负责决定多 Field predicate 的执行顺序；顺序由上层 planner 决定。

### 6.8 locate_index_handle

**接口定义**：
```rust
impl MetaHandle {
    pub fn locate_index_handle(&self, pairs: &[(String, i64)]) -> Result<Vec<RowRange>, CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `&self` | `&MetaHandle` | 输入 | 只读 Handle |
| `pairs` | `&[(String, i64)]` | 输入 | `(sym, time)` 联合键序列；必须按 `(sym ASC, time ASC)` 排序且唯一 |
| 返回 | `Result<Vec<RowRange>, CoreError>` | 输出 | 合并后的连续 RowRanges；`sum(length) == pairs.len()` 是定位成功的充要条件 |

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

**说明**：
- META 利用输入有序性 × SYM INDEX / TIME AXIS 有序性做双指针扫描，避免逐行查找。
- 返回与输入分段对应的 `RowRange[]`：每个 RowRange 对应一段连续输入与 Dataset 中一段连续逻辑行。
- 供 `write_table` 等批量「按 key 定位已有行」的场景；不读取 Field 数据。

与 `scan_index_handle` 的区别：

```
scan_index_handle    条件查询 → RowRanges
locate_index_handle  (sym, time) 联合键 → RowRanges
```

### 6.9 close_meta_handle

**接口定义**：
```rust
impl MetaHandle {
    pub fn close(self) -> Result<(), CoreError>
}
```

**参数**：

| 参数 | 类型 | 方向 | 说明 |
| --- | --- | --- | --- |
| `self` | `MetaHandle` | 输入 | 按值消费 Handle |
| 返回 | `Result<(), CoreError>` | 输出 | Ok = 资源释放完成 |

**说明**：
- close 后 Handle 不可再用；释放 fd / mmap 等资源；META 无任何写回。

### 6.10 META 更新策略

META immutable，无 `update_meta()`。内容或 layout 变化（新增 sym、扩大 time 范围、布局重排）时由 `MetaBuilder` 构建新文件：写临时文件 → 原子 rename 替换旧 `.meta`；替换原子完成，不出现 META 短暂不存在的状态。
