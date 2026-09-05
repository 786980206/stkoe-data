# splayed-core / Dataset API

## 7. Dataset API

### 7.1 结构与对象

```
Dataset（目录）
├── .meta                     // Index：sym/time → 逻辑行
└── <name>.field × N          // 字段数据文件
```

- `open_dataset` 只打开 META；Dataset Schema 从 META 与 Field 元信息得到。
- Field Handle 按需打开；首次访问某个 Field 时校验其与 Dataset 的 generation / Schema 一致性。
- Field 名称 ↔ 文件名的映射由 Dataset 层定义（沿用 V1.0 的字段名文件名约定）。
- Dataset 不引入独立 Schema 文件、独立 Index 文件——Dataset Index 就是 `.meta`。

### 7.2 总览

| 接口 | 职责 | 层 |
| --- | --- | --- |
| `create_dataset` | 创建完整 Dataset（META + Fields） | File |
| `create_dataset_index` | 创建 / 重建 `.meta` | File |
| `open_dataset` | 打开 Dataset，返回 `DatasetHandle` | File |
| `delete_dataset` | 删除完整 Dataset 目录 | File |
| `create_dataset_field` | 新增单个 Field | Dataset |
| `create_dataset_fields` | 批量新增 Field | Dataset |
| `delete_dataset_field` | 删除指定 Field | Dataset |
| `rename_dataset_field` | 重命名指定 Field | Dataset |
| `cast_dataset_field` | 转换指定 Field 类型 | Dataset |
| `compress_dataset_field` / `decompress_dataset_field` | 压缩 / 解压指定 Field | Dataset |
| `read_dataset_schema` | 读取逻辑 Schema | Dataset |
| `read_dataset_statistics` | 读取 Dataset 级统计 | Dataset |
| `read_dataset` | 按逻辑行读取多列 | Dataset |
| `write_dataset` | 按逻辑行覆盖写入 | Dataset |
| `scan_dataset` | 条件扫描 → `DatasetScanner` | Dataset |
| `locate_dataset_index` | (sym, time) 联合键批量定位（转发 META） | Dataset |
| `close_dataset` | 关闭并释放资源 | Dataset |

> 规范说明：草稿中单字段入口与多字段入口重名（两处 `create_dataset_fields`），规范为 `create_dataset_field`（单）/ `create_dataset_fields`（批量）；`delete / cast / compress / decompress_dataset_field` 在草稿中签名缺失，此处补齐。新增 `locate_dataset_index` 作为 META `locate_index_handle` 的 Dataset 级封装，使 splayed-table 只依赖 Dataset API、不持有 MetaHandle。

### 7.3 create_dataset

```
create_dataset(path, data: Data) -> Result<()>
```

**内部实现**：
```
1. 校验 path 不存在、data 含 sym/time
2. create_meta_file(path/.meta, data.as_view())
      → MetaBuilder::build（批量 cast + run 检测）
      → write_meta_atomic（tmp + rename）
3. 遍历非 sym/time 列 → create_field_file(path/<name>, type, Data(col))
4. 失败 → remove_dir_all(path) 清理残留
```
- 新路径直写（无 temp dir 中转——目录不存在时无原子性需求）
- 失败时清理整个目录

- `data`：拥有数据所有权的完整表数据（Schema + 全部列值）。
- 必须包含 `sym` 与 `time`；输入必须按 `(sym ASC, time ASC)` 排序；不要求各 SYM 时间集合相同。
- sym / time 转为 META（Index），其余列逐个转为 Field 文件；三者来自同一份输入，天然一致。

流程：临时目录中创建 META + 全部 Field → 全部成功后原子 rename 到 `path`；任何一步失败不留不完整 Dataset，目标路径保持不变。

### 7.4 create_dataset_index

```
create_dataset_index(path, data) -> Result<()>
```

Dataset 层对 `create_meta_file(path/.meta, data)` 的封装；只创建 / 重建 `.meta`，不创建 Field。用于 META 重建场景。

### 7.5 open_dataset

```
open_dataset(path, mode) -> Result<DatasetHandle>
```

**内部实现**：
```
1. path.is_dir() 校验
2. MetaHandle::open(path/.meta)  →  File::open + Mmap::map + header 校验
3. build_schema(path, time_type)  →  read_dir 列出字段名（跳过 . 和 .tmp）
                                    →  逐 field File::open + read 64B header → data_type
4. 构造 DatasetHandle { meta, schema, fields: RefCell<HashMap>, mode }
```
- 不预打开 Field Handle——按需打开（首次访问时 ensure_field）
- open 成本 = META mmap + N 次 field header 读取（N = 字段数）

- `mode = read | write`（访问意图，不是压缩状态）。
- 打开时只打开 META 并计算 Schema；不预先打开任何 Field Handle。
- Schema 直接从返回的 DatasetHandle 获取（`read_dataset_schema`），不另设 `get_dataset_schema()`。

### 7.6 delete_dataset

```
delete_dataset(path) -> Result<()>
```

删除整个 Dataset 根目录（META + 全部 Field），不逐个删除。

### 7.7 Field 结构操作

```
create_dataset_field(path, name, data_type, init?) -> Result<()>
create_dataset_fields(path, data: DataView) -> Result<()>
delete_dataset_field(path, name) -> Result<()>
rename_dataset_field(path, name, new_name) -> Result<()>
cast_dataset_field(path, name, target_type) -> Result<()>
compress_dataset_field(path, name) -> Result<()>
decompress_dataset_field(path, name) -> Result<()>
```

共同语义：

- `sym / time` 不作为普通 Field 操作；其身份由 META 管理。
- 新增：名称不得与已有 Field 重复（批量时彼此也不得重复）；字段长度必须等于当前逻辑长度 `L`；不改变 sym / time 范围；完成后 Schema 同步增长。`create_dataset_field` 的 `init` 对齐 core `create_field_file`：省略 = 全 NULL（`length(L)`）；`data(ColumnView)` / `stream(reader)` = 带数据初始化，行数必须恰为 `L`，分配与写值一步完成；`data_type` 显式传入并与数据一致。
- 删除：内部复用 `delete_field_file`；META 不受影响；Schema 同步移除。
- rename：内部复用 `rename_field_file`（同目录原子 rename）；`new_name` 不得与已有 Field 重复、不得为 `sym` / `time`；原子完成，失败时原字段名保持不变；完成后 Schema 同步更新。
- cast：内部复用 `cast_field_file`（临时文件 + 原子替换在其内部完成）；完成后 Schema 中该 Field 类型更新。
- compress：由 META 的 SYM INDEX 生成 chunk 边界（`k` 个连续 sym，默认 `k = 8`；单 sym 区间超过上限 64K 行时按行数劈开），以 `offsets` 传给 `compress_field_file`；decompress：直接调用 `decompress_field_file`。批量 = 多次调用。
  compress 内部流程：borrow_mut().remove(name)（释放缓存 Handle）→ 遍历 SYM INDEX 累计 row_start → 每 k 个 sym 取一个边界 → 超长 sym 按行数劈开 → compress_field_file(path, Some(offsets))。

### 7.8 read_dataset_schema

```
read_dataset_schema(handle) -> Schema
```

- 返回当前全部逻辑字段（含 sym / time）及 `DataType`；不触发数据扫描。
- 不返回物理属性（encoding / compression / offset）。
- 返回对象为只读元数据视图，生命周期不超过 Dataset。

### 7.9 read_dataset_statistics

```
read_dataset_statistics(handle) -> DatasetStatistics
DatasetStatistics
├── row_count        // 逻辑行数 = L（容量网格）
├── sym_count
├── sym_min / sym_max
├── time_count
└── time_min / time_max
```

- 统计来自 META，不扫描 Field 数据；只读。
- `sym_min / sym_max` 取字典首尾；`time_min / time_max` 取 TIME AXIS 首尾。
- 不含 Field 级 min / max / null_count。

### 7.10 read_dataset

```
read_dataset(handle, offset, length, columns?) -> Result<DataView>
```

**内部实现**（两阶段借用）：
```
阶段 1（&mut self.fields）  →  ensure_field：确保所有需要的 Field Handle 已打开
阶段 2（&self.meta + &self.fields）  →  共享借用创建视图
    base = meta.read_index_handle(offset, length)
        →  locate_row(offset) 二分 SYM INDEX
        →  逐 sym：keys 常量填充（物化进 meta scratch arena）
        →  time 列切片 TIME AXIS（零拷贝多段）
    各 Field → field_handle(...).read_field_handle(offset, length)
        →  mmap 切片（PLAIN+NONE 单段）或 working 切片（compressed 多段）
    组装 → DataView
```
- 两阶段借用避免 &mut self.fields 与 &self.meta 冲突

- `offset / length` 是 Dataset 逻辑行范围；因逻辑 = 物理，各 Field 直接以相同 offset / length 读取，无需换算。
- 默认返回 `sym` 与 `time`（来自 META，零拷贝）；其余 Field 由 `columns` 指定，无需重复指定 sym / time。
- 一个范围可跨多个 sym；所需 Field 按需打开。
- 零拷贝优先；返回的 `DataView` 不拥有数据，生命周期不超过相关 Handle。

流程：

```
read_dataset(handle, offset, length, columns?)
        ├── META → sym/time view（TIME AXIS + SYM INDEX）
        ├── 各 Field → read_field_handle → ColumnView
        └── 组装 → DataView
```

### 7.11 write_dataset

```
write_dataset(handle, offset, data: DataView) -> Result<()>
```

**内部实现**：
```
1. mode.require_write + 边界校验（offset + len ≤ L）+ 类型校验
2. 逐列（按 data.schema 顺序）：
     ensure_field（&self.fields）
     self.fields.borrow_mut().get_mut(name).write_field_handle(offset, col_view)
3. 不保证跨 Field 原子性：第 N 列写入失败时前 N-1 列已生效
```
- write_field_handle 内部按 Field 的物理表示分派（mmap 直写或 working 修改）
- 每次 write 成功后 field generation += 1

职责：对已有逻辑行做 positional overwrite。

- 只覆盖已有数据区域；不改变 META layout、逻辑长度、sym / time 身份；不是追加接口。
- `offset + data.length ≤ L`；`length = 0` 合法 no-op。
- 支持 projection write：`data` 可只含 Schema 的部分字段；字段必须属于 Schema 且类型兼容；未提供字段保持原值。
- 所有输入列等长；`sym / time` 不作为写入列。
- 每列按名称定位到对应 Field，调用 `write_field_handle`（values + validity 成对写入；`validity = null` 表示本段全有效）。
- **不保证跨 Field 原子性**：多个 Field 独立写入，部分成功不回滚，Dataset 可能处于部分更新状态；不提供跨 Field transaction / rollback。

### 7.12 scan_dataset

```
scan_dataset(handle, request) -> Result<DatasetScanner>
DatasetScanner::next() -> Result<RowRange?>    // 逻辑行范围
```

**内部实现**：
```
1. clamp_ranges(request.ranges, L)  →  裁剪到 [0, L)
2. collect_for_fields(predicate, ["sym","time"])  →  提取 sym/time 子谓词
     →  meta.scan_index_handle(sym_time_req)  →  narrowed ranges
3. predicate_groups(predicate)  →  按字段分组（跳过 sym/time 保留名）
     →  按名称排序 → 逐 Field：ensure_field → scan_field_handle(sub_req)
     →  逐 Field 收窄 ranges（intersect_range_lists 归并求交）
4. DatasetScanner { ranges: VecDeque(current), remaining: request.limit }
```
- 跨字段的 Or / Not 不支持（返回 Invalid）——行级过滤由上层兜底
- 求交用双指针归并 O(a + b)，非 O(a × b)

- `ScanRequest.ranges`：逻辑行候选范围（空 = 整个 Dataset）；`projection`：需要读取的字段集合；`predicate` / `limit` 同公共契约。
- Scanner 组合 META 与各 Field 的扫描结果（求交 / 裁剪 / 合并），输出 **Dataset 逻辑 RowRange**。
- 只定位不读取；实际数据由 `read_dataset` 消费；batch 收集由上层负责。
- 多 predicate 的执行顺序由上层 planner 决定。

流程：

```
scan_index（sym/time 条件）→ candidate ranges
    → scan_field_handle（值条件，逐 Field）→ 求交 / 裁剪
    → DatasetScanner → 逻辑 RowRange → read_dataset → DataView
```

### 7.13 locate_dataset_index

```
locate_dataset_index(handle, data: DataView) -> Result<RowRange[]>
```

META `locate_index_handle` 的 Dataset 级封装（参数与语义一致）。供 splayed-table 的 `write_table` 批量定位使用；Table 层不直接持有 MetaHandle。

### 7.14 close_dataset

```
close_dataset(handle) -> Result<()>
```

- 关闭 META Handle，释放 Dataset 层维护的 Field Handle / 元数据。
- 已返回的 `DataView` / `ColumnView` 在依赖资源关闭后不再保证有效。
- 关闭后不可继续 Dataset 读写或扫描。

### 7.15 Dataset 结构变化的边界

```
Field 级变化（原地）
├── create_dataset_field(s) / delete_dataset_field / cast_dataset_field
└── compress / decompress_dataset_field

Dataset 级变化（重建）
├── 扩大容量 / 新增 sym / 扩大 time 范围
└── 改变整体物理布局与 META/Field 对应关系 → 重建 Dataset，不提供原地 API
```
