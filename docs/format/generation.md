# Generation 与原子提交

## Generation 版本一致性

用于判断 META 与 FIELD 是否属于同一个数据版本：

```text
META    generation = 12345
close   generation = 12345
volume  generation = 12345
```

若 FIELD generation 与 META 不匹配（如 `close = 12344`），Reader 拒绝使用。

- **V1 使用单独的 uint64 generation 序列**（严格单调递增），而非 wall-clock timestamp（时钟回拨不单调）。
- `generation` 在 `update_field` 成功后递增。
- `update_meta` 重建 META 时 generation+1，并把全部 FIELD 重写为新 generation。

## 原子 META 提交

```text
.meta.new
   |
   v
fsync
   |
   v
atomic rename
   |
   v
.meta
```

保证 Reader 看到完整 generation。

## update_meta（布局重排）流程

`update_meta(dir, time_type, sym, time)` 以新 (SYM, TIME) 布局重建 `.meta` 并**把全部现有 FIELD 并发重散布到新布局**（区别于 `update_table` 的「格子内更新」）：

1. 读旧 meta；
2. `MetaBuilder` 建新布局（generation+1）；
3. 构建 **gather 映射**：新行 (sym,time) → 旧行号（旧 meta 符号哈希 + 区间二分；旧数据没有 → NULL）；
4. 写 `.meta.new`（fsync **暂不改名**）；
5. **每字段一线程**并发重写 `name.tmp`（新 generation、真实 `null_count`、重算统计 footer；gather 连续段一次 memcpy）→ fsync + rename；
6. 最后 rename `.meta.new → .meta`（**提交点**）。

**原子性**：提交点之前，任何 Reader 因 FIELD/META **generation 不匹配而拒绝**；中途失败重跑本函数即完成提交（重散布确定性、幂等）。

**上层联动**：`DataFusion::SplayedDatasetProvider::reload()` / ADBC `Connection::refresh()` 之后上层立即看到新布局。

!!! warning "Windows"
    被 mmap 的字段无法 rename——调用方须先 drop 所有打开的 FieldReader / provider / 连接再调用。
