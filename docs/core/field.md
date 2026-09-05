# splayed-core / Field API

## 5. Field API

### 5.0 总览

| 接口 | 职责 | 层 |
| --- | --- | --- |
| `create_field_file` | 创建 Field 文件并初始化 | File |
| `open_field_file` | 打开已有 Field，返回 `FieldHandle` | File |
| `delete_field_file` | 删除 Field 物理文件 | File |
| `rename_field_file` | 重命名 Field 文件 | File |
| `cast_field_file` | 将 Field 原地转换为 `target_type` | File |
| `compress_field_file` | uncompressed → compressed 物理表示 | File |
| `decompress_field_file` | compressed → uncompressed 物理表示 | File |
| `read_field_handle` | 按逻辑行读取，返回 `ColumnView` | Handle |
| `write_field_handle` | 按逻辑行覆盖写入 | Handle |
| `update_field_handle` | 修改 FieldHeader（不改 data） | Handle |
| `scan_field_handle` | 条件扫描 → `FieldScanner` | Handle |
| `close_field_handle` | 关闭 Handle；compressed write 收尾 | Handle |

### 5.1 create_field_file

**内部实现**：
```
length(n)  →  File::create_new(path) → 写 64B header → set_len(64+data+validity)
                （OS 零填充，无需显式写 NULL）
data(col)  →  File::create_new → 写 header → write_all(values) → write_all(validity_bits)
stream(r)  →  File::create_new → 写占位 header → 循环 write_all(values)+write_all(bits)
                → seek(0) 回填 header（row_count/data_length/null_count）
```
- 三种 init 共用 `File::options().write(true).create_new(true)` 防止覆盖已有文件
- data 形态全有效时 `has_validity = 0`（不写 validity 区，文件更小）

```
create_field_file(path, data_type, init) -> Result<()>
init = length(n) | data(ColumnView) | stream(reader)
```

职责：创建并初始化一个 Field 文件。

- `length(n)`：创建指定逻辑长度的空占位 Field（全 NULL，validity 全 0）。
- `data(ColumnView)`：以给定数据初始化；长度由数据推断。
- `stream(reader)`：从流式数据源持续读取初始化；最终长度无需预先知道。
- header 不要求调用方完整构造；可从 `path / data_type / init` 推断的信息由 core 生成。

流程（按 `init` 分派，分配与写值一步完成，不做先预分配再写值的二次写入）：

- `length(n)`：写 header（`row_count = n`）→ 预分配 DATA（NULL）+ VALIDITY（全 0 位）。
- `data(ColumnView)`：`row_count` = 数据长度；header + values + validity 一次性顺序写出；数据全有效时不写 validity 区（`has_validity = 0`）。
- `stream(reader)`：写 header → 流式追加 values + validity → 结束时回填 `row_count` / `data_length` / `null_count`。

创建完成前 fsync，配合上层（Dataset / Table）的临时文件 + 原子 rename。

注意事项：

- 创建完成后才可被 `open_field_file` 打开；create 不返回 Handle。
- 带数据初始化时写真实 `null_count`；stream 初始化由 validity 位 word 批量 popcount 精确统计（批内行尾填充位不计）。

### 5.2 open_field_file

```
open_field_file(path, mode) -> Result<FieldHandle>
```

- Field 必须已存在；open 不负责创建。
- 打开时校验 magic / version / generation。
- `read`：允许 read / scan；不修改原文件；compressed Field 的解压对上层隐藏。
- `write`：允许 read / scan / write / update。uncompressed Field 直接原地修改；compressed Field 内部进入解压后的 working representation，发生修改后由 close 自动重压缩写回。

### 5.3 rename_field_file

```
rename_field_file(path, new_name) -> Result<()>
```

- 同目录内重命名 Field 文件；文件名即字段名（沿用 Dataset 层的名称解析约定）。
- 原子完成；`new_name` 对应文件已存在时 Error，不覆盖。
- 只改文件名，不修改数据、header、generation。
- 字段名合法性与重复检查由上层负责。

### 5.4 read_field_handle

```
read_field_handle(handle, offset, length) -> Result<ColumnView>
```

**内部实现**：
```
bounds        →  end = offset.checked_add(length)（溢出安全）→ end ≤ row_count
length = 0    →  ColumnView::empty（单零行段，满足 segments 非空不变式）
uncompressed  →  mmap 切片 values[offset×size .. (offset+length)×size]
                  + BitmapView::new(validity_bytes, offset, length)
                  → 单段 ColumnView::from_one
compressed    →  chunk_ends（open 时累积行末）上 partition_point 二分首个 chunk
                  → 自首个 chunk 顺序切段，cstart ≥ end 即 break
                  → 每段 ColumnSegment::new（values 字节切片 + validity.slice 位视图，均零拷贝）
                  → ColumnView::new(多段；段容量按平均 chunk 行数预分配)
```
- 职责边界：read 只负责"逻辑行范围 → ColumnView"转换 —— 解压在 open 一次性完成；
  不在此做数据复制、段合并或谓词求值（归 Scanner / Dataset 层）
- PLAIN+NONE 返回的值指针直接指向 mmap 区域（零拷贝）
- compressed Field 打开时全量解压到 `Working { values: Buffer, validity: Option<Bitmap> }`，
  后续读取从 working 上切片（chunk 级惰性解码为优化项）
- compressed 定位复杂度 O(log C + 交叠 chunk 数)：二分定位 + 交叠段连续产出，
  不从头遍历 chunk、不做逐 chunk 前缀和；validity 只建位视图（`BitmapView::slice`），不复制位图
- `offset / length` 为逻辑行（= 物理行）；`offset + length ≤ row_count`（`checked_add` 溢出安全）；`length = 0` 返回空 view。
- 返回 zero-copy ColumnView：`PLAIN + NONE` 为单段 mmap 切片；compressed Field 逐 chunk 物化，跨 chunk 的读取返回多段。
- view 生命周期不能超过 Handle / 底层资源；close 后失效。
- 不提供 `parallel` 参数，并发由上层控制。

### 5.5 write_field_handle

```
write_field_handle(handle, offset, data: ColumnView) -> Result<()>
```

**内部实现**（按物理表示分派；写入三原则：values 逐段 memcpy、validity 字节/word 批量、null_count 增量）：
```
bounds        →  end = offset.checked_add(data.length)（溢出安全）→ end ≤ row_count
length = 0    →  no-op（不改数据、不递增 generation）
uncompressed  →  先校验后写入（无 validity 区的字段拒绝含 NULL 的段）
                  → 逐段：values copy_from_slice（一次连续 memcpy）
                  + validity：BitmapView::copy_bits_into / bitmap_fill_bits（按字节批量：
                    头尾掩码 RMW，中间同相位 memcpy / 异相位逐字节移位，不逐 bit）
                  + null_count 按（覆盖前 1 位数 − 覆盖后 1 位数）增量修正
                  → generation += 1 → header 回写 mmap[0..64]（落盘即含新 generation / null_count）
compressed    →  working.values 同上逐段 copy_from_slice
                  + 若段含 NULL 且 working 无位图 → 先物化全 1 位图
                  + Bitmap::copy_bits_from / set_range（字节批量）
                  + null_count 增量修正（基线 open 时由解压位图精确重建）→ modified = true
```
- 复杂度 O(data.length)，实际执行以连续内存复制为主（接近 memcpy）；`generation` 每次 write 调用恰好 +1，不按段递增
- write 路径不做 fsync（uncompressed 写入 mmap 即生效；compressed 在 close 时统一落盘）
- 并发写非重叠区域安全：MmapMut 或 working 上按 offset 切片互不干扰

职责：positional overwrite，按逻辑行覆盖写入。

- 需要 write mode；从 `offset` 起覆盖写入。
- values + validity 成对写入；`data` 含多个 segment 时按逻辑行序逐段写入，segment 的 `validity = null` 表示该段全部有效。
- 只改 data，不改 header；不改变逻辑长度；`offset + data.length ≤ row_count`。
- 这是覆盖写，不是追加 / 扩容接口。
- compressed Field 修改内部 working representation，close 时统一收尾。
- 成功后递增 `FIELD.generation`。
- 允许并发写非重叠区域；重叠区域不允许；并发度由上层控制。

> 规范说明：数据参数统一为 `ColumnView`（草稿中 buffer/stream 与 ColumnView 混用）。写路径长度有界，流式大数据 = 分块多次调用；`stream` 仅保留在 create 的 `init` 中（最终长度未知的场景）。

### 5.6 update_field_handle

```
update_field_handle(handle, header: FieldHeader) -> Result<()>
```

- 只修改 header，不修改 data；core 校验 header 与现有 data 的一致性（`row_count`、`data_type` 等）。
- `null_count` 为派生统计，update 不接受调用方改写（保持现值，由写路径增量维护）。
- 与 `write_field_handle` 的区别：write 改 data，update 改 header。
- compressed Field 的 header 更新随 close 流程保持文件一致。

### 5.7 scan_field_handle

```
scan_field_handle(handle, request) -> Result<FieldScanner>
FieldScanner::next() -> Result<RowRange?>
```

**内部实现**（顺序批量管线，不物化数据）：
```
构造      →  ranges 直接顺序消费：仅与 [0, row_count) 求交防越界（保序、不排序、不合并；
              有序不重叠由上游保证）；空 ranges = 整个 Field
next()    →  消费当前段命中位图：word 级 next_true_run 找下一连续命中区 → RowRange
              （word 跳零字 / 满字扩展；limit 达到即截断并结束）
段求值    →  段 = [row, min(range.end, row + 1M))：整段连续 values（PLAIN mmap 切片 /
              compressed working 切片，零拷贝）批量求谓词
              → Cmp：类型化切片比较循环（算子分派在循环外，LLVM 自动向量化；
                跨域加宽语义保持 compare_scalar 规则：有符号 ↔ 无符号 ↔ 浮点）
              → And / Or / Not：字节级位运算（AND 全零短路）
              → 根部统一与 validity 求交：NULL 行不命中任何条件（含 NOT / OR）
              → 0/1 字节掩码 → branchless 打包位图（缓冲跨 next() 重用）
```
- 不复制 values、不物化数据、不创建 DataView、不逐行生成 RowRange、不做 ranges merge、不处理 sym / time
- 输出为连续命中区：段内相邻命中行合并为单个 RowRange；跨段不合并
- 无谓词 = 只输出有效行（validity 过滤）
- 浮点比较遵循 IEEE 语义（NaN 行 / NaN 目标按 IEEE 求值，仅 Ne 命中；不再逐行报错）
- 段上限 1M 行仅为限定掩码内存；掩码缓冲跨 `next()` 重用
- 只定位，不物化数据；输出可交给 `read_field_handle`，或作为其他 Field scan 的 `ranges` 输入做多字段下推。
- Field 不理解 sym / time；只做值过滤。不支持 order 下推。

### 5.8 close_field_handle

```
close_field_handle(handle) -> Result<()>
```

**内部实现**（按 mode × 是否 chunked 分派）：
```
read                            → 直接 Ok（Mmap 随 Drop 释放）
write + uncompressed            → MmapMut::flush → Ok
write + compressed + 未修改      → Ok（不写回）
write + compressed + 已修改      → 流式：header（编码前即完全确定，无需占位回填）直写 tmp
                                    → 逐 chunk：从 working 切段 → extract_bits → encode_chunk
                                      → 直写 tmp（内存 O(working + 一个 chunk)，不拼接整个重压缩文件）
                                    → tmp sync_all（rename 生效时新文件内容已持久）
                                    → fs::rename(tmp, path) 原子替换；失败清理 tmp，原文件保持不变
```
- compressed 重压缩沿用文件既有 chunk 分组（打开时从 chunk 头读得，写路径不改 row_count）
- 临时文件路径 = `{field_path}.tmp`，rename 原子替换

- close 后 Handle 不可再用；释放 fd / mmap / working memory。
- read handle：无写回。
- uncompressed write handle：写入已直接生效，无需额外动作。
- compressed write handle：发生修改 → 自动 compress + rewrite，文件保持 compressed；重压缩沿用文件既有 chunk 分组（打开时从 chunk 头读得，自描述，不依赖 META；写路径不改 `row_count`，`Σ rows == row_count` 恒成立，分组可精确复用），写临时文件后原子替换；未修改 → 不写回。
- 不提供 `commit / flush / dump` public API。

### 5.9 cast_field_file

```
cast_field_file(path, target_type) -> Result<()>
```

- 读取 `path` 处 Field → 数据类型转换 → 写临时文件 → 原子 rename 替换原文件；对外表现为原地转换。
- 转换成功后该 Field 的 `data_type` 为 `target_type`，逻辑数据逐行完成类型转换。
- 转换失败时原文件保持不变。
- 原文件的压缩状态对调用方透明；转换通过临时文件完成，不属于 `write_field_handle` 的原地覆盖。

### 5.10 compress_field_file / decompress_field_file

```
compress_field_file(path, offsets?) -> Result<()>
decompress_field_file(path) -> Result<()>
```

**compress 内部实现**：
```
open(read) → clone_view → drop(handle)
    → 边界生成（offsets 或均匀 8192）
    → 逐段 encode_chunk(Plain, Zstd, ...) → 拼 header + chunks
    → write_field_atomic（tmp + rename）
```

**decompress 内部实现**：
```
open(read) → decode_working（全量解压）→ drop(handle)
    → 写 uncompressed header + values + validity
    → write_field_atomic
```

- File 级物理表示转换；不依赖已打开 Handle；逻辑数据与 header 语义不变。
- `offsets`：可选的 chunk 起始行号，升序、`offsets[0] == 0`，隐含最后一块延伸到 `row_count`；省略时按固定 8192 行均匀分块（最后一块允许不足）。
- 分块策略是调用方的职责：Dataset 层按 META 网格生成 sym 对齐边界（见 7.7），裸调用可省略 `offsets`。
- 状态不符时返回明确错误（如 AlreadyCompressed / NotCompressed），不做静默 no-op。
- 与 close 的自动压缩互补：一个面向离线维护，一个面向写生命周期。
