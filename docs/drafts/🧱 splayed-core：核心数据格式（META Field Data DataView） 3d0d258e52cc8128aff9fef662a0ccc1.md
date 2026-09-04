# 🧱 splayed-core：核心数据格式（META / Field / Data / DataView）

# splayed-core：核心数据格式（META / Field / Data / DataView）

本页记录当前已经确定的 core 数据表示与物理文件设计。原则是：**简单、性能优先、0-copy 优先**。

## 1. 总体结构

```
DataType
   ↓
Buffer / BufferView
   ↓
Validity / BitmapView
   ↓
ColumnView
   ↓
Schema / FieldSchema
   ↓
Data / DataView
   ↓
Field / META
   ↓
Dataset
```

核心原则：

- `Data` / `DataView` 是逻辑数据交换结构，不是文件格式。
- `DataView` 默认 non-owning，优先 0-copy。
- Column 数据第一版必须 contiguous，不引入 stride。
- NULL 与数据分离，由 validity bitmap 表示。
- 不同 Column 可以来自不同 Buffer。
- Schema 是逻辑描述，不单独作为一个 schema 文件。

## 2. Buffer / BufferView

```
Buffer
├── ptr
├── size
├── alignment
└── ownership / lifetime

BufferView
├── ptr
└── size
```

- `Buffer` 负责底层内存及生命周期。
- `BufferView` 是 non-owning 视图。
- 支持 `Owned / Borrowed / Mmap`。
- `BufferView` 可通过 `offset + size` 构造，不复制数据。
- `BufferView` 生命周期不能超过底层 Buffer。

## 3. Validity / BitmapView

```
value:     10  20  ??  40  50
validity:   1   1   0   1   1
```

```
BitmapView
├── data: BufferView
├── offset: bit offset
└── length: bit count
```

- 一 bit 对应一个元素。
- `validity = null` 表示全部有效，不分配 bitmap。
- 支持 slice，不复制 bitmap。
- 连续 bitset 便于批量/SIMD 扫描。
- `BitmapView` non-owning，生命周期受底层 Buffer 约束。

## 4. ColumnView

第一版保持最简单：

```
ColumnView
├── data: BufferView
├── length
└── validity: BitmapView?
```

- Column 数据必须 contiguous。
- `ColumnView` 不拥有数据，只描述已有数据。
- `data` 与 `validity` 可以来自不同 Buffer。

## 5. Schema / FieldSchema

```
FieldSchema
├── name
└── data_type

Schema
└── fields[]
```

- `FieldSchema` 只描述字段名和类型。
- Schema 不携带压缩、编码、offset 等物理属性。
- Schema 从 Dataset / Data 的逻辑结构得到，不独立持久化。

## 6. DataView

`DataView` 是 core 内部优先使用的数据交换对象：

```
DataView
├── schema / field references
├── columns[]
└── length
```

语义：

- non-owning。
- read-only 语义优先。
- 不复制 Column 数据。
- 可以只包含部分字段（projection）。
- 所有 Column 具有统一 logical row count。
- 不要求不同 Column 在物理内存上连续。

示例：

```
DataView
├── price  → mmap A
├── volume → mmap B
├── qty    → Arrow buffer C
└── time   → mmap META
```

## 7. Data

```
Data
= owning / materialized

DataView
= non-owning / zero-copy
```

`Data` 是需要拥有数据时使用的物化数据结构；`DataView` 是零拷贝视图。

明确的物化边界：

```
DataView → Data
```

只有需要独立拥有数据时才发生必要的数据复制。

## 8. META

`.meta` 是 Dataset 唯一的定位索引，负责 `SYM/TIME → FIELD row range` 映射。

固定 Header：64 bytes。

```
0   8   magic
8   2   version
10  2   flags
12  1   time_type
13  3   reserved
16  8   generation
24  4   time_count
28  4   sym_count
32  8   sym_dict_offset
40  8   sym_index_offset
48  8   file_size
64      HEADER END
```

数据区：

```
TIME AXIS
  time_count × time_size

SYM DICT INDEX
  (sym_count + 1) × uint64

SYM STRING DATA
  variable length

SYM INDEX
  sym_count × 12 bytes
```

SYM INDEX：

```
0  4  time_start
4  4  time_count
8  4  row_start
```

其中 `time_count` 同时表示：

- SYM 在全局 TIME AXIS 中的连续时间区间长度；
- 对应 FIELD 的预留 row capacity。

区间内部缺失时间用 FIELD NULL 表示。

META 不支持压缩和原地更新；布局演进通过 rebuild 新 META，再原子替换旧文件。

## 9. META API

```
File
├── create_meta_file(path, data)
├── open_meta_file(path, mode)
└── delete_meta_file(path)

Handle
├── read_meta_handle()
├── read_index_handle(offset, length)
├── scan_index_handle(request)
└── close_meta_handle()
```

`create_meta_file(path, data)` 的 `data` 是已经按 `(sym ASC, time ASC)` 排序的两列逻辑数据。

`scan_index_handle()` 返回 FIELD 可复用的 row ranges，而不是直接返回 Column 数据。

## 10. Field API

```
File
├── create_field_file()
├── open_field_file(path, mode)
├── delete_field_file()
├── cast_field_file(source, target, target_type)
├── compress_field_file()
└── decompress_field_file()

Handle
├── read_field_handle()
├── write_field_handle()
├── scan_field_handle()
├── update_field_handle()
└── close_field_handle()
```

写入：

```
write_field_handle(field_handle, offset, data)
```

- 只做 positional overwrite。
- 不改变 logical length。
- `data` 支持 buffer 或 stream。
- compressed Field 在 write mode 下内部使用可写表示；关闭时按需重新压缩并写回。

读取：

```
read_field_handle(field_handle, offset, length) → view
```

返回 0-copy view；view 生命周期不能超过对应资源。

扫描：

```
scan_field_handle(field_handle, request) → scanner
```

`ScanRequest`：

```
ScanRequest
├── ranges
├── projection
├── predicate
├── limit
└── batch_size
```

scanner 输出 row ranges / selections。predicate 顺序由上层 query planner 决定。

## 11. Dataset API

Dataset 是一个物理存储目录，由 META + Field files 构成；不额外引入独立 schema 文件。

```
create_dataset(path, data)
```

- `data` 是完整表数据，包含 schema + column data。
- 必须包含 `sym`、`time`。
- 输入必须已经按 `(sym ASC, time ASC)` 排序。
- core 将 `sym/time` 转为 Dataset Index（`.meta`），其余列转为 Dataset Fields。

Dataset Index：

Dataset 不提供独立 Index 文件 API；Dataset Index 实际由 `.meta` 承载。

Dataset Field：

```
create_dataset_field(path, field, type, init)
delete_dataset_field(path, field)
cast_dataset_field(path, field, target_type)
```

这些是 Dataset 对外的 Field 结构操作入口，内部复用对应的 Field 文件 API。

## 12. 典型查询路径

```
.meta
  │
  ▼
scan_index
  │
  ▼
row ranges
  │
  ├──────────────┐
  ▼              ▼
price.field   volume.field
  │              │
  ▼              ▼
ColumnView    ColumnView
  └──────┬───────┘
         ▼
      DataView
```

多字段过滤时，同一组 ranges 可以逐步传递给不同 Field：

```
META predicate
      ↓
candidate ranges
      ↓
Field A scan
      ↓
smaller ranges
      ↓
Field B scan
      ↓
final ranges
```

谓词执行顺序属于上层 planner/executor，core 只提供高效的 range / scan / read 原语。

## 13. 当前设计边界

暂不增加：

- 独立 Schema 文件/API。
- `commit / flush / dump` 等写入控制 API。
- Field handle 的公开压缩/解压 API。
- META 原地 update API。
- stride / 非 contiguous Column。
- core 内部的全局 predicate reorder / query planner。

设计原则：**先保持 core API 足够薄，只有真正需要的能力才下沉到 core。**