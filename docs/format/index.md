# 磁盘格式规范

本节是磁盘格式的**权威定义**（对应用户侧 `plan.md` §5）。改动任何 header / 常量 / 布局前必须同步更新本节。

## 全局常量

| 常量 | 值 |
| --- | --- |
| META magic | `SPLAYMTA`（u64 LE） |
| FIELD magic | `SPLAYFLD`（u64 LE） |
| Header size | 64 字节（META 与 FIELD 均为 64B） |
| Data offset | 64（紧接 header） |
| SYM INDEX record | 12 字节：`time_start(4) + time_count(4) + row_start(4)` |
| META 文件名 | `.meta` |
| 格式版本 | 1 |

## 文件形态

```text
dataset/
├── .meta        # 唯一定位索引：TIME AXIS + SYM DICT + SYM INDEX
├── .sub.hs300   # 可选子集索引：父 `.meta` 网格的行区间子集（每 SYM 可多条区间）
├── close        # FIELD 纯数据：值 + 可选统计 footer
├── open
└── ...
```

## 核心映射

- `row_start(SYM_n) = sum(time_count(SYM_0 .. SYM_{n-1}))`
- `global_row = row_start + (time_index - time_start)`
- 子集：`.sub.xxx` 的 `row_start` 同样落在父 `.meta` 的全局行空间（直接指向 FIELD data 区）。

## 页面索引

- [META 格式](meta-format.md)
- [FIELD 格式](field-format.md)
- [SUBSET 格式（.sub.xxx）](subset-format.md)
- [类型与 NULL 编码](types-null.md)
- [编码与压缩](codec.md)
- [Generation 与原子提交](generation.md)
