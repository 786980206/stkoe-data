# splayed-core：接口与架构分层设计（Draft）

# splayed-core API

只讨论 **接口功能、命名、参数、返回值与职责边界**。暂不展开 Dataset / Partition 等逻辑抽象，也不把内部实现细节作为 public API。

## 核心对象

直接围绕实际文件类型设计 API：

- `.meta`：元数据
- `.sub.xxx`：子数据 / 辅助数据
- `field`：实际字段数据

## 命名规则

统一采用：

```
action_object_handle
action_object_file
```

- `*_file`：直接操作物理文件，不依赖已经打开的 Handle。
- `*_handle`：操作已经打开的 Handle。

这样明确区分“物理文件级操作”和“已打开资源上的操作”。

## API 分层

目前 core 的接口设计以 File / Handle 两层为基础：

```
File API
├── Field
│   ├── create_field_file()
│   ├── open_field_file(path, mode)
│   ├── delete_field_file()
│   ├── compress_field_file()
│   └── decompress_field_file()
│
└── 其他文件类型
    └── 后续分别定义

Handle API
└── FieldHandle
    ├── read_field_handle()
    ├── write_field_handle()
    ├── scan_field_handle()
    ├── update_field_handle()
    └── close_field_handle()
```

## Field API

Field 的完整接口与语义已单独整理：

[🧱 splayed-core：Field API](%F0%9F%A7%B1%20splayed-core%EF%BC%9AField%20API%203d0d258e52cc811b879ae50448d3d4f6.md)

核心原则：

- Field 是实际字段数据文件。
- `FieldHandle` 是对外不透明的生命周期对象，可以内部管理真实文件 Handle、header、data backing、解压后的 working representation、mode 与修改状态等资源。
- compression 是 Field 的物理存储状态；正常通过 Handle 访问时，对上层透明。
- 同时提供独立的 `compress_field_file()` / `decompress_field_file()`，用于直接改变已有 Field 文件的物理 representation。

## 当前明确的 Field public API

```
// File
create_field_file()
open_field_file(path, mode)
delete_field_file()
compress_field_file()
decompress_field_file()

// Handle
read_field_handle()
write_field_handle()
scan_field_handle()
update_field_handle()
close_field_handle()
```

Handle 内部的：

```
compress_field_handle()
decompress_field_handle()
dump_field_file()
```

仍属于 private / internal implementation，不作为 public API。

## 后续

其他文件类型的 API 在确定其数据布局与职责后，再分别整理成独立笔记；不在本页提前引入 Dataset / Partition 等更高层逻辑抽象。

## .meta

`.meta` 是与数据字段文件配套的只读结构化文件，同时承载 **Meta** 与 **Index** 两层职责。

```
.meta
├── Meta
│   └── META 文件自身的结构化信息
│
└── Index
    └── SYM / TIME → FIELD row range
```

- `sym`：symbol 维度
- `time`：时间维度
- `.meta` 通过 Index 将 `SYM / TIME` 条件转换为 FIELD row ranges
- `.meta` 只读，不提供写入或更新接口
- `.meta` 不支持压缩/解压
- `.meta` 的内容或 layout 发生变化时，重新通过 `MetaBuilder` 构建新的 META 文件，再替换旧文件；不提供 `update_meta()`

`.meta` API 单独整理为：

[🧱 splayed-core：META API](%F0%9F%A7%B1%20splayed-core%EF%BC%9AMETA%20API%203d0d258e52cc81e08da8e982dec4e900.md)

核心原则：

- `read_meta_handle()` 负责读取 META 的结构化信息。
- `read_index_handle(handle, offset, length)` 与 `read_field_handle()` 保持一致的读取参数语义，返回 Index 逻辑数据对应的 SYM / TIME view。
- `scan_index_handle(handle, request)` 与 `scan_field_handle()` 使用统一的 `ScanRequest` / `ranges` 抽象；Index Scan 根据 SYM / TIME predicate 产生 FIELD row ranges。
- `ranges` 是可选的 candidate range 输入，可以来自上一个 predicate 阶段；具体多个字段 predicate 的执行顺序由上层 query planner / execution engine 决定。

[🧭 splayed：API 总览与调用关系](%F0%9F%A7%AD%20splayed%EF%BC%9AAPI%20%E6%80%BB%E8%A7%88%E4%B8%8E%E8%B0%83%E7%94%A8%E5%85%B3%E7%B3%BB%203d1d258e52cc818db96fd3f26e71e900.md)

[🧱 splayed-core：核心数据格式（META / Field / Data / DataView）](%F0%9F%A7%B1%20splayed-core%EF%BC%9A%E6%A0%B8%E5%BF%83%E6%95%B0%E6%8D%AE%E6%A0%BC%E5%BC%8F%EF%BC%88META%20Field%20Data%20DataView%EF%BC%89%203d0d258e52cc8128aff9fef662a0ccc1.md)

[🧱 splayed-core：Field API](%F0%9F%A7%B1%20splayed-core%EF%BC%9AField%20API%203d0d258e52cc811b879ae50448d3d4f6.md)

[🧱 splayed-core：META API](%F0%9F%A7%B1%20splayed-core%EF%BC%9AMETA%20API%203d0d258e52cc81e08da8e982dec4e900.md)

[🧱 splayed-core：Dataset API](%F0%9F%A7%B1%20splayed-core%EF%BC%9ADataset%20API%203d0d258e52cc8138b95fd64db65cacde.md)

## 数据抽象更新

Core 数据参数统一遵循：单列使用 `ColumnView`，多列使用 `Data / DataView`。

```
Field 读写       → ColumnView
META 读/创建     → DataView
Dataset Field    → ColumnView
Dataset Index    → DataView
```

`ColumnView` 是单列非拥有视图，包含 `type / values / validity / length`，用于保持 Field → DataView → Arrow 等路径的 0-copy。

[Table API](Table%20API%203d1d258e52cc815b9387fdb88e66bdd4.md)