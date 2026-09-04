# 开发流程

## 权威来源

**`plan.md` 是格式与 API 决策的权威来源**。改动任何格式、header、函数签名前，先读 `plan.md` 的相关章节；改动磁盘格式后同步更新 [磁盘格式规范](../format/index.md) 与 `README.md`。

## 如何添加新特性

1. 在 `plan.md` 找到相关章节。
2. 在正确的 crate 实现（format → codec → core → arrow）。
3. 模块内添加单元测试。
4. 在 `crates/splayed-arrow/tests/integration.rs`（或新测试文件）添加集成测试。
5. 运行 `cargo build && cargo test` —— 零警告、全部通过。
6. 更新 `plan.md` 阶段状态、`README.md`、本文档。

## 如何添加新 DataType

1. 在 `splayed-format/src/types.rs` 的 `DataType` 枚举添加变体。
2. 实现 `size_of` / `null_bytes` / `from_id` / `as_str`。
3. 添加 `RawValue::from_*` 与 `as_*` 方法。
4. 更新 `arrow_conv.rs` 的 Arrow 映射。
5. 添加 NULL 检测与 roundtrip 测试。

## 测试模式

- 使用 `std::env::temp_dir()` + `std::process::id()` 保证临时目录唯一。
- 测试结束 `fs::remove_dir_all`（或开头 `let _ = fs::remove_dir_all`）。
- roundtrip：`create_table` 创建 → `FieldReader` 读回 → 断言值。
- 错误测试：`matches!(result, Err(ExpectedError::Variant))`。

## 什么不要做

- 不要给 `splayed-core` 添加 Arrow 依赖。
- 不要改动 header 布局而不更新 [磁盘格式规范](../format/index.md) 与 `plan.md` §5.1/§5.2。
- 不要用 `bytemuck::from_bytes` 处理 `Vec<u8>` 支撑的切片——用 `bytemuck::pod_read_unaligned`（对齐问题）。
- 不要假设 `f64::NAN` 与 canonical NaN 不同——测试中非 canonical NaN 用 `f64::from_bits(0x7FF8000000000001)`。
- Windows 上不要对同一文件保持 `FieldReader` 打开时执行写操作。
