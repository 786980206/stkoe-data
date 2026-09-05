# AGENTS.md

Guidance for AI agents working on the Splayed V2.0 codebase.

## Project Summary

Splayed V2.0 is a Rust columnar storage engine for `SYM × TIME × FIELD` financial time-series data. The V2.0 design docs live in `docs/` — they are the **authoritative source of truth** for all format and API decisions:

- `docs/splayed-format.md` — binary format（META/FIELD header、SYM INDEX、内存数据模型、NULL 语义、原子提交规则）
- `docs/splayed-core.md` + `docs/core/*.md` — core 三层 API（Field / META / Dataset）公共语义与各函数内部实现流程
- `docs/splayed-table.md` / `docs/splayed-arrow.md` / `docs/splayed-codec.md` / `docs/splayed-adapters.md` — 上层设计

Before changing any format, header, or function signature, read the relevant section; update the corresponding doc in the same change.

V1.0 归档于 git 分支（`f572500` 快照），`plan.md` 为 V1 规格——不要再以它为准。

## Workspace Layout

```
Cargo.toml                 # workspace root
crates/
  splayed-format/          # NO deps — 二进制格式定义（types / headers / meta / field / column 内存模型 / bitmap）
  splayed-codec/           # depends on format — 编码 + 压缩（PLAIN, DELTA, RLE, BITPACK + ZSTD, LZ4；chunk 布局）
  splayed-core/            # depends on format + codec — field_file / meta_file / dataset / scan（三层 API + Scanner）
  splayed-table/           # depends on core — 表层 API（Partition = Dataset 1:1、Hive 式分区、query/write/结构操作）
  splayed-arrow/           # depends on core — Arrow 类型映射与零拷贝转换（column_to_arrow / data_to_record_batch / scan_to_arrow）
  splayed-polars/          # depends on core + polars 0.45 — AnonymousScan 惰性扫描（分区发现 / vstack 合并）
# V1 遗留 crate 在 Cargo.toml exclude 中（splayed / adbc / datafusion / duckdb / cli / python），待模块对齐阶段回归
```

**Dependency invariant:** `splayed-core` must NEVER depend on Arrow. Arrow-related functions live in `splayed-arrow`; engine adapters depend on core directly.

## Build & Test

```bash
cargo build        # must succeed with zero warnings
cargo test         # all tests must pass
```

- **Zero warnings policy.** If a build produces warnings, fix them before moving on.
- **Tests are integration tests** in `crates/*/tests/*.rs`. They create temp dirs, write datasets, and read them back.
- On Windows, mmap'd files cannot be renamed/replaced while open. Structural operations（close 收尾 / cast / compress / decompress）在 rename 前 drop 掉源文件的全部句柄与 mmap。
- **MSVC PDB 抖动（LNK1318）**：`cargo test --workspace` 偶发链接失败（并发 link.exe 写 PDB 争用）。规避：`cargo test --workspace --config "profile.test.debug=false"`（测试无 debuginfo、不产生 PDB；`CARGO_BUILD_JOBS=1` 串行更稳）。

## Format Constants (do not change without updating docs/splayed-format.md)

| Constant | Value |
|---|---|
| META magic | `SPLAYMTA` (u64 LE) |
| FIELD magic | `SPLAYFLD` (u64 LE) |
| Header size | 64 bytes (both META and FIELD) |
| Data offset | 64 (immediately after header) |
| SYM INDEX record | 12 bytes: `time_start(4) + time_count(4) + row_start(4)` |
| META file name | `.meta` |
| Format version | 2 |

## Key Design Decisions

1. **Capacity grid（容量网格）：** `L = Σ_sym time_count(sym)`，`row_start = Σ 前序 time_count`；三层 API 的 offset/length 一一对应。
2. **NULL via validity bitmap:** LSB-first（bit i ↔ row i，`1 = 有效`）；全有效列不写 validity 区；内存中 `validity = None` 等价全有效。**没有 sentinel 位型**——NaN 是合法数据。
3. **连续子区间原则:** 每个 sym 的 time 必须严格等于 TIME AXIS 的一个连续子区间；SYM INDEX 只存 `row_start + time_start + time_count`，构建期校验 `time_count == 数据行数`（`NonContiguousTime` 拒绝）。
4. **Write paths:** uncompressed `write_field_handle` 原地覆盖（mmap）；compressed 走解压 working 表示，`close_field_handle` 发生修改才流式重压缩写回（tmp + sync_all + rename）。
5. **Compressed = chunked:** chunk 头自描述分组；compress/decompress chunk 流式（内存 O(单 chunk)），逻辑语义（data_type/row_count/null_count/generation）不变。
6. **Generation:** u64, strictly monotonic；写成功 +1 且随 header 落盘；结构操作（cast/compress/decompress）保持源值。
7. **Atomic commits:** 所有文件级替换（META、cast/compress/decompress/close 产物）= 写 tmp → `sync_all` → 原子 rename；失败清理 tmp，原文件保持不变。

## How to Add a New Feature

1. Find the relevant section in `docs/`（先设计后实现；接口变更先改文档）。
2. Implement in the correct crate（format → codec → core → table → arrow）。
3. Add unit tests in the module + integration tests in `crates/splayed-core/tests/`（或对应 crate 的 tests）。
4. Run `cargo build && cargo test` — zero warnings, all pass.
5. Update the corresponding `docs/` section（内部实现流程与原则随代码一起改）。

## How to Add a New DataType

1. Add variant to `DataType` enum in `splayed-format/src/types.rs`.
2. Implement `size_of`, `from_id`, `as_str`（V2 无 sentinel NULL，无 `null_bytes`）。
3. Update `splayed-arrow` type mapping（`from_arrow_type` / `to_arrow_type` / 数组转换）。
4. Add tests for roundtrip（create → write → read）与谓词求值路径（`eval_cmp_bytes` 类型化分支）。

## Testing Patterns

- Use `std::env::temp_dir()` with `std::process::id()` for unique temp dirs.
- Always `fs::remove_dir_all` at end of test (or use `let _ = fs::remove_dir_all` at start).
- For roundtrip tests: create via `create_field_file` / `create_table`, read via `FieldHandle::read_field_handle` or `scan_to_arrow`, assert values。
- For error tests: use `matches!(result, Err(ExpectedError::Variant))`.

## What NOT to Do

- Do not add Arrow dependency to `splayed-core`.
- Do not change header layouts without updating `docs/splayed-format.md`。
- Do not use `bytemuck::from_bytes` on `Vec<u8>`-backed slices — use `bytemuck::pod_read_unaligned` (alignment issue).
- Do not write validity bit-by-bit in hot paths — use the batched primitives（`BitmapView::copy_bits_into` / `Bitmap::copy_bits_from` / `set_range` / `bitmap_count_ones` / `bitmap_fill_bits`）。
- Do not materialize whole fields in structural operations（cast / compress / decompress / close）——逐批 / 逐 chunk 流式，tmp `sync_all` 后 rename。
- Do not keep file handles / mmaps open across `fs::rename` on Windows。

## Current Phase Status (V2.0)

- 设计文档（format / codec / core / table / arrow / adapters）：**Complete**（docs/）。
- format / codec / core / table / arrow / polars 实现 + 对齐审查：**Complete**。
- 性能优化批次：read/write 零拷贝与批量位操作、Scanner 批量管线（向量化谓词 + word 级命中区）、META 构建轴二分 + 连续子区间校验、close/cast/compress/decompress 流式化：**Complete**。
- 循环7c splayed-duckdb（IPC 桥）与 循环7d splayed-adbc（DataFusion provider）：**Pending**（V1 实现参考归档分支）。
- Benchmark 复测（首轮基线后的一批优化未计入）：**Pending**。
