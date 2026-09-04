# 构建与测试

## 基础命令

```bash
cargo build        # 必须零警告
cargo test         # 全部测试通过
```

- **零警告政策**：构建产生警告必须先修复再继续。
- **测试是集成测试**：位于 `crates/*/tests/*.rs`，创建临时目录、写数据集、读回断言。

## Windows 注意事项

1. **mmap 文件不可写**：若已打开 `FieldReader` 后又对同一路径 `compact_field` / `update_field`，会得到 OS error 1224——先 drop reader 再写。

2. **MSVC PDB 抖动（LNK1318）**：`cargo test --workspace` 偶发链接失败（并发 link.exe 写 PDB 争用）。规避：

   ```bash
   cargo test --workspace --config "profile.test.debug=false"
   # CARGO_BUILD_JOBS=1 串行更稳
   ```

3. **DuckDB 扩展链接库**：cdylib/staticlib 不固化在 Cargo.toml（避免 MSVC link 输出被当 warning），按需生成：

   ```bash
   cargo build -p splayed-duckdb --release --config "lib.crate-type=['cdylib','staticlib']"
   # PowerShell 用单引号字符串
   ```

## 工具链

- Rust 1.86+（workspace `rust-version`）。
- Arrow 59 / DataFusion 55（workspace 固定；DataFusion 中请用 `datafusion::arrow::*` 重导出避免版本不匹配）。
- Polars 固定 `=0.45.1`（`polars-arrow` 自研，与 arrow-rs 不冲突）。
