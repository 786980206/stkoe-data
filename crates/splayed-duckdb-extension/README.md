# splayed-duckdb-extension（DuckDB 扩展壳，C++）

**纯壳工程**：只负责「加载 Rust 集成层（`splayed-duckdb`）并注册扩展入口」，
扫描/转换/错误处理等全部委托给 Rust 层（见 `crates/splayed-duckdb/src/ffi.rs`
的 C ABI）。不是 cargo workspace 成员（需要 DuckDB SDK，无法在本仓库环境编译
验证——`extension.cpp` 是按 duckdb-extension-template 惯例书写的骨架）。

## 构建

```bash
# 1) 构建 Rust 集成层（cdylib + staticlib + import lib）
cargo build -p splayed-duckdb --release --config "lib.crate-type=['cdylib','staticlib']"

# 产出（target/release 下）：
#   splayed_duckdb.dll / splayed_duckdb.lib（staticlib）/ splayed_duckdb.dll.lib（import lib）

# 2) 用 duckdb-extension-template 构建扩展（MSVC 示例）
cmake -S . -B build \
  -DDUCKDB_EXTENSION_NAMES="splayed_read" \  # 见下
  -DDUCKDB_EXTENSION_LINKED_NAMES="splayed_read" ...
  # 在 CMake 中 target_link_libraries(splayed_read PRIVATE <repo>/target/release/splayed_duckdb.dll.lib)
```

本文件夹的 `CMakeLists.txt` 为最小示例，实际请对照
`https://github.com/duckdb/extension-template` 的结构补齐（duckdb-src 子模块、
CMake 扩展宏等）。

## 运行

```sql
LOAD 'splayed_read';                -- 加载扩展（壳）
SELECT * FROM read_splayed('data/2024');
SELECT sym, avg(close) FROM read_splayed('data/2024')
  WHERE time >= DATE '2024-06-01' GROUP BY sym;
```

> 扫描当前为「全列 + 全时间」；谓词下推（时间范围/分区列）、写入、物化视图等
> 逐步在 Rust 层 C ABI 扩展，壳只跟进新入口。

## 文件

- `extension.cpp`：扩展入口（Load/Register）+ 表函数 bind/init/function，
  消费 `splayed_scan_next` 返回的列视图填充 `DataChunk`（定长列 memcpy，
  NULL 从 validity 位图写 `ValidityMask`，SYM 字典逐行取串）。
- `CMakeLists.txt`：最小构建骨架。