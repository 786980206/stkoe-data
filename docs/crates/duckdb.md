# splayed-duckdb（DuckDB 集成层）

**依赖**：core（+ 可选用 arrow，feature `"arrow"`）。

两条路径，按需开启：

```text
Path 1（feature "arrow"，默认开）：外部文件交换
  Splayed -> CoreBatch -> Arrow(零拷贝) -> IPC 文件 -> DuckDB read_arrow
  （代价 = IPC 序列化 + DuckDB 重解析一段拷贝）

Path 2（原生 DataChunk，零 Arrow）：
  Splayed -> CoreBatch -> scan_to_chunks() / C ABI -> DuckDB Extension 逐批消费
```

## 接口

| 接口 | 说明 |
|---|---|
| `export_to_arrow_ipc(dir, out.arrow)`（feature "arrow"） | Splayed → Arrow IPC 文件（DuckDB `read_arrow`） |
| `build_arrow_schema(dataset)` | 导出 schema（time/sym/fields） |
| `native::scan_to_chunks(dataset, req, n, sink)` | **零 Arrow**：CoreBatch 流直出（扩展层逐批消费，数据/validity 与 DuckDB `Vector` 布局同构） |
| `native::scan_table_to_chunks(dir, preq, n, sink)` | **零 Arrow 分区表**：走 `core::partition`（与 DataFusion 共用剪裁/合并） |
| `ffi`（C ABI，`#[no_mangle]`） | 见下方 C ABI 表 |

## C ABI（`ffi.rs`）

| 函数 | 说明 |
|---|---|
| `splayed_dataset_open(dir, dir_len) -> *mut c_void` / `splayed_dataset_close` | 打开/关闭 dataset 句柄 |
| `splayed_dataset_schema_count` / `splayed_dataset_schema_field` | schema 查询 |
| `splayed_scan_open / next / dict_value / close` | 扫描迭代：列视图 + SYM 字典取串 |
| `splayed_last_error() -> *const c_char` | 线程本地错误消息 |

**数据合同**：

- 句柄 = 裸指针（`Box::into_raw`，谁创建谁释放）；错误 = 返回码 + `splayed_last_error()`（线程本地）。
- 列视图：`SplayedColumnFFI{kind, type_id, data, data_len, validity, dict_count}`；`SPLAYED_TYPE_STRING_DICT = 200`（SYM 字典列）。
- 列布局：`[0]=time`（原生 DATE/TIMESTAMP）、`[1]=sym`（字典列）、`[2..]=FIELD`（splayed `DataType` id 0..=13）。
- 定长列缓冲与 DuckDB `Vector` 布局同构——C API 路径 memcpy 进向量缓冲；扩展工程若改用 C++ `Vector(LogicalType, data_ptr)` 构造可零拷贝。

## 构建产物

```bash
cargo build -p splayed-duckdb --release --config "lib.crate-type=['cdylib','staticlib']"
```

（PowerShell 用单引号字符串）→ `target/release/{splayed_duckdb.dll, splayed_duckdb.lib, splayed_duckdb.dll.lib}`。引擎内扩展由 `splayed-duckdb-extension`（C++ 壳骨架）消费。

使用指南见 [DuckDB 集成](../integrations/duckdb.md)。
