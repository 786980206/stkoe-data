# splayed（Umbrella）

统一入口 crate，按 features 开关聚合各组件：

```rust
pub use splayed_codec as codec;      // 恒有
pub use splayed_core as core;        // 恒有
pub use splayed_format as format;    // 恒有

// features 开关：
//   "arrow"（默认开、可关）→ splayed::arrow
//   "datafusion"           → splayed::datafusion
//   "duckdb"               → splayed::duckdb
//   "polars"               → splayed::polars
//   "adbc"                 → splayed::adbc（隐式拉入 datafusion）
```

## 用法

```toml
[dependencies]
splayed = { path = "crates/splayed", features = ["arrow", "adbc", "duckdb", "polars"] }
```

```rust
use splayed::core::Scanner;
use splayed::format::DataType;
use splayed::adbc::Connection;   // 需 feature "adbc"
```

## 分层原则

- `splayed-core` 不依赖 Arrow，提供引擎无关的扫描/写入与 `CoreBatch`；
- `splayed-arrow` 是**可选共享转换工具**；
- 各引擎绑定（DataFusion / DuckDB / Polars）与上层 ADBC 驱动各自成 crate（umbrella features 开关）。
