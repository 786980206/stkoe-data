//! Splayed V1 DataFusion adapter — three-layer `TableProvider` integration.
//!
//! ```text
//! Layer 3  register          register_splayed_table / SplayedTableFactory /
//!                            SplayedTableFunction   (DataFusion binding)
//! Layer 2  table             SplayedTableProvider   (Hive-partitioned-table analogue)
//! Layer 1  dataset           SplayedDatasetProvider (one .meta folder = a "parquet file")
//! ```
//!
//! - **Layer 1** [`dataset::SplayedDatasetProvider`]: one dataset directory
//!   (`.meta` + FIELD files) — self-contained, directly queryable.
//! - **Layer 2** [`table::SplayedTableProvider`]: a table over many dataset
//!   directories (each a partition), with schema validation and time-based
//!   partition pruning.
//! - **Layer 3** [`register`]: programmatic registration, `CREATE EXTERNAL
//!   TABLE ... STORED AS SPLAYED` and the `read_splayed('dir')` table function.
//!
//! Pushdown support:
//! - Projection pushdown: only opens requested FIELD files.
//! - Predicate pushdown: SYM → `SymbolSelection`, TIME → `TimeRange`, and
//!   conjunctive value filters on FIELD columns are pushed to the Scanner.
//! - A filter is only marked `Exact` when the scan really applies it (see
//!   [`filter`]) — otherwise DataFusion re-applies it.
//!
//! Note: DataFusion re-exports its own Arrow crates as `datafusion::arrow::*`.
//! We use those re-exports to guarantee version compatibility.

mod convert;
pub mod dataset;
pub mod exec;
mod filter;
pub mod register;
pub mod table;

pub use dataset::SplayedDatasetProvider;
pub use register::{
    auto_provider, register_splayed_table, SplayedTableFactory, SplayedTableFunction,
};
pub use table::SplayedTableProvider;
