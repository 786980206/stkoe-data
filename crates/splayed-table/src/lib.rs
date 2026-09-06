//! splayed-table：V2.0 表组织层（Table / Partition / 结构操作 / query）。
//!
//! 权威设计见 `docs/splayed-table.md`。Partition = Dataset（1:1）；
//! 所有数据访问经 `splayed-core` API，不触碰 META / Field 物理细节。

pub mod partition;
pub mod query;
pub mod table;
pub mod write;

pub use partition::{days_from_civil, PartitionScheme};
pub use query::{
    query_table, read_table, scan_table, PartitionRowRange, TableReader, TableScanRequest,
    TableScanner,
};
pub use table::{
    close_table, create_table, create_table_partition, delete_table, delete_table_partition,
    open_table, rename_table, Capabilities, PartitionInfo, Partitioning, TableFieldInit,
    TableHandle, TableMetadata,
    TableOptions, TableStatistics,
};

pub use write::write_table;
