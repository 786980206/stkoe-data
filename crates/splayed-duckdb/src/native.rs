//! 原生 DataChunk 路由（**无 Arrow 依赖**）。
//!
//! 目的：让 DuckDB 扩展层（未来的 TableFunction / Copy 函数）**直接消费
//! `CoreBatch`**，绕过 Arrow/IPC，消除序列化与二次解析的性能损失。
//!
//! # CoreBatch → DuckDB DataChunk 零拷贝契约
//!
//! DuckDB 的 `Vector` 是 `data_ptr + type + NullMask`；`CoreBatch` 的列正是
//! 「定长小端连续缓冲 + validity 位图」，二者**布局同构**。扩展实现建议：
//!
//! ```text
//! CoreColumn (Primitive) ──► Vector(LogicalType, data_ptr)     数据零拷贝
//!                          （DuckDB 逻辑类型由 CoreType 映射）
//! CoreColumn.nulls ───────► NullMask（1 bit/值，n/8 字节小拷贝）
//! CoreColumn (Dictionary) ─► 字符串/字典 Vector（扩展展开，单次拷贝或复用字典）
//! TIME 列（已按类型产出） ─► Date32 / Timestamp[µs] Vector（零拷贝）
//! ```
//!
//! - 数值 / Float / Date32 / Date64 / Timestamp 列：`data_ptr` 直接用批次缓冲，
//!   无拷贝（DuckDB 对定长数值列的物理布局即连续小端数组）；
//! - NULL：把位图拷成 DuckDB `NullMask`（每 8 值 1 字节，极小）；
//! - SYM：字典列展开为字符串向量（或 DuckDB 字典向量）；
//! - 生命周期：DataChunk 存活期间，借用的 `CoreBatch` 必须保活（把批次放入
//!   扩展持有的 Arc/队列，待 chunk 释放后再回收）。
//!
//! 本模块提供批次的**原生出口**（`scan_to_chunks`），扩展层只需逐批消费；
//! 真正的 DuckDB 扩展骨架（C ABI / C++ `Vector(LogicalType, data_ptr)`）
//! 作为后续工程接入（见 plan.md §10.4）。

use std::sync::Arc;

use splayed_core::{CoreBatch, Dataset, ScanRequest, Scanner, scan_owned_parallel};

/// 原生扫描错误。
#[derive(Debug)]
pub enum NativeError {
    Scan(String),
}

impl std::fmt::Display for NativeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scan(s) => write!(f, "scan failed: {s}"),
        }
    }
}
impl std::error::Error for NativeError {}

/// 把一次扫描的结果以 `CoreBatch` 流逐批交给 `sink`（扩展层在此消费）。
///
/// - 与 Arrow 完全无关：`sink` 收到的是引擎无关的 [`CoreBatch`]（含
///   validity 位图与字典列），可直接按上面的契约构造 DuckDB `Vector`；
/// - 并行：`parallelism > 1` 时按 `(sym,time)` 保序的多线程流式产出；
/// - 返回产出的总行数。
pub fn scan_to_chunks<F>(
    dataset: Arc<Dataset>,
    request: &ScanRequest,
    parallelism: usize,
    mut sink: F,
) -> Result<usize, NativeError>
where
    F: FnMut(CoreBatch),
{
    let scanner = Scanner::new(&dataset);
    let plan = scanner
        .plan(request)
        .map_err(|e| NativeError::Scan(e.to_string()))?;
    let mut stream = scan_owned_parallel(dataset, &plan, request, parallelism)
        .map_err(|e| NativeError::Scan(e.to_string()))?;

    let mut total = 0usize;
    while let Some(batch) = stream
        .next_batch()
        .map_err(|e| NativeError::Scan(e.to_string()))?
    {
        total += batch.num_rows();
        sink(batch);
    }
    Ok(total)
}