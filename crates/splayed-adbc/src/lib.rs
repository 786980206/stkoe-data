//! 上层 ADBC 驱动（`splayed-adbc`）。
//!
//! 面向**外部应用**的统一数据库连接接口：`Connection::open(dir)` → `Statement`
//! （携带 SQL）→ `execute()` 返回 **Arrow `RecordBatch` 流**。
//!
//! 架构位置（不与其他适配组件并列）：
//!
//! ```text
//! 外部应用
//!    │
//!    ▼
//! splayed-adbc ── ADBC 驱动（本 crate）
//!    │  内部调用（执行 SQL）
//!    ▼
//! splayed-datafusion ── DataFusion TableProvider + SQL 执行
//!    │
//!    ▼
//! splayed-core ── 引擎无关核心（扫描/写入/CoreBatch）
//!    ▲
//!    │ 共享转换工具（可选）
//! splayed-arrow ── CoreBatch → Arrow 零拷贝
//! ```
//!
//! SQL 的解析 / 优化 / 执行全部由 DataFusion 承担（本组件**不复实现 SQL 解析**）；
//! DuckDB 执行路径（DuckDB Extension）可作为平替后端后续加入。ADBC C ABI
//! (`adbc.h`) FFI 可在本 Connection/Statement 之上加薄层。

use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use splayed_datafusion::register_splayed_table;

/// 统一错误类型。
#[derive(Debug)]
pub enum AdbcError {
    /// 打开数据库 / 建连接失败。
    OpenDatabase(String),
    /// SQL 规划 / 执行失败。
    Execution(String),
    /// 结果流读取出错。
    ResultStream(String),
}

impl std::fmt::Display for AdbcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenDatabase(s) => write!(f, "open database failed: {s}"),
            Self::Execution(s) => write!(f, "statement execution failed: {s}"),
            Self::ResultStream(s) => write!(f, "result stream error: {s}"),
        }
    }
}
impl std::error::Error for AdbcError {}

/// ADBC 连接：打开一个 Splayed 表目录，内部持有 DataFusion `SessionContext`。
///
/// `dir` 可以是单个 dataset（含 `.meta`）或分区表目录（自动探测，与
/// DataFusion 适配层一致，见 [`splayed_datafusion::register_splayed_table`]）。
#[derive(Clone)]
pub struct Connection {
    ctx: SessionContext,
    runtime: Arc<tokio::runtime::Runtime>,
    dir: std::path::PathBuf,
}

impl Connection {
    /// 打开并注册 `dir` 为表 `splayed`（ADBC 连接语义）。
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, AdbcError> {
        let runtime = Arc::new(
            tokio::runtime::Runtime::new()
                .map_err(|e| AdbcError::OpenDatabase(format!("tokio runtime: {e}")))?,
        );
        let dir = dir.as_ref().to_path_buf();
        let ctx = runtime.block_on(async {
            let ctx = SessionContext::new();
            register_splayed_table(&ctx, "splayed", &dir)
                .map_err(|e| AdbcError::OpenDatabase(e.to_string()))?;
            Ok::<_, AdbcError>(ctx)
        })?;
        Ok(Self { ctx, runtime, dir })
    }

    /// 以单个 dataset 目录（`dir/.meta` 必须存在）打开。
    pub fn open_dataset(dir: impl AsRef<Path>) -> Result<Self, AdbcError> {
        Self::open(dir)
    }

    /// 重新加载磁盘状态（例如 `splayed_core::update_meta` 之后）：用新的
    /// provider 重新注册 `splayed` 表（旧注册先注销再注册）。调用前请确保
    /// 没有正在执行的语句/流。
    pub fn refresh(&self) -> Result<(), AdbcError> {
        let ctx = self.ctx.clone();
        let dir = self.dir.clone();
        self.runtime.block_on(async move {
            let _ = ctx.deregister_table("splayed"); // 忽略「不存在」错误
            register_splayed_table(&ctx, "splayed", dir)
                .map_err(|e| AdbcError::OpenDatabase(e.to_string()))?;
            Ok::<_, AdbcError>(())
        })
    }

    /// 创建一个携带 SQL 的语句。
    pub fn statement(&self, sql: impl Into<String>) -> Statement {
        Statement {
            conn: self.clone(),
            sql: sql.into(),
        }
    }

    /// 便捷：执行 SQL 并收集全部批次。
    pub fn execute(&self, sql: impl Into<String>) -> Result<Vec<RecordBatch>, AdbcError> {
        self.statement(sql).execute_all()
    }
}

/// ADBC 语句：一段 SQL 文本 + 指向连接的句柄。
#[derive(Clone)]
pub struct Statement {
    conn: Connection,
    sql: String,
}

impl Statement {
    /// 覆盖/设置 SQL 文本（builder 风格）。
    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.sql = sql.into();
        self
    }

    /// 执行查询，返回流式 Arrow `RecordBatch` 迭代器。
    ///
    /// 内部：DataFusion `SessionContext::sql` → DataFrame → 物理执行流，
    /// 每个批次在调用方线程上 `block_on` 取回（ADBC 是同步接口）。
    pub fn execute(&self) -> Result<ArrowRecordBatchStream, AdbcError> {
        let df = self
            .conn
            .runtime
            .block_on(self.conn.ctx.sql(&self.sql))
            .map_err(|e| AdbcError::Execution(e.to_string()))?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let stream = self
            .conn
            .runtime
            .block_on(df.execute_stream())
            .map_err(|e| AdbcError::Execution(e.to_string()))?;
        Ok(ArrowRecordBatchStream {
            runtime: Arc::clone(&self.conn.runtime),
            stream,
            schema,
        })
    }

    /// 执行并收集全部批次到内存。
    pub fn execute_all(&self) -> Result<Vec<RecordBatch>, AdbcError> {
        self.conn
            .runtime
            .block_on(async {
                let df = self.conn.ctx.sql(&self.sql).await?;
                df.collect().await
            })
            .map_err(|e| AdbcError::Execution(e.to_string()))
    }
}

/// 流式 Arrow 查询结果（同步 `Iterator`，每次 `next` 驱动 DataFusion 一步）。
pub struct ArrowRecordBatchStream {
    runtime: Arc<tokio::runtime::Runtime>,
    stream: datafusion::physical_plan::SendableRecordBatchStream,
    schema: arrow::datatypes::SchemaRef,
}

impl ArrowRecordBatchStream {
    pub fn schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.schema
    }
}

impl Iterator for ArrowRecordBatchStream {
    type Item = Result<RecordBatch, AdbcError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.runtime.block_on(self.stream.next()) {
            Some(Ok(rb)) => Some(Ok(rb)),
            Some(Err(e)) => Some(Err(AdbcError::ResultStream(e.to_string()))),
            None => None,
        }
    }
}