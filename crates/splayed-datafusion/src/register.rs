//! Layer 3 — DataFusion binding for Splayed tables.
//!
//! Three ways to reach a Splayed table from SQL/DataFusion:
//! - [`register_splayed_table`] — programmatic registration (auto-detects).
//! - [`SplayedTableFactory`] — `CREATE EXTERNAL TABLE ... STORED AS SPLAYED LOCATION 'dir'`.
//! - [`SplayedTableFunction`] — `read_splayed('dir')` table function.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{
    Session, TableFunctionArgs, TableFunctionImpl, TableProvider, TableProviderFactory,
};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{CreateExternalTable, Expr};

use splayed_format::META_FILE_NAME;

use crate::dataset::SplayedDatasetProvider;
use crate::table::SplayedTableProvider;

/// Build the right provider for `dir`:
/// - `dir` directly contains `.meta` → a single-dataset provider (one partition);
/// - otherwise → a partitioned table over `dir`'s children that contain `.meta`.
pub fn auto_provider(dir: impl Into<PathBuf>) -> DFResult<Arc<dyn TableProvider>> {
    let dir = dir.into();
    if dir.join(META_FILE_NAME).exists() {
        Ok(Arc::new(SplayedDatasetProvider::new(dir)?))
    } else {
        Ok(Arc::new(SplayedTableProvider::new(dir)?))
    }
}

/// Register `dir` as a table named `name` (auto-detecting the layout).
pub fn register_splayed_table(
    ctx: &SessionContext,
    name: &str,
    dir: impl Into<PathBuf>,
) -> DFResult<Arc<dyn TableProvider>> {
    let provider = auto_provider(dir)?;
    ctx.register_table(name, Arc::clone(&provider))?;
    Ok(provider)
}

/// `CREATE EXTERNAL TABLE t ... STORED AS SPLAYED LOCATION '/dir'`
///
/// Register with `SessionStateBuilder::with_table_factory("SPLAYED", ...)`.
#[derive(Debug, Default)]
pub struct SplayedTableFactory;

#[async_trait]
impl TableProviderFactory for SplayedTableFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DFResult<Arc<dyn TableProvider>> {
        if !cmd.file_type.eq_ignore_ascii_case("SPLAYED") {
            return Err(DataFusionError::Execution(format!(
                "SplayedTableFactory: expected STORED AS SPLAYED, got {}",
                cmd.file_type
            )));
        }
        let loc = cmd.locations.first().ok_or_else(|| {
            DataFusionError::Execution("SplayedTableFactory: missing LOCATION".to_string())
        })?;
        auto_provider(PathBuf::from(loc))
    }
}

/// `read_splayed('/path/to/dir')` table function.
///
/// Register with `ctx.register_udtf("read_splayed", Arc::new(SplayedTableFunction))`.
#[derive(Debug, Default)]
pub struct SplayedTableFunction;

impl TableFunctionImpl for SplayedTableFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DFResult<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(path)), _)) = args.exprs().first() else {
            return Err(DataFusionError::Execution(
                "read_splayed requires a single string argument: read_splayed('/path/to/dir')"
                    .to_string(),
            ));
        };
        auto_provider(PathBuf::from(path))
    }
}
