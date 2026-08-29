//! 聚合下推 optimizer rule。
//!
//! 无过滤条件的 `MIN(col)` / `MAX(col)` / `COUNT(*)` / `COUNT(col)` over
//! 单 dataset 时，列统计（header null_count + footer min/max，均
//! `Precision::Exact`）足以直接给出答案——把 `Aggregate` 重写为**单行常量
//! `Values` 计划**，整个扫描被跳过。
//!
//! 保守性：任何条件不满足（有 filter、分组、统计缺失、非上述聚合、带
//! distinct/filter/order_by、非本 provider）→ 不改写，原计划照常执行。
//!
//! 说明：`TableScan.source` 是 `Arc<dyn TableSource>`（非 `TableProvider`），
//! 识别本 provider 用 `TableSource: Any` 的 trait 上转型（需要 rustc ≥ 1.86，
//! 见 workspace rust-version）。

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::logical_expr::expr::{AggregateFunction, Expr};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::execution::SessionStateBuilder;

use crate::dataset::SplayedDatasetProvider;

/// 聚合下推规则：命中时把 Aggregate 换成单行常量计划。
#[derive(Debug, Default)]
pub struct SplayedStatsAggRule;

impl OptimizerRule for SplayedStatsAggRule {
    fn name(&self) -> &str {
        "splayed_stats_aggregation"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        match self.try_rewrite(&plan) {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(plan)),
        }
    }
}

impl SplayedStatsAggRule {
    /// 尝试改写；不匹配/统计缺失 → `None`（保持原计划）。
    fn try_rewrite(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let LogicalPlan::Aggregate(agg) = plan else {
            return None;
        };
        if !agg.group_expr.is_empty() {
            return None;
        }
        let LogicalPlan::TableScan(ts) = agg.input.as_ref() else {
            return None;
        };
        if !ts.filters.is_empty() {
            return None;
        }
        let provider = (&*ts.source as &dyn std::any::Any)
            .downcast_ref::<SplayedDatasetProvider>()?;
        let stats = provider.statistics()?;

        // 逐聚合：只认 MIN/MAX/COUNT（无 distinct/filter/order_by）。
        let mut scalars: Vec<ScalarValue> = Vec::with_capacity(agg.aggr_expr.len());
        for ae in &agg.aggr_expr {
            let s = aggregate_scalar(ae, provider, &stats)?;
            scalars.push(s);
        }

        // 单行常量计划 + 按聚合 schema 改名（别名保持输出列名一致）。
        let rows = vec![scalars
            .iter()
            .map(|s| Expr::Literal(s.clone(), None))
            .collect::<Vec<_>>()];
        let values = LogicalPlanBuilder::values(rows).ok()?;
        let projected = LogicalPlanBuilder::from(values)
            .project(
                scalars
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        Expr::Literal(s.clone(), None)
                            .alias(agg.schema.field(i).name().as_str())
                    })
                    .collect::<Vec<_>>(),
            )
            .ok()?;
        projected.build().ok()
    }
}

/// 注册：`SessionStateBuilder` 挂上本规则（用返回的 builder 构建
/// `SessionContext`）。
pub fn with_splayed_optimizer_rules(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.with_optimizer_rule(Arc::new(SplayedStatsAggRule))
}

// ---------------------------------------------------------------------------
// 聚合求值（基于 provider 统计）
// ---------------------------------------------------------------------------

/// 单聚合表达式 → 常量 ScalarValue（不匹配返回 None）。
#[allow(deprecated)] // 内部消化内建 COUNT(*) 的 Expr::Wildcard 表示
fn aggregate_scalar(
    expr: &Expr,
    provider: &SplayedDatasetProvider,
    stats: &datafusion::common::Statistics,
) -> Option<ScalarValue> {
    // 剥 Alias。
    let expr = match expr {
        Expr::Alias(a) => a.expr.as_ref(),
        e => e,
    };
    let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
        return None;
    };
    if params.distinct || params.filter.is_some() || !params.order_by.is_empty() {
        return None;
    }
    let name = func.name();
    let args = &params.args;
    let is_wildcard = |e: &Expr| matches!(e, Expr::Wildcard { .. });
    match name {
        "count" => {
            // COUNT(*) → num_rows；COUNT(col) → num_rows − null_count。
            let n = exact_num_rows(stats)?;
            match args.as_slice() {
                [] => Some(ScalarValue::Int64(Some(n as i64))),
                [e] if is_wildcard(e) => Some(ScalarValue::Int64(Some(n as i64))),
                [Expr::Column(c)] => {
                    let ci = provider.schema().index_of(c.name.as_str()).ok()?;
                    let null_count = exact_null_count(stats, ci)?;
                    Some(ScalarValue::Int64(Some(n.saturating_sub(null_count) as i64)))
                }
                _ => None,
            }
        }
        "min" | "max" => {
            let [Expr::Column(c)] = args.as_slice() else {
                return None;
            };
            let ci = provider.schema().index_of(c.name.as_str()).ok()?;
            let col = stats.column_statistics.get(ci)?;
            let is_min = name == "min";
            let v = if is_min {
                match &col.min_value {
                    Precision::Exact(v) => v,
                    _ => return None,
                }
            } else {
                match &col.max_value {
                    Precision::Exact(v) => v,
                    _ => return None,
                }
            };
            Some(v.clone())
        }
        _ => None,
    }
}

fn exact_num_rows(stats: &datafusion::common::Statistics) -> Option<usize> {
    match stats.num_rows {
        Precision::Exact(n) => Some(n),
        _ => None,
    }
}

fn exact_null_count(stats: &datafusion::common::Statistics, col_idx: usize) -> Option<usize> {
    match stats.column_statistics.get(col_idx)?.null_count {
        Precision::Exact(n) => Some(n),
        _ => None,
    }
}