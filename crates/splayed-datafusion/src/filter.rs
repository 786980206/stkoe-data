//! Shared predicate analysis: DataFusion filter expressions → Splayed pushdown.
//!
//! The two entry points MUST stay consistent:
//! - [`classify`] (used by `TableProvider::supports_filters_pushdown`) decides
//!   whether DataFusion may skip re-applying a filter (`Exact`), must re-apply
//!   it (`Inexact`), or should keep it above the scan (`Unsupported`).
//! - [`parse_filters`] (used by `scan`) turns the pushed-down filters into a
//!   Splayed `ScanRequest` fragment.
//!
//! A filter is only `Exact` when the scan actually applies it. Anything the
//! scan cannot apply exactly is `Unsupported` (or `Inexact` for the special
//! multi-symbol conjunction case) — never `Exact`.

use std::collections::HashSet;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};

use splayed_arrow::arrow_to_splayed_type;
use splayed_core::{Filter as SplayedFilter, FilterValue, SymbolSelection, TimeRange};
use splayed_format::DataType as SplayedDataType;

/// A single pushdown fragment extracted from one filter expression.
pub(crate) enum Fragment {
    /// `sym = 'X'` → membership in the symbol selection.
    Symbols(Vec<String>),
    /// A `time` comparison → a half-open `[start, end)` range.
    Time(TimeRange),
    /// A value comparison on a FIELD column.
    Value(SplayedFilter),
}

/// Result of analyzing one filter expression.
pub(crate) enum Analysis {
    /// The scan will apply this filter exactly.
    Applied(Fragment),
    /// A `sym = <unknown>` filter: this dataset has no such symbol, so the scan
    /// contributes zero rows for it. Still exact — unless multiple distinct
    /// symbol literals appear in the same conjunction (see [`classify`]).
    SymUnknown,
    /// Not applicable to this provider.
    NotPushdownable,
}

/// The pushdown fragment extracted from a filter conjunction.
pub(crate) struct PushdownPlan {
    /// `Some` when at least one `sym` filter was seen. An *empty* selection
    /// means "a symbol filter referenced only symbols unknown to this dataset"
    /// → the scan must return zero rows (not everything).
    pub symbols: Option<SymbolSelection>,
    pub time_range: Option<TimeRange>,
    /// Conjunctive value filters on FIELD columns.
    pub value_filters: Vec<SplayedFilter>,
}

/// A literal `Column <op> Literal` (either operand order).
fn binary_col_literal(expr: &Expr) -> Option<(String, ScalarValue, bool, Operator)> {
    let Expr::BinaryExpr(bin) = expr else {
        return None;
    };
    match (bin.left.as_ref(), bin.right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) => {
            Some((c.name.clone(), v.clone(), false, bin.op.clone()))
        }
        (Expr::Literal(v, _), Expr::Column(c)) => {
            Some((c.name.clone(), v.clone(), true, bin.op.clone()))
        }
        _ => None,
    }
}

/// Analyze one filter expression against this dataset's schema and symbol set.
fn analyze(expr: &Expr, schema: &ArrowSchema, known_symbols: &HashSet<String>) -> Analysis {
    let Some((col, lit, swapped, op)) = binary_col_literal(expr) else {
        return Analysis::NotPushdownable;
    };
    let op = if swapped { invert_op(&op) } else { op };

    match col.as_str() {
        "sym" => {
            if op != Operator::Eq {
                return Analysis::NotPushdownable;
            }
            let Some(s) = scalar_to_string(&lit) else {
                return Analysis::NotPushdownable;
            };
            if known_symbols.contains(&s) {
                Analysis::Applied(Fragment::Symbols(vec![s]))
            } else {
                Analysis::SymUnknown
            }
        }
        "time" => {
            let Some(t) = extract_int_from_scalar(&lit) else {
                return Analysis::NotPushdownable;
            };
            let Some(r) = time_fragment(&op, t) else {
                return Analysis::NotPushdownable;
            };
            Analysis::Applied(Fragment::Time(r))
        }
        other => match build_value_filter(other, &op, &lit, schema) {
            Some(f) => Analysis::Applied(Fragment::Value(f)),
            None => Analysis::NotPushdownable,
        },
    }
}

/// Convert a `time` comparison into a half-open `[start, end)` range.
fn time_fragment(op: &Operator, t: i64) -> Option<TimeRange> {
    match op {
        Operator::Eq => Some(TimeRange::new(t, t.saturating_add(1))),
        Operator::Gt => Some(TimeRange::new(t.saturating_add(1), i64::MAX)),
        Operator::GtEq => Some(TimeRange::new(t, i64::MAX)),
        Operator::Lt => Some(TimeRange::new(i64::MIN, t)),
        Operator::LtEq => Some(TimeRange::new(i64::MIN, t.saturating_add(1))),
        _ => None,
    }
}

/// Classify each filter for `TableProvider::supports_filters_pushdown`.
///
/// `known_symbols` is the set of symbols this table/dataset knows about. For a
/// partitioned table this should be the union across all partitions.
pub(crate) fn classify(
    filters: &[&Expr],
    schema: &ArrowSchema,
    known_symbols: &HashSet<String>,
) -> Vec<TableProviderFilterPushDown> {
    // If one conjunction mentions more than one distinct symbol, the symbol
    // selection can only express a *union*, but the conjunction needs an
    // *intersection* — so DataFusion must re-apply those filters.
    let distinct_syms: HashSet<String> = filters
        .iter()
        .filter_map(|f| {
            let (col, lit, _, op) = binary_col_literal(f)?;
            if col == "sym" && op == Operator::Eq {
                scalar_to_string(&lit)
            } else {
                None
            }
        })
        .collect();
    let multi_sym = distinct_syms.len() > 1;

    filters
        .iter()
        .map(|f| match analyze(f, schema, known_symbols) {
            Analysis::Applied(Fragment::Symbols(_)) | Analysis::SymUnknown => {
                if multi_sym {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Exact
                }
            }
            Analysis::Applied(_) => TableProviderFilterPushDown::Exact,
            Analysis::NotPushdownable => TableProviderFilterPushDown::Unsupported,
        })
        .collect()
}

/// Turn the pushed-down filters into a Splayed pushdown plan (AND semantics).
pub(crate) fn parse_filters(
    filters: &[Expr],
    schema: &ArrowSchema,
    known_symbols: &HashSet<String>,
) -> PushdownPlan {
    let mut symbols: Vec<String> = Vec::new();
    let mut saw_sym_filter = false;
    let mut time_start = i64::MIN;
    let mut time_end = i64::MAX;
    let mut value_filters: Vec<SplayedFilter> = Vec::new();

    for expr in filters {
        match analyze(expr, schema, known_symbols) {
            Analysis::Applied(Fragment::Symbols(mut s)) => {
                saw_sym_filter = true;
                symbols.append(&mut s);
            }
            Analysis::Applied(Fragment::Time(r)) => {
                time_start = time_start.max(r.start);
                time_end = time_end.min(r.end);
            }
            Analysis::Applied(Fragment::Value(f)) => value_filters.push(f),
            // Unknown symbol: contributes zero rows (empty selection below).
            Analysis::SymUnknown => saw_sym_filter = true,
            Analysis::NotPushdownable => {}
        }
    }

    let symbols_sel = if saw_sym_filter {
        Some(SymbolSelection::syms(symbols))
    } else {
        None
    };

    let time_range = if time_start == i64::MIN && time_end == i64::MAX {
        None
    } else {
        Some(TimeRange::new(time_start, time_end))
    };

    PushdownPlan {
        symbols: symbols_sel,
        time_range,
        value_filters,
    }
}

// ---------------------------------------------------------------------------
// Scalar helpers
// ---------------------------------------------------------------------------

fn invert_op(op: &Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        other => other.clone(),
    }
}

fn scalar_to_string(scalar: &ScalarValue) -> Option<String> {
    match scalar {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

fn extract_int_from_scalar(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Date32(Some(v)) => Some(*v as i64),
        ScalarValue::Date64(Some(v)) => Some(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(*v),
        _ => None,
    }
}

fn build_value_filter(
    field: &str,
    op: &Operator,
    scalar: &ScalarValue,
    schema: &ArrowSchema,
) -> Option<SplayedFilter> {
    let arrow_field = schema.field_with_name(field).ok()?;
    let splayed_ty = arrow_to_splayed_type(arrow_field.data_type())?;
    let fv = scalar_to_filter_value(scalar, splayed_ty)?;

    Some(match op {
        Operator::Gt => SplayedFilter::GreaterThan {
            field: field.to_string(),
            value: fv,
        },
        Operator::GtEq => SplayedFilter::GreaterOrEqual {
            field: field.to_string(),
            value: fv,
        },
        Operator::Lt => SplayedFilter::LessThan {
            field: field.to_string(),
            value: fv,
        },
        Operator::LtEq => SplayedFilter::LessOrEqual {
            field: field.to_string(),
            value: fv,
        },
        Operator::Eq => SplayedFilter::Equal {
            field: field.to_string(),
            value: fv,
        },
        Operator::NotEq => SplayedFilter::NotEqual {
            field: field.to_string(),
            value: fv,
        },
        _ => return None,
    })
}

fn scalar_to_filter_value(scalar: &ScalarValue, splayed_ty: SplayedDataType) -> Option<FilterValue> {
    match (splayed_ty, scalar) {
        (SplayedDataType::Bool, ScalarValue::Boolean(Some(v))) => Some(FilterValue::Bool(*v)),
        (SplayedDataType::Int32, ScalarValue::Int32(Some(v))) => Some(FilterValue::Int32(*v)),
        (SplayedDataType::Int64, ScalarValue::Int64(Some(v))) => Some(FilterValue::Int64(*v)),
        (SplayedDataType::Float32, ScalarValue::Float32(Some(v))) => Some(FilterValue::Float32(*v)),
        (SplayedDataType::Float64, ScalarValue::Float64(Some(v))) => Some(FilterValue::Float64(*v)),
        (SplayedDataType::Date32, ScalarValue::Int32(Some(v))) => Some(FilterValue::Date32(*v)),
        (SplayedDataType::Date32, ScalarValue::Date32(Some(v))) => Some(FilterValue::Date32(*v)),
        (SplayedDataType::TimestampUs, ScalarValue::Int64(Some(v))) => {
            Some(FilterValue::TimestampUs(*v))
        }
        (SplayedDataType::TimestampUs, ScalarValue::TimestampMicrosecond(Some(v), _)) => {
            Some(FilterValue::TimestampUs(*v))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType as A, Field as F};
    use datafusion::logical_expr::{col, lit};

    fn schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            F::new("time", A::Date32, false),
            F::new("sym", A::Utf8, false),
            F::new("close", A::Float64, true),
            F::new("volume", A::Int64, true),
        ])
    }

    fn known() -> HashSet<String> {
        ["SYM01", "SYM02"].iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn classify_sym_exact() {
        let s = schema();
        let f = col("sym").eq(lit("SYM01"));
        let v = classify(&[&f], &s, &known());
        assert_eq!(v[0], TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn classify_unknown_sym_is_exact_zero_rows() {
        let s = schema();
        let f = col("sym").eq(lit("NOPE"));
        let v = classify(&[&f], &s, &known());
        assert_eq!(v[0], TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn classify_multi_sym_inexact() {
        let s = schema();
        let f1 = col("sym").eq(lit("SYM01"));
        let f2 = col("sym").eq(lit("SYM02"));
        let v = classify(&[&f1, &f2], &s, &known());
        assert_eq!(v[0], TableProviderFilterPushDown::Inexact);
        assert_eq!(v[1], TableProviderFilterPushDown::Inexact);
    }

    #[test]
    fn classify_unconvertible_time_unsupported() {
        let s = schema();
        // time compared against a Utf8 literal that can't become an int range.
        let f = col("time").gt(lit("not-a-date"));
        let v = classify(&[&f], &s, &known());
        assert_eq!(v[0], TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn parse_single_value_filter() {
        let s = schema();
        let f = col("close").gt(lit(100.0f64));
        let p = parse_filters(&[f], &s, &known());
        assert!(p.symbols.is_none());
        assert!(p.time_range.is_none());
        assert_eq!(p.value_filters.len(), 1);
    }

    #[test]
    fn parse_two_value_filters_both_kept() {
        let s = schema();
        let f1 = col("close").gt(lit(100.0f64));
        let f2 = col("volume").gt(lit(5000i64));
        let p = parse_filters(&[f1, f2], &s, &known());
        assert_eq!(p.value_filters.len(), 2);
    }

    #[test]
    fn parse_unknown_sym_yields_empty_selection() {
        let s = schema();
        let f = col("sym").eq(lit("NOPE"));
        let p = parse_filters(&[f], &s, &known());
        // Empty selection ⇒ zero rows, not "scan everything".
        match p.symbols {
            Some(SymbolSelection::Symbols(v)) => assert!(v.is_empty()),
            other => panic!("expected Some(Symbols([])), got {other:?}"),
        }
    }
}
