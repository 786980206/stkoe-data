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

/// The name of the column behind an expression, stripping *identity* casts
/// (cast target == the column's declared type). Non-identity casts return
/// `None` so they are never pushed down.
fn column_ref<'a>(expr: &'a Expr, schema: &ArrowSchema) -> Option<&'a str> {
    match expr {
        Expr::Column(c) => Some(&c.name),
        Expr::Cast(cast) => {
            if let Expr::Column(c) = cast.expr.as_ref() {
                let is_identity = schema
                    .field_with_name(&c.name)
                    .map(|f| f.data_type() == cast.field.data_type())
                    .unwrap_or(false);
                is_identity.then_some(c.name.as_str())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// A column reference (`Ok`) or a literal (`Err`), cast-stripped.
fn col_or_literal(expr: &Expr, schema: &ArrowSchema) -> Option<Result<String, ScalarValue>> {
    if let Some(c) = column_ref(expr, schema) {
        return Some(Ok(c.to_string()));
    }
    match expr {
        Expr::Literal(v, _) => Some(Err(v.clone())),
        _ => None,
    }
}

/// A literal `Column <op> Literal` (either operand order), with identity casts
/// stripped from the column side.
fn binary_col_literal(
    expr: &Expr,
    schema: &ArrowSchema,
) -> Option<(String, ScalarValue, bool, Operator)> {
    let Expr::BinaryExpr(bin) = expr else {
        return None;
    };
    let l = col_or_literal(bin.left.as_ref(), schema)?;
    let r = col_or_literal(bin.right.as_ref(), schema)?;
    match (l, r) {
        (Ok(col), Err(v)) => Some((col, v, false, bin.op)),
        (Err(v), Ok(col)) => Some((col, v, true, bin.op)),
        _ => None,
    }
}

/// The symbol literals referenced by a sym filter expression: `sym = 'X'` or a
/// non-negated `sym IN ('A', ...)`. Returns `None` for anything else.
fn sym_literals(expr: &Expr, schema: &ArrowSchema) -> Option<Vec<String>> {
    if let Some((col, lit, _, op)) = binary_col_literal(expr, schema) {
        if col != "sym" || op != Operator::Eq {
            return None;
        }
        return scalar_to_string(&lit).map(|s| vec![s]);
    }
    if let Expr::InList(in_list) = expr {
        if in_list.negated || column_ref(in_list.expr.as_ref(), schema)? != "sym" {
            return None;
        }
        let mut out = Vec::with_capacity(in_list.list.len());
        for e in &in_list.list {
            let Expr::Literal(v, _) = e else { return None; };
            out.push(scalar_to_string(v)?);
        }
        Some(out)
    } else {
        None
    }
}

/// If `expr` is `IS NULL` / `IS NOT NULL` on a FIELD column, return the field
/// name and whether it tests for NULL. `time`/`sym` are non-nullable and are
/// left to DataFusion.
fn null_check_field(expr: &Expr, schema: &ArrowSchema) -> Option<(String, bool)> {
    let (inner, is_null) = match expr {
        Expr::IsNull(inner) => (inner, true),
        Expr::IsNotNull(inner) => (inner, false),
        _ => return None,
    };
    let col = column_ref(inner, schema)?;
    if col == "time" || col == "sym" || schema.field_with_name(col).is_err() {
        return None;
    }
    Some((col.to_string(), is_null))
}

/// Analyze one filter expression against this dataset's schema and symbol set.
fn analyze(expr: &Expr, schema: &ArrowSchema, known_symbols: &HashSet<String>) -> Analysis {
    // sym IN (...): a single union → selection (drop literals unknown to this
    // dataset; if none remain, the scan contributes zero rows).
    if let Some(literals) = sym_literals(expr, schema) {
        let known: Vec<String> = literals
            .into_iter()
            .filter(|s| known_symbols.contains(s))
            .collect();
        return if known.is_empty() {
            Analysis::SymUnknown
        } else {
            Analysis::Applied(Fragment::Symbols(known))
        };
    }

    // field IS [NOT] NULL → NULL-sentinel value filter.
    if let Some((field, is_null)) = null_check_field(expr, schema) {
        let f = if is_null {
            SplayedFilter::IsNull { field }
        } else {
            SplayedFilter::IsNotNull { field }
        };
        return Analysis::Applied(Fragment::Value(f));
    }

    let Some((col, lit, swapped, op)) = binary_col_literal(expr, schema) else {
        return Analysis::NotPushdownable;
    };
    let op = if swapped { invert_op(&op) } else { op };

    match col.as_str() {
        "sym" => {
            // Non-Eq sym comparisons (`!=`, `<`, ...) are not pushable.
            Analysis::NotPushdownable
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
    // If a conjunction carries more than one sym filter, the symbol selection
    // can only express one *union*, but the conjunction needs an *intersection*
    // of per-filter unions — so DataFusion must re-apply those filters. A
    // single `sym IN ('A','B')` is still one union and stays Exact.
    let sym_filter_count = filters
        .iter()
        .filter(|f| sym_literals(f, schema).is_some())
        .count();
    let multi_sym = sym_filter_count > 1;

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
        other => *other,
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
        (SplayedDataType::Int8, ScalarValue::Int8(Some(v))) => Some(FilterValue::Int8(*v)),
        (SplayedDataType::Int16, ScalarValue::Int16(Some(v))) => Some(FilterValue::Int16(*v)),
        (SplayedDataType::Int32, ScalarValue::Int32(Some(v))) => Some(FilterValue::Int32(*v)),
        (SplayedDataType::Int64, ScalarValue::Int64(Some(v))) => Some(FilterValue::Int64(*v)),
        (SplayedDataType::UInt8, ScalarValue::UInt8(Some(v))) => Some(FilterValue::UInt8(*v)),
        (SplayedDataType::UInt16, ScalarValue::UInt16(Some(v))) => Some(FilterValue::UInt16(*v)),
        (SplayedDataType::UInt32, ScalarValue::UInt32(Some(v))) => Some(FilterValue::UInt32(*v)),
        (SplayedDataType::UInt64, ScalarValue::UInt64(Some(v))) => Some(FilterValue::UInt64(*v)),
        (SplayedDataType::Float32, ScalarValue::Float32(Some(v))) => Some(FilterValue::Float32(*v)),
        (SplayedDataType::Float64, ScalarValue::Float64(Some(v))) => Some(FilterValue::Float64(*v)),
        (SplayedDataType::Date32, ScalarValue::Int32(Some(v))) => Some(FilterValue::Date32(*v)),
        (SplayedDataType::Date32, ScalarValue::Date32(Some(v))) => Some(FilterValue::Date32(*v)),
        (SplayedDataType::Date64, ScalarValue::Int64(Some(v))) => Some(FilterValue::Date64(*v)),
        (SplayedDataType::Date64, ScalarValue::Date64(Some(v))) => Some(FilterValue::Date64(*v)),
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

    // --- sym IN (...) pushdown ---

    #[test]
    fn classify_in_single_filter_exact() {
        let s = schema();
        // A single IN is one union — must stay Exact (not Inexact).
        let f = col("sym").in_list(vec![lit("SYM01"), lit("SYM02")], false);
        let v = classify(&[&f], &s, &known());
        assert_eq!(v[0], TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn classify_in_with_unknown_literal_exact() {
        let s = schema();
        let f = col("sym").in_list(vec![lit("SYM01"), lit("NOPE")], false);
        let v = classify(&[&f], &s, &known());
        assert_eq!(v[0], TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn classify_in_all_unknown_exact() {
        let s = schema();
        let f = col("sym").in_list(vec![lit("NOPE1"), lit("NOPE2")], false);
        let v = classify(&[&f], &s, &known());
        assert_eq!(v[0], TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn classify_in_plus_eq_inexact() {
        let s = schema();
        // Two sym filters in one conjunction → intersection needed → Inexact.
        let f1 = col("sym").in_list(vec![lit("SYM01"), lit("SYM02")], false);
        let f2 = col("sym").eq(lit("SYM01"));
        let v = classify(&[&f1, &f2], &s, &known());
        assert_eq!(v[0], TableProviderFilterPushDown::Inexact);
        assert_eq!(v[1], TableProviderFilterPushDown::Inexact);
    }

    #[test]
    fn classify_negated_in_unsupported() {
        let s = schema();
        let f = col("sym").in_list(vec![lit("SYM01")], true); // NOT IN
        let v = classify(&[&f], &s, &known());
        assert_eq!(v[0], TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn parse_in_known_symbols() {
        let s = schema();
        let f = col("sym").in_list(vec![lit("SYM02"), lit("SYM01")], false);
        let p = parse_filters(&[f], &s, &known());
        match p.symbols {
            Some(SymbolSelection::Symbols(v)) => {
                // Both literals known → both selected (union).
                assert_eq!(v.len(), 2);
            }
            other => panic!("expected Some(Symbols(..)), got {other:?}"),
        }
    }

    #[test]
    fn parse_in_mixed_unknown_drops_unknown() {
        let s = schema();
        let f = col("sym").in_list(vec![lit("SYM01"), lit("NOPE")], false);
        let p = parse_filters(&[f], &s, &known());
        match p.symbols {
            Some(SymbolSelection::Symbols(v)) => {
                assert_eq!(v, vec!["SYM01"]);
            }
            other => panic!("expected Some(Symbols(..)), got {other:?}"),
        }
    }

    // --- field IS [NOT] NULL ---

    #[test]
    fn parse_is_null() {
        let s = schema();
        let f = col("close").is_null();
        let p = parse_filters(&[f], &s, &known());
        assert_eq!(p.value_filters.len(), 1);
        assert!(matches!(
            p.value_filters[0],
            SplayedFilter::IsNull { .. }
        ));
    }

    #[test]
    fn parse_is_not_null() {
        let s = schema();
        let f = col("close").is_not_null();
        let p = parse_filters(&[f], &s, &known());
        assert_eq!(p.value_filters.len(), 1);
        assert!(matches!(
            p.value_filters[0],
            SplayedFilter::IsNotNull { .. }
        ));
    }

    #[test]
    fn is_null_on_sym_not_pushable() {
        let s = schema();
        let f = col("sym").is_null();
        assert!(matches!(analyze(&f, &s, &known()), Analysis::NotPushdownable));
        let p = parse_filters(&[f], &s, &known());
        assert!(p.value_filters.is_empty());
    }

    // --- identity cast stripping ---

    #[test]
    fn identity_cast_value_filter_pushable() {
        let s = schema();
        // CAST(close AS DOUBLE) > 100 — close is already Float64 (identity).
        let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(col("close")),
            A::Float64,
        ));
        let f = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(cast),
            Operator::Gt,
            Box::new(lit(100.0f64)),
        ));
        let p = parse_filters(&[f], &s, &known());
        assert_eq!(p.value_filters.len(), 1);
        assert!(matches!(
            p.value_filters[0],
            SplayedFilter::GreaterThan { .. }
        ));
    }

    #[test]
    fn non_identity_cast_not_pushable() {
        let s = schema();
        // CAST(close AS INT) > 100 — narrows Float64, must not be pushed.
        let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(col("close")),
            A::Int32,
        ));
        let f = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(cast),
            Operator::Gt,
            Box::new(lit(100i32)),
        ));
        assert!(matches!(analyze(&f, &s, &known()), Analysis::NotPushdownable));
    }

    /// Extended fixed-width types (Phase 2) map to FilterValue correctly.
    #[test]
    fn extended_types_value_filters_pushable() {
        let s = ArrowSchema::new(vec![
            F::new("time", A::Date32, false),
            F::new("sym", A::Utf8, false),
            F::new("i16", A::Int16, true),
            F::new("u32", A::UInt32, true),
            F::new("count", A::UInt64, false),
            F::new("d64", A::Date64, true),
        ]);
        let k: HashSet<String> = ["SYM01"].into_iter().map(|s| s.to_string()).collect();

        let p = parse_filters(&[col("i16").gt(lit(7i16))], &s, &k);
        assert!(matches!(
            p.value_filters.first(),
            Some(SplayedFilter::GreaterThan {
                value: splayed_core::FilterValue::Int16(7),
                ..
            })
        ));

        let p = parse_filters(&[col("u32").gt(lit(20u32))], &s, &k);
        assert!(matches!(
            p.value_filters.first(),
            Some(SplayedFilter::GreaterThan {
                value: splayed_core::FilterValue::UInt32(20),
                ..
            })
        ));

        let p = parse_filters(&[col("count").lt(lit(1000u64))], &s, &k);
        assert!(matches!(
            p.value_filters.first(),
            Some(SplayedFilter::LessThan {
                value: splayed_core::FilterValue::UInt64(1000),
                ..
            })
        ));

        let p = parse_filters(&[col("d64").gt(lit(86400000i64))], &s, &k);
        assert!(matches!(
            p.value_filters.first(),
            Some(SplayedFilter::GreaterThan {
                value: splayed_core::FilterValue::Date64(86400000),
                ..
            })
        ));

        // Classification marks them Exact (scan applies them).
        let f = col("u32").gt(lit(20u32));
        let v = classify(&[&f], &s, &k);
        assert_eq!(v[0], TableProviderFilterPushDown::Exact);
    }
}
