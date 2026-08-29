//! Polars 逻辑谓词 → 核心层扫描裁剪（尽力翻译）。
//!
//! 只翻译「列 op 字面量」的 AND 链；其余原样留给调用方（anon scan 会用
//! polars 物理评估兜底），核心推送**仅作为裁剪优化**，正确性由兜底保证。

use polars::prelude::{DataType as PDataType, Expr, LiteralValue, Operator, TimeUnit as PTimeUnit};

use std::collections::HashSet;

use splayed_core::{Filter, FilterValue, SymbolSelection, TimeRange};

/// 可下推到核心层的剪裁片段。
#[derive(Debug, Default)]
pub struct Hint {
    pub symbols: Option<SymbolSelection>,
    pub time_range: Option<TimeRange>,
    /// 数据字段的值过滤。
    pub filters: Vec<Filter>,
    /// 声明式分区列上的过滤（分区级剪裁）。
    pub partition_filters: Vec<Filter>,
}

/// `dtype_of` 按列名给出 polars 数据类型（来自扫描 schema）；
/// `partition_cols` 为声明式分区列名（其过滤路由到 `partition_filters`）。
pub fn translate(
    expr: Option<&Expr>,
    dtype_of: &dyn Fn(&str) -> Option<PDataType>,
    partition_cols: &HashSet<String>,
) -> Hint {
    let mut hint = Hint::default();
    if let Some(e) = expr {
        collect(e, dtype_of, partition_cols, &mut hint);
    }
    hint
}

fn collect(
    expr: &Expr,
    dtype_of: &dyn Fn(&str) -> Option<PDataType>,
    partition_cols: &HashSet<String>,
    hint: &mut Hint,
) {
    match expr {
        Expr::BinaryExpr { left, op, right } if *op == Operator::And => {
            collect(left, dtype_of, partition_cols, hint);
            collect(right, dtype_of, partition_cols, hint);
        }
        Expr::BinaryExpr { left, op, right } => {
            if let Some((col, lit)) = col_literal(left, right) {
                apply(&col, op, lit, dtype_of, partition_cols, hint);
            }
        }
        _ => {}
    }
}

fn col_literal<'a>(l: &'a Expr, r: &'a Expr) -> Option<(String, &'a LiteralValue)> {
    match (l, r) {
        (Expr::Column(name), Expr::Literal(lit)) => Some((name.to_string(), lit)),
        (Expr::Literal(lit), Expr::Column(name)) => Some((name.to_string(), lit)),
        _ => None,
    }
}

fn apply(
    col: &str,
    op: &Operator,
    lit: &LiteralValue,
    dtype_of: &dyn Fn(&str) -> Option<PDataType>,
    partition_cols: &HashSet<String>,
    hint: &mut Hint,
) {
    if partition_cols.contains(col) {
        // 声明式分区列：直接构造成 core Filter（Int64/String 字面量）。
        if let Some(fv) = partition_literal(lit) {
            if let Some(f) = filter_for(col, op, fv) {
                hint.partition_filters.push(f);
            }
        }
        return;
    }
    match col {
        "sym" if *op == Operator::Eq => {
            if let LiteralValue::String(s) = lit {
                hint.symbols = Some(SymbolSelection::syms([s.to_string()]));
            }
        }
        "time" => {
            if let Some(t) = time_or_datetime_lit(dtype_of("time").as_ref(), lit) {
                hint.time_range = merge_time(hint.time_range.as_ref(), op, t);
            }
        }
        field => {
            if let Some(dt) = dtype_of(field) {
                if let Some(fv) = filter_value(&dt, lit) {
                    if let Some(f) = filter_for(field, op, fv) {
                        hint.filters.push(f);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 字面量 → FilterValue / 时间
// ---------------------------------------------------------------------------

/// 分区列字面量：字符串 → String；整型/数值 → Int64。
fn partition_literal(lit: &LiteralValue) -> Option<FilterValue> {
    match lit {
        LiteralValue::String(s) => Some(FilterValue::String(s.to_string())),
        v => as_i64(v).map(FilterValue::Int64),
    }
}

fn filter_value(dt: &PDataType, lit: &LiteralValue) -> Option<FilterValue> {
    match dt {
        PDataType::Float64 => as_f64(lit).map(FilterValue::Float64),
        PDataType::Float32 => as_f64(lit).map(|v| FilterValue::Float32(v as f32)),
        PDataType::Int64 => as_i64(lit).map(FilterValue::Int64),
        PDataType::Int32 => as_i32(lit).map(FilterValue::Int32),
        PDataType::UInt64 => as_u64(lit).map(FilterValue::UInt64),
        PDataType::UInt32 => as_u64(lit).map(|v| FilterValue::UInt32(v as u32)),
        PDataType::Date => as_i32(lit).map(FilterValue::Date32),
        PDataType::Datetime(PTimeUnit::Microseconds | PTimeUnit::Milliseconds | PTimeUnit::Nanoseconds, _) => as_i64(lit).map(FilterValue::TimestampUs),
        _ => None,
    }
}

/// time 列：date(Int32) / datetime(Int64) 字面量 → i64（统一轴单位）。
fn time_or_datetime_lit(dt: Option<&PDataType>, lit: &LiteralValue) -> Option<i64> {
    match dt {
        Some(PDataType::Date) => as_i64(lit),
        _ => as_i64(lit),
    }
}

fn merge_time(prev: Option<&TimeRange>, op: &Operator, t: i64) -> Option<TimeRange> {
    let cur = time_range_of(op, t);
    match prev {
        Some(p) => Some(TimeRange::new(p.start.max(cur.start), p.end.min(cur.end))),
        None => Some(cur),
    }
}

fn time_range_of(op: &Operator, t: i64) -> TimeRange {
    match op {
        Operator::Eq => TimeRange::new(t, t.saturating_add(1)),
        Operator::Gt => TimeRange::new(t.saturating_add(1), i64::MAX),
        Operator::GtEq => TimeRange::new(t, i64::MAX),
        Operator::Lt => TimeRange::new(i64::MIN, t),
        Operator::LtEq => TimeRange::new(i64::MIN, t.saturating_add(1)),
        _ => TimeRange::all(),
    }
}

fn filter_for(field: &str, op: &Operator, fv: FilterValue) -> Option<Filter> {
    Some(match op {
        Operator::Gt => Filter::GreaterThan { field: field.to_string(), value: fv },
        Operator::GtEq => Filter::GreaterOrEqual { field: field.to_string(), value: fv },
        Operator::Lt => Filter::LessThan { field: field.to_string(), value: fv },
        Operator::LtEq => Filter::LessOrEqual { field: field.to_string(), value: fv },
        Operator::Eq => Filter::Equal { field: field.to_string(), value: fv },
        Operator::NotEq => Filter::NotEqual { field: field.to_string(), value: fv },
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// 数值提取（跨整型/浮点字面量宽容转换）
// ---------------------------------------------------------------------------

fn as_f64(lit: &LiteralValue) -> Option<f64> {
    Some(match lit {
        LiteralValue::Float64(v) => *v,
        LiteralValue::Float32(v) => *v as f64,
        LiteralValue::Int64(v) => *v as f64,
        LiteralValue::Int32(v) => *v as f64,
        LiteralValue::UInt64(v) => *v as f64,
        LiteralValue::UInt32(v) => *v as f64,
        _ => return None,
    })
}

fn as_i64(lit: &LiteralValue) -> Option<i64> {
    Some(match lit {
        LiteralValue::Int64(v) => *v,
        LiteralValue::Int32(v) => *v as i64,
        LiteralValue::UInt64(v) => *v as i64,
        LiteralValue::UInt32(v) => *v as i64,
        LiteralValue::Float64(v) => *v as i64,
        LiteralValue::Float32(v) => *v as i64,
        _ => return None,
    })
}

fn as_i32(lit: &LiteralValue) -> Option<i32> {
    as_i64(lit).and_then(|v| i32::try_from(v).ok())
}

fn as_u64(lit: &LiteralValue) -> Option<u64> {
    Some(match lit {
        LiteralValue::UInt64(v) => *v,
        LiteralValue::UInt32(v) => *v as u64,
        LiteralValue::Int64(v) => *v as u64,
        LiteralValue::Int32(v) => *v as u64,
        LiteralValue::Float64(v) => *v as u64,
        _ => return None,
    })
}