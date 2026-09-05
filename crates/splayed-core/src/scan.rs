use std::sync::Arc;

use splayed_format::DataType;

use crate::error::CoreError;

/// 连续行段：`offset / length` 统一表示（docs/splayed-core.md §3.1）。
///
/// 在 META Index / Dataset 中是逻辑行，在 Field 中是物理行——容量网格下两者一一对应。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRange {
    pub offset: u64,
    pub length: u64,
}

impl RowRange {
    pub fn new(offset: u64, length: u64) -> Self {
        RowRange { offset, length }
    }

    pub fn end(&self) -> u64 {
        self.offset + self.length
    }

    pub fn intersect(&self, other: &RowRange) -> Option<RowRange> {
        let start = self.offset.max(other.offset);
        let end = self.end().min(other.end());
        (end > start).then(|| RowRange::new(start, end - start))
    }

    pub fn contains(&self, row: u64) -> bool {
        row >= self.offset && row < self.end()
    }
}

/// 排序并合并相邻 / 重叠 ranges。
pub fn merge_ranges(mut ranges: Vec<RowRange>) -> Vec<RowRange> {
    if ranges.len() <= 1 {
        return ranges;
    }
    ranges.sort_by_key(|r| (r.offset, r.end()));
    let mut out: Vec<RowRange> = Vec::with_capacity(ranges.len());
    for r in ranges {
        match out.last_mut() {
            Some(last) if r.offset <= last.end() => {
                last.length = last.length.max(r.end() - last.offset);
            }
            _ => out.push(r),
        }
    }
    out
}

/// 将 ranges 与 `[0, bound)` 求交并合并。
pub fn clamp_ranges(ranges: &[RowRange], bound: u64) -> Vec<RowRange> {
    let full = [RowRange::new(0, bound)];
    let source: &[RowRange] = if ranges.is_empty() { &full } else { ranges };
    let clipped: Vec<RowRange> = source
        .iter()
        .filter_map(|r| r.intersect(&RowRange::new(0, bound)))
        .collect();
    merge_ranges(clipped)
}

/// 扫描请求（docs/splayed-core.md §3.4）。
///
/// - `ranges`：候选行范围（空 = 整个空间）；Dataset 级为逻辑行，Field 级为物理行（网格下等价）；
/// - `projection`：预留字段——core 扫描路径（scan_index / scan_field / scan_dataset）
///   不消费它，参与谓词求值的字段由谓词自身决定；读取列集由 read_dataset 的 `columns` 表达；
/// - `limit`：最多产生的匹配行数，可提前结束；
/// - 不含 `batch_size`：Scanner 一次 `next()` 只产出一个连续 RowRange，batch 聚合是 Reader 层职责。
#[derive(Debug, Clone, Default)]
pub struct ScanRequest {
    pub ranges: Vec<RowRange>,
    pub projection: Vec<Arc<str>>,
    pub predicate: Option<Predicate>,
    pub limit: Option<u64>,
}

/// 比较算子。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    pub(crate) fn matches(self, ord: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::*;
        match self {
            CmpOp::Eq => ord == Equal,
            CmpOp::Ne => ord != Equal,
            CmpOp::Lt => ord == Less,
            CmpOp::Le => ord != Greater,
            CmpOp::Gt => ord == Greater,
            CmpOp::Ge => ord != Less,
        }
    }
}

/// 谓词字面量。
#[derive(Debug, Clone)]
pub enum Scalar {
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(Arc<str>),
}

/// core 定义的可组合条件表达式（docs/splayed-core.md §3.5）。
///
/// - Dataset / META 级：`Cmp` 的 `field` 携带字段名（`sym` / `time` 或普通字段）；
/// - Field 级：`field = None`，作用在本 Field 的值上。
/// - 值过滤在各 ColumnView 的 segment 内逐行求值（段内连续）；执行顺序由上层决定。
#[derive(Debug, Clone)]
pub enum Predicate {
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    Cmp {
        field: Option<Arc<str>>,
        op: CmpOp,
        value: Scalar,
    },
}

impl Predicate {
    pub fn cmp(field: impl Into<Arc<str>>, op: CmpOp, value: Scalar) -> Self {
        Predicate::Cmp { field: Some(field.into()), op, value }
    }

    pub fn value_cmp(op: CmpOp, value: Scalar) -> Self {
        Predicate::Cmp { field: None, op, value }
    }

    /// 收集引用的字段名（去重保序）。
    pub fn fields(&self, out: &mut Vec<Arc<str>>) {
        match self {
            Predicate::And(children) | Predicate::Or(children) => {
                for c in children {
                    c.fields(out);
                }
            }
            Predicate::Not(inner) => inner.fields(out),
            Predicate::Cmp { field, .. } => {
                if let Some(name) = field {
                    if !out.iter().any(|f| f == name) {
                        out.push(name.clone());
                    }
                }
            }
        }
    }

    /// 谓词引用的字段是否全部属于给定集合。
    pub fn references_only(&self, allowed: &[Arc<str>]) -> bool {
        let mut fields = Vec::new();
        self.fields(&mut fields);
        fields.iter().all(|f| allowed.contains(f))
    }
}

/// 按 DataType 将一行原始字节解释为标量（NULL 行由调用方先行跳过；Utf8 无定宽标量）。
pub(crate) fn read_row_scalar(data_type: DataType, bytes: &[u8]) -> Result<Scalar, CoreError> {
    macro_rules! le {
        ($t:ty, $b:expr) => {
            <$t>::from_le_bytes(($b).try_into().unwrap())
        };
    }
    Ok(match data_type {
        DataType::Bool => Scalar::Bool(bytes[0] != 0),
        DataType::Int8 => Scalar::Int(le!(i8, &bytes[..1]) as i64),
        DataType::Int16 => Scalar::Int(le!(i16, &bytes[..2]) as i64),
        DataType::Int32 | DataType::Date32 => Scalar::Int(le!(i32, &bytes[..4]) as i64),
        DataType::Int64 | DataType::TimestampUs | DataType::Date64 => Scalar::Int(le!(i64, bytes)),
        DataType::UInt8 => Scalar::UInt(le!(u8, &bytes[..1]) as u64),
        DataType::UInt16 => Scalar::UInt(le!(u16, &bytes[..2]) as u64),
        DataType::UInt32 => Scalar::UInt(le!(u32, &bytes[..4]) as u64),
        DataType::UInt64 => Scalar::UInt(le!(u64, bytes)),
        DataType::Float32 => Scalar::Float(le!(f32, &bytes[..4]) as f64),
        DataType::Float64 => Scalar::Float(le!(f64, bytes)),
        DataType::Utf8 => {
            return Err(CoreError::Invalid("Utf8 has no fixed-width scalar".into()))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_ranges_sorts_and_coalesces() {
        let merged = merge_ranges(vec![
            RowRange::new(100, 30),
            RowRange::new(50, 10),
            RowRange::new(130, 20),
            RowRange::new(200, 5),
        ]);
        assert_eq!(
            merged,
            vec![
                RowRange::new(50, 10),
                RowRange::new(100, 50),
                RowRange::new(200, 5),
            ]
        );
    }

    #[test]
    fn clamp_and_intersect() {
        // [0, 30) 边界：第一段被裁剪，第二段完全出界被过滤
        let clamped = clamp_ranges(&[RowRange::new(5, 40), RowRange::new(100, 5)], 30);
        assert_eq!(clamped, vec![RowRange::new(5, 25)]);
        // 空 ranges = 全表
        assert_eq!(clamp_ranges(&[], 30), vec![RowRange::new(0, 30)]);
    }
}
