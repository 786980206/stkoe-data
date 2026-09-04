use crate::bitmap::{Bitmap, BitmapView};
use crate::buffer::BufferView;
use crate::error::FormatError;
use crate::types::DataType;
use crate::validity_size;

/// 单列段：一段连续行区间。
///
/// 段内连续是硬约束（SIMD 逐段求值的前提）；`validity = None` 表示该段全部有效。
#[derive(Debug, Clone, Copy)]
pub struct ColumnSegment<'a> {
    values: BufferView<'a>,
    validity: Option<BitmapView<'a>>,
    rows: usize,
}

impl<'a> ColumnSegment<'a> {
    pub fn new(
        data_type: DataType,
        values: BufferView<'a>,
        validity: Option<BitmapView<'a>>,
        rows: usize,
    ) -> Result<Self, FormatError> {
        let expected = rows * data_type.size_of();
        if values.len() != expected {
            return Err(FormatError::SizeMismatch { expected, found: values.len() });
        }
        Ok(ColumnSegment { values, validity, rows })
    }

    pub fn values(&self) -> BufferView<'a> {
        self.values
    }

    pub fn validity(&self) -> Option<BitmapView<'a>> {
        self.validity
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    pub fn null_count(&self) -> usize {
        self.validity.map(|v| v.null_count()).unwrap_or(0)
    }
}

/// 单列视图：一个或多个 segment 按逻辑行序组成。
///
/// 不变式（docs/splayed-format.md §3）：
/// - `segments` 非空、按逻辑行序排列；
/// - `Σ segments.rows == length`；
/// - `PLAIN + NONE` 路径恒为单段；多段仅出现在 compressed 跨 chunk 读取与跨 Partition 聚合。
#[derive(Debug, Clone)]
pub struct ColumnView<'a> {
    data_type: DataType,
    segments: Vec<ColumnSegment<'a>>,
    length: usize,
}

impl<'a> ColumnView<'a> {
    pub fn new(data_type: DataType, segments: Vec<ColumnSegment<'a>>) -> Result<Self, FormatError> {
        if segments.is_empty() {
            return Err(FormatError::InvalidLayout(
                "ColumnView requires at least one segment".into(),
            ));
        }
        let length = segments.iter().map(|s| s.rows()).sum();
        Ok(ColumnView { data_type, segments, length })
    }

    pub fn from_one(
        data_type: DataType,
        values: BufferView<'a>,
        validity: Option<BitmapView<'a>>,
        rows: usize,
    ) -> Result<Self, FormatError> {
        let segment = ColumnSegment::new(data_type, values, validity, rows)?;
        ColumnView::new(data_type, vec![segment])
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn segments(&self) -> &[ColumnSegment<'a>] {
        &self.segments
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn null_count(&self) -> usize {
        self.segments.iter().map(|s| s.null_count()).sum()
    }
}

/// 拥有型单列（[`Data`] 的列）。物化即连续单 Buffer。
#[derive(Debug, Clone)]
pub struct Column {
    pub data_type: DataType,
    pub values: crate::buffer::Buffer,
    pub validity: Option<Bitmap>,
}

impl Column {
    /// 创建全 NULL（validity 全 0）或全有效（validity = None）的占位列。
    pub fn zeroed(data_type: DataType, rows: usize, all_null: bool) -> Self {
        let values = crate::buffer::Buffer::zeroed_aligned(rows * data_type.size_of(), 8);
        let validity = if all_null { Some(Bitmap::zeros(rows)) } else { None };
        Column { data_type, values, validity }
    }

    pub fn length(&self) -> usize {
        self.values.len() / self.data_type.size_of()
    }

    pub fn null_count(&self) -> usize {
        self.validity.as_ref().map(|v| v.as_view().null_count()).unwrap_or(0)
    }

    pub fn as_view(&self) -> ColumnView<'_> {
        let values = BufferView::from_buffer(&self.values);
        let validity = self.validity.as_ref().map(|v| v.as_view());
        let segment =
            ColumnSegment::new(self.data_type, values, validity, self.length()).expect(
                "owned column is consistent by construction",
            );
        ColumnView::new(self.data_type, vec![segment])
            .expect("single-segment ColumnView is valid by construction")
    }
}

/// 保证 validity 区大小约定不被静默破坏（编译期锚点）。
const _: () = assert!(validity_size(9) == 2);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::Buffer;

    #[test]
    fn segment_checks_value_length() {
        let values = Buffer::from_vec(vec![0u8; 10]);
        let view = BufferView::from_buffer(&values);
        // Int32 需要每行 4 字节：10 字节不对应整数行
        assert!(matches!(
            ColumnSegment::new(DataType::Int32, view, None, 2),
            Err(FormatError::SizeMismatch { expected: 8, found: 10 })
        ));
        assert!(ColumnSegment::new(DataType::Int32, view, None, 2).is_err());
        assert!(ColumnSegment::new(DataType::Int8, view, None, 10).is_ok());
    }

    #[test]
    fn column_view_sums_segments() {
        let a = Buffer::from_vec(vec![1u8, 2, 3, 4]);
        let b = Buffer::from_vec(vec![5u8, 6, 7, 8]);
        let s1 = ColumnSegment::new(DataType::UInt16, BufferView::from_buffer(&a), None, 2).unwrap();
        let s2 = ColumnSegment::new(DataType::UInt16, BufferView::from_buffer(&b), None, 2).unwrap();
        let view = ColumnView::new(DataType::UInt16, vec![s1, s2]).unwrap();
        assert_eq!(view.length(), 4);
        assert_eq!(view.segments().len(), 2);
        assert!(ColumnView::new(DataType::UInt16, vec![]).is_err());
    }

    #[test]
    fn owned_column_zeroed_all_null() {
        let col = Column::zeroed(DataType::Float64, 250, true);
        assert_eq!(col.length(), 250);
        assert_eq!(col.null_count(), 250);
        assert_eq!(col.values.alignment(), 8);
        let view = col.as_view();
        assert_eq!(view.length(), 250);
        assert_eq!(view.null_count(), 250);

        let valid = Column::zeroed(DataType::Float64, 250, false);
        assert_eq!(valid.null_count(), 0);
    }
}
