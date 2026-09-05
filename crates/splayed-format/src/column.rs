use crate::bitmap::{Bitmap, BitmapView};
use crate::buffer::{Buffer, BufferView};
use crate::error::FormatError;
use crate::types::DataType;
use crate::validity_size;

/// 段的值表示：定宽连续数组，或 Utf8 字典视图（keys u32 + 字典 offsets/strings）。
#[derive(Debug, Clone, Copy)]
pub enum ColumnValues<'a> {
    Fixed(BufferView<'a>),
    Dict {
        keys: BufferView<'a>,
        dict_offsets: BufferView<'a>,
        dict_strings: BufferView<'a>,
    },
    /// 隐式重复：字典中第 `dict_index` 个字符串重复 `rows` 次（零存储）。
    /// read_index_handle 用此变体避免 sym keys 逐行物化。
    RepeatDict {
        dict_offsets: BufferView<'a>,
        dict_strings: BufferView<'a>,
        dict_index: u32,
    },
}

/// 单列段：一段连续行区间。
///
/// 段内连续是硬约束（SIMD 逐段求值的前提）；`validity = None` 表示该段全部有效。
#[derive(Debug, Clone, Copy)]
pub struct ColumnSegment<'a> {
    values: ColumnValues<'a>,
    validity: Option<BitmapView<'a>>,
    rows: usize,
}

impl<'a> ColumnSegment<'a> {
    /// 定宽段：`values.len() == rows * data_type.size_of()`。
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
        Ok(ColumnSegment { values: ColumnValues::Fixed(values), validity, rows })
    }

    /// 隐式重复字典段：字典中第 `dict_index` 个值重复 `rows` 次（零存储）。
    pub fn new_repeat_dict(
        dict_offsets: BufferView<'a>,
        dict_strings: BufferView<'a>,
        dict_index: u32,
        validity: Option<BitmapView<'a>>,
        rows: usize,
    ) -> Result<Self, FormatError> {
        if dict_offsets.is_empty() || dict_offsets.len() % 8 != 0 {
            return Err(FormatError::InvalidLayout(
                "dict offsets must be a non-empty multiple of 8 bytes".into(),
            ));
        }
        if dict_index as usize >= dict_offsets.len() / 8 - 1 {
            return Err(FormatError::InvalidLayout(format!(
                "dict_index {dict_index} out of range"
            )));
        }
        Ok(ColumnSegment {
            values: ColumnValues::RepeatDict { dict_offsets, dict_strings, dict_index },
            validity,
            rows,
        })
    }

    /// Utf8 字典段：keys 为 u32 LE 字典索引；dict_offsets 为 `(n+1) × u64`；
    /// dict_strings 为连续字符串字节。
    pub fn new_dict(
        keys: BufferView<'a>,
        dict_offsets: BufferView<'a>,
        dict_strings: BufferView<'a>,
        validity: Option<BitmapView<'a>>,
        rows: usize,
    ) -> Result<Self, FormatError> {
        if keys.len() != rows * 4 {
            return Err(FormatError::SizeMismatch { expected: rows * 4, found: keys.len() });
        }
        if dict_offsets.is_empty() || dict_offsets.len() % 8 != 0 {
            return Err(FormatError::InvalidLayout(
                "dict offsets must be a non-empty multiple of 8 bytes".into(),
            ));
        }
        Ok(ColumnSegment {
            values: ColumnValues::Dict { keys, dict_offsets, dict_strings },
            validity,
            rows,
        })
    }

    pub fn values(&self) -> &ColumnValues<'a> {
        &self.values
    }

    /// 定宽段的字节切片（Dict 段返回 `None`）。
    pub fn fixed_bytes(&self) -> Option<&'a [u8]> {
        match &self.values {
            ColumnValues::Fixed(v) => Some(v.as_slice()),
            ColumnValues::Dict { .. } | ColumnValues::RepeatDict { .. } => None,
        }
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

    /// Utf8 列按行取字符串（Fixed 列返回 `None`）。
    pub fn string_at(&self, row: usize) -> Option<&'a str> {
        let mut pos = row;
        for seg in &self.segments {
            if pos < seg.rows() {
                return match &seg.values {
                    ColumnValues::Dict { keys, dict_offsets, dict_strings } => {
                        let kb = keys.as_slice();
                        if pos * 4 + 4 > kb.len() {
                            return None;
                        }
                        let key =
                            u32::from_le_bytes(kb[pos * 4..pos * 4 + 4].try_into().ok()?) as usize;
                        let ob = dict_offsets.as_slice();
                        if (key + 2) * 8 > ob.len() {
                            return None;
                        }
                        let lo = u64::from_le_bytes(ob[key * 8..key * 8 + 8].try_into().ok()?);
                        let hi =
                            u64::from_le_bytes(ob[(key + 1) * 8..(key + 1) * 8 + 8].try_into().ok()?);
                        let sb = dict_strings.as_slice();
                        if hi as usize > sb.len() {
                            return None;
                        }
                        std::str::from_utf8(&sb[lo as usize..hi as usize]).ok()
                    }
                    ColumnValues::RepeatDict { dict_offsets, dict_strings, dict_index } => {
                        let ob = dict_offsets.as_slice();
                        let lo = u64::from_le_bytes(
                            ob[*dict_index as usize * 8..*dict_index as usize * 8 + 8]
                                .try_into()
                                .ok()?,
                        );
                        let hi = u64::from_le_bytes(
                            ob[*dict_index as usize * 8 + 8..*dict_index as usize * 8 + 16]
                                .try_into()
                                .ok()?,
                        );
                        let sb = dict_strings.as_slice();
                        if hi as usize > sb.len() {
                            return None;
                        }
                        std::str::from_utf8(&sb[lo as usize..hi as usize]).ok()
                    }
                    ColumnValues::Fixed(_) => None,
                };
            }
            pos -= seg.rows();
        }
        None
    }

    /// 行区间切片：跨段时返回多段视图，零拷贝（validity 位级切分）。
    pub fn slice_rows(&self, offset: usize, length: usize) -> Result<Self, FormatError> {
        if offset + length > self.length {
            return Err(FormatError::InvalidLayout(format!(
                "slice [{offset}, {}) out of range (len={})",
                offset,
                offset + length,
            )));
        }
        let size = self.data_type.size_of();
        let mut segments = Vec::new();
        let mut seg_start = 0usize; // 当前 segment 的起始行（view 全局坐标）
        let mut pos = offset;
        let end = offset + length;
        for seg in &self.segments {
            if pos >= end {
                break;
            }
            let seg_end = seg_start + seg.rows();
            let lo = pos.max(seg_start);
            let hi = end.min(seg_end);
            if hi > lo {
                let local_lo = lo - seg_start;
                let n = hi - lo;
                let validity = seg
                    .validity()
                    .map(|b| BitmapView::new(BufferView::new(b.as_raw()), local_lo, n))
                    .transpose()?;
                match &seg.values {
                    ColumnValues::Fixed(v) => {
                        let bytes = BufferView::new(&v.as_slice()[local_lo * size..(local_lo + n) * size]);
                        segments.push(ColumnSegment::new(self.data_type, bytes, validity, n)?);
                    }
                    ColumnValues::Dict { keys, dict_offsets, dict_strings } => {
                        let kb = BufferView::new(&keys.as_slice()[local_lo * 4..(local_lo + n) * 4]);
                        segments.push(ColumnSegment::new_dict(
                            kb,
                            *dict_offsets,
                            *dict_strings,
                            validity,
                            n,
                        )?);
                    }
                    ColumnValues::RepeatDict { dict_offsets, dict_strings, dict_index } => {
                        segments.push(ColumnSegment::new_repeat_dict(
                            *dict_offsets,
                            *dict_strings,
                            *dict_index,
                            validity,
                            n,
                        )?);
                    }
                }
                pos = hi;
            }
            seg_start = seg_end;
        }
        ColumnView::new(self.data_type, segments)
    }
}

/// 拥有型字典缓冲（Utf8 列的 offsets + strings）。
#[derive(Debug, Clone)]
pub struct DictBuffers {
    pub offsets: Buffer,
    pub strings: Buffer,
}

/// 拥有型单列（[`Data`](crate::dataview::Data) 的列）。物化即连续单 Buffer；
/// Utf8 列的 `values` 存字典 keys（u32），字符串数据在 `dict` 中。
#[derive(Debug, Clone)]
pub struct Column {
    pub data_type: DataType,
    pub values: Buffer,
    pub validity: Option<Bitmap>,
    pub dict: Option<DictBuffers>,
}

impl Column {
    /// 创建全 NULL（validity 全 0）或全有效（validity = None）的定宽占位列。
    pub fn zeroed(data_type: DataType, rows: usize, all_null: bool) -> Self {
        assert!(data_type != DataType::Utf8, "use Column::from_dict for Utf8");
        let values = Buffer::zeroed_aligned(rows * data_type.size_of(), 8);
        let validity = if all_null { Some(Bitmap::zeros(rows)) } else { None };
        Column { data_type, values, validity, dict: None }
    }

    /// 从字典数据构造 Utf8 列（keys 的数量即逻辑行数；offsets 为字典条目 n+1 项）。
    pub fn from_dict(
        keys: Vec<u32>,
        offsets: Vec<u64>,
        strings: Vec<u8>,
        validity: Option<Bitmap>,
    ) -> Self {
        assert!(
            offsets.len() >= 2 && offsets[0] == 0,
            "dict offsets must have at least 2 entries starting at 0"
        );
        Column {
            data_type: DataType::Utf8,
            values: Buffer::from_vec(
                keys.iter().flat_map(|k| k.to_le_bytes()).collect::<Vec<u8>>(),
            ),
            validity,
            dict: Some(DictBuffers {
                offsets: Buffer::from_vec(offsets.iter().flat_map(|o| o.to_le_bytes()).collect()),
                strings: Buffer::from_vec(strings),
            }),
        }
    }

    pub fn length(&self) -> usize {
        match self.data_type {
            // Utf8 列：values 存字典 keys（u32），行数 = keys 数
            DataType::Utf8 => self.values.len() / 4,
            _ => self.values.len() / self.data_type.size_of(),
        }
    }

    pub fn null_count(&self) -> usize {
        self.validity.as_ref().map(|v| v.as_view().null_count()).unwrap_or(0)
    }

    pub fn as_view(&self) -> ColumnView<'_> {
        let validity = self.validity.as_ref().map(|v| v.as_view());
        let segment = match &self.dict {
            Some(dict) => ColumnSegment::new_dict(
                BufferView::from_buffer(&self.values),
                BufferView::from_buffer(&dict.offsets),
                BufferView::from_buffer(&dict.strings),
                validity,
                self.length(),
            )
            .expect("owned dict column is consistent by construction"),
            None => ColumnSegment::new(
                self.data_type,
                BufferView::from_buffer(&self.values),
                validity,
                self.length(),
            )
            .expect("owned fixed column is consistent by construction"),
        };
        ColumnView::new(self.data_type, vec![segment])
            .expect("single-segment ColumnView is valid by construction")
    }
}

/// 保证 validity 区大小约定不被静默破坏（编译期锚点）。
const _: () = assert!(validity_size(9) == 2);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferView;

    #[test]
    fn segment_checks_value_length() {
        let values = Buffer::from_vec(vec![0u8; 10]);
        let view = BufferView::from_buffer(&values);
        assert!(matches!(
            ColumnSegment::new(DataType::Int32, view, None, 2),
            Err(FormatError::SizeMismatch { expected: 8, found: 10 })
        ));
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

    #[test]
    fn dict_column_string_at() {
        let strings = b"AAPLMSFTGOOG".to_vec();
        let offsets = vec![0u64, 4, 8, 12];
        let keys: Vec<u32> = vec![0, 0, 1, 2, 1];
        let col = Column::from_dict(keys, offsets, strings, None);
        assert_eq!(col.length(), 5);
        let view = col.as_view();
        assert_eq!(view.data_type(), DataType::Utf8);
        assert_eq!(view.string_at(0), Some("AAPL"));
        assert_eq!(view.string_at(1), Some("AAPL"));
        assert_eq!(view.string_at(2), Some("MSFT"));
        assert_eq!(view.string_at(3), Some("GOOG"));
        assert_eq!(view.string_at(4), Some("MSFT"));
        assert_eq!(view.string_at(5), None);
    }
}
