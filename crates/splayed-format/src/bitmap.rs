use crate::buffer::{Buffer, BufferView};
use crate::error::FormatError;

/// LSB-first validity bitmap 视图：bit i ↔ row i，`1 = 有效，0 = NULL`。
///
/// `bit_offset` 允许视图从 buffer 的任意 bit 开始（如按 chunk 切片 validity 区）。
#[derive(Debug, Clone, Copy)]
pub struct BitmapView<'a> {
    data: BufferView<'a>,
    bit_offset: usize,
    len: usize,
}

impl<'a> BitmapView<'a> {
    /// `data` 必须覆盖 `[bit_offset, bit_offset + len)` 全部 bit。
    pub fn new(data: BufferView<'a>, bit_offset: usize, len: usize) -> Result<Self, FormatError> {
        let needed = (bit_offset + len + 7) / 8;
        if data.len() < needed {
            return Err(FormatError::InvalidLayout(format!(
                "validity buffer too small: need {} bytes for bits [{}, {}), got {}",
                needed,
                bit_offset,
                bit_offset + len,
                data.len()
            )));
        }
        Ok(BitmapView { data, bit_offset, len })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn is_valid(&self, i: usize) -> bool {
        assert!(i < self.len, "bit index {i} out of range (len={})", self.len);
        let bit = self.bit_offset + i;
        (self.data.as_slice()[bit / 8] >> (bit % 8)) & 1 == 1
    }

    /// 底层字节切片（自 `bit_offset` 所在字节起）。
    pub fn as_raw(&self) -> &'a [u8] {
        self.data.as_slice()
    }

    /// 全部有效时返回 true（不分配的快速路径由调用方用 `Option<BitmapView>` 表达）。
    pub fn all_valid(&self) -> bool {
        (0..self.len).all(|i| self.is_valid(i))
    }

    pub fn null_count(&self) -> usize {
        self.len - (0..self.len).filter(|&i| self.is_valid(i)).count()
    }

    /// 子区间切片，不复制。
    pub fn slice(&self, offset: usize, len: usize) -> Result<Self, FormatError> {
        if offset + len > self.len {
            return Err(FormatError::InvalidLayout(format!(
                "bitmap slice [{}, {}) out of range (len={})",
                offset,
                offset + len,
                self.len
            )));
        }
        BitmapView::new(self.data, self.bit_offset + offset, len)
    }
}

/// 拥有型 validity bitmap。
#[derive(Debug, Clone)]
pub struct Bitmap {
    data: Buffer,
    bit_offset: usize,
    len: usize,
}

impl Bitmap {
    /// 全 NULL（全 0 位）。
    pub fn zeros(len: usize) -> Self {
        Bitmap { data: Buffer::zeroed_aligned((len + 7) / 8, 1), bit_offset: 0, len }
    }

    /// 全有效（全 1 位，最后不完整字节按位补齐）。
    pub fn ones(len: usize) -> Self {
        let mut data = Buffer::zeroed_aligned((len + 7) / 8, 1);
        let bytes = data.as_mut_slice();
        for byte in bytes.iter_mut() {
            *byte = 0xFF;
        }
        if len % 8 != 0 {
            let last = bytes.len() - 1;
            bytes[last] = (1 << (len % 8)) - 1;
        }
        Bitmap { data, bit_offset: 0, len }
    }

    pub fn from_bytes(bytes: Vec<u8>, len: usize) -> Self {
        assert_eq!(bytes.len(), (len + 7) / 8, "bitmap byte length mismatch");
        Bitmap { data: Buffer::from_vec(bytes), bit_offset: 0, len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn is_valid(&self, i: usize) -> bool {
        self.as_view().is_valid(i)
    }

    pub fn set(&mut self, i: usize, valid: bool) {
        assert!(i < self.len, "bit index {i} out of range (len={})", self.len);
        let bit = self.bit_offset + i;
        let byte = &mut self.data.as_mut_slice()[bit / 8];
        if valid {
            *byte |= 1 << (bit % 8);
        } else {
            *byte &= !(1 << (bit % 8));
        }
    }

    pub fn as_view(&self) -> BitmapView<'_> {
        BitmapView::new(BufferView::from_buffer(&self.data), self.bit_offset, self.len)
            .expect("owned bitmap layout is valid by construction")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ones_and_zeros_roundtrip() {
        for len in [0usize, 1, 7, 8, 9, 63, 64, 65, 250] {
            let ones = Bitmap::ones(len);
            assert_eq!(ones.len(), len);
            assert_eq!(ones.as_view().null_count(), 0);

            let zeros = Bitmap::zeros(len);
            assert_eq!(zeros.as_view().null_count(), len);
        }
        let bmp = Bitmap::ones(10);
        assert_eq!(bmp.as_view().as_raw().len(), 2);
        // 最后一个不完整字节只置低 2 位
        assert_eq!(bmp.as_view().as_raw()[1], 0b0000_0011);
    }

    #[test]
    fn set_and_slice() {
        let mut bmp = Bitmap::ones(20);
        bmp.set(3, false);
        bmp.set(17, false);
        assert!(!bmp.is_valid(3));
        assert!(!bmp.is_valid(17));
        assert!(bmp.is_valid(4));

        let tail = bmp.as_view().slice(16, 4).unwrap();
        assert_eq!(tail.len(), 4);
        assert!(tail.is_valid(0));
        assert!(!tail.is_valid(1));

        assert!(bmp.as_view().slice(18, 4).is_err());
    }

    #[test]
    fn bit_offset_view() {
        let mut bmp = Bitmap::ones(16);
        bmp.set(9, false);
        // 从 bit 8 开始的视图：其 index 1 对应全局 bit 9
        let view = BitmapView::new(
            crate::buffer::BufferView::from_buffer(&bmp.data),
            8,
            8,
        )
        .unwrap();
        assert!(!view.is_valid(1));
        assert!(view.is_valid(0));
        assert!(view.is_valid(7));
    }
}
