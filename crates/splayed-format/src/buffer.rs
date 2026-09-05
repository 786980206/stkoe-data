use std::fmt;

use crate::error::FormatError;

/// 拥有型字节 Buffer。
///
/// `raw` 为实际分配，`[offset, offset + len)` 为对外数据区；`zeroed_aligned`
/// 通过「多分配 + 起始对齐」在纯安全代码内保证 `alignment` 对齐（raw 不会再扩容）。
#[derive(Debug)]
pub struct Buffer {
    raw: Vec<u8>,
    offset: usize,
    len: usize,
    alignment: usize,
}

impl Buffer {
    /// 直接接管一个 `Vec<u8>`，对齐为 1。
    pub fn from_vec(v: Vec<u8>) -> Self {
        let len = v.len();
        Buffer { raw: v, offset: 0, len, alignment: 1 }
    }

    /// 分配 `len` 字节零填充区域，数据区起始按 `alignment`（2 的幂）对齐。
    pub fn zeroed_aligned(len: usize, alignment: usize) -> Self {
        assert!(alignment.is_power_of_two(), "alignment must be a power of two");
        let raw = vec![0u8; len + alignment - 1];
        let base = raw.as_ptr() as usize;
        let offset = ((base + alignment - 1) & !(alignment - 1)) - base;
        Buffer { raw, offset, len, alignment }
    }

    /// 分配对齐区域并拷入字节（空数据给出空缓冲）。
    pub fn zeroed_aligned_bytes(bytes: Vec<u8>, alignment: usize) -> Self {
        let len = bytes.len();
        let mut buf = Buffer::zeroed_aligned(len, alignment);
        buf.as_mut_slice().copy_from_slice(&bytes);
        buf
    }

    /// 以拷贝方式从 typed 连续数据构造（Data 物化边界的预期拷贝）。
    pub fn from_slice_copy<T: bytemuck::NoUninit>(values: &[T]) -> Self {
        let bytes: &[u8] = bytemuck::cast_slice(values);
        let alignment = std::mem::align_of::<T>();
        let mut buf = Buffer::zeroed_aligned(bytes.len(), alignment);
        buf.as_mut_slice().copy_from_slice(bytes);
        buf
    }

    pub fn alignment(&self) -> usize {
        self.alignment
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.raw[self.offset..self.offset + self.len]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let offset = self.offset;
        let len = self.len;
        &mut self.raw[offset..offset + len]
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(v: Vec<u8>) -> Self {
        Buffer::from_vec(v)
    }
}

impl Clone for Buffer {
    fn clone(&self) -> Self {
        let mut buf = Buffer::zeroed_aligned(self.len, self.alignment);
        buf.as_mut_slice().copy_from_slice(self.as_slice());
        buf
    }
}

impl fmt::Display for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Buffer(len={}, alignment={})", self.len, self.alignment)
    }
}

/// 非拥有字节视图（non-owning）。
///
/// 指针对齐由生产方保证：mmap 切片（FIELD 数据区固定起始于 64，天然 8 对齐）与
/// [`Buffer`] 均满足；`BufferView::cast` 在转换时显式校验。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferView<'a> {
    data: &'a [u8],
}

impl<'a> BufferView<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        BufferView { data }
    }

    pub fn from_buffer(buffer: &'a Buffer) -> Self {
        BufferView { data: buffer.as_slice() }
    }

    /// 经 `offset + size` 切片构造，不复制；越界 panic（与 slice 语义一致）。
    pub fn slice(&self, offset: usize, len: usize) -> BufferView<'a> {
        BufferView { data: &self.data[offset..offset + len] }
    }

    pub fn as_slice(&self) -> &'a [u8] {
        self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// 按 `T` 转换为连续切片：校验总长度与指针对齐，零拷贝。
    pub fn cast<T: bytemuck::Pod>(&self) -> Result<&'a [T], FormatError> {
        let required = std::mem::size_of::<T>();
        if self.data.len() % required != 0 {
            return Err(FormatError::SizeMismatch {
                expected: required,
                found: self.data.len() % required,
            });
        }
        let found = self.data.as_ptr() as usize % std::mem::align_of::<T>();
        if found != 0 {
            return Err(FormatError::Alignment {
                required: std::mem::align_of::<T>(),
                found,
            });
        }
        Ok(bytemuck::cast_slice(self.data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zeroed_aligned_offsets_to_alignment() {
        for alignment in [1usize, 2, 4, 8, 16, 64] {
            for len in [0usize, 1, 7, 64, 4096] {
                let buf = Buffer::zeroed_aligned(len, alignment);
                assert_eq!(buf.len(), len);
                let addr = buf.as_slice().as_ptr() as usize;
                assert_eq!(addr % alignment, 0);
                assert!(buf.as_slice().iter().all(|&b| b == 0));
            }
        }
    }

    #[test]
    fn from_slice_copy_preserves_bytes_and_alignment() {
        let src: Vec<i64> = (0..100).map(|i| i * 7919 - 3).collect();
        let buf = Buffer::from_slice_copy(&src);
        assert_eq!(buf.alignment(), 8);
        let round: &[i64] = bytemuck::cast_slice(buf.as_slice());
        assert_eq!(round, src.as_slice());
    }

    #[test]
    fn buffer_view_slice_and_cast() {
        let buf = Buffer::from_vec((0..16u8).collect());
        let view = BufferView::from_buffer(&buf);
        assert_eq!(view.slice(4, 8).as_slice(), &[4, 5, 6, 7, 8, 9, 10, 11]);
        let words: &[u32] = view.cast::<u32>().unwrap();
        assert_eq!(words.len(), 4);

        let odd = Buffer::from_vec(vec![0u8; 7]);
        let misaligned = BufferView::from_buffer(&odd).slice(1, 4);
        assert!(matches!(
            misaligned.cast::<u32>(),
            Err(FormatError::Alignment { required: 4, found: 1 })
        ));
    }
}
