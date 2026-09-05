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
        self.count_ones() == self.len
    }

    /// 统计 1 位个数（word 批量 popcount）。
    pub fn count_ones(&self) -> usize {
        bitmap_count_ones(self.data.as_slice(), self.bit_offset, self.len) as usize
    }

    pub fn null_count(&self) -> usize {
        self.len - self.count_ones()
    }

    /// 把本视图 `[0 .. len)` 位复制到 `dst` 字节缓冲的 `[dst_off .. dst_off + len)` 位
    /// （LSB-first，按字节批量：头尾掩码 RMW，中间同相位 memcpy / 异相位逐字节移位）。
    /// 返回 `(dst 覆盖前区间 1 位数, 覆盖后区间 1 位数)`，供 null_count 增量维护。
    pub fn copy_bits_into(&self, dst: &mut [u8], dst_off: usize, len: usize) -> (u64, u64) {
        bitmap_copy_bits(dst, dst_off, self.data.as_slice(), self.bit_offset, len)
    }

    /// 把本视图全部位打包为独立字节（bit 0 ↔ 本视图第 0 行，按字节批量复制）。
    pub fn to_packed_bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; (self.len + 7) / 8];
        bitmap_copy_bits(&mut out, 0, self.data.as_slice(), self.bit_offset, self.len);
        out
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

    /// 提取 `[offset, offset + len)` 位区间为独立字节（bit 0 对应区间第 0 行）。
    pub fn extract_bits(&self, offset: usize, len: usize) -> Result<Vec<u8>, FormatError> {
        if offset + len > self.len {
            return Err(FormatError::InvalidLayout(format!(
                "bitmap extract [{offset}, {}) out of range (len={})",
                offset,
                offset + len
            )));
        }
        let mut out = vec![0u8; (len + 7) / 8];
        bitmap_copy_bits(&mut out, 0, self.data.as_slice(), self.bit_offset + offset, len);
        Ok(out)
    }

    /// 统计 1 位个数（word 批量 popcount）。
    pub fn count_ones(&self) -> usize {
        bitmap_count_ones(self.data.as_slice(), self.bit_offset, self.len) as usize
    }

    /// 统计 0 位（NULL）个数。
    pub fn null_count(&self) -> usize {
        self.len - self.count_ones()
    }

    /// 把 `src` 的 `[0 .. len)` 位复制到本位图 `[offset .. offset+len)`（按字节批量）。
    /// 返回 `(覆盖前区间 1 位数, 覆盖后区间 1 位数)`，供 null_count 增量维护。
    pub fn copy_bits_from(&mut self, offset: usize, src: &BitmapView<'_>, len: usize) -> (u64, u64) {
        assert!(offset + len <= self.len, "bit range out of bitmap");
        bitmap_copy_bits(
            self.data.as_mut_slice(),
            self.bit_offset + offset,
            src.data.as_slice(),
            src.bit_offset,
            len,
        )
    }

    /// `[offset .. offset+len)` 批量置为 `valid`（按字节：0xFF / 0x00 + 掩码边缘）；
    /// 返回覆盖前区间 1 位数，供 null_count 增量维护。
    pub fn set_range(&mut self, offset: usize, len: usize, valid: bool) -> u64 {
        assert!(offset + len <= self.len, "bit range out of bitmap");
        bitmap_fill_bits(self.data.as_mut_slice(), self.bit_offset + offset, len, valid)
    }
}

/// 统计 `bits` 中 `[bit_off, bit_off + len)` 区间的 1 位个数。
/// 头尾非对齐位按字节掩码（各 <8 位），中间整字节按 u64 word 批量 popcount。
pub fn bitmap_count_ones(bits: &[u8], bit_off: usize, len: usize) -> u64 {
    if len == 0 {
        return 0;
    }
    debug_assert!(bits.len() * 8 >= bit_off + len, "bit range out of buffer");
    let mut count = 0u64;
    let mut pos = bit_off;
    let mut rest = len;
    // 头部：对齐到字节边界
    let head = ((8 - pos % 8) % 8).min(rest);
    if head > 0 {
        let mask = (((1u16 << head) - 1) << (pos % 8)) as u8;
        count += (bits[pos / 8] & mask).count_ones() as u64;
        pos += head;
        rest -= head;
    }
    // 中间：整字节按 word
    let body = &bits[pos / 8..pos / 8 + rest / 8];
    let mut chunks = body.chunks_exact(8);
    for chunk in &mut chunks {
        count += u64::from_le_bytes(chunk.try_into().expect("8 bytes")).count_ones() as u64;
    }
    for &b in chunks.remainder() {
        count += b.count_ones() as u64;
    }
    pos += rest / 8 * 8;
    rest %= 8;
    // 尾部
    if rest > 0 {
        let mask = ((1u16 << rest) - 1) as u8;
        count += (bits[pos / 8] & mask).count_ones() as u64;
    }
    count
}

/// 把 `dst[bit_off .. bit_off + len)` 批量置为 `valid`（按字节：0xFF / 0x00 + 掩码边缘）；
/// 返回覆盖前区间 1 位数。
pub fn bitmap_fill_bits(dst: &mut [u8], bit_off: usize, len: usize, valid: bool) -> u64 {
    if len == 0 {
        return 0;
    }
    debug_assert!(dst.len() * 8 >= bit_off + len, "bit range out of buffer");
    let old = bitmap_count_ones(dst, bit_off, len);
    let p = bit_off % 8;
    let head = if p == 0 { 0 } else { (8 - p).min(len) };
    let body_bit = bit_off + head;
    let body_bytes = (len - head) / 8;
    let tail = (len - head) % 8;
    if head > 0 {
        let mask = (((1u16 << head) - 1) << p) as u8;
        let d = &mut dst[bit_off / 8];
        if valid {
            *d |= mask;
        } else {
            *d &= !mask;
        }
    }
    if body_bytes > 0 {
        let d = body_bit / 8;
        if valid {
            dst[d..d + body_bytes].fill(0xFF);
        } else {
            dst[d..d + body_bytes].fill(0);
        }
    }
    if tail > 0 {
        let mask = ((1u16 << tail) - 1) as u8;
        let idx = (body_bit + body_bytes * 8) / 8;
        if valid {
            dst[idx] |= mask;
        } else {
            dst[idx] &= !mask;
        }
    }
    old
}

/// 读取 `src` 中以位 `bit` 开始的 `n`（≤8）位，低位对齐返回（高于 n 的位为垃圾，由调用方掩码）。
fn read_shifted_byte(src: &[u8], bit: usize, n: usize) -> u8 {
    let p = bit % 8;
    let lo = src[bit / 8] >> p;
    if p + n <= 8 {
        lo
    } else {
        lo | (src[bit / 8 + 1] << (8 - p))
    }
}

/// 把 `src[src_off .. +len)` 位复制到 `dst[dst_off .. +len)`（LSB-first），按字节批量：
/// 头尾部分字节掩码 RMW，中间整字节同相位 memcpy / 异相位逐字节移位；不逐 bit。
/// 返回 `(覆盖前 dst 区间 1 位数, 覆盖后 dst 区间 1 位数)`。
fn bitmap_copy_bits(
    dst: &mut [u8],
    dst_off: usize,
    src: &[u8],
    src_off: usize,
    len: usize,
) -> (u64, u64) {
    if len == 0 {
        return (0, 0);
    }
    debug_assert!(dst.len() * 8 >= dst_off + len, "dst bit range out of buffer");
    debug_assert!(src.len() * 8 >= src_off + len, "src bit range out of buffer");
    let old = bitmap_count_ones(dst, dst_off, len);

    let p = dst_off % 8;
    let head = if p == 0 { 0 } else { (8 - p).min(len) };
    let body_bit = dst_off + head; // 字节对齐
    let body_bytes = (len - head) / 8;
    let tail = (len - head) % 8;

    // 头部部分字节：源位读出为低位对齐，需左移 p 对齐到 dst 字节内的覆盖位位置
    if head > 0 {
        let mask = (((1u16 << head) - 1) << p) as u8;
        let sb = read_shifted_byte(src, src_off, head) << p;
        let d = &mut dst[dst_off / 8];
        *d = (*d & !mask) | (sb & mask);
    }
    // 中间整字节：同相位纯 memcpy，异相位逐字节移位（无逐 bit 循环）
    if body_bytes > 0 {
        let src_bit = src_off + head;
        let d = body_bit / 8;
        if src_bit % 8 == 0 {
            let s = src_bit / 8;
            dst[d..d + body_bytes].copy_from_slice(&src[s..s + body_bytes]);
        } else {
            let sh = src_bit % 8;
            for k in 0..body_bytes {
                let bit = src_bit + k * 8;
                dst[d + k] = (src[bit / 8] >> sh) | (src[bit / 8 + 1] << (8 - sh));
            }
        }
    }
    // 尾部部分字节
    if tail > 0 {
        let dst_bit = body_bit + body_bytes * 8;
        let src_bit = src_off + head + body_bytes * 8;
        let mask = ((1u16 << tail) - 1) as u8;
        let sb = read_shifted_byte(src, src_bit, tail);
        let d = &mut dst[dst_bit / 8];
        *d = (*d & !mask) | (sb & mask);
    }

    let new = bitmap_count_ones(dst, dst_off, len);
    (old, new)
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

    /// 确定性伪随机位模式（无 rand 依赖）。
    fn pattern_bitmap(len: usize) -> Bitmap {
        let mut bmp = Bitmap::zeros(len);
        for i in 0..len {
            if (i * 7 + 3) % 5 != 0 && (i ^ 0x2A) % 3 != 0 {
                bmp.set(i, true);
            }
        }
        bmp
    }

    #[test]
    fn count_ones_matches_perbit_on_misaligned_ranges() {
        let bmp = pattern_bitmap(257);
        let expected: usize = (0..257).filter(|&i| bmp.is_valid(i)).count();
        assert_eq!(bmp.count_ones(), expected);
        assert_eq!(bmp.null_count(), 257 - expected);
        // 各种非对齐区间与逐位结果一致
        for off in [0usize, 1, 3, 7, 8, 9, 15, 16, 63, 64, 65, 128, 250] {
            for len in [0usize, 1, 2, 7, 8, 9, 16, 33, 64, 65] {
                if off + len > 257 {
                    continue;
                }
                let view = bmp.as_view().slice(off, len).unwrap();
                let per_bit = (0..len).filter(|&i| view.is_valid(i)).count();
                assert_eq!(view.count_ones(), per_bit, "range [{off}, {})", off + len);
            }
        }
    }

    #[test]
    fn copy_bits_roundtrip_all_phases() {
        let src = pattern_bitmap(200);
        for dst_off in [0usize, 1, 3, 7, 8, 9, 15, 16, 63, 65] {
            for src_off in [0usize, 1, 5, 8, 13, 16, 64] {
                let len = 100;
                if src_off + len > 200 || dst_off + len > 260 {
                    continue;
                }
                let mut dst = Bitmap::zeros(260);
                let (old_ones, new_ones) = {
                    let view = src.as_view().slice(src_off, len).unwrap();
                    dst.copy_bits_from(dst_off, &view, len)
                };
                assert_eq!(old_ones, 0, "dst starts all zero");
                assert_eq!(new_ones as usize, {
                    let v = src.as_view().slice(src_off, len).unwrap();
                    (0..len).filter(|&i| v.is_valid(i)).count()
                });
                for i in 0..len {
                    assert_eq!(
                        dst.is_valid(dst_off + i),
                        src.is_valid(src_off + i),
                        "dst_off={dst_off} src_off={src_off} bit {i}"
                    );
                }
                // 区间外不被污染
                assert!((0..dst_off).all(|i| !dst.is_valid(i)));
                assert!((dst_off + len..260).all(|i| !dst.is_valid(i)));
            }
        }
    }

    #[test]
    fn copy_bits_returns_delta_counts() {
        // dst [5..13) 有已知 1 位，覆盖后统计变化
        let mut dst = Bitmap::zeros(40);
        dst.set_range(5, 8, true); // 位 5..12 为 1
        assert_eq!(dst.count_ones(), 8);
        let mut src_bits = Bitmap::zeros(8);
        src_bits.set(1, true);
        src_bits.set(6, true); // 2 个 1 位
        let (old, new) = {
            let view = src_bits.as_view();
            dst.copy_bits_from(5, &view, 8)
        };
        assert_eq!(old, 8);
        assert_eq!(new, 2);
        assert_eq!(dst.count_ones(), 2);
    }

    #[test]
    fn set_range_fill_bytes_and_edges() {
        let mut bmp = Bitmap::zeros(70);
        let old = bmp.set_range(3, 60, true);
        assert_eq!(old, 0);
        assert_eq!(bmp.count_ones(), 60);
        assert!(!bmp.is_valid(2));
        assert!(bmp.is_valid(3));
        assert!(bmp.is_valid(62));
        assert!(!bmp.is_valid(63));
        // 部分清除（跨字节边界）
        let old = bmp.set_range(10, 5, false);
        assert_eq!(old, 5);
        assert_eq!(bmp.count_ones(), 55);
        for i in 0..70 {
            let expected = (3..63).contains(&i) && !(10..15).contains(&i);
            assert_eq!(bmp.is_valid(i), expected, "bit {i}");
        }
    }

    #[test]
    fn extract_bits_matches_source() {
        let src = pattern_bitmap(129);
        for (off, len) in [(0usize, 129usize), (1, 64), (7, 9), (63, 3), (120, 9)] {
            let out = src.extract_bits(off, len).unwrap();
            for i in 0..len {
                let bit = (out[i / 8] >> (i % 8)) & 1 == 1;
                assert_eq!(bit, src.is_valid(off + i), "extract [{off},+{len}) bit {i}");
            }
        }
        assert!(src.extract_bits(128, 2).is_err());
    }
}
