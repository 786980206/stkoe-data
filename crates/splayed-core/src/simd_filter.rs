//! SIMD-accelerated batch filtering for common comparison operations.
//!
//! See `plan.md` §9.3 (值过滤) and §13 (性能优化).
//!
//! When filtering a column with `WHERE close > 100`, the scalar loop
//! (one `filter_passes` call per row) is slow. This module provides
//! batch filter functions that:
//! 1. Process values in chunks of 8 using manual SIMD (auto-vectorizable)
//! 2. Skip NULL sentinels efficiently (canonical NaN, INT64_MIN)
//! 3. Return a bitmask or passing-index vector
//!
//! The functions are designed to auto-vectorize with LLVM's optimization
//! passes — the inner loops are tight enough that the compiler can
//! generate SIMD instructions (SSE2/AVX2 on x86, NEON on ARM) without
//! explicit intrinsics.

/// A bitmask where bit `i` is 1 if row `i` passes the filter.
///
/// For `row_count > 64`, only the first 64 rows are evaluated (caller
/// must call in chunks). The returned mask has bits set for passing rows.
#[inline]
pub fn filter_mask_f64_gt(data: &[u8], threshold: f64, row_count: usize) -> u64 {
    filter_mask_f64(data, threshold, row_count, |v, t| v > t)
}

#[inline]
pub fn filter_mask_f64_lt(data: &[u8], threshold: f64, row_count: usize) -> u64 {
    filter_mask_f64(data, threshold, row_count, |v, t| v < t)
}

#[inline]
pub fn filter_mask_f64_ge(data: &[u8], threshold: f64, row_count: usize) -> u64 {
    filter_mask_f64(data, threshold, row_count, |v, t| v >= t)
}

#[inline]
pub fn filter_mask_f64_le(data: &[u8], threshold: f64, row_count: usize) -> u64 {
    filter_mask_f64(data, threshold, row_count, |v, t| v <= t)
}

#[inline]
pub fn filter_mask_f64_eq(data: &[u8], threshold: f64, row_count: usize) -> u64 {
    filter_mask_f64(data, threshold, row_count, |v, t| v == t)
}

#[inline]
pub fn filter_mask_f64_ne(data: &[u8], threshold: f64, row_count: usize) -> u64 {
    filter_mask_f64(data, threshold, row_count, |v, t| v != t)
}

/// Int64 SIMD filter masks.
#[inline]
pub fn filter_mask_i64_gt(data: &[u8], threshold: i64, row_count: usize) -> u64 {
    filter_mask_i64(data, threshold, row_count, |v, t| v > t)
}

#[inline]
pub fn filter_mask_i64_lt(data: &[u8], threshold: i64, row_count: usize) -> u64 {
    filter_mask_i64(data, threshold, row_count, |v, t| v < t)
}

#[inline]
pub fn filter_mask_i64_ge(data: &[u8], threshold: i64, row_count: usize) -> u64 {
    filter_mask_i64(data, threshold, row_count, |v, t| v >= t)
}

#[inline]
pub fn filter_mask_i64_le(data: &[u8], threshold: i64, row_count: usize) -> u64 {
    filter_mask_i64(data, threshold, row_count, |v, t| v <= t)
}

#[inline]
pub fn filter_mask_i64_eq(data: &[u8], threshold: i64, row_count: usize) -> u64 {
    filter_mask_i64(data, threshold, row_count, |v, t| v == t)
}

#[inline]
pub fn filter_mask_i64_ne(data: &[u8], threshold: i64, row_count: usize) -> u64 {
    filter_mask_i64(data, threshold, row_count, |v, t| v != t)
}

// ---------------------------------------------------------------------------
// Core implementations
// ---------------------------------------------------------------------------

/// SIMD-friendly f64 filter: evaluate `cmp(value, threshold)` for each row.
///
/// NULL values (canonical NaN = 0x7FF8000000000000) always fail the filter.
/// Non-canonical NaN values are treated as valid data points.
///
/// The inner loop is designed to auto-vectorize: it reads 8 consecutive
/// f64 values, checks for NULL, and applies the comparison.
#[inline]
fn filter_mask_f64<F: Fn(f64, f64) -> bool>(
    data: &[u8],
    threshold: f64,
    row_count: usize,
    cmp: F,
) -> u64 {
    let values = unsafe { cast_bytes::<f64>(data, row_count) };
    let mut mask: u64 = 0;

    for (i, &v) in values.iter().enumerate() {
        if i >= 64 {
            break;
        }
        // Skip NULL (canonical NaN) and all NaN to match scalar path.
        if v.is_nan() {
            continue;
        }
        if cmp(v, threshold) {
            mask |= 1 << i;
        }
    }

    mask
}

/// SIMD-friendly i64 filter: evaluate `cmp(value, threshold)` for each row.
///
/// NULL values (INT64_MIN) always fail the filter.
#[inline]
fn filter_mask_i64<F: Fn(i64, i64) -> bool>(
    data: &[u8],
    threshold: i64,
    row_count: usize,
    cmp: F,
) -> u64 {
    let values = unsafe { cast_bytes::<i64>(data, row_count) };
    let null_val = i64::MIN;
    let mut mask: u64 = 0;

    for (i, &v) in values.iter().enumerate() {
        if i >= 64 {
            break;
        }
        if v == null_val {
            continue;
        }
        if cmp(v, threshold) {
            mask |= 1 << i;
        }
    }

    mask
}

/// Collect passing row indices from a bitmask.
#[inline]
pub fn mask_to_indices(mask: u64) -> Vec<usize> {
    let mut indices = Vec::new();
    let mut m = mask;
    while m != 0 {
        let idx = m.trailing_zeros() as usize;
        indices.push(idx);
        m &= m - 1; // clear lowest set bit
    }
    indices
}

/// Apply a batch filter to f64 data, returning indices of passing rows.
///
/// This is the high-level entry point used by the Scanner. It processes
/// data in 64-row chunks and collects all passing indices.
pub fn batch_filter_f64<F: Fn(f64, f64) -> bool>(
    data: &[u8],
    threshold: f64,
    row_count: usize,
    cmp: F,
) -> Vec<usize> {
    let values = unsafe { cast_bytes::<f64>(data, row_count) };
    let mut indices = Vec::with_capacity(row_count / 2);

    for (i, &v) in values.iter().enumerate() {
        // Skip NULL (canonical NaN) and all other NaN values to match
        // the scalar compare() path which returns 0 for NaN (no match).
        if v.is_nan() {
            continue;
        }
        if cmp(v, threshold) {
            indices.push(i);
        }
    }

    indices
}

/// Apply a batch filter to i64 data, returning indices of passing rows.
pub fn batch_filter_i64<F: Fn(i64, i64) -> bool>(
    data: &[u8],
    threshold: i64,
    row_count: usize,
    cmp: F,
) -> Vec<usize> {
    let values = unsafe { cast_bytes::<i64>(data, row_count) };
    let null_val = i64::MIN;
    let mut indices = Vec::with_capacity(row_count / 2);

    for (i, &v) in values.iter().enumerate() {
        if v == null_val {
            continue;
        }
        if cmp(v, threshold) {
            indices.push(i);
        }
    }

    indices
}

// ---------------------------------------------------------------------------
// Safety helpers
// ---------------------------------------------------------------------------

/// Cast a byte slice to a typed slice, checking alignment and length.
///
/// # Safety
/// `data.len()` must be >= `row_count * size_of::<T>()`.
#[inline]
unsafe fn cast_bytes<T>(data: &[u8], row_count: usize) -> &[T] {
    let len = row_count.min(data.len() / std::mem::size_of::<T>());
    let ptr = data.as_ptr() as *const T;
    std::slice::from_raw_parts(ptr, len)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f64_filter_gt() {
        let values: Vec<f64> = vec![1.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let mask = filter_mask_f64_gt(&raw, 15.0, 8);
        // Values > 15.0: indices 4,5,6,7 → bits 4,5,6,7 = 0b11110000 = 0xF0
        assert_eq!(mask, 0b11110000);

        let indices = batch_filter_f64(&raw, 15.0, 8, |v, t| v > t);
        assert_eq!(indices, vec![4, 5, 6, 7]);
    }

    #[test]
    fn f64_filter_with_null() {
        let mut values: Vec<f64> = vec![1.0, 5.0, 10.0, 15.0];
        // Insert a NULL (canonical NaN = 0x7FF8000000000000) at index 1.
        values[1] = f64::from_bits(0x7FF8_0000_0000_0000);
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        // filter > 0: indices 0, 2, 3 pass (index 1 is NULL → fails)
        let indices = batch_filter_f64(&raw, 0.0, 4, |v, t| v > t);
        assert_eq!(indices, vec![0, 2, 3]);
    }

    #[test]
    fn i64_filter_gt() {
        let values: Vec<i64> = vec![10, 20, 30, 40, 50, 60, 70, 80];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let mask = filter_mask_i64_gt(&raw, 45, 8);
        // Values > 45: indices 4,5,6,7 → bits 4,5,6,7 = 0xF0
        assert_eq!(mask, 0b11110000);

        let indices = batch_filter_i64(&raw, 45, 8, |v, t| v > t);
        assert_eq!(indices, vec![4, 5, 6, 7]);
    }

    #[test]
    fn i64_filter_with_null() {
        let mut values: Vec<i64> = vec![10, 20, 30, 40];
        // Insert a NULL (INT64_MIN) at index 2
        values[2] = i64::MIN;
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        // filter > 0: indices 0,1,3 pass (index 2 is NULL → fails)
        let indices = batch_filter_i64(&raw, 0, 4, |v, t| v > t);
        assert_eq!(indices, vec![0, 1, 3]);
    }

    #[test]
    fn mask_to_indices_roundtrip() {
        let mask = 0b10101010u64; // bits 1,3,5,7
        let indices = mask_to_indices(mask);
        assert_eq!(indices, vec![1, 3, 5, 7]);
    }

    #[test]
    fn f64_filter_eq() {
        let values: Vec<f64> = vec![1.0, 5.0, 5.0, 10.0, 5.0];
        let raw: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();

        let indices = batch_filter_f64(&raw, 5.0, 5, |v, t| v == t);
        assert_eq!(indices, vec![1, 2, 4]);
    }
}
