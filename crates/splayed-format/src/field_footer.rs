//! FIELD 文件尾部统计（footer）：min/max 持久化。
//!
//! 布局（追加在数据区之后，定长 28 字节）：
//!
//! ```text
//! [0..4)   magic   u32 LE = 0x46544653 ("SFTF")
//! [4)      version u8  = 1
//! [5)      flags   u8  = bit0: min/max 有效
//! [6..8)   reserved u16
//! [8..16)  min     [u8;8]（原始 LE 槽位；宽度 = DataType::size_of()）
//! [16..24) max     [u8;8]
//! [24..28) total   u32 LE = 28
//! ```
//!
//! 规则：
//! - min/max 计算**跳过 NULL 哨兵位型**（浮点 canonical NaN、INT_MIN、无符号全 1）;
//! - 全 NULL 列不写统计（flags=0）或直接不写 footer；读取侧以 magic 判定存在；
//! - `update_field` 原地写后统计失效 → 覆写 magic 为 0 使 footer 无效；
//! - `compact_field` 重写文件时**重算**统计（compacted 只读，统计永久有效）。

use crate::types::DataType;

/// 定长 footer 总字节数。
pub const FOOTER_SIZE: usize = 28;
/// Footer magic（小端 "SFTF"）。
pub const FOOTER_MAGIC: u32 = u32::from_le_bytes(*b"SFTF");
/// 统计有效的 flags 位。
pub const FOOTER_FLAG_STATS_VALID: u8 = 0x01;

/// 对一列已解码后的原始字节（LE，行×size_of）计算 min/max 统计。
///
/// 返回 `(min_slot, max_slot)`，槽位为 8 字节原始 LE（宽类型只在低 `size_of`
/// 字节有效）；全 NULL 或空数据返回 `None`。
pub fn compute_stats(data_type: DataType, data: &[u8]) -> Option<([u8; 8], [u8; 8])> {
    let w = data_type.size_of();
    if w == 0 || data.len() < w {
        return None;
    }
    let nulls = data_type.null_bytes();

    // 统一比较域：i128（有符号整数/日期/时间戳）、u128（无符号）、f64（浮点）、u8（BOOL）。
    let mut i_min: Option<i128> = None;
    let mut i_max: Option<i128> = None;
    let mut u_min: Option<u128> = None;
    let mut u_max: Option<u128> = None;
    let mut f_min: Option<f64> = None;
    let mut f_max: Option<f64> = None;

    let rows = data.len() / w;
    for r in 0..rows {
        let chunk = &data[r * w..(r + 1) * w];
        if chunk == nulls {
            continue; // NULL 哨兵，跳过
        }
        use DataType::*;
        match data_type {
            Int8 | Int16 | Int32 | Int64 | Date32 | Date64 | TimestampUs => {
                let v = int_of(chunk, w);
                i_min = Some(i_min.map_or(v, |m| m.min(v)));
                i_max = Some(i_max.map_or(v, |m| m.max(v)));
            }
            UInt8 | UInt16 | UInt32 | UInt64 => {
                let v = uint_of(chunk, w);
                u_min = Some(u_min.map_or(v, |m| m.min(v)));
                u_max = Some(u_max.map_or(v, |m| m.max(v)));
            }
            Float32 | Float64 => {
                let v = float_of(chunk, w);
                f_min = Some(f_min.map_or(v, |m| m.min(v)));
                f_max = Some(f_max.map_or(v, |m| m.max(v)));
            }
            Bool => {
                let v = chunk[0];
                u_min = Some(u_min.map_or(v as u128, |m| m.min(v as u128)));
                u_max = Some(u_max.map_or(v as u128, |m| m.max(v as u128)));
            }
        }
    }

    let slot = |v: u64| v.to_le_bytes();
    use DataType::*;
    match data_type {
        Int8 | Int16 | Int32 | Int64 | Date32 | Date64 | TimestampUs => {
            let (mn, mx) = (i_min?, i_max?);
            Some((slot(mn as u64), slot(mx as u64)))
        }
        UInt8 | UInt16 | UInt32 | UInt64 | Bool => {
            let (mn, mx) = (u_min?, u_max?);
            Some((slot(mn as u64), slot(mx as u64)))
        }
        Float32 | Float64 => {
            let (mn, mx) = (f_min?, f_max?);
            let s = |v: f64| -> [u8; 8] {
                if data_type == Float32 {
                    // NaN 已排除；f32 槽位存其 4 字节位型
                    let b4 = (v as f32).to_bits();
                    let mut out = [0u8; 8];
                    out[..4].copy_from_slice(&b4.to_le_bytes());
                    out
                } else {
                    v.to_bits().to_le_bytes()
                }
            };
            Some((s(mn), s(mx)))
        }
    }
}

/// 编码 footer（`stats=None` 时 flags=0，槽位清零）。
pub fn encode_footer(stats: Option<([u8; 8], [u8; 8])>) -> [u8; FOOTER_SIZE] {
    let mut f = [0u8; FOOTER_SIZE];
    f[..4].copy_from_slice(&FOOTER_MAGIC.to_le_bytes());
    f[4] = 1; // version
    if let Some((mn, mx)) = stats {
        f[5] = FOOTER_FLAG_STATS_VALID;
        f[8..16].copy_from_slice(&mn);
        f[16..24].copy_from_slice(&mx);
    }
    f[24..28].copy_from_slice(&(FOOTER_SIZE as u32).to_le_bytes());
    f
}

/// 解析尾部字节：magic + 长度合法 → `Some((stats_valid, min, max))`。
pub fn parse_footer(bytes: &[u8]) -> Option<(bool, [u8; 8], [u8; 8])> {
    if bytes.len() != FOOTER_SIZE {
        return None;
    }
    let magic = u32::from_le_bytes(bytes[..4].try_into().ok()?);
    if magic != FOOTER_MAGIC {
        return None;
    }
    let total = u32::from_le_bytes(bytes[24..28].try_into().ok()?);
    if total as usize != FOOTER_SIZE {
        return None;
    }
    let valid = bytes[5] & FOOTER_FLAG_STATS_VALID != 0;
    let mut mn = [0u8; 8];
    let mut mx = [0u8; 8];
    mn.copy_from_slice(&bytes[8..16]);
    mx.copy_from_slice(&bytes[16..24]);
    Some((valid, mn, mx))
}

// ---------------------------------------------------------------------------
// 域转换（小端）
// ---------------------------------------------------------------------------

fn int_of(chunk: &[u8], w: usize) -> i128 {
    let mut buf = [0u8; 8];
    buf[..w].copy_from_slice(chunk);
    let raw = u64::from_le_bytes(buf);
    if w < 8 {
        // 符号扩展：把 w 字节的值在位 63 处对齐后算术右移。
        let shift = 64 - (w as u32) * 8;
        ((raw << shift) as i64 >> shift) as i128
    } else {
        raw as i64 as i128
    }
}

fn uint_of(chunk: &[u8], w: usize) -> u128 {
    let mut buf = [0u8; 8];
    buf[..w].copy_from_slice(chunk);
    u64::from_le_bytes(buf) as u128
}

fn float_of(chunk: &[u8], w: usize) -> f64 {
    if w == 4 {
        let mut b = [0u8; 4];
        b.copy_from_slice(chunk);
        f32::from_le_bytes(b) as f64
    } else {
        let mut b = [0u8; 8];
        b.copy_from_slice(chunk);
        f64::from_le_bytes(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(v: u64) -> [u8; 8] {
        v.to_le_bytes()
    }

    #[test]
    fn encode_parse_roundtrip() {
        let f = encode_footer(Some((slot(10), slot(20))));
        let (valid, mn, mx) = parse_footer(&f).unwrap();
        assert!(valid);
        assert_eq!(mn, slot(10));
        assert_eq!(mx, slot(20));

        let f2 = encode_footer(None);
        let (valid2, _, _) = parse_footer(&f2).unwrap();
        assert!(!valid2);

        // magic 被破坏 → 无效（update_field 失效路径）
        let mut bad = f;
        bad[..4].copy_from_slice(&0u32.to_le_bytes());
        assert!(parse_footer(&bad).is_none());
    }

    #[test]
    fn compute_stats_int_skips_nulls() {
        use DataType::*;
        // Int32：[MIN(哨兵), 5, 3, MAX(哨兵), 7] → [3, 7]
        let mut data = Vec::new();
        data.extend_from_slice(Int32.null_bytes());
        data.extend_from_slice(&5i32.to_le_bytes());
        data.extend_from_slice(&3i32.to_le_bytes());
        data.extend_from_slice(Int32.null_bytes());
        data.extend_from_slice(&7i32.to_le_bytes());
        let (mn, mx) = compute_stats(Int32, &data).unwrap();
        assert_eq!(mn, slot(3));
        assert_eq!(mx, slot(7));
    }

    #[test]
    fn compute_stats_float_and_unsigned() {
        use DataType::*;
        let mut f = DataType::Float64.null_bytes().to_vec();
        f.extend_from_slice(&1.5f64.to_le_bytes());
        f.extend_from_slice(&(-2.25f64).to_le_bytes());
        let (mn, mx) = compute_stats(Float64, &f).unwrap();
        assert_eq!(f64::from_le_bytes(mn), -2.25);
        assert_eq!(f64::from_le_bytes(mx), 1.5);

        // UInt32：全 1 = 哨兵
        let mut u = Vec::new();
        u.extend_from_slice(&u32::MAX.to_le_bytes()); // null
        u.extend_from_slice(&10u32.to_le_bytes());
        u.extend_from_slice(&20u32.to_le_bytes());
        let (mn, mx) = compute_stats(UInt32, &u).unwrap();
        assert_eq!(mn, slot(10));
        assert_eq!(mx, slot(20));
    }

    #[test]
    fn compute_stats_all_null_is_none() {
        use DataType::*;
        // Float64 全到底是什么取决于内容；直接用 Int32 全 NULL
        let nulls = Int32.null_bytes();
        let mut d = Vec::new();
        for _ in 0..6 {
            d.extend_from_slice(nulls);
        }
        assert!(compute_stats(Int32, &d).is_none());
    }

    #[test]
    fn compute_stats_narrow_types() {
        use DataType::*;
        let d = vec![(-5i8) as u8, 3i8 as u8];
        let (mn, mx) = compute_stats(Int8, &d).unwrap();
        assert_eq!(mn, slot(-5i8 as i64 as u64));
        assert_eq!(mx, slot(3));
    }
}