//! Partition 命名与时间边界：Hive-style 单层目录，四选一
//! （none / year=YYYY / month=YYYY-MM / date=YYYY-MM-DD）。

use splayed_format::TimeType;

pub const US_PER_DAY: i64 = 86_400_000_000;

/// 分区方案（Table 级固定四选一）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionScheme {
    None,
    Year,
    Month,
    Date,
}

impl PartitionScheme {
    /// 从分区目录名前缀推断方案（`year=2026` → Year）。
    pub fn from_partition_name(name: &str) -> Option<Self> {
        let prefix = name.split('=').next()?;
        match prefix {
            "year" => Some(PartitionScheme::Year),
            "month" => Some(PartitionScheme::Month),
            "date" => Some(PartitionScheme::Date),
            _ => None,
        }
    }

    pub fn allows_partitions(self) -> bool {
        self != PartitionScheme::None
    }
}

/// 民用日 ↔ 天数（Hinnant 算法，proleptic Gregorian，epoch = 1970-01-01）。
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let mp: u64 = if m > 2 { u64::from(m - 3) } else { u64::from(m + 9) };
    let doy = (153 * mp + 2) / 5 + u64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m: u32 = if mp < 10 { (mp + 3) as u32 } else { (mp - 9) as u32 };
    let y = y + i64::from(m <= 2);
    (y, m, d)
}

/// 把时间值换算为「天」（Date32 值即天数；TimestampUs / Date64 / Int64 为微秒）。
pub fn value_to_days(value: i64, tt: TimeType) -> i64 {
    match tt {
        TimeType::Date32 => value,
        TimeType::TimestampUs => value.div_euclid(US_PER_DAY),
    }
}

/// 把「天」换算回时间值。
pub fn days_to_value(days: i64, tt: TimeType) -> i64 {
    match tt {
        TimeType::Date32 => days,
        TimeType::TimestampUs => days * US_PER_DAY,
    }
}

/// 时间值 → 分区目录名。
pub fn partition_name(scheme: PartitionScheme, value: i64, tt: TimeType) -> String {
    let (y, m, d) = civil_from_days(value_to_days(value, tt));
    match scheme {
        PartitionScheme::None => String::new(),
        PartitionScheme::Year => format!("year={y}"),
        PartitionScheme::Month => format!("month={y:04}-{m:02}"),
        PartitionScheme::Date => format!("date={y:04}-{m:02}-{d:02}"),
    }
}

/// 分区目录名 → 时间值边界 `[min, max)`（单位与 `tt` 一致）。
pub fn partition_range(scheme: PartitionScheme, name: &str, tt: TimeType) -> Option<(i64, i64)> {
    let (prefix, value) = name.split_once('=')?;
    match (scheme, prefix) {
        (PartitionScheme::Year, "year") => {
            let y: i64 = value.parse().ok()?;
            Some((days_from_civil(y, 1, 1), days_from_civil(y + 1, 1, 1)))
        }
        (PartitionScheme::Month, "month") => {
            let (y, m) = value.split_once('-')?;
            let (y, m): (i64, u32) = (y.parse().ok()?, m.parse().ok()?);
            let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
            Some((days_from_civil(y, m, 1), days_from_civil(ny, nm, 1)))
        }
        (PartitionScheme::Date, "date") => {
            let mut it = value.split('-');
            let y: i64 = it.next()?.parse().ok()?;
            let m: u32 = it.next()?.parse().ok()?;
            let d: u32 = it.next()?.parse().ok()?;
            Some((days_from_civil(y, m, d), days_from_civil(y, m, d) + 1))
        }
        _ => None,
    }
    .map(|(lo, hi)| (days_to_value(lo, tt), days_to_value(hi, tt)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_roundtrip() {
        for days in [-719_468i64, 0, 1, 19_000, 20_000, 100_000] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(days_from_civil(2026, 9, 4)), (2026, 9, 4));
    }

    #[test]
    fn partition_names_and_ranges() {
        let days = days_from_civil(2026, 9, 4);
        let name = partition_name(PartitionScheme::Date, days, TimeType::Date32);
        assert_eq!(name, "date=2026-09-04");
        assert_eq!(
            partition_range(PartitionScheme::Date, &name, TimeType::Date32),
            Some((days, days + 1))
        );
        let name = partition_name(PartitionScheme::Month, days, TimeType::Date32);
        assert_eq!(name, "month=2026-09");
        let (lo, hi) = partition_range(PartitionScheme::Month, &name, TimeType::Date32).unwrap();
        assert_eq!(hi - lo, 30); // 九月 30 天
        let name = partition_name(PartitionScheme::Year, days * US_PER_DAY, TimeType::TimestampUs);
        assert_eq!(name, "year=2026");
        let (lo, hi) = partition_range(PartitionScheme::Year, &name, TimeType::TimestampUs).unwrap();
        assert_eq!((hi - lo) / US_PER_DAY, 365); // 2026 非闰年
        assert_eq!(
            PartitionScheme::from_partition_name("month=2026-09"),
            Some(PartitionScheme::Month)
        );
        assert_eq!(PartitionScheme::from_partition_name("other"), None);
    }
}
