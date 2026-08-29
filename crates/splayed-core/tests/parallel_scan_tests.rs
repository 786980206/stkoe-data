//! Parallel scan integration tests: `split_ranges` + `scan_owned_parallel`.
//!
//! The parallel stream must be byte-for-byte equivalent to a serial
//! `scan_owned` scan (same batches, same order), while the ranges are still
//! split into multiple row-balanced groups.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use splayed_core::{
    create_table, open_dataset, scan_owned, scan_owned_parallel, Scanner, ScanRequest, TableColumn,
};
use splayed_format::{DataType, RawValue, TimeType};

/// Global sequence so concurrently-running tests never share a temp dir.
static DIR_SEQ: AtomicUsize = AtomicUsize::new(0);

fn temp_dir(suffix: &str) -> PathBuf {
    let seq = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "splayed_par_{suffix}_{seq}_{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// Build a sparse, multi-symbol dataset (times with gaps → row-count skew).
fn make_dataset() -> PathBuf {
    let dir = temp_dir("par_scan");
    let n = 24usize;
    let mut syms = Vec::with_capacity(n);
    let mut times = Vec::with_capacity(n);
    let mut close = Vec::with_capacity(n);
    // SYM01: 16 rows over times 0,2,4,...,30 (gappy)
    // SYM02: 8 rows over times 0..7
    for t in (0..32).step_by(2) {
        syms.push("SYM01".to_string());
        times.push(t);
        close.push(RawValue::from_f64(100.0 + t as f64));
    }
    for t in 0..8 {
        syms.push("SYM02".to_string());
        times.push(t);
        close.push(RawValue::from_f64(200.0 + t as f64));
    }
    let elem = DataType::Float64.size_of();
    let mut bytes = Vec::with_capacity(n * elem);
    for v in &close {
        let off = bytes.len();
        bytes.resize(off + elem, 0);
        v.write_le(&mut bytes, off);
    }
    let columns = vec![TableColumn {
        name: "close".to_string(),
        data_type: DataType::Float64,
        values: bytes,
    }];
    create_table(&dir, TimeType::Date32, &syms, &times, &columns, false).unwrap();
    dir
}

/// Fully consume a stream into a comparable signature: (sym_idx, time, col bytes).
fn drain(it: &mut splayed_core::OwnedScanBatches) -> Vec<(usize, i64, Vec<u8>)> {
    let mut out = Vec::new();
    while let Some(b) = it.next_batch().unwrap() {
        assert_eq!(b.sym_indices.len(), b.row_count);
        assert_eq!(b.time_values.len(), b.row_count);
        let col = &b.columns[0];
        let elem = col.2.size_of();
        for i in 0..b.row_count {
            let start = i * elem;
            out.push((
                b.sym_indices[i],
                b.time_values[i],
                col.1[start..start + elem].to_vec(),
            ));
        }
    }
    out
}

#[test]
fn split_ranges_are_row_balanced_and_ordered() {
    let dir = make_dataset();
    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);
    let request = ScanRequest::new(vec!["close".to_string()]);
    let plan = scanner.plan(&request).unwrap();

    let groups = splayed_core::split_ranges(&plan, 4);
    assert_eq!(groups.len(), 4);

    let total: usize = plan.ranges.iter().map(|r| r.count as usize).sum();
    let grouped: usize = groups.iter().flatten().map(|r| r.count as usize).sum();
    assert_eq!(total, grouped, "split must cover every row exactly once");

    // Each group is internally ordered (sym asc, time asc).
    for g in &groups {
        let mut prev: Option<(usize, i64)> = None;
        for r in g {
            // Key of the range's first row = (sym index, first time axis value).
            let first = (r.sym_idx, dataset.meta.time_axis[r.time_start_idx as usize]);
            if let Some((ps, pt)) = prev {
                assert!(first.0 >= ps, "group not sym-ordered");
                assert!(first.0 > ps || first.1 >= pt, "group not time-ordered");
            }
            prev = Some(first);
        }
    }

    // Balance: no group should hold more than ~60% of the rows (16-row raw
    // group test would be horrible without splitting).
    let max_share = groups
        .iter()
        .map(|g| g.iter().map(|r| r.count as usize).sum::<usize>() as f64 / total as f64)
        .fold(0.0f64, f64::max);
    assert!(max_share < 0.6, "worst group share {max_share:.2} too high");

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn parallel_stream_matches_serial() {
    let dir = make_dataset();
    let dataset = Arc::new(open_dataset(&dir).unwrap());
    let scanner = Scanner::new(&dataset);
    let request = ScanRequest::new(vec!["close".to_string()]);
    let plan = scanner.plan(&request).unwrap();

    // Serial reference.
    let serial = {
        let mut it = scan_owned(Arc::clone(&dataset), &plan, &request).unwrap();
        drain(&mut it)
    };

    // Parallel (4 threads) must produce the identical ordered stream.
    let parallel = {
        let mut it = scan_owned_parallel(Arc::clone(&dataset), &plan, &request, 4).unwrap();
        let mut out = Vec::new();
        while let Some(b) = it.next_batch().unwrap() {
            for i in 0..b.row_count {
                out.push((b.sym_indices[i], b.time_values[i], b.columns[0].1[i * 8..i * 8 + 8].to_vec()));
            }
        }
        out
    };

    assert_eq!(serial.len(), parallel.len(), "row count differs");
    assert_eq!(serial, parallel, "parallel stream diverges from serial");

    // Sanity: the data itself is correct (SYM02 time 7 → 207.0).
    let last = serial.last().unwrap();
    assert_eq!(last.0, 1 /* SYM02 */);
    assert_eq!(last.1, 7);
    assert_eq!(f64::from_le_bytes(last.2.clone().try_into().unwrap()), 207.0);

    fs::remove_dir_all(&dir).ok();
}

#[test]
fn parallel_scan_respects_filters_and_time_range() {
    let dir = make_dataset();
    let dataset = open_dataset(&dir).unwrap();
    let scanner = Scanner::new(&dataset);

    let mut req = ScanRequest::new(vec!["close".to_string()]);
    req.time_range = splayed_core::TimeRange::new(10, 20); // [10, 20)
    let plan = scanner.plan(&req).unwrap();

    let mut it = scan_owned_parallel(Arc::new(dataset), &plan, &req, 4).unwrap();
    let mut times = Vec::new();
    while let Some(b) = it.next_batch().unwrap() {
        times.extend(b.time_values.iter().copied());
    }
    assert!(!times.is_empty());
    assert!(times.iter().all(|t| (10..20).contains(t)), "time range not respected");

    fs::remove_dir_all(&dir).ok();
}