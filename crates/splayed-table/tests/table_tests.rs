//! Table 层集成测试：create（分区切分）/ open / query / write / 结构操作 / rename。

use std::path::{Path, PathBuf};

use splayed_format::{Bitmap, Buffer, Column, Data, DataType, FieldSchema, Schema};
use splayed_core::{CmpOp, Mode, Predicate, Scalar};
use splayed_table::{
    create_table, create_table_columns, create_table_partition, delete_table, delete_table_partition,
    open_table, query_table, rename_table, scan_table, write_table, PartitionScheme, TableOptions,
    TableScanRequest,
};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("splayed_core_table_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// 取 price 列（f64）全部行值。
fn f64s(view: &splayed_format::ColumnView<'_>) -> Vec<f64> {
    view.segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, f64>(s.fixed_bytes().unwrap()).to_vec())
        .collect()
}

/// 取 i32 列全部行值（跨段拼接）。
fn i32s(view: &splayed_format::ColumnView<'_>) -> Vec<i32> {
    view.segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, i32>(s.fixed_bytes().unwrap()).to_vec())
        .collect()
}

/// 取 i64 列全部行值（跨段拼接）。
fn i64s(view: &splayed_format::ColumnView<'_>) -> Vec<i64> {
    view.segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, i64>(s.fixed_bytes().unwrap()).to_vec())
        .collect()
}

/// 全局行号 → 有效性（跨段定位段内行号；无 validity = 全有效）。
fn is_valid_at(view: &splayed_format::ColumnView<'_>, row: usize) -> bool {
    let mut pos = row;
    for s in view.segments() {
        if pos < s.rows() {
            return s.validity().map_or(true, |v| v.is_valid(pos));
        }
        pos -= s.rows();
    }
    false
}

/// 构造 (sym, time, price) 数据。time 用天序号（Date32），便于按月分区断言。
/// rows: (sym, day, price)，调用方保证 (sym, day) 有序唯一。
fn make_data(rows: &[(&str, i32, f64)]) -> Data {
    let syms: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let times: Vec<i32> = rows.iter().map(|r| r.1).collect();
    let prices: Vec<f64> = rows.iter().map(|r| r.2).collect();

    let mut dict: Vec<&str> = syms.clone();
    dict.sort();
    dict.dedup();
    let mut offsets = vec![0u64];
    let mut strings = Vec::new();
    for s in &dict {
        strings.extend_from_slice(s.as_bytes());
        offsets.push(strings.len() as u64);
    }
    let keys: Vec<u32> = syms
        .iter()
        .map(|s| dict.iter().position(|x| x == s).unwrap() as u32)
        .collect();
    let sym_col = Column::from_dict(keys, offsets, strings, None);
    let time_col = Column {
        data_type: DataType::Date32,
        values: Buffer::from_slice_copy(&times),
        validity: None,
        dict: None,
    };
    let price_col = Column {
        data_type: DataType::Float64,
        values: Buffer::from_slice_copy(&prices),
        validity: None,
        dict: None,
    };
    Data::new(
        Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", DataType::Date32),
            FieldSchema::new("price", DataType::Float64),
        ]),
        vec![sym_col, time_col, price_col],
    )
    .unwrap()
}

/// 2026-08 与 2026-09 各两行（两个 sym），days 相对 epoch。
fn month_sample() -> Data {
    let d = |y: i32, m: u32, dd: u32| splayed_table::days_from_civil(y as i64, m, dd) as i32;
    make_data(&[
        ("AAPL", d(2026, 8, 3), 10.0),
        ("AAPL", d(2026, 8, 4), 11.0),
        ("MSFT", d(2026, 8, 3), 20.0),
        ("MSFT", d(2026, 8, 4), 21.0),
        ("AAPL", d(2026, 9, 1), 12.0),
        ("AAPL", d(2026, 9, 2), 13.0),
        ("MSFT", d(2026, 9, 1), 22.0),
        ("MSFT", d(2026, 9, 2), 23.0),
    ])
}

#[test]
fn month_table_create_query() {
    let dir = temp_dir("month");
    let root = dir.join("tbl");
    create_table(&root, month_sample(), PartitionScheme::Month, TableOptions::default()).unwrap();
    // 分区目录
    assert!(root.join("month=2026-08").exists());
    assert!(root.join("month=2026-09").exists());

    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();
    assert_eq!(table.scheme(), PartitionScheme::Month);

    // metadata：分区边界
    let meta = table.read_table_metadata().unwrap();
    assert_eq!(meta.partitions.len(), 2);
    assert_eq!(meta.partitions[0].name, "month=2026-08");
    assert_eq!(meta.ordering, vec!["sym", "time"]);
    assert!(meta.capabilities.limit_pushdown);

    // statistics：row_count 求和
    let stats = table.read_table_statistics().unwrap();
    assert_eq!(stats.row_count, 8);
    assert_eq!(stats.partition_count, 2);
    assert_eq!(stats.sym_min.as_deref(), Some("AAPL"));
    assert_eq!(stats.sym_max.as_deref(), Some("MSFT"));

    // schema：最后一个 Partition（09）的 schema
    let schema = table.read_table_schema().unwrap();
    assert_eq!(schema.data_type_of("price"), Some(DataType::Float64));

    // query：全表（Partition ASC + sym/time ASC）
    let req = TableScanRequest::default();
    let mut reader = query_table(&table, req, None).unwrap();
    let mut all_rows: Vec<(String, f64)> = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        for i in 0..view.length() {
            let sym = view.column("sym").unwrap().string_at(i).unwrap().to_owned();
            let price = f64s(view.column("price").unwrap())[i];
            all_rows.push((sym, price));
        }
    }
    reader.close().unwrap();
    assert_eq!(all_rows.len(), 8);
    assert_eq!(all_rows[0], ("AAPL".into(), 10.0));
    assert_eq!(all_rows[2], ("MSFT".into(), 20.0));
    assert_eq!(all_rows[3], ("MSFT".into(), 21.0));
    assert_eq!(all_rows[4], ("AAPL".into(), 12.0)); // 09 分区第一个 sym 是 AAPL

    // time 条件：只命中 09 分区（pruning）
    let sep1 = splayed_table::days_from_civil(2026, 9, 1) as i64;
    let oct1 = splayed_table::days_from_civil(2026, 10, 1) as i64;
    let req = TableScanRequest {
        time: Some((sep1, oct1)),
        ..Default::default()
    };
    let mut reader = query_table(&table, req, Some(3)).unwrap();
    let mut count = 0usize;
    while let Some(view) = reader.next().unwrap() {
        count += view.length();
    }
    reader.close().unwrap();
    assert_eq!(count, 4);

    // sym 条件
    let req = TableScanRequest {
        sym: Some("MSFT".into()),
        ..Default::default()
    };
    let mut reader = query_table(&table, req, None).unwrap();
    let mut syms = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        for i in 0..view.length() {
            syms.push(view.column("sym").unwrap().string_at(i).unwrap().to_owned());
        }
    }
    reader.close().unwrap();
    assert_eq!(syms, vec!["MSFT", "MSFT", "MSFT", "MSFT"]);
    drop(table);
    cleanup(&dir);
}

#[test]
fn table_write_overwrites_existing_rows() {
    let dir = temp_dir("write");
    let root = dir.join("tbl");
    create_table(&root, month_sample(), PartitionScheme::Month, TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Write, TableOptions::default()).unwrap();

    // 覆盖 08-03 的 AAPL/MSFT（输入按 (sym,time) 有序唯一）
    let d803 = splayed_table::days_from_civil(2026, 8, 3) as i32;
    let patch = make_data(&[("AAPL", d803, 111.0), ("MSFT", d803, 222.0)]);
    write_table(&table, &patch.as_view()).unwrap();

    let req = TableScanRequest::default();
    let mut reader = query_table(&table, req, None).unwrap();
    let mut prices = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        prices.extend(f64s(view.column("price").unwrap()));
    }
    reader.close().unwrap();
    assert_eq!(prices, vec![111.0, 11.0, 222.0, 21.0, 12.0, 13.0, 22.0, 23.0]);
    drop(table);

    // 覆盖不存在的行（时间不在数据集）→ Error：key 不存在
    let table = open_table(&root, Mode::Write, TableOptions::default()).unwrap();
    let d899 = splayed_table::days_from_civil(2026, 8, 30) as i32;
    let bad = make_data(&[("AAPL", d899, 1.0)]);
    assert!(write_table(&table, &bad.as_view()).is_err());
    // 不存在的分区 → Error
    let d915 = splayed_table::days_from_civil(2026, 9, 15) as i32;
    let other_part = make_data(&[("AAPL", d915, 1.0)]);
    assert!(write_table(&table, &other_part.as_view()).is_err());
    // 无序输入 → Error
    let unordered = make_data(&[
        ("MSFT", d803, 5.0),
        ("AAPL", d803, 6.0),
    ]);
    assert!(write_table(&table, &unordered.as_view()).is_err());
    drop(table);
    cleanup(&dir);
}

#[test]
fn none_scheme_table() {
    let dir = temp_dir("none");
    let root = dir.join("tbl");
    // none 方案：整个输入必须 (sym ASC, time ASC)
    let d803 = splayed_table::days_from_civil(2026, 8, 3) as i32;
    let d804 = splayed_table::days_from_civil(2026, 8, 4) as i32;
    let sorted = make_data(&[
        ("AAPL", d803, 10.0),
        ("AAPL", d804, 11.0),
        ("MSFT", d803, 20.0),
        ("MSFT", d804, 21.0),
    ]);
    create_table(&root, sorted, PartitionScheme::None, TableOptions::default()).unwrap();
    assert!(root.join(".meta").exists());
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();
    assert_eq!(table.scheme(), PartitionScheme::None);
    let req = TableScanRequest::default();
    let mut reader = query_table(&table, req, None).unwrap();
    let mut count = 0;
    while let Some(view) = reader.next().unwrap() {
        count += view.length();
    }
    reader.close().unwrap();
    assert_eq!(count, 4);
    drop(table);
    cleanup(&dir);
}

#[test]
fn partition_and_table_lifecycle() {
    let dir = temp_dir("lifecycle");
    let root = dir.join("tbl");
    // 先建 08 分区
    let d803 = splayed_table::days_from_civil(2026, 8, 3) as i32;
    let partial = make_data(&[("AAPL", d803, 10.0)]);
    create_table(&root, partial, PartitionScheme::Month, TableOptions::default()).unwrap();

    // create_table_partition：新增 09 分区（scheme 从已有分区推断）
    let sep_sample = make_data(&[("AAPL", splayed_table::days_from_civil(2026, 9, 1) as i32, 12.0)]);
    create_table_partition(&root, "month=2026-09", sep_sample, splayed_core::CreateDatasetOptions::default()).unwrap();
    // 冲突 scheme → Error
    let sep_sample2 = make_data(&[("AAPL", splayed_table::days_from_civil(2026, 9, 2) as i32, 13.0)]);
    assert!(create_table_partition(&root, "year=2026", sep_sample2, splayed_core::CreateDatasetOptions::default()).is_err());
    // 已存在 → Error
    let dup = make_data(&[("AAPL", splayed_table::days_from_civil(2026, 9, 2) as i32, 13.0)]);
    assert!(create_table_partition(&root, "month=2026-09", dup, splayed_core::CreateDatasetOptions::default()).is_err());

    // rename_table
    rename_table(&root, "tbl_v2").unwrap();
    let root2 = dir.join("tbl_v2");
    assert!(root2.join("month=2026-08").exists());

    let table = open_table(&root2, Mode::Read, TableOptions::default()).unwrap();
    let stats = table.read_table_statistics().unwrap();
    assert_eq!(stats.row_count, 2);
    assert_eq!(stats.partition_count, 2);
    drop(table);

    // delete_table_partition
    delete_table_partition(&root2, "month=2026-09").unwrap();
    assert!(!root2.join("month=2026-09").exists());

    // delete_table
    delete_table(&root2).unwrap();
    assert!(!root2.exists());
    cleanup(&dir);
}

#[test]
fn empty_table_scheme_cannot_be_inferred() {
    let dir = temp_dir("empty");
    let root = dir.join("tbl");
    std::fs::create_dir_all(&root).unwrap();
    assert!(open_table(&root, Mode::Read, TableOptions::default()).is_err());
    cleanup(&dir);
}

#[test]
fn validity_survives_partition_split() {
    let dir = temp_dir("validity");
    let root = dir.join("tbl");
    // price 含 NULL：第二行
    let rows: [(&str, i32, f64); 4] = [
        ("AAPL", splayed_table::days_from_civil(2026, 8, 3) as i32, 10.0),
        ("AAPL", splayed_table::days_from_civil(2026, 8, 4) as i32, 0.0), // NULL
        ("AAPL", splayed_table::days_from_civil(2026, 9, 1) as i32, 12.0),
        ("MSFT", splayed_table::days_from_civil(2026, 8, 3) as i32, 20.0),
    ];
    let mut bits = Bitmap::ones(4);
    bits.set(1, false);
    let data = {
        let mut d = make_data(&[
            ("AAPL", rows[0].1, 10.0),
            ("AAPL", rows[1].1, 0.0),
            ("AAPL", rows[2].1, 12.0),
            ("MSFT", rows[3].1, 20.0),
        ]);
        // 把 price 列的 validity 换成含 NULL 的
        let price_idx = d.schema.position("price").unwrap();
        d.columns[price_idx].validity = Some(bits);
        d
    };
    create_table(&root, data, PartitionScheme::Month, TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();
    let req = TableScanRequest::default();
    let mut reader = query_table(&table, req, None).unwrap();
    let mut nulls = 0usize;
    while let Some(view) = reader.next().unwrap() {
        nulls += view.column("price").unwrap().null_count();
    }
    reader.close().unwrap();
    assert_eq!(nulls, 1);
    drop(table);
    cleanup(&dir);
}

#[test]
fn table_field_structure_operations() {
    let dir = temp_dir("fieldops");
    let root = dir.join("tbl");
    create_table(&root, month_sample(), PartitionScheme::Month, TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Write, TableOptions::default()).unwrap();

    // create：全 NULL
    table.create_table_field("volume", DataType::Int64, splayed_table::TableFieldInit::AllNull).unwrap();
    // 验证各分区物理文件大小均为 64B
    let meta = table.read_table_metadata().unwrap();
    for p in meta.partitions {
        assert_eq!(std::fs::metadata(root.join(&p.name).join("volume")).unwrap().len(), 64);
    }
    let schema = table.read_table_schema().unwrap();
    assert_eq!(schema.data_type_of("volume"), Some(DataType::Int64));
    // 重复创建 → Error
    assert!(table.create_table_field("volume", DataType::Int64, splayed_table::TableFieldInit::AllNull).is_err());

    // update：header（volume 保持 Int64，行数由 core 强制）
    // update 需要合法 FieldHeader：用 core 的 new_uncompressed 构造
    let header = splayed_format::FieldHeader::new_uncompressed(DataType::Int64, 0, 0, 0, false);
    table.update_table_field("volume", header).unwrap();

    // rename
    table.rename_table_field("volume", "vol").unwrap();
    let schema = table.read_table_schema().unwrap();
    assert!(schema.data_type_of("volume").is_none());
    assert_eq!(schema.data_type_of("vol"), Some(DataType::Int64));

    // cast：price f64 → i64 → f64
    table.cast_table_field("price", DataType::Int64).unwrap();
    let schema = table.read_table_schema().unwrap();
    assert_eq!(schema.data_type_of("price"), Some(DataType::Int64));
    table.cast_table_field("price", DataType::Float64).unwrap();

    // compress / decompress
    table.compress_table_field("price").unwrap();
    {
        let req = TableScanRequest::default();
        let mut reader = splayed_table::query_table(&table, req, None).unwrap();
        let mut prices = Vec::new();
        while let Some(view) = reader.next().unwrap() {
            prices.extend(f64s(view.column("price").unwrap()));
        }
        reader.close().unwrap();
        assert_eq!(
            prices,
            vec![10.0, 11.0, 20.0, 21.0, 12.0, 13.0, 22.0, 23.0]
        );
    }
    table.decompress_table_field("price").unwrap();

    // delete
    table.delete_table_field("vol").unwrap();
    assert!(table.read_table_schema().unwrap().data_type_of("vol").is_none());

    // 前置校验：缺字段 → Error
    assert!(table.rename_table_field("vol", "vol2").is_err());
    assert!(table.delete_table_field("vol").is_err());

    table.close().unwrap();
    cleanup(&dir);
}

/// 3 sym × 2 月交错数据；price 两个 NULL（源行 1/7）、side（Int32）一个 NULL（源行 4）。
/// 行序满足 (sym ASC, time ASC)：sym 外层、月内层。返回 (Data, 源行集)。
fn parallel_opts_sample() -> (Data, Vec<(String, i32, Option<f64>, Option<i32>)>) {
    let d = |m: u32, dd: u32| splayed_table::days_from_civil(2026, m, dd) as i32;
    let syms = ["AAPL", "GOOG", "MSFT"];
    let mut sym_keys: Vec<u32> = Vec::new();
    let mut times: Vec<i32> = Vec::new();
    let mut prices: Vec<f64> = Vec::new();
    let mut side_vals: Vec<i32> = Vec::new();
    let mut rows: Vec<(String, i32, Option<f64>, Option<i32>)> = Vec::new();
    for (si, sym) in syms.iter().enumerate() {
        for k in 0..6usize {
            let t = d(if k < 3 { 8 } else { 9 }, (k % 3 + 1) as u32);
            let (price, side) = ((si * 10 + k) as f64, (k % 2) as i32);
            sym_keys.push(si as u32);
            times.push(t);
            prices.push(price);
            side_vals.push(side);
            rows.push((sym.to_string(), t, Some(price), Some(side)));
        }
    }
    let mut price_bits = vec![0xFFu8; (prices.len() + 7) / 8];
    price_bits[0] &= !(1 << 1); // 源行 1 NULL
    price_bits[0] &= !(1 << 7); // 源行 7 NULL
    let mut side_bits = vec![0xFFu8; (side_vals.len() + 7) / 8];
    side_bits[0] &= !(1 << 4); // 源行 4 NULL
    for (i, r) in rows.iter_mut().enumerate() {
        if i == 1 || i == 7 {
            r.2 = None;
        }
        if i == 4 {
            r.3 = None;
        }
    }
    let mut sym_offsets = vec![0u64];
    let mut sym_strings: Vec<u8> = Vec::new();
    for s in &syms {
        sym_strings.extend_from_slice(s.as_bytes());
        sym_offsets.push(sym_strings.len() as u64);
    }
    let data = Data::new(
        Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", DataType::Date32),
            FieldSchema::new("price", DataType::Float64),
            FieldSchema::new("side", DataType::Int32),
        ]),
        vec![
            Column::from_dict(sym_keys, sym_offsets, sym_strings, None),
            Column { data_type: DataType::Date32, values: Buffer::from_slice_copy(&times), validity: None, dict: None },
            Column { data_type: DataType::Float64, values: Buffer::from_slice_copy(&prices), validity: Some(Bitmap::from_bytes(price_bits, prices.len())), dict: None },
            Column { data_type: DataType::Int32, values: Buffer::from_slice_copy(&side_vals), validity: Some(Bitmap::from_bytes(side_bits, side_vals.len())), dict: None },
        ],
    )
    .unwrap();
    (data, rows)
}

/// 并行选项等价性：max_parallelism = 1 与 8 产出内容一致的 Table
/// （分区 gather 批量拼接 + 预算切分并行的正确性回归）。
#[test]
fn create_table_parallel_options_equivalent() {
    let dir = temp_dir("create_par_opts");
    let read_rows = |root: &Path| -> Vec<(String, i32, Option<f64>, Option<i32>)> {
        let table = open_table(root, Mode::Read, TableOptions::default()).unwrap();
        let req = TableScanRequest::default();
        let mut reader = query_table(&table, req, None).unwrap();
        let mut out = Vec::new();
        while let Some(view) = reader.next().unwrap() {
            let prices_all = f64s(view.column("price").unwrap());
            let times_all = i32s(view.column("time").unwrap());
            let side_all = i32s(view.column("side").unwrap());
            let price_col = view.column("price").unwrap();
            let side_col = view.column("side").unwrap();
            for i in 0..view.length() {
                let sym = view.column("sym").unwrap().string_at(i).unwrap().to_string();
                let price_ok = is_valid_at(price_col, i);
                let side_ok = is_valid_at(side_col, i);
                out.push((
                    sym,
                    times_all[i],
                    if price_ok { Some(prices_all[i]) } else { None },
                    if side_ok { Some(side_all[i]) } else { None },
                ));
            }
        }
        reader.close().unwrap();
        out
    };

    let (data1, expected) = parallel_opts_sample();
    let (data2, _) = parallel_opts_sample();
    let root1 = dir.join("serial");
    create_table(&root1, data1, PartitionScheme::Month, TableOptions { max_parallelism: Some(1), ..Default::default() }).unwrap();
    let root2 = dir.join("par");
    create_table(&root2, data2, PartitionScheme::Month, TableOptions { max_parallelism: Some(8), ..Default::default() }).unwrap();

    let rows1 = read_rows(&root1);
    let rows2 = read_rows(&root2);
    // 期望输出序：分区名 ASC（2026-08 → 2026-09），分区内保持源行序
    let sep_start = splayed_table::days_from_civil(2026, 9, 1) as i32;
    let mut expected_out: Vec<_> = expected.iter().filter(|r| r.1 < sep_start).cloned().collect();
    expected_out.extend(expected.iter().filter(|r| r.1 >= sep_start).cloned());
    assert_eq!(rows1.len(), 18);
    assert_eq!(rows1, expected_out, "serial (max_parallelism=1) 内容/行序/NULL 不符");
    assert_eq!(rows1, rows2, "并行 (max_parallelism=8) 与串行结果不一致");
    cleanup(&dir);
}

/// 空数据（0 行）：仅创建根目录并返回 Ok，不创建任何分区。
#[test]
fn create_table_empty_data_creates_root() {
    let dir = temp_dir("create_empty");
    let root = dir.join("tbl");
    let data = Data::new(
        Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", DataType::Date32),
        ]),
        vec![
            Column::from_dict(Vec::new(), vec![0u64, 0], Vec::new(), None),
            Column { data_type: DataType::Date32, values: Buffer::from_vec(Vec::new()), validity: None, dict: None },
        ],
    )
    .unwrap();
    create_table(&root, data, PartitionScheme::Month, TableOptions::default()).unwrap();
    assert!(root.is_dir());
    assert!(std::fs::read_dir(&root).unwrap().next().is_none()); // 无任何子项
    cleanup(&dir);
}

/// create_table_partition / delete_table_partition 校验语义（主线程轻量校验 + 委托）。
#[test]
fn partition_entry_validation() {
    let dir = temp_dir("part_validate");
    let root = dir.join("tbl");

    // create：Table 根不存在 → NotFound（不隐式引导建表）
    let d803 = splayed_table::days_from_civil(2026, 8, 3) as i32;
    let sample = make_data(&[("AAPL", d803, 10.0)]);
    assert!(matches!(
        create_table_partition(&root, "month=2026-08", sample.clone(), splayed_core::CreateDatasetOptions::default()),
        Err(splayed_core::CoreError::NotFound(_))
    ));

    // create：分区名不符合 scheme → Invalid
    std::fs::create_dir_all(&root).unwrap();
    assert!(create_table_partition(&root, "not-a-partition", sample.clone(), splayed_core::CreateDatasetOptions::default()).is_err());

    // create：缺 sym/time 列 → Invalid（在任何盘上落痕之前失败）
    let bare = Data::new(
        Schema::new(vec![FieldSchema::new("price", DataType::Float64)]),
        vec![Column { data_type: DataType::Float64, values: Buffer::from_slice_copy(&[1.0]), validity: None, dict: None }],
    )
    .unwrap();
    assert!(create_table_partition(&root, "month=2026-08", bare, splayed_core::CreateDatasetOptions::default()).is_err());

    // create：空数据 → Invalid（禁止空 Dataset）
    let empty = Data::new(
        Schema::new(vec![
            FieldSchema::new("sym", DataType::Utf8),
            FieldSchema::new("time", DataType::Date32),
        ]),
        vec![
            Column::from_dict(Vec::new(), vec![0u64, 0], Vec::new(), None),
            Column { data_type: DataType::Date32, values: Buffer::from_vec(Vec::new()), validity: None, dict: None },
        ],
    )
    .unwrap();
    assert!(create_table_partition(&root, "month=2026-08", empty, splayed_core::CreateDatasetOptions::default()).is_err());

    // delete：非 scheme 命名子目录拒绝删除（防误删任意目录）
    std::fs::create_dir_all(root.join("random_dir")).unwrap();
    assert!(delete_table_partition(&root, "random_dir").is_err());
    assert!(root.join("random_dir").exists()); // 未被误删

    // delete：分区不存在 → NotFound；Table 根不存在 → NotFound
    assert!(matches!(
        delete_table_partition(&root, "month=2026-08"),
        Err(splayed_core::CoreError::NotFound(_))
    ));
    assert!(matches!(
        delete_table_partition(&dir.join("nope"), "month=2026-08"),
        Err(splayed_core::CoreError::NotFound(_))
    ));

    // 委托 happy path：create + delete 全链路
    create_table_partition(&root, "month=2026-08", sample, splayed_core::CreateDatasetOptions::default()).unwrap();
    assert!(root.join("month=2026-08").is_dir());
    delete_table_partition(&root, "month=2026-08").unwrap();
    assert!(!root.join("month=2026-08").exists());
    cleanup(&dir);
}

/// Field 结构操作统一并发模型：分区级并行（max_parallelism）+ 严格前置校验。
#[test]
fn table_field_struct_ops_parallel_and_state_checks() {
    let dir = temp_dir("struct_par");
    let root = dir.join("tbl");
    // 3 分区 × 2 sym × 2 行；sym 外层保证 (sym ASC, time ASC) 契约
    let d = |m: u32, dd: u32| splayed_table::days_from_civil(2026, m, dd) as i32;
    let mut rows: Vec<(&str, i32, f64)> = Vec::new();
    for (i, sym) in ["AAPL", "MSFT"].iter().enumerate() {
        for m in [7u32, 8, 9] {
            for k in 0..2usize {
                rows.push((sym, d(m, (k + 1) as u32), (i * 100 + m as usize * 2 + k) as f64));
            }
        }
    }
    create_table(&root, make_data(&rows), PartitionScheme::Month, TableOptions { max_parallelism: Some(8), ..Default::default() }).unwrap();

    let read_prices = |root: &Path| -> Vec<f64> {
        let table = open_table(root, Mode::Read, TableOptions::default()).unwrap();
        let req = TableScanRequest::default();
        let mut reader = query_table(&table, req, None).unwrap();
        let mut out = Vec::new();
        while let Some(view) = reader.next().unwrap() {
            out.extend(f64s(view.column("price").unwrap()));
        }
        reader.close().unwrap();
        out
    };
    let before = read_prices(&root);
    assert_eq!(before.len(), 12);

    let table = open_table(&root, Mode::Write, TableOptions { max_parallelism: Some(8), ..Default::default() }).unwrap();
    // create：并行全分区新增（全 NULL = 稀疏 set_len，非逐行写入）
    table.create_table_field("volume", DataType::Int64, splayed_table::TableFieldInit::AllNull).unwrap();
    assert_eq!(table.read_table_schema().unwrap().data_type_of("volume"), Some(DataType::Int64));
    assert!(table.create_table_field("volume", DataType::Int64, splayed_table::TableFieldInit::AllNull).is_err()); // 已存在 → Invalid
    assert!(table.delete_table_field("nope").is_err()); // 缺字段 → Invalid

    // compress / decompress：并行重操作；64B header 状态前置校验
    table.compress_table_field("price").unwrap();
    assert!(table.compress_table_field("price").is_err()); // 已压缩 → Invalid
    assert!(table.decompress_table_field("volume").is_err()); // 未压缩 → Invalid
    table.decompress_table_field("price").unwrap();
    assert!(table.decompress_table_field("price").is_err()); // 已解压 → Invalid

    // cast：f64 → f32 → f64 往返（小整数值精确）；跨分区类型一致才允许
    table.cast_table_field("price", DataType::Float32).unwrap();
    table.cast_table_field("price", DataType::Float64).unwrap();

    // rename：保留名冲突 → Invalid
    assert!(table.rename_table_field("price", "sym").is_err());
    table.rename_table_field("volume", "qty").unwrap();
    table.delete_table_field("qty").unwrap();
    assert_eq!(table.read_table_schema().unwrap().data_type_of("qty"), None);
    table.close().unwrap();

    // 全部重操作往返后 price 数据不变（分区级并行正确性）
    assert_eq!(read_prices(&root), before);

    // delete_table / rename_table 显式 NotFound
    assert!(matches!(
        delete_table(&dir.join("nope")),
        Err(splayed_core::CoreError::NotFound(_))
    ));
    assert!(matches!(
        rename_table(&dir.join("nope"), "x"),
        Err(splayed_core::CoreError::NotFound(_))
    ));
    cleanup(&dir);
}

/// 统一缓存模型：元数据读 API 的正确性与缓存新鲜度。
#[test]
fn metadata_read_apis_cache_semantics() {
    let dir = temp_dir("meta_cache");
    let root = dir.join("tbl");
    let d = |m: u32, dd: u32| splayed_table::days_from_civil(2026, m, dd) as i32;
    let mut rows: Vec<(&str, i32, f64)> = Vec::new();
    for (i, sym) in ["AAPL", "MSFT"].iter().enumerate() {
        for m in [7u32, 8] {
            for k in 0..2usize {
                rows.push((sym, d(m, (k + 1) as u32), (i * 100 + m as usize * 2 + k) as f64));
            }
        }
    }
    create_table(&root, make_data(&rows), PartitionScheme::Month, TableOptions::default()).unwrap();

    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();

    // statistics：row_count 求和 + **实际数据**的 time 界（time_min 聚合 bug 回归：
    // 不得为 default 的 0）+ sym 界
    let st = table.read_table_statistics().unwrap();
    assert_eq!(st.partition_count, 2);
    assert_eq!(st.row_count, 8);
    assert_eq!(st.time_min, d(7, 1) as i64);
    assert_eq!(st.time_max, d(8, 2) as i64);
    assert_eq!(st.sym_min.as_deref(), Some("AAPL"));
    assert_eq!(st.sym_max.as_deref(), Some("MSFT"));

    // 缓存路径：第二次调用结果一致（逐分区 memo 命中）
    assert_eq!(table.read_table_statistics().unwrap(), st);

    // metadata：分区名升序 + 分区范围由名字纯推导（零 I/O）
    let md = table.read_table_metadata().unwrap();
    assert_eq!(md.partitions.len(), 2);
    assert_eq!(md.partitions[0].name, "month=2026-07");
    assert_eq!(md.partitions[1].name, "month=2026-08");
    assert_eq!(md.partitions[0].time_min, d(7, 1) as i64);
    assert_eq!(md.partitions[1].time_max, d(9, 1) as i64 - 1);

    // 缓存新鲜度（关键）：handle 打开期间**外部**新建分区 → 统计/元数据自动纳入
    let sep = make_data(&[("AAPL", d(9, 1), 1.0), ("MSFT", d(9, 2), 2.0)]);
    create_table_partition(&root, "month=2026-09", sep, splayed_core::CreateDatasetOptions::default()).unwrap();
    let st2 = table.read_table_statistics().unwrap();
    assert_eq!(st2.partition_count, 3);
    assert_eq!(st2.row_count, 10);
    assert_eq!(st2.time_max, d(9, 2) as i64);
    let md2 = table.read_table_metadata().unwrap();
    assert_eq!(md2.partitions.len(), 3);
    assert_eq!(md2.partitions[2].name, "month=2026-09");

    // 外部删除分区 → 自动剔除
    delete_table_partition(&root, "month=2026-07").unwrap();
    let st3 = table.read_table_statistics().unwrap();
    assert_eq!(st3.partition_count, 2);
    assert_eq!(st3.row_count, 6);
    assert_eq!(st3.time_min, d(8, 1) as i64);

    // schema：仍为最后分区（month=2026-09）的 Schema
    let schema = table.read_table_schema().unwrap();
    assert_eq!(schema.data_type_of("price"), Some(DataType::Float64));
    cleanup(&dir);
}

/// scan_table 惰性扫描：分区裁剪 + 边界分区精确过滤 + limit 精确不超发 + 顺序。
#[test]
fn scan_table_lazy_prune_limit_and_boundary() {
    let dir = temp_dir("scan_lazy");
    let root = dir.join("tbl");
    let d = |m: u32, dd: u32| splayed_table::days_from_civil(2026, m, dd) as i32;
    let mut rows: Vec<(&str, i32, f64)> = Vec::new();
    for (i, sym) in ["AAPL", "MSFT"].iter().enumerate() {
        for m in [7u32, 8, 9] {
            for k in 0..2usize {
                rows.push((sym, d(m, (k + 1) as u32), (i * 100 + m as usize * 2 + k) as f64));
            }
        }
    }
    create_table(&root, make_data(&rows), PartitionScheme::Month, TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();

    let collect = |req: TableScanRequest| -> (Vec<String>, usize) {
        let mut scanner = scan_table(&table, req).unwrap();
        let mut parts: Vec<String> = Vec::new();
        let mut total = 0usize;
        while let Some(prr) = scanner.next().unwrap() {
            if parts.last() != Some(&prr.partition) {
                parts.push(prr.partition.clone());
            }
            total += prr.row_range.length as usize;
        }
        scanner.close().unwrap();
        (parts, total)
    };

    // 边界分区时间精确过滤：窗口 [7-2, 9-1) → 07 被选中但 7-1 的行由 Dataset 内
    // 残差 time 条件滤掉（完整谓词下传，裁剪不剥时间条件）；09 被 prune
    let req = TableScanRequest { time: Some((d(7, 2) as i64, d(9, 1) as i64)), ..Default::default() };
    let (parts, total) = collect(req);
    assert_eq!(parts, vec!["month=2026-07".to_string(), "month=2026-08".to_string()]);
    assert_eq!(total, 6); // 7-2 × 2 行 + 8 月 4 行（7-1 × 2 行被精确过滤）

    // limit 精确不超发：全局剩余下推 + 返回前防御裁剪
    let req = TableScanRequest { limit: Some(5), ..Default::default() };
    let (parts, total) = collect(req);
    assert_eq!(total, 5);
    assert!(parts.len() <= 2); // 07(4 行) + 08 裁剪 1 行，09 不打开语义下不产出

    // 空窗口：立即结束、零产出
    let req = TableScanRequest { time: Some((0, d(7, 1) as i64)), ..Default::default() };
    let (parts, total) = collect(req);
    assert!(parts.is_empty());
    assert_eq!(total, 0);

    // 用户 predicate 携带 time 条件（predicate 路径，不参与裁剪但精确过滤生效）
    let req = TableScanRequest {
        predicate: Some(Predicate::cmp("time", CmpOp::Ge, Scalar::Int(d(9, 2) as i64))),
        ..Default::default()
    };
    let (parts, total) = collect(req);
    assert_eq!(parts, vec!["month=2026-09".to_string()]);
    assert_eq!(total, 2); // 9-2 × 2 行（9-1 × 2 行被滤掉）

    // 无条件：全部分区按名升序
    let (parts, total) = collect(TableScanRequest::default());
    assert_eq!(
        parts,
        vec![
            "month=2026-07".to_string(),
            "month=2026-08".to_string(),
            "month=2026-09".to_string()
        ]
    );
    assert_eq!(total, 12);
    cleanup(&dir);
}

/// read_table：聚合路径精确不超发（range 跨批次边界 → 截断头 + pending）；
/// 原始路径（None）一 range 一批；Some(0) 拒绝。
#[test]
fn read_table_batch_semantics() {
    let dir = temp_dir("reader_batch");
    let root = dir.join("tbl");
    let d = |m: u32, dd: u32| splayed_table::days_from_civil(2026, m, dd) as i32;
    // 1 sym × 3 月 × 3 行（prices 0..8 按时间序）；分区行数 3 与 batch 4 互质 → 跨界
    let rows: Vec<(&str, i32, f64)> = (0..3usize)
        .flat_map(|m| {
            (0..3usize)
                .map(move |k| ("AAPL", d((7 + m) as u32, (k + 1) as u32), (m * 3 + k) as f64))
        })
        .collect();
    create_table(&root, make_data(&rows), PartitionScheme::Month, TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();

    // 聚合路径：batch = 4 → [4, 4, 1]（最后一批允许小）；跨界批次跨分区多段拼接
    let mut reader = query_table(&table, TableScanRequest::default(), Some(4)).unwrap();
    let mut lens = Vec::new();
    let mut got: Vec<f64> = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        lens.push(view.length());
        let price = view.column("price").unwrap();
        // 跨界批次的列是多段（每段一个来源 range）——段拼接后值序正确
        got.extend(
            price
                .segments()
                .iter()
                .flat_map(|s| bytemuck::cast_slice::<u8, f64>(s.fixed_bytes().unwrap()).to_vec()),
        );
    }
    reader.close().unwrap();
    assert_eq!(lens, vec![4, 4, 1]); // 精确不超发（旧实现会产出 [6, 6]）
    assert_eq!(got, (0..9).map(|i| i as f64).collect::<Vec<_>>());

    // 原始路径（None）：一 range 一批（每分区一个 range = 3 行），共 3 批
    let mut reader = query_table(&table, TableScanRequest::default(), None).unwrap();
    let mut lens = Vec::new();
    let mut got: Vec<f64> = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        lens.push(view.length());
        let price = view.column("price").unwrap();
        got.extend(
            price
                .segments()
                .iter()
                .flat_map(|s| bytemuck::cast_slice::<u8, f64>(s.fixed_bytes().unwrap()).to_vec()),
        );
    }
    reader.close().unwrap();
    assert_eq!(lens, vec![3, 3, 3]);
    assert_eq!(got, (0..9).map(|i| i as f64).collect::<Vec<_>>());

    // Some(0) → Invalid（拒绝，不静默当 1）
    let mut reader = query_table(&table, TableScanRequest::default(), Some(0)).unwrap();
    assert!(reader.next().is_err());
    cleanup(&dir);
}

/// write_table 并行等价性：分区含多个 run（sym 优先导致同名分区输入不相邻）时，
/// max_parallelism = 1 与 8 写入结果一致。
#[test]
fn write_table_parallel_equivalence() {
    let dir = temp_dir("write_par_eq");
    let d = |m: u32, dd: u32| splayed_table::days_from_civil(2026, m, dd) as i32;
    // 3 分区 × 2 sym：分区 07 含 2 个 run（A@7 与 M@7 在输入中不相邻）
    let initial = make_data(&[
        ("AAPL", d(7, 1), 10.0),
        ("AAPL", d(8, 1), 20.0),
        ("AAPL", d(9, 1), 30.0),
        ("MSFT", d(7, 1), 40.0),
        ("MSFT", d(8, 1), 50.0),
        ("MSFT", d(9, 1), 60.0),
    ]);
    let patch_data = make_data(&[
        ("AAPL", d(7, 1), 11.0),
        ("AAPL", d(8, 1), 21.0),
        ("AAPL", d(9, 1), 31.0),
        ("MSFT", d(7, 1), 41.0),
        ("MSFT", d(8, 1), 51.0),
        ("MSFT", d(9, 1), 61.0),
    ]);
    let patch = patch_data.as_view();

    let read_prices = |root: &Path| -> Vec<f64> {
        let table = open_table(root, Mode::Read, TableOptions::default()).unwrap();
        let req = TableScanRequest::default();
        let mut reader = query_table(&table, req, None).unwrap();
        let mut out = Vec::new();
        while let Some(view) = reader.next().unwrap() {
            out.extend(f64s(view.column("price").unwrap()));
        }
        reader.close().unwrap();
        out
    };

    let root1 = dir.join("serial");
    let root2 = dir.join("par");
    for (root, mp) in [(&root1, Some(1usize)), (&root2, Some(8usize))] {
        create_table(root, initial.clone(), PartitionScheme::Month, TableOptions::default()).unwrap();
        let table = open_table(root, Mode::Write, TableOptions { max_parallelism: mp, ..Default::default() }).unwrap();
        write_table(&table, &patch).unwrap();
        table.close().unwrap();
    }
    // 表序 = 分区 ASC × 分区内 sym ASC
    let want: Vec<f64> = vec![11.0, 41.0, 21.0, 51.0, 31.0, 61.0];
    assert_eq!(read_prices(&root1), want);
    assert_eq!(read_prices(&root2), want);
    cleanup(&dir);
}

#[test]
fn table_parallelism_control_and_scan_request() {
    let dir = temp_dir("tbl_par_ctrl");
    let d = |m: u32, dd: u32| splayed_table::days_from_civil(2026, m, dd) as i32;
    let initial = make_data(&[("AAPL", d(7, 1), 10.0), ("MSFT", d(8, 1), 20.0)]);
    let root = dir.join("tbl");
    create_table(&root, initial, PartitionScheme::Month, TableOptions::default()).unwrap();

    let mut table = open_table(&root, Mode::Write, TableOptions { max_parallelism: Some(2), ..Default::default() }).unwrap();
    assert_eq!(table.max_parallelism(), 2);

    // 动态下发更高并发预算
    table.set_max_parallelism(8);
    assert_eq!(table.max_parallelism(), 8);

    // 查询带 max_parallelism
    let req = TableScanRequest {
        max_parallelism: Some(4),
        ..Default::default()
    };
    let mut reader = query_table(&table, req, None).unwrap();
    let mut got = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        got.extend(f64s(view.column("price").unwrap()));
    }
    reader.close().unwrap();
    assert_eq!(got, vec![10.0, 20.0]);

    table.close().unwrap();
    cleanup(&dir);
}

/// 任何写入前完成全部定位与校验：后续分区的 key 缺失 → 整体报错且
/// 前面分区**不产生部分写入**（无事务契约下的最强前置语义）。
#[test]
fn write_table_locate_all_before_write() {
    let dir = temp_dir("write_locate_all");
    let d = |m: u32, dd: u32| splayed_table::days_from_civil(2026, m, dd) as i32;
    let initial = make_data(&[("AAPL", d(7, 1), 10.0), ("MSFT", d(8, 1), 20.0)]);
    let root = dir.join("tbl");
    create_table(&root, initial, PartitionScheme::Month, TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Write, TableOptions::default()).unwrap();

    // 07 的 key 合法、08 的 key（A@8-2）不存在 → 定位阶段整体失败
    let patch_data = make_data(&[("AAPL", d(7, 1), 99.0), ("AAPL", d(8, 2), 99.0)]);
    let patch = patch_data.as_view();
    assert!(write_table(&table, &patch).is_err());

    // 07 的原值未被部分写入覆盖（旧实现在此会写入 99）
    let req = TableScanRequest::default();
    let mut reader = query_table(&table, req, None).unwrap();
    let mut got = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        got.extend(f64s(view.column("price").unwrap()));
    }
    reader.close().unwrap();
    assert_eq!(got, vec![10.0, 20.0]);
    table.close().unwrap();
    cleanup(&dir);
}

/// 创建即压缩贯通 Table 层：create_table 压缩 + create_table_partition 差异化
/// （新分区未压缩、旧分区压缩共存，读路径透明）。
#[test]
fn create_table_compression_and_partition_override() {
    let dir = temp_dir("tbl_compress");
    let root = dir.join("tbl");
    let d = |m: u32, dd: u32| splayed_table::days_from_civil(2026, m, dd) as i32;
    let initial = make_data(&[
        ("AAPL", d(7, 1), 10.0),
        ("AAPL", d(7, 2), 11.0),
        ("MSFT", d(7, 1), 20.0),
        ("MSFT", d(7, 2), 21.0),
    ]);
    create_table(
        &root,
        initial,
        PartitionScheme::Month,
        TableOptions {
            compression: Some(splayed_format::Compression::Zstd),
            chunk_target_rows: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    // 差异化：新分区以默认选项（未压缩）创建
    let aug = make_data(&[("AAPL", d(8, 1), 30.0), ("MSFT", d(8, 1), 40.0)]);
    create_table_partition(&root, "month=2026-08", aug, splayed_core::CreateDatasetOptions::default()).unwrap();

    // 07 分区 chunked（压缩创建）、08 分区未压缩
    let fh07 = splayed_core::open_field_file(&root.join("month=2026-07").join("price"), Mode::Read).unwrap();
    assert!(fh07.is_chunked());
    splayed_core::close_field_handle(fh07).unwrap();
    let fh08 = splayed_core::open_field_file(&root.join("month=2026-08").join("price"), Mode::Read).unwrap();
    assert!(!fh08.is_chunked());
    splayed_core::close_field_handle(fh08).unwrap();

    // 读回跨两种物理表示，值一致
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();
    let req = TableScanRequest::default();
    let mut reader = query_table(&table, req, None).unwrap();
    let mut got = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        got.extend(f64s(view.column("price").unwrap()));
    }
    reader.close().unwrap();
    assert_eq!(got, vec![10.0, 11.0, 20.0, 21.0, 30.0, 40.0]);
    cleanup(&dir);
}

/// 构造带额外列的 Data：sym/time 来自 (sym, day) 对，cols 为 (name, 定宽列) 列表。
/// 调用方保证 (sym, day) 有序唯一；cols 列长度 = rows 长度。
fn build_multi_col_data(rows: &[(&str, i32)], cols: Vec<(&str, Column)>) -> Data {
    let syms: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let times: Vec<i32> = rows.iter().map(|r| r.1).collect();
    let mut dict: Vec<&str> = syms.clone();
    dict.sort();
    dict.dedup();
    let mut offsets = vec![0u64];
    let mut strings = Vec::new();
    for s in &dict {
        strings.extend_from_slice(s.as_bytes());
        offsets.push(strings.len() as u64);
    }
    let keys: Vec<u32> = syms
        .iter()
        .map(|s| dict.iter().position(|x| x == s).unwrap() as u32)
        .collect();
    let sym_col = Column::from_dict(keys, offsets, strings, None);
    let time_col = Column {
        data_type: DataType::Date32,
        values: Buffer::from_slice_copy(&times),
        validity: None,
        dict: None,
    };
    let mut fields = vec![
        FieldSchema::new("sym", DataType::Utf8),
        FieldSchema::new("time", DataType::Date32),
    ];
    let mut columns = vec![sym_col, time_col];
    for (name, col) in cols {
        fields.push(FieldSchema::new(name, col.data_type));
        columns.push(col);
    }
    Data::new(Schema::new(fields), columns).unwrap()
}

/// 定宽列构造：全有效 + 给定值。
fn fixed_col(dt: DataType, bytes: Vec<u8>) -> Column {
    Column {
        data_type: dt,
        values: Buffer::from_vec(bytes),
        validity: None,
        dict: None,
    }
}

/// create_table_columns：子集输入为新缺失列建列，未覆盖行为 NULL，已有列不被改写。
#[test]
fn create_table_columns_creates_missing_columns_subset() {
    let dir = temp_dir("crc_subset");
    let root = dir.join("tbl");
    create_table(&root, month_sample(), PartitionScheme::Month, TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Write, TableOptions::default()).unwrap();

    // 子集输入：只覆盖 08 分区的 AAPL/MSFT 各一行（08-03）；新增 volume(i64)/qty(i32)
    let d803 = splayed_table::days_from_civil(2026, 8, 3) as i32;
    let i64v: Vec<i64> = vec![111, 222];
    let i32v: Vec<i32> = vec![11, 22];
    let data = build_multi_col_data(
        &[("AAPL", d803), ("MSFT", d803)],
        vec![
            ("volume", fixed_col(DataType::Int64, bytemuck::cast_slice(&i64v).to_vec())),
            ("qty", fixed_col(DataType::Int32, bytemuck::cast_slice(&i32v).to_vec())),
        ],
    );
    create_table_columns(&table, &data.as_view()).unwrap();

    // schema 含新列且类型正确
    let schema = table.read_table_schema().unwrap();
    assert_eq!(schema.data_type_of("volume"), Some(DataType::Int64));
    assert_eq!(schema.data_type_of("qty"), Some(DataType::Int32));
    assert_eq!(schema.data_type_of("price"), Some(DataType::Float64));

    // 08 分区：volume/qty 覆盖行有值，未覆盖行为 NULL；price 未被改写
    let req = TableScanRequest {
        time: Some((d803 as i64, (d803 + 1) as i64)),
        ..Default::default()
    };
    let mut reader = query_table(&table, req, None).unwrap();
    let mut vol = Vec::new();
    let mut qty = Vec::new();
    let mut price = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        vol.extend(i64s(view.column("volume").unwrap()));
        qty.extend(i32s(view.column("qty").unwrap()));
        price.extend(f64s(view.column("price").unwrap()));
    }
    reader.close().unwrap();
    assert_eq!(vol, vec![111, 222]);
    assert_eq!(qty, vec![11, 22]);
    assert_eq!(price, vec![10.0, 20.0]); // price 未被动

    // 09 分区：volume/qty 已建且全 NULL（无输入行覆盖）
    // 验证 09 分区的 volume 与 qty 文件为 header-only 64 字节
    assert_eq!(std::fs::metadata(root.join("month=2026-09").join("volume")).unwrap().len(), 64);
    assert_eq!(std::fs::metadata(root.join("month=2026-09").join("qty")).unwrap().len(), 64);
    let d901 = splayed_table::days_from_civil(2026, 9, 1) as i32;
    let req = TableScanRequest {
        time: Some((d901 as i64, (d901 + 1) as i64)),
        ..Default::default()
    };
    let mut reader = query_table(&table, req, None).unwrap();
    while let Some(view) = reader.next().unwrap() {
        let v = view.column("volume").unwrap();
        for i in 0..view.length() {
            assert!(!is_valid_at(v, i), "volume row {i} should be NULL");
        }
    }
    reader.close().unwrap();
    drop(table);
    cleanup(&dir);
}

/// create_table_columns：已有列同类型 → 不报错且不覆盖；新列正常创建。
#[test]
fn create_table_columns_skips_existing_same_type() {
    let dir = temp_dir("crc_skip");
    let root = dir.join("tbl");
    create_table(&root, month_sample(), PartitionScheme::Month, TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Write, TableOptions::default()).unwrap();

    // 输入含已有 price(Float64) + 新 volume(Int64)，覆盖 08-04 两行
    let d804 = splayed_table::days_from_civil(2026, 8, 4) as i32;
    let pr: Vec<f64> = vec![999.0, 888.0];
    let vv: Vec<i64> = vec![1, 2];
    let data = build_multi_col_data(
        &[("AAPL", d804), ("MSFT", d804)],
        vec![
            ("price", fixed_col(DataType::Float64, bytemuck::cast_slice(&pr).to_vec())),
            ("volume", fixed_col(DataType::Int64, bytemuck::cast_slice(&vv).to_vec())),
        ],
    );
    create_table_columns(&table, &data.as_view()).unwrap();

    // price 未被改写（仍是 11.0 / 21.0），volume 正确创建
    let req = TableScanRequest::default();
    let mut reader = query_table(&table, req, None).unwrap();
    let mut price = Vec::new();
    let mut volume = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        price.extend(f64s(view.column("price").unwrap()));
        if let Some(v) = view.column("volume") {
            volume.extend(i64s(v));
        }
    }
    reader.close().unwrap();
    assert_eq!(price, vec![10.0, 11.0, 20.0, 21.0, 12.0, 13.0, 22.0, 23.0]);
    // volume 已在各分区创建：08-04 覆盖行有值（1,2），其余行 NULL
    assert_eq!(volume, vec![0, 1, 0, 2, 0, 0, 0, 0]);
    drop(table);
    cleanup(&dir);
}

/// create_table_columns：已有列类型冲突 → Invalid，且无任何建列残留。
#[test]
fn create_table_columns_refuses_conflicting_type() {
    let dir = temp_dir("crc_type");
    let root = dir.join("tbl");
    create_table(&root, month_sample(), PartitionScheme::Month, TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Write, TableOptions::default()).unwrap();

    // 输入把已有 price 声明为 Int64（类型冲突）→ 应前置报错
    let d803 = splayed_table::days_from_civil(2026, 8, 3) as i32;
    let iv: Vec<i64> = vec![1, 2];
    let data = build_multi_col_data(
        &[("AAPL", d803), ("MSFT", d803)],
        vec![("price", fixed_col(DataType::Int64, bytemuck::cast_slice(&iv).to_vec()))],
    );
    let err = create_table_columns(&table, &data.as_view()).unwrap_err();
    assert!(matches!(err, splayed_core::CoreError::Invalid(_)));

    // 无残留：schema 仍是 price(Float64)，无新增列
    let schema = table.read_table_schema().unwrap();
    assert_eq!(schema.data_type_of("price"), Some(DataType::Float64));
    assert_eq!(schema.data_type_of("volume"), None);
    drop(table);
    cleanup(&dir);
}

#[test]
fn table_query_batch_sizes_and_limit_boundaries() {
    let dir = temp_dir("batch_boundaries");
    let root = dir.join("tbl");
    // month_sample() 包含 2 个分区（2026-08 4 行，2026-09 4 行，共 8 行）
    create_table(&root, month_sample(), PartitionScheme::Month, TableOptions::default()).unwrap();
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();

    // 1. batch_size = Some(1)：每次迭代返回恰好 1 行
    let req = TableScanRequest::default();
    let mut reader = query_table(&table, req, Some(1)).unwrap();
    let mut count = 0;
    while let Some(view) = reader.next().unwrap() {
        assert_eq!(view.length(), 1);
        count += 1;
    }
    reader.close().unwrap();
    assert_eq!(count, 8);

    // 2. batch_size = Some(3)：8 行分为 3, 3, 2
    let req = TableScanRequest::default();
    let mut reader = query_table(&table, req, Some(3)).unwrap();
    let mut lengths = Vec::new();
    while let Some(view) = reader.next().unwrap() {
        lengths.push(view.length());
    }
    reader.close().unwrap();
    assert_eq!(lengths, vec![3, 3, 2]);

    // 3. batch_size = Some(100)（超出总行数）：聚合所有行至单批次或按分区批次（不可超发）
    let req = TableScanRequest::default();
    let mut reader = query_table(&table, req, Some(100)).unwrap();
    let mut total_rows = 0;
    while let Some(view) = reader.next().unwrap() {
        total_rows += view.length();
    }
    reader.close().unwrap();
    assert_eq!(total_rows, 8);

    // 4. limit 跨分区截断：limit = 5（分区 1 取 4 行，分区 2 取 1 行）
    let req = TableScanRequest {
        limit: Some(5),
        ..Default::default()
    };
    let mut reader = query_table(&table, req, None).unwrap();
    let mut total_limit = 0;
    while let Some(view) = reader.next().unwrap() {
        total_limit += view.length();
    }
    reader.close().unwrap();
    assert_eq!(total_limit, 5);

    drop(table);
    cleanup(&dir);
}

#[test]
fn table_partition_delete_and_rescan() {
    let dir = temp_dir("part_del");
    let root = dir.join("tbl");
    create_table(&root, month_sample(), PartitionScheme::Month, TableOptions::default()).unwrap();

    // 删除 2026-08 分区
    delete_table_partition(&root, "month=2026-08").unwrap();
    assert!(!root.join("month=2026-08").exists());
    assert!(root.join("month=2026-09").exists());

    // 重新打开表并扫描，此时只剩下 2026-09 分区的 4 行
    let table = open_table(&root, Mode::Read, TableOptions::default()).unwrap();
    let meta = table.read_table_metadata().unwrap();
    let partitions: Vec<String> = meta.partitions.iter().map(|p| p.name.clone()).collect();
    assert_eq!(partitions, vec!["month=2026-09"]);

    let req = TableScanRequest::default();
    let mut scanner = scan_table(&table, req).unwrap();
    let mut rows = 0;
    while let Some(prr) = scanner.next().unwrap() {
        assert_eq!(prr.partition, "month=2026-09");
        rows += prr.row_range.length;
    }
    scanner.close().unwrap();
    assert_eq!(rows, 4);

    drop(table);
    cleanup(&dir);
}
