//! Table 层集成测试：create（分区切分）/ open / query / write / 结构操作 / rename。

use std::path::{Path, PathBuf};

use splayed_format::{Bitmap, Buffer, Column, Data, DataType, FieldSchema, Schema};
use splayed_core::Mode;
use splayed_table::{
    create_table, create_table_partition, delete_table, delete_table_partition, open_table,
    query_table, rename_table, write_table, PartitionScheme, TableOptions, TableScanRequest,
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
    create_table_partition(&root, "month=2026-09", sep_sample).unwrap();
    // 冲突 scheme → Error
    let sep_sample2 = make_data(&[("AAPL", splayed_table::days_from_civil(2026, 9, 2) as i32, 13.0)]);
    assert!(create_table_partition(&root, "year=2026", sep_sample2).is_err());
    // 已存在 → Error
    let dup = make_data(&[("AAPL", splayed_table::days_from_civil(2026, 9, 2) as i32, 13.0)]);
    assert!(create_table_partition(&root, "month=2026-09", dup).is_err());

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
    let mut table = open_table(&root, Mode::Write, TableOptions::default()).unwrap();

    // create：全 NULL
    table.create_table_field("volume", DataType::Int64).unwrap();
    let schema = table.read_table_schema().unwrap();
    assert_eq!(schema.data_type_of("volume"), Some(DataType::Int64));
    // 重复创建 → Error
    assert!(table.create_table_field("volume", DataType::Int64).is_err());

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
    create_table(&root1, data1, PartitionScheme::Month, TableOptions { max_parallelism: Some(1) }).unwrap();
    let root2 = dir.join("par");
    create_table(&root2, data2, PartitionScheme::Month, TableOptions { max_parallelism: Some(8) }).unwrap();

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
