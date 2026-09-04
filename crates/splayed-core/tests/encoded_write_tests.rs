//! 编码/压缩字段直接写入测试：`create_field_with_data_encoded` 与
//! `create_table_with_options` / `update_table_with_options`（稀疏/NULL-heavy
//! 因子列的"一次写入、多次读取"路径）。

use std::fs;
use std::path::PathBuf;

use splayed_core::{
    CreateFieldError, FieldReader, UpdateError, create_meta, create_table_with_options,
    update_table, update_table_with_options,
};
use splayed_format::{Compression, DataType, Encoding, RawValue, TimeType};

fn temp_dir(suffix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splayed_encoded_{}_{suffix}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn syms_of(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

/// 重复同一 SYM N 次（与 times 对齐）。
fn syms_repeat(sym: &str, n: usize) -> Vec<String> {
    vec![sym.to_string(); n]
}

/// RawValue 列表 → LE 字节（按元素大小）。
fn native_column(ty: DataType, vals: &[RawValue]) -> Vec<u8> {
    let w = ty.size_of();
    let mut out = vec![0u8; vals.len() * w];
    for (i, v) in vals.iter().enumerate() {
        v.write_le(&mut out, i * w);
    }
    out
}

/// 全 NULL 字段（FLOAT64 全 NaN 哨兵）。
fn nulls(ty: DataType, n: usize) -> Vec<u8> {
    let nulls = ty.null_bytes();
    nulls.repeat(n)
}

// ---------------------------------------------------------------------------
// create_field_with_data_encoded：直接写成编码/压缩，读回一致
// ---------------------------------------------------------------------------

#[test]
fn encoded_field_roundtrip_rle_zstd_sparse() {
    let dir = temp_dir("rle_zstd");
    create_meta(&dir, TimeType::Date32, &syms_repeat("SYM01", 6), &[0, 1, 2, 3, 4, 5])
        .unwrap();

    // 稀疏列：只有第 2、5 行有值，其余 NULL（90%~99% NULL 的模拟）。
    let mut vals = nulls(DataType::Float64, 6);
    {
        let w = DataType::Float64.size_of();
        let v = RawValue::from_f64(123.5);
        v.write_le(&mut vals, 2 * w);
        v.write_le(&mut vals, 5 * w);
    }

    let path = dir.join("factor.alpha001");
    splayed_core::create_field_with_data_encoded(
        &path,
        DataType::Float64,
        &vals,
        Encoding::Rle,
        Compression::Zstd,
    )
    .expect("create_field_with_data_encoded");

    // 读回：值一致、NULL 保持。
    let reader = FieldReader::open(&path).unwrap();
    assert_eq!(reader.data_type(), DataType::Float64);
    assert_eq!(reader.row_count(), 6);
    assert_eq!(reader.header().encoding().unwrap(), Encoding::Rle);
    assert_eq!(reader.header().compression().unwrap(), Compression::Zstd);
    assert_eq!(reader.header().null_count, 4, "真实 NULL 计数");
    assert_eq!(
        reader.read_row(2).unwrap().as_f64().unwrap(),
        123.5,
        "非 NULL 值读回"
    );
    assert!(reader.read_row(0).unwrap().is_null(), "NULL 保持");
    assert!(reader.read_row(3).unwrap().is_null(), "NULL 保持");
    assert!(reader.read_row(5).unwrap().as_f64().unwrap() == 123.5);

    // 统计 footer（非 NULL 极值）。
    let st = reader.stats().expect("footer 有效");
    assert_eq!(RawValue::read_le(&st.min, 0, DataType::Float64).as_f64().unwrap(), 123.5);
    assert_eq!(RawValue::read_le(&st.max, 0, DataType::Float64).as_f64().unwrap(), 123.5);
    drop(reader);

    // 压缩字段只读：update_field 拒绝。
    let items = [splayed_core::UpdateItem::new(
        0,
        native_column(DataType::Float64, &[RawValue::from_f64(1.0)]),
    )];
    let err = splayed_core::update_field(&path, &items).unwrap_err();
    assert!(matches!(err, UpdateError::ReadOnlyAfterCompress));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn encoded_field_delta_zstd_roundtrip_dense() {
    let dir = temp_dir("delta_zstd");
    create_meta(&dir, TimeType::Date32, &syms_repeat("SYM01", 8), &[0, 1, 2, 3, 4, 5, 6, 7])
        .unwrap();

    // DELTA 适合等差序列。
    let vals: Vec<i64> = (1000..1008).collect();
    let raw = native_column(
        DataType::Int64,
        &vals.iter().map(|&v| RawValue::from_i64(v)).collect::<Vec<_>>(),
    );

    let path = dir.join("close");
    splayed_core::create_field_with_data_encoded(
        &path,
        DataType::Int64,
        &raw,
        Encoding::Delta,
        Compression::Zstd,
    )
    .expect("encoded field");

    let reader = FieldReader::open(&path).unwrap();
    assert_eq!(reader.header().encoding().unwrap(), Encoding::Delta);
    assert_eq!(reader.header().compression().unwrap(), Compression::Zstd);
    for (i, &v) in vals.iter().enumerate() {
        assert_eq!(reader.read_row(i as u32).unwrap().as_i64().unwrap(), v);
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn encoded_field_plain_none_equals_create_field_with_data() {
    let dir = temp_dir("plain_none");
    create_meta(&dir, TimeType::Date32, &syms_repeat("SYM01", 3), &[0, 1, 2])
        .unwrap();

    let vals = native_column(
        DataType::Float64,
        &[RawValue::from_f64(1.0), RawValue::null(DataType::Float64), RawValue::from_f64(3.0)],
    );
    let a = dir.join("a");
    let b = dir.join("b");
    splayed_core::create_field_with_data_encoded(
        &a,
        DataType::Float64,
        &vals,
        Encoding::Plain,
        Compression::None,
    )
    .unwrap();
    splayed_core::create_field_with_data(&b, DataType::Float64, &vals).unwrap();

    // 字节级一致（PLAIN+NONE 布局无前缀）。
    let ab = fs::read(&a).unwrap();
    let bb = fs::read(&b).unwrap();
    assert_eq!(ab, bb, "PLAIN+NONE 编码写与普通写输出一致");

    // 可写（非压缩）。
    let items = [splayed_core::UpdateItem::new(
        1,
        native_column(DataType::Float64, &[RawValue::from_f64(2.0)]),
    )];
    splayed_core::update_field(&a, &items).expect("PLAIN+NONE 可写");
    assert_eq!(FieldReader::open(&a).unwrap().read_row(1).unwrap().as_f64().unwrap(), 2.0);
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// create_table_with_options / update_table_with_options：表级直接写压缩
// ---------------------------------------------------------------------------

#[test]
fn create_table_with_options_writes_all_fields_compressed() {
    let dir = temp_dir("create_opts");
    let syms = syms_repeat("SYM01", 3);
    let times = [0, 1, 2];
    let col = |name: &str, ty: DataType, vals: Vec<RawValue>| splayed_core::TableColumn {
        name: name.to_string(),
        data_type: ty,
        values: native_column(ty, &vals),
    };
    let columns = vec![
        col("close", DataType::Float64, vec![
            RawValue::from_f64(1.0),
            RawValue::null(DataType::Float64),
            RawValue::from_f64(3.0),
        ]),
        col("factor.alpha001", DataType::Float64, vec![
            RawValue::null(DataType::Float64),
            RawValue::from_f64(2.0),
            RawValue::null(DataType::Float64),
        ]),
    ];

    create_table_with_options(
        &dir,
        TimeType::Date32,
        &syms,
        &times,
        &columns,
        true,
        splayed_core::FieldWriteOptions {
            encoding: Encoding::Rle,
            compression: Compression::Zstd,
        },
    )
    .expect("create_table_with_options");

    let r1 = FieldReader::open(dir.join("close")).unwrap();
    assert_eq!(r1.header().encoding().unwrap(), Encoding::Rle);
    assert_eq!(r1.header().compression().unwrap(), Compression::Zstd);
    assert!(r1.read_row(1).unwrap().is_null());
    assert_eq!(r1.read_row(2).unwrap().as_f64().unwrap(), 3.0);
    drop(r1);

    let r2 = FieldReader::open(dir.join("factor.alpha001")).unwrap();
    assert_eq!(r2.header().encoding().unwrap(), Encoding::Rle);
    assert!(r2.read_row(1).unwrap().as_f64().unwrap() == 2.0);
    drop(r2);

    // 全部只读（写后不可 update）。
    let err = update_table(
        &dir,
        &syms_of(&["SYM01"]),
        &[0],
        &[col("close", DataType::Float64, vec![RawValue::from_f64(9.0)])],
        false,
    );
    assert!(err.is_err(), "压缩字段不可原地更新");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn update_table_with_options_writes_new_field_compressed() {
    let dir = temp_dir("update_opts");
    let syms = syms_repeat("SYM01", 3);
    let times = [0, 1, 2];
    let col = |name: &str, ty: DataType, vals: Vec<RawValue>| splayed_core::TableColumn {
        name: name.to_string(),
        data_type: ty,
        values: native_column(ty, &vals),
    };
    // 先建可写 PLAIN 表（close）。
    create_table_with_options(
        &dir,
        TimeType::Date32,
        &syms,
        &times,
        &[col("close", DataType::Float64, vec![
            RawValue::from_f64(1.0),
            RawValue::from_f64(2.0),
            RawValue::from_f64(3.0),
        ])],
        true,
        splayed_core::FieldWriteOptions::default(),
    )
    .unwrap();

    // 追加稀疏因子列，直接写成 RLE+ZSTD（写后只读）。
    update_table_with_options(
        &dir,
        &syms,
        &times,
        &[col("factor.alpha002", DataType::Float64, vec![
            RawValue::null(DataType::Float64),
            RawValue::from_f64(2.0),
            RawValue::null(DataType::Float64),
        ])],
        true,
        splayed_core::FieldWriteOptions {
            encoding: Encoding::Rle,
            compression: Compression::Zstd,
        },
    )
    .expect("update_table_with_options create_missing");

    // 新字段压缩只读；既有 close 仍 PLAIN 可写。
    let r_new = FieldReader::open(dir.join("factor.alpha002")).unwrap();
    assert_eq!(r_new.header().encoding().unwrap(), Encoding::Rle);
    assert_eq!(r_new.header().compression().unwrap(), Compression::Zstd);
    assert!(r_new.read_row(0).unwrap().is_null());
    assert_eq!(r_new.read_row(1).unwrap().as_f64().unwrap(), 2.0);
    drop(r_new);

    let r_close = FieldReader::open(dir.join("close")).unwrap();
    assert_eq!(r_close.header().compression().unwrap(), Compression::None);
    drop(r_close);

    // 既有列仍可更新（PLAIN 可写）。
    update_table(
        &dir,
        &syms[..1],
        &[1],
        &[col("close", DataType::Float64, vec![RawValue::from_f64(20.0)])],
        false,
    )
    .expect("existing plain field still writable");
    assert_eq!(
        FieldReader::open(dir.join("close")).unwrap().read_row(1).unwrap().as_f64().unwrap(),
        20.0
    );
    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 错误路径：编码/压缩失败
// ---------------------------------------------------------------------------

#[test]
fn create_field_with_data_encoded_rejects_hidden_name() {
    let dir = temp_dir("hidden");
    create_meta(&dir, TimeType::Date32, &syms_repeat("SYM01", 2), &[0, 1]).unwrap();
    let err = splayed_core::create_field_with_data_encoded(
        dir.join(".hidden"),
        DataType::Float64,
        &nulls(DataType::Float64, 2),
        Encoding::Rle,
        Compression::Zstd,
    )
    .expect_err("leading-dot field name rejected");
    assert!(matches!(err, CreateFieldError::HiddenFileName(_)));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn create_field_with_data_encoded_errors_without_meta() {
    // 无 `.meta` 时报 Io(NotFound)。
    let dir = temp_dir("no_meta");
    let err = splayed_core::create_field_with_data_encoded(
        dir.join("close"),
        DataType::Float64,
        &nulls(DataType::Float64, 2),
        Encoding::Plain,
        Compression::None,
    )
    .unwrap_err();
    assert!(matches!(err, CreateFieldError::Io(_)));
    let _ = fs::remove_dir_all(&dir);
}
