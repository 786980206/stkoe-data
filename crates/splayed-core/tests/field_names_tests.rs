//! Field-file naming rules integration tests.
//!
//! 规则：
//! - 字段文件名**可以包含 `.`**（如 `close.bid`、`price.usd`）——正常建表/读取/列出；
//! - **以 `.` 开头**的文件（`.meta` 及任意隐藏/元数据文件）一律不作为字段：
//!   列表时忽略、创建时拒绝；隐藏目录（如 `.cache/.meta`）不作为分区。
//! - 目录里只存在点文件（`.gitkeep`、`.DS_Store`）时，`create_table` 的
//!   "目录必须为空"检查不受阻。

use std::fs;
use std::path::PathBuf;

use splayed_core::{
    create_field_with_data, create_meta, create_table, CreateFieldError, FieldReader,
    PartitionedTable, TableColumn, TableError,
};
use splayed_format::{DataType, RawValue, TimeType};

fn temp_dir(suffix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("splayed_core_fn_{suffix}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn syms_of(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

fn native_column(name: &str, ty: DataType, vals: &[RawValue]) -> TableColumn {
    let elem_sz = ty.size_of();
    let mut values = vec![0u8; vals.len() * elem_sz];
    for (i, v) in vals.iter().enumerate() {
        v.write_le(&mut values, i * elem_sz);
    }
    TableColumn {
        name: name.to_string(),
        data_type: ty,
        values,
    }
}

// ---------------------------------------------------------------------------
// 1. 字段名中间含 `.` 正常支持
// ---------------------------------------------------------------------------

#[test]
fn field_names_with_internal_dots_are_supported() {
    let dir = temp_dir("dots");
    let sym = syms_of(&["A", "A", "B"]);
    let time = vec![0i64, 1, 0];
    let columns = vec![
        native_column(
            "close.bid",
            DataType::Float64,
            &[RawValue::from_f64(1.0), RawValue::from_f64(2.0), RawValue::from_f64(3.0)],
        ),
        native_column(
            "price.usd",
            DataType::Int64,
            &[RawValue::from_i64(10), RawValue::from_i64(20), RawValue::from_i64(30)],
        ),
    ];
    create_table(&dir, TimeType::Date32, &sym, &time, &columns, false).expect("create_table");

    // list_fields 返回点号字段（按名升序）。
    let ds = splayed_core::open_dataset(&dir).expect("open_dataset");
    assert_eq!(
        ds.list_fields().unwrap(),
        vec!["close.bid".to_string(), "price.usd".to_string()]
    );

    // FieldReader 可直接打开点号字段并读值。
    let r = FieldReader::open(dir.join("close.bid")).unwrap();
    assert_eq!(r.row_count(), 3);
    assert_eq!(r.read_row(0).unwrap().as_f64(), Some(1.0));
    assert_eq!(r.read_row(2).unwrap().as_f64(), Some(3.0));

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 2. 以 `.` 开头的文件在列表中被忽略
// ---------------------------------------------------------------------------

#[test]
fn leading_dot_files_are_ignored_by_list_fields() {
    let dir = temp_dir("hidden");
    let sym = syms_of(&["A"]);
    let time = vec![0i64];
    let columns = vec![native_column("close", DataType::Float64, &[RawValue::from_f64(1.0)])];
    create_table(&dir, TimeType::Date32, &sym, &time, &columns, false).unwrap();

    // 塞入各种点文件 + 一个普通文件。
    fs::write(dir.join(".hidden"), b"junk").unwrap();
    fs::write(dir.join(".DS_Store"), b"junk").unwrap();
    fs::write(dir.join("visible.txt"), b"junk").unwrap();

    let ds = splayed_core::open_dataset(&dir).unwrap();
    let fields = ds.list_fields().unwrap();
    // `.meta`、`.hidden`、`.DS_Store` 均忽略；普通文件（含点号在中间）列出。
    assert_eq!(fields, vec!["close".to_string(), "visible.txt".to_string()]);
    assert!(!fields.iter().any(|f| f.starts_with('.')));

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 3. 创建以 `.` 开头的字段被拒绝
// ---------------------------------------------------------------------------

#[test]
fn create_field_with_data_rejects_leading_dot() {
    let dir = temp_dir("reject_fd");
    create_meta(&dir, TimeType::Date32, &syms_of(&["A"]), &[0]).unwrap();

    // 8 字节 = 1 × Float64（total_rows=1）。
    let err = create_field_with_data(dir.join(".hidden"), DataType::Float64, &[0u8; 8])
        .expect_err("create_field_with_data('.hidden') should fail");
    match err {
        CreateFieldError::HiddenFileName(name) => assert_eq!(name, ".hidden"),
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(!dir.join(".hidden").exists());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn create_table_rejects_leading_dot_field() {
    let dir = temp_dir("reject_ct");
    let sym = syms_of(&["A"]);
    let time = vec![0i64];
    let columns = vec![native_column(".hidden", DataType::Float64, &[RawValue::from_f64(1.0)])];
    let err = create_table(&dir, TimeType::Date32, &sym, &time, &columns, false)
        .expect_err("create_table with leading-dot field should fail");
    match err {
        TableError::CreateField(CreateFieldError::HiddenFileName(name)) => assert_eq!(name, ".hidden"),
        other => panic!("unexpected error: {other:?}"),
    }
    // 不应留下一个会被忽略的 `.hidden` 字段文件。
    assert!(!dir.join(".hidden").exists());

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 4. 目录里只有点文件时 create_table 不受"必须为空"约束
// ---------------------------------------------------------------------------

#[test]
fn create_table_ignores_dotfiles_in_dir() {
    let dir = temp_dir("dotfiles");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(".gitkeep"), b"").unwrap();
    fs::write(dir.join(".DS_Store"), b"junk").unwrap();

    let sym = syms_of(&["A"]);
    let time = vec![0i64];
    let columns = vec![native_column("close", DataType::Float64, &[RawValue::from_f64(1.0)])];
    create_table(&dir, TimeType::Date32, &sym, &time, &columns, false)
        .expect("create_table with only dotfiles in dir");
    assert!(dir.join(".meta").exists());
    assert!(dir.join("close").exists());

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 5. 隐藏目录不作为分区
// ---------------------------------------------------------------------------

#[test]
fn hidden_partition_dir_is_ignored() {
    let root = temp_dir("ppart");
    let sym = syms_of(&["A"]);
    let time = vec![0i64];
    let columns = vec![native_column("close", DataType::Float64, &[RawValue::from_f64(1.0)])];

    create_table(root.join("year=2024"), TimeType::Date32, &sym, &time, &columns, false).unwrap();
    // 隐藏目录里也放了 .meta —— 不应被当作分区。
    create_table(root.join(".cache"), TimeType::Date32, &sym, &time, &columns, false).unwrap();

    let table = PartitionedTable::open(&root).unwrap();
    let names: Vec<String> = table.partitions().iter().map(|p| p.name.clone()).collect();
    assert_eq!(names, vec!["year=2024".to_string()]);

    let _ = fs::remove_dir_all(&root);
}
