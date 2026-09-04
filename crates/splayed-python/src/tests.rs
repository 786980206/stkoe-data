//! splayed-python 绑定测试：经 pyo3 内嵌 Python（auto-initialize）构造
//! `pyarrow.RecordBatch`，再调用绑定函数，验证写/读/分区/子集/异常全链路。

use arrow::array::Array;
use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::pyarrow::PyArrowType;
use arrow_array::Date32Array;
use arrow_array::RecordBatch;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyModule};

use crate::{
    PartitionWriteInput, SubsetInput, create_partitioned_table, create_subset, create_table,
    read_subset, scan_dataset, scan_partitioned, splayed, update_table,
};

/// 内嵌解释器初始化前确保 `PYTHONHOME` 指向真实 Python 前缀。scoop 的 `current`
/// junction 会让 `Py_Initialize` 找不到 stdlib `encodings`；首次调用时用 PATH 上的
/// `python` 探测 `sys.prefix` 写入 `PYTHONHOME`（仅当未设置时）。
fn init_python() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("PYTHONHOME").is_none() {
            if let Ok(out) = std::process::Command::new("python")
                .args(["-c", "import sys; print(sys.prefix)"])
                .output()
            {
                if let Ok(prefix) = String::from_utf8(out.stdout) {
                    let prefix = prefix.trim();
                    if !prefix.is_empty() {
                        std::env::set_var("PYTHONHOME", prefix);
                    }
                }
            }
        }
    });
}

/// 运行一段 Python 代码，返回其中 `t`（pyarrow.Table）的首个 RecordBatch。
fn build_rb(py: Python<'_>, code: &str) -> PyArrowType<RecordBatch> {
    let g = py.import("__main__").unwrap().dict();
    let c = std::ffi::CString::new(code).unwrap();
    py.run(&c, None, Some(&g)).unwrap();
    let t = g.get_item("t").unwrap().unwrap();
    let rb = t.call_method0("to_batches").unwrap().get_item(0).unwrap();
    rb.extract().unwrap()
}

fn temp_dir(suffix: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "splayed_py_{suffix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// 2 SYM × 2 天，含 NULL（vol）。代码里必须绑定到 `t`。
const BATCH_CODE: &str = r#"
import pyarrow as pa
t = pa.table({
    "time": pa.array([0, 1, 0, 1], type=pa.date32()),
    "sym": pa.array(["SYM01", "SYM01", "SYM02", "SYM02"]),
    "close": pa.array([100.0, 101.0, 200.0, 201.0]),
    "vol": pa.array([1, None, 4, 5], type=pa.int64()),
})
"#;

fn first_batch(out: Bound<'_, PyAny>) -> RecordBatch {
    let list = out.cast::<PyList>().unwrap();
    let first: PyArrowType<RecordBatch> = list.get_item(0).unwrap().extract().unwrap();
    first.0
}

#[test]
fn create_table_then_scan_dataset() {
    init_python();
    Python::attach(|py| {
        let dir = temp_dir("scan").display().to_string();
        let rb = build_rb(py, BATCH_CODE);
        create_table(dir.clone(), rb, true).unwrap();

        let out = scan_dataset(dir.clone(), None, None, None, 65536, 1, None, py).unwrap();
        let rb = first_batch(out);
        assert_eq!(rb.num_rows(), 4);

        let schema = rb.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["time", "sym", "close", "vol"]);

        let close = rb.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(close.values().to_vec(), vec![100.0, 101.0, 200.0, 201.0]);
        let vol = rb.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(vol.is_null(1));
        assert_eq!(vol.value(0), 1);

        std::fs::remove_dir_all(&dir).ok();
    });
}

#[test]
fn scan_dataset_selection_and_limit() {
    init_python();
    Python::attach(|py| {
        let dir = temp_dir("sel").display().to_string();
        let rb = build_rb(py, BATCH_CODE);
        create_table(dir.clone(), rb, true).unwrap();

        // 符号选择 + 列裁剪 → 只回 SYM02 的 close。
        let out = scan_dataset(
            dir.clone(),
            Some(vec!["close".to_string()]),
            Some(vec!["SYM02".to_string()]),
            None,
            65536,
            1,
            None,
            py,
        )
        .unwrap();
        let rb = first_batch(out);
        assert_eq!(rb.num_rows(), 2);
        assert_eq!(
            rb.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "SYM02"
        );

        // 时间范围 [0,1) → 只回 time=0 两行；limit=1 → 只回 1 行。
        let out = scan_dataset(
            dir.clone(),
            None,
            None,
            Some((0, 1)),
            65536,
            1,
            Some(1),
            py,
        )
        .unwrap();
        let rb = first_batch(out);
        assert_eq!(rb.num_rows(), 1);

        std::fs::remove_dir_all(&dir).ok();
    });
}

#[test]
fn update_table_adds_missing_field() {
    init_python();
    Python::attach(|py| {
        let dir = temp_dir("upd").display().to_string();
        let rb = build_rb(py, BATCH_CODE);
        create_table(dir.clone(), rb, true).unwrap();

        // 追加 flag 列（新字段）。
        let code = r#"
import pyarrow as pa
t = pa.table({
    "time": pa.array([0, 1, 0, 1], type=pa.date32()),
    "sym": pa.array(["SYM01", "SYM01", "SYM02", "SYM02"]),
    "flag": pa.array([True, False, True, True]),
})
"#;
        let rb2 = build_rb(py, code);
        update_table(dir.clone(), rb2, true).unwrap();

        // 全字段扫描确认新增列可读。
        let out = scan_dataset(dir.clone(), None, None, None, 65536, 1, None, py).unwrap();
        let rb = first_batch(out);
        let flag = rb.column(3).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(flag.value(0));
        assert!(!flag.value(1));

        std::fs::remove_dir_all(&dir).ok();
    });
}

#[test]
fn subset_create_and_read() {
    init_python();
    Python::attach(|py| {
        let dir = temp_dir("sub").display().to_string();
        let rb = build_rb(py, BATCH_CODE);
        create_table(dir.clone(), rb, true).unwrap();

        // 子集：SYM01 两天 + SYM02 只取 day0。
        create_subset(
            dir.clone(),
            "hs300".to_string(),
            vec![
                SubsetInput::new("SYM01".to_string(), vec![(0i64, 2u32)]),
                SubsetInput::new("SYM02".to_string(), vec![(0i64, 1u32)]),
            ],
        )
        .unwrap();

        let out = read_subset(dir.clone(), "hs300".to_string(), None, py).unwrap();
        let rb: PyArrowType<RecordBatch> = out.extract().unwrap();
        let rb = rb.0;
        assert_eq!(rb.num_rows(), 3); // 父全局行序：SYM01(0,1) + SYM02(0)
        let close = rb.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(close.values().to_vec(), vec![100.0, 101.0, 200.0]);
        let time = rb.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(time.value(2), 0);

        std::fs::remove_dir_all(&dir).ok();
    });
}

#[test]
fn partition_table_roundtrip() {
    init_python();
    Python::attach(|py| {
        let root = temp_dir("part").display().to_string();
        let rb1 = build_rb(py, BATCH_CODE); // SYM01/02 @ day 0,1
        // 第二个分区：SYM01/02 @ day 10,11。
        let code2 = r#"
import pyarrow as pa
t = pa.table({
    "time": pa.array([10, 11, 10, 11], type=pa.date32()),
    "sym": pa.array(["SYM01", "SYM01", "SYM02", "SYM02"]),
    "close": pa.array([500.0, 501.0, 600.0, 601.0]),
    "vol": pa.array([1, 2, 3, 4], type=pa.int64()),
})
"#;
        let rb2 = build_rb(py, code2);

        create_partitioned_table(
            root.clone(),
            "date32".to_string(),
            vec![
                PartitionWriteInput::new("p1".to_string(), rb1, true),
                PartitionWriteInput::new("p2".to_string(), rb2, true),
            ],
            "plain",
            "none",
        )
        .unwrap();

        let out = scan_partitioned(root.clone(), None, None, None, 65536, 1, py).unwrap();
        // 每分区产出一个 batch（未合并）；跨 batch 汇总。
        let list = out.cast::<PyList>().unwrap();
        let mut total = 0usize;
        let mut has_500 = false;
        for i in 0..list.len() {
            let b: PyArrowType<RecordBatch> = list.get_item(i).unwrap().extract().unwrap();
            let b = b.0;
            total += b.num_rows();
            if let Some(arr) = b.column(2).as_any().downcast_ref::<Float64Array>() {
                has_500 |= arr.values().contains(&500.0);
            }
        }
        assert_eq!(total, 8); // p1(4行) + p2(4行)
        assert!(has_500); // p2 的 close 500.0 在返回数据里

        std::fs::remove_dir_all(&root).ok();
    });
}

#[test]
fn module_registers_and_raises_splayed_error() {
    init_python();
    Python::attach(|py| {
        let m = PyModule::new(py, "splayed").unwrap();
        splayed(&m).unwrap();

        // 模块对象暴露所有函数与类。
        for name in [
            "create_table", "scan_dataset", "read_subset", "create_partitioned_table",
            "PartitionWriteInput", "SubsetInput", "SplayedError",
        ] {
            assert!(m.getattr(name).is_ok(), "missing {name}");
        }

        // 通过 Python 调用：建表成功。
        let rb_obj = {
            let g = py.import("__main__").unwrap().dict();
            let c = std::ffi::CString::new(BATCH_CODE).unwrap();
            py.run(&c, None, Some(&g)).unwrap();
            let t = g.get_item("t").unwrap().unwrap();
            let rb = t.call_method0("to_batches").unwrap().get_item(0).unwrap();
            rb.unbind()
        };
        let dir = temp_dir("mod").display().to_string();
        m.getattr("create_table")
            .unwrap()
            .call1((dir.clone(), rb_obj, true))
            .unwrap();

        // 错误路径：不存在目录 → 抛 SplayedError。
        let err = m
            .getattr("scan_dataset")
            .unwrap()
            .call1((dir.clone() + "/nonexistent",))
            .unwrap_err();
        assert!(err.is_instance_of::<crate::SplayedError>(py));

        std::fs::remove_dir_all(&dir).ok();
    });
}
