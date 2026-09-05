//! Field 层集成测试：create / open / read / write / scan / compress / decompress / cast。

use std::path::Path;

use splayed_core::{
    cast_field_file, close_field_handle, compress_field_file, create_field_file,
    decompress_field_file, delete_field_file, open_field_file, rename_field_file, FieldChunkReader,
    FieldHandle, FieldInit, Mode, Predicate, RowRange, Scalar, ScanRequest, StreamValues,
};
use splayed_format::{Bitmap, Buffer, BufferView, Column, ColumnView, DataType};

use std::path::PathBuf;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("splayed_core_field_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

fn f64_column(values: &[f64], validity: Option<Bitmap>) -> Column {
    Column {
        data_type: DataType::Float64,
        values: Buffer::from_slice_copy(values),
        validity,
        dict: None,
    }
}

fn view_values(view: &ColumnView<'_>) -> Vec<f64> {
    let mut out = Vec::new();
    for seg in view.segments() {
        for &v in bytemuck::cast_slice::<u8, f64>(seg.fixed_bytes().unwrap()) {
            out.push(v);
        }
    }
    out
}

/// 流式 reader：按 chunk_rows 切分给定数据（模拟大数据流式写入）。
struct VecReader {
    values: Vec<u8>,
    chunk_rows: usize,
    size: usize,
    pos: usize,
    phase: u8, // 0 = values, 1 = validity, 2 = done
}

impl FieldChunkReader for VecReader {
    fn next_values(&mut self) -> splayed_core::Result<Option<StreamValues>> {
        if self.phase != 0 {
            return Ok(None);
        }
        if self.pos * self.size >= self.values.len() {
            self.phase = 1;
            return Ok(None);
        }
        let rows = self
            .chunk_rows
            .min((self.values.len() / self.size) - self.pos);
        let chunk = StreamValues {
            values: self.values[self.pos * self.size..(self.pos + rows) * self.size].to_vec(),
            rows,
        };
        self.pos += rows;
        Ok(Some(chunk))
    }

    fn next_validity(&mut self) -> splayed_core::Result<Option<Vec<u8>>> {
        if self.phase == 0 {
            self.phase = 1;
        }
        if self.phase == 1 {
            self.phase = 2;
            // 全有效
            let total = self.values.len() / self.size;
            Ok(Some(vec![0xFFu8; (total + 7) / 8]))
        } else {
            Ok(None)
        }
    }
}

#[test]
fn create_length_gives_all_null_field() {
    let dir = temp_dir("length");
    let path = dir.join("price");
    create_field_file(&path, DataType::Float64, FieldInit::Length(10)).unwrap();
    // 重复创建 → AlreadyExists
    assert!(matches!(
        create_field_file(&path, DataType::Float64, FieldInit::Length(10)),
        Err(splayed_core::CoreError::AlreadyExists(_))
    ));

    let handle = open_field_file(&path, Mode::Read).unwrap();
    assert_eq!(handle.row_count(), 10);
    let view = handle.read_field_handle(0, 10).unwrap();
    assert_eq!(view.null_count(), 10);
    assert!(view.string_at(0).is_none());
    close_field_handle(handle).unwrap();

    delete_field_file(&path).unwrap();
    assert!(!path.exists());
    cleanup(&dir);
}

#[test]
fn data_init_and_positional_overwrite() {
    let dir = temp_dir("overwrite");
    let path = dir.join("price");
    let values: Vec<f64> = (0..8).map(|i| i as f64 * 1.5).collect();
    create_field_file(&path, DataType::Float64, FieldInit::Data(f64_column(&values, None))).unwrap();

    let mut handle = open_field_file(&path, Mode::Write).unwrap();
    let view = handle.read_field_handle(0, 8).unwrap();
    assert_eq!(view_values(&view), values);

    // 覆盖写 [2, 5)
    let patch = f64_column(&[100.0, 200.0, 300.0], None);
    handle.write_field_handle(2, &patch.as_view()).unwrap();
    let view = handle.read_field_handle(0, 8).unwrap();
    let got = view_values(&view);
    assert_eq!(&got[2..5], &[100.0, 200.0, 300.0]);
    assert_eq!(got[0], 0.0);
    close_field_handle(handle).unwrap();

    // 重开验证已落盘
    let handle = open_field_file(&path, Mode::Read).unwrap();
    assert_eq!(view_values(&handle.read_field_handle(0, 8).unwrap())[3], 200.0);
    close_field_handle(handle).unwrap();
    cleanup(&dir);
}

#[test]
fn validity_roundtrip_and_null_write() {
    let dir = temp_dir("validity");
    let path = dir.join("price");
    let values: Vec<f64> = [1.0, 0.0, 3.0, 0.0, 5.0].to_vec();
    let mut bits = Bitmap::ones(5);
    bits.set(1, false);
    bits.set(3, false);
    create_field_file(
        &path,
        DataType::Float64,
        FieldInit::Data(Column {
            data_type: DataType::Float64,
            values: Buffer::from_slice_copy(&values),
            validity: Some(bits),
            dict: None,
        }),
    )
    .unwrap();

    let handle = open_field_file(&path, Mode::Read).unwrap();
    let view = handle.read_field_handle(0, 5).unwrap();
    assert_eq!(view.null_count(), 2);
    assert!(!view.segments()[0].validity().unwrap().is_valid(1));
    close_field_handle(handle).unwrap();
    cleanup(&dir);
}

#[test]
fn stream_init_matches_data_init() {
    let dir = temp_dir("stream");
    let values: Vec<f64> = (0..100).map(|i| i as f64).collect();
    let stream_path = dir.join("stream");
    create_field_file(
        &stream_path,
        DataType::Float64,
        FieldInit::Stream {
            reader: Box::new(VecReader {
                values: bytemuck::cast_slice::<f64, u8>(&values).to_vec(),
                chunk_rows: 7,
                size: 8,
                pos: 0,
                phase: 0,
            }),
        },
    )
    .unwrap();
    let data_path = dir.join("data");
    create_field_file(&data_path, DataType::Float64, FieldInit::Data(f64_column(&values, None))).unwrap();

    let a = open_field_file(&stream_path, Mode::Read).unwrap();
    let b = open_field_file(&data_path, Mode::Read).unwrap();
    assert_eq!(view_values(&a.read_field_handle(0, 100).unwrap()), values);
    assert_eq!(view_values(&b.read_field_handle(0, 100).unwrap()), values);
    assert_eq!(a.row_count(), 100);
    close_field_handle(a).unwrap();
    close_field_handle(b).unwrap();
    cleanup(&dir);
}

#[test]
fn scan_with_predicate_and_limit() {
    let dir = temp_dir("scan");
    let path = dir.join("price");
    let values: Vec<f64> = [10.0, 55.0, 20.0, 60.0, 5.0, 70.0].to_vec();
    create_field_file(&path, DataType::Float64, FieldInit::Data(f64_column(&values, None))).unwrap();
    let handle = open_field_file(&path, Mode::Read).unwrap();

    // 无谓词：全表
    let mut scanner = handle.scan_field_handle(&ScanRequest::default()).unwrap();
    let mut ranges = Vec::new();
    while let Some(r) = scanner.next().unwrap() {
        ranges.push(r);
    }
    scanner.close().unwrap();
    assert_eq!(ranges, vec![RowRange::new(0, 6)]);

    // 谓词 price > 50 → 行 1, 3, 5
    let pred = Predicate::value_cmp(splayed_core::CmpOp::Gt, Scalar::Float(50.0));
    let mut scanner = handle.scan_field_handle(&ScanRequest {
        ranges: vec![],
        projection: vec![],
        predicate: Some(pred),
        limit: None,
    })
    .unwrap();
    let mut rows = Vec::new();
    while let Some(r) = scanner.next().unwrap() {
        for i in 0..r.length {
            rows.push(r.offset + i);
        }
    }
    scanner.close().unwrap();
    assert_eq!(rows, vec![1, 3, 5]);

    // limit = 2：只保留前两个命中行
    let pred = Predicate::value_cmp(splayed_core::CmpOp::Gt, Scalar::Float(50.0));
    let mut scanner = handle.scan_field_handle(&ScanRequest {
        ranges: vec![],
        projection: vec![],
        predicate: Some(pred),
        limit: Some(2),
    })
    .unwrap();
    let mut count = 0usize;
    while let Some(r) = scanner.next().unwrap() {
        count += r.length as usize;
    }
    scanner.close().unwrap();
    assert_eq!(count, 2);

    // ranges 输入：只在 [3, 6) 内扫描
    let pred = Predicate::value_cmp(splayed_core::CmpOp::Gt, Scalar::Float(50.0));
    let mut scanner = handle
        .scan_field_handle(&ScanRequest {
            ranges: vec![RowRange::new(3, 3)],
            projection: vec![],
            predicate: Some(pred),
            limit: None,
        })
        .unwrap();
    let mut rows = Vec::new();
    while let Some(r) = scanner.next().unwrap() {
        for i in 0..r.length {
            rows.push(r.offset + i);
        }
    }
    scanner.close().unwrap();
    assert_eq!(rows, vec![3, 5]);
    close_field_handle(handle).unwrap();
    cleanup(&dir);
}

#[test]
fn compress_decompress_roundtrip() {
    let dir = temp_dir("compress");
    let path = dir.join("price");
    let values: Vec<f64> = (0..100).map(|i| (i % 17) as f64 * 1.25).collect();
    create_field_file(&path, DataType::Float64, FieldInit::Data(f64_column(&values, None))).unwrap();

    // 对称 offsets [0, 37] → chunks [0,37), [37,100)
    compress_field_file(&path, Some(vec![0, 37])).unwrap();
    let handle = open_field_file(&path, Mode::Read).unwrap();
    assert!(handle.is_chunked());
    assert_eq!(view_values(&handle.read_field_handle(0, 100).unwrap()), values);
    // 跨 chunk 读取（返回多段）
    let view = handle.read_field_handle(30, 20).unwrap();
    assert!(view.segments().len() >= 2);
    close_field_handle(handle).unwrap();

    // 已压缩再次压缩 → 错误
    assert!(compress_field_file(&path, None).is_err());

    // compressed write：修改后 close 自动重压缩写回
    let mut handle = open_field_file(&path, Mode::Write).unwrap();
    let patch = f64_column(&[-1.0, -2.0], None);
    handle.write_field_handle(40, &patch.as_view()).unwrap();
    close_field_handle(handle).unwrap();

    let handle = open_field_file(&path, Mode::Read).unwrap();
    assert!(handle.is_chunked());
    let got = view_values(&handle.read_field_handle(0, 100).unwrap());
    assert_eq!(&got[40..42], &[-1.0, -2.0]);
    let mut expected = values.clone();
    expected[40] = -1.0;
    expected[41] = -2.0;
    assert_eq!(got, expected);
    close_field_handle(handle).unwrap();

    // 解压恢复 uncompressed
    decompress_field_file(&path).unwrap();
    let handle = open_field_file(&path, Mode::Read).unwrap();
    assert!(!handle.is_chunked());
    assert_eq!(view_values(&handle.read_field_handle(0, 100).unwrap()), expected);
    // 未压缩再解压 → 错误
    assert!(decompress_field_file(&path).is_err());
    close_field_handle(handle).unwrap();
    cleanup(&dir);
}

#[test]
fn cast_converts_values_in_place() {
    let dir = temp_dir("cast");
    let path = dir.join("price");
    let values: Vec<f64> = [1.0, 2.5, -3.0].to_vec();
    create_field_file(&path, DataType::Float64, FieldInit::Data(f64_column(&values, None))).unwrap();

    cast_field_file(&path, DataType::Int64).unwrap();
    let handle = open_field_file(&path, Mode::Read).unwrap();
    assert_eq!(handle.data_type(), DataType::Int64);
    let view = handle.read_field_handle(0, 3).unwrap();
    let got: Vec<i64> = view
        .segments()
        .iter()
        .flat_map(|s| bytemuck::cast_slice::<u8, i64>(s.fixed_bytes().unwrap()).to_vec())
        .collect();
    assert_eq!(got, vec![1, 2, -3]);
    close_field_handle(handle).unwrap();

    // 同类型 cast = no-op
    cast_field_file(&path, DataType::Int64).unwrap();
    cleanup(&dir);
}

#[test]
fn rename_field_file_moves_atomically() {
    let dir = temp_dir("rename");
    let path = dir.join("old");
    create_field_file(&path, DataType::Int32, FieldInit::Length(4)).unwrap();
    rename_field_file(&path, "new").unwrap();
    assert!(!path.exists());
    assert!(dir.join("new").exists());
    // 目标已存在 → Error
    create_field_file(&path, DataType::Int32, FieldInit::Length(4)).unwrap();
    assert!(rename_field_file(&path, "new").is_err());
    cleanup(&dir);
}

#[test]
fn read_out_of_bounds_rejected() {
    let dir = temp_dir("bounds");
    let path = dir.join("price");
    create_field_file(&path, DataType::Float64, FieldInit::Length(5)).unwrap();
    let handle = open_field_file(&path, Mode::Read).unwrap();
    assert!(handle.read_field_handle(3, 3).is_err());
    assert!(handle.read_field_handle(5, 1).is_err());
    assert!(handle.read_field_handle(0, 0).unwrap().is_empty());
    close_field_handle(handle).unwrap();

    // read mode handle 上写入 → InvalidState
    let handle = open_field_file(&path, Mode::Read).unwrap();
    let col = Column::zeroed(DataType::Float64, 1, false);
    let mut handle = handle;
    assert!(matches!(
        handle.write_field_handle(0, &col.as_view()),
        Err(splayed_core::CoreError::InvalidState(_))
    ));
    close_field_handle(handle).unwrap();
    cleanup(&dir);
}
