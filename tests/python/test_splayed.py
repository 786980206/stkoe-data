import os
import shutil
import tempfile
import unittest
import pyarrow as pa
import splayed


def make_sample_batch():
    schema = pa.schema([
        ("sym", pa.dictionary(pa.int32(), pa.utf8())),
        ("time", pa.timestamp("us")),
        ("open", pa.float64()),
        ("close", pa.float64()),
    ])
    sym_indices = pa.array([0, 0, 1, 1], type=pa.int32())
    sym_dict = pa.array(["AAPL", "GOOG"])
    sym_array = pa.DictionaryArray.from_arrays(sym_indices, sym_dict)
    time_array = pa.array([1_000_000, 2_000_000, 1_000_000, 2_000_000], type=pa.timestamp("us"))
    open_array = pa.array([150.0, 152.0, 2800.0, 2810.0], type=pa.float64())
    close_array = pa.array([151.5, 153.2, 2805.5, 2815.0], type=pa.float64())

    return pa.record_batch(
        [sym_array, time_array, open_array, close_array],
        schema=schema,
    )


class TestSplayedPython(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="splayed_test_")

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_table_create_write_read_roundtrip(self):
        tbl_path = os.path.join(self.temp_dir, "roundtrip_table")
        batch = make_sample_batch()

        # 1. 骨架创建
        writer = splayed.TableWriter.create(
            tbl_path,
            batch.schema,
            scheme=splayed.PartitionScheme.NONE,
        )
        self.assertEqual(writer.path(), tbl_path)
        self.assertEqual(writer.scheme(), "none")
        self.assertEqual(writer.statistics()["row_count"], 0)

        # 2. 初始更新
        writer.update(batch)
        stats = writer.statistics()
        self.assertEqual(stats["row_count"], 4)
        self.assertEqual(stats["sym_min"], "AAPL")
        self.assertEqual(stats["sym_max"], "GOOG")

        # 3. 覆盖写（write）
        writer.write(batch)
        self.assertEqual(writer.statistics()["row_count"], 4)

        # 4. 转为 TableReader
        reader = writer.as_reader()
        self.assertEqual(reader.path(), tbl_path)

        # 5. 全量读取为 pyarrow.Table
        tbl = reader.read_all()
        self.assertIsInstance(tbl, pa.Table)
        self.assertEqual(len(tbl), 4)
        self.assertEqual(tbl.num_columns, 4)
        self.assertEqual(set(tbl.column_names), {"sym", "time", "open", "close"})

        # 6. 流式批次读取（batch_size=2）
        stream = reader.read(batch_size=2)
        batches = list(stream)
        self.assertEqual(len(batches), 2)
        for b in batches:
            self.assertIsInstance(b, pa.RecordBatch)
            self.assertEqual(len(b), 2)
        stream.close()

        reader.close()
        writer.remove()
        self.assertFalse(os.path.exists(tbl_path))

    def test_table_init_from_batch_or_table(self):
        tbl_path = os.path.join(self.temp_dir, "init_table")
        batch = make_sample_batch()

        # 使用 TableWriter.init 直接连带数据初始化
        writer = splayed.TableWriter.init(
            tbl_path,
            batch,
            scheme=splayed.PartitionScheme.NONE,
        )
        self.assertEqual(writer.statistics()["row_count"], 4)

        # 传入 pa.Table 进行更新
        table_data = pa.Table.from_batches([batch])
        writer.update(table_data)
        self.assertEqual(writer.statistics()["row_count"], 4)

        reader = splayed.TableReader.open(tbl_path)
        res = reader.read_all(columns=["sym", "close"])
        self.assertEqual(res.column_names, ["sym", "time", "close"])
        self.assertEqual(len(res), 4)

        reader.close()
        writer.remove()

    def test_column_self_heal_and_ddl(self):
        tbl_path = os.path.join(self.temp_dir, "ddl_table")
        batch = make_sample_batch()

        writer = splayed.TableWriter.init(tbl_path, batch)
        self.assertEqual(len(writer.schema()), 4)

        # 1. 字段 DDL: init_field, rename_field, delete_field
        writer.init_field("volume", "int64")
        self.assertIn("volume", writer.schema().names)

        writer.rename_field("volume", "vol")
        self.assertIn("vol", writer.schema().names)
        self.assertNotIn("volume", writer.schema().names)

        writer.delete_field("vol")
        self.assertNotIn("vol", writer.schema().names)

        # 2. write 自动建列自愈补列
        cols = list(batch.columns)
        cols.append(pa.array([10.5, 20.5, 30.5, 40.5], type=pa.float64()))
        new_schema = batch.schema.append(pa.field("factor.momentum", pa.float64()))
        extended_batch = pa.record_batch(cols, schema=new_schema)

        writer.write(extended_batch)
        self.assertIn("factor.momentum", writer.schema().names)

        reader = writer.as_reader()
        df = reader.read_all().to_pandas()
        self.assertIn("factor.momentum", df.columns)
        self.assertEqual(len(df), 4)

        reader.close()
        writer.remove()

    def test_table_partition_scheme_month(self):
        tbl_path = os.path.join(self.temp_dir, "partitioned_table")
        batch = make_sample_batch()

        writer = splayed.TableWriter.create(
            tbl_path,
            batch.schema,
            scheme=splayed.PartitionScheme.MONTH,
        )
        writer.update(batch)
        meta = writer.metadata()
        self.assertEqual(meta["scheme"], "month")
        self.assertGreaterEqual(meta["partition_count"], 1)

        reader = writer.as_reader()
        tbl = reader.read_all()
        self.assertEqual(len(tbl), 4)

        reader.close()
        writer.remove()

    def test_table_fix_clean_dirs_and_heal_missing_columns(self):
        tbl_path = os.path.join(self.temp_dir, "fix_table")
        
        # 1. 创建两个月度分区的数据 (1970-01 与 1970-02)
        # 1970-01-01 -> 1_000_000 us (month 1970-01)
        # 1970-02-01 -> 2_678_400_000_000 us (month 1970-02)
        schema = pa.schema([
            ("sym", pa.dictionary(pa.int32(), pa.utf8())),
            ("time", pa.timestamp("us")),
            ("price", pa.float64()),
        ])
        sym_array = pa.DictionaryArray.from_arrays(
            pa.array([0, 0], type=pa.int32()),
            pa.array(["AAPL"]),
        )
        time_array = pa.array([1_000_000, 2_678_400_000_000], type=pa.timestamp("us"))
        price_array = pa.array([150.0, 155.0], type=pa.float64())
        batch = pa.record_batch([sym_array, time_array, price_array], schema=schema)

        writer = splayed.TableWriter.init(tbl_path, batch, scheme=splayed.PartitionScheme.MONTH)
        meta = writer.metadata()
        self.assertEqual(meta["partition_count"], 2)
        partitions = sorted([p["name"] for p in meta["partitions"]])
        part1_name, part2_name = partitions[0], partitions[1]

        part1_dir = os.path.join(tbl_path, part1_name)
        part2_dir = os.path.join(tbl_path, part2_name)

        # 2. 在历史分区 1 下模拟删除字段后遗留的嵌套空文件夹 factor/alpha/
        empty_dir = os.path.join(part1_dir, "factor", "alpha")
        os.makedirs(empty_dir, exist_ok=True)
        self.assertTrue(os.path.exists(empty_dir))

        # 3. 在历史分区 1 目录下人工创建一个新字段文件 "signal"
        # 模拟历史分区有该字段而最新分区没有的情况
        signal_file = os.path.join(part1_dir, "signal")
        with open(signal_file, "wb") as f:
            header = bytearray(64)
            header[0:8] = b"SPLAYFLD"
            header[8:10] = (2).to_bytes(2, "little")  # version = 2
            header[10:12] = (0).to_bytes(2, "little") # flags = 0
            header[12] = 7  # Float64 (id = 7)
            header[13] = 0  # Plain
            header[14] = 0  # None
            header[24:28] = (1).to_bytes(4, "little")  # row_count = 1
            header[28:32] = (1).to_bytes(4, "little")  # null_count = 1
            f.write(header)

        self.assertTrue(os.path.exists(signal_file))
        self.assertFalse(os.path.exists(os.path.join(part2_dir, "signal")))

        # 4. 执行 fix()
        writer.fix()

        # 5. 验证空文件夹被清理
        self.assertFalse(os.path.exists(empty_dir))
        self.assertFalse(os.path.exists(os.path.join(part1_dir, "factor", "alpha")))

        # 6. 验证最新分区 2 自动通过 init_field 补齐了 signal
        self.assertTrue(os.path.exists(os.path.join(part2_dir, "signal")))

        # 7. 验证读取
        reader = writer.as_reader()
        tbl = reader.read_all()
        self.assertIn("signal", tbl.column_names)
        self.assertEqual(len(tbl), 2)

        reader.close()
        writer.remove()


if __name__ == "__main__":
    unittest.main()
