import unittest
import tempfile
import shutil
import os
import polars as pl
import splayed

class TestSplayedPolars(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_sink_and_scan_roundtrip_basic(self):
        tbl_path = os.path.join(self.temp_dir, "tbl_basic")
        df = pl.DataFrame({
            "sym": ["AAPL", "AAPL", "MSFT", "MSFT"],
            "time": [100, 200, 100, 200],
            "price": [15.5, 16.5, 25.5, 26.5],
            "volume": [1000, 2000, 3000, 4000],
        })

        # 1. 目标表不存在：自动 init 建表并写入
        df.sink_splayed(tbl_path, scheme="none")
        self.assertTrue(os.path.exists(os.path.join(tbl_path, ".meta")))

        # 2. 读取并全量校验
        lf = pl.scan_splayed(tbl_path)
        res = lf.collect()
        self.assertEqual(len(res), 4)
        self.assertEqual(set(res.columns), {"sym", "time", "price", "volume"})

    def test_lazy_pushdown_optimizations(self):
        tbl_path = os.path.join(self.temp_dir, "tbl_pushdown")
        df = pl.DataFrame({
            "sym": ["AAPL", "AAPL", "MSFT", "MSFT", "GOOG", "GOOG"],
            "time": [100, 200, 100, 200, 100, 200],
            "price": [10.0, 12.0, 20.0, 22.0, 30.0, 32.0],
            "volume": [100, 200, 300, 400, 500, 600],
        })
        splayed.sink_splayed(df, tbl_path, scheme="none")

        # 惰性查询链：Filter (谓词下推) + Select (列裁剪/投影下推) + Limit (行数下推)
        lf = splayed.scan_splayed(tbl_path)
        q = lf.filter(pl.col("price") > 15.0).select(["sym", "price"]).limit(2)
        res = q.collect()

        self.assertEqual(len(res), 2)
        self.assertEqual(res.columns, ["sym", "price"])
        # 结果应为 MSFT
        self.assertEqual(list(res["sym"]), ["MSFT", "MSFT"])
        self.assertEqual(list(res["price"]), [20.0, 22.0])

    def test_sink_auto_routing_init_and_overwrite(self):
        tbl_path = os.path.join(self.temp_dir, "tbl_routing")
        df1 = pl.DataFrame({
            "sym": ["AAPL", "MSFT"],
            "time": [100, 100],
            "price": [10.0, 20.0],
        })
        # 首次写入：表不存在 -> 自动 init
        df1.sink_splayed(tbl_path)
        
        r1 = pl.scan_splayed(tbl_path).collect()
        self.assertEqual(len(r1), 2)
        self.assertEqual(list(r1["price"]), [10.0, 20.0])

        # 二次写入：表已存在 -> 自动 open + write 覆盖
        df2 = pl.DataFrame({
            "sym": ["AAPL", "MSFT"],
            "time": [100, 100],
            "price": [99.0, 88.0],
        })
        df2.sink_splayed(tbl_path)

        r2 = pl.scan_splayed(tbl_path).collect()
        self.assertEqual(len(r2), 2)
        self.assertEqual(list(r2["price"]), [99.0, 88.0])

    def test_lazyframe_sink_streaming(self):
        tbl_path = os.path.join(self.temp_dir, "tbl_lazy_sink")
        src_df = pl.DataFrame({
            "sym": ["A", "B", "C"],
            "time": [1, 1, 1],
            "price": [1.0, 2.0, 3.0],
        })
        # 从 LazyFrame 直接 sink 到存储
        lf = src_df.lazy().filter(pl.col("price") >= 2.0)
        lf.sink_splayed(tbl_path)

        read_back = pl.scan_splayed(tbl_path).collect()
        self.assertEqual(len(read_back), 2)
        self.assertEqual(list(read_back["sym"]), ["B", "C"])

if __name__ == "__main__":
    unittest.main()
