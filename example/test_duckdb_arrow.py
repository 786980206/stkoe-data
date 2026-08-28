"""Test DuckDB reading Arrow IPC exported from Splayed via pyarrow."""
import duckdb
import pyarrow.ipc as ipc
import pyarrow as pa

ARROW_FILE = "D:/proj/stkoe/stkoe-data/target/demo_dataset.arrow"

# Read Arrow IPC stream with pyarrow (StreamWriter uses stream format)
reader = ipc.open_stream(ARROW_FILE)
table = reader.read_all()
print(f"Arrow table loaded: {table.num_rows} rows, {table.num_columns} cols")
print(f"Schema: {table.schema}")
print()

# Register the Arrow table in DuckDB
con = duckdb.connect()
con.register("splayed", table)

# Query the Splayed data via DuckDB
result = con.execute("SELECT * FROM splayed ORDER BY sym, time").fetchall()
print(f"Total rows: {len(result)}")
for row in result:
    print(row)

print()

# Run analytical SQL with window functions
print("--- Analytical SQL ---")
result = con.execute("""
    SELECT sym, AVG(close) as avg_close, SUM(volume) as total_vol
    FROM splayed GROUP BY sym ORDER BY sym
""").fetchall()
for r in result:
    print(r)

print()

# Window function: 3-day moving average
print("--- 3-day moving average (window function) ---")
result = con.execute("""
    SELECT sym, time, close,
           AVG(close) OVER (PARTITION BY sym ORDER BY time ROWS 2 PRECEDING) as ma_3
    FROM splayed ORDER BY sym, time
""").fetchall()
for r in result:
    print(r)
