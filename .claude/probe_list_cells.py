"""What does each DuckDB fetch path hand over for a DOUBLE[] cell with NaN in it?

Writes through pyarrow exactly as scidb does (pa.list_(pa.float64())), then reads
the same three cells — [1, NaN, 3], [NaN, NaN, NaN], NULL — through every path.
"""
import duckdb
import numpy as np
import pyarrow as pa

print("duckdb", duckdb.__version__, "pyarrow", pa.__version__, "numpy", np.__version__)

con = duckdb.connect()
con.execute("CREATE TABLE t (record_id INTEGER, v DOUBLE[])")
table = pa.table(
    {
        "record_id": pa.array([1, 2, 3], pa.int32()),
        "v": pa.array([[1.0, float("nan"), 3.0], [float("nan")] * 3, None], pa.list_(pa.float64())),
    }
)
con.register("arrow_in", table)
con.execute("INSERT INTO t SELECT * FROM arrow_in")


def show(label, cells):
    print(f"\n--- {label}")
    for i, c in enumerate(cells, start=1):
        extra = ""
        if isinstance(c, np.ndarray):
            extra = f" dtype={c.dtype} shape={c.shape}"
        print(f"  row {i}: {type(c).__name__:14s}{extra}  {c!r}"[:160])


rows = con.execute("SELECT v FROM t ORDER BY record_id").fetchall()
show("fetchall", [r[0] for r in rows])

df = con.execute("SELECT v FROM t ORDER BY record_id").df()
print(f"\n.df() column dtype: {df['v'].dtype}")
show(".df()", list(df["v"]))

npy = con.execute("SELECT v FROM t ORDER BY record_id").fetchnumpy()
print(f"\nfetchnumpy: {type(npy['v']).__name__} dtype={npy['v'].dtype}")
show("fetchnumpy", list(npy["v"]))

res = con.execute("SELECT v FROM t ORDER BY record_id").arrow()
tbl = res.read_all() if hasattr(res, "read_all") else res
col = tbl.column("v").combine_chunks()
values = col.values.to_numpy(zero_copy_only=False)
offsets = col.offsets.to_numpy()
print(f"\narrow: values={values!r} offsets={offsets!r} null_count={col.null_count}")
valid = col.is_valid().to_numpy(zero_copy_only=False)
show("arrow via offsets", [values[offsets[i]:offsets[i + 1]] if valid[i] else None for i in range(len(col))])

print("\n--- what the database actually holds (SQL view of the same cells)")
for r in con.execute("SELECT record_id, v, len(v), list_count(v), v[2] FROM t ORDER BY record_id").fetchall():
    print("  ", r)

print("\n=== the scidb single-record path: parameter binding, not Arrow ===")
con.execute("CREATE TABLE p (record_id INTEGER, v DOUBLE[])")
con.execute("INSERT INTO p VALUES (?, ?)", [1, [1.0, float("nan"), 3.0]])
con.execute("INSERT INTO p VALUES (?, ?)", [2, [float("nan")] * 3])
con.execute("INSERT INTO p VALUES (?, ?)", [3, np.array([4.0, np.nan, 6.0]).tolist()])
con.execute("INSERT INTO p VALUES (?, ?)", [4, [1.0, None, 3.0]])
for r in con.execute("SELECT record_id, v, v[2], v[2] IS NULL, isnan(v[2]) FROM p ORDER BY record_id").fetchall():
    print("  stored:", r)
dfp = con.execute("SELECT v FROM p ORDER BY record_id").df()
show(".df() after parameter binding", list(dfp["v"]))
for i, c in enumerate(dfp["v"], start=1):
    print(f"  row {i}: masked={isinstance(c, np.ma.MaskedArray)}  np.asarray -> {np.asarray(c)!r}")
