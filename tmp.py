import numpy as np
from sciduckdb import SciDuck

duck = SciDuck(":memory:", dataset_schema=["subject", "trial"])
duck.save("ints", {"ids": np.array([1, 2, 3], dtype=np.int64)}, subject="S01", trial="1")
duck._execute('UPDATE "ints" SET ids = [1, NULL, 3]')

raw = duck._fetchdf('SELECT ids FROM "ints"')["ids"].iloc[0]
print("type      :", type(raw))
print("repr      :", repr(raw))
print("is_masked :", np.ma.is_masked(raw))
print("dtype     :", getattr(raw, "dtype", None))

# and the same for a float column, which we know works
duck.save("dbls", {"v": np.array([np.nan, 0.5])}, subject="S01", trial="1")
rawf = duck._fetchdf('SELECT v FROM "dbls"')["v"].iloc[0]
print("float type:", type(rawf), "| is_masked:", np.ma.is_masked(rawf))
duck.close()