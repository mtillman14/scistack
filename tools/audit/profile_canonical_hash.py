"""Where does canonical_hash spend its time? (cleanup-audit F30)

Run from the repo root with the project's Python:

    python tools/audit/profile_canonical_hash.py

It builds records shaped like the 7,000-record DummyMixed save (one-row
DataFrames: 27 float columns, 25 columns holding a length-10 array, 2 holding
a dict) plus a multi-row variant, then times:

  total            canonical_hash(df) per record, as save_batch calls it
  column_access    df[col].to_numpy() for every column (pandas overhead only)
  serialize_num    _serialize_for_hash on the float columns
  serialize_obj    _serialize_for_hash on the object columns (tolist + recursion)
  sha256           hashing the final bytes
  json_dumps_calls how many json.dumps calls one record costs

and prints the top of a cProfile run. Paste the whole output back.
"""

import cProfile
import hashlib
import io
import json
import pstats
import time
from unittest import mock

import numpy as np
import pandas as pd

from scicanonicalhash import hashing
from scicanonicalhash.hashing import _serialize_for_hash, canonical_hash

N = 2000
rng = np.random.default_rng(0)


def one_row_record(i: int) -> pd.DataFrame:
    cols: dict = {}
    for j in range(27):
        cols[f"dbl_{j:02d}"] = [float(rng.standard_normal())]
    for j in range(25):
        cols[f"arr_{j:02d}"] = [rng.standard_normal(10)]
    for j in range(2):
        cols[f"json_{j:02d}"] = [{"mean": float(rng.standard_normal()), "idx": i}]
    return pd.DataFrame(cols)


def multi_row_record(i: int, rows: int = 30) -> pd.DataFrame:
    """A per-trial table: numeric columns plus one object column of arrays."""
    return pd.DataFrame(
        {
            "t": np.arange(rows, dtype=float),
            "x": rng.standard_normal(rows),
            "label": [f"step{k}" for k in range(rows)],
            "curve": [rng.standard_normal(10) for _ in range(rows)],
        }
    )


def per_record(label: str, seconds: float, n: int) -> None:
    print(f"  {label:<16} {seconds:8.3f}s total  {seconds / n * 1e3:8.3f} ms/record")


def profile_shape(name: str, records: list) -> None:
    n = len(records)
    print(f"\n=== {name}: {n} records, {records[0].shape[0]} row(s) x {records[0].shape[1]} col(s) ===")

    t = time.perf_counter()
    for df in records:
        canonical_hash(df)
    per_record("total", time.perf_counter() - t, n)

    t = time.perf_counter()
    arrays = [[df[c].to_numpy() for c in sorted(df.columns, key=str)] for df in records]
    per_record("column_access", time.perf_counter() - t, n)

    t_num = t_obj = 0.0
    serialized = []
    for cols in arrays:
        parts = []
        for arr in cols:
            t0 = time.perf_counter()
            parts.append(_serialize_for_hash(arr))
            dt = time.perf_counter() - t0
            if arr.dtype == object:
                t_obj += dt
            else:
                t_num += dt
        serialized.append(b"|".join(parts))
    per_record("serialize_num", t_num, n)
    per_record("serialize_obj", t_obj, n)

    t = time.perf_counter()
    for b in serialized:
        hashlib.sha256(b).hexdigest()
    per_record("sha256", time.perf_counter() - t, n)
    print(f"  bytes/record     {sum(map(len, serialized)) // n}")

    calls = {"n": 0}
    real_dumps = json.dumps

    def counting(*a, **k):
        calls["n"] += 1
        return real_dumps(*a, **k)

    with mock.patch.object(hashing.json, "dumps", counting):
        canonical_hash(records[0])
    print(f"  json_dumps_calls {calls['n']} per record")

    prof = cProfile.Profile()
    prof.enable()
    for df in records[: min(n, 500)]:
        canonical_hash(df)
    prof.disable()
    out = io.StringIO()
    pstats.Stats(prof, stream=out).sort_stats("tottime").print_stats(12)
    print("  --- cProfile (500 records, by own time) ---")
    print("\n".join("  " + line for line in out.getvalue().splitlines()[4:24]))


if __name__ == "__main__":
    print(f"numpy {np.__version__}, pandas {pd.__version__}")
    profile_shape("DummyMixed-like", [one_row_record(i) for i in range(N)])
    profile_shape("multi-row table", [multi_row_record(i) for i in range(N // 4)])
