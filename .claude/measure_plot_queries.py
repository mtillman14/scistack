"""Time every query shape the plot plan proposes, against the real database.

Copy to tmp.py and run with the GUI closed (it holds the DuckDB file lock).
Each line prints one number; paste the whole output back. Nothing is written.
"""
import time

import duckdb

DB = r"C:\Users\mtillman\Datasets\Stroke Aim 2\Stroke-R01-Aim2.duckdb"
VAR = "RawEMG"

con = duckdb.connect(DB, read_only=True)


def timed(label, fn):
    t0 = time.perf_counter()
    try:
        out = fn()
    except Exception as exc:  # report and keep going
        print(f"{label:70s} FAILED: {type(exc).__name__}: {str(exc)[:120]}")
        return None
    dt = time.perf_counter() - t0
    size = ""
    if hasattr(out, "shape"):
        size = f"  shape={out.shape}"
    elif isinstance(out, list):
        size = f"  rows={len(out)}"
    print(f"{label:70s} {dt:8.3f}s{size}")
    return out


row = con.execute(
    "SELECT table_name FROM _registered_types WHERE type_name = ?", [VAR]
).fetchone()
TABLE = row[0] if row and row[0] else f"{VAR}_data"
COLS = [
    r[0]
    for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = ? AND column_name != 'record_id' ORDER BY ordinal_position",
        [TABLE],
    ).fetchall()
]
COL = COLS[0]
print(f"table={TABLE}  columns={COLS}\n")

BASE = (
    f'FROM "{TABLE}" t JOIN _record r ON t.record_id = r.record_id '
    f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE"
)
CLEAN = f'list_filter(t."{COL}", x -> NOT isnan(x))'
UNNEST = (
    f'FROM "{TABLE}" t JOIN _record r ON t.record_id = r.record_id, '
    f'UNNEST(t."{COL}") WITH ORDINALITY AS v(val, pos) '
    f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE AND NOT isnan(v.val)"
)

print("--- stage 2: what a metadata-only get_table would cost")
timed(
    "metadata only: record_id + schema keys, fetchall",
    lambda: con.execute(
        f'SELECT t.record_id, s.* FROM "{TABLE}" t JOIN _record r ON t.record_id = r.record_id '
        f"LEFT JOIN _schema s ON r.schema_id = s.schema_id "
        f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE",
        [VAR],
    ).fetchall(),
)
timed(
    "list lengths only: len() per record, one column",
    lambda: con.execute(f'SELECT len(t."{COL}") {BASE}', [VAR]).fetchall(),
)

print("\n--- stage 4: fetch method, ONE column, all 419 records (x10 = today's load)")
timed(f"{COL}: fetchall  (today: boxed Python floats)", lambda: con.execute(f'SELECT t."{COL}" {BASE}', [VAR]).fetchall())
timed(f"{COL}: .fetch_arrow_table() (materialized, no boxing)", lambda: con.execute(f'SELECT t."{COL}" {BASE}', [VAR]).fetch_arrow_table())
timed(f"{COL}: .df()     (numpy array per cell)", lambda: con.execute(f'SELECT t."{COL}" {BASE}', [VAR]).df())
timed(f"{COL}: .fetchnumpy()", lambda: con.execute(f'SELECT t."{COL}" {BASE}', [VAR]).fetchnumpy())

print("\n--- stage 3: ONE record (example C), one column")
first = con.execute(f"SELECT t.record_id {BASE} LIMIT 1", [VAR]).fetchone()[0]
timed(
    f"{COL}: one record by record_id, fetchall",
    lambda: con.execute(f'SELECT t."{COL}" {BASE} AND t.record_id = ?', [VAR, first]).fetchall(),
)
timed(
    f"{COL}: one record, UNNEST -> .df()",
    lambda: con.execute(f"SELECT v.pos - 1 AS pos, v.val {UNNEST} AND t.record_id = ?", [VAR, first]).df(),
)

print("\n--- y extents (reducer.y_extents raw), one column, all 419")
timed(
    "MIN(list_min)/MAX(list_max) with NaN filter",
    lambda: con.execute(f"SELECT MIN(list_min({CLEAN})), MAX(list_max({CLEAN})) {BASE}", [VAR]).fetchall(),
)

print("\n--- explode (reducer.explode_series as committed), one column, all 419")
timed(
    "UNNEST all samples -> .df()  (x10 columns = example D.2)",
    lambda: con.execute(f"SELECT t.record_id, v.pos - 1 AS pos, v.val {UNNEST}", [VAR]).df(),
)

print("\n--- summarize_series (example D.3): centre/sd/n per position, one column, all 419")
timed(
    "GROUP BY pos: avg, stddev_samp, count -> .df()",
    lambda: con.execute(
        f"SELECT v.pos - 1 AS pos, avg(v.val), stddev_samp(v.val), count(v.val) {UNNEST} "
        f"GROUP BY pos ORDER BY pos",
        [VAR],
    ).df(),
)
timed(
    "GROUP BY subject, pos: avg, stddev_samp, count -> .df()",
    lambda: con.execute(
        f'SELECT s."subject", v.pos - 1 AS pos, avg(v.val), stddev_samp(v.val), count(v.val) '
        f'FROM "{TABLE}" t JOIN _record r ON t.record_id = r.record_id '
        f"LEFT JOIN _schema s ON r.schema_id = s.schema_id, "
        f'UNNEST(t."{COL}") WITH ORDINALITY AS v(val, pos) '
        f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE AND NOT isnan(v.val) "
        f'GROUP BY s."subject", pos ORDER BY s."subject", pos',
        [VAR],
    ).df(),
)

print("\n--- summarize with in-query stride (example D.4), one column, all 419")
timed(
    "GROUP BY subject, pos WHERE pos % 200 = 0 -> .df()",
    lambda: con.execute(
        f'SELECT s."subject", v.pos - 1 AS pos, avg(v.val), stddev_samp(v.val), count(v.val) '
        f'FROM "{TABLE}" t JOIN _record r ON t.record_id = r.record_id '
        f"LEFT JOIN _schema s ON r.schema_id = s.schema_id, "
        f'UNNEST(t."{COL}") WITH ORDINALITY AS v(val, pos) '
        f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE AND NOT isnan(v.val) "
        f"AND (v.pos - 1) % 200 = 0 "
        f'GROUP BY s."subject", pos ORDER BY s."subject", pos',
        [VAR],
    ).df(),
)

print("\ndone")

# ---------------------------------------------------------------------------
# Part 2: the numpy alternative — reduce IN MEMORY after an Arrow fetch
# ---------------------------------------------------------------------------
import numpy as np

print("\n=== numpy over Arrow-fetched cells, one column, all 419 ===")
tbl = timed(
    f"{COL}: .fetch_arrow_table() with record_id + subject",
    lambda: con.execute(
        f'SELECT t.record_id, s."subject", t."{COL}" AS val '
        f'FROM "{TABLE}" t JOIN _record r ON t.record_id = r.record_id '
        f"LEFT JOIN _schema s ON r.schema_id = s.schema_id "
        f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE",
        [VAR],
    ).fetch_arrow_table(),
)
col = tbl.column("val").combine_chunks()
subjects = np.asarray(tbl.column("subject").to_pylist())


def cells_from_list_array(arr):
    values = np.asarray(arr.values.to_numpy(zero_copy_only=False), dtype=np.float64)
    offsets = np.asarray(arr.offsets.to_numpy(), dtype=np.int64)
    return [values[offsets[i] : offsets[i + 1]] for i in range(len(offsets) - 1)], values, offsets


cells, flat, offsets = timed(
    "split ListArray into per-record numpy views (zero copy)",
    lambda: cells_from_list_array(col),
)
lengths = np.diff(offsets)
print(f"   {len(cells)} cells, {flat.size:,} samples, min/max len {lengths.min()}/{lengths.max()}")

timed(
    "y extents: np.nanmin / np.nanmax over the flat buffer",
    lambda: (float(np.nanmin(flat)), float(np.nanmax(flat))),
)


def explode_numpy():
    row = np.repeat(np.arange(len(cells)), lengths)
    pos = np.arange(flat.size) - np.repeat(offsets[:-1], lengths)
    keep = ~np.isnan(flat)
    return row[keep], pos[keep], flat[keep]


timed("explode: concatenate + repeat + positions (the LINE frame)", explode_numpy)


def pad(group_cells):
    width = max(len(c) for c in group_cells)
    out = np.full((len(group_cells), width), np.nan)
    for i, c in enumerate(group_cells):
        out[i, : len(c)] = c
    return out


def summarize_all():
    padded = pad(cells)
    n = np.sum(~np.isnan(padded), axis=0)
    mean = np.nanmean(padded, axis=0)
    sd = np.nanstd(padded, axis=0, ddof=1)
    return mean, sd, n


timed("summarize: mean/sd/n per position over ALL records (padded 2-D)", summarize_all)


def summarize_by_subject():
    out = {}
    for subject in np.unique(subjects):
        group = [c for c, s in zip(cells, subjects) if s == subject]
        padded = pad(group)
        out[subject] = (
            np.nanmean(padded, axis=0),
            np.nanstd(padded, axis=0, ddof=1),
            np.sum(~np.isnan(padded), axis=0),
        )
    return out


timed("summarize: mean/sd/n per position PER SUBJECT (the band plot)", summarize_by_subject)
print("done (part 2)")
