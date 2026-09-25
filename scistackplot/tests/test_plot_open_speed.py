"""Opening a wide variable (2026-09-25).

SymmetryTable, 11,609 records × 80 fields, melted to 928,720 rows, took ~35 s
to show its first figure: y limits walked every row in Python (~11.5 s) and
logged one DEBUG line per scope group (~7 s), and the default spec faceted
all 80 fields (80-panel render ~10 s).

* ``_raw_extents`` is one groupby — pinned here against the row-by-row
  reference it replaced, including a missing level, a categorical column and
  array cells;
* ``default_spec`` opens a table with more than ``WIDE_FIELD_LIMIT`` fields on
  its first field only.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from scistackplot import LongTable
from scistackplot.roles import WIDE_FIELD_LIMIT, default_spec
from scistackplot.ylimits import ExtentMode, _cell_extents, _raw_extents, hashable


def _reference(frame, measure, scope, mode):
    """The row-by-row fold `_raw_extents` used before 2026-09-25."""
    lows, highs = _cell_extents(frame[measure], mode)
    usable = ~(np.isnan(lows) | np.isnan(highs))
    keys = [
        tuple(hashable(v) for v in key)
        for key in zip(*[frame[name].to_numpy() for name in scope], strict=True)
    ]
    out: dict = {}
    for key, low, high, ok in zip(keys, lows, highs, usable, strict=True):
        if not ok:
            continue
        seen = out.get(key)
        out[key] = (low, high) if seen is None else (min(seen[0], low), max(seen[1], high))
    return {k: (float(a), float(b)) for k, (a, b) in out.items()}


def _mode(log: bool = False):
    # The plain raw mode: no reduction.
    return ExtentMode(summary=False, collapse=False, from_zero=False, log=log)


def test_groupby_matches_row_by_row_reference():
    rng = np.random.default_rng(0)
    n = 500
    frame = pd.DataFrame(
        {
            "subject": rng.choice(["01", "02", None], n),
            "ColName": pd.Categorical(rng.choice(["a", "b", "c"], n), categories=["a", "b", "c", "unused"]),
            "y": rng.normal(size=n),
        }
    )
    frame.loc[::7, "y"] = np.nan
    # A few array cells, as a melted struct holds beside its scalars.
    frame["y"] = frame["y"].astype(object)
    frame.at[3, "y"] = [5.0, -9.0]
    frame.at[4, "y"] = [np.nan]
    scope = ["subject", "ColName"]
    got = _raw_extents(frame, "y", scope, _mode())
    assert got == _reference(frame, "y", scope, _mode())
    # A missing subject is ONE group keyed None, as `limits_for` looks it up.
    assert any(key[0] is None for key in got)
    # An unused category is not a group.
    assert all(key[1] != "unused" for key in got)


def test_log_mode_matches_reference():
    frame = pd.DataFrame(
        {"subject": ["01", "01", "02", "02"], "y": [-1.0, 4.0, 0.0, 2.0]}
    )
    got = _raw_extents(frame, "y", ["subject"], _mode(log=True))
    assert got == _reference(frame, "y", ["subject"], _mode(log=True))


def test_single_scope_column_keys_are_tuples():
    frame = pd.DataFrame({"subject": ["01", "01", "02"], "y": [1.0, 3.0, 2.0]})
    assert _raw_extents(frame, "y", ["subject"], _mode()) == {
        ("01",): (1.0, 3.0),
        ("02",): (2.0, 2.0),
    }


def _field_table(n_fields: int) -> LongTable:
    frame = pd.DataFrame(
        {
            "subject": ["01"] * n_fields,
            "ColName": [f"f{n:03d}" for n in range(n_fields)],
            "Measure": [float(n) for n in range(n_fields)],
        }
    )
    return LongTable.from_frame(
        frame,
        factors=["subject", "ColName"],
        measures=["Measure"],
        field_factors=["ColName"],
    )


def test_wide_table_opens_on_its_first_field():
    spec = default_spec(_field_table(WIDE_FIELD_LIMIT + 1), "Measure")
    assert [(f.column, f.include) for f in spec.filters] == [("ColName", ["f000"])]
    # One visible field -> one panel, so no grid width is pinned.
    assert spec.facet.n_cols is None


def test_table_at_the_limit_opens_every_field():
    spec = default_spec(_field_table(WIDE_FIELD_LIMIT), "Measure")
    assert spec.filters == []
