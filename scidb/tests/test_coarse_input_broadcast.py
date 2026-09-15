"""Regression: an input stored at a COARSER level than the iteration must
BROADCAST across the dimensions it does not have, for every input kind — not
just inside a ``Merge``.

Bug (2026-09-15, found on ``grSides``): ``load_all_as_df(layout="spread")``
emits one column per *dataset* schema key, so a subject-level variable such as
Demographics comes back carrying ``session``/``speed``/``trial`` as all-NULL
columns. ``_load_input`` dropped those columns only for ``Merge``
constituents. Bound straight to a parameter — with or without a
``ColumnSelection`` — the NULL columns reached the per-combo filter, where:

* Python (``scifor._filter_df_for_combo``) evaluates ``None == "BL"`` → every
  combo filters to zero rows → ``scifor:NoData`` everywhere, ``completed=0``.
* MATLAB (``filter_table_for_combo``) receives a cell array of ``0×0 double``
  and ``string(col_data)`` raises ``MATLAB:string:MustBeConvertibleCellArray``
  → every iteration fails with "failed to filter <param>".

Both end in an empty result table, so the node reads as a silent no-op.

Fix: ``_drop_unpopulated_schema_columns`` in ``scidb/foreach.py``, applied by
``_load_input`` to every input kind. See docs/claude/coarse-level-inputs.md.
"""

import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, Fixed, Merge, configure_database, for_each

SCHEMA = ["subject", "session", "trial"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_coarse_broadcast.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class Demographics(BaseVariable):
    """Subject-level: no session, no trial. The coarse input under test."""

    pass


class TrialSignal(BaseVariable):
    """Trial-level: populates every schema key. The fine input."""

    pass


class Combined(BaseVariable):
    pass


def _seed(db):
    """Two subjects × two sessions × two trials of signal; side per subject."""
    for subj, side in (("S01", "L"), ("S02", "R")):
        Demographics.save(
            pd.DataFrame({"PareticSide": [side], "Age": [60]}), subject=subj
        )
        for sess in ("BL", "POST"):
            for trial in ("1", "2"):
                TrialSignal.save(
                    float(trial), subject=subj, session=sess, trial=trial
                )


def _run(inputs, calls):
    """for_each over the full subject/session/trial grid, recording every call."""

    def record(signal, side):
        calls.append((signal, side))
        return 1.0

    for_each(
        record,
        inputs,
        [Combined],
        subject=["S01", "S02"],
        session=["BL", "POST"],
        trial=["1", "2"],
    )


def _sides(calls):
    """The ``side`` argument of each call, normalised out of its container."""
    out = []
    for _signal, side in calls:
        if isinstance(side, pd.DataFrame):
            side = side.iloc[0, 0]
        elif hasattr(side, "tolist"):
            flat = side.tolist()
            while isinstance(flat, list) and flat:
                flat = flat[0]
            side = flat
        out.append(side)
    return out


# ---------------------------------------------------------------------------
# The four input kinds
# ---------------------------------------------------------------------------


def test_bare_coarse_variable_broadcasts(db):
    """A subject-level variable bound directly runs at every combo below it."""
    _seed(db)
    calls: list = []
    _run({"signal": TrialSignal, "side": Demographics}, calls)

    assert len(calls) == 8, (
        f"expected one call per subject×session×trial combo, got {len(calls)} — "
        f"the coarse input filtered to nothing instead of broadcasting"
    )


def test_column_selection_on_coarse_variable_broadcasts(db):
    """The grSides shape: Demographics["PareticSide"] at the trial level."""
    _seed(db)
    calls: list = []
    _run({"signal": TrialSignal, "side": Demographics["PareticSide"]}, calls)

    assert len(calls) == 8, (
        f"expected 8 calls, got {len(calls)} — ColumnSelection on a coarse "
        f"variable did not broadcast"
    )
    # Each subject keeps ITS side across all four of its session/trial combos
    # (sorted, so the assertion does not pin the combo iteration order).
    assert sorted(_sides(calls)) == ["L"] * 4 + ["R"] * 4


def test_fixed_wrapping_coarse_variable_broadcasts(db):
    """Fixed() pins a key the coarse variable has; the rest still broadcast."""
    _seed(db)
    calls: list = []
    _run(
        {"signal": TrialSignal, "side": Fixed(Demographics, subject="S01")},
        calls,
    )

    assert len(calls) == 8, (
        f"expected 8 calls, got {len(calls)} — Fixed() around a coarse variable "
        f"did not broadcast across the keys it does not have"
    )


def test_merge_with_coarse_constituent_still_broadcasts(db):
    """The path that always worked — it must keep working after the refactor."""
    _seed(db)
    calls: list = []

    def record(merged, side):
        calls.append((merged, side))
        return 1.0

    for_each(
        record,
        {"merged": Merge(TrialSignal, Demographics), "side": Demographics},
        [Combined],
        subject=["S01", "S02"],
        session=["BL", "POST"],
        trial=["1", "2"],
    )
    assert len(calls) == 8


# ---------------------------------------------------------------------------
# The drop itself
# ---------------------------------------------------------------------------


def test_only_entirely_null_schema_columns_are_dropped():
    """A partially-populated key still filters; conflating the two would make a
    row that genuinely has no session broadcast across every session."""
    from scidb.foreach import _drop_unpopulated_schema_columns

    df = pd.DataFrame(
        {
            "subject": ["S01", "S02"],
            "session": ["BL", None],  # partially populated — must stay
            "trial": [None, None],  # entirely null — must go
            "value": [1.0, 2.0],
        }
    )
    out = _drop_unpopulated_schema_columns(df, set(SCHEMA), context="test")

    assert "trial" not in out.columns
    assert "session" in out.columns, (
        "a partially-populated schema key was dropped — rows that DO have a "
        "session would then broadcast across every session"
    )
    assert "subject" in out.columns
    assert "value" in out.columns, "a non-schema column was dropped"


def test_drop_leaves_a_fully_populated_frame_untouched():
    from scidb.foreach import _drop_unpopulated_schema_columns

    df = pd.DataFrame(
        {"subject": ["S01"], "session": ["BL"], "trial": ["1"], "value": [1.0]}
    )
    out = _drop_unpopulated_schema_columns(df, set(SCHEMA), context="test")

    assert list(out.columns) == list(df.columns)
    assert out is df, "an untouched frame should not be copied"


def test_coarse_input_load_drops_the_unpopulated_columns(db):
    """The loaded input scifor actually receives carries no all-NULL schema key."""
    from scidb.foreach import _load_input

    _seed(db)
    loaded = _load_input(Demographics, db, None, param_name="side")

    assert isinstance(loaded, pd.DataFrame)
    assert "subject" in loaded.columns
    assert "session" not in loaded.columns
    assert "trial" not in loaded.columns
