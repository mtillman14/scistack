"""NaN inside an array-valued column must survive save → load → for_each.

The shape here is ``GAITRiteLoaded``: a DataFrame-mode variable whose cells
hold variable-length float vectors, some of whose elements are NaN. Stored,
each vector becomes a DuckDB ``DOUBLE[]`` whose NaN elements are NULL; on load
DuckDB returns a masked array, and before 2026-09-15 ``np.asarray`` dropped the
mask so the next function in the pipeline received ``0`` where the producer had
written NaN (see ``.claude/plan-null-list-elements-to-nan.md``).

sciduckdb/tests/test_null_list_roundtrip.py pins the deserialiser itself. This
pins the two scidb load paths that feed user functions: ``load()`` per record
and the bulk spread load that ``for_each`` uses to build its inputs.
"""

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject", "trial"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_nan_roundtrip.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class StepTable(BaseVariable):
    """One row per walk; each cell is a vector of step lengths."""


class StepSummary(BaseVariable):
    pass


def _table(first_value):
    """A GAITRiteLoaded-shaped frame: leading element NaN, as the loader emits
    for the first step (no preceding footfall)."""
    return pd.DataFrame(
        {
            "steps": [
                np.array([first_value, 0.49146, 0.45202]),
                np.array([0.51156, np.nan]),
            ],
            "foot": ["L", "R"],
        }
    )


# ---------------------------------------------------------------------------
# load()
# ---------------------------------------------------------------------------


def test_load_keeps_nan_not_zero(db):
    StepTable.save(_table(np.nan), subject=1, trial=1)

    loaded = StepTable.load(subject=1, trial=1).data

    first = loaded["steps"].iloc[0]
    assert np.isnan(first[0]), first
    assert not np.any(first == 0.0), first
    np.testing.assert_allclose(first[1:], [0.49146, 0.45202])

    second = loaded["steps"].iloc[1]
    assert second[0] == pytest.approx(0.51156)
    assert np.isnan(second[1]), second


def test_a_real_zero_is_still_a_zero(db):
    """The fix must not turn genuine zeros into NaN."""
    StepTable.save(_table(0.0), subject=2, trial=1)

    first = StepTable.load(subject=2, trial=1).data["steps"].iloc[0]
    assert first[0] == 0.0
    assert not np.isnan(first[0])


# ---------------------------------------------------------------------------
# for_each — the path grSides took
# ---------------------------------------------------------------------------


def test_for_each_input_keeps_nan(db):
    for subject in (1, 2):
        StepTable.save(_table(np.nan), subject=subject, trial=1)

    received = []

    def summarize(steps):
        received.append(steps)
        return 1.0

    for_each(
        summarize,
        {"steps": StepTable},
        [StepSummary],
        subject=[1, 2],
        trial=[1],
        save=False,
    )

    assert received, "for_each never called the function"

    # Shape-agnostic on purpose: whether scifor hands the function the vector
    # itself or a frame holding it is pinned elsewhere; what this test owns is
    # that the NaN survived the trip. Collect every float vector that arrived.
    vectors = []
    for r in received:
        if isinstance(r, pd.DataFrame):
            vectors.extend(v for v in r["steps"] if isinstance(v, np.ndarray))
        elif isinstance(r, pd.Series):
            vectors.extend(v for v in r if isinstance(v, np.ndarray))
        elif isinstance(r, np.ndarray):
            vectors.append(r)

    assert vectors, f"no step vectors reached the function: {received!r}"
    three = [v for v in vectors if len(v) == 3]
    assert three, f"the 3-element vector never arrived: {vectors!r}"
    assert all(np.isnan(v[0]) for v in three), three
    assert not any(v[0] == 0.0 for v in three), three


# ---------------------------------------------------------------------------
# Content-hash stability: the round trip must be a fixed point
# ---------------------------------------------------------------------------


def test_reload_hashes_the_same(db):
    """Before the fix, saving NaN and re-saving what came back produced a
    DIFFERENT record_id (NaN out, 0 back in), so a no-op re-run silently wrote
    a second record at the same location."""
    StepTable.save(_table(np.nan), subject=3, trial=1)
    first_id = StepTable.load(subject=3, trial=1).record_id

    reloaded = StepTable.load(subject=3, trial=1).data
    StepTable.save(reloaded, subject=3, trial=1)

    assert StepTable.load(subject=3, trial=1).record_id == first_id
