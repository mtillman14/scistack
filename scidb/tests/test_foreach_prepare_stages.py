"""``_for_each_prepare`` as named stages (cleanup-audit F4,
``.claude/plan-split-for-each-prepare.md``).

Two regressions guarded here:

* The dry-run preview used to carry its OWN copy of the existing-combo
  prefilter and the exclusion pass. Two spellings of one filter drift; the
  preview now calls ``_existing_combos``, the same function the real run
  uses, so its iteration count is the real run's.
* ``_for_each_prepare`` was a 1,518-line function whose blocks all read each
  other's locals. It is now a short sequence of stage calls; the size guard
  keeps new logic going into a stage (or a new one), not back inline.
"""

import inspect

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb import foreach as scidb_foreach
from scidb.exclusions import exclude_schema

SCHEMA = ["subject", "trial"]


class StageInput(BaseVariable):
    pass


class StageOutput(BaseVariable):
    pass


def double(signal):
    return signal * 2


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "prepare_stages.duckdb", SCHEMA)
    # A ragged grid: S02 has no t2, so the Cartesian product (4) is NOT
    # what exists (3).
    for subject, trial in [("S01", "t1"), ("S01", "t2"), ("S02", "t1")]:
        StageInput.save(np.array([1.0, 2.0]), subject=subject, trial=trial)
    yield database
    _scifor.set_schema([])
    database.close()


def _written(db) -> set[tuple[str, str]]:
    frame = db.load_all_as_df(StageOutput, stringify_schema=True)
    if len(frame) == 0:
        return set()
    return {(row.subject, row.trial) for row in frame.itertuples()}


def test_the_dry_run_counts_what_the_real_run_does(db, capsys):
    """Missing combos pruned AND excluded combos dropped — in both paths."""
    exclude_schema("bad trial", db=db, subject="S01", trial="t2")

    for_each(
        double, {"signal": StageInput}, [StageOutput],
        subject=[], trial=[], dry_run=True,
    )
    out = capsys.readouterr().out
    assert "[dry-run] 2 iterations" in out, out
    assert _written(db) == set()

    for_each(double, {"signal": StageInput}, [StageOutput], subject=[], trial=[])
    assert _written(db) == {("S01", "t1"), ("S02", "t1")}


def test_the_dry_run_uses_the_one_existing_combo_filter(db, monkeypatch):
    calls = []
    real = scidb_foreach._existing_combos

    def spy(*args, **kwargs):
        result = real(*args, **kwargs)
        calls.append(result)
        return result

    monkeypatch.setattr(scidb_foreach, "_existing_combos", spy)
    for_each(
        double, {"signal": StageInput}, [StageOutput],
        subject=[], trial=[], dry_run=True,
    )
    assert len(calls) == 1
    assert {(c["subject"], c["trial"]) for c in calls[0]} == {
        ("S01", "t1"), ("S01", "t2"), ("S02", "t1"),
    }


def test_prepare_stays_a_sequence_of_stages():
    lines = inspect.getsource(scidb_foreach._for_each_prepare).count("\n")
    assert lines < 200, (
        f"_for_each_prepare is {lines} lines; put new pre-loop logic in a "
        f"named stage function, not inline"
    )
