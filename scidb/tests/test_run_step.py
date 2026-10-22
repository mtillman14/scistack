"""``for_each(..., step="name")``: a step name recorded on the run, never
identity (plan-canvas-as-document Q6)."""

import pytest

import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each
from scidb import provenance_query as pq
from scidb.database import aggregate_pipeline_variants
from scidb.intent import current_step, run_step, set_ambient_step

SUBJECTS = ["S01", "S02"]


class StepRaw(BaseVariable):
    pass


class StepOut(BaseVariable):
    pass


def double(x, k):
    return x * k


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "steps.duckdb", ["subject"])
    for s in SUBJECTS:
        StepRaw.save(1.0, subject=s)
    yield database
    set_ambient_step(None)
    _scifor.set_schema([])
    database.close()


def _run(step=None, k=2.0):
    for_each(double, {"x": StepRaw, "k": k}, [StepOut], subject=SUBJECTS, step=step)


def _counts(db):
    duck = db._duck
    return (
        duck._fetchone("SELECT COUNT(*) FROM _invocation")[0],
        duck._fetchone("SELECT COUNT(*) FROM _record")[0],
    )


def test_the_step_name_is_recorded_on_the_run(db):
    _run("filter")
    rows = db._duck._fetchall("SELECT step FROM _run_step")
    assert rows == [("filter",)]
    [variant] = [v for v in pq.pipeline_variants(db._duck) if v["function_name"] == "double"]
    assert variant["steps"] == ["filter"]


def test_it_is_not_identity(db):
    _run("filter")
    before = _counts(db)
    _run("renamed")
    assert _counts(db) == before, "renaming a step re-ran something"
    [variant] = [v for v in pq.pipeline_variants(db._duck) if v["function_name"] == "double"]
    assert variant["steps"] == ["filter", "renamed"]


def test_an_unnamed_run_records_none(db):
    _run()
    assert db._duck._fetchall("SELECT * FROM _run_step") == []
    [variant] = [v for v in pq.pipeline_variants(db._duck) if v["function_name"] == "double"]
    assert variant.get("steps", []) == []


def test_the_aggregate_carries_it_per_call_site(db):
    _run("filter")
    agg = aggregate_pipeline_variants(pq.pipeline_variants(db._duck))
    [entry] = [d for (fn, _cid), d in agg["functions"].items() if fn == "double"]
    assert entry["steps"] == ["filter"]


def test_the_context_is_cleared_after_the_call(db):
    _run("filter")
    assert current_step() is None


def test_a_bad_name_is_refused(db):
    with pytest.raises(ValueError, match="step="):
        _run("   ")


def test_the_ambient_entry_point_names_and_clears(db):
    set_ambient_step("from_matlab")
    _run()
    set_ambient_step(None)
    _run(k=3.0)
    assert sorted(r[0] for r in db._duck._fetchall("SELECT step FROM _run_step")) == ["from_matlab"]


def test_run_step_context_manager(db):
    with run_step("ctx"):
        assert current_step() == "ctx"
    assert current_step() is None
