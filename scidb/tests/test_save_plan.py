"""A ``for_each`` save is ``build_save_plan`` -> ``apply_save_plan``.

Characterizes the plan (P4 stage 3, design review 04b W2) on a two-output
run and a ``distribute`` run: which batches it holds, how many records each,
the input edges of every record, and that one invocation id is shared by
everything one call produced. Also pins that the plan writes nothing (the
writes, and their all-or-nothing transaction, are ``apply_save_plan``'s;
``test_save_provenance_failure.py`` covers the rollback) and that a re-run
yields the same record ids.
"""

import numpy as np
import pytest

import scidb.run.foreach_save as scidb_foreach  # the module that calls build_save_plan / apply_save_plan (patch where called)
import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each


class PlanIn(BaseVariable):
    pass


class PlanOutA(BaseVariable):
    pass


class PlanOutB(BaseVariable):
    pass


class PlanPieces(BaseVariable):
    pass


def two_outputs(x):
    return x * 2, x * 3


def three_pieces(x):
    return np.array([x.sum(), x.sum() + 1, x.sum() + 2])


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "save_plan.duckdb", ["subject", "trial"])
    for s in (1, 2):
        PlanIn.save(np.arange(3.0) + s, db=database, subject=s)
    yield database
    _scifor.set_schema([])
    database.close()


@pytest.fixture
def plans(monkeypatch):
    """Every SavePlan built during the test, in order."""
    seen = []
    real = scidb_foreach.build_save_plan

    def spy(*args, **kwargs):
        plan = real(*args, **kwargs)
        seen.append(plan)
        return plan

    monkeypatch.setattr(scidb_foreach, "build_save_plan", spy)
    return seen


def _record_ids(db, type_name):
    return sorted(
        r[0]
        for r in db._duck.fetchall("SELECT record_id FROM _record WHERE type = ?", [type_name])
    )


def test_two_output_plan(db, plans):
    for_each(two_outputs, {"x": PlanIn}, [PlanOutA, PlanOutB], subject=[1, 2], db=db)
    (plan,) = plans
    assert plan.output_names == ["PlanOutA", "PlanOutB"]
    assert set(plan.batches) == {(0, "normal"), (1, "normal")}
    assert [len(items) for items in plan.batches.values()] == [2, 2]
    assert plan.generated == [] and plan.no_output_reasons == {}

    for items in plan.batches.values():
        for item in items:
            assert [e.param for e in item.edges] == ["x"]
    # The two outputs of one call share its invocation; the two calls differ.
    by_subject: dict = {}
    for items in plan.batches.values():
        for item in items:
            by_subject.setdefault(item.metadata["subject"], set()).add(
                item.metadata["__invocation_id"]
            )
    assert all(len(ids) == 1 for ids in by_subject.values())
    assert len({next(iter(ids)) for ids in by_subject.values()}) == 2


def test_distribute_plan(db, plans):
    for_each(
        three_pieces, {"x": PlanIn}, [PlanPieces], subject=[1, 2], distribute=True, db=db
    )
    (plan,) = plans
    (items,) = plan.batches.values()
    assert len(items) == 6
    assert sorted({str(i.metadata["trial"]) for i in items}) == ["1", "2", "3"]
    assert all(len(i.edges) == 1 for i in items)
    # Every piece of one call carries that call's invocation id.
    assert len({i.metadata["__invocation_id"] for i in items}) == 2
    assert len(_record_ids(db, "PlanPieces")) == 6


def test_plan_writes_nothing(db, plans, monkeypatch):
    # Applying nothing: whatever reaches the database came from the plan.
    monkeypatch.setattr(
        scidb_foreach, "apply_save_plan", lambda plan, state, database: scidb_foreach.SaveReport()
    )
    for_each(two_outputs, {"x": PlanIn}, [PlanOutA, PlanOutB], subject=[1, 2], db=db)
    (plan,) = plans
    assert sum(len(items) for items in plan.batches.values()) == 4
    assert _record_ids(db, "PlanOutA") == [] and _record_ids(db, "PlanOutB") == []


def test_rerun_keeps_record_ids(db):
    for_each(two_outputs, {"x": PlanIn}, [PlanOutA, PlanOutB], subject=[1, 2], db=db)
    first = (_record_ids(db, "PlanOutA"), _record_ids(db, "PlanOutB"))
    for_each(two_outputs, {"x": PlanIn}, [PlanOutA, PlanOutB], subject=[1, 2], db=db)
    assert (_record_ids(db, "PlanOutA"), _record_ids(db, "PlanOutB")) == first
    assert all(len(ids) == 2 for ids in first)
