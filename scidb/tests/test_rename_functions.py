"""Results across a rename (``scidb.rename_functions``;
``.claude/plan-library-fork-chain.md`` R2).

Share as library / Make my own copy rename a function (``double`` ->
``fakelib.double``). When its code is unchanged, its results are re-recorded
under the new name without calling it: the next run of the new name skips,
the newest record names the new function, the old history is kept.
"""

from __future__ import annotations

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each, names, rename_functions
from scidb.foreach_config import function_hash_for

CALLS: list[str] = []


@pytest.fixture
def db(tmp_path):
    from scifor.pathinput import clear_project_root, set_project_root

    from scidb import schema_order

    (tmp_path / "scistack.toml").write_text('modules = []\npackages = ["fakelib"]\n')
    set_project_root(tmp_path)
    names.clear_cache()
    schema_order.clear_cache()
    _scifor.set_schema([])
    database = configure_database(tmp_path / "rename.duckdb", ["subject"])
    created: list[str] = []
    yield database, created
    database.close()
    for name in created:
        BaseVariable.unregister(name)
    _scifor.set_schema([])
    clear_project_root()
    names.clear_cache()
    schema_order.clear_cache()


def _counted(module: str, name: str):
    """Identical source (so an identical hash) whatever *module* says."""

    def f(x):
        CALLS.append(name)
        return np.asarray(x, dtype=float) * 2

    f.__name__ = name
    f.__module__ = module
    return f


def _var(name: str, created: list):
    created.append(name)
    return type(BaseVariable)(name, (BaseVariable,), {"__module__": "test_rename_functions"})


def _names(db) -> set:
    return {r[0] for r in db._duck._fetchall("SELECT function_name FROM _invocation")}


def _latest_fn(db, type_name: str) -> set:
    rows = db._duck._fetchall(
        "SELECT inv.function_name FROM _record r "
        "JOIN _record_save s ON s.record_id = r.record_id "
        "JOIN _invocation_output io ON io.output_record_id = r.record_id "
        "JOIN _invocation inv ON inv.invocation_id = io.invocation_id "
        "WHERE r.type = ? AND s.timestamp = (SELECT max(s2.timestamp) FROM _record_save s2 "
        "JOIN _record r2 ON r2.record_id = s2.record_id "
        "WHERE r2.type = r.type AND r2.schema_id = r.schema_id)",
        [type_name],
    )
    return {r[0] for r in rows}


def test_an_unchanged_function_is_carried_and_not_run_again(db):
    database, created = db
    Raw, Out = _var("RenRaw", created), _var("RenOut", created)
    Raw.save(np.array([1.0, 2.0]), subject="S01")
    Raw.save(np.array([3.0]), subject="S02")
    old = _counted("my_analysis", "double")
    for_each(old, {"x": Raw}, [Out], subject=[])
    assert _names(database) == {"double"}

    new = _counted("fakelib.math", "double")
    report = rename_functions(database, {"double": "fakelib.double"},
                              {"fakelib.double": function_hash_for(new)})
    assert report.carried == {"fakelib.double": 2} and report.not_carried == []
    assert _names(database) == {"double", "fakelib.double"}
    assert _latest_fn(database, "RenOut") == {"fakelib.double"}

    n_inv = len(database._duck._fetchall("SELECT 1 FROM _invocation"))
    CALLS.clear()
    for_each(new, {"x": Raw}, [Out], subject=[], skip_computed=True)
    assert CALLS == [], "the carried results were not current: the function ran"
    assert len(database._duck._fetchall("SELECT 1 FROM _invocation")) == n_inv
    assert list(Out.load(subject="S01").data) == [2.0, 4.0]

    again = rename_functions(database, {"double": "fakelib.double"},
                             {"fakelib.double": function_hash_for(new)})
    assert again.invocations == 0, "a second pass wrote again"


def test_changed_code_is_not_carried_and_says_why(db):
    database, created = db
    Raw, Out = _var("RenRaw2", created), _var("RenOut2", created)
    Raw.save(np.array([1.0]), subject="S01")
    for_each(_counted("my_analysis", "double"), {"x": Raw}, [Out], subject=[])
    report = rename_functions(database, {"double": "fakelib.double"},
                              {"fakelib.double": "0123456789abcdef"})
    assert report.carried == {}
    assert report.not_carried[0]["function"] == "fakelib.double"
    assert "code changed" in report.not_carried[0]["reason"]
    assert _names(database) == {"double"}


def test_a_downstream_step_consumes_the_carried_records(db):
    database, created = db
    Raw, Mid, Out = _var("RenRaw3", created), _var("RenMid3", created), _var("RenOut3", created)
    Raw.save(np.array([1.0]), subject="S01")
    for_each(_counted("my_analysis", "double"), {"x": Raw}, [Mid], subject=[])
    for_each(_counted("my_analysis", "again"), {"x": Mid}, [Out], subject=[])
    up, down = _counted("fakelib.a", "double"), _counted("fakelib.b", "again")
    report = rename_functions(
        database,
        {"double": "fakelib.double", "again": "fakelib.again"},
        {"fakelib.double": function_hash_for(up), "fakelib.again": function_hash_for(down)},
    )
    assert report.carried == {"fakelib.double": 1, "fakelib.again": 1}
    CALLS.clear()
    for_each(up, {"x": Raw}, [Mid], subject=[], skip_computed=True)
    for_each(down, {"x": Mid}, [Out], subject=[], skip_computed=True)
    assert CALLS == []
    assert list(Out.load(subject="S01").data) == [4.0]


def test_no_history_is_reported_not_carried(db):
    database, _ = db
    report = rename_functions(database, {"never_ran": "fakelib.never_ran"},
                              {"fakelib.never_ran": "x"})
    assert report.not_carried == [
        {"function": "fakelib.never_ran", "reason": "no results recorded under never_ran"}]


def test_an_invocation_from_an_older_id_recipe_runs_once_more(db):
    """Before 2026-10-23 the function name was not part of invocation_id. An
    invocation stored under such an id is the same computation, but node
    state (which predicts today's ids) never finds it, so the skip gate
    recomputes it ONCE; after that the run skips again."""
    database, created = db
    Raw, Out = _var("RenRaw5", created), _var("RenOut5", created)
    Raw.save(np.array([1.0]), subject="S01")
    fn = _counted("my_analysis", "double")
    for_each(fn, {"x": Raw}, [Out], subject=[])
    duck = database._duck
    (inv,) = duck._fetchone("SELECT invocation_id FROM _invocation WHERE function_name = 'double'")
    # What an old database holds: an invocation id the current recipe does
    # not produce, AND a record id that differs too (a record id includes the
    # invocation id), so a re-run writes NEW rows rather than the same ones.
    (rid,) = duck._fetchone(
        "SELECT output_record_id FROM _invocation_output WHERE invocation_id = ?", [inv])
    _rekey(duck, ("_invocation", "_invocation_input", "_invocation_output", "_run_invocation"),
           "invocation_id", inv, "0" * 16)
    _rekey(duck, ("_record", "_record_save", "RenOut5_data"), "record_id", rid, "f" * 16)
    _rekey(duck, ("_invocation_output",), "output_record_id", rid, "f" * 16)

    CALLS.clear()
    for_each(fn, {"x": Raw}, [Out], subject=[], skip_computed=True)
    assert CALLS == ["double"], "an old-recipe invocation was taken as current"
    CALLS.clear()
    for_each(fn, {"x": Raw}, [Out], subject=[], skip_computed=True)
    assert CALLS == [], "the re-run did not write a current invocation"


def _rekey(duck, tables, column: str, old: str, new: str) -> None:
    """Move every row whose *column* is *old* to *new*: read the rows, delete
    them, insert them re-keyed (an UPDATE of a key column is unreliable in
    DuckDB, and inserting first can clash with the row still there)."""
    for table in tables:
        cols = [r[0] for r in duck._fetchall(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ? "
            "ORDER BY ordinal_position", [table])]
        quoted = ", ".join(f'"{c}"' for c in cols)
        rows = duck._fetchall(f'SELECT {quoted} FROM "{table}" WHERE "{column}" = ?', [old])
        duck._execute(f'DELETE FROM "{table}" WHERE "{column}" = ?', [old])
        at = cols.index(column)
        marks = ", ".join("?" for _ in cols)
        for row in rows:
            values = list(row)
            values[at] = new
            duck._execute(f'INSERT INTO "{table}" ({quoted}) VALUES ({marks})', values)
