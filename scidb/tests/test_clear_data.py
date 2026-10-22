"""Clearing every record while keeping the pipeline (``scidb.clear_data``).

``.claude/plan-clear-data.md``; user decisions 2026-10-21. What is pinned:

1. **The plan writes nothing**, and the clear removes exactly what it counted.
2. **Every record and its run history go**; the run LOG (``_run``), the
   variable registrations, exclusions, tombstones and pin rows stay.
3. **A re-run after a clear reproduces the same record ids** (content
   identity is untouched).
4. **An emptied table takes a new column set** on the next save.
5. **All or nothing**: a failure, a changed plan or a missing reason deletes
   nothing.
6. Pins are released; one tombstone is written; ``--backup`` copies the file.
7. **A GUI project is refused** (its canvas is partly derived from history).
8. **Every scidb table is classified** as cleared or kept, so a new history
   table cannot survive a clear unnoticed.
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
import scifor as _scifor
from scidb import BaseVariable, ColumnSetChangedError, configure_database, for_each
from scidb import clear_data as clear_mod
from scidb.clear_data import (
    CLEARED_TABLES,
    KEPT_TABLES,
    ClearRefused,
    clear_data,
    clear_plan,
)
from scidb.exclusions import exclude_schema
from scidb.inspect.cli import main as cli_main
from scidb.variant_delete import tombstones
from scidb.variant_pins import active_pins, pin_history, pin_variant

SCHEMA = ["subject", "trial"]
SUBJECTS = ["01", "02"]


class CdRaw(BaseVariable):
    pass


class CdFiltered(BaseVariable):
    pass


class CdTable(BaseVariable):
    pass


def cd_bandpass(signal, low_hz):
    return np.asarray(signal) * float(low_hz)


def _seed(db):
    for subject in SUBJECTS:
        CdRaw.save(np.array([1.0, 2.0]), subject=subject, trial=1)
    for low in (10, 20):
        for_each(
            cd_bandpass, {"signal": CdRaw, "low_hz": low}, [CdFiltered],
            subject=list(SUBJECTS), trial=[],
        )


def _build(path):
    _scifor.set_schema([])
    db = configure_database(path, SCHEMA)
    _seed(db)
    return db


@pytest.fixture
def db(tmp_path):
    database = _build(tmp_path / "cd.duckdb")
    yield database
    database.close()
    _scifor.set_schema([])


def _n(db, sql: str) -> int:
    return int(db._duck._fetchone(sql)[0])


def _rids(db, type_name: str) -> set:
    return {r for (r,) in db._duck._fetchall("SELECT record_id FROM _record WHERE type = ?", [type_name])}


def _snapshot(db) -> dict:
    duck = db._duck
    return {
        t: _n(db, f'SELECT COUNT(*) FROM "{t}"')
        for t in list(CLEARED_TABLES) + ["_run", CdRaw.table_name(), CdFiltered.table_name()]
        if duck._table_exists(t)
    }


class TestPlan:
    def test_the_plan_writes_nothing_and_counts_by_variable(self, db):
        before = _snapshot(db)
        plan = clear_plan(db)
        assert _snapshot(db) == before
        assert plan.by_variable == {"CdFiltered": 4, "CdRaw": 2}
        assert plan.total_records == 6
        assert plan.runs_kept >= 2
        assert plan.table_rows[CdFiltered.table_name()] == 4
        assert not plan.has_canvas


class TestClear:
    def test_records_and_history_go_the_run_log_stays(self, db):
        runs_before = _n(db, "SELECT COUNT(*) FROM _run")
        variables_before = db._duck._fetchall("SELECT * FROM _variables ORDER BY 1")
        clear_data(db, "debugging")
        for table in CLEARED_TABLES:
            if db._duck._table_exists(table):
                assert _n(db, f'SELECT COUNT(*) FROM "{table}"') == 0, table
        assert _n(db, f'SELECT COUNT(*) FROM "{CdFiltered.table_name()}"') == 0
        assert _n(db, "SELECT COUNT(*) FROM _run") == runs_before
        assert db._duck._fetchall("SELECT * FROM _variables ORDER BY 1") == variables_before
        # The views are still queryable.
        assert _n(db, f'SELECT COUNT(*) FROM "{CdFiltered.view_name()}"') == 0

    def test_a_rerun_reproduces_the_same_record_ids(self, db):
        raw, filtered = _rids(db, "CdRaw"), _rids(db, "CdFiltered")
        clear_data(db, "debugging")
        assert _rids(db, "CdFiltered") == set()
        _seed(db)
        assert _rids(db, "CdRaw") == raw
        assert _rids(db, "CdFiltered") == filtered

    def test_an_emptied_table_takes_a_new_column_set(self, db):
        CdTable.save(pd.DataFrame({"a": [1.0, 2.0]}), subject="01", trial=1)
        with pytest.raises(ColumnSetChangedError):
            CdTable.save(pd.DataFrame({"b": [1.0]}), subject="02", trial=1)
        clear_data(db, "new columns")
        CdTable.save(pd.DataFrame({"b": [1.0]}), subject="02", trial=1)
        assert db.stored_data_columns(CdTable.table_name()) == ["b"]

    def test_declarations_and_audit_survive_pins_are_released(self, db):
        exclude_schema("bad subject", db=db, subject="02")
        pin_variant(db, CdFiltered, {"cd_bandpass.low_hz": 10}, "the good one")
        assert "CdFiltered" in active_pins(db)
        overrides = _n(db, "SELECT COUNT(*) FROM __scidb_schema_overrides")

        result = clear_data(db, "debugging")
        assert result.released_pins == ["CdFiltered"]
        assert active_pins(db) == {}
        assert len(pin_history(db, "CdFiltered")) == 1, "the pin row is kept"
        assert _n(db, "SELECT COUNT(*) FROM __scidb_schema_overrides") == overrides
        (stone,) = tombstones(db)
        assert stone.reason == "debugging"
        assert stone.targets == [{"all": True}]
        assert stone.by_variable == {"CdFiltered": 4, "CdRaw": 2}


class TestAtomic:
    def test_a_failure_deletes_nothing(self, db, monkeypatch):
        before = _snapshot(db)

        def failing(*_a, **_k):
            raise RuntimeError("simulated failure")

        monkeypatch.setattr("scidb.variant_delete.write_tombstone", failing)
        with pytest.raises(RuntimeError, match="simulated"):
            clear_data(db, "x")
        assert _snapshot(db) == before
        assert tombstones(db) == []

    def test_a_changed_plan_is_refused(self, db):
        before = _snapshot(db)
        with pytest.raises(ValueError, match="changed since the plan"):
            clear_data(db, "x", expect_fingerprint="0000000000000000")
        assert _snapshot(db) == before

    def test_a_reason_is_required(self, db):
        with pytest.raises(ValueError, match="reason"):
            clear_data(db, "  ")


class TestCanvasAndBackup:
    def test_a_gui_project_is_refused(self, db):
        db._duck._execute("CREATE TABLE _pipelines (pipeline_id VARCHAR)")
        before = _snapshot(db)
        assert clear_plan(db).has_canvas
        with pytest.raises(ClearRefused, match="GUI canvas"):
            clear_data(db, "x")
        assert _snapshot(db) == before
        clear_data(db, "x", allow_canvas=True)
        assert _n(db, "SELECT COUNT(*) FROM _record") == 0

    def test_backup_copies_the_file_first(self, db):
        result = clear_data(db, "x", backup=True)
        path = result.backup_path
        assert path is not None and path.is_file() and path.stat().st_size > 0
        assert path.name.startswith(db.dataset_db_path.name + ".bak-")
        assert path.suffix != ".duckdb", "discovery must never mistake it for a project"


class TestClassification:
    def test_every_scidb_table_is_classified(self, db):
        """Every table a used database holds (data tables aside) is either
        emptied or kept with a reason in ``clear_data``."""
        exclude_schema("x", db=db, subject="02")
        pin_variant(db, CdFiltered, {"cd_bandpass.low_hz": 10}, "x")
        clear_data(db, "x")  # creates the tombstone table too
        duck = db._duck
        data_tables = {t for (t,) in duck._fetchall("SELECT DISTINCT table_name FROM _registered_types")}
        tables = {
            t
            for (t,) in duck._fetchall(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            )
        }
        unclassified = sorted(tables - data_tables - set(CLEARED_TABLES) - set(KEPT_TABLES))
        assert unclassified == [], (
            "classify each table in scidb/clear_data.py (CLEARED via bundle "
            "HISTORY_TABLES, or KEPT_TABLES with a reason)"
        )

    def test_kept_and_cleared_do_not_overlap(self):
        assert not set(CLEARED_TABLES) & set(KEPT_TABLES)
        assert "_run" in KEPT_TABLES and "_run_invocation" in CLEARED_TABLES


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "cd_cli.duckdb"
    _build(path).close()
    _scifor.set_schema([])
    return path


class TestCli:
    def test_dry_run_then_clear(self, db_path, capsys):
        base = ["--db", str(db_path)]
        rc = cli_main(base + ["clear-data", "--reason", "debug", "--json"])
        out = capsys.readouterr().out
        assert rc == 0, out
        plan = json.loads(out)
        assert plan["by_variable"] == {"CdFiltered": 4, "CdRaw": 2}

        rc = cli_main(base + ["clear-data", "--reason", "debug"])
        assert "Nothing was deleted" in capsys.readouterr().out

        rc = cli_main(base + ["clear-data", "--reason", "debug", "--yes",
                              "--fingerprint", plan["fingerprint"]])
        out = capsys.readouterr().out
        assert rc == 0, out
        assert "clear_data" in out and "kept" in out

    def test_cli_refuses_a_gui_project(self, db_path, capsys):
        import duckdb

        con = duckdb.connect(str(db_path))
        con.execute("CREATE TABLE _pipelines (pipeline_id VARCHAR)")
        con.close()
        rc = cli_main(["--db", str(db_path), "clear-data", "--reason", "x", "--yes"])
        captured = capsys.readouterr()
        assert rc == 1
        assert "GUI canvas" in captured.err


def test_module_names_its_own_tables():
    """The classification is read from bundle.py, not restated."""
    from scidb.bundle import HISTORY_TABLES

    assert set(CLEARED_TABLES) - {"_record_save"} <= set(HISTORY_TABLES)
    assert clear_mod.KEPT_BY_CLEAR.keys() <= set(HISTORY_TABLES)
