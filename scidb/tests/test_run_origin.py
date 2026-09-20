"""A run's origin — which surfaces it read — recorded on `_run`.

Rule 3 of docs/claude/intent-and-fact.md. The origin is ambient
(`scidb.intent.run_origin` / `set_ambient_origin` / `$SCIDB_RUN_ORIGIN`),
because it is decided by whoever starts a run and read by the one place that
records it, several call chains apart.
"""

from __future__ import annotations

import pandas as pd
import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each
from scidb.intent import (
    ORIGIN_ENV_VAR,
    ORIGIN_GUI,
    ORIGIN_SCRIPT,
    IntentError,
    current_origin,
    run_origin,
    set_ambient_origin,
)
from scidb.provenance_query import latest_runs


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "origin.duckdb", ["subject", "trial"])
    yield db
    _scifor.set_schema([])
    db.close()


@pytest.fixture(autouse=True)
def _reset_origin(monkeypatch):
    """Every test starts unlabelled: no env var, no ambient origin."""
    monkeypatch.delenv(ORIGIN_ENV_VAR, raising=False)
    set_ambient_origin(ORIGIN_SCRIPT)
    yield
    set_ambient_origin(ORIGIN_SCRIPT)


class TestAmbient:
    def test_a_run_nobody_labelled_came_from_code(self):
        assert current_origin() == ORIGIN_SCRIPT

    def test_the_context_manager_scopes_the_label(self):
        with run_origin(ORIGIN_GUI):
            assert current_origin() == ORIGIN_GUI
        assert current_origin() == ORIGIN_SCRIPT

    def test_the_env_var_is_the_cross_process_fallback(self, monkeypatch):
        set_ambient_origin(ORIGIN_SCRIPT)
        # The context variable, when unset, defers to the environment; an
        # explicit ambient label beats it. Simulate "unset" by clearing.
        from scidb import intent as _intent

        _intent._current_origin.set(None)
        monkeypatch.setenv(ORIGIN_ENV_VAR, ORIGIN_GUI)
        assert current_origin() == ORIGIN_GUI

    def test_an_unknown_label_is_refused(self):
        with pytest.raises(IntentError):
            run_origin("somewhere")
        with pytest.raises(IntentError):
            set_ambient_origin("somewhere")

    def test_an_unknown_env_value_falls_back_to_script(self, monkeypatch):
        from scidb import intent as _intent

        _intent._current_origin.set(None)
        monkeypatch.setenv(ORIGIN_ENV_VAR, "elsewhere")
        assert current_origin() == ORIGIN_SCRIPT


class TestRecorded:
    def _run(self, db, origin=None):
        class Src(BaseVariable):
            pass

        class Out(BaseVariable):
            pass

        Src.save(pd.DataFrame({"a": [1.0], "b": [2.0]}), subject="01", trial="1")

        def pick(value):
            return float(pd.Series(value).iloc[0])

        if origin:
            with run_origin(origin):
                for_each(pick, {"value": Src["a"]}, [Out], subject=[], trial=[])
        else:
            for_each(pick, {"value": Src["a"]}, [Out], subject=[], trial=[])
        return latest_runs(db._duck, ["pick"])["pick"]

    def test_a_script_run_says_so(self, db):
        assert self._run(db)["origin"] == ORIGIN_SCRIPT

    def test_a_gui_run_says_so(self, db):
        assert self._run(db, ORIGIN_GUI)["origin"] == ORIGIN_GUI

    def test_the_latest_run_carries_what_it_bound(self, db):
        last = self._run(db)
        assert last["selectors"] == {"value": {"columns": ["a"], "iterate": False}}

    def test_a_function_that_never_ran_is_absent(self, db):
        assert latest_runs(db._duck, ["never"]) == {}


class TestRecordedSchemaKeys:
    """The `schema_location` aspect's fact: where a function last ran."""

    def test_the_iterated_keys_are_read_off_the_records(self, db):
        from scidb.provenance_query import recorded_schema_keys

        class Src2(BaseVariable):
            pass

        class Out2(BaseVariable):
            pass

        Src2.save(pd.DataFrame({"a": [1.0], "b": [2.0]}), subject="01", trial="1")

        def pick2(value):
            return float(pd.Series(value).iloc[0])

        for_each(pick2, {"value": Src2["a"]}, [Out2], subject=[], trial=[])
        assert recorded_schema_keys(db._duck, "pick2", ["subject", "trial"]) == [
            "subject",
            "trial",
        ]

    def test_a_coarser_run_reports_its_own_level(self, db):
        """A run iterating subject only saves subject-level records, and that
        — not every key the dataset has — is what a re-run should default to."""
        from scidb.provenance_query import recorded_schema_keys

        class Src3(BaseVariable):
            pass

        class Out3(BaseVariable):
            pass

        Src3.save(pd.DataFrame({"a": [1.0, 2.0]}), subject="01")

        def pick3(value):
            return float(len(value))

        for_each(pick3, {"value": Src3}, [Out3], subject=[])
        assert recorded_schema_keys(db._duck, "pick3", ["subject", "trial"]) == ["subject"]

    def test_no_history_is_none(self, db):
        from scidb.provenance_query import recorded_schema_keys

        assert recorded_schema_keys(db._duck, "never", ["subject", "trial"]) is None


class TestInputSchemaKeys:
    """A variable's inherent level, read off its records, and the union rule
    for inputs at different levels."""

    def test_variable_levels_are_read_off_records(self, db):
        from scidb.provenance_query import variable_schema_keys

        class Coarse(BaseVariable):
            pass

        class Fine(BaseVariable):
            pass

        Coarse.save(pd.DataFrame({"a": [1.0]}), subject="01")
        Fine.save(pd.DataFrame({"a": [1.0]}), subject="01", trial="1")
        levels = variable_schema_keys(db._duck, ["Coarse", "Fine", "Absent"], ["subject", "trial"])
        assert levels == {"Coarse": ["subject"], "Fine": ["subject", "trial"]}

    def test_the_finest_level_is_the_union_in_dataset_order(self):
        from scidb.provenance_query import finest_schema_keys

        keys = ["subject", "session", "trial"]
        assert finest_schema_keys([["subject", "trial"], ["subject", "session"]], keys) == keys
        assert finest_schema_keys([["subject"]], keys) == ["subject"]
        assert finest_schema_keys([], keys) == []


class TestDatasetLevel:
    """No schema key at all is a LEVEL — once over the whole dataset — and
    must never be mistaken for "no information" (which is `None`)."""

    def test_a_variable_saved_with_no_keys_is_dataset_level(self, db):
        from scidb.provenance_query import variable_schema_keys

        class Whole(BaseVariable):
            pass

        Whole.save(pd.DataFrame({"a": [1.0, 2.0]}))
        assert variable_schema_keys(db._duck, ["Whole"], ["subject", "trial"]) == {"Whole": []}

    def test_a_dataset_level_run_records_an_empty_level(self, db):
        from scidb.provenance_query import recorded_schema_keys

        class Whole2(BaseVariable):
            pass

        class Once(BaseVariable):
            pass

        Whole2.save(pd.DataFrame({"a": [1.0, 2.0]}))

        def count(value):
            return float(len(value))

        # schema_keys=[]: pool everything into ONE call.
        for_each(count, {"value": Whole2}, [Once], schema_keys=[])
        assert len(Once.load(as_df=True, version="all")) == 1
        assert recorded_schema_keys(db._duck, "count", ["subject", "trial"]) == []
