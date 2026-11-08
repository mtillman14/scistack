"""Run outcomes per location (scidb.provenance.outcomes, plan Stage 7).

A location where the function failed or found no data gets a ``_run_outcome``
row, and a missing row in ``expectation()`` (and so ``follow``) says why.
"""

import json
from datetime import datetime

import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scifor.project import schema_order
from scidb.provenance import expectation as ex
from scidb.provenance.coverage import follow
from scidb.provenance.outcomes import (
    FAILED,
    NO_DATA,
    Outcome,
    OutcomeCollector,
    latest_outcomes,
    outcome_for,
    record_outcomes,
)

SCHEMA = ["subject", "trial"]


class OutRaw(BaseVariable):
    pass


class OutFilt(BaseVariable):
    pass


def out_fragile(signal):
    if signal > 5:
        raise ValueError("too big")
    return signal


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    schema_order.clear_cache()
    _scifor.set_schema([])
    yield
    _scifor.set_schema([])
    schema_order.clear_cache()


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "out.duckdb", SCHEMA)
    OutRaw.save(1.0, subject="S01", trial="1")
    OutRaw.save(9.0, subject="S01", trial="2")  # out_fragile raises here
    OutRaw.save(1.0, subject="S02", trial="1")
    for_each(
        out_fragile, {"signal": OutRaw}, [OutFilt],
        subject=["S01", "S02"], trial=["1", "2"],
    )
    yield database
    database.close()


def event(no_data=False, **meta):
    return {
        "metadata": meta,
        "failure": {"reason": "ValueError: x", "no_data": no_data, "blame": "user_code", "probable": False},
    }


class TestRecorded:
    def test_a_failed_location_is_stored(self, db):
        outs = latest_outcomes(db._duck, SCHEMA, "OutFilt")
        failed = [o for o in outs if o.outcome == FAILED]
        assert [o.keys for o in failed] == [{"subject": "S01", "trial": "2"}]
        assert failed[0].reason == "ValueError: too big"
        assert failed[0].function_name == "out_fragile"
        assert failed[0].blame

    def test_the_missing_row_says_why(self, db):
        row = next(
            r for r in ex.expectation(db, OutFilt).rows
            if r.values == {"subject": "S01", "trial": "2"}
        )
        assert row.status == ex.MISSING_UNEXPECTED
        assert row.last_run is not None
        assert "out_fragile failed here when run on" in row.why
        assert "too big" in row.why
        assert json.loads(json.dumps(row.to_dict()))["last_run"]["outcome"] == FAILED

    def test_follow_shows_it(self, db):
        fol = follow(db, subject="S01", trial="2")
        step = {s.variable: s for s in fol.steps}["OutFilt"]
        assert "failed here" in step.summary
        assert fol.lost_at == ["OutFilt"]

    def test_a_present_row_is_never_explained(self, db):
        record_outcomes(db, "out_fragile", ["OutFilt"], [event(subject="S01", trial="1")])
        row = next(
            r for r in ex.expectation(db, OutFilt).rows
            if r.values == {"subject": "S01", "trial": "1"}
        )
        assert row.status == ex.PRESENT and row.last_run is None


class TestRecordOutcomes:
    def test_non_schema_keys_dropped_and_keyless_skipped(self, db):
        n = record_outcomes(
            db, "f", ["OutFilt"],
            [event(subject="S09", ColName="a"), event(ColName="b")],
        )
        assert n == 1
        (o,) = [o for o in latest_outcomes(db._duck, SCHEMA, "OutFilt") if o.function_name == "f"]
        assert o.keys == {"subject": "S09"}

    def test_newest_per_location_wins(self, db):
        record_outcomes(db, "g", ["OutFilt"], [event(subject="S05", trial="1")])
        record_outcomes(db, "g", ["OutFilt"], [event(no_data=True, subject="S05", trial="1")])
        (o,) = [o for o in latest_outcomes(db._duck, SCHEMA, "OutFilt") if o.function_name == "g"]
        assert o.outcome == NO_DATA and o.reason is None
        assert "found no input data here" in o.describe()

    def test_only_runs_listing_the_variable(self, db):
        record_outcomes(db, "h", ["Other"], [event(subject="S05", trial="1")])
        assert not [o for o in latest_outcomes(db._duck, SCHEMA, "OutFilt") if o.function_name == "h"]

    def test_many_rows_are_chunked(self, db):
        events = [event(subject=f"S{i}", trial="1") for i in range(1203)]
        assert record_outcomes(db, "bulk", ["OutFilt"], events) == 1203

    def test_nothing_to_write(self, db):
        assert record_outcomes(db, "f", ["OutFilt"], []) == 0


class TestMatching:
    def test_a_coarser_outcome_explains_a_finer_row(self):
        o = Outcome("f", ("X",), {"subject": "S02"}, FAILED, "user_code", False, "E: x", datetime(2026, 1, 1))
        assert outcome_for([o], {"subject": "S02", "trial": "3"}) is o
        assert outcome_for([o], {"subject": "S03", "trial": "3"}) is None

    def test_a_finer_outcome_does_not_explain_a_coarser_row(self):
        o = Outcome("f", ("X",), {"subject": "S02", "trial": "1"}, FAILED, None, False, "E", datetime(2026, 1, 1))
        assert outcome_for([o], {"subject": "S02"}) is None

    def test_collector_keeps_only_skips(self):
        c = OutcomeCollector()
        c.observe({"event": "combo_done", "metadata": {"subject": 1}})
        c.observe({"event": "combo_skip", "metadata": {"subject": 1}, "failure": {"reason": "E"}})
        c.observe({"event": "combo_skip", "metadata": {"subject": 2}, "failure": None})
        assert [e["metadata"] for e in c.events] == [{"subject": 1}]
