"""What only the run knows (scidb.provenance.run_summary, `_run_summary`).

Every ``for_each`` execution leaves one row, including a run that saved
nothing, and the run's failed / no-data locations name it.
"""

import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.provenance.run_summary import (
    RUN_RECORDED_EVENT,
    RunRecorder,
    run_summaries,
    run_summary,
)
from scifor import LoopHooks
from scifor.project import schema_order

SCHEMA = ["subject", "trial"]


class SumRaw(BaseVariable):
    pass


class SumOut(BaseVariable):
    pass


def sum_double(signal):
    return signal * 2


def sum_fragile(signal):
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
    database = configure_database(tmp_path / "sum.duckdb", SCHEMA)
    SumRaw.save(1.0, subject="S01", trial="1")
    SumRaw.save(9.0, subject="S01", trial="2")
    SumRaw.save(2.0, subject="S02", trial="1")
    yield database
    database.close()


def _run(fn, **kwargs):
    return for_each(
        fn, {"signal": SumRaw}, [SumOut],
        subject=["S01", "S02"], trial=["1", "2"], **kwargs,
    )


class TestRecorded:
    def test_a_saving_run_shares_the_run_row_id(self, db):
        _run(sum_double)
        (s,) = run_summaries(db._duck, "sum_double")
        assert s.computed == 3
        assert s.failed == 0
        # S02/2 has no SumRaw: it is left out before the loop, not counted.
        assert s.no_data == 0
        assert s.total == 3
        assert s.saved_records == 3
        assert s.output_types == ("SumOut",)
        assert s.verdict == "ok"
        assert s.duration_ms is not None and s.duration_ms >= 0
        assert s.language == "python"
        run_ids = [r[0] for r in db._duck.fetchall("SELECT run_id FROM _run")]
        assert run_ids == [s.run_id]
        assert run_summary(db._duck, s.run_id) == s

    def test_a_run_that_saves_nothing_is_still_recorded(self, db):
        _run(sum_double)
        _run(sum_double, skip_computed=True)
        newest, first = run_summaries(db._duck, "sum_double")
        assert newest.run_id != first.run_id
        assert newest.computed == 0
        assert newest.up_to_date == 3
        assert newest.saved_records == 0
        assert newest.settings["skip_computed"] is True
        assert newest.verdict == "ok"
        # No second `_run` row: nothing reached the graph.
        assert db._duck.fetchone("SELECT COUNT(*) FROM _run")[0] == 1
        assert "3 already up to date" in newest.status_line()

    def test_failures_are_counted_and_name_their_run(self, db):
        _run(sum_fragile)
        (s,) = run_summaries(db._duck, "sum_fragile")
        assert (s.computed, s.failed, s.no_data) == (2, 1, 0)
        assert s.verdict == "partial"
        assert s.notes["failures"][0]["reason"] == "ValueError: too big"
        rows = db._duck.fetchall(
            "SELECT outcome, run_id FROM _run_outcome ORDER BY outcome"
        )
        assert rows == [("failed", s.run_id)]

    def test_on_error_stop_is_recorded_before_the_reraise(self, db):
        with pytest.raises(ValueError, match="too big"):
            _run(sum_fragile, on_error="stop")
        (s,) = run_summaries(db._duck, "sum_fragile")
        assert s.stopped is True
        assert s.verdict == "stopped"
        assert s.settings["on_error"] == "stop"
        assert s.computed == 1  # S01/1 completed before S01/2 raised

    def test_locations_and_where_are_kept(self, db):
        _run(sum_double, locations={"include": [[["subject", "S01"]]]})
        (s,) = run_summaries(db._duck, "sum_double")
        assert s.settings["locations"]["include"] == [[["subject", "S01"]]]
        assert s.settings["save"] is True

    def test_a_dry_run_records_nothing(self, db):
        _run(sum_double, dry_run=True)
        assert run_summaries(db._duck, "sum_double") == []

    def test_outputs_filter_and_limit(self, db):
        _run(sum_double)
        _run(sum_double)
        assert len(run_summaries(db._duck, "sum_double", outputs=["SumOut"])) == 2
        assert run_summaries(db._duck, "sum_double", outputs=["Other"]) == []
        assert len(run_summaries(db._duck, "sum_double", limit=1)) == 1
        assert run_summaries(db._duck, "nobody") == []


class TestEvent:
    def test_the_caller_is_told_the_run_id(self, db):
        events = []
        _run(sum_double, hooks=LoopHooks(progress_fn=events.append))
        recorded = [e for e in events if e["event"] == RUN_RECORDED_EVENT]
        (s,) = run_summaries(db._duck, "sum_double")
        assert [e["run_id"] for e in recorded] == [s.run_id]
        # It carries the positional fields every progress consumer reads.
        assert {"current", "total", "completed", "skipped"} <= set(recorded[0])
        # ... and comes after the loop's own summary.
        names = [e["event"] for e in events]
        assert names.index("summary") < names.index(RUN_RECORDED_EVENT)

    def test_a_pipeline_step_report_lists_its_runs(self, db):
        from scidb import Pipeline

        p = Pipeline("sum_pipe")
        with p:
            for_each(
                sum_double, {"signal": SumRaw}, [SumOut],
                subject=["S01", "S02"], trial=["1", "2"],
            )
        p.run_all(skip_computed=False)
        (s,) = run_summaries(db._duck, "sum_double")
        (entry,) = p.last_run_report
        assert entry["run_ids"] == [s.run_id]


class TestRecorder:
    def test_no_database_is_not_an_error(self):
        assert RunRecorder("f").finish(None, output_types=["X"]) is None

    def test_status_line_wording(self, db):
        r = RunRecorder("f")
        r.observe({"event": "summary", "total": 5, "completed": 2, "skipped": 2, "no_data": 1})
        run_id = r.finish(db, output_types=["SumOut"], up_to_date=4)
        s = run_summary(db._duck, run_id)
        assert s.status_line() == (
            "9 combinations: 2 computed, 4 already up to date, 1 failed, 1 no data, "
            "1 not reached."
        )
