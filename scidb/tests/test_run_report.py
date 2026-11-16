"""The run report (scidb.provenance.run_report, GitHub issue #17).

After a run the researcher verifies what it did: each input and output at
every schema-key combination, the settings used, the exclusions in force,
NaNs and other missing data. These tests run real ``for_each`` calls on a
real database and read the report back.
"""

import json

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.inspect import Inspector
from scidb.inspect.cells import describe_value, record_cells
from scidb.model.exceptions import NotFoundError
from scidb.provenance.run_report import (
    COMPUTED,
    FAILED,
    NO_DATA,
    NOTE,
    PROBLEM,
    run_report,
    run_report_data,
    run_reports,
)
from scidb.store.exclusions import exclude_schema
from scifor.project import schema_order

SCHEMA = ["subject", "trial"]


class RepRaw(BaseVariable):
    pass


class RepGain(BaseVariable):
    pass


class RepOut(BaseVariable):
    pass


class RepPeak(BaseVariable):
    pass


def rep_scale(signal, gain, factor):
    return signal * gain * factor


def rep_two(signal):
    return signal + 1, float(np.nanmax(signal))


def rep_fragile(signal):
    if np.nansum(signal) > 100:
        raise ValueError("too big")
    return signal


def rep_make_nan(signal):
    out = np.array(signal, dtype=float)
    out[0] = np.nan
    return out


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
    database = configure_database(tmp_path / "rep.duckdb", SCHEMA)
    RepRaw.save(np.array([1.0, 2.0, 3.0]), subject="S01", trial="1")
    RepRaw.save(np.array([1.0, np.nan, 3.0]), subject="S01", trial="2")
    RepRaw.save(np.array([4.0, 5.0, 6.0, 7.0]), subject="S02", trial="1")
    for subject in ("S01", "S02"):
        for trial in ("1", "2"):
            RepGain.save(2.0, subject=subject, trial=trial)
    yield database
    database.close()


def _grid(**extra):
    return dict(subject=["S01", "S02"], trial=["1", "2"], **extra)


def _latest(db, fn):
    return run_reports(db, fn)[0].run_id


def _kinds(attention):
    return {a.kind for a in attention}


class TestSummaryPart:
    def test_header_settings_inputs_outputs(self, db):
        for_each(rep_scale, {"signal": RepRaw, "gain": RepGain, "factor": 3}, [RepOut], **_grid())
        report = run_report(db, _latest(db, rep_scale))
        s = report.summary
        # S02/2 has no RepRaw: left out before the loop, so not a combination.
        assert (s.computed, s.no_data, s.failed) == (3, 0, 0)
        assert report.run["timestamp"] is not None
        assert report.settings["constants"] == {"factor": ["3"]}
        (version,) = report.settings["function_versions"]
        assert version["invocations"] == 3
        assert "rep_scale" in "".join(version["source"]["units"])
        (label,) = report.settings["run_options"]
        assert label.startswith("distribute=false")
        assert report.settings["on_error"] == "continue"
        assert [(c["param"], c["variable"], c["records"]) for c in report.inputs] == [
            ("gain", "RepGain", 3), ("signal", "RepRaw", 3),
        ]
        (out,) = report.outputs
        assert out == {
            "variable": "RepOut", "records": 3, "locations": 3,
            "new_records": 3, "resaved_records": 0,
        }
        assert "failed" not in _kinds(report.attention)
        json.dumps(report.to_dict(), default=str)  # the wire form serialises

    def test_a_rerun_is_its_own_report_and_resaves(self, db):
        for_each(rep_scale, {"signal": RepRaw, "gain": RepGain, "factor": 3}, [RepOut], **_grid())
        for_each(rep_scale, {"signal": RepRaw, "gain": RepGain, "factor": 3}, [RepOut], **_grid())
        newest, first = run_reports(db, rep_scale)
        (out,) = run_report(db, newest.run_id).outputs
        assert (out["new_records"], out["resaved_records"]) == (0, 3)
        (out,) = run_report(db, first.run_id).outputs
        assert (out["new_records"], out["resaved_records"]) == (3, 0)

    def test_a_run_that_saved_nothing_says_so(self, db):
        for_each(rep_two, {"signal": RepRaw}, [RepOut, RepPeak], **_grid())
        for_each(rep_two, {"signal": RepRaw}, [RepOut, RepPeak], skip_computed=True, **_grid())
        report = run_report(db, _latest(db, rep_two))
        assert report.summary.up_to_date == 3
        assert report.run["timestamp"] is None
        assert report.inputs == []
        assert [c["records"] for c in report.outputs] == [0, 0]
        assert any("already up to date" in n for n in report.notes)
        assert any("saved nothing" in n for n in report.notes)

    def test_exclusions_in_force_at_run_time_only(self, db):
        exclude_schema("pilot", subject="S02")
        for_each(rep_two, {"signal": RepRaw}, [RepOut, RepPeak], **_grid())
        exclude_schema("added after the run", subject="S01", trial="2")
        report = run_report(db, _latest(db, rep_two))
        assert [(e["keys"], e["reason"]) for e in report.exclusions] == [
            ({"subject": "S02"}, "pilot"),
        ]

    def test_failures_and_stop_are_flagged(self, db):
        RepRaw.save(np.array([500.0, 1.0]), subject="S02", trial="2")
        for_each(rep_fragile, {"signal": RepRaw}, [RepOut], **_grid())
        report = run_report(db, _latest(db, rep_fragile))
        failed = [a for a in report.attention if a.kind == "failed"]
        assert failed and failed[0].severity == PROBLEM
        assert "too big" in failed[0].text

    def test_unknown_run(self, db):
        with pytest.raises(NotFoundError):
            run_report(db, "nope")
        with pytest.raises(NotFoundError):
            run_report_data(db, "nope")


class TestDataPart:
    def test_every_combination_describes_inputs_and_outputs(self, db):
        for_each(rep_scale, {"signal": RepRaw, "gain": RepGain, "factor": 3}, [RepOut], **_grid())
        data = run_report_data(db, _latest(db, rep_scale))
        assert data.status_counts == {COMPUTED: 3, FAILED: 0, NO_DATA: 0}
        rows = {(r["location"]["subject"], r["location"]["trial"]): r for r in data.rows}
        row = rows[("S01", "1")]
        assert row["status"] == COMPUTED
        assert row["inputs"]["signal"]["shape_text"] == "3"
        assert row["inputs"]["gain"]["shape_text"] == "scalar"
        assert row["inputs"]["gain"]["value"] == "2.0"
        assert row["outputs"]["RepOut"]["shape_text"] == "3"
        assert row["outputs"]["RepOut"]["nan_elements"] == 0
        nan_row = rows[("S01", "2")]
        assert nan_row["inputs"]["signal"]["nan_elements"] == 1
        assert nan_row["outputs"]["RepOut"]["nan_elements"] == 1
        assert "1 of 3 values NaN" in nan_row["outputs"]["RepOut"]["findings"]
        assert rows[("S02", "1")]["outputs"]["RepOut"]["shape_text"] == "4"
        assert ("S02", "2") not in rows  # no input there: never a combination
        json.dumps(data.to_dict(), default=str)

    def test_variable_profiles_and_attention(self, db):
        for_each(rep_scale, {"signal": RepRaw, "gain": RepGain, "factor": 3}, [RepOut], **_grid())
        data = run_report_data(db, _latest(db, rep_scale))
        out = data.variables["RepOut"]
        assert out["role"] == "output" and out["records"] == 3
        (col,) = out["columns"].values()
        assert col["distinct_shapes"] == 2
        assert col["shapes"][0] == {"shape": "3", "count": 2}
        assert col["cells"]["nan_elements"] == 1
        assert data.variables["RepRaw"]["role"] == "input"
        by_kind = {}
        for a in data.attention:
            by_kind.setdefault(a.kind, []).append(a)
        assert {a.variable for a in by_kind["cells"]} == {"RepOut", "RepRaw"}
        assert {a.severity for a in by_kind["cells"] if a.variable == "RepOut"} == {PROBLEM}
        assert {a.severity for a in by_kind["cells"] if a.variable == "RepRaw"} == {NOTE}
        assert "shapes" in by_kind
        # The NaN in the output came from a NaN in the input.
        assert "carried" in by_kind and "introduced" not in by_kind
        assert data.attention[0].severity == PROBLEM  # problems first

    def test_nan_the_function_introduced_is_a_problem(self, db):
        for_each(rep_make_nan, {"signal": RepRaw}, [RepOut], subject=["S02"], trial=["1"])
        data = run_report_data(db, _latest(db, rep_make_nan))
        (item,) = [a for a in data.attention if a.kind == "introduced"]
        assert item.severity == PROBLEM and "1 combination" in item.text

    def test_two_outputs_per_combination(self, db):
        for_each(rep_two, {"signal": RepRaw}, [RepOut, RepPeak], **_grid())
        data = run_report_data(db, _latest(db, rep_two), status=COMPUTED)
        assert data.total_rows == 3
        row = next(r for r in data.rows if r["location"] == {"subject": "S02", "trial": "1"})
        assert set(row["outputs"]) == {"RepOut", "RepPeak"}
        assert row["outputs"]["RepPeak"]["value"] == "7.0"

    def test_an_older_run_keeps_its_own_records(self, db):
        """A re-run under a new input value writes new records; the first
        run's report still describes the records IT wrote."""
        for_each(rep_two, {"signal": RepRaw}, [RepOut, RepPeak], subject=["S01"], trial=["1"])
        first = _latest(db, rep_two)
        RepRaw.save(np.array([10.0, 20.0]), subject="S01", trial="1")
        for_each(rep_two, {"signal": RepRaw}, [RepOut, RepPeak], subject=["S01"], trial=["1"])
        second = _latest(db, rep_two)
        assert first != second
        (old,) = run_report_data(db, first).rows
        (new,) = run_report_data(db, second).rows
        assert old["outputs"]["RepPeak"]["value"] == "3.0"
        assert new["outputs"]["RepPeak"]["value"] == "20.0"
        assert old["inputs"]["signal"]["shape_text"] == "3"
        assert new["inputs"]["signal"]["shape_text"] == "2"

    def test_failed_rows_say_why(self, db):
        RepRaw.save(np.array([500.0, 1.0]), subject="S02", trial="2")
        for_each(rep_fragile, {"signal": RepRaw}, [RepOut], **_grid())
        data = run_report_data(db, _latest(db, rep_fragile), status=FAILED)
        (row,) = data.rows
        assert row["location"] == {"subject": "S02", "trial": "2"}
        assert "too big" in row["reason"]

    def test_limit_and_bad_status(self, db):
        for_each(rep_two, {"signal": RepRaw}, [RepOut, RepPeak], **_grid())
        run_id = _latest(db, rep_two)
        data = run_report_data(db, run_id, limit=1)
        assert len(data.rows) == 1 and data.total_rows == 3
        assert data.to_dict()["truncated"] is True
        with pytest.raises(ValueError, match="status must be one of"):
            run_report_data(db, run_id, status="bogus")


class TestCellsOwner:
    def test_describe_value(self):
        assert describe_value(np.zeros((3, 4))).shape_text == "3×4"
        assert describe_value(np.zeros(5)).nbytes == 40
        assert describe_value(2.5).shape_text == "scalar"
        assert describe_value(2.5).text == "2.5"
        assert describe_value(float("nan")).kind == "blank"
        assert describe_value(None).shape_text == "blank"
        assert describe_value("abc").shape_text == "text (3 chars)"
        assert describe_value([1, 2, 3]).shape_text == "3"
        assert describe_value({"a": 1}).shape_text == "dict (1 keys)"

    def test_record_cells_reads_superseded_versions(self, db):
        first = RepRaw.save(np.array([1.0, 2.0]), subject="S03", trial="1")
        second = RepRaw.save(np.array([1.0, 2.0, 3.0]), subject="S03", trial="1")
        cells = record_cells(db, "RepRaw", [first, second])
        assert cells[first].shape_text() == "2"
        assert cells[second].shape_text() == "3"
        assert cells[first].location == {"subject": "S03", "trial": "1"}
        assert record_cells(db, "RepRaw", []) == {}


class TestSurfaces:
    def test_inspector(self, db):
        for_each(rep_two, {"signal": RepRaw}, [RepOut, RepPeak], **_grid())
        insp = Inspector(db)
        (run,) = insp.run_reports("rep_two")
        assert insp.run_report(run.run_id).summary == run
        assert insp.run_report_data(run.run_id).total_rows == 3
        assert insp.run_reports("rep_two", outputs=["RepPeak"]) == [run]

    def test_cli(self, tmp_path, capsys):
        from scidb.inspect.cli import main

        path = str(tmp_path / "cli.duckdb")
        database = configure_database(path, SCHEMA)
        RepRaw.save(np.array([1.0, 2.0, 3.0]), subject="S01", trial="1")
        RepRaw.save(np.array([1.0, np.nan, 3.0]), subject="S01", trial="2")
        RepRaw.save(np.array([4.0, 5.0, 6.0, 7.0]), subject="S02", trial="1")
        for subject, trial in (("S01", "1"), ("S01", "2"), ("S02", "1")):
            RepGain.save(2.0, subject=subject, trial=trial)
        for_each(rep_scale, {"signal": RepRaw, "gain": RepGain, "factor": 3}, [RepOut], **_grid())
        database.close()
        assert main(["--db", path, "run-report", "rep_scale"]) == 0
        out = capsys.readouterr().out
        assert "3 combinations: 3 computed." in out
        assert "factor = 3" in out and "Needs attention" in out
        csv_path = tmp_path / "rows.csv"
        assert main(["--db", path, "run-report", "rep_scale", "--locations", "--csv", str(csv_path)]) == 0
        out = capsys.readouterr().out
        assert "Combinations (3)" in out and "values NaN" in out
        header = csv_path.read_text().splitlines()[0]
        assert header.startswith("subject,trial,status,in gain")
        assert main(["--db", path, "run-report", "rep_scale", "--list"]) == 0
        assert "rep_scale" in capsys.readouterr().out
        assert main(["--db", path, "run-report", "never_ran"]) == 1
