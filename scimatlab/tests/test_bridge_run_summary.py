"""A MATLAB run is recorded like a Python run (``_run_summary``).

The loop runs in MATLAB, so Python learns its counts and its failed
locations from ``scifor.matlab_loop.assemble``, which keeps the loop's
events; ``for_each_save`` takes them and writes the run's record
(scidb.provenance.run_summary), so the run report covers MATLAB runs
(docs/claude/run-report.md).

Runs entirely in Python without MATLAB: prepare -> the plan and summary
MATLAB's scifor would send -> for_each_save.
"""

import json
import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import numpy as np
import pandas as pd
import pytest
from scidb.provenance.run_report import run_report, run_report_data
from scidb.provenance.run_summary import run_summaries
from scidb.store.database import configure_database
from scifor import matlab_loop
from scimatlab.bridge import for_each_prepare, for_each_save, register_matlab_variable

SCHEMA = ["subject"]


@pytest.fixture
def db(tmp_path):
    import scifor as _scifor

    _scifor.set_schema([])
    db = configure_database(tmp_path / "runsum.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


def _prepare(db, subjects):
    raw = register_matlab_variable("Raw_RS")
    register_matlab_variable("Out_RS")
    for s in (1, 2):
        db.save_variable(raw, np.array([1.0, 2.0, 3.0]) * s, subject=s)
    return for_each_prepare(
        "double_rs",
        "hash0",
        {"x": {"kind": "var_type", "type_name": "Raw_RS"}},
        ["Out_RS"],
        {"subject": list(subjects)},
        db=db,
    )


def _matlab_loop_ran(subjects, *, completed, failures=()):
    """What +scifor/for_each.m does around its loop: plan, then assemble."""
    desc = {
        "fn_name": "double_rs", "fn_nargin": -1, "fn_nargout": 1, "call_nargout": 1,
        "outputs": {"kind": "default"},
        "metadata": [{"key": "subject", "values": {"numbers": [0, len(subjects)]}}],
        "schema_keys": [],
        "options": {"where": [], "locations": "", "record_id_column": ""},
        "inputs": [],
    }
    numbers = np.asarray([float(s) for s in subjects], dtype="<f8").tobytes()
    plan = json.loads(matlab_loop.plan(json.dumps(desc), numbers)["json"])
    matlab_loop.assemble(plan["token"], json.dumps({
        "completed": completed, "skipped": len(subjects) - completed, "elapsed": 0.1,
        "failures": list(failures), "stop": [], "output_skips": [], "group_columns": [],
        "no_data_groups": [], "distribute_from_column": 0,
    }))
    matlab_loop.close(plan["token"], "")


def _failure(combo_index):
    record = {
        "fn_name": "double_rs", "metadata": {}, "combo": "", "stage": "call",
        "exc_type": "test:bad", "message": "bad", "reason": "test:bad: bad",
        "frames": {"file": "/u/f.m", "line": 3, "function": "f", "code": "", "origin": "user"},
        "user_frame": {"file": "/u/f.m", "line": 3, "function": "f", "code": "", "origin": "user"},
        "no_data": False, "blame": "user_code", "probable": False, "remedy": [],
    }
    return {"record": record, "combos": [combo_index]}


def _result(prep, subjects):
    rows = []
    for combo in prep["full_combos"]:
        row = dict(combo.items())
        if int(row["subject"]) in subjects:
            row["Out_RS"] = np.array([2.0, 4.0, 6.0]) * int(row["subject"])
            rows.append(row)
    return pd.DataFrame(rows)


def test_a_matlab_run_is_recorded_with_its_counts_and_failed_locations(db):
    prep = _prepare(db, [1, 2])
    _matlab_loop_ran([1, 2], completed=1, failures=[_failure(2)])
    for_each_save(prep["handle"], [_result(prep, {1})], save=True)

    (s,) = run_summaries(db._duck, "double_rs")
    assert s.language == "matlab"
    assert (s.total, s.computed, s.failed, s.no_data) == (2, 1, 1, 0)
    assert s.saved_records == 1
    assert s.output_types == ("Out_RS",)
    # It shares the `_run` row's id, so the report finds its records.
    assert [r[0] for r in db._duck.fetchall("SELECT run_id FROM _run")] == [s.run_id]
    assert db._duck.fetchall("SELECT subject, outcome, run_id FROM _run_outcome") == [
        ("2", "failed", s.run_id),
    ]
    report = run_report(db, s.run_id)
    assert [c["records"] for c in report.outputs] == [1]
    data = run_report_data(db, s.run_id)
    assert data.status_counts == {"computed": 1, "failed": 1, "no_data": 0}


def test_a_cleanup_call_without_a_finished_loop_records_nothing(db):
    prep = _prepare(db, [1])
    # No loop ran since prepare (0 combos, or the loop raised before its summary).
    for_each_save(prep["handle"], [], save=False)
    assert run_summaries(db._duck, "double_rs") == []


def test_an_earlier_standalone_loop_is_not_mistaken_for_this_run(db):
    _matlab_loop_ran([1], completed=1)  # before prepare: someone else's loop
    prep = _prepare(db, [1])
    for_each_save(prep["handle"], [], save=False)
    assert run_summaries(db._duck, "double_rs") == []
