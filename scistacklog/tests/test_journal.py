"""The run journal (scistack.log) and the debug file (scistack-debug.log).

Pins:
- set_path(debug, journal_path=...) writes two files; the debug file
  defaults to DEBUG and is a superset of the journal
- Log.run: one block per OUTERMOST run, written only when a summary or an
  error reached it; nested runs share it; summaries aggregate
- every debug-file line inside a run carries run=<id>, the id the block names
- an exception escaping unclassified is recorded neutrally ("The run failed")
- the renderer: counts, blame headlines, advice lines, combo capping,
  MATLAB jsonencode quirks, never raises
- journal_note writes once per key
- the debug file rolls over past its cap
- nothing outside scistacklog writes the journal (source guard)
See docs/claude/logging-architecture.md.
"""

import json
import pathlib
import re

import pytest

import scistacklog
from scistacklog import Log, debug_path_for
from scistacklog.journal import BLAME_HEADLINES, blame_headline, render


@pytest.fixture(autouse=True)
def clean_log_state():
    Log._reset_for_tests()
    yield
    Log._reset_for_tests()


@pytest.fixture
def files(tmp_path):
    journal = tmp_path / "scistack.log"
    debug = tmp_path / "scistack-debug.log"
    Log.set_path(str(debug), journal_path=str(journal))
    return journal, debug


def _summary(**over):
    base = {
        "event": "summary", "total": 3, "completed": 2, "failed": 1, "no_data": 0,
        "cancelled": False, "stopped": False, "failures": [],
    }
    return {**base, **over}


def _group(blame="user_code", count=1, combos=("subject=1",), probable=False, **example):
    ex = {
        "fn_name": "clean", "exc_type": "KeyError", "message": "'chan'", "blame": blame,
        "probable": probable,
        "user_frame": {"file": "/p/clean.py", "line": 12, "function": "clean",
                       "code": "x = d['chan']", "origin": "user"},
        **example,
    }
    return {
        "reason": f"{ex['exc_type']}: {ex['message']}", "fn_name": "clean",
        "exc_type": ex["exc_type"], "message": ex["message"], "no_data": blame == "no_data",
        "count": count, "combos": list(combos), "blame": blame, "probable": probable,
        "user_frame": ex["user_frame"], "example": ex,
    }


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------


def test_debug_path_is_derived_from_the_journal():
    assert debug_path_for("/d/scistack.log") == "/d/scistack-debug.log"


def test_debug_file_defaults_to_debug_and_journal_gets_only_blocks(files):
    journal, debug = files
    Log.debug("framework internals")
    with Log.run("clean") as run:
        run.add_summary(_summary())
    debug_text = debug.read_text(encoding="utf-8")
    journal_text = journal.read_text(encoding="utf-8")
    assert "framework internals" in debug_text
    assert "framework internals" not in journal_text
    assert "clean" in journal_text
    # Superset: the block itself is in the debug file too.
    first_line = journal_text.splitlines()[0]
    assert first_line in debug_text


def test_run_without_summary_or_error_writes_nothing(files):
    journal, _debug = files
    with Log.run("registered_only"):
        pass
    assert not journal.exists() or "registered_only" not in journal.read_text(encoding="utf-8")


def test_nested_runs_share_one_block_and_aggregate(files):
    journal, _ = files
    with Log.run("outer") as outer:
        with Log.run("inner") as inner:
            assert inner is outer
            inner.add_summary(_summary(completed=1, failed=1, failures=[_group(count=1)]))
        outer.add_summary(_summary(completed=2, failed=1, failures=[_group(count=2, combos=["subject=2", "subject=3"])]))
    text = journal.read_text(encoding="utf-8")
    assert text.count("── ") == 1
    assert "outer" in text and "inner" not in text
    assert "✓ 3 completed" in text and "✗ 2 failed" in text
    assert "3 combos" in text  # one reason, merged across the two loops


def test_run_token_stamps_debug_lines_and_names_the_block(files):
    journal, debug = files
    with Log.run("clean") as run:
        Log.info("inside", layer="scifor")
        run.add_summary(_summary())
    Log.info("outside", layer="scifor")
    lines = debug.read_text(encoding="utf-8").splitlines()
    (inside,) = [l for l in lines if l.endswith("inside")]
    (outside,) = [l for l in lines if l.endswith("outside")]
    assert f"[scifor] run={run.run_id} inside" in inside
    assert "run=" not in outside
    assert f"(run={run.run_id})" in journal.read_text(encoding="utf-8")


def test_unclassified_exception_is_recorded_neutrally(files):
    journal, _ = files
    with pytest.raises(RuntimeError):
        with Log.run("clean"):
            raise RuntimeError("boom")
    text = journal.read_text(encoding="utf-8")
    assert "did not finish" in text
    assert "The run failed" in text
    assert "RuntimeError: boom" in text
    assert "internal" not in text  # nothing classified it, so nothing is claimed


def test_fail_none_means_already_accounted_for(files):
    journal, _ = files
    with pytest.raises(ValueError):
        with Log.run("clean") as run:
            run.add_summary(_summary(stopped=True, failures=[_group()]))
            run.fail(None)
            raise ValueError("the stop error, already in the summary")
    text = journal.read_text(encoding="utf-8")
    assert "stopped at the first error" in text
    assert "already in the summary" not in text


def test_journal_note_once_per_key(files):
    journal, _ = files
    Log.journal_note("LoadedData: 3 text columns", key="k")
    Log.journal_note("LoadedData: 3 text columns", key="k")
    Log.journal_note("other", key="k2")
    text = journal.read_text(encoding="utf-8")
    assert text.count("LoadedData: 3 text columns") == 1
    assert "note: other" in text


def test_journal_json_reads_matlab_events(files):
    journal, _ = files
    event = {
        "kind": "run", "name": "clean_m", "language": "matlab", "completed": 1,
        "failed": 1, "no_data": 0, "total": 2, "error": [],
        # jsonencode: a 1-element struct array is an object; a 1-element
        # cellstr is a bare string.
        "failures": {**_group(combos=()), "combos": "subject=1"},
    }
    Log.journal_json(json.dumps(event))
    text = journal.read_text(encoding="utf-8")
    assert "clean_m" in text and "Error in your function clean" in text
    assert "combos: subject=1" in text


def test_journal_without_a_file_still_reaches_the_debug_stream(tmp_path):
    debug = tmp_path / "d.log"
    Log.set_path(str(debug))
    Log.journal({"kind": "note", "text": "hello"})
    assert "[journal]" in debug.read_text(encoding="utf-8")


def test_debug_file_rolls_over_past_its_cap(tmp_path, monkeypatch):
    debug = tmp_path / "scistack-debug.log"
    debug.write_text("x" * 200, encoding="utf-8")
    monkeypatch.setattr(scistacklog, "DEBUG_MAX_BYTES", 100)
    Log.set_path(str(debug))
    Log.info("fresh")
    assert (tmp_path / "scistack-debug.log.1").read_text(encoding="utf-8") == "x" * 200
    text = debug.read_text(encoding="utf-8")
    assert "rolled" in text and "fresh" in text


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------


def test_render_user_code_block_points_at_the_line():
    text = render({"kind": "run", "name": "clean", "run_id": "ab12cd34",
                   **_summary(failures=[_group()])})
    assert "finished with 1 failure" in text
    assert "✗ Error in your function clean — 1 combo" in text
    assert "KeyError: 'chan'" in text
    assert "at /p/clean.py:12 in clean" in text
    assert "x = d['chan']" in text
    assert "Failed combos saved nothing" in text
    assert "Please report" not in text


def test_render_internal_asks_for_a_report_with_the_run_id():
    text = render({"kind": "run", "name": "clean", "run_id": "ab12cd34",
                   **_summary(failures=[_group(blame="internal")])})
    assert BLAME_HEADLINES["internal"] in text
    assert 'search "run=ab12cd34"' in text
    assert "/p/clean.py" not in text  # nothing the scientist can edit


def test_render_probable_user_value_is_hedged():
    text = render({"kind": "run", "name": "clean", "run_id": "r",
                   **_summary(failures=[_group(blame="user_value", probable=True)])})
    assert "Probably fixable in your function clean" in text
    assert "may be a SciStack bug" in text


def test_render_caps_combos_and_message_lines():
    combos = [f"subject={i}" for i in range(20)]
    long_message = "\n".join(f"line {i}" for i in range(40))
    text = render({"kind": "run", "name": "f", **_summary(
        failures=[_group(count=20, combos=combos, message=long_message)])})
    assert "(+15 more)" in text
    assert "more line(s) in scistack-debug.log" in text


def test_render_no_data_is_one_quiet_line():
    text = render({"kind": "run", "name": "f", **_summary(
        failed=0, no_data=2, failures=[_group(blame="no_data", count=2, combos=["s=1", "s=2"])])})
    assert "– no data for 2 combo(s) — s=1; s=2" in text
    assert "✗" not in text.split("\n", 2)[2]


def test_render_action_failure_internal_is_hedged():
    text = render({"kind": "action_failed", "action": "put_edge", "run_id": "q1",
                   "failure": {"exc_type": "ValueError", "message": "cycle", "blame": "internal"}})
    assert "Couldn't complete put_edge" in text
    assert "If the message doesn't say what to change" in text


def test_render_never_raises():
    text = render({"kind": "mystery"})
    assert "could not be formatted" in text


def test_blame_headline_covers_every_category():
    for blame in ("user_code", "user_value", "setup", "internal", "no_data", None):
        assert blame_headline(blame, False, "f")
    assert blame_headline("user_value", True, "f").startswith("Probably")


# ---------------------------------------------------------------------------
# One writer
# ---------------------------------------------------------------------------


def test_only_scistacklog_reads_the_journal_path():
    """The journal has one writer (Log.journal). Outside scistacklog, only
    scidb.log.attach_log_file may touch its path (to stay idempotent)."""
    repo = pathlib.Path(__file__).resolve().parents[2]
    allowed = {
        repo / "scistacklog" / "src" / "scistacklog" / "__init__.py",
        repo / "scidb" / "src" / "scidb" / "log.py",
    }
    offenders = []
    for src in repo.glob("*/src/**/*.py"):
        if src in allowed:
            continue
        text = src.read_text(encoding="utf-8", errors="replace")
        if re.search(r"_journal_path\b|get_journal_path\(", text):
            offenders.append(str(src.relative_to(repo)))
    assert offenders == []
