"""Knowing whether a MATLAB run is still going, and whether it worked.

A run dispatched to the MathWorks terminal used to report ``run_done
{success: true}`` the instant the script text was handed over — before
MATLAB had executed a line, and whether or not it then failed. Plan:
``.claude/plan-matlab-run-completion.md``.

Two signals replace that guess, and the tests below are organised around
them:

* the run's own **markers** (``scidb.run_markers``), which say how it ended;
* the **DuckDB lock holder** (``scistack_gui.db.probe_lock_holder``), which
  says whether it is still going and needs nothing from MATLAB — the half
  that survives a killed or Ctrl-C'd MATLAB.

The rule every test here defends: **a run whose outcome we do not know is
never reported as success**, and never as a plain failure either. "Unknown"
is its own verdict, because telling a user their analysis broke when the
truth is that a marker was lost sends them to debug working code.
"""

from __future__ import annotations

import json
import os
import time

import pytest

from scidb import run_markers
from scistack_gui import db as gui_db
from scistack_gui.matlab_run_watch import (
    IDLE_GRACE_S,
    STARTUP_GRACE_S,
    classify,
)

# --- the format, from both sides ------------------------------------------


def test_marker_dir_sits_beside_the_database():
    """Same convention as scidb.log and <stem>.layout.json.

    Per-database by construction, so two GUI sessions watching two databases
    cannot read each other's markers.
    """
    d = run_markers.marker_dir("/studies/gait/experiment.duckdb")
    assert d.name == "experiment.runs"
    assert d.parent.as_posix().endswith("/studies/gait")


def test_a_run_id_that_is_not_a_safe_filename_is_refused():
    """Refusing beats sanitising: a writer and a reader that sanitise
    differently look for different files, and neither says why."""
    for bad in ("../escape", "has/slash", "", "a" * 129):
        with pytest.raises(run_markers.UnsafeRunId):
            run_markers.check_run_id(bad)


def test_reads_the_exact_bytes_matlab_writes(tmp_path):
    """The MATLAB writer's output, pinned as a fixture.

    ``+scidb/run_marker.m`` builds this string with sprintf. If either side
    drifts, this is the test that fails instead of a run silently becoming
    "unknown" on a user's machine.
    """
    (tmp_path / "run-abc.started").write_text(
        '{"version":1,"run_id":"run-abc","pid":4242,"at":1758547200.500}\n',
        encoding="utf-8",
    )
    (tmp_path / "run-abc.done").write_text(
        '{"version":1,"run_id":"run-abc","ok":false,"interrupted":false,'
        '"identifier":"MATLAB:undefinedFunction",'
        '"message":"Unrecognized function or variable \'foo\'.",'
        '"at":1758547260.250}\n',
        encoding="utf-8",
    )

    started = run_markers.read_started(tmp_path, "run-abc")
    assert started is not None
    assert started.pid == 4242

    done = run_markers.read_done(tmp_path, "run-abc")
    assert done is not None
    assert done.ok is False
    assert done.identifier == "MATLAB:undefinedFunction"
    assert "Unrecognized function" in done.message


def test_a_windows_error_message_survives_the_round_trip(tmp_path):
    """Backslashes and quotes are the normal case in MATLAB errors.

    An unescaped Windows path would produce a marker the reader cannot
    parse — i.e. a run that completed, reported as unknown. The MATLAB
    writer escapes; this pins that the escaped form reads back intact.
    """
    message = 'Cannot find C:\\Data\\"study".m'
    (tmp_path / "r1.done").write_text(
        json.dumps(
            {"version": 1, "run_id": "r1", "ok": False, "identifier": "X", "message": message}
        ),
        encoding="utf-8",
    )
    done = run_markers.read_done(tmp_path, "r1")
    assert done is not None and done.message == message


def test_a_half_written_marker_is_not_an_answer(tmp_path):
    """The watcher polls, so it WILL catch a file mid-write.

    The right response is to look again next tick — not to fail the run, and
    emphatically not to treat an unparseable file as success.
    """
    (tmp_path / "r2.done").write_text('{"version":1,"ok":tr', encoding="utf-8")
    assert run_markers.read_done(tmp_path, "r2") is None


def test_a_marker_without_ok_is_not_success(tmp_path):
    """`ok` absent must not read as True — that is the original bug."""
    (tmp_path / "r3.done").write_text('{"version":1,"run_id":"r3"}', encoding="utf-8")
    done = run_markers.read_done(tmp_path, "r3")
    assert done is not None and done.ok is False


def test_sweep_removes_only_stale_markers(tmp_path):
    now = time.time()
    fresh = tmp_path / "new.started"
    stale = tmp_path / "old.started"
    for p in (fresh, stale):
        p.write_text("{}", encoding="utf-8")
    os.utime(stale, (now - 10_000, now - 10_000))
    unrelated = tmp_path / "notes.txt"
    unrelated.write_text("keep me", encoding="utf-8")

    assert run_markers.sweep(tmp_path, older_than_s=3_600, now=now) == 1
    assert fresh.exists()
    assert not stale.exists()
    assert unrelated.exists(), "sweep must only touch marker files"


# --- the verdict ----------------------------------------------------------


def _done(ok=True, interrupted=False, identifier="", message=""):
    return run_markers.DoneMarker(
        run_id="r", ok=ok, identifier=identifier, message=message, at=None,
        interrupted=interrupted,
    )


def test_a_report_wins_over_any_liveness_inference():
    """Checked first, deliberately.

    A run that finished between two polls has released the database, so the
    liveness half would read it as "stopped without reporting". The marker
    is the authoritative answer and must not be overtaken by an inference
    drawn from the very act of finishing.
    """
    verdict = classify(
        done=_done(ok=True),
        started=True,
        busy=False,
        idle_for=IDLE_GRACE_S * 10,
        since_dispatch=STARTUP_GRACE_S * 10,
        matlab_gone=True,
    )
    assert verdict.kind == "done"


def test_a_failed_run_carries_matlabs_own_error():
    verdict = classify(
        done=_done(ok=False, identifier="MATLAB:badsubscript", message="Index exceeds"),
        started=True, busy=False, idle_for=0, since_dispatch=1, matlab_gone=False,
    )
    assert verdict.kind == "error"
    assert "MATLAB:badsubscript" in verdict.message
    assert "Index exceeds" in verdict.message


def test_an_interrupted_run_is_a_failure_not_a_success():
    verdict = classify(
        done=_done(ok=False, interrupted=True),
        started=True, busy=False, idle_for=0, since_dispatch=1, matlab_gone=False,
    )
    assert verdict.kind == "error"
    assert "interrupted" in verdict.message.lower()


def test_a_running_run_keeps_waiting():
    """Started, and something live still holds the database."""
    verdict = classify(
        done=None, started=True, busy=True,
        idle_for=0, since_dispatch=STARTUP_GRACE_S * 100, matlab_gone=False,
    )
    assert verdict.kind == "wait", "a long run must not time out while it is working"


def test_a_brief_lock_release_does_not_end_a_run():
    """A run that has just closed the database is momentarily lock-free
    while its marker is written. Ending it here would report "unknown" for
    runs that are about to report success."""
    verdict = classify(
        done=None, started=True, busy=False,
        idle_for=IDLE_GRACE_S / 2, since_dispatch=60, matlab_gone=False,
    )
    assert verdict.kind == "wait"


def test_a_run_that_stopped_without_reporting_is_unknown_not_failed():
    verdict = classify(
        done=None, started=True, busy=False,
        idle_for=IDLE_GRACE_S + 1, since_dispatch=60, matlab_gone=True,
    )
    assert verdict.kind == "unknown", "never a plain failure — we do not know it failed"
    assert "process is gone" in verdict.message
    assert "still in the database" in verdict.message, (
        "the user needs to know partial results were kept"
    )


def test_a_script_that_never_started_is_reported_once_the_grace_expires():
    """The commonest cause is a pyenv failure, which kills the script before
    it can report anything — which is exactly why .started is written BEFORE
    the preamble."""
    assert classify(
        done=None, started=False, busy=False,
        idle_for=0, since_dispatch=STARTUP_GRACE_S / 2, matlab_gone=False,
    ).kind == "wait", "MATLAB is allowed time to parse and load pyenv"

    verdict = classify(
        done=None, started=False, busy=False,
        idle_for=0, since_dispatch=STARTUP_GRACE_S + 1, matlab_gone=False,
    )
    assert verdict.kind == "unknown"
    assert "never started" in verdict.message


def test_a_slow_start_that_holds_the_database_is_not_declared_dead():
    """Busy means something is working, whatever the markers say."""
    verdict = classify(
        done=None, started=False, busy=True,
        idle_for=0, since_dispatch=STARTUP_GRACE_S * 10, matlab_gone=False,
    )
    assert verdict.kind == "wait"


# --- liveness -------------------------------------------------------------


def test_pid_alive_knows_this_process():
    assert gui_db.pid_alive(os.getpid()) is True


def test_pid_alive_reports_a_dead_pid():
    # 0 and negatives are never valid process ids.
    assert gui_db.pid_alive(0) is False
    assert gui_db.pid_alive(-1) is False


def test_pid_alive_prefers_alive_when_it_cannot_tell(monkeypatch):
    """A false "dead" ends a run that is still working — the worse error by
    far, since the user then re-runs work already in flight against the same
    database. A false "alive" only delays the verdict to the ceiling."""
    def _raise(*_args, **_kwargs):
        raise OSError("no idea")

    monkeypatch.setattr(os, "kill", _raise)
    if os.name != "nt":
        assert gui_db.pid_alive(os.getpid()) is True


# --- the backoff ----------------------------------------------------------


def test_backoff_shortens_only_while_a_run_is_in_flight(monkeypatch):
    """5 s is right for MATLAB's own sub-second writes and pointless against
    a 20-minute run, where every click costs 5 s before reporting a conflict
    that was knowable at t=0."""
    monkeypatch.setattr(gui_db, "_matlab_run_in_flight", lambda: False)
    assert gui_db._effective_acquire_timeout(5.0) == 5.0

    monkeypatch.setattr(gui_db, "_matlab_run_in_flight", lambda: True)
    assert gui_db._effective_acquire_timeout(5.0) == gui_db.ACQUIRE_RETRY_TIMEOUT_TRACKED


def test_backoff_never_lengthens_a_caller_supplied_timeout(monkeypatch):
    """A caller that already asked to wait less keeps its answer."""
    monkeypatch.setattr(gui_db, "_matlab_run_in_flight", lambda: True)
    assert gui_db._effective_acquire_timeout(0.1) == 0.1


def test_backoff_is_never_zero(monkeypatch):
    """A hint may change how long we WAIT, never whether we ATTEMPT — and a
    200 ms MATLAB write that would have succeeded must not start reporting
    failures if the hint is ever wrong."""
    monkeypatch.setattr(gui_db, "_matlab_run_in_flight", lambda: True)
    assert gui_db._effective_acquire_timeout(5.0) > 0


def test_a_broken_hint_falls_back_to_the_patient_default(monkeypatch):
    """The hint only tunes a timeout; failing to answer must not break an
    acquire."""
    def _boom():
        raise RuntimeError("watcher exploded")

    monkeypatch.setattr(
        "scistack_gui.matlab_run_watch.any_run_in_flight", _boom
    )
    assert gui_db._effective_acquire_timeout(5.0) == 5.0


# --- the generated script -------------------------------------------------
#
# The script is what writes the markers, so these assert on its text. No
# MATLAB needed — and this is where a placement mistake shows up, because on
# a real machine it would surface only as a run that mysteriously reports
# "unknown".


def _script(run_id="run-xyz", **kwargs):
    from scistack_gui.api.matlab_command import generate_matlab_command

    return generate_matlab_command(
        function_name="bandpass_filter",
        db_path="/data/experiment.duckdb",
        schema_keys=["subject", "session"],
        run_id=run_id,
        **kwargs,
    )


def test_the_script_announces_itself_before_the_preamble():
    """Before, and that ordering is the whole point.

    The pyenv preamble rethrows, and a misconfigured pyenv is the commonest
    way these scripts die. A .started written after it could never tell
    "MATLAB never launched the script" from "the script died in setup" —
    and those have completely different fixes.
    """
    cmd = _script(python_executable="/usr/bin/python3")
    begin = cmd.index("scidb.run_marker('begin'")
    preamble = cmd.index("pyenv")
    assert begin < preamble, "the start marker must precede the pyenv preamble"


def test_the_script_reports_success_and_failure_separately():
    cmd = _script()
    assert "scidb.run_marker('finish'" in cmd
    # The failure form reads MATLAB's own exception, so it is only valid
    # inside the catch block — which is where a failure is known precisely.
    assert "scistack_err__.identifier, scistack_err__.message" in cmd
    # ...and the success form is after the try/catch, which the catch's
    # rethrow makes unreachable on the failure path.
    assert cmd.index("scistack_err__.identifier") < cmd.rindex(
        "scidb.run_marker('finish'"
    )


def test_the_marker_directory_is_the_one_python_will_watch():
    """Writer and reader must agree on the path, or the watcher waits for a
    file that is being written somewhere else."""
    cmd = _script()
    assert str(run_markers.marker_dir("/data/experiment.duckdb")) in cmd


def test_a_script_with_no_run_id_writes_no_markers():
    """A preview, or a copy-to-clipboard with no run behind it: nothing is
    watching, so the script must not litter the user's database folder."""
    cmd = _script(run_id=None)
    assert "run_marker" not in cmd


def test_a_first_run_reports_too():
    """The no-variants branch is a REAL run, and it returns early.

    It is what a never-yet-run MATLAB node executes, and it has its own
    try/catch and its own `return` well before the variant branch's. It was
    missed on the first pass, which would have made every *first* run of a
    function write .started, never write .done, and resolve as "unknown" —
    precisely the failure this whole mechanism exists to prevent.
    """
    cmd = _script()  # no variants => the first-run branch
    assert "scidb.for_each" in cmd and "fill in inputs/outputs" in cmd, (
        "this test is only meaningful if it still exercises the first-run branch"
    )
    assert "scidb.run_marker('begin'" in cmd
    assert "scidb.run_marker('finish'" in cmd
    assert "scistack_err__.identifier, scistack_err__.message" in cmd


def test_the_pipeline_script_reports_too():
    from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

    cmd = generate_matlab_pipeline_command(
        pipeline_id="main",
        steps=[
            {
                "function_name": "bandpass_filter",
                "variants": [
                    {
                        "input_types": {"signal": "RawSignal"},
                        "output_type": "FilteredSignal",
                        "constants": {"low_hz": 20},
                        "record_count": 4,
                    }
                ],
            }
        ],
        db_path="/data/experiment.duckdb",
        schema_keys=["subject"],
        run_id="run-pipe",
    )
    assert "scidb.run_marker('begin'" in cmd
    assert "scidb.run_marker('finish'" in cmd
    assert "run-pipe" in cmd
    assert "scistack_err__.identifier, scistack_err__.message" in cmd


def test_every_generator_exit_reports_the_run():
    """Structural guard: no return path may skip the success marker.

    This is how the first-run gap hid. ``generate_matlab_command`` has TWO
    returns — the no-variants first-run branch returns early, hundreds of
    lines before the variant branch — and only the second one was wired up.
    A script that writes ``.started`` and never ``.done`` resolves as
    "unknown" no matter how well it ran, so a missed exit is not a cosmetic
    omission; it silently breaks the feature for a whole class of runs.

    Reading the source rather than generating scripts, because the point is
    to catch an exit that no test happens to reach.
    """
    import ast
    from pathlib import Path

    import scistack_gui.api.matlab_command as mod

    tree = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
    generators = {
        "generate_matlab_command",
        "generate_matlab_pipeline_command",
    }
    checked = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef) or node.name not in generators:
            continue
        for block in ast.walk(node):
            body = getattr(block, "body", None)
            if not isinstance(body, list):
                continue
            for i, stmt in enumerate(body):
                if not isinstance(stmt, ast.Return):
                    continue
                before = ast.dump(ast.Module(body=body[max(0, i - 3):i], type_ignores=[]))
                assert "_run_marker_finish_lines" in before, (
                    f"{node.name}: the return at line {stmt.lineno} does not "
                    f"emit a success marker first — a run that exits this way "
                    f"writes .started and never .done, and always resolves as "
                    f"'unknown'"
                )
                checked += 1
    assert checked >= 3, (
        f"expected to check at least 3 generator exits, saw {checked} — "
        f"has a generator been renamed?"
    )


# --- the completion path announces that records changed --------------------
#
# A terminal run writes records like any other. Until 2026-09-22 it was the one
# completion path that never said so: `_start_matlab_run` returns
# `host_execution_required` and exits without spawning a thread, so the
# `_notify_records_changed()` every in-process path ends with had no caller on
# this side. Observed: 4 `dag_updated` messages in a 55-minute session against
# 9 runs, none of them after a run — node colours stayed as they were and Plot
# Studio kept serving pre-run frames.


def _capture_pushes(monkeypatch) -> list:
    """Every frame either push_message reaches the transport with.

    There are two, deliberately: `matlab_run_watch` pushes through
    `notify.push_message` (the JSON-RPC path) and `api.run` binds
    `ws.push_message` at import. `ws.push_message` delegates to the JSON-RPC
    one when the extension host is driving, so in production they converge —
    but a test that patched only one would miss half the frames.
    """
    import scistack_gui.api.run as run_mod
    import scistack_gui.notify as notify_mod

    seen: list = []
    monkeypatch.setattr(notify_mod, "push_message", seen.append)
    monkeypatch.setattr(run_mod, "push_message", seen.append)
    return seen


def _tracked_run(tmp_path, run_id: str = "r1"):
    from scistack_gui.matlab_run_watch import TrackedRun

    return TrackedRun(
        run_id=run_id,
        db_path=str(tmp_path / "study.duckdb"),
        markers=str(run_markers.marker_dir(tmp_path / "study.duckdb")),
        label=run_id,
        dispatched_at=time.time(),
    )


@pytest.mark.parametrize(
    "success, error, unknown",
    [
        (True, "", False),
        (False, "Undefined function 'grSides'", False),
        (False, "", True),
    ],
    ids=["success", "failure", "unknown"],
)
def test_every_verdict_announces_that_records_changed(
    tmp_path, monkeypatch, success, error, unknown
):
    """Not just success.

    The message means "records may have changed", which is true of a run that
    failed halfway — it saved what it got to — and of one we cannot classify.
    Announcing too often costs a refetch; announcing too rarely is the bug.
    """
    from scistack_gui.matlab_run_watch import _finish

    seen = _capture_pushes(monkeypatch)
    _finish(_tracked_run(tmp_path), success=success, error=error, unknown=unknown)

    types = [m.get("type") for m in seen]
    assert "run_done" in types, types
    assert "dag_updated" in types, (
        f"the verdict was delivered but the canvas was never told to refetch: "
        f"{types}"
    )
    assert types.index("run_done") < types.index("dag_updated"), (
        "the verdict comes first — a refresh that lands before it would race "
        "the run's own result"
    )


def test_the_announcement_never_buries_the_verdict(tmp_path, monkeypatch):
    """A refresh we failed to request is a stale canvas, not a lost run."""
    import scistack_gui.api.run as run_mod
    from scistack_gui.matlab_run_watch import _finish

    seen = _capture_pushes(monkeypatch)

    def _explode() -> None:
        raise RuntimeError("no transport")

    monkeypatch.setattr(run_mod, "_notify_records_changed", _explode)
    _finish(_tracked_run(tmp_path), success=True, error="", unknown=False)

    assert [m.get("type") for m in seen] == ["run_done"]


def test_no_run_completion_path_here_forgets_to_announce():
    """Structural, because this is exactly how the gap appeared: a new
    completion path was written and the announcement was not carried over.

    Scoped to this module, where the rule is local and checkable — any
    function that tells the frontend a run is over must also say that records
    may have changed.
    """
    import ast
    from pathlib import Path

    import scistack_gui.matlab_run_watch as mod

    tree = ast.parse(Path(mod.__file__).read_text(encoding="utf-8"))
    checked = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        body = ast.dump(node)
        if "'run_done'" not in body and '"run_done"' not in body:
            continue
        checked += 1
        assert "_notify_records_changed" in body, (
            f"{node.name}() emits run_done without announcing that records "
            f"changed — the canvas will keep its pre-run colours"
        )
    assert checked == 1, (
        f"expected exactly one run_done emitter in this module, found {checked} "
        f"— if a second completion path was added, confirm it announces too"
    )
