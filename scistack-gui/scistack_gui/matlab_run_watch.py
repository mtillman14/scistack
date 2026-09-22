"""Watching a MATLAB run that nobody can see finish.

A run dispatched to the MathWorks MATLAB terminal is handed over as text and
never reports back. The GUI therefore used to synthesise the answer —
``run_done {success: true}`` the instant the script was delivered, before
MATLAB had executed a line and whether or not it then failed. This module is
what replaces that guess with an observation.

Two independent signals, because MATLAB can be Ctrl-C'd, wedged or killed
and a design that needs its cooperation would hang on exactly those cases:

* **the markers** (``scidb.run_markers``) — the script writes one file when
  it starts and one when it ends. Authoritative about *how it ended*.
* **the lock holder** (``db.probe_lock_holder``) — DuckDB names the process
  holding the database, and the OS says whether that process still exists.
  Needs nothing from MATLAB. Authoritative about *whether it is still
  going*.

Together:

| ``.done`` | lock | verdict |
|---|---|---|
| present | — | success or failure, with MATLAB's own message |
| absent | held by a live process | still running — keep waiting |
| absent | free, or holder gone | **unknown** — never reported as success |

"Unknown" is a real third outcome, not a dressed-up failure: a marker lost
to a network share is not the same as a run that broke, and telling a user
their analysis failed when it did not is its own bug. The frontend renders
it as its own state.

Everything here runs on a daemon thread per tracked run and talks to the
frontend through the same ``run_output`` / ``run_done`` frames the sidecar
tier already uses, so nothing downstream needed a new concept — the signal
simply became real.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from scidb.run_markers import marker_dir, read_done, read_started

logger = logging.getLogger(__name__)

#: How often to look. A second is imperceptible next to a MATLAB run and
#: cheap even on a network share, where these few small files live.
POLL_INTERVAL_S = 1.0

#: How long to wait for ``.started`` before concluding the script never ran.
#: Generous: MATLAB has to parse the script, and a cold `pyenv` load can
#: take tens of seconds on Windows.
STARTUP_GRACE_S = 120.0

#: How long the database may be free, after the run has started, before the
#: run is declared unknown. A run that has just called ``close_database`` is
#: momentarily lock-free while its ``onCleanup`` writes the marker, so this
#: must comfortably exceed a file write — but it is the difference between
#: noticing a killed MATLAB in seconds and in minutes, so not too generous.
IDLE_GRACE_S = 20.0

#: Last-resort backstop, in case both signals somehow say "keep waiting"
#: forever. Long, because exceeding it on a genuinely long run reports
#: "unknown" for work that is fine.
CEILING_S = 12 * 60 * 60


@dataclass(frozen=True)
class Verdict:
    """What the watcher has concluded, if anything."""

    #: "wait" | "done" | "error" | "unknown"
    kind: str
    message: str = ""

    @property
    def finished(self) -> bool:
        return self.kind != "wait"


#: Nothing has been concluded yet; keep polling.
WAIT = Verdict("wait")


def classify(
    *,
    done,
    started: bool,
    busy: bool,
    idle_for: float,
    since_dispatch: float,
    matlab_gone: bool,
) -> Verdict:
    """Decide what a single poll means. Pure, so it can be tested directly.

    The ordering is the design:

    1. **A report wins.** Checked before anything else, so a run that
       finished between two polls is never overtaken by a liveness verdict
       inferred from a database it has just released.
    2. **Never started** — nothing announced itself and nothing holds the
       database, for longer than MATLAB could plausibly take to parse the
       script and load ``pyenv``. The script never ran.
    3. **Stopped without reporting** — it started, and now nothing holds the
       database. Given the grace period, that means MATLAB is gone or wedged.

    Reported as *unknown*, never as failure: a marker lost to a network
    share is not a broken analysis, and telling a user their run failed
    when it did not sends them to debug working code.
    """
    if done is not None:
        if done.interrupted:
            return Verdict(
                "error", "MATLAB stopped before finishing (interrupted)."
            )
        if done.ok:
            return Verdict("done")
        detail = done.message or "the MATLAB script reported an error"
        if done.identifier:
            detail = f"{done.identifier}: {detail}"
        return Verdict("error", detail)

    if not started:
        if since_dispatch > STARTUP_GRACE_S and not busy:
            return Verdict(
                "unknown",
                "MATLAB never started this run. The script was sent to the "
                "MATLAB terminal but never reported starting — check the "
                "MATLAB Command Window, and that nothing interrupted it "
                "before it ran.",
            )
        return WAIT

    if not busy and idle_for > IDLE_GRACE_S:
        return Verdict(
            "unknown",
            "MATLAB stopped without reporting how this run ended"
            + (" (the MATLAB process is gone)." if matlab_gone else ".")
            + " Whatever it wrote before stopping is still in the database; "
            "re-run if you need the rest.",
        )

    return WAIT


@dataclass
class TrackedRun:
    """One MATLAB run the GUI is waiting on."""

    run_id: str
    db_path: str
    markers: str
    label: str
    dispatched_at: float
    #: MATLAB's own process id, once ``.started`` has been read.
    matlab_pid: int | None = None
    started_at: float | None = None
    #: Last probe result, for the diagnostics command.
    last_probe: str = "(not probed yet)"
    verdict: str = "running"
    _stop: threading.Event = field(default_factory=threading.Event)

    def snapshot(self) -> dict:
        """What ``SciStack: Show MATLAB Run State`` prints."""
        now = time.time()
        return {
            "run_id": self.run_id,
            "label": self.label,
            "db_path": self.db_path,
            "marker_dir": self.markers,
            "dispatched_s_ago": round(now - self.dispatched_at, 1),
            "started": self.started_at is not None,
            "matlab_pid": self.matlab_pid,
            "last_probe": self.last_probe,
            "verdict": self.verdict,
            "started_marker_exists": Path(self.markers).joinpath(
                f"{self.run_id}.started"
            ).exists(),
            "done_marker_exists": Path(self.markers).joinpath(
                f"{self.run_id}.done"
            ).exists(),
        }


_tracked: dict[str, TrackedRun] = {}
_lock = threading.Lock()


def tracked_runs() -> list[dict]:
    """Every run currently being watched — the diagnostics surface.

    "Which run is the GUI waiting on, and what does it think is happening?"
    has no other answer: the run is in another process, on the far side of a
    terminal nobody can query.
    """
    with _lock:
        return [run.snapshot() for run in _tracked.values()]


def any_run_in_flight() -> bool:
    """Whether a MATLAB run is believed to own a database right now.

    Read by ``db.acquire_db_connection`` to shorten its backoff: retrying
    for five seconds is right for MATLAB's own sub-second writes and
    pointless against a twenty-minute run.
    """
    with _lock:
        return any(run.verdict == "running" for run in _tracked.values())


def track(run_id: str, db_path: str, label: str = "") -> dict:
    """Start watching *run_id*. Idempotent for a run already tracked.

    Called right after the host dispatches a script to the MATLAB terminal
    (or the clipboard — a pasted script writes the same markers, so those
    runs stop being a black hole too).
    """
    from scistack_gui.notify import push_message

    with _lock:
        if run_id in _tracked:
            logger.debug("[matlab_watch] %s is already tracked", run_id)
            return {"ok": True, "already": True}
        run = TrackedRun(
            run_id=run_id,
            db_path=str(db_path),
            markers=str(marker_dir(db_path)),
            label=label or run_id,
            dispatched_at=time.time(),
        )
        _tracked[run_id] = run

    logger.info(
        "[matlab_watch] tracking %s (%s) — markers in %s",
        run_id,
        run.label,
        run.markers,
    )
    push_message(
        {
            "type": "run_output",
            "run_id": run_id,
            "text": "Waiting for MATLAB to report this run…\n",
        }
    )
    threading.Thread(
        target=_watch, args=(run,), name=f"matlab-watch-{run_id}", daemon=True
    ).start()
    return {"ok": True, "already": False, "marker_dir": run.markers}


def stop(run_id: str) -> bool:
    """Stop watching *run_id* without emitting a verdict.

    For the user's "stop waiting" button: they have decided the run is not
    coming back, and a watcher that keeps polling a dead run is noise.
    """
    with _lock:
        run = _tracked.get(run_id)
    if run is None:
        return False
    run._stop.set()
    return True


def stop_all() -> None:
    with _lock:
        runs = list(_tracked.values())
    for run in runs:
        run._stop.set()


def _finish(run: TrackedRun, *, success: bool, error: str, unknown: bool) -> None:
    """Emit the real verdict and stop tracking."""
    from scistack_gui.notify import push_message

    run.verdict = "unknown" if unknown else ("done" if success else "error")
    with _lock:
        _tracked.pop(run.run_id, None)

    logger.info(
        "[matlab_watch] %s finished: verdict=%s error=%r",
        run.run_id,
        run.verdict,
        error,
    )
    if error:
        push_message(
            {"type": "run_output", "run_id": run.run_id, "text": f"{error}\n"}
        )
    push_message(
        {
            "type": "run_done",
            "run_id": run.run_id,
            "success": success,
            "error": error,
            "cancelled": False,
            # The frontend renders this as its own state. It is NOT a
            # failure: a marker lost to a network share is not a broken
            # analysis, and saying so would send the user to debug code that
            # is fine.
            "unknown": unknown,
            "duration_ms": int((time.time() - run.dispatched_at) * 1000),
        }
    )

    # A terminal run writes records like any other, and until 2026-09-22 this
    # was the one completion path that never said so: `_start_matlab_run`
    # returns `host_execution_required` and exits, spawning no thread, so the
    # `_notify_records_changed()` every in-process path ends with had no
    # caller here. The observed cost was 4 `dag_updated` messages in a
    # 55-minute session against 9 runs, none of them after a run — node
    # colours stayed as they were until something else happened to refresh
    # the canvas, and Plot Studio kept serving pre-run frames.
    #
    # Sent on EVERY verdict, not just success. The message means "records may
    # have changed", which is true of a run that failed halfway (it saved what
    # it got to) and of one we cannot classify. Announcing too often costs a
    # refetch; announcing too rarely is the bug being fixed.
    try:
        from scistack_gui.api.run import _notify_records_changed

        _notify_records_changed()
    except Exception:
        # The verdict is already delivered. A missed refresh is a stale
        # canvas, not a lost run, and must never look like a failed one.
        logger.exception(
            "[matlab_watch] %s: could not announce that records changed",
            run.run_id,
        )


def _watch(run: TrackedRun) -> None:
    """Poll until the run reports, or until we can say it will not.

    The thread does the observing; :func:`classify` does the deciding.
    """
    from scistack_gui.db import probe_lock_holder

    deadline = run.dispatched_at + CEILING_S
    idle_since: float | None = None

    while not run._stop.wait(POLL_INTERVAL_S):
        now = time.time()

        # Note the start once, for the PID and to leave the startup grace.
        if run.started_at is None:
            started = read_started(run.markers, run.run_id)
            if started is not None:
                run.started_at = now
                run.matlab_pid = started.pid
                logger.info(
                    "[matlab_watch] %s started in MATLAB (pid=%s)",
                    run.run_id,
                    started.pid,
                )
                _emit(run, f"MATLAB started this run (PID {started.pid}).\n")

        holder = probe_lock_holder(run.db_path)
        busy = (not holder.free) and holder.alive
        run.last_probe = (
            "database free"
            if holder.free
            else f"held by {holder.holder or 'another process'} "
            f"(PID {holder.pid}, {'alive' if holder.alive else 'GONE'})"
        )
        if busy:
            idle_since = None
        elif idle_since is None:
            idle_since = now

        verdict = classify(
            done=read_done(run.markers, run.run_id),
            started=run.started_at is not None,
            busy=busy,
            idle_for=0.0 if idle_since is None else now - idle_since,
            since_dispatch=now - run.dispatched_at,
            matlab_gone=(
                run.matlab_pid is not None and not _pid_alive(run.matlab_pid)
            ),
        )
        if verdict.finished:
            _finish(
                run,
                success=verdict.kind == "done",
                error=verdict.message,
                unknown=verdict.kind == "unknown",
            )
            _cleanup_markers(run)
            return

        if now > deadline:
            _finish(
                run,
                success=False,
                error=(
                    f"Stopped waiting for this MATLAB run after "
                    f"{CEILING_S / 3600:.0f}h. It may still be running in MATLAB."
                ),
                unknown=True,
            )
            return

    # Stopped by request: no verdict, because the user supplied their own.
    with _lock:
        _tracked.pop(run.run_id, None)
    logger.info("[matlab_watch] stopped watching %s on request", run.run_id)


def _pid_alive(pid: int) -> bool:
    from scistack_gui.db import pid_alive

    return pid_alive(pid)


def _emit(run: TrackedRun, text: str) -> None:
    from scistack_gui.notify import push_message

    push_message({"type": "run_output", "run_id": run.run_id, "text": text})


def _cleanup_markers(run: TrackedRun) -> None:
    """Remove a reported run's markers.

    They have done their job, and leaving them accumulates files in a
    directory sitting beside the user's database.
    """
    from scidb.run_markers import clear

    clear(run.markers, run.run_id)
