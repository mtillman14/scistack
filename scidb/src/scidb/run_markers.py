"""Run markers: how a MATLAB run says it started, and how it ended.

A MATLAB run dispatched to the MathWorks terminal used to be a black hole.
The GUI handed the script text to the terminal and heard nothing back, so it
reported ``run_done {success: true}`` the moment the text was delivered —
before MATLAB had executed a line, and regardless of whether MATLAB then
failed. See ``.claude/plan-matlab-run-completion.md``.

A **marker** is how the run reports for itself. The script writes one file
when it begins and one when it ends, and the reader turns the pair into an
answer:

* ``<run_id>.started`` — written at the very top of the generated script,
  **before the pyenv preamble**. That preamble rethrows, and a misconfigured
  ``pyenv`` is the commonest way these scripts die, so without a marker
  written ahead of it there is no way to tell "never launched" from "died in
  setup".
* ``<run_id>.done`` — written from an ``onCleanup`` object, so it is
  produced on normal completion, on error, and on most Ctrl-C interrupts,
  which a ``catch`` block misses.

This module owns the **directory convention and the file format**; the
writer is MATLAB (``scimatlab``'s ``+scidb/run_marker.m``) and the reader is
the GUI server. It lives in scidb rather than in the GUI because "did this
run finish, and did it work" is a statement about a scidb run, not about a
webview (CLAUDE.md NOTE 3) — and because the markers sit beside the
database, which is scidb's territory.

**Markers are not the whole answer.** A killed MATLAB writes nothing, so a
missing ``.done`` is ambiguous on its own: still running, or gone. The
second half of the answer needs no cooperation from MATLAB at all — see
``scistack_gui.db.probe_lock_holder``, which asks who holds the DuckDB file
and whether that process is still alive.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

#: Schema version written into every marker. Bump only for a change the
#: reader cannot absorb; the reader tolerates unknown extra fields so that
#: adding one is not a breaking change.
MARKER_VERSION = 1

#: Filename suffixes. The MATLAB writer builds the same two names, and
#: ``tests/test_run_markers.py`` pins one shared fixture across both sides.
STARTED_SUFFIX = ".started"
DONE_SUFFIX = ".done"

#: A run id has to be safe as a filename, because it IS one. The ids the GUI
#: mints are of the form ``run-<hex>``; anything else is refused rather than
#: sanitised, since a silently-renamed marker is a marker nobody finds.
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


class UnsafeRunId(ValueError):
    """A run id that cannot be used as a filename."""


def check_run_id(run_id: str) -> str:
    """*run_id* if it is safe to use as a filename, else raise.

    Refusing beats sanitising: a writer and a reader that sanitise
    differently look for different files and neither says why.
    """
    if not _SAFE_RUN_ID.match(run_id or ""):
        raise UnsafeRunId(
            f"run id {run_id!r} is not usable as a marker filename "
            f"(allowed: letters, digits, '_', '.', '-', 1-128 chars)"
        )
    return run_id


def marker_dir(db_path) -> Path:
    """The directory of run markers belonging to *db_path*.

    ``<stem>.runs/`` beside the database, matching the conventions already
    there — ``scidb.log`` (``scidb.log.log_path_for``) and
    ``<stem>.layout.json``. Beside the database rather than in the OS temp
    directory so that the markers are per-database (two GUI sessions cannot
    collide) and so they still work when MATLAB runs on a different machine
    from VS Code, as long as the project path is shared.
    """
    db_path = Path(db_path)
    return db_path.parent / f"{db_path.stem}.runs"


def started_path(dir_: Path | str, run_id: str) -> Path:
    return Path(dir_) / f"{check_run_id(run_id)}{STARTED_SUFFIX}"


def done_path(dir_: Path | str, run_id: str) -> Path:
    return Path(dir_) / f"{check_run_id(run_id)}{DONE_SUFFIX}"


@dataclass(frozen=True)
class StartedMarker:
    """The run announced itself before doing anything else."""

    run_id: str
    #: MATLAB's own process id (``feature('getpid')``), so a watcher can ask
    #: the OS whether that process is still alive.
    pid: int | None
    #: Seconds since the epoch, as written by MATLAB.
    at: float | None


@dataclass(frozen=True)
class DoneMarker:
    """The run finished, and said how."""

    run_id: str
    ok: bool
    #: MATLAB's ``MException`` identifier, e.g. ``MATLAB:undefinedFunction``.
    #: Empty on success.
    identifier: str
    #: MATLAB's error message. Empty on success.
    message: str
    at: float | None
    #: True when the ``onCleanup`` fired without the script reaching its end
    #: — a Ctrl-C or a `return` out of the script. Reported as cancelled
    #: rather than as a failure, because the user asked for it.
    interrupted: bool = False


def _load(path: Path) -> dict | None:
    """One marker file as a dict, or None if it is absent or unreadable.

    Never raises. A half-written file is the normal case, not an error: the
    watcher polls, so it WILL sometimes read a file mid-write, and the right
    response is to look again next tick rather than to fail the run.
    """
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError as exc:
        logger.debug("[run_markers] could not read %s: %s", path, exc)
        return None
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except ValueError:
        # Mid-write, or a marker from a future version written differently.
        logger.debug("[run_markers] %s is not valid JSON (yet)", path)
        return None
    return parsed if isinstance(parsed, dict) else None


def _as_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_started(dir_: Path | str, run_id: str) -> StartedMarker | None:
    data = _load(started_path(dir_, run_id))
    if data is None:
        return None
    pid = data.get("pid")
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        pid = None
    return StartedMarker(run_id=run_id, pid=pid, at=_as_float(data.get("at")))


def read_done(dir_: Path | str, run_id: str) -> DoneMarker | None:
    data = _load(done_path(dir_, run_id))
    if data is None:
        return None
    # `ok` absent is NOT success. A marker we cannot read as a clear success
    # must not be reported as one — that is the whole bug this file exists
    # to fix.
    return DoneMarker(
        run_id=run_id,
        ok=data.get("ok") is True,
        identifier=str(data.get("identifier") or ""),
        message=str(data.get("message") or ""),
        at=_as_float(data.get("at")),
        interrupted=data.get("interrupted") is True,
    )


def clear(dir_: Path | str, run_id: str) -> None:
    """Remove both markers for *run_id*, ignoring what is not there.

    Called when a watcher stops tracking a run. Stale markers are otherwise
    the one way a later run can be told the wrong answer, and the run id is
    reused only by a caller that reuses ids.
    """
    for path in (started_path(dir_, run_id), done_path(dir_, run_id)):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            logger.debug("[run_markers] could not remove %s: %s", path, exc)


def sweep(dir_: Path | str, older_than_s: float, now: float) -> int:
    """Delete markers older than *older_than_s*. Returns how many went.

    A run whose MATLAB was killed leaves its ``.started`` behind for ever,
    and the directory sits beside the user's database where they will see
    it. Called on GUI startup, so the cost is paid once per session.
    """
    directory = Path(dir_)
    if not directory.is_dir():
        return 0
    removed = 0
    for path in directory.iterdir():
        if path.suffix not in (STARTED_SUFFIX, DONE_SUFFIX):
            continue
        try:
            if now - path.stat().st_mtime <= older_than_s:
                continue
            path.unlink()
            removed += 1
        except OSError:
            continue
    if removed:
        logger.info(
            "[run_markers] swept %d stale marker(s) from %s", removed, directory
        )
    return removed
