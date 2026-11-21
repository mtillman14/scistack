"""Live pytest progress for the Claude Code status line (dev tool, not shipped).

Loaded by every pytest run through the ``pytest11`` entry point. It does
nothing unless ``SCISTACK_PYTEST_PROGRESS_DIR`` is set (``.claude/settings.json``
sets it for Claude Code sessions). Each run then keeps one JSON file,
``<dir>/<pid>.json``, rewritten as tests finish:

    {"state": "running" | "done", "package": "scidb", "done": 37,
     "total": 412, "passed": 35, "failed": 1, "errors": 0, "skipped": 1,
     "current": "tests/test_x.py::test_y", "failures": [...],
     "started": <epoch>, "updated": <epoch>, "exitstatus": null | int}

``.claude/statusline.sh`` reads every fresh file there. One file per process,
so runs side by side don't overwrite each other. Files older than a day are
removed when a run starts.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

ENV = "SCISTACK_PYTEST_PROGRESS_DIR"
WRITE_EVERY_S = 0.5
MAX_FAILURES = 5
STALE_S = 24 * 3600


class _Progress:
    def __init__(self, directory: Path, package: str):
        self.path = directory / f"{os.getpid()}.json"
        self.last_write = 0.0
        self.data = {
            "state": "collecting", "package": package, "done": 0, "total": 0,
            "passed": 0, "failed": 0, "errors": 0, "skipped": 0,
            "current": "", "failures": [], "started": time.time(),
            "updated": time.time(), "exitstatus": None,
        }

    def write(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_write < WRITE_EVERY_S:
            return
        self.last_write = now
        self.data["updated"] = now
        tmp = self.path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(self.data))
            os.replace(tmp, self.path)
        except OSError as exc:  # never fail a test run over a progress file
            print(f"[pytest-progress] could not write {self.path}: {exc}")


_progress: _Progress | None = None


def _clean_stale(directory: Path) -> None:
    cutoff = time.time() - STALE_S
    for f in directory.glob("*.json"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
        except OSError:
            pass


def pytest_configure(config):
    global _progress
    target = os.environ.get(ENV)
    if not target or hasattr(config, "workerinput"):  # off, or an xdist worker
        return
    directory = Path(target)
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"[pytest-progress] {ENV}={target} unusable ({exc}); progress off")
        return
    _clean_stale(directory)
    _progress = _Progress(directory, Path(str(config.rootpath)).name)
    _progress.write(force=True)


def pytest_collection_finish(session):
    if _progress is None:
        return
    _progress.data["total"] = len(session.items)
    _progress.data["state"] = "running"
    _progress.write(force=True)


def pytest_runtest_logstart(nodeid, location):
    if _progress is not None:
        _progress.data["current"] = nodeid


def pytest_runtest_logreport(report):
    if _progress is None:
        return
    d = _progress.data
    failed_now = False
    if report.when == "call":
        if report.passed:
            d["passed"] += 1
        elif report.failed:
            d["failed"] += 1
            failed_now = True
        elif report.skipped:
            d["skipped"] += 1
    elif report.failed:  # setup / teardown error
        d["errors"] += 1
        failed_now = True
    elif report.when == "setup" and report.skipped:
        d["skipped"] += 1
    if report.when == "teardown":
        d["done"] += 1
    if failed_now and len(d["failures"]) < MAX_FAILURES:
        d["failures"].append(report.nodeid)
    # A new failure is shown at once; everything else is throttled.
    _progress.write(force=failed_now)


def pytest_sessionfinish(session, exitstatus):
    if _progress is None:
        return
    _progress.data["state"] = "done"
    _progress.data["current"] = ""
    _progress.data["exitstatus"] = int(exitstatus)
    _progress.write(force=True)
