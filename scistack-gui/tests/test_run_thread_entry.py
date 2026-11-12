"""Run threads enter their database in one place, and log their phases
(grug stages explicit_db_at_thread_entry + split_run_in_thread,
docs/claude/grugbrain-principles.md).

``_start_run_thread(..., db=)`` is THE thread entry: it wraps the target in
``scidb.using_db(db)``. Before, each of four run targets called
``db.set_current_db()`` by hand, and a fifth that forgot it would have run
against no database at all.
"""

import logging
import threading
import time

import pytest

from scidb import configure_database, get_database
from scidb.model.exceptions import DatabaseNotConfiguredError
from scidb.store.database import _local

from scistack_gui.services import run_service


@pytest.fixture
def db(tmp_path):
    import scifor

    scifor.set_schema([])
    db = configure_database(tmp_path / "entry.duckdb", ["subject"])
    yield db
    db.close()
    scifor.set_schema([])
    if hasattr(_local, "database"):
        delattr(_local, "database")


def _start_and_wait(run_id, target, db):
    seen: dict = {}

    def body():
        try:
            seen["db"] = get_database()
        except DatabaseNotConfiguredError as exc:
            seen["error"] = exc
        finally:
            with run_service._active_runs_lock:
                run_service._active_runs.pop(run_id, None)
            seen["thread"] = threading.current_thread().name

    run_service._start_run_thread(run_id, target or body, db=db)
    deadline = time.monotonic() + 30
    while "thread" not in seen and time.monotonic() < deadline:
        time.sleep(0.01)
    return seen


def test_a_run_thread_enters_its_database(db):
    seen = _start_and_wait("entry-db", None, db)
    assert seen.get("db") is db, seen.get("error")


def test_a_run_thread_without_a_database_has_none(db):
    seen = _start_and_wait("entry-none", None, None)
    assert isinstance(seen.get("error"), DatabaseNotConfiguredError)


def test_db_is_a_required_keyword():
    """Every caller decides, so a new run kind can't forget."""
    with pytest.raises(TypeError):
        run_service._start_run_thread("entry-missing", lambda: None)  # type: ignore[call-arg]


def test_run_phases_are_logged(db, monkeypatch, caplog):
    """A run that ends in its first phase still logs that phase's time, so a
    slow or stuck run names its phase."""
    emitted: list[dict] = []
    monkeypatch.setattr(run_service, "push_message", emitted.append)
    monkeypatch.setattr(run_service, "enter_project_root", lambda: None)

    def missing(name):
        raise KeyError(name)

    monkeypatch.setattr(run_service.registry, "get_function", missing)
    with caplog.at_level(logging.INFO, logger=run_service.logger.name):
        run_service._run_in_thread("entry-phase", "no_such_fn", [], db)

    phase_lines = [r.getMessage() for r in caplog.records if " phase " in r.getMessage()]
    assert any("phase resolve_function done in" in m for m in phase_lines), phase_lines
    assert not any("phase prepare" in m for m in phase_lines)
    (done,) = [m for m in emitted if m.get("type") == "run_done"]
    assert done["success"] is False and done["cancelled"] is False
    assert "no_such_fn" in done["error"]


def test_a_database_that_cannot_be_entered_still_ends_the_run(monkeypatch):
    """If entering the database fails, the target never runs. The run must
    still end with run_done, or the GUI shows "Running…" forever."""
    emitted: list[dict] = []
    monkeypatch.setattr(run_service, "push_message", emitted.append)
    ran = threading.Event()

    run_service._start_run_thread("entry-broken", ran.set, db=object())
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and not any(
        m.get("type") == "run_done" for m in emitted
    ):
        time.sleep(0.01)

    assert not ran.is_set()
    (done,) = [m for m in emitted if m.get("type") == "run_done"]
    assert done["success"] is False
    assert "set_current_db" in done["error"]


def test_prepare_steps_are_logged_and_no_targets_ends_the_run(db, monkeypatch, caplog):
    """_prepare_run is a sequence of timed steps (grug follow-up 9); a node
    with no targets ends at the first one with an explicit run_done."""
    from scistack_gui.services import execution_service

    emitted: list[dict] = []
    monkeypatch.setattr(run_service, "push_message", emitted.append)
    monkeypatch.setattr(run_service, "enter_project_root", lambda: None)
    monkeypatch.setattr(run_service.registry, "get_function", lambda name: (lambda: None))
    monkeypatch.setattr(execution_service, "derive_fn_targets", lambda db, name: [])
    monkeypatch.setattr(execution_service, "disconnected_reason", lambda db, name, node: None)

    with caplog.at_level(logging.INFO, logger=run_service.logger.name):
        run_service._run_in_thread("entry-no-targets", "lonely_fn", [], db)

    lines = [r.getMessage() for r in caplog.records]
    assert any("phase prepare.derive_targets done in" in m for m in lines), lines
    assert not any("phase prepare.resolve_variants" in m for m in lines)
    (done,) = [m for m in emitted if m.get("type") == "run_done"]
    assert done["success"] is False
    assert "lonely_fn" in done["error"]
