"""``using_db`` is THE way a thread enters a database (grug stage
explicit_db_at_thread_entry, docs/claude/grugbrain-principles.md).

The current database is thread-local: a new thread starts with none, and
before ``using_db`` every thread start had to remember ``set_current_db()``.
"""

import threading

import pytest

import scifor as _scifor

from scidb import configure_database, get_database, using_db
from scidb.model.exceptions import DatabaseNotConfiguredError
from scidb.store.database import _local


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "using.duckdb", ["subject"])
    yield db
    _scifor.set_schema([])
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


def _in_thread(fn):
    """Run *fn* on a fresh thread; return what it returned or raised."""
    out: dict = {}

    def body():
        try:
            out["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 - reported to the test
            out["error"] = exc

    t = threading.Thread(target=body)
    t.start()
    t.join(timeout=30)
    assert not t.is_alive(), "thread did not finish"
    return out


def test_a_new_thread_has_no_database(db):
    """The trap using_db exists for: configuring on one thread does not
    configure another."""
    out = _in_thread(get_database)
    assert isinstance(out.get("error"), DatabaseNotConfiguredError)


def test_using_db_sets_the_database_inside_a_thread(db):
    def work():
        with using_db(db):
            return get_database()

    out = _in_thread(work)
    assert out.get("value") is db, out.get("error")


def test_using_db_leaves_a_fresh_thread_as_it_found_it(db):
    def work():
        with using_db(db):
            pass
        return get_database()

    out = _in_thread(work)
    assert isinstance(out.get("error"), DatabaseNotConfiguredError)


def test_using_db_restores_the_previous_database(db, tmp_path):
    other = configure_database(tmp_path / "other.duckdb", ["subject"])
    try:
        with using_db(db):
            assert get_database() is db
        assert get_database() is other
    finally:
        other.close()


def test_using_db_restores_on_error(db, tmp_path):
    other = configure_database(tmp_path / "other2.duckdb", ["subject"])
    try:
        with pytest.raises(RuntimeError):
            with using_db(db):
                raise RuntimeError("boom")
        assert get_database() is other
    finally:
        other.close()
