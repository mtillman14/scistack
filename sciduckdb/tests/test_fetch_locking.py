"""Regression tests for the execute+fetch locking contract.

Background (2026-08-25): a GUI graph build crashed with

    _duckdb.InternalException: INTERNAL Error:
        Attempted to dereference shared_ptr that is NULL!

raised from ``pipeline_store.list_path_input_history``, which did

    _duck(db)._execute(sql, params).fetchall()

``SciDuck._execute`` (now ``execute``) released ``_lock`` when its ``with``
block exited, so the ``.fetchall()`` ran unprotected. DuckDB's Python connection returns *itself*
from ``execute()``, so a concurrent ``execute()`` from another thread tears
down the pending result mid-fetch. Two FastAPI threadpool threads (GET
/path-inputs and GET /pipeline, 1ms apart) hit exactly that window.

``fetchall`` / ``fetchone`` / ``fetchdf`` are the safe helpers: they hold
``_lock`` across execute *and* fetch. ``execute`` now returns nothing, so the
original chain cannot be written against a SciStack connection at all.
(A repo-wide AST scan for the chain guarded this until the 2026-11-01
rename; the name ``execute`` now collides with raw DuckDB connections.)
"""

import threading

import pytest

from sciduckdb import DuckConnection


@pytest.fixture
def duck():
    """A fresh in-memory DuckConnection."""
    db = DuckConnection(":memory:")
    yield db
    db.close()


class TestExecuteReturnsNothing:
    """The bug class is unwritable: there is no result to fetch outside the lock."""

    def test_execute_returns_none(self, duck):
        assert duck.execute("CREATE TABLE t (a INTEGER)") is None
        assert duck.execute("INSERT INTO t VALUES (?)", [1]) is None

    def test_chained_fetch_fails_loudly(self, duck):
        with pytest.raises(AttributeError):
            duck.execute("SELECT 1").fetchall()  # type: ignore[union-attr]


class TestFetchOne:
    """fetchone is the locked primitive the four offenders were missing."""

    def test_returns_row_tuple(self, duck):
        duck.execute("CREATE TABLE t (a INTEGER, b VARCHAR)")
        duck.execute("INSERT INTO t VALUES (1, 'x')")
        assert duck.fetchone("SELECT a, b FROM t") == (1, "x")

    def test_returns_none_when_no_rows(self, duck):
        duck.execute("CREATE TABLE t (a INTEGER)")
        assert duck.fetchone("SELECT a FROM t WHERE a = 99") is None

    def test_honours_params(self, duck):
        duck.execute("CREATE TABLE t (a INTEGER, b VARCHAR)")
        duck.execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
        assert duck.fetchone("SELECT b FROM t WHERE a = ?", [2]) == ("y",)

    def test_raises_on_bad_sql(self, duck):
        with pytest.raises(Exception):
            duck.fetchone("SELECT * FROM table_that_does_not_exist")

    def test_connection_usable_after_failure(self, duck):
        """_recover_from_autocommit_failure must leave the connection healthy."""
        with pytest.raises(Exception):
            duck.fetchone("SELECT * FROM nope")
        assert duck.fetchone("SELECT 1") == (1,)


class TestConcurrentFetch:
    """The actual crash condition: many threads, one shared connection."""

    def _hammer(self, target, threads=8, iterations=40):
        errors: list[BaseException] = []
        barrier = threading.Barrier(threads)

        def run():
            barrier.wait()  # maximise overlap
            try:
                for _ in range(iterations):
                    target()
            except BaseException as exc:  # noqa: BLE001 - recorded and re-raised
                errors.append(exc)

        workers = [threading.Thread(target=run) for _ in range(threads)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()
        return errors

    def test_concurrent_fetchall_is_safe(self, duck):
        duck.execute("CREATE TABLE t (a INTEGER)")
        duck.execute("INSERT INTO t SELECT * FROM range(200)")

        errors = self._hammer(lambda: duck.fetchall("SELECT a FROM t ORDER BY a"))

        assert not errors, f"concurrent fetchall raised: {errors[:3]}"

    def test_concurrent_fetchone_is_safe(self, duck):
        duck.execute("CREATE TABLE t (a INTEGER)")
        duck.execute("INSERT INTO t SELECT * FROM range(200)")

        errors = self._hammer(lambda: duck.fetchone("SELECT COUNT(*) FROM t"))

        assert not errors, f"concurrent fetchone raised: {errors[:3]}"

    def test_concurrent_mixed_readers_and_writers(self, duck):
        """Mirrors the crash: readers fetching while another thread executes."""
        duck.execute("CREATE TABLE t (a INTEGER)")
        duck.execute("INSERT INTO t SELECT * FROM range(200)")
        duck.execute("CREATE TABLE w (a INTEGER)")

        counter = iter(range(10_000))

        def mixed():
            duck.fetchall("SELECT a FROM t ORDER BY a")
            duck.execute("INSERT INTO w VALUES (?)", [next(counter, 0)])
            duck.fetchone("SELECT COUNT(*) FROM t")

        errors = self._hammer(mixed, threads=6, iterations=25)

        assert not errors, f"concurrent mixed access raised: {errors[:3]}"
