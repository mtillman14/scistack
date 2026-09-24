"""Regression tests for the execute+fetch locking contract.

Background (2026-08-25): a GUI graph build crashed with

    _duckdb.InternalException: INTERNAL Error:
        Attempted to dereference shared_ptr that is NULL!

raised from ``pipeline_store.list_path_input_history``, which did

    _duck(db)._execute(sql, params).fetchall()

``SciDuck._execute`` releases ``_lock`` when its ``with`` block exits, so the
``.fetchall()`` ran unprotected. DuckDB's Python connection returns *itself*
from ``execute()``, so a concurrent ``execute()`` from another thread tears
down the pending result mid-fetch. Two FastAPI threadpool threads (GET
/path-inputs and GET /pipeline, 1ms apart) hit exactly that window.

``_fetchall`` / ``_fetchone`` / ``_fetchdf`` are the safe helpers — they hold
``_lock`` across execute *and* fetch.
"""

import ast
import threading
from pathlib import Path

import pytest

from sciduckdb import SciDuck

# Fetch methods that must never be chained onto a bare _execute() call.
_FETCH_METHODS = {
    "fetchall",
    "fetchone",
    "fetchdf",
    "fetchmany",
    "fetchnumpy",
    "df",
    "arrow",
    "fetch_arrow_table",
    "fetch_df",
}

# Packages scanned by the repo-wide guard.
_SCANNED_PACKAGES = (
    "scidb",
    "scidb-net",
    "sciduckdb",
    "scifor",
    "scihist",
    "scilineage",
    "scimatlab",
    "scistack-gui",
    "scistacklog",
    "scistackplot",
    "scistackplotdb",
)

_SKIP_DIR_PARTS = {".venv", "node_modules", "__pycache__", ".git", "build", "dist"}


@pytest.fixture
def duck():
    """A fresh in-memory SciDuck."""
    db = SciDuck(":memory:", dataset_schema=["subject", "session"])
    yield db
    db.close()


def _repo_root() -> Path:
    """Walk up until we find the directory holding the sibling packages."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "sciduckdb").is_dir() and (parent / "scifor").is_dir():
            return parent
    raise RuntimeError("could not locate repo root from %s" % __file__)


def _python_files():
    root = _repo_root()
    for pkg in _SCANNED_PACKAGES:
        pkg_dir = root / pkg
        if not pkg_dir.is_dir():
            continue
        for path in pkg_dir.rglob("*.py"):
            if _SKIP_DIR_PARTS & set(path.parts):
                continue
            yield path


class _UnlockedFetchVisitor(ast.NodeVisitor):
    """Find ``<anything>._execute(...).<fetch>()`` call chains."""

    def __init__(self):
        self.hits: list[tuple[int, str]] = []

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        # Outer call must be an attribute access naming a fetch method...
        if isinstance(func, ast.Attribute) and func.attr in _FETCH_METHODS:
            inner = func.value
            # ...applied directly to the result of an _execute()/_executemany() call.
            if (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Attribute)
                and inner.func.attr in ("_execute", "_executemany")
            ):
                self.hits.append((node.lineno, f"_execute(...).{func.attr}()"))
        self.generic_visit(node)


class TestNoUnlockedFetchAfterExecute:
    """Repo-wide guard for the whole bug class, not just the one crash site.

    Uses the AST rather than grep: three of the four original offenders were
    split across lines, so a line-oriented search found only one of them.
    """

    def test_no_unlocked_fetch_after_execute(self):
        offenders = []
        root = _repo_root()
        for path in _python_files():
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            visitor = _UnlockedFetchVisitor()
            visitor.visit(tree)
            for lineno, pattern in visitor.hits:
                offenders.append(f"{path.relative_to(root)}:{lineno}: {pattern}")

        assert not offenders, (
            "execute+fetch must stay under one lock — use _fetchall / _fetchone "
            "/ _fetchdf instead. _execute releases _lock before you fetch, so a "
            "concurrent execute() on the shared connection can raise "
            "'INTERNAL Error: Attempted to dereference shared_ptr that is NULL'."
            "\nOffending call sites:\n  " + "\n  ".join(offenders)
        )

    def test_guard_detects_the_original_pattern(self):
        """The guard must actually fire — otherwise it passes vacuously."""
        tree = ast.parse("rows = _duck(db)._execute(sql, params).fetchall()\n")
        visitor = _UnlockedFetchVisitor()
        visitor.visit(tree)
        assert visitor.hits == [(1, "_execute(...).fetchall()")]

    def test_guard_allows_locked_helpers(self):
        tree = ast.parse(
            "a = _duck(db)._fetchall(sql, params)\n"
            "b = _duck(db)._fetchone(sql, params)\n"
            "c = con.execute(sql).fetchall()\n"  # inside SciDuck, under _lock
        )
        visitor = _UnlockedFetchVisitor()
        visitor.visit(tree)
        assert visitor.hits == []


class TestFetchOne:
    """_fetchone is the locked primitive the four offenders were missing."""

    def test_returns_row_tuple(self, duck):
        duck._execute("CREATE TABLE t (a INTEGER, b VARCHAR)")
        duck._execute("INSERT INTO t VALUES (1, 'x')")
        assert duck._fetchone("SELECT a, b FROM t") == (1, "x")

    def test_returns_none_when_no_rows(self, duck):
        duck._execute("CREATE TABLE t (a INTEGER)")
        assert duck._fetchone("SELECT a FROM t WHERE a = 99") is None

    def test_honours_params(self, duck):
        duck._execute("CREATE TABLE t (a INTEGER, b VARCHAR)")
        duck._execute("INSERT INTO t VALUES (1, 'x'), (2, 'y')")
        assert duck._fetchone("SELECT b FROM t WHERE a = ?", [2]) == ("y",)

    def test_public_alias_matches(self, duck):
        """MATLAB cannot call underscore methods; fetchone() mirrors fetchall()."""
        duck._execute("CREATE TABLE t (a INTEGER)")
        duck._execute("INSERT INTO t VALUES (7)")
        assert duck.fetchone("SELECT a FROM t") == duck._fetchone("SELECT a FROM t")

    def test_raises_on_bad_sql(self, duck):
        with pytest.raises(Exception):
            duck._fetchone("SELECT * FROM table_that_does_not_exist")

    def test_connection_usable_after_failure(self, duck):
        """_recover_from_autocommit_failure must leave the connection healthy."""
        with pytest.raises(Exception):
            duck._fetchone("SELECT * FROM nope")
        assert duck._fetchone("SELECT 1") == (1,)


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
        duck._execute("CREATE TABLE t (a INTEGER)")
        duck._execute("INSERT INTO t SELECT * FROM range(200)")

        errors = self._hammer(lambda: duck._fetchall("SELECT a FROM t ORDER BY a"))

        assert not errors, f"concurrent _fetchall raised: {errors[:3]}"

    def test_concurrent_fetchone_is_safe(self, duck):
        duck._execute("CREATE TABLE t (a INTEGER)")
        duck._execute("INSERT INTO t SELECT * FROM range(200)")

        errors = self._hammer(lambda: duck._fetchone("SELECT COUNT(*) FROM t"))

        assert not errors, f"concurrent _fetchone raised: {errors[:3]}"

    def test_concurrent_mixed_readers_and_writers(self, duck):
        """Mirrors the crash: readers fetching while another thread executes."""
        duck._execute("CREATE TABLE t (a INTEGER)")
        duck._execute("INSERT INTO t SELECT * FROM range(200)")
        duck._execute("CREATE TABLE w (a INTEGER)")

        counter = iter(range(10_000))

        def mixed():
            duck._fetchall("SELECT a FROM t ORDER BY a")
            duck._execute("INSERT INTO w VALUES (?)", [next(counter, 0)])
            duck._fetchone("SELECT COUNT(*) FROM t")

        errors = self._hammer(mixed, threads=6, iterations=25)

        assert not errors, f"concurrent mixed access raised: {errors[:3]}"
