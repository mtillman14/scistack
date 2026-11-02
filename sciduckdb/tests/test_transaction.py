"""SciDuck.transaction: one re-entrant transaction per connection.

Guards the for_each save (plan-tidy P0, decision D9): save_batch and
record_run each open a level of their own inside the call's one
transaction. DuckDB cannot nest BEGIN and has no savepoints, so only the
outermost level may issue SQL, and a rollback at any level must roll back
the whole transaction.
"""

import pytest

from sciduckdb import SciDuck, TransactionRolledBackError


@pytest.fixture
def duck():
    db = SciDuck(":memory:", dataset_schema=["subject"])
    db.execute("CREATE TABLE t (x INTEGER)")
    yield db
    db.close()


def _rows(duck) -> list:
    return [r[0] for r in duck.fetchall("SELECT x FROM t ORDER BY x")]


def test_nested_commit_writes_once(duck):
    with duck.transaction("outer"):
        duck.execute("INSERT INTO t VALUES (1)")
        with duck.transaction("inner"):
            duck.execute("INSERT INTO t VALUES (2)")
            assert duck._tx_depth == 2
        assert duck._tx_depth == 1
    assert duck._tx_depth == 0
    assert duck._tx_owner is None
    assert _rows(duck) == [1, 2]


def test_inner_exception_rolls_back_the_outer(duck):
    with pytest.raises(RuntimeError, match="boom"):
        with duck.transaction("outer"):
            duck.execute("INSERT INTO t VALUES (1)")
            with duck.transaction("inner"):
                duck.execute("INSERT INTO t VALUES (2)")
                raise RuntimeError("boom")
    assert _rows(duck) == []
    assert duck._tx_depth == 0


def test_swallowed_inner_rollback_dooms_the_outer_commit(duck):
    """A caller that catches an inner failure cannot commit the rest."""
    with pytest.raises(TransactionRolledBackError):
        with duck.transaction("outer"):
            duck.execute("INSERT INTO t VALUES (1)")
            try:
                with duck.transaction("inner"):
                    raise ValueError("inner")
            except ValueError:
                pass
    assert _rows(duck) == []
    assert duck._tx_depth == 0
    assert duck._tx_doomed is False


def test_legacy_begin_commit_nest_inside_a_transaction(duck):
    """_begin/_commit (still used by older writers) join an open transaction
    instead of issuing a second BEGIN."""
    with pytest.raises(RuntimeError):
        with duck.transaction("outer"):
            duck._begin()
            duck.execute("INSERT INTO t VALUES (1)")
            duck._commit()
            raise RuntimeError("after the inner commit")
    assert _rows(duck) == []


def test_statement_after_rollback_works(duck):
    with pytest.raises(Exception):
        with duck.transaction("outer"):
            duck.execute("SELECT * FROM no_such_table")
    duck.execute("INSERT INTO t VALUES (3)")
    assert _rows(duck) == [3]


def test_rollback_listeners_run_after_the_outermost_rollback(duck):
    calls = []
    duck.add_rollback_listener(lambda: calls.append(duck._tx_depth))
    with pytest.raises(RuntimeError):
        with duck.transaction("outer"):
            with duck.transaction("inner"):
                raise RuntimeError("boom")
    assert calls == [0]  # once, and only after the outermost level
    with duck.transaction("clean"):
        duck.execute("INSERT INTO t VALUES (4)")
    assert calls == [0]  # a commit does not notify
