"""DuckConnection.insert_frame: THE way to insert a frame (design review F14).

It replaced scidb's raw ``duck.con.register`` / ``duck.con.execute`` inserts,
which skipped the contention log and the autocommit-failure recovery.
"""

import logging

import pandas as pd
import pytest

from sciduckdb import DuckConnection


@pytest.fixture
def db(tmp_path):
    con = DuckConnection(str(tmp_path / "insert_frame.duckdb"))
    con.execute("CREATE TABLE t (id VARCHAR PRIMARY KEY, x DOUBLE)")
    yield con
    con.close()


def test_inserts_pandas_rows_by_column_name(db):
    n = db.insert_frame("t", pd.DataFrame({"x": [1.5, 2.5], "id": ["a", "b"]}))
    assert n == 2
    assert db.fetchall("SELECT id, x FROM t ORDER BY id") == [("a", 1.5), ("b", 2.5)]


def test_inserts_arrow_table(db):
    pa = pytest.importorskip("pyarrow")
    db.insert_frame("t", pa.table({"id": ["c"], "x": [3.0]}))
    assert db.fetchall("SELECT id FROM t") == [("c",)]


def test_conflict_skips_existing_keys(db):
    db.insert_frame("t", pd.DataFrame({"id": ["a"], "x": [1.0]}))
    db.insert_frame("t", pd.DataFrame({"id": ["a", "b"], "x": [9.0, 2.0]}), conflict=["id"])
    assert db.fetchall("SELECT id, x FROM t ORDER BY id") == [("a", 1.0), ("b", 2.0)]


def test_empty_frame_is_a_no_op(db):
    assert db.insert_frame("t", pd.DataFrame({"id": [], "x": []})) == 0


def test_failure_is_logged_and_the_connection_still_works(db, caplog):
    caplog.set_level(logging.DEBUG, logger="sciduck")
    with pytest.raises(Exception):
        db.insert_frame("no_such_table", pd.DataFrame({"id": ["a"]}))
    assert any("insert_frame FAILED" in r.getMessage() for r in caplog.records)
    db.insert_frame("t", pd.DataFrame({"id": ["z"], "x": [0.0]}))
    assert db.fetchall("SELECT id FROM t") == [("z",)]


def test_failure_inside_a_transaction_rolls_back(db):
    with pytest.raises(Exception):
        with db.transaction():
            db.insert_frame("t", pd.DataFrame({"id": ["a"], "x": [1.0]}))
            db.insert_frame("t", pd.DataFrame({"id": ["a"], "x": [2.0]}))  # PK clash
    assert db.fetchall("SELECT COUNT(*) FROM t") == [(0,)]


def test_timing_line_is_logged(db, caplog):
    caplog.set_level(logging.DEBUG, logger="sciduck")
    db.insert_frame("t", pd.DataFrame({"id": ["q"], "x": [1.0]}))
    assert any(r.getMessage().startswith("[insert_frame] table=t rows=1 ms=")
               for r in caplog.records)
