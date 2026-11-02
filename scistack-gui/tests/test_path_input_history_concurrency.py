"""Regression test for the concurrent graph-build crash of 2026-08-25.

``GET /path-inputs`` and ``GET /pipeline`` are both sync ``def`` handlers, so
FastAPI runs each on its own threadpool thread against one shared SciDuck
connection. They overlapped by 1ms::

    17:40:54.002  GET /path-inputs        <- thread A, touches the DB
    17:40:54.003  Starting graph build    <- thread B
    17:40:54.028  CRASH in list_path_input_history

``list_path_input_history`` fetched *after* ``execute`` had released the
lock, so DuckDB tore the pending result down mid-fetch::

    _duckdb.InternalException: INTERNAL Error:
        Attempted to dereference shared_ptr that is NULL!

The connection-level guard (``execute`` returns nothing) lives in
sciduckdb/tests/test_fetch_locking.py; this test exercises the real store functions through real threads.
"""

import threading

import pytest
from scistack_gui import pipeline_store


@pytest.fixture
def db_with_history(populated_db):
    """populated_db plus a handful of recorded PathInput history rows."""
    for i in range(5):
        pipeline_store.record_path_input_value(
            populated_db,
            f"pi_{i}",
            "{subject}/{subject}_{session}_CPET.csv",
            f"examples/vo2max/data_{i}",
        )
    return populated_db


def _hammer(targets, threads=8, iterations=25):
    """Run *targets* round-robin across threads; collect any exception."""
    errors: list[BaseException] = []
    barrier = threading.Barrier(threads)

    def run(idx):
        target = targets[idx % len(targets)]
        barrier.wait()  # maximise overlap, as the 1ms window did
        try:
            for _ in range(iterations):
                target()
        except BaseException as exc:  # noqa: BLE001 - recorded for assertion
            errors.append(exc)

    workers = [threading.Thread(target=run, args=(i,)) for i in range(threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    return errors


class TestPathInputHistoryConcurrency:
    def test_list_history_concurrent_with_writes(self, db_with_history):
        """The exact crash shape: readers fetching while a writer executes."""
        db = db_with_history
        counter = iter(range(10_000))

        def reader():
            pipeline_store.list_path_input_history(db)

        def indexer():
            pipeline_store.path_input_history_index(db)

        def writer():
            n = next(counter, 0)
            pipeline_store.record_path_input_value(
                db, f"w_{n}", "{subject}/w.csv", "root"
            )

        errors = _hammer([reader, indexer, writer])

        assert not errors, f"concurrent path-input history access raised: {errors[:3]}"

    def test_lookup_name_concurrent_with_writes(self, db_with_history):
        """lookup_path_input_name had the same unlocked fetch."""
        db = db_with_history
        counter = iter(range(10_000))

        def lookup():
            pipeline_store.lookup_path_input_name(
                db, "{subject}/{subject}_{session}_CPET.csv", "examples/vo2max/data_0"
            )

        def writer():
            n = next(counter, 0)
            pipeline_store.record_path_input_value(
                db, f"w_{n}", "{subject}/w.csv", "root"
            )

        errors = _hammer([lookup, writer])

        assert not errors, f"concurrent lookup raised: {errors[:3]}"

    def test_graph_build_concurrent_with_path_inputs_endpoint(self, db_with_history):
        """End-to-end: the two handlers that actually collided."""
        from scistack_gui.services.pipeline_service import get_pipeline_graph

        db = db_with_history

        def build_graph():
            get_pipeline_graph(db, "main")

        def list_path_inputs():
            pipeline_store.list_path_input_history(db)

        errors = _hammer([build_graph, list_path_inputs], threads=6, iterations=6)

        assert not errors, f"concurrent graph build + path-inputs raised: {errors[:3]}"


class TestPathInputHistoryBehaviourUnchanged:
    """The fetchall/fetchone conversion must not alter results."""

    def test_list_returns_recorded_rows(self, db_with_history):
        rows = pipeline_store.list_path_input_history(db_with_history)
        names = {r["name"] for r in rows}
        assert {f"pi_{i}" for i in range(5)} <= names

    def test_list_filters_by_name(self, db_with_history):
        rows = pipeline_store.list_path_input_history(db_with_history, name="pi_2")
        assert [r["name"] for r in rows] == ["pi_2"]

    def test_empty_root_folder_comes_back_as_none(self, populated_db):
        pipeline_store.record_path_input_value(populated_db, "bare", "{subject}.csv")
        rows = pipeline_store.list_path_input_history(populated_db, name="bare")
        assert rows == [
            {"name": "bare", "template": "{subject}.csv", "root_folder": None}
        ]

    def test_lookup_hit_and_miss(self, db_with_history):
        assert (
            pipeline_store.lookup_path_input_name(
                db_with_history,
                "{subject}/{subject}_{session}_CPET.csv",
                "examples/vo2max/data_3",
            )
            == "pi_3"
        )
        assert (
            pipeline_store.lookup_path_input_name(
                db_with_history, "no/such/template.csv", "nowhere"
            )
            is None
        )
