"""Wiring and pending-value writes land in the database the caller passes
(cleanup-audit F10).

``layout`` used to carry db-less forwarders (``read_manual_edges()``,
``add_pending_constant(name, value)``, …) that looked the database up with
``get_db()``. Functions that were HANDED a ``db`` — ``put_edge``,
``_build_graph``, both MATLAB generators — mixed them with
``pipeline_store.*(db, …)`` calls, so one function read from two holders of
"which database". With a GUI session per database those can differ. The
forwarders are gone; these tests pin that the passed ``db`` is the one used.
"""

import pytest
import scistack_gui.db as _gui_db
from scistack_gui import pipeline_store
from scistack_gui.services import layout_service

from scidb import configure_database


@pytest.fixture
def other_db(tmp_path, populated_db):
    """A second database, NOT the one ``get_db()`` answers."""
    other = configure_database(tmp_path / "other.duckdb", ["subject", "session"])
    pipeline_store._ensure_tables(other)
    assert _gui_db.get_db() is populated_db
    yield other
    other.close()


@pytest.fixture(autouse=True)
def _no_ws(monkeypatch):
    monkeypatch.setattr(layout_service, "_notify_dag_updated", lambda: None)


def test_pending_values_go_to_the_passed_db(populated_db, other_db):
    layout_service.put_pending_constant(other_db, "low_hz", "42")
    assert "42" in pipeline_store.get_pending_constants(other_db).get("low_hz", set())
    assert "42" not in pipeline_store.get_pending_constants(populated_db).get(
        "low_hz", set()
    )

    layout_service.delete_pending_constant(other_db, "low_hz", "42")
    assert "42" not in pipeline_store.get_pending_constants(other_db).get(
        "low_hz", set()
    )


def test_manual_edges_go_to_the_passed_db(populated_db, other_db):
    layout_service.put_edge(other_db, "manual__route1", "var__A", "fn__b")
    assert [e["id"] for e in pipeline_store.get_manual_edges(other_db)] == [
        "manual__route1"
    ]
    assert pipeline_store.get_manual_edges(populated_db) == []

    layout_service.delete_edge(other_db, "manual__route1")
    assert pipeline_store.get_manual_edges(other_db) == []
