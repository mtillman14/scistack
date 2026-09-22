"""A server with no database: Explorer ▸ Plot CSV.

Plotting a CSV needs no project and no DuckDB — ``plot_service.get_source``
builds a ``scistackplot`` ``CsvSource`` from the path and every plot entry
point is written as ``db_connection(..., needed=not csv_path)``. What used to
make it need a pipeline anyway was *startup*: ``--db`` was required, so a CSV
tab had to borrow an open project's server and died when that project closed.

These tests pin the two halves of the fix that can silently regress:

* the declaration — which methods a database-less server may serve — must
  come from the handler table (``db_optional`` / ``needs_db``), not from a
  hand-kept list that drifts. A new ``plot_*`` method that forgets to
  declare itself is refused at runtime with no hint why;
* a method that does need a database must be refused *by name, up front*,
  rather than reaching ``get_db()`` and coming back as "Database not
  initialised. Call init_db() first." — which reads like a broken session
  instead of one that never had a database.
"""

from __future__ import annotations

import pytest

from scistack_gui import server
from scistack_gui.api.handlers import without_database
from scistack_gui.api.plot import PLOT_HANDLERS
from scistack_gui.api.tables import ALL_HANDLERS

#: The methods the Plot Studio issues when it is showing a CSV — read off
#: frontend/src/components/PlotStudio. Anything the studio calls on a CSV
#: tab must be servable without a database, or the tab half-works.
CSV_TAB_METHODS = {
    "plot_describe",
    "plot_capabilities",
    "plot_resolve",
    "plot_export",
    "plot_save_start",
    "plot_location_tree",
    "report_client_error",
}

#: Methods that write into the project. A CSV has nowhere to put any of
#: them, so a plot-only server must refuse them rather than half-perform.
PROJECT_ONLY_METHODS = {
    "plot_add_to_pipeline",
    "plot_variant_sets_save",
    "plot_invalidate",
}


def test_every_method_a_csv_tab_calls_is_servable_without_a_database():
    servable = without_database(ALL_HANDLERS)
    missing = CSV_TAB_METHODS - servable
    assert not missing, (
        f"the Plot Studio calls {sorted(missing)} on a CSV tab, but they are "
        f"not declared db_optional — a plot-only server will refuse them"
    )


def test_project_writing_methods_are_not_servable_without_a_database():
    servable = without_database(ALL_HANDLERS)
    leaked = PROJECT_ONLY_METHODS & servable
    assert not leaked, (
        f"{sorted(leaked)} write into the project; a CSV tab has nowhere to "
        f"put them and they must not be declared db_optional"
    )


def test_db_optional_is_only_claimed_by_methods_that_take_a_csv_path():
    """The declaration has to match the request model.

    ``db_optional`` means "this call decides from its own request whether it
    needs the database", and the only thing that decides is ``csv_path``. A
    row that claims it without the field would be handed ``None`` and fail
    on the first attribute access.
    """
    for handler in PLOT_HANDLERS:
        if not handler.db_optional:
            continue
        assert handler.params is not None, f"{handler.name} has no request model"
        assert "csv_path" in handler.params.model_fields, (
            f"{handler.name} declares db_optional but its request has no "
            f"csv_path, so nothing can tell it the database is unnecessary"
        )


def test_plot_only_refuses_a_database_method_by_name(monkeypatch):
    """The refusal names the method and says why, before any db access."""
    sent: list[dict] = []
    monkeypatch.setattr(server, "_send", lambda obj: sent.append(obj))
    monkeypatch.setattr(server, "_PLOT_ONLY", True)

    server._handle_request({"jsonrpc": "2.0", "id": 7, "method": "get_pipeline"})

    assert len(sent) == 1, "exactly one frame per request"
    error = sent[0]["error"]["message"]
    assert "get_pipeline" in error
    assert "database" in error.lower()
    # The failure this replaces:
    assert "init_db" not in error


def test_plot_only_still_reports_an_unknown_method_as_unknown(monkeypatch):
    """A typo must not be reported as "needs a database"."""
    sent: list[dict] = []
    monkeypatch.setattr(server, "_send", lambda obj: sent.append(obj))
    monkeypatch.setattr(server, "_PLOT_ONLY", True)

    server._handle_request({"jsonrpc": "2.0", "id": 8, "method": "plot_descibe"})

    assert sent[0]["error"]["code"] == -32601
    assert "Method not found" in sent[0]["error"]["message"]


def test_plot_only_lets_a_declared_method_through(monkeypatch):
    """The gate must not be the thing that blocks CSV plotting itself."""
    sent: list[dict] = []
    monkeypatch.setattr(server, "_send", lambda obj: sent.append(obj))
    monkeypatch.setattr(server, "_PLOT_ONLY", True)
    monkeypatch.setitem(server.METHODS, "plot_describe", lambda params: {"ok": True})

    server._handle_request(
        {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "plot_describe",
            "params": {"csv_path": "/tmp/x.csv"},
        }
    )

    assert sent[0].get("result") == {"ok": True}, sent[0]


def test_a_db_optional_method_is_handed_none_when_no_database_is_open(monkeypatch):
    """The whole point of the flag: `get_db()` is never called.

    Without this the dispatch raises "Database not initialised" before the
    handler ever sees the ``csv_path`` that says it needed no database.
    """
    from scistack_gui.api import handlers as handlers_mod

    seen: list[object] = []

    def _call(db, req):
        seen.append(db)
        return {"ok": True}

    handler = handlers_mod.Handler(
        "fake_plot",
        None,
        next(h for h in PLOT_HANDLERS if h.name == "plot_describe").params,
        _call,
        db_optional=True,
    )
    monkeypatch.setattr("scistack_gui.db.is_loaded", lambda: False)

    rpc = handlers_mod.rpc_methods([handler])["fake_plot"]
    assert rpc({"variable": "", "csv_path": "/tmp/x.csv"}) == {"ok": True}
    assert seen == [None]


def test_a_required_db_method_still_raises_when_no_database_is_open(monkeypatch):
    """Only `db_optional` rows get the exemption."""
    from scistack_gui.api import handlers as handlers_mod

    handler = handlers_mod.Handler("fake_needs_db", None, None, lambda db: {"ok": True})
    monkeypatch.setattr("scistack_gui.db.is_loaded", lambda: False)
    monkeypatch.setattr("scistack_gui.db._db", None, raising=False)

    rpc = handlers_mod.rpc_methods([handler])["fake_needs_db"]
    with pytest.raises(RuntimeError, match="Database not initialised"):
        rpc({})
