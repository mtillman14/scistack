"""One handler table, two transports — the table is the contract.

Every ``Handler`` in a table must be reachable as a JSON-RPC method (the
extension), as an HTTP route (the browser), and be named in the frontend's
route map with the SAME path; and its lock policy must be what the dispatch
loop actually applies. Before the table these were four hand-kept lists,
and each could drift silently — see ``api/handlers.py``.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from scistack_gui.api.handlers import Handler, rpc_methods, self_managed
from scistack_gui.api.plot import PLOT_HANDLERS

GUI_ROOT = Path(__file__).parent.parent
TABLES: dict[str, tuple[Handler, ...]] = {"plot": PLOT_HANDLERS}
ALL_HANDLERS = [h for table in TABLES.values() for h in table]


def _frontend_routes() -> dict[str, tuple[str, str, bool]]:
    """``{method: (path, http_method, has_body)}`` from ``frontend/src/api.ts``
    — only the static-path entries, which is every table handler."""
    src = (GUI_ROOT / "frontend/src/api.ts").read_text()
    out = {}
    for m in re.finditer(
        r"^\s*(\w+):\s*\{\s*path:\s*'([^']+)'(?:,\s*method:\s*'(\w+)')?(?:,\s*body:\s*(true))?",
        src,
        re.M,
    ):
        out[m.group(1)] = (m.group(2), m.group(3) or "GET", m.group(4) == "true")
    return out


@pytest.mark.parametrize("h", ALL_HANDLERS, ids=lambda h: h.name)
def test_every_handler_is_a_json_rpc_method(h):
    from scistack_gui import server

    assert h.name in server.METHODS
    assert getattr(server.METHODS[h.name], "handler", None) is h, (
        f"{h.name} in METHODS is not the table's entry — a hand-written "
        "_h_ shadowing it?"
    )


@pytest.mark.parametrize("h", ALL_HANDLERS, ids=lambda h: h.name)
def test_every_handler_is_an_http_route(h):
    from scistack_gui.app import create_app

    # openapi() sees every route however starlette nests them (2026-09-13).
    paths = create_app().openapi()["paths"]
    assert h.http_method.lower() in paths.get("/api" + h.path, {}), (
        f"{h.name}: no {h.http_method} /api{h.path} among {sorted(paths)}"
    )


@pytest.mark.parametrize("h", ALL_HANDLERS, ids=lambda h: h.name)
def test_every_handler_is_in_the_frontend_route_map_with_the_same_path(h):
    routes = _frontend_routes()
    assert h.name in routes, (
        f"{h.name} is not in frontend/src/api.ts — the browser build would "
        "throw 'Unknown method'"
    )
    path, method, has_body = routes[h.name]
    assert path == "/api" + h.path
    assert method == h.http_method
    assert has_body == (h.params is not None), (
        f"{h.name}: the table says {'a body' if h.params else 'no body'}, "
        f"api.ts says {'body' if has_body else 'no body'}"
    )


@pytest.mark.parametrize("h", ALL_HANDLERS, ids=lambda h: h.name)
def test_lock_policy_is_what_the_dispatch_loop_applies(h):
    from scistack_gui import server

    assert (h.name in server.SELF_MANAGED_DB_METHODS) == (not h.holds_db_lock)


def test_a_handler_that_needs_no_database_never_holds_the_lock():
    for h in ALL_HANDLERS:
        if not h.needs_db:
            assert not h.holds_db_lock, h.name


def test_the_table_is_the_only_declaration():
    """No hand-written ``_h_plot_*`` in server.py and no ``@router.post`` in
    api/plot.py: a second spelling of a table handler is the drift the table
    exists to end."""
    server_src = (GUI_ROOT / "scistack_gui/server.py").read_text()
    names = {h.name for h in ALL_HANDLERS}
    hand_written = [
        n.name[len("_h_"):]
        for n in ast.walk(ast.parse(server_src))
        if isinstance(n, ast.FunctionDef)
        and n.name.startswith("_h_")
        and n.name[len("_h_"):] in names
    ]
    assert not hand_written, f"table handlers also written by hand: {hand_written}"

    plot_src = (GUI_ROOT / "scistack_gui/api/plot.py").read_text()
    decorated = [
        n.name
        for n in ast.walk(ast.parse(plot_src))
        if isinstance(n, ast.FunctionDef)
        and any(
            isinstance(d, ast.Call)
            and isinstance(d.func, ast.Attribute)
            and d.func.attr in {"get", "post", "put", "delete"}
            for d in n.decorator_list
        )
    ]
    assert not decorated, f"routes declared outside the table: {decorated}"


def test_rpc_params_are_validated_like_the_http_body():
    """The RPC transport used to read raw dicts; now both transports reject
    the same request — and BEFORE the database is touched (no database is
    open in this test; a bad request must not read as "not initialised")."""
    from pydantic import ValidationError

    methods = rpc_methods(PLOT_HANDLERS)
    with pytest.raises(ValidationError):
        methods["plot_capabilities"]({})  # `spec` is required


def test_self_managed_reads_the_rows():
    assert self_managed(PLOT_HANDLERS) == {
        h.name for h in PLOT_HANDLERS if not h.holds_db_lock
    }
    assert "plot_add_to_pipeline" not in self_managed(PLOT_HANDLERS)
    assert "plot_invalidate" not in self_managed(PLOT_HANDLERS)


def test_http_errors_map_to_statuses(client):
    """The mapping a route used to spell inline is now a row: a KeyError
    inside the service is a 400 with the message intact."""
    pytest.importorskip("scistackplot")
    response = client.post(
        "/api/plot/capabilities", json={"spec": {"nonsense": True}}
    )
    assert response.status_code == 400
    assert response.json()["detail"]


def test_the_report_needs_no_database_over_http(client):
    response = client.post(
        "/api/client-error", json={"where": "tab", "message": "boom"}
    )
    assert response.status_code == 200
    assert response.json()["message"] == "boom"
