"""One handler table, two transports — the table is the contract.

Every ``Handler`` in every table must be reachable as a JSON-RPC method (the
extension), as an HTTP route (the browser) unless it declares itself
RPC-only, and be named in the frontend's route map with the SAME path,
method and body-ness; and its lock policy must be what the dispatch loop
actually applies. Before the tables these were four hand-kept lists, and
each could drift silently — see ``api/handlers.py``.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from scistack_gui.api.handlers import Handler, rpc_methods, self_managed
from scistack_gui.api.plot import PLOT_HANDLERS
from scistack_gui.api.tables import ALL_HANDLERS, TABLES

GUI_ROOT = Path(__file__).parent.parent
HTTP_HANDLERS = [h for h in ALL_HANDLERS if h.path is not None]
RPC_ONLY = [h for h in ALL_HANDLERS if h.path is None]


# --- reading frontend/src/api.ts ------------------------------------------


def _template_to_path(template: str) -> str:
    """``/api/layout/${encodeURIComponent(p.node_id as string)}/config`` →
    ``/layout/{node_id}/config``.

    A ``${...}`` block that builds a query string (contains ``?``) is
    dropped; a literal ``?`` cuts the rest; any other block becomes the
    first ``p.<field>`` it names, as a path placeholder.
    """
    out = []
    i = 0
    while i < len(template):
        if template.startswith("${", i):
            depth, j = 0, i
            while j < len(template):
                if template.startswith("${", j):
                    depth += 1
                    j += 2
                    continue
                if template[j] == "}":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            block = template[i + 2 : j]
            if "?" not in block:
                m = re.search(r"\bp\.(\w+)", block)
                out.append("{" + (m.group(1) if m else "?") + "}")
            i = j + 1
            continue
        if template[i] == "?":
            break
        out.append(template[i])
        i += 1
    path = "".join(out)
    return path[len("/api") :] if path.startswith("/api") else path


def _frontend_routes() -> dict[str, tuple[str, str, bool]]:
    """``{method: (path, http_method, has_body)}`` from ``frontend/src/api.ts``."""
    src = (GUI_ROOT / "frontend/src/api.ts").read_text()
    out = {}
    for line in src.splitlines():
        m = re.match(r"^\s*(\w+):\s*\{\s*path:\s*(.*)$", line)
        if not m:
            continue
        name, rest = m.group(1), m.group(2)
        if rest.startswith("'"):
            path = rest[1 : rest.index("'", 1)]
            path = path[len("/api") :] if path.startswith("/api") else path
        else:
            tpl = rest[rest.index("`") + 1 : rest.rindex("`")]
            path = _template_to_path(tpl)
        method = re.search(r"method:\s*'(\w+)'", rest)
        body = re.search(r"body:\s*true", rest) is not None
        out[name] = (path, method.group(1) if method else "GET", body)
    return out


def _strip_converters(path: str) -> str:
    return re.sub(r"\{(\w+):\w+\}", r"{\1}", path)


# --- per row -------------------------------------------------------------------


@pytest.mark.parametrize("h", ALL_HANDLERS, ids=lambda h: h.name)
def test_every_handler_is_a_json_rpc_method(h):
    from scistack_gui import server

    assert h.name in server.METHODS
    assert getattr(server.METHODS[h.name], "handler", None) is h, (
        f"{h.name} in METHODS is not the table's entry"
    )


@pytest.mark.parametrize("h", HTTP_HANDLERS, ids=lambda h: h.name)
def test_every_handler_is_an_http_route(h):
    from scistack_gui.app import create_app

    # openapi() sees every route however starlette nests them (2026-09-13).
    paths = create_app().openapi()["paths"]
    wanted = "/api" + _strip_converters(h.path)
    assert h.http_method.lower() in paths.get(wanted, {}), (
        f"{h.name}: no {h.http_method} {wanted} among {sorted(paths)}"
    )


@pytest.mark.parametrize("h", HTTP_HANDLERS, ids=lambda h: h.name)
def test_every_handler_is_in_the_frontend_route_map_with_the_same_path(h):
    routes = _frontend_routes()
    assert h.name in routes, (
        f"{h.name} is not in frontend/src/api.ts — the browser build would "
        "throw 'Unknown method'"
    )
    path, method, has_body = routes[h.name]
    assert path == _strip_converters(h.path), f"{h.name}: api.ts {path!r} vs table {h.path!r}"
    assert method == h.http_method, f"{h.name}: api.ts {method} vs table {h.http_method}"
    assert has_body == h.http_body, (
        f"{h.name}: the table says {'a body' if h.http_body else 'no body'}, "
        f"api.ts says {'body' if has_body else 'no body'}"
    )


@pytest.mark.parametrize("h", RPC_ONLY, ids=lambda h: h.name)
def test_an_rpc_only_method_is_not_offered_to_the_browser(h):
    """A method with no HTTP path is one the browser has no host for
    (MATLAB terminal dispatch). Listing it in api.ts would only turn
    'Unknown method' into a 404."""
    assert h.name not in _frontend_routes()


def test_the_frontend_map_names_no_method_the_server_lacks():
    """The other direction: an api.ts entry that no table declares is a call
    the extension answers with 'Method not found'."""
    from scistack_gui import server

    unknown = sorted(
        n
        for n in _frontend_routes()
        if n not in server.METHODS and n not in {"create_project", "open_project"}
    )
    assert not unknown, f"api.ts names methods no table declares: {unknown}"


@pytest.mark.parametrize("h", ALL_HANDLERS, ids=lambda h: h.name)
def test_lock_policy_is_what_the_dispatch_loop_applies(h):
    from scistack_gui import server

    assert (h.name in server.SELF_MANAGED_DB_METHODS) == (not h.holds_db_lock)


def test_path_placeholders_are_model_fields():
    """A ``{name}`` in the path has to land somewhere: the request model must
    have that field, or the URL segment is silently dropped."""
    for h in HTTP_HANDLERS:
        for p in h.path_params:
            assert h.params is not None and p in h.params.model_fields, (
                f"{h.name}: path parameter {{{p}}} is not a field of "
                f"{h.params.__name__ if h.params else None}"
            )


def test_the_tables_are_the_only_declaration():
    """No hand-written ``_h_*`` in server.py and no ``@router.<verb>`` for a
    table method anywhere under api/: a second spelling is the drift the
    tables exist to end."""
    server_src = (GUI_ROOT / "scistack_gui/server.py").read_text()
    hand_written = [
        n.name
        for n in ast.walk(ast.parse(server_src))
        if isinstance(n, ast.FunctionDef) and n.name.startswith("_h_")
    ]
    assert not hand_written, f"hand-written RPC handlers: {hand_written}"

    names = {h.name for h in ALL_HANDLERS}
    decorated = []
    for module in sorted((GUI_ROOT / "scistack_gui/api").glob("*.py")):
        for n in ast.walk(ast.parse(module.read_text())):
            if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            routed = any(
                isinstance(d, ast.Call)
                and isinstance(d.func, ast.Attribute)
                and d.func.attr in {"get", "post", "put", "delete", "patch"}
                for d in n.decorator_list
            )
            if routed and n.name in names:
                decorated.append(f"{module.name}:{n.name}")
    assert not decorated, f"table methods also declared as routes: {decorated}"


# --- the mechanism ------------------------------------------------------------


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


def test_http_body_is_inferred_from_the_model_and_the_path():
    from pydantic import BaseModel

    class Only(BaseModel):
        node_id: str

    class More(BaseModel):
        node_id: str
        x: float

    assert not Handler("a", "/n/{node_id}", Only, lambda r: r).http_body
    assert Handler("b", "/n/{node_id}", More, lambda r: r).http_body
    assert not Handler("c", "/n", More, lambda r: r, http_method="GET").http_body
    assert Handler("d", "/n/{node_id}", Only, lambda r: r, body=True).http_body


def test_http_errors_map_to_statuses(client):
    """The mapping a route used to spell inline is now a row: a ValueError
    inside the service is a 400 with the message intact."""
    response = client.put("/api/pipelines/main", json={"name": "   "})
    assert response.status_code == 400
    assert "non-empty" in response.json()["detail"]


def test_a_missing_required_field_is_a_422_over_http(client):
    response = client.post("/api/pipelines", json={})
    assert response.status_code == 422


def test_path_query_and_body_fill_one_model(client):
    """``PUT /api/layout/{node_id}`` carries the id in the URL and the
    position in the body; the model sees one request."""
    response = client.put(
        "/api/layout/var__Wide", json={"x": 1.0, "y": 2.0, "pipeline_id": "main"}
    )
    assert response.status_code == 200
    layout = client.get("/api/layout?pipeline_id=main").json()
    assert layout["positions"]["var__Wide"] == {"x": 1.0, "y": 2.0}


def test_the_report_needs_no_database_over_http(client):
    response = client.post(
        "/api/client-error", json={"where": "tab", "message": "boom"}
    )
    assert response.status_code == 200
    assert response.json()["message"] == "boom"


def test_every_table_is_registered():
    assert set(TABLES) >= {"pipeline", "layout", "scopes", "run", "plot", "project"}
