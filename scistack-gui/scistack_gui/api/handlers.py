"""One handler table, two transports.

The GUI is reachable two ways — JSON-RPC over stdin/stdout from the VS Code
extension (``server.py``) and HTTP from the standalone browser build
(``app.py``, FastAPI). Both call the same service functions, and until
2026-09-20 they said so in a docstring: every handler was written twice, an
``_h_<name>(params)`` function reading a raw dict and a ``@router.post``
route reading a pydantic model, with a third list (``SELF_MANAGED_DB_METHODS``)
naming which of them take the DuckDB connection themselves. Three places to
add one method, and two of them silent when forgotten: a method missing
from the RPC table is "Method not found" in the extension only; a method
missing from the self-managed list quietly holds the file lock against
MATLAB for the whole request (31 s once, 2026-09-11); a method missing from
the frontend's route map is "Unknown method" in the browser only. And the
two copies drifted in behaviour too: the HTTP glue routes broadcast
``dag_updated`` and the RPC ones did not; the HTTP run route refused a glue
node and passed the clicked node id, the RPC one did neither.

A :class:`Handler` is the ONE declaration. From it:

* :func:`rpc_methods` builds the ``name -> callable(params)`` entries for
  ``server.METHODS`` — the raw params are validated through the same
  pydantic model the HTTP request uses, so both transports reject the same
  requests, and BEFORE the database is touched;
* :func:`self_managed` lists the names whose ``holds_db_lock`` is False;
* :func:`install_routes` adds one FastAPI route per handler. The request
  model is filled from the URL's path parameters, the query string and the
  JSON body together (path wins), so one model describes the method on
  both transports whatever the REST spelling puts in the path.

``tests/test_api_handlers.py`` walks every table and asserts each row is
reachable through both transports AND named in the frontend's route map
with the same path, method and body-ness. ``ALL_HANDLERS`` in
``api/tables.py`` is the union the server and the test read.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import APIRouter
    from pydantic import BaseModel

logger = logging.getLogger(__name__)

#: ``{name}`` / ``{name:path}`` placeholders in an HTTP path.
_PATH_PARAM = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)(?::[a-z]+)?\}")


@dataclass(frozen=True)
class Handler:
    """One GUI operation, declared once for both transports.

    ``call`` receives the open :class:`~scidb.database.DatabaseManager`
    (when ``needs_db``) and the validated request model (when ``params`` is
    not None), in that order — plus ``transport="rpc" | "http"`` as a
    keyword when ``wants_transport`` — and returns a JSON-serialisable
    result.
    """

    #: The JSON-RPC method name — also the key in the frontend's route map.
    name: str
    #: The HTTP path under ``/api`` (``"/layout/{node_id}/config"``), or
    #: None for a method the browser build cannot offer at all (the MATLAB
    #: terminal dispatch needs the VS Code host).
    path: str | None
    #: The request model — RPC params, and on HTTP the union of path
    #: parameters, query string and JSON body. None for a bodiless method.
    params: type[BaseModel] | None
    call: Callable[..., Any]
    #: False = the service takes the connection itself, for exactly as long
    #: as it needs it, and the JSON-RPC dispatch must NOT hold the file lock
    #: across the whole call (the 2026-09-11 rule; see ``server.py``).
    holds_db_lock: bool = True
    #: False = the call never sees the database (a log line, say) and must
    #: work while MATLAB holds the file.
    needs_db: bool = True
    #: True = the call takes the database when there is one and works
    #: without it when there is not, deciding for itself from its own
    #: request (every plot method: a ``csv_path`` reads a file through
    #: ``scistackplot``'s ``CsvSource`` and never touches DuckDB, which is
    #: why they are already written as
    #: ``db_connection(..., needed=not csv_path)``).
    #:
    #: This is what a **plot-only** server is made of: with no ``--db`` there
    #: is no connection to hand over, so ``get_db()`` would raise before the
    #: handler ever saw the request that says it did not need one. These
    #: methods get ``None`` instead, and ``server.py`` refuses every other
    #: method in that mode by name rather than letting it fail deeper down.
    db_optional: bool = False
    #: Exception type → HTTP status, for the browser transport. The RPC
    #: transport reports every exception through the dispatch loop's one
    #: error frame, message intact, so nothing is mapped there.
    http_errors: Mapping[type[BaseException], int] = field(default_factory=dict)
    http_method: str = "POST"
    #: Whether the browser sends the params as a JSON body. Must agree with
    #: the frontend's route map (the test checks). None = inferred: a
    #: non-GET method whose model has fields outside the path.
    body: bool | None = None
    #: Push ``dag_updated`` to every client after a successful call (a
    #: result that is not ``{"ok": False, ...}``) — through ``ws.push_message``,
    #: which is the WebSocket in the browser and the stdout notification in
    #: the extension, so both transports refresh alike.
    notify_dag_updated: bool = False
    #: Pass ``transport=`` to ``call``: the one legitimate difference between
    #: the transports is what the HOST can do (the extension can dispatch a
    #: MATLAB script to the MathWorks terminal; the browser cannot).
    wants_transport: bool = False

    @property
    def path_params(self) -> tuple[str, ...]:
        return tuple(_PATH_PARAM.findall(self.path or ""))

    @property
    def http_body(self) -> bool:
        if self.body is not None:
            return self.body
        if self.params is None or self.http_method == "GET":
            return False
        return bool(set(self.params.model_fields) - set(self.path_params))

    def parse(self, params: Mapping[str, Any] | None) -> Any:
        """Raw params as the request model (``None`` for a bodiless method).
        Validation comes BEFORE the database is touched, so a bad request is
        refused as such — not as "database not initialised" or "database
        locked" when it never needed either."""
        if self.params is None:
            return None
        return self.params.model_validate(dict(params or {}))

    def invoke(self, req: Any, db: Any, *, transport: str) -> Any:
        """Run the handler on a parsed request — shared by both transports."""
        args: list[Any] = []
        if self.needs_db:
            args.append(db)
        if self.params is not None:
            args.append(req)
        kwargs = {"transport": transport} if self.wants_transport else {}
        result = self.call(*args, **kwargs)
        if self.notify_dag_updated and _succeeded(result):
            from scistack_gui.api.ws import push_message

            push_message({"type": "dag_updated"})
        return result


def _succeeded(result: Any) -> bool:
    return not (isinstance(result, dict) and result.get("ok") is False)


def rpc_methods(handlers: Iterable[Handler]) -> dict[str, Callable[[dict], Any]]:
    """``{name: callable(params)}`` for ``server.METHODS``."""

    def _entry(h: Handler) -> Callable[[dict], Any]:
        def rpc(params: dict) -> Any:
            req = h.parse(params)
            db = None
            if h.needs_db:
                from scistack_gui.db import get_db, is_loaded

                # A db_optional method in a plot-only server: there is no
                # connection to give it, and it is written to work without
                # one. Anything else still raises "Database not initialised",
                # which is the truthful answer.
                if h.db_optional and not is_loaded():
                    db = None
                else:
                    db = get_db()
            return h.invoke(req, db, transport="rpc")

        rpc.__name__ = f"rpc_{h.name}"
        rpc.__doc__ = h.call.__doc__
        rpc.handler = h  # type: ignore[attr-defined]
        return rpc

    return {h.name: _entry(h) for h in handlers}


def self_managed(handlers: Iterable[Handler]) -> frozenset[str]:
    """The names that take the DuckDB connection themselves."""
    return frozenset(h.name for h in handlers if not h.holds_db_lock)


def without_database(handlers: Iterable[Handler]) -> frozenset[str]:
    """The names a server with no ``--db`` can still serve.

    Both kinds: the methods that never wanted a database (``needs_db=False``)
    and the ones that work without it when the request says so
    (``db_optional``). ``server.py`` answers everything else in plot-only
    mode with one clear refusal instead of a failure from three layers down.
    """
    return frozenset(
        h.name for h in handlers if not h.needs_db or h.db_optional
    )


def install_routes(router: APIRouter, handlers: Iterable[Handler]) -> None:
    """Add one route per handler with an HTTP path to *router* (mounted
    under ``/api``).

    The endpoint reads the raw request itself — path parameters, query
    string and JSON body merged, path winning — and validates the union
    through the handler's model, so the REST spelling (``PUT
    /layout/{node_id}/config`` with ``{"config": ...}`` as the body) and the
    RPC spelling (``{"node_id": ..., "config": ...}``) are one model. A
    validation failure is a 422 with pydantic's errors, as FastAPI's own
    body validation would report; a mapped service exception is its status
    with the message as ``detail``. The service runs on the threadpool, as
    a sync FastAPI route would.
    """
    from fastapi import HTTPException, Request
    from pydantic import ValidationError
    from starlette.concurrency import run_in_threadpool

    def _endpoint(h: Handler) -> Callable[..., Any]:
        async def endpoint(request: Request):
            data: dict[str, Any] = dict(request.query_params)
            if h.http_body:
                raw = await request.body()
                if raw:
                    try:
                        parsed = json.loads(raw)
                    except ValueError:
                        raise HTTPException(status_code=400, detail="body must be JSON")
                    if not isinstance(parsed, dict):
                        raise HTTPException(
                            status_code=400, detail="body must be a JSON object"
                        )
                    data.update(parsed)
            data.update(request.path_params)
            try:
                req = h.parse(data)
            except ValidationError as exc:
                raise HTTPException(status_code=422, detail=json.loads(exc.json()))
            db = None
            if h.needs_db:
                from scistack_gui.db import get_db, is_loaded

                # Same rule as the RPC transport above — the two must not
                # differ on when a handler gets a connection.
                db = None if (h.db_optional and not is_loaded()) else get_db()

            def _run() -> Any:
                try:
                    return h.invoke(req, db, transport="http")
                except BaseException as exc:
                    for exc_type, status in h.http_errors.items():
                        if isinstance(exc, exc_type):
                            raise HTTPException(status_code=status, detail=str(exc))
                    raise

            return await run_in_threadpool(_run)

        # A real class, not the string this module's `from __future__ import
        # annotations` would leave: FastAPI resolves annotations against the
        # endpoint's globals, where `Request` (imported above, locally) is not
        # — and an unresolved "Request" became a required QUERY parameter named
        # `request`, so every route answered 422 (first run, 2026-09-21).
        endpoint.__annotations__ = {"request": Request}
        endpoint.__name__ = h.name
        endpoint.__doc__ = h.call.__doc__
        return endpoint

    for h in handlers:
        if h.path is None:
            continue
        router.add_api_route(
            h.path, _endpoint(h), methods=[h.http_method], name=h.name
        )
