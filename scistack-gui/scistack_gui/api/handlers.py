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
the frontend's route map is "Unknown method" in the browser only.

A :class:`Handler` is the ONE declaration. From it:

* :func:`rpc_methods` builds the ``name -> callable(params)`` entries for
  ``server.METHODS`` — the raw params are validated through the same
  pydantic model the HTTP body uses, so both transports reject the same
  requests;
* :func:`self_managed` lists the names whose ``holds_db_lock`` is False;
* :func:`install_routes` adds one FastAPI route per handler, with the
  exception → status mapping the route used to spell inline.

``tests/test_api_handlers.py`` walks the table and asserts every entry is
reachable through both transports AND named in the frontend's route map
with the same path. The plot family moved first (newest, smallest); each
remaining ``_h_*`` / ``@router`` pair moves the same way.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import APIRouter
    from pydantic import BaseModel


@dataclass(frozen=True)
class Handler:
    """One GUI operation, declared once for both transports.

    ``call`` receives the open :class:`~scidb.database.DatabaseManager`
    (when ``needs_db``) and the validated request model (when ``params`` is
    not None), in that order, and returns a JSON-serialisable result.
    """

    #: The JSON-RPC method name — also the key in the frontend's route map.
    name: str
    #: The HTTP path under ``/api`` (``"/plot/describe"``).
    path: str
    #: The request body / RPC params model, or None for a bodiless method.
    params: type[BaseModel] | None
    call: Callable[..., Any]
    #: False = the service takes the connection itself, for exactly as long
    #: as it needs it, and the JSON-RPC dispatch must NOT hold the file lock
    #: across the whole call (the 2026-09-11 rule; see ``server.py``).
    holds_db_lock: bool = True
    #: False = the call never sees the database (a log line, say) and must
    #: work while MATLAB holds the file.
    needs_db: bool = True
    #: Exception type → HTTP status, for the browser transport. The RPC
    #: transport reports every exception through the dispatch loop's one
    #: error frame, message intact, so nothing is mapped there.
    http_errors: Mapping[type[BaseException], int] = field(default_factory=dict)
    http_method: str = "POST"

    def invoke(self, params: Mapping[str, Any] | None, db: Any) -> Any:
        """Run the handler on raw ``params`` — the JSON-RPC entry point."""
        args: list[Any] = []
        if self.needs_db:
            args.append(db)
        if self.params is not None:
            args.append(self.params.model_validate(dict(params or {})))
        return self.call(*args)


def rpc_methods(handlers: Iterable[Handler]) -> dict[str, Callable[[dict], Any]]:
    """``{name: callable(params)}`` for ``server.METHODS``."""

    def _entry(h: Handler) -> Callable[[dict], Any]:
        def rpc(params: dict) -> Any:
            db = None
            if h.needs_db:
                from scistack_gui.db import get_db

                db = get_db()
            return h.invoke(params, db)

        rpc.__name__ = f"rpc_{h.name}"
        rpc.__doc__ = h.call.__doc__
        rpc.handler = h  # type: ignore[attr-defined]
        return rpc

    return {h.name: _entry(h) for h in handlers}


def self_managed(handlers: Iterable[Handler]) -> frozenset[str]:
    """The names that take the DuckDB connection themselves."""
    return frozenset(h.name for h in handlers if not h.holds_db_lock)


def install_routes(router: APIRouter, handlers: Iterable[Handler]) -> None:
    """Add one route per handler to *router* (mounted under ``/api``)."""
    from fastapi import Depends, HTTPException

    from scistack_gui.db import get_db

    def _endpoint(h: Handler) -> Callable[..., Any]:
        def _run(db: Any, req: Any) -> Any:
            args: list[Any] = []
            if h.needs_db:
                args.append(db)
            if h.params is not None:
                args.append(req)
            try:
                return h.call(*args)
            except BaseException as exc:
                for exc_type, status in h.http_errors.items():
                    if isinstance(exc, exc_type):
                        raise HTTPException(status_code=status, detail=str(exc))
                raise

        # FastAPI reads the signature: a body parameter annotated with the
        # model, and the db dependency only when the handler needs it (a
        # handler that must work without a database must not depend on one).
        if h.params is not None and h.needs_db:

            def endpoint(req, db=Depends(get_db)):
                return _run(db, req)

            endpoint.__annotations__ = {"req": h.params, "return": dict}
        elif h.params is not None:

            def endpoint(req):
                return _run(None, req)

            endpoint.__annotations__ = {"req": h.params, "return": dict}
        elif h.needs_db:

            def endpoint(db=Depends(get_db)):
                return _run(db, None)

            endpoint.__annotations__ = {"return": dict}
        else:

            def endpoint():
                return _run(None, None)

            endpoint.__annotations__ = {"return": dict}

        endpoint.__name__ = h.name
        endpoint.__doc__ = h.call.__doc__
        return endpoint

    for h in handlers:
        router.add_api_route(
            h.path, _endpoint(h), methods=[h.http_method], name=h.name
        )
