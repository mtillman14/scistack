"""Every handler table, in one tuple — what ``server.py`` builds its
JSON-RPC dispatch from and what ``tests/test_api_handlers.py`` walks.

One family per module, each declaring its rows beside the request models
and the calls they make (``api/handlers.py`` for the ``Handler`` type).
``app.py`` mounts each module's ``router`` (built by ``install_routes``
from the same rows) under ``/api``. Two HTTP-only routes stay outside the
tables because they have no JSON-RPC shape: ``/api/artifacts/file`` (a
file response) and the ``/api/bootstrap/*`` project wizard (the extension
opens projects through the host).
"""

from __future__ import annotations

from scistack_gui.api.artifacts import ARTIFACT_HANDLERS
from scistack_gui.api.builtin_functions import BUILTIN_FUNCTION_HANDLERS
from scistack_gui.api.glue import GLUE_HANDLERS
from scistack_gui.api.handlers import Handler
from scistack_gui.api.layout import LAYOUT_HANDLERS
from scistack_gui.api.pipeline import PIPELINE_HANDLERS
from scistack_gui.api.plot import PLOT_HANDLERS
from scistack_gui.api.project import PROJECT_HANDLERS
from scistack_gui.api.provenance import PROVENANCE_HANDLERS
from scistack_gui.api.registry import REGISTRY_HANDLERS
from scistack_gui.api.run import RUN_HANDLERS
from scistack_gui.api.schema import SCHEMA_HANDLERS
from scistack_gui.api.scopes import SCOPE_HANDLERS
from scistack_gui.api.variables import VARIABLE_HANDLERS

TABLES: dict[str, tuple[Handler, ...]] = {
    "pipeline": PIPELINE_HANDLERS,
    "layout": LAYOUT_HANDLERS,
    "schema": SCHEMA_HANDLERS,
    "registry": REGISTRY_HANDLERS,
    "variables": VARIABLE_HANDLERS,
    "builtin_functions": BUILTIN_FUNCTION_HANDLERS,
    "glue": GLUE_HANDLERS,
    "project": PROJECT_HANDLERS,
    "scopes": SCOPE_HANDLERS,
    "run": RUN_HANDLERS,
    "artifacts": ARTIFACT_HANDLERS,
    "provenance": PROVENANCE_HANDLERS,
    "plot": PLOT_HANDLERS,
}

ALL_HANDLERS: tuple[Handler, ...] = tuple(h for table in TABLES.values() for h in table)

_names = [h.name for h in ALL_HANDLERS]
_dupes = sorted({n for n in _names if _names.count(n) > 1})
if _dupes:  # a method declared in two tables would silently shadow itself
    raise RuntimeError(f"handler names declared twice: {_dupes}")
