"""
Pipeline service — single source of truth for pipeline graph operations.

Orchestrates data fetching, domain logic, and side effects.
Called by both JSON-RPC handlers (server.py) and FastAPI routes (api/pipeline.py).
"""

from __future__ import annotations

import inspect
import logging

from scidb.roles import endpoint_kind

from scistack_gui.ids import ROOT_SCOPE

logger = logging.getLogger(__name__)


def get_pipeline_graph(
    db, pipeline_id: str = ROOT_SCOPE, *, run_states: bool = True
) -> dict:
    """Build the pipeline graph (nodes + edges) for one SCOPE.

    Delegates to api/pipeline._build_graph which already orchestrates
    domain modules. This service function provides a stable entry point
    for both protocol adapters.

    Failures are logged here before propagating. FastAPI runs the sync
    handler on a threadpool thread, so an uncaught exception surfaces only
    on uvicorn's stderr and never reaches scidb.log — on 2026-08-25 that
    left a graph build that just stopped mid-log with no error at all, and
    the traceback had to be recovered from the terminal by hand.
    """
    from scistack_gui.api.pipeline import _build_graph

    try:
        return _build_graph(db, pipeline_id, run_states=run_states)
    except Exception:
        logger.exception(
            "[pipeline] graph build FAILED (scope=%s) — see traceback", pipeline_id
        )
        raise


def get_function_params(fn_name: str) -> list[str]:
    """Return non-private parameter names from the function's signature."""
    from scistack_gui import matlab_registry
    from scistack_gui.api.pipeline import _fn_params_from_registry

    if matlab_registry.is_matlab_function(fn_name):
        info = matlab_registry.get_matlab_function(fn_name)
        return list(info.params)
    return _fn_params_from_registry(fn_name)


def get_function_full_info(fn_name: str) -> dict:
    """Return params, output_names, language, and endpoint_kind for a
    function.

    Used when dropping a function node onto the canvas so the node is
    created with the correct number of output handles — and with its
    endpoint classification (plot_/stat_, scidb.roles.endpoint_kind), so a
    freshly dragged endpoint shows its badge/Show button BEFORE any run
    exists (the graph post-pass only tags nodes on a refetch).
    """
    from scistack_gui import matlab_registry
    from scistack_gui.api.pipeline import _fn_params_from_registry

    if matlab_registry.is_matlab_function(fn_name):
        info = matlab_registry.get_matlab_function(fn_name)
        return {
            "params": list(info.params),
            "output_names": list(info.output_names),
            "language": "matlab",
            "endpoint_kind": endpoint_kind(fn_name),
        }
    return {
        "params": _fn_params_from_registry(fn_name),
        "output_names": [],
        "language": "python",
        "endpoint_kind": endpoint_kind(fn_name),
    }


def get_function_source(fn_name: str) -> dict:
    """Return the source file path and line number for a registered function."""
    from scistack_gui import matlab_registry, registry

    if matlab_registry.is_matlab_function(fn_name):
        info = matlab_registry.get_matlab_function(fn_name)
        if info.file_path is None:
            return {
                "ok": False,
                "error": f"'{fn_name}' is a manually-added built-in MATLAB reference "
                "— there is no source file to show.",
            }
        return {"ok": True, "file": str(info.file_path), "line": 1}
    fn = registry.lookup_function(fn_name)
    if fn is None:
        return {
            "ok": False,
            "error": f"Function '{fn_name}' is not registered (pass --module at startup).",
        }
    try:
        # Unwrap FIRST, and unwrap for both halves. inspect is inconsistent
        # here: getsourcelines() calls unwrap() internally, but getsourcefile()
        # reads __code__.co_filename and does not. On a wrapped callable —
        # a library reference carries a name-qualifying wrapper, see
        # library_functions.with_qualified_name — that mismatch returns the
        # WRAPPER's file paired with the WRAPPED function's line number, i.e.
        # an arbitrary line of library_functions.py.
        fn = inspect.unwrap(fn)
        file = inspect.getsourcefile(fn) or inspect.getfile(fn)
        _, line = inspect.getsourcelines(fn)
    except (TypeError, OSError) as e:
        return {"ok": False, "error": f"Could not locate source for '{fn_name}': {e}"}
    return {"ok": True, "file": file, "line": line}


def get_function_doc(fn_name: str) -> dict:
    """Return a display-ready signature string and docstring for a
    registered function (Python or MATLAB) — used by the sidebar palette's
    item-info panel. Mirrors ``get_function_source``'s not-registered error
    shape.
    """
    from scistack_gui import matlab_registry, registry

    if matlab_registry.is_matlab_function(fn_name):
        info = matlab_registry.get_matlab_function(fn_name)
        outputs = ", ".join(info.output_names) if info.output_names else None
        params = ", ".join(info.params)
        signature = (
            f"[{outputs}] = {fn_name}({params})"
            if outputs and len(info.output_names) > 1
            else f"{outputs} = {fn_name}({params})"
            if outputs
            else f"{fn_name}({params})"
        )
        return {
            "ok": True,
            "language": "matlab",
            "signature": signature,
            "docstring": info.docstring,
        }
    fn = registry.lookup_function(fn_name)
    if fn is None:
        return {
            "ok": False,
            "error": f"Function '{fn_name}' is not registered (pass --module at startup).",
        }
    try:
        signature = f"{fn_name}{inspect.signature(fn)}"
    except (TypeError, ValueError) as e:
        return {"ok": False, "error": f"Could not read signature for '{fn_name}': {e}"}
    return {
        "ok": True,
        "language": "python",
        "signature": signature,
        "docstring": inspect.getdoc(fn),
    }


def get_schema(db) -> dict:
    """Return schema keys and distinct values, in the DECLARED level order.

    ``[schema_keys]`` in the project config says a key's levels are
    chronological rather than alphabetical (``session = ["BL","POST","FU"]``).
    ``DatabaseManager.distinct_schema_values`` applies it (scidb owns the
    declaration, CLAUDE.md NOTE 3), so every list the GUI draws from this call
    agrees with the figures, the tables and for_each's iteration order.
    """
    keys = db.dataset_schema_keys
    values = {key: db.distinct_schema_values(key) for key in keys}
    return {"keys": keys, "values": values}


def get_info() -> dict:
    """Return metadata about the open database.

    Returns ``{"db_loaded": False}`` if no database has been opened or
    created yet — the browser frontend falls back to the project-creation
    wizard in that case instead of the normal DAG shell (VS Code always
    opens/creates a database before its webview mounts, so this branch is
    standalone-frontend-only).
    """
    from scistack_gui import startup as _startup
    from scistack_gui.db import get_db_path, is_loaded

    if not is_loaded():
        return {"db_loaded": False}

    return {
        "db_loaded": True,
        "db_name": get_db_path().name,
        "startup_errors": [e.to_dict() for e in _startup.get_startup_errors()],
    }


def get_registry() -> dict:
    """Return all registered functions, variables, and MATLAB functions.

    Python library references (``pandas.read_csv``) are NOT in
    ``registry._functions`` — they're imported on demand — so the persisted
    list is unioned in here. That table is their only record.
    """
    from scidb import BaseVariable
    from scistack_gui import matlab_registry, registry

    library_fns = _python_library_function_names()
    matlab_fns = matlab_registry.get_all_function_names()
    matlab_mismatched = matlab_registry.get_mismatched_function_names()
    load_errors = registry.all_load_errors()
    logger.info(
        "get_registry: %d python fns (+%d library refs), %d matlab fns, "
        "%d vars, %d load errors",
        len(registry._functions),
        len(library_fns),
        len(matlab_fns),
        len(BaseVariable._all_subclasses),
        len(load_errors),
    )
    if library_fns:
        logger.info("library_functions: %s", library_fns)
    if matlab_fns:
        logger.info("matlab_functions: %s", matlab_fns)
    if matlab_mismatched:
        logger.info("matlab_functions with name/file mismatch: %s", matlab_mismatched)
    if load_errors:
        logger.warning("get_registry: %d discovery load error(s): %s", len(load_errors), load_errors)
    all_fns = sorted(set(registry._functions) | set(library_fns))
    # Role (process / plot / stat / glue) comes from scidb, never re-derived
    # here or in the frontend: the prefixes already drive execution behaviour
    # inside scidb, so a second copy of the strings would drift (CLAUDE.md
    # NOTE 3 — the sidebar's role DROPDOWN is display and lives in the GUI;
    # what a role IS is scidb's).
    from scidb import function_role

    return {
        "functions": all_fns,
        "variables": sorted(BaseVariable._all_subclasses.keys()),
        "matlab_functions": matlab_fns,
        "matlab_functions_mismatched": matlab_mismatched,
        "function_roles": {
            name: function_role(name) for name in [*all_fns, *matlab_fns]
        },
        "load_errors": load_errors,
    }


def _python_library_function_names() -> list[str]:
    """Persisted Python library references, or ``[]`` if no DB is open.

    ``get_registry`` is reachable before a database exists (the browser
    frontend calls it while showing the project-creation wizard), so a
    missing DB is a normal state here, not an error.
    """
    from scistack_gui.db import get_db, is_loaded
    from scistack_gui.services.builtin_function_service import (
        get_python_library_function_names,
    )

    if not is_loaded():
        return []
    return get_python_library_function_names(get_db())


def get_variables_list() -> list[dict]:
    """Return all registered variable type names."""
    from scidb import BaseVariable

    return [
        {"variable_name": name} for name in sorted(BaseVariable._all_subclasses.keys())
    ]


def refresh_module() -> dict:
    """Re-import user module and refresh registries."""
    from scistack_gui import matlab_registry, registry

    try:
        if registry._config is not None:
            result = registry.refresh_all()
        else:
            result = registry.refresh_module()
        matlab_registry.refresh_all()
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        logger.exception("Failed to refresh module")
        return {"ok": False, "error": f"Import error: {e}"}

    # Manually-declared builtin function references (numpy.mean, a MATLAB
    # builtin, ...) aren't rediscovered by the refresh above — it has no
    # file on disk to find them from — so they'd otherwise be silently
    # dropped by the registry .clear() every refresh does internally.
    try:
        from scistack_gui.db import get_db
        from scistack_gui.services.builtin_function_service import (
            replay_persisted_builtins,
        )

        replay_persisted_builtins(get_db())
    except Exception:
        logger.exception("Failed to replay persisted builtin functions after refresh")

    return {"ok": True, **result}
