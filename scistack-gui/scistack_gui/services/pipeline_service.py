"""
Pipeline service — single source of truth for pipeline graph operations.

Orchestrates data fetching, domain logic, and side effects.
Called by both JSON-RPC handlers (server.py) and FastAPI routes (api/pipeline.py).
"""

from __future__ import annotations

import inspect
import logging

logger = logging.getLogger(__name__)


def get_pipeline_graph(db) -> dict:
    """Build the full pipeline graph (nodes + edges).

    Delegates to api/pipeline._build_graph which already orchestrates
    domain modules. This service function provides a stable entry point
    for both protocol adapters.
    """
    from scistack_gui.api.pipeline import _build_graph
    return _build_graph(db)


def get_function_params(fn_name: str) -> list[str]:
    """Return non-private parameter names from the function's signature."""
    from scistack_gui.api.pipeline import _fn_params_from_registry
    from scistack_gui import matlab_registry
    if matlab_registry.is_matlab_function(fn_name):
        info = matlab_registry.get_matlab_function(fn_name)
        return list(info.params)
    return _fn_params_from_registry(fn_name)


def get_function_full_info(fn_name: str) -> dict:
    """Return params, output_names, and language for a function.

    Used when dropping a function node onto the canvas so the node
    is created with the correct number of output handles.
    """
    from scistack_gui.api.pipeline import _fn_params_from_registry
    from scistack_gui import matlab_registry
    if matlab_registry.is_matlab_function(fn_name):
        info = matlab_registry.get_matlab_function(fn_name)
        return {
            "params": list(info.params),
            "output_names": list(info.output_names),
            "language": "matlab",
        }
    return {
        "params": _fn_params_from_registry(fn_name),
        "output_names": [],
        "language": "python",
    }


def get_function_source(fn_name: str) -> dict:
    """Return the source file path and line number for a registered function."""
    from scistack_gui import registry
    from scistack_gui import matlab_registry
    if matlab_registry.is_matlab_function(fn_name):
        info = matlab_registry.get_matlab_function(fn_name)
        return {"ok": True, "file": str(info.file_path), "line": 1}
    fn = registry._functions.get(fn_name)
    if fn is None:
        return {"ok": False, "error": f"Function '{fn_name}' is not registered (pass --module at startup)."}
    try:
        file = inspect.getsourcefile(fn) or inspect.getfile(fn)
        _, line = inspect.getsourcelines(fn)
    except (TypeError, OSError) as e:
        return {"ok": False, "error": f"Could not locate source for '{fn_name}': {e}"}
    return {"ok": True, "file": file, "line": line}


def get_schema(db) -> dict:
    """Return schema keys and distinct values."""
    keys = db.dataset_schema_keys
    values = {key: db.distinct_schema_values(key) for key in keys}
    return {"keys": keys, "values": values}


def get_info() -> dict:
    """Return metadata about the open database."""
    from scistack_gui.db import get_db_path
    from scistack_gui import startup as _startup
    return {
        "db_name": get_db_path().name,
        "startup_errors": [e.to_dict() for e in _startup.get_startup_errors()],
    }


def get_registry() -> dict:
    """Return all registered functions, variables, and MATLAB functions."""
    from scistack_gui import registry
    from scistack_gui import matlab_registry
    from scidb import BaseVariable
    matlab_fns = matlab_registry.get_all_function_names()
    matlab_mismatched = matlab_registry.get_mismatched_function_names()
    logger.info(
        "get_registry: %d python fns, %d matlab fns, %d vars",
        len(registry._functions), len(matlab_fns),
        len(BaseVariable._all_subclasses),
    )
    if matlab_fns:
        logger.info("matlab_functions: %s", matlab_fns)
    if matlab_mismatched:
        logger.info("matlab_functions with name/file mismatch: %s", matlab_mismatched)
    return {
        "functions": sorted(registry._functions.keys()),
        "variables": sorted(BaseVariable._all_subclasses.keys()),
        "matlab_functions": matlab_fns,
        "matlab_functions_mismatched": matlab_mismatched,
    }


def get_variables_list() -> list[dict]:
    """Return all registered variable type names."""
    from scidb import BaseVariable
    return [{"variable_name": name} for name in sorted(BaseVariable._all_subclasses.keys())]


def refresh_module() -> dict:
    """Re-import user module and refresh registries."""
    from scistack_gui import registry
    from scistack_gui import matlab_registry
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
    return {"ok": True, **result}
