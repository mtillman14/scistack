"""
Pipeline service — single source of truth for pipeline graph operations.

Orchestrates data fetching, domain logic, and side effects.
Called by both JSON-RPC handlers (server.py) and FastAPI routes (api/pipeline.py).
"""

from __future__ import annotations

import inspect
import logging
import time

from scidb.model.roles import endpoint_kind

from scistack_gui.domain.display_order import display_sorted
from scistack_gui.ids import ROOT_SCOPE

logger = logging.getLogger(__name__)


def get_pipeline_graph(
    db, pipeline_id: str = ROOT_SCOPE, *, run_states: bool = True
) -> dict:
    """Build the pipeline graph (nodes + edges) for one SCOPE.

    Delegates to graph_service.build_graph which already orchestrates
    domain modules. This service function provides a stable entry point
    for both protocol adapters.

    Failures are logged here before propagating. FastAPI runs the sync
    handler on a threadpool thread, so an uncaught exception surfaces only
    on uvicorn's stderr and never reaches scistack.log — on 2026-08-25 that
    left a graph build that just stopped mid-log with no error at all, and
    the traceback had to be recovered from the terminal by hand.
    """
    from scistack_gui.services.graph_service import build_graph

    try:
        return build_graph(db, pipeline_id, run_states=run_states)
    except Exception:
        logger.exception(
            "[pipeline] graph build FAILED (scope=%s) — see traceback", pipeline_id
        )
        raise


def get_function_params(fn_name: str) -> list[str]:
    """Return non-private parameter names from the function's signature
    (``registry.function_arguments``: Python or MATLAB)."""
    from scistack_gui.services.graph_service import fn_params_from_registry

    return fn_params_from_registry(fn_name)


def get_function_full_info(fn_name: str) -> dict:
    """Return params, output_names, language, and endpoint_kind for a
    function.

    Used when dropping a function node onto the canvas so the node is
    created with the correct number of output handles — and with its
    endpoint classification (plot_/stat_, scidb.model.roles.endpoint_kind), so a
    freshly dragged endpoint shows its badge/Show button BEFORE any run
    exists (the graph post-pass only tags nodes on a refetch). Its ports
    come from ``graph_builder.port_visibility`` with nothing fed yet, the
    rule the canvas build applies, so the node doesn't redraw on refetch.
    """
    from scistack_gui.matlab import registry as matlab_registry
    from scistack_gui.project import registry
    from scistack_gui.domain.graph_builder import port_visibility

    arguments = registry.function_arguments(fn_name)
    params = [a.name for a in arguments or [] if not a.private]
    input_ports, optional_ports = port_visibility(arguments, {}, [], set())
    if matlab_registry.is_matlab_function(fn_name):
        info = matlab_registry.get_matlab_function(fn_name)
        return {
            "params": params,
            "input_ports": input_ports,
            "optional_ports": optional_ports,
            "output_names": list(info.output_names),
            "language": "matlab",
            "endpoint_kind": endpoint_kind(fn_name),
        }
    return {
        "params": params,
        "input_ports": input_ports,
        "optional_ports": optional_ports,
        "output_names": [],
        "language": "python",
        "endpoint_kind": endpoint_kind(fn_name),
    }


def get_function_source(fn_name: str) -> dict:
    """Return the source file path and line number for a registered function."""
    from scistack_gui.matlab import registry as matlab_registry
    from scistack_gui.project import registry

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
    from scistack_gui.matlab import registry as matlab_registry
    from scistack_gui.project import registry

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
    from scistack_gui.project import startup as _startup
    from scistack_gui.runtime.db import get_db_path, is_loaded

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
    from scistack_gui.matlab import registry as matlab_registry
    from scistack_gui.project import registry

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
    all_fns = display_sorted(set(registry._functions) | set(library_fns))
    matlab_fns = display_sorted(matlab_fns)
    # The sidebar's ONE list: Python and MATLAB ordered together, so a
    # MATLAB name isn't stranded after every Python one (display_order.py).
    sidebar_functions = display_sorted({*all_fns, *matlab_fns})
    logger.debug("get_registry: sidebar function order %s", sidebar_functions)
    # Role (process / plot / stat / glue) comes from scidb, never re-derived
    # here or in the frontend: the prefixes already drive execution behaviour
    # inside scidb, so a second copy of the strings would drift (CLAUDE.md
    # NOTE 3 — the sidebar's role DROPDOWN is display and lives in the GUI;
    # what a role IS is scidb's).
    from scidb import function_role

    variables = display_sorted(BaseVariable._all_subclasses.keys())
    return {
        "functions": all_fns,
        "variables": variables,
        "matlab_functions": matlab_fns,
        "sidebar_functions": sidebar_functions,
        "matlab_functions_mismatched": matlab_mismatched,
        "function_roles": {
            name: function_role(name) for name in [*all_fns, *matlab_fns]
        },
        # The palette's "By file" grouping (plan-palette-order-and-delete).
        "function_sources": _function_sources(sidebar_functions, set(library_fns)),
        # Per-variable delete eligibility + "Last saved" sort, in `variables` order.
        "variable_entries": _variable_entries(variables),
        "load_errors": load_errors,
    }


def _function_sources(names: list[str], library_fns: set[str]) -> dict:
    """``{name: {file, line, library}}`` for every palette function -- where
    each is declared, so the palette can group a file's functions together.

    The file/line come from :func:`get_function_source`, the one owner of
    "where is this function's source" (it unwraps library wrappers and knows
    MATLAB). A library reference (``numpy.mean``) or a MATLAB builtin has no
    file of the user's: ``library=True``, ``file=None``, and the palette puts
    it in one "Library" group. A function whose source cannot be located is
    ``file=None, library=False`` (logged), never dropped.
    """
    t0 = time.perf_counter()
    out: dict[str, dict] = {}
    unlocated: list[str] = []
    for name in names:
        if name in library_fns:
            out[name] = {"file": None, "line": None, "library": True}
            continue
        src = get_function_source(name)
        if src.get("ok"):
            out[name] = {"file": str(src["file"]), "line": src.get("line"), "library": False}
        else:
            # A MATLAB builtin reference is a library function, not a fault.
            builtin = "built-in" in (src.get("error") or "")
            out[name] = {"file": None, "line": None, "library": builtin}
            if not builtin:
                unlocated.append(name)
    per_file: dict[str, int] = {}
    for info in out.values():
        key = info["file"] or ("<library>" if info["library"] else "<unknown>")
        per_file[key] = per_file.get(key, 0) + 1
    logger.info(
        "get_registry: function sources: %d function(s) in %d group(s) (%.0f ms); "
        "%d without a locatable source",
        len(out),
        len(per_file),
        (time.perf_counter() - t0) * 1000.0,
        len(unlocated),
    )
    if unlocated:
        logger.warning("get_registry: no source located for: %s", unlocated)
    logger.debug("get_registry: functions per source: %s", per_file)
    return out


def _variable_entries(names: list[str]) -> list[dict]:
    """``[{name, source_file, declared_in_entities_file, last_saved}]`` in
    *names* order.

    * ``declared_in_entities_file`` is the palette's Delete eligibility --
      the same comparison the Parameter rows use
      (``graph_builder.is_declared_in_entities_file``); the write path
      re-checks through ``entity_editability`` regardless.
    * ``last_saved`` is ``Inspector.last_saved_by_variable`` (scidb, the one
      owner), ``None`` for a type never saved or with no database open
      (``get_registry`` is reachable before one exists).
    """
    from scistack_gui.domain.graph_builder import is_declared_in_entities_file
    from scistack_gui.project import registry
    from scistack_gui.runtime.db import get_db, is_loaded

    entities_file = (
        str(registry._config.entities_file)
        if registry._config is not None and registry._config.entities_file is not None
        else None
    )
    last_saved: dict[str, str] = {}
    if is_loaded():
        try:
            last_saved = get_db().inspect.last_saved_by_variable()
        except Exception:
            logger.exception("get_registry: last_saved_by_variable failed; sorting without it")
    entries = []
    for name in names:
        source = registry._variable_sources.get(name)
        entries.append(
            {
                "name": name,
                "source_file": source,
                "declared_in_entities_file": is_declared_in_entities_file(source, entities_file),
                "last_saved": last_saved.get(name),
            }
        )
    logger.info(
        "get_registry: %d variable(s), %d deletable (in %s), %d with a last save",
        len(entries),
        sum(e["declared_in_entities_file"] for e in entries),
        entities_file,
        sum(e["last_saved"] is not None for e in entries),
    )
    return entries


def _python_library_function_names() -> list[str]:
    """Persisted Python library references, or ``[]`` if no DB is open.

    ``get_registry`` is reachable before a database exists (the browser
    frontend calls it while showing the project-creation wizard), so a
    missing DB is a normal state here, not an error.
    """
    from scistack_gui.runtime.db import get_db, is_loaded
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
    """The Refresh Code button: re-import the project's code from disk,
    including files created since startup (registry_reload_service)."""
    from scistack_gui.services.registry_reload_service import refresh_registries

    try:
        result = refresh_registries()
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
        from scistack_gui.runtime.db import get_db
        from scistack_gui.services.builtin_function_service import (
            replay_persisted_builtins,
        )

        replay_persisted_builtins(get_db())
    except Exception:
        logger.exception("Failed to replay persisted builtin functions after refresh")

    # After the builtins are back, so every signature is readable: an edge on
    # an argument the function no longer takes is disconnected (renamed or
    # removed argument; the user redraws it).
    disconnected: list[dict] = []
    try:
        from scistack_gui.runtime.db import get_db
        from scistack_gui.services.layout_service import disconnect_removed_arguments

        disconnected = disconnect_removed_arguments(get_db())
    except Exception:
        logger.exception("Failed to disconnect edges on removed arguments after refresh")

    return {"ok": True, **result, "disconnected_edges": disconnected}
