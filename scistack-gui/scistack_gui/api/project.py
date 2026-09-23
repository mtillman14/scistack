"""
Project config panel — the scan logic, and the handler table for both
transports (``api/handlers.py``; ``PROJECT_HANDLERS`` at the bottom).

    GET    /api/project/code           get_project_code   — scanned exports from src/{project}/
    GET    /api/project/paths          get_project_paths  — resolved [tool.scistack] paths (Paths popup)
    POST   /api/project/paths          add_project_path   — add a discovery path (loose-script projects)
    DELETE /api/project/paths          remove_project_path
    POST   /api/project/entities-file  set_entities_file
    DELETE /api/project/entities-file  clear_entities_file
    POST   /api/project/refresh        refresh_project    — re-run both scans

Every mutation notifies ``dag_updated`` on success (the row says so), on
both transports alike.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.db import get_db_path

logger = logging.getLogger(__name__)

router = APIRouter(tags=["project"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _project_root() -> Path:
    """The loaded project's root (``registry.get_project_root``, scifor's
    pinned root), or -- before any project has loaded -- what
    ``config.resolve_project_root`` would decide.

    It used to walk up from the DATABASE file to the nearest
    ``pyproject.toml``: a fifth rule for "which folder is the project", and
    the one ``resolve_project_root`` explicitly rejected (a database on
    another drive never reaches its project).
    """
    from scistack_gui import registry
    from scistack_gui.config import resolve_project_root

    root = registry.get_project_root()
    if root is not None:
        return root
    root = resolve_project_root(None, get_db_path())
    logger.info("[project] No project loaded yet; using the resolved root %s", root)
    return root


def _serialise_module_exports(mod) -> dict:
    return {
        "module_name": mod.module_name,
        "variables": [cls.__name__ for cls in mod.variables],
        "functions": [
            # @scistack functions are PLAIN callables (name on the function
            # itself); the .fcn fallback covers any legacy wrapper object.
            getattr(f, "__name__", None)
            or getattr(getattr(f, "fcn", None), "__name__", str(f))
            for f in mod.functions
        ],
        "parameters": [
            {
                "name": name,
                # Every declared value: a Parameter may hold one or many,
                # and showing only the first would misreport a fan-out.
                "value": repr(p.values[0]) if len(p.values) == 1 else repr(p.values),
                "values": [repr(v) for v in p.values],
                "description": p.description,
                "source_file": p.source_file,
                "source_line": p.source_line,
            }
            for name, p in mod.parameters
        ],
        "variable_count": len(mod.variables),
        "function_count": len(mod.functions),
        "parameter_count": len(mod.parameters),
    }


def _serialise_module_error(err) -> dict:
    return {
        "module_name": err.module_name,
        "traceback": err.traceback,
    }


def _serialise_package_result(pkg) -> dict:
    return {
        "name": pkg.name,
        "modules": [_serialise_module_exports(m) for m in pkg.modules],
        "errors": [_serialise_module_error(e) for e in pkg.errors],
        "variable_count": pkg.variable_count,
        "function_count": pkg.function_count,
        "parameter_count": pkg.parameter_count,
        "is_empty": pkg.is_empty,
    }


# Cache the last scan result so GET calls are fast after a refresh.
_last_result = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
def get_project_code() -> dict:
    """Return scanned exports from ``src/{project}/``."""
    global _last_result
    if _last_result is None:
        _run_scan()
    return _serialise_package_result(_last_result.project_code)


def get_project_paths() -> dict:
    """Return the resolved [tool.scistack] paths for the header's Paths popup.

    Reads the same pyproject.toml/scistack.toml that project mode loaded at
    startup (re-parsed here since the GUI doesn't keep the SciStackConfig
    around after registry load). Single-file mode has no such config, so
    that case is reported as ``configured: False`` rather than an error.

    Also reports ``packaged`` (pyproject.toml present -- read-only in the
    Paths popup) and ``managed_paths`` (the raw, pre-discovery ``modules``
    entries as written to scistack.toml -- what the popup's editable add/
    remove list actually displays; empty until the first path is added via
    :func:`add_project_path`, even in folder-scan mode).
    """
    from scistack_gui.config import describe_managed_paths, load_config

    db_path = get_db_path()
    managed_info = describe_managed_paths(db_path)

    try:
        config = load_config(None, db_path)
    except FileNotFoundError:
        logger.info(
            "[project.paths] no [tool.scistack] config found near %s "
            "(single-file mode)",
            db_path,
        )
        # project_root comes from managed_info — the resolved project root,
        # NOT db_path.parent. The database is routinely nowhere near the
        # project (see config.resolve_project_root).
        return {
            "configured": False,
            **managed_info,
        }

    logger.info(
        "[project.paths] resolved config at %s: %d modules, %d packages, "
        "%d matlab functions, %d matlab variables, %d matlab sources",
        config.project_root,
        len(config.modules),
        len(config.packages),
        len(config.matlab_functions),
        len(config.matlab_variables),
        len(config.matlab_sources),
    )
    return {
        "configured": True,
        "modules": [str(p) for p in config.modules],
        "entities_file": str(config.entities_file) if config.entities_file else None,
        # Read-only legacy declaration files, surfaced so the Paths popup can
        # show where entities it refuses to edit are coming from.
        "variable_file": str(config.variable_file) if config.variable_file else None,
        "matlab_entities_file": (
            str(config.matlab_entities_file) if config.matlab_entities_file else None
        ),
        "packages": config.packages,
        "auto_discover": config.auto_discover,
        "matlab_functions": [str(p) for p in config.matlab_functions],
        "matlab_variables": [str(p) for p in config.matlab_variables],
        "matlab_addpath": [str(p) for p in config.matlab_addpath],
        "matlab_variable_dir": (
            str(config.matlab_variable_dir) if config.matlab_variable_dir else None
        ),
        **managed_info,
    }


def _reload_config_and_rescan() -> None:
    """Re-read scistack.toml from disk and reload both registries against
    the fresh config, then rebuild the cached scan result.

    Deliberately NOT the same as ``_run_scan(force_refresh=True)`` /
    ``_refresh_registries()`` -- those replay ``registry.refresh_all()`` /
    ``matlab_registry.refresh_all()`` against the *stale* in-memory
    ``_config`` object captured at the last load (see registry.py:
    ``refresh_all`` calls ``load_from_config(_config)``, it never re-parses
    the TOML file). That's fine for the existing "Refresh" button, which
    only needs to pick up *content* changes in already-configured files.
    But after :func:`~scistack_gui.config.add_path`/``remove_path``/
    ``set_entities_file`` change *which paths are configured*, reusing the
    stale config would silently fail to discover the new path until the
    server restarts. So this re-runs ``load_config`` fresh first (see
    ``services.registry_reload_service``, shared with the auto-create
    fallback in ``services.target_file_service``).
    """
    from scistack_gui.services.registry_reload_service import (
        reload_registries_from_disk,
    )

    reload_registries_from_disk(get_db_path())
    _run_scan(force_refresh=False)


def add_project_path(path_str: str) -> dict:
    """Add a directory to scistack.toml and re-scan (loose-script projects
    only)."""
    from scistack_gui.config import add_path

    try:
        add_path(get_db_path(), Path(path_str))
    except (ValueError, FileNotFoundError, NotADirectoryError) as e:
        logger.warning("[project.paths] add_project_path failed: %s", e)
        return {"ok": False, "error": str(e)}

    _reload_config_and_rescan()
    result = get_project_paths()
    result["ok"] = True
    return result


def remove_project_path(path: str) -> dict:
    """Remove a directory from scistack.toml and re-scan (loose-script
    projects only)."""
    from scistack_gui.config import remove_path

    try:
        remove_path(get_db_path(), Path(path))
    except (ValueError, FileNotFoundError) as e:
        logger.warning("[project.paths] remove_project_path failed: %s", e)
        return {"ok": False, "error": str(e)}

    _reload_config_and_rescan()
    result = get_project_paths()
    result["ok"] = True
    return result


def set_project_entities_file(path_str: "str | None") -> dict:
    """Set the TOML file new Variable/Parameter/PathInput declarations are
    written to (loose-script projects only). An absolute path (or a
    relative one, resolved against the project root), or ``None`` to
    auto-create the default ``src/scistack_entities.toml`` in the project
    root.
    """
    from scistack_gui.config import set_entities_file

    path_str = path_str or None
    try:
        set_entities_file(get_db_path(), Path(path_str) if path_str else None)
    except (ValueError, OSError) as e:
        logger.warning("[project.paths] set_project_entities_file failed: %s", e)
        return {"ok": False, "error": str(e)}

    _reload_config_and_rescan()
    result = get_project_paths()
    result["ok"] = True
    return result


def clear_project_entities_file() -> dict:
    """Clear the configured entities_file (loose-script projects only).
    Never deletes the file itself -- see ``config.clear_entities_file``."""
    from scistack_gui.config import clear_entities_file

    try:
        clear_entities_file(get_db_path())
    except (ValueError, FileNotFoundError) as e:
        logger.warning("[project.paths] clear_project_entities_file failed: %s", e)
        return {"ok": False, "error": str(e)}

    _reload_config_and_rescan()
    result = get_project_paths()
    result["ok"] = True
    return result


def refresh_project_sync() -> dict:
    """Re-run the discovery scan and return a summary.

    In registry-backed mode (no pyproject.toml — loose-script/folder-scan
    projects) this also re-imports the configured files from disk first,
    so "Refresh" here does real work instead of just re-reporting stale
    in-memory state — see ``_run_scan(force_refresh=True)``.

    Transport-agnostic and deliberately synchronous: the handler row
    (``notify_dag_updated=True``) does the notification through
    ``ws.push_message`` for both transports, and the RPC path runs in a
    plain thread with no event loop, so this must never be a coroutine.
    """
    _run_scan(force_refresh=True)
    return {
        "ok": True,
        "project_code": _serialise_package_result(_last_result.project_code),
        "libraries_shown": len(_last_result.non_empty_libraries()),
        "libraries_total": len(_last_result.libraries),
    }



def _run_scan(*, force_refresh: bool = False) -> None:
    """Run the discovery scanner and cache the result.

    Built entirely from ``registry``/``matlab_registry`` state (see
    ``_build_registry_backed_result``) for **both** packaged and
    loose-script/folder-scan projects — this is the exact same registry
    ``execution_service.py`` reads at run time, so the "Discovered Code"
    panel can no longer show a function that isn't actually resolvable.
    A packaged project's own ``src/{name}/`` code is auto-folded into
    ``config.packages`` by ``scistack_gui/config.py``'s ``load_config``,
    so it flows through ``registry.load_from_config`` -> ``_load_packages``
    like any other configured package — previously this branch called
    ``scidb.discover.scan_project`` directly, which never touched the
    registry, so a function shown here could raise ``KeyError`` at actual
    run time. See docs/claude/code-discovery-categories.md.

    Note: packaged projects no longer show a separate uv.lock-derived
    "libraries" section here (``scan_project``'s library-scanning half is
    unused by the GUI now, kept only as a standalone ``scidb.discover``
    API feature) — ``libraries`` is always empty in the returned result.
    """
    global _last_result
    root = _project_root()
    logger.info("Running discovery scan on %s (force_refresh=%s)", root, force_refresh)

    if force_refresh:
        _refresh_registries()
    _last_result = _build_registry_backed_result(root)

    logger.info(
        "Scan complete: project=%s (vars=%d, fns=%d, params=%d), "
        "libraries=%d (shown=%d)",
        _last_result.project_code.name,
        _last_result.project_code.variable_count,
        _last_result.project_code.function_count,
        _last_result.project_code.parameter_count,
        len(_last_result.libraries),
        len(_last_result.non_empty_libraries()),
    )


def _refresh_registries() -> None:
    """Re-import configured files from disk (registry-backed mode only)."""
    from scistack_gui import matlab_registry, registry

    try:
        if registry._config is not None:
            registry.refresh_all()
        else:
            registry.refresh_module()
    except RuntimeError:
        logger.debug("Nothing to refresh in registry (no config/module loaded)")
    except Exception:
        logger.exception("Failed to refresh Python registry before discovery scan")

    try:
        matlab_registry.refresh_all()
    except Exception:
        logger.exception("Failed to refresh MATLAB registry before discovery scan")

    # Re-importing above may have re-registered scidb.Pipeline objects
    # (source -> GUI pipeline import — see pipeline_discovery.py); seed any
    # new ones now that the registry reflects the current source files.
    try:
        from scistack_gui.db import get_db, is_loaded
        from scistack_gui.pipeline_discovery import discover_and_seed_pipelines

        if is_loaded():
            discover_and_seed_pipelines(get_db())
    except Exception:
        logger.exception("Failed to discover/seed pipelines from source")


def _build_registry_backed_result(root: Path):
    """Build a ``scidb.discover.DiscoveryResult`` from ``registry``/
    ``matlab_registry`` state — the single source of truth for both
    packaged and loose-script/folder-scan projects. Reuses scidb's own
    result dataclasses so the existing ``_serialise_*`` helpers (and the
    frontend rendering code originally built for ``scan_project``'s output)
    work completely unchanged.

    Full parity with the old ``scan_project``-backed panel: functions,
    ``BaseVariable`` subclasses, and ``scidb.constant()`` instances are all
    covered — the last of those via ``registry.get_parameters_registry()``,
    which ``registry._scan_module_parameters`` populates alongside functions
    at every registry load. As of the source-declared migration (see
    docs/claude/code-discovery-categories.md), this is the SAME registry the
    GUI-native Parameter node concept — ``get_parameters()``/ EditTab's
    palette — reads from; the two are not separate concepts. The per-combo *value* a constant runs with still comes from
    ``pipeline_store``'s pending-constant table (a distinct, orthogonal
    mechanism — see ``execution_service.derive_fn_targets``), independent
    of the constant's source-declared default.

    ``project_code.name`` is the real ``[project].name`` from
    ``pyproject.toml`` when one exists (packaged mode), falling back to the
    project directory's own name otherwise (loose-script/folder-scan mode,
    where there is no pyproject.toml to read).
    """
    from scidb import BaseVariable
    from scidb.discover import DiscoveryResult, ModuleError, ModuleExports, PackageResult
    from scifor.discovery import read_project_name
    from scistack_gui import matlab_registry, registry

    by_source: dict[str, ModuleExports] = {}

    def module_for(source: str) -> ModuleExports:
        if source not in by_source:
            by_source[source] = ModuleExports(module_name=source)
        return by_source[source]

    for name, fn in registry._functions.items():
        source = registry._function_sources.get(name, "<unknown>")
        module_for(source).functions.append(fn)

    # BaseVariable._all_subclasses already contains BOTH real Python
    # variable classes AND the Python surrogate classes matlab_registry
    # creates for each MATLAB variable (see _register_matlab_variable) — a
    # separate pass over matlab_registry.get_all_variable_names() would
    # double-count them. Just re-attribute the MATLAB ones to their real
    # .m file path instead of the surrogate's (uninformative) __module__.
    matlab_var_paths = {
        name: str(path) for name, path in matlab_registry._matlab_variables.items()
    }
    for name, cls in BaseVariable._all_subclasses.items():
        if name in matlab_var_paths:
            source = matlab_var_paths[name]
        else:
            source = registry.resolve_module_source(
                getattr(cls, "__module__", None) or "<unknown>"
            )
        module_for(source).variables.append(cls)

    for fn_name in matlab_registry.get_all_function_names():
        info = matlab_registry.get_matlab_function(fn_name)
        source = str(info.file_path) if info.file_path is not None else "<matlab builtin>"
        # Not a real object with __name__ — _serialise_module_exports falls
        # back to str(f) for anything without one, which is already fn_name.
        module_for(source).functions.append(fn_name)

    for name, param in registry.get_parameters_registry().items():
        source = registry._parameter_sources.get(name, "<unknown>")
        module_for(source).parameters.append((name, param))

    errors = [
        ModuleError(module_name=e["source"], traceback=e["error"])
        for e in registry.all_load_errors()
    ]

    modules = sorted(by_source.values(), key=lambda m: m.module_name)
    project_name = read_project_name(root) or root.name
    project_code = PackageResult(name=project_name, modules=modules, errors=errors)
    return DiscoveryResult(project_code=project_code, libraries={})


# ---------------------------------------------------------------------------
# The handler table
# ---------------------------------------------------------------------------


class PathBody(BaseModel):
    path: str | None = ""


def _get_project_code() -> dict:
    return get_project_code()


def _get_project_paths() -> dict:
    return get_project_paths()


def _add_project_path(req: PathBody) -> dict:
    return add_project_path(req.path or "")


def _remove_project_path(req: PathBody) -> dict:
    return remove_project_path(req.path or "")


def _set_entities_file(req: PathBody) -> dict:
    return set_project_entities_file(req.path or None)


def _clear_entities_file() -> dict:
    return clear_project_entities_file()


def _refresh_project() -> dict:
    return refresh_project_sync()


_NO_DB = {"needs_db": False}

PROJECT_HANDLERS: tuple[Handler, ...] = (
    Handler("get_project_code", "/project/code", None, _get_project_code, http_method="GET", **_NO_DB),
    Handler("get_project_paths", "/project/paths", None, _get_project_paths, http_method="GET", **_NO_DB),
    Handler("add_project_path", "/project/paths", PathBody, _add_project_path, notify_dag_updated=True, **_NO_DB),
    # The browser sends the path as a query parameter (DELETE without a body).
    Handler("remove_project_path", "/project/paths", PathBody, _remove_project_path, http_method="DELETE", body=False, notify_dag_updated=True, **_NO_DB),
    Handler("set_entities_file", "/project/entities-file", PathBody, _set_entities_file, notify_dag_updated=True, **_NO_DB),
    Handler("clear_entities_file", "/project/entities-file", None, _clear_entities_file, http_method="DELETE", notify_dag_updated=True, **_NO_DB),
    Handler("refresh_project", "/project/refresh", None, _refresh_project, notify_dag_updated=True, **_NO_DB),
)

install_routes(router, PROJECT_HANDLERS)
