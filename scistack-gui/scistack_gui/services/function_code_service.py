"""New functions written from the GUI: create, read and edit their source.

Until 2026-10-22 the functions tab's **+** only REFERENCED an existing
library function (``builtin_function_service``: ``numpy.mean``). A plain new
name went down that path and failed with ``'fn_name' not found in
'builtins'``: the GUI had no way to create a function the user writes.

A GUI-created function is one ``.py`` file per function in the project's
``functions_dir`` (default ``src/scistack_functions/``,
``config.set_functions_dir``), exactly as a glue node is one file in
``glue_dir`` (``glue_service``). The file is the persistence: the user edits
the body in the sidebar's code panel, and ordinary discovery finds it. A
function anywhere else stays read-only here.

The file mechanics (where to refresh, how to verify a write took, how to roll
back a body that no longer imports) are glue's, imported, not copied:
``glue_service._reload_project`` / ``_refresh`` / ``_resolves`` /
``_is_editable`` / ``_source_path``.
"""

from __future__ import annotations

import logging
from pathlib import Path

from scistack_gui import history

logger = logging.getLogger(__name__)


def _functions_dir(create: bool = False) -> "Path | None":
    """The project's ``functions_dir``, auto-created (and recorded in
    scistack.toml) on the first write, like ``glue_service._glue_dir``."""
    from scistack_gui import registry

    cfg = getattr(registry, "_config", None)
    if cfg is not None and getattr(cfg, "functions_dir", None) is not None:
        path = Path(cfg.functions_dir)
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path
    if not create:
        return None

    from scistack_gui import db as db_module
    from scistack_gui.config import set_functions_dir

    try:
        db_path = db_module.get_db_path()
    except Exception:
        db_path = None
    if db_path is None:
        return None
    path = set_functions_dir(Path(db_path))
    if cfg is not None:
        cfg.functions_dir = path
    logger.info("[function_code] auto-created functions_dir at %s", path)
    return path


def initial_body(name: str, params: "list[str] | None" = None) -> str:
    """The minimum that parses: the parameters, returned unchanged.

    No scaffolding comments (the same choice glue makes): a one-line step
    should read as a one-line file.
    """
    params = [p for p in (params or []) if p] or ["data"]
    first = params[0]
    return f"def {name}({', '.join(params)}):\n    return {first}\n"


def validate_function_name(name: str) -> "str | None":
    """Error string for an unusable new function name, or ``None``."""
    from scidb import function_role

    from scistack_gui import registry
    from scistack_gui.services.target_file_service import validate_entity_name

    err = validate_entity_name(name)
    if err:
        return err
    if function_role(name) == "glue":
        return "A name starting with 'glue_' is a glue node: create it under the Glue role."
    if registry.lookup_function(name) is not None:
        return f"'{name}' already exists."
    return None


def function_file_path(name: str, create: bool = False) -> "Path | None":
    directory = _functions_dir(create=create)
    return None if directory is None else directory / f"{name}.py"


def create_function(name: str, params: "list[str] | None" = None) -> dict:
    """Write a new function file and reload the project.

    Returns ``{"ok": True, "name", "path", "source"}`` or
    ``{"ok": False, "error": ...}``.
    """
    from scistack_gui.services import glue_service

    name = (name or "").strip()
    err = validate_function_name(name)
    if err:
        return {"ok": False, "error": err}
    for p in params or []:
        if not str(p).isidentifier():
            return {"ok": False, "error": f"'{p}' is not a valid parameter name."}

    path = function_file_path(name, create=True)
    if path is None:
        return {
            "ok": False,
            "error": "No project directory is configured, so there is nowhere to write the function.",
        }
    if path.exists():
        return {"ok": False, "error": f"{path} already exists."}

    source = initial_body(name, params)
    history.note_write(path)
    path.write_text(source, encoding="utf-8")
    logger.info("[function_code] created %s", path)
    # A brand-new file is not in the cached module list: a full re-read.
    result = glue_service._reload_project()
    if result.get("error"):
        return {"ok": False, "error": f"{name} was written but not loaded: {result['error']}"}
    if not glue_service._resolves(name, "python"):
        return {
            "ok": False,
            "error": f"{name} was written to {path} but discovery did not find it (see the server log).",
        }
    return {"ok": True, "name": name, "path": str(path), "source": source}


def read_function_source(name: str) -> dict:
    """The current source of a Python function, and whether it is editable
    here (it lives in ``functions_dir``)."""
    from scistack_gui import registry
    from scistack_gui.services import glue_service

    fn = registry.lookup_function(name)
    if fn is None:
        return {"ok": False, "error": f"'{name}' is not a known Python function."}
    path = glue_service._source_path(fn)
    if not path or not Path(path).exists():
        return {"ok": False, "error": f"No source file found for '{name}'."}
    return {
        "ok": True,
        "name": name,
        "language": "python",
        "path": str(path),
        "editable": glue_service._is_editable(path, _functions_dir()),
        "source": Path(path).read_text(encoding="utf-8"),
    }


def update_function_source(name: str, source: str) -> dict:
    """Rewrite a GUI-created function's file, verify it still loads, and roll
    back if it does not (glue's policy, ``glue_service.update_glue_source``).
    A new body is a new function hash, so its next run recomputes."""
    from scistack_gui.services import glue_service

    current = read_function_source(name)
    if not current.get("ok"):
        return current
    if not current["editable"]:
        return {
            "ok": False,
            "error": (
                f"'{name}' lives at {current['path']}, outside the project's functions "
                f"directory, so it is read-only here. Edit it in your editor."
            ),
        }
    path = Path(current["path"])
    previous = path.read_text(encoding="utf-8")
    history.note_write(path)
    path.write_text(source, encoding="utf-8")
    result = glue_service._refresh(path)
    failure = result.get("error")
    if not failure and not glue_service._resolves(name, "python"):
        failure = (
            "it no longer loads (check the body for a syntax error, and that the "
            f"function is still named {name}; the server log has the traceback)"
        )
    if failure:
        history.note_write(path)
        path.write_text(previous, encoding="utf-8")
        glue_service._refresh(path)
        return {"ok": False, "error": f"{name} was not saved: {failure}"}
    logger.info("[function_code] updated %s (%d chars)", path, len(source))
    return {"ok": True, "name": name, "path": str(path)}
