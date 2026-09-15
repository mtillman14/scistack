"""
Glue nodes — create, read, edit and hide the free-code reshapers that sit
between a variable and the function consuming it.

A glue node is a ``glue_``-prefixed function in its own file inside the
project's ``glue_dir`` (default ``src/scistack_glue/``). The file is
**persistence, not a destination**: the user writes and edits the body in the
GUI's code panel and never has to navigate to it. It is a real file anyway,
for three reasons that GUI state cannot provide — it diffs and reviews in git,
it takes a MATLAB breakpoint, and it is found by ordinary discovery with no
new discovery path.

This widens the GUI's writable surface from ``entities_file`` alone to
``entities_file + glue_dir`` — a deliberate amendment to
``docs/claude/entity-editability-model.md``'s confinement rule, documented in
``SciStackConfig.glue_dir``. A ``glue_`` function found anywhere else stays
read-only, exactly like a declaration outside the entities file.

Design: ``docs/claude/free-code-glue-nodes.md``.
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

GLUE_PREFIX = "glue_"


def _glue_dir(create: bool = False) -> "Path | None":
    """The project's configured glue directory, creating it on demand.

    ``create=True`` registers ``glue_dir`` in scistack.toml as a side effect
    (via ``config.set_glue_dir``) — the same auto-create-on-first-write
    fallback ``target_file_service`` gives the entities file, and for the same
    reason: without it, the first "New glue node" click fails with a
    configuration error the GUI offers no way to fix.
    """
    from scistack_gui import registry

    cfg = getattr(registry, "_config", None)
    if cfg is not None and getattr(cfg, "glue_dir", None) is not None:
        path = Path(cfg.glue_dir)
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path
    if not create:
        return None

    from scistack_gui import db as db_module
    from scistack_gui.config import set_glue_dir

    try:
        db_path = db_module.get_db_path()
    except Exception:
        db_path = None
    if db_path is None:
        return None
    # A packaged project refuses the auto-write (config._reject_packaged_project);
    # let the caller turn that into a hand-edit message rather than a 500.
    path = set_glue_dir(Path(db_path))
    if cfg is not None:
        cfg.glue_dir = path
    logger.info("[glue] auto-created glue_dir at %s", path)
    return path


def normalize_glue_name(name: str) -> str:
    """Apply the ``glue_`` prefix if the user did not type it.

    The prefix is what makes the node a glue node (``scidb.function_role``),
    so the GUI applies it rather than making the user remember it.
    """
    name = (name or "").strip()
    return name if name.startswith(GLUE_PREFIX) else f"{GLUE_PREFIX}{name}"


def validate_glue_name(name: str) -> "str | None":
    """Error string for an unusable glue name, or ``None``."""
    from scistack_gui.services.target_file_service import validate_entity_name

    err = validate_entity_name(name)
    if err:
        return err
    if not name.startswith(GLUE_PREFIX):
        return f"A glue node's name must start with '{GLUE_PREFIX}'."
    return None


def initial_body(name: str, param: str = "value", language: str = "python") -> str:
    """The minimum that parses, and nothing more.

    No docstring, no comment block, no TODO. A one-line reshape must read as
    a one-line file — scaffolding is what made the "just write it in GUI
    state" objection reasonable in the first place, so there is none.
    """
    if language == "matlab":
        return f"function out = {name}({param})\n    out = {param};\nend\n"
    return f"def {name}({param}):\n    return {param}\n"


def glue_file_path(name: str, language: str = "python", create: bool = False):
    """Where ``name``'s file lives, or ``None`` if there is no glue dir.

    One public function per file, named after the file — MATLAB requires it,
    and matching the layout in both languages keeps the two paths identical.
    """
    directory = _glue_dir(create=create)
    if directory is None:
        return None
    suffix = ".m" if language == "matlab" else ".py"
    return directory / f"{name}{suffix}"


def list_glue_nodes() -> list[dict]:
    """Every discovered glue function: ``{name, language, path, editable}``.

    ``editable`` is False for a ``glue_`` function found outside ``glue_dir``
    — discovered and perfectly usable, but hand-written and therefore
    read-only, the same contract entity declarations have always had.
    """
    from scidb import function_role
    from scistack_gui import registry

    directory = _glue_dir()
    out: list[dict] = []

    for name, fn in dict(registry._functions).items():
        if function_role(name) != "glue":
            continue
        path = _source_path(fn)
        out.append(
            {
                "name": name,
                "language": "python",
                "path": str(path) if path else None,
                "editable": _is_editable(path, directory),
            }
        )

    for name in _matlab_glue_names():
        path = _matlab_source_path(name)
        out.append(
            {
                "name": name,
                "language": "matlab",
                "path": str(path) if path else None,
                "editable": _is_editable(path, directory),
            }
        )

    out.sort(key=lambda e: e["name"])
    logger.debug("[glue] list_glue_nodes: %d node(s)", len(out))
    return out


def _matlab_glue_names() -> list[str]:
    from scidb import function_role

    try:
        from scistack_gui import matlab_registry as _mr

        return [n for n in _mr.get_all_function_names() if function_role(n) == "glue"]
    except Exception:
        return []


def _matlab_source_path(name: str):
    try:
        from scistack_gui import matlab_registry as _mr

        fn = _mr.get_matlab_function(name)
        return Path(fn.path) if getattr(fn, "path", None) else None
    except Exception:
        return None


def _source_path(fn):
    import inspect

    try:
        return Path(inspect.getsourcefile(fn) or "")
    except (TypeError, OSError):
        return None


def _is_editable(path, directory) -> bool:
    if path is None or directory is None:
        return False
    try:
        return Path(path).resolve().is_relative_to(Path(directory).resolve())
    except (OSError, ValueError):
        return False


def create_glue_node(
    name: str, param: str = "value", language: str = "python"
) -> dict:
    """Write a new glue file and refresh the registry.

    Returns ``{"ok": True, "name", "path", "source"}`` or
    ``{"ok": False, "error": ...}``.
    """
    from scistack_gui import registry

    name = normalize_glue_name(name)
    err = validate_glue_name(name)
    if err:
        return {"ok": False, "error": err}
    if registry.lookup_function(name) is not None:
        return {"ok": False, "error": f"'{name}' already exists."}

    try:
        path = glue_file_path(name, language, create=True)
    except ValueError as exc:
        # Packaged project: the Paths popup never auto-writes pyproject.toml.
        return {
            "ok": False,
            "error": (
                f'Add glue_dir = "src/scistack_glue" under [tool.scistack] in '
                f"pyproject.toml by hand, then hit Refresh. ({exc})"
            ),
        }
    if path is None:
        return {
            "ok": False,
            "error": (
                "No project directory is configured, so there is nowhere to "
                "write the glue node."
            ),
        }
    if path.exists():
        return {"ok": False, "error": f"{path} already exists."}

    source = initial_body(name, param=param, language=language)
    path.write_text(source, encoding="utf-8")
    logger.info("[glue] created %s (%s)", path, language)
    # Full config re-read, not the narrow per-file reload: a brand-new glue
    # file is not in the cached module list yet. See _reload_project.
    result = _reload_project()
    if result.get("error"):
        return {"ok": False, "error": f"{name} was written but not loaded: {result['error']}"}
    return {"ok": True, "name": name, "path": str(path), "source": source}


def read_glue_source(name: str) -> dict:
    """The current body of a glue node, for the code panel."""
    for entry in list_glue_nodes():
        if entry["name"] != name:
            continue
        path = entry["path"]
        if not path or not Path(path).exists():
            return {"ok": False, "error": f"No source file found for '{name}'."}
        return {
            "ok": True,
            "name": name,
            "language": entry["language"],
            "path": path,
            "editable": entry["editable"],
            "source": Path(path).read_text(encoding="utf-8"),
        }
    return {"ok": False, "error": f"'{name}' is not a known glue node."}


def update_glue_source(name: str, source: str) -> dict:
    """Rewrite a glue node's file, then refresh the registry.

    The refresh is what closes the loop: the new body has a new hash, so the
    consuming function's glue chain hash changes, so its next run recomputes
    instead of skipping. Without it the panel would look saved and the run
    would silently reuse the old body.
    """
    current = read_glue_source(name)
    if not current.get("ok"):
        return current
    if not current["editable"]:
        return {
            "ok": False,
            "error": (
                f"'{name}' lives at {current['path']}, outside the project's glue "
                f"directory, so it is read-only. Edit it in your editor."
            ),
        }

    path = Path(current["path"])
    language = current.get("language", "python")
    previous = path.read_text(encoding="utf-8")
    path.write_text(source, encoding="utf-8")
    result = _refresh(path)

    # Post-write verification, then rollback — the same policy
    # ``target_file_service.update_declaration`` applies to an entity edit.
    # A syntax error does NOT surface as a refresh error: discovery captures
    # per-module import failures and carries on, so the only evidence is that
    # the function stopped resolving. Without this check a broken body would
    # be saved, the panel would say "Saved", and the node would quietly
    # vanish from the registry.
    failure = result.get("error")
    if not failure and not _resolves(name, language):
        failure = (
            "it no longer loads (check the body for a syntax error — see the "
            "server log for the import traceback)"
        )
    if failure:
        path.write_text(previous, encoding="utf-8")
        _refresh(path)
        return {"ok": False, "error": f"{name} was not saved: {failure}"}

    logger.info("[glue] updated %s (%d chars)", path, len(source))
    return {"ok": True, "name": name, "path": str(path)}


def _resolves(name: str, language: str) -> bool:
    """Whether ``name`` is still discoverable after a write."""
    if language == "matlab":
        try:
            from scistack_gui import matlab_registry as _mr

            return name in set(_mr.get_all_function_names())
        except Exception:
            return False
    from scistack_gui import registry

    return registry.lookup_function(name) is not None


def delete_glue_node(name: str, db=None) -> dict:
    """Remove a glue node from the canvas — **never unlink its file**.

    Project ethos (``feedback_never_delete_mark_hidden``): "remove X" means
    stop showing it, never destroy the user's code. The ``.py``/``.m`` file
    stays on disk and in git, so the node can be re-added by wiring it up
    again and nothing the user wrote is lost.

    Returns the number of canvas nodes removed, so the UI can say "removed
    from 2 pipelines" rather than reporting a silent no-op.
    """
    from scistack_gui import db as db_module
    from scistack_gui import pipeline_store
    from scistack_gui.domain.edge_resolver import GLUE_NODE_TYPE

    try:
        active = db if db is not None else db_module.get_db()
    except Exception:
        active = None
    if active is None:
        return {"ok": False, "error": "No database is open."}

    removed = 0
    for node_id, meta in pipeline_store.get_manual_nodes(active).items():
        if meta.get("type") == GLUE_NODE_TYPE and meta.get("label") == name:
            pipeline_store.delete_node(active, node_id)
            removed += 1

    logger.info(
        "[glue] removed '%s' from %d canvas node(s); the source file is left "
        "on disk",
        name,
        removed,
    )
    return {"ok": True, "name": name, "removed": removed}


def input_columns(variable_type: str, db=None) -> dict:
    """The column list a glue on ``variable_type`` will actually receive.

    Thin delegation to :func:`variable_service.input_columns`, which is where
    this logic now lives: a variable's storage shape is a question about the
    VARIABLE, and it acquired a second asker (the function node's Inputs
    column picker) that has nothing to do with glue. Copying it there instead
    would have been the drift this codebase keeps paying for
    (``feedback_avoid_scifor_scidb_duplication``); behaviour for glue is
    unchanged, byte for byte.
    """
    from scistack_gui.services import variable_service

    return variable_service.input_columns(variable_type, db=db)


def _refresh(path: "Path | None" = None) -> dict:
    """Re-read the written file, exactly as the entities-file write path does.

    This is what closes the edit→recompute loop: the refreshed function
    object carries the new body, so ``compute_function_hash`` moves, so the
    chain hash moves, so the consumer's virtual glue record moves, so its
    next run recomputes instead of skipping.

    A ``.m`` glue file gets the narrow per-source reload; a ``.py`` one falls
    through to the full re-scan, because a project module has no narrower
    re-read (see ``target_file_service._reload_after_write``).
    """
    from scistack_gui.services.target_file_service import _reload_after_write

    try:
        error = _reload_after_write(path)
    except Exception as exc:  # a bad body must not take the server down
        logger.exception("[glue] registry refresh failed")
        return {"error": str(exc)}
    return {"error": error} if error else {}


def _reload_project() -> dict:
    """Re-read scistack.toml from disk, then reload both registries.

    Required after CREATING a glue file, and not interchangeable with
    :func:`_refresh`. The in-memory ``registry._config`` holds a ``modules``
    list computed when the config was last parsed; ``glue_dir``'s files are
    folded into that list at parse time (``config.load_config``). A brand-new
    glue file therefore is not in it, so a plain re-scan walks the old module
    set and the function is never discovered — it would exist on disk, be
    absent from the registry, and the node would appear to create successfully
    while doing nothing.

    The same reasoning is why ``target_file_service.get_or_create_target_file``
    calls ``reload_registries_from_disk`` after auto-creating the entities
    file, rather than the narrow reload it uses for ordinary writes.
    """
    from scistack_gui import db as db_module
    from scistack_gui.services.registry_reload_service import (
        reload_registries_from_disk,
    )

    try:
        db_path = db_module.get_db_path()
    except Exception:
        db_path = None
    if db_path is None:
        return _refresh()
    try:
        reload_registries_from_disk(Path(db_path))
    except Exception as exc:  # a bad body must not take the server down
        logger.exception("[glue] project reload failed")
        return {"error": str(exc)}
    return {}
