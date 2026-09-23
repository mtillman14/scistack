"""
Where entity declarations get written, and the guards around writing them.

Two jobs:

1. **Append** (creation) -- the original role: shared "where do new
   PathInput/Sweep/Variable declarations get appended" lookup, used by
   ``services/path_input_service.py`` and ``api/variables.py``. Without the
   auto-create fallback here, every creation failed with a "--module not
   passed" error unless the user had hand-edited ``variable_file`` into
   scistack.toml themselves, with no GUI way to do so -- see
   ``.claude/pathinput-sweep-variable-creation-fixes.md``.

2. **Rewrite** (editing, Stage 5 of
   ``.claude/plan-gui-entity-editing-26-08-24.md``) --
   :func:`update_declaration` replaces an existing declaration's right-hand
   side in place. This module owns the *policy* around that: which file may
   be written, whether it has changed underneath us, atomic replacement,
   re-scan verification, and rollback. The *grammar* -- locating the span
   and rendering the replacement -- belongs to ``scidb.source_edit``
   (Python) and ``scistack_gui.matlab_parser`` (MATLAB).

Writes are confined to the configured entities file (``variable_file`` for
Python, ``[matlab] entities_file`` for MATLAB). A declaration living
anywhere else is read-only by design, and :func:`update_declaration`
refuses it with the exact source location so the UI can point at it. See
``docs/claude/entity-editability-model.md``.
"""

from __future__ import annotations

import hashlib
import keyword
import logging
import os
import re
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

_IMPORT_SCIDB_RE = re.compile(r"^import scidb$", re.MULTILINE)


def validate_entity_name(name: str) -> "str | None":
    """Return an error string if *name* isn't a valid top-level identifier
    for a new PathInput/Sweep/Constant binding, else ``None``. Shared by
    every append-only entity-creation service that writes into the
    configured entities file."""
    name = name.strip()
    if not name or not name.isidentifier() or keyword.iskeyword(name):
        return f"'{name}' is not a valid name."
    if name.startswith("_"):
        return "Names must not start with an underscore."
    return None


def is_toml_target(path: "Path | None") -> bool:
    """Whether *path* is a TOML entities file, and so uses
    ``scidb.entities``' grammar rather than Python's.

    Dispatch is by suffix because two target shapes genuinely coexist: a
    config-mode project writes ``entities_file`` (``.toml``), while legacy
    single-file mode (``--module pipeline.py``) still appends declarations
    to the module itself -- there is no config file there to record an
    entities file in, and writing one would flip the project into
    config-driven discovery as a side effect of creating a Parameter.
    """
    return path is not None and Path(path).suffix == ".toml"


TOML_SECTIONS = {"parameter": "parameters", "path_input": "path_inputs"}
"""Entity kind -> the ``[section]`` it lives in. Variables are not here:
they are a value-less top-level array, written by
:func:`scidb.entities.add_variable`."""


def write_entity(
    target_file: Path,
    *,
    section: str,
    name: str,
    rendered: str,
) -> "dict | None":
    """Set ``name`` in *section* of the TOML entities file, and refresh.

    The creation counterpart of :func:`update_declaration`: same file, same
    grammar, different entry point (there is nothing to locate first, and
    no rollback -- a failed create leaves an unusable declaration the user
    can see, not a corrupted one).

    Returns an error dict, or ``None`` on success.
    """
    from scidb.entities import initial_text, upsert_entry

    try:
        text = target_file.read_text(encoding="utf-8") if target_file.exists() else initial_text()
        updated = upsert_entry(text, section, name, rendered)
        _atomic_write(target_file, updated)
    except OSError as e:
        return {"ok": False, "error": f"Failed to write to entities file: {e}"}
    except ValueError as e:
        # render_value refusing a value TOML cannot hold (None, NaN, ...).
        return {"ok": False, "error": str(e)}

    logger.info(
        "[target_file_service] Wrote %s.%s to %s", section, name, target_file
    )
    error = _reload_after_write(target_file)
    if error is not None:
        return {"ok": False, "error": error}
    record_source_hash(target_file)
    return None


def write_variable(target_file: Path, name: str) -> "dict | None":
    """Add *name* to the TOML entities file's ``variables`` array."""
    from scidb.entities import add_variable, initial_text

    try:
        text = target_file.read_text(encoding="utf-8") if target_file.exists() else initial_text()
        _atomic_write(target_file, add_variable(text, name))
    except OSError as e:
        return {"ok": False, "error": f"Failed to write to entities file: {e}"}

    logger.info("[target_file_service] Wrote variable %s to %s", name, target_file)
    error = _reload_after_write(target_file)
    if error is not None:
        return {"ok": False, "error": error}
    record_source_hash(target_file)
    return None


def ensure_scidb_import(target_file: Path) -> None:
    """Idempotently make sure *target_file* has a bare ``import scidb`` line.

    Appended entity declarations use the qualified ``scidb.PathInput(...)``/
    ``scidb.Sweep(...)``/``scidb.constant(...)``/``scidb.BaseVariable`` form
    specifically so they never depend on what a pre-existing target file
    happens to already import -- a freshly auto-created file only ever gets
    a docstring header, and a bare-name append (``PathInput(...)`` with no
    import at all) would raise ``NameError`` the next time the file is
    scanned, silently (module-load failures during discovery are logged at
    DEBUG, not raised). Checked with a per-line regex rather than a
    substring check so an unrelated line like ``# see scidb docs`` can't
    produce a false positive.
    """
    text = target_file.read_text() if target_file.exists() else ""
    if _IMPORT_SCIDB_RE.search(text):
        return
    with open(target_file, "a") as f:
        f.write("\nimport scidb\n" if text else "import scidb\n")
    logger.debug("[target_file_service] Added 'import scidb' to %s", target_file)


def append_and_refresh(line: str, target_file: Path) -> "dict | None":
    """Ensure the required import is present, write *line* to *target_file*,
    and refresh the registry. Returns an error dict on failure, or ``None``
    on success. Shared by every append-only entity-creation service.

    Refreshes through :func:`_refresh_registries` rather than calling
    ``registry.refresh_all`` itself: this used to be a second, Python-only
    copy of that sequence, which dropped every MATLAB-registered entity from
    the shared registry on each create."""
    try:
        ensure_scidb_import(target_file)
        with open(target_file, "a") as f:
            f.write(line)
    except OSError as e:
        return {"ok": False, "error": f"Failed to write to module file: {e}"}

    error = _reload_after_write(target_file)
    if error is not None:
        return {"ok": False, "error": error}
    return None


def get_or_create_target_file() -> "tuple[Path | None, str | None]":
    """Return ``(target_file, error)`` -- exactly one is non-``None``.

    Resolution order:
      1. An already-configured ``entities_file`` (the TOML), or legacy
         single-file mode (``--module``).
      2. Project-mode config with no ``entities_file`` set: auto-create a
         default one for loose-script projects and persist it into
         scistack.toml. Packaged (``pyproject.toml``) projects get a clear
         hand-edit error instead -- the Paths popup never auto-writes to
         pyproject.toml (see ``config._reject_packaged_project``).
      3. No config and no module loaded at all: the original error.

    Note what is *not* here any more: ``variable_file``. A ``.py`` entities
    file is still discovered and shown, but it is no longer a write target
    -- new declarations go to the TOML even in a project that has one (see
    ``config.SciStackConfig.variable_file``).
    """
    from scistack_gui import registry

    if registry._config is not None and registry._config.entities_file is not None:
        return registry._config.entities_file, None
    if registry._module_path is not None:
        return registry._module_path, None

    if registry._config is not None:
        from scistack_gui import config as config_mod
        from scistack_gui.db import get_db_path
        from scistack_gui.services.registry_reload_service import (
            reload_registries_from_disk,
        )

        db_path = get_db_path()
        logger.info(
            "[target_file_service] No entities_file configured; attempting "
            "auto-create for project at %s",
            db_path,
        )
        try:
            config_mod.set_entities_file(db_path, None)
        except ValueError as e:
            logger.info("[target_file_service] Auto-create refused: %s", e)
            return None, (
                "No entities_file configured for this packaged project. Add "
                'entities_file = "path/to/scistack_entities.toml" under '
                "[tool.scistack] in pyproject.toml, then hit Refresh."
            )

        new_config = reload_registries_from_disk(db_path)
        logger.info(
            "[target_file_service] Auto-created entities_file=%s",
            new_config.entities_file,
        )
        return new_config.entities_file, None

    return None, "No module file was loaded at startup (--module not passed)."


# ---------------------------------------------------------------------------
# Rewriting an existing declaration
# ---------------------------------------------------------------------------

_SOURCE_KIND_REGISTRIES = {
    "parameter": "_parameter_sources",
    "path_input": "_path_input_sources",
}

_source_hashes: dict[str, str] = {}
"""Absolute file path -> sha256 of its contents when the registry last
scanned it. Populated by :func:`record_source_hash`; consumed by
:func:`update_declaration`'s stale-file guard, which refuses to write over
a file that has changed on disk since the values the GUI is showing were
read."""


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def record_source_hash(path: "Path | None") -> None:
    """Remember *path*'s current contents hash, as of a registry scan.

    Called after every scan of an entities file. A file we have no hash for
    is treated as "not verifiable" rather than "unchanged" -- see
    :func:`update_declaration`.
    """
    if path is None:
        return
    try:
        _source_hashes[str(path)] = _hash_text(path.read_text())
    except OSError as e:
        logger.debug("[target_file_service] Cannot hash %s: %s", path, e)


def declaration_source(kind: str, name: str) -> "str | None":
    """The source file the registry recorded for this entity, or ``None``."""
    from scistack_gui import registry

    attr = _SOURCE_KIND_REGISTRIES.get(kind)
    if attr is None:
        return None
    return getattr(registry, attr).get(name)


def _resolves_as_any_kind(name: str) -> bool:
    """Whether *name* is registered as ANY entity kind after a rewrite.

    Deliberately kind-agnostic: an edit may legitimately change an entity's
    kind. Adding a second value to a Constant rewrites it as a ``Sweep``
    (see ``docs/claude/entity-editability-model.md`` D4), so verifying
    against the kind it had *before* the edit would report success as a
    failure and roll a perfectly good write back.
    """
    return any(
        declaration_source(kind, name) is not None for kind in _SOURCE_KIND_REGISTRIES
    )


def _editable_targets() -> "tuple[Path | None, Path | None]":
    """``(entities_file, matlab_entities_file)`` -- the only files the GUI
    may rewrite.

    The first slot is ``entities_file``, **not** ``variable_file``: a
    legacy ``.py`` entities file is read-only now (see
    ``config.SciStackConfig.variable_file``), so returning it here would let
    an edit write to a file the model says is read-only -- and returning
    ``None`` for it, as this did before the fix, made *every* edit fail as
    ``read_only`` regardless of where the declaration lived.

    ``matlab_entities_file`` stays writable until Stage 5 gives MATLAB a
    TOML path (``+scidb/entities.m``). Demoting it before then would leave
    MATLAB projects with no editable surface at all, which is a worse
    outcome than the inconsistency.
    """
    from scistack_gui import registry

    config = registry._config
    if config is None:
        return (registry._module_path, None)
    return (config.entities_file, config.matlab_entities_file)


def _location_of(kind: str, name: str, source: str) -> dict:
    """``{file, line}`` for a read-only declaration, so the UI can say
    exactly where to go instead of a generic "edit it in source" hint."""
    from scidb.source_edit import find_binding_span, line_number

    path = Path(source)
    payload = {"file": source, "line": None}
    try:
        text = path.read_text()
    except OSError:
        return payload

    if path.suffix == ".toml":
        from scidb.entities import find_entry_span

        section = TOML_SECTIONS.get(kind)
        span = find_entry_span(text, section, name) if section else None
        if span is None:
            # Variables have no [section] entry -- they are elements of the
            # top-level array, so point at the array itself.
            span = find_entry_span(text, None, "variables")
        if span is not None:
            payload["line"] = line_number(text, span.start)
        return payload

    if path.suffix == ".m":
        from scistack_gui.matlab_parser import find_entities_binding

        binding = find_entities_binding(path, name)
        if binding is not None:
            payload["line"] = line_number(text, binding.expr_span.start)
        return payload

    span = find_binding_span(text, name)
    if span is not None:
        payload["line"] = line_number(text, span.start)
    return payload


def entity_editability(kind: str, name: str) -> dict:
    """Whether the GUI may rewrite *name*'s declaration, and if not, why.

    The ONE owner of the confinement rule (only the entities file is
    writable, docs/claude/entity-editability-model.md). Two consumers read it
    and must never diverge: :func:`update_declaration`, which refuses the
    write, and the sidebar panels (via ``get_entity_editability``), which
    grey their controls out before the user types anything. Before this the
    rule was only checked at write time, so a source-declared PathInput's
    root folder looked editable, took input, and then refused it.

    Returns ``{editable, reason, file, line, message}`` where *reason* is
    ``None`` (editable), ``"read_only"`` (declared outside the entities
    file) or ``"unknown"`` (not registered). *file*/*line* locate the
    declaration; *message* is the user-facing sentence for a refusal.
    """
    source = declaration_source(kind, name)
    if source is None:
        return {
            "editable": False,
            "reason": "unknown",
            "file": None,
            "line": None,
            "message": f"No {kind} named '{name}' is registered.",
        }

    path = Path(source)
    py_target, m_target = _editable_targets()
    target = m_target if path.suffix == ".m" else py_target
    if target is not None and Path(target) == path:
        return {
            "editable": True,
            "reason": None,
            "file": source,
            "line": None,
            "message": "",
        }

    location = _location_of(kind, name, source)
    where = location["file"]
    if location["line"] is not None:
        where = f"{where}:{location['line']}"
    logger.debug(
        "[target_file_service] entity_editability: %s '%s' is read-only "
        "(declared in %s, entities file is %s)",
        kind,
        name,
        where,
        target,
    )
    return {
        "editable": False,
        "reason": "read_only",
        "file": location["file"],
        "line": location["line"],
        "message": (
            f"'{name}' is declared in {where} — edit it there and hit "
            f"Refresh Code. The GUI only writes to the entities file."
        ),
    }


def update_declaration(
    kind: str, name: str, *, python_expr: str, matlab_expr: str, toml_expr: str
) -> dict:
    """Replace the right-hand side of an existing declaration in place.

    *toml_expr* / *python_expr* / *matlab_expr* are the rendered
    replacements; which one is used follows the owning file's format, so
    callers render all three and stay format-agnostic.

    Returns ``{"ok": True, "name", "file", "old", "new"}`` on success, or
    ``{"ok": False, "error", ...}``. Failure modes, all non-destructive:

    - the entity is unknown, or lives outside the entities file
      (``"read_only"``, with ``{file, line}``);
    - the file changed on disk since the last scan (``"stale"``);
    - the declaration can no longer be located;
    - the write or the post-write verification failed, in which case the
      original bytes are restored.
    """
    from scidb.source_edit import find_binding_span, splice

    editability = entity_editability(kind, name)
    if editability["reason"] == "unknown":
        return {"ok": False, "error": f"No {kind} named '{name}' is registered."}

    if not editability["editable"]:
        logger.info(
            "[target_file_service] Refusing to edit %s '%s': declared in %s, "
            "outside the configured entities file",
            kind,
            name,
            editability["file"],
        )
        return {
            "ok": False,
            "error": editability["message"],
            "reason": "read_only",
            "file": editability["file"],
            "line": editability["line"],
        }

    path = Path(editability["file"])
    is_matlab = path.suffix == ".m"
    is_toml = path.suffix == ".toml"

    try:
        original = path.read_text()
    except OSError as e:
        return {"ok": False, "error": f"Cannot read {path}: {e}"}

    known_hash = _source_hashes.get(str(path))
    if known_hash is not None and known_hash != _hash_text(original):
        logger.info(
            "[target_file_service] Refusing to edit %s '%s': %s changed on disk "
            "since the last scan",
            kind,
            name,
            path,
        )
        return {
            "ok": False,
            "error": (
                f"{path.name} has changed on disk since it was last read. "
                f"Hit 🔄 Refresh Code, then try again."
            ),
            "reason": "stale",
        }

    if is_toml:
        from scidb.entities import find_entry_span

        section = TOML_SECTIONS.get(kind)
        span = find_entry_span(original, section, name) if section else None
        replacement = toml_expr
    elif is_matlab:
        from scistack_gui.matlab_parser import find_entities_binding

        binding = find_entities_binding(path, name)
        span = binding.expr_span if binding is not None else None
        replacement = matlab_expr
    else:
        span = find_binding_span(original, name)
        replacement = python_expr

    if span is None:
        return {
            "ok": False,
            "error": (
                f"Could not locate the declaration of '{name}' in {path.name}. "
                f"It may be nested inside a function or written in a form the "
                f"editor cannot rewrite — edit it directly and hit Refresh Code."
            ),
            "reason": "unlocatable",
        }

    old_expr = span.extract(original)
    if kind == "path_input":
        _record_current_path_input_value(name)
    if old_expr == replacement:
        logger.debug(
            "[target_file_service] %s '%s' already reads %s; nothing to write",
            kind,
            name,
            replacement,
        )
        return {"ok": True, "name": name, "file": str(path), "old": old_expr,
                "new": replacement, "unchanged": True}

    updated = splice(original, span, replacement)

    logger.info(
        "[target_file_service] update_declaration: %s '%s' in %s: %s -> %s",
        kind,
        name,
        path,
        old_expr,
        replacement,
    )

    try:
        _atomic_write(path, updated)
    except OSError as e:
        return {"ok": False, "error": f"Failed to write {path}: {e}"}

    refresh_error = _reload_after_write(path)
    if refresh_error is None and not _resolves_as_any_kind(name):
        refresh_error = _verify_failure_reason(name)

    if refresh_error is not None:
        # Never leave a file the scanner can no longer read: put the original
        # bytes back and re-scan so the registry matches disk again.
        logger.warning(
            "[target_file_service] Rolling back edit to %s: %s", path, refresh_error
        )
        try:
            _atomic_write(path, original)
        except OSError as e:
            return {
                "ok": False,
                "error": (
                    f"{refresh_error} — AND restoring {path} failed: {e}. "
                    f"The file may be left in a bad state."
                ),
            }
        _reload_after_write(path)
        return {"ok": False, "error": refresh_error, "reason": "verify_failed"}

    record_source_hash(path)
    return {
        "ok": True,
        "name": name,
        "file": str(path),
        "old": old_expr,
        "new": replacement,
    }


def _record_current_path_input_value(name: str) -> None:
    """Remember the template *name* currently holds, immediately before a
    write-back overwrites it (D7).

    This is the ONLY moment history is recorded. Run history attributes a
    PathInput to a canvas node by content-matching its template, so
    overwriting a template would otherwise detach every run recorded against
    the old one. Editing a template *directly in source* still detaches it —
    that is unchanged, pre-existing behaviour for a deliberate hand-edit,
    and not something this table set out to fix.

    Best-effort: no open database (bootstrap, tests) means nothing to
    record, never a failed edit.
    """
    from scistack_gui import pipeline_store, registry

    try:
        from scistack_gui.db import get_db

        db = get_db()
    except Exception as e:
        logger.warning(
            "[target_file_service] No database while recording PathInput "
            "history for '%s' (%s) — a run recorded against its previous "
            "template will not resolve to this node",
            name,
            e,
        )
        return
    if db is None:
        logger.warning(
            "[target_file_service] No open database while recording PathInput "
            "history for '%s' — a run recorded against its previous template "
            "will not resolve to this node",
            name,
        )
        return

    pi = registry.get_path_inputs_registry().get(name)
    if pi is None:
        logger.warning(
            "[target_file_service] '%s' is not in the PathInput registry; "
            "cannot record the template being overwritten",
            name,
        )
        return

    # A PathInput with alternate templates is an EachOf of them; any arm can
    # be what a historical run matched on, so record each.
    arms = [pi] if getattr(pi, "path_template", None) is not None else (
        getattr(pi, "alternatives", None) or []
    )
    for arm in arms:
        template = getattr(arm, "path_template", None)
        if template is None:
            continue
        root = getattr(arm, "root_folder", None)
        try:
            pipeline_store.record_path_input_value(
                db, name, template, str(root) if root is not None else None
            )
        except Exception as e:  # never break an edit over this, but never hide it
            logger.warning(
                "[target_file_service] Could not record PathInput history for "
                "'%s' (template %r): %s",
                name,
                template,
                e,
            )
        else:
            logger.info(
                "[target_file_service] Recorded previous PathInput value: "
                "%s -> template=%r root_folder=%r",
                name,
                template,
                root,
            )


def _atomic_write(path: Path, text: str) -> None:
    """Replace *path*'s contents via a temp file in the same directory, so a
    crash mid-write can never leave a half-written entities file (which the
    scanner would then fail to parse, taking every entity in it down)."""
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    _invalidate_bytecode(path)


def _invalidate_bytecode(path: Path) -> None:
    """Drop any cached bytecode for *path*, so the re-scan actually re-reads
    it.

    A no-op for the TOML entities file: it is parsed, never imported, so it
    has no bytecode -- and asking ``cache_from_source`` for a ``.toml``
    path invents a ``__pycache__`` entry that never existed.

    ``registry`` loads entities files with ``spec_from_file_location`` +
    ``exec_module``, whose ``SourceFileLoader`` validates cached ``.pyc``
    against the source's **mtime (whole seconds) and size**. An entity edit
    routinely changes neither: ``constant(30)`` -> ``constant(45)`` and
    ``'a.csv'`` -> ``'b.csv'`` are byte-for-byte the same length, and the
    rewrite lands in the same second as the load that preceded it. Python
    then re-executes the STALE bytecode and the GUI keeps showing the old
    value even though the file on disk is correct — with nothing logged
    anywhere, because as far as every layer is concerned the write and the
    refresh both succeeded.
    """
    import importlib.util

    if path.suffix != ".py":
        return
    try:
        cached = importlib.util.cache_from_source(str(path))
    except (NotImplementedError, ValueError):
        return
    try:
        os.unlink(cached)
        logger.debug("[target_file_service] Dropped stale bytecode %s", cached)
    except FileNotFoundError:
        pass
    except OSError as e:
        logger.warning(
            "[target_file_service] Could not drop bytecode %s: %s — the next "
            "refresh may show a stale value",
            cached,
            e,
        )
    importlib.invalidate_caches()


def _reload_after_write(path: "Path | None") -> "str | None":
    """Re-read just the file that was written. Returns an error, or ``None``.

    The default for every GUI entity write. A write can only have changed
    the file it wrote, so re-importing every configured module and
    re-parsing every MATLAB source to discover that is pure waste: on a real
    project a full :func:`_refresh_registries` is ~16.5 s (2.5 s config +
    1.6 s Python modules + 14.9 s for 303 MATLAB sources, measured
    2026-09-01), against a single file parse here. Creating one variable
    used to cost the full amount.

    Dispatch is by suffix, matching :func:`is_toml_target`'s reasoning:

    - ``.toml`` -> ``registry.reload_entities_file()``
    - ``.m``    -> ``matlab_registry.reload_source(path)``
    - the loaded single-file module (legacy ``--module``) ->
      ``registry.refresh_module()``, which is already a one-file reload.

    Anything else falls back to the full :func:`_refresh_registries`. That
    is the hand-configured ``.py`` entities file: it is imported as one of
    many project modules, so there is no narrower way to re-read it, and
    being slow is much better than not reloading it at all.

    Note what does *not* happen here: scistack.toml is never re-read. No
    entity write can change which files are configured. The two things that
    can -- add/remove path, and creating the entities file itself -- go
    through ``services.registry_reload_service`` instead, and the Refresh
    Code button keeps its full reload as the deliberate escape hatch.
    """
    from scistack_gui import matlab_registry, registry

    if path is None:
        return _refresh_registries()

    suffix = Path(path).suffix
    try:
        if suffix == ".toml":
            return registry.reload_entities_file()
        if suffix == ".m":
            return matlab_registry.reload_source(Path(path))
        if registry._module_path is not None and Path(path) == Path(
            registry._module_path
        ):
            registry.refresh_module()
            return None
        return _refresh_registries()
    except Exception as e:
        logger.exception("[target_file_service] Narrow reload of %s failed", path)
        return f"Definition was written but re-reading {Path(path).name} failed: {e}"
    return None


def _refresh_registries() -> "str | None":
    """Re-scan after a write. Returns an error string, or ``None``.

    **Both** registries, Python then MATLAB -- the same order bootstrap and
    ``api/project._refresh_registries`` use. The MATLAB half is not
    optional: ``registry.load_from_config`` CLEARS the shared
    ``_path_inputs``/``_parameters`` dicts and repopulates them from Python
    modules and the TOML entities file only, while every MATLAB-declared
    PathInput/Parameter is registered into those same shared dicts by
    ``matlab_registry`` (see its ``_matlab_path_inputs`` docstring).
    Refreshing only the Python side therefore *deleted* every MATLAB entity
    from the registry on every entity write -- and for an edit to a
    MATLAB-declared PathInput that meant :func:`update_declaration`'s
    post-write verification found nothing, reported "no longer resolves
    after the edit", and rolled a perfectly good write back.
    """
    from scistack_gui import matlab_registry, registry

    try:
        if registry._config is not None:
            registry.refresh_all()
        else:
            registry.refresh_module()
    except Exception as e:
        return f"Definition was written but refresh failed: {e}"

    # No-op (with its own warning) when no MATLAB config was ever loaded, so
    # a pure-Python project pays nothing for this.
    try:
        matlab_registry.refresh_all()
    except Exception as e:
        return f"Definition was written but the MATLAB refresh failed: {e}"
    return None


def _verify_failure_reason(name: str) -> str:
    """Why *name* did not come back after a refresh, as specifically as the
    registries can say.

    The bare "may be invalid" message this replaces was the only thing the
    user saw for a write that was then rolled back -- so the evidence for
    *why* was destroyed along with the file. Load errors are recorded per
    source by both registries (``entities.EntityError.describe`` carries the
    entry name and line), so anything mentioning this name is the real
    reason and belongs in front of the user.
    """
    from scistack_gui import matlab_registry, registry

    reasons = [
        f"{err['source']}: {err['error']}"
        for err in registry.all_load_errors()
        if name in err.get("error", "")
    ]
    detail = f" Reported: {'; '.join(reasons)}" if reasons else ""
    logger.warning(
        "[target_file_service] '%s' is absent from every registry after the "
        "refresh. Known path inputs: %s. Known parameters: %s. Load errors: %s",
        name,
        sorted(registry.get_path_inputs_registry()),
        sorted(registry.get_parameters_registry()),
        registry.all_load_errors(),
    )
    return (
        f"'{name}' no longer resolves after the edit — the new value may be "
        f"invalid, so the file was restored.{detail}"
    )
