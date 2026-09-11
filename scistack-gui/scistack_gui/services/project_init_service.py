"""
Making a project's declaration surfaces exist, once, when it is opened.

Every SciStack project needs the same three things before the GUI can
declare anything into it: a ``scistack.toml`` at the project root, an
``entities_file`` key in it, and the TOML entities file that key names.
Nothing used to guarantee any of them. ``config.add_path`` created a
``scistack.toml`` with no ``entities_file`` key, and
``scidb.entities.entities_path`` only falls back to the conventional
``src/scistack_entities.toml`` *if that file already exists* -- so a project
could run for a whole session with no writable declaration surface at all,
which is what produced ``Unrecognized function or variable 'Raw_EMG'``: the
variable was placed on the canvas, nothing could declare it, and no MATLAB
classdef could be materialized because ``scimatlab.stubs.variable_stub_dir``
returns ``None`` when there is no entities file to sit beside.

This runs from ``bootstrap.open_or_create_project`` -- the one function both
entry points already share, so opening a project from the CLI and from the
frontend cannot drift.

**Every step is create-only-if-absent.** An existing file is never
rewritten, never reformatted, never reordered. Opening a project is not an
occasion to touch the user's source.

**No extra reload.** Steps that change *which* files are configured run
before the caller's first ``load_config``, so they are picked up by a load
that was going to happen anyway. The language stubs run after it, and a
freshly created stub declares nothing, so nothing needs re-reading. Opening
a project costs exactly the registry loads it always did.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class InitResult:
    """What init actually did, so the caller can log or surface it."""

    project_root: "Path | None" = None
    entities_file: "Path | None" = None
    created: list[str] = field(default_factory=list)
    """Absolute paths of files created by this run (empty on reopen)."""
    warnings: list[str] = field(default_factory=list)


_PY_STUB = '''"""Read-only entity declarations for this project.

The GUI never writes to this file. It writes to scistack_entities.toml,
which is the language-neutral declaration surface for Variables,
Parameters and PathInputs.

Declare things here only when the TOML form cannot express them:

  - a Variable with a custom ``to_db``/``from_db`` or a non-default
    ``schema_version``
  - a PathInput needing ``aliases``, ``key_regex`` or ``regex``
  - a Parameter whose value is computed rather than literal

Anything declared here is discovered and usable exactly like a TOML
declaration -- it just cannot be edited from the GUI, which will point you
back at this file instead.
"""

import scidb  # noqa: F401
'''

_M_STUB = """% Read-only entity declarations for this project.
%
% The GUI never writes to this file. It writes to scistack_entities.toml,
% which is the language-neutral declaration surface for Variables,
% Parameters and PathInputs.
%
% Declare things here only when the TOML form cannot express them --
% a PathInput needing aliases/key_regex/regex, or a Parameter whose value
% is computed rather than literal.
%
% Variables are the exception: a MATLAB Variable is a classdef, so declare
% it in scistack_entities.toml and a classdef stub is materialized for you
% under scistack_matlab_variables/. Do not hand-write one here.
"""


def ensure_project_files(
    db_path: Path,
    project: "Path | None" = None,
    entities_file: "str | Path | None" = None,
) -> InitResult:
    """Ensure scistack.toml and the TOML entities file exist. Idempotent.

    Call BEFORE the first ``load_config``: this can create the config file
    the load is about to read.

    *entities_file* is where to put one **if the project has none** -- the
    creation wizard's "Entities file" field, relative to the project root;
    ``None`` means the conventional ``src/scistack_entities.toml``. A project
    that already declares one keeps it, and the request is reported rather
    than applied: creating a database must never silently re-point an
    existing project's declarations at a new, empty file (which is what the
    wizard did, since it always sent its default).

    A packaged project (``pyproject.toml`` at the root) is left completely
    alone and reported instead -- ``config._reject_packaged_project``'s
    standing decision is that the GUI never hand-edits a pyproject.
    """
    from scistack_gui import config as config_mod

    result = InitResult()
    try:
        project_root = config_mod.resolve_project_root(project, Path(db_path))
    except Exception as e:
        logger.warning("[project_init] Could not resolve a project root: %s", e)
        result.warnings.append(f"Could not resolve a project root: {e}")
        return result
    result.project_root = project_root

    existing = config_mod.locate_config_at(project_root)
    if existing is not None and existing.name == "pyproject.toml":
        message = (
            f"Packaged project ({existing}): not creating a scistack.toml or an "
            f"entities file. To declare entities from the GUI, add "
            f'entities_file = "src/scistack_entities.toml" under [tool.scistack] '
            f"in pyproject.toml."
        )
        logger.info("[project_init] %s", message)
        result.warnings.append(message)
        return result

    # Already fully set up: don't rewrite scistack.toml just to put back what
    # it already says. set_entities_file re-renders the whole file, which
    # would bump its mtime on every open and churn the user's git status.
    from scidb.entities import entities_path, is_entities_opt_out
    from scifor.discovery import read_scistack_section

    # read_scistack_section, not the GUI's own reader: it swallows a parse
    # error into None, and an unparseable config must not turn "should I
    # create a file?" into a crash on project open.
    if existing is not None and is_entities_opt_out(read_scistack_section(existing)):
        # The user cleared the entities file on purpose. Creating one here
        # would silently put it back on the next open.
        logger.info(
            "[project_init] %s opts out of an entities file "
            '(entities_file = ""); not creating one',
            existing,
        )
        return result

    configured = entities_path(project_root) if existing is not None else None
    if configured is not None and configured.exists():
        logger.info(
            "[project_init] Project at %s already has %s; nothing to create",
            project_root,
            configured,
        )
        result.entities_file = configured
        if entities_file is not None and not _same_file(
            configured, project_root, entities_file
        ):
            message = (
                f"This project already declares its entities in {configured}; "
                f"keeping it rather than switching to the requested "
                f"{entities_file}. Change it in 📁 Paths if that is really "
                f"what you want."
            )
            logger.warning("[project_init] %s", message)
            result.warnings.append(message)
        return result

    if configured is not None:
        # The config names a file that isn't there yet (only an explicit key
        # can say that -- the conventional fallback resolves only when the
        # file exists). Create the file the project already names, not the
        # one the caller asked for: the key is a decision already made.
        logger.info(
            "[project_init] %s declares %s, which does not exist yet; creating it",
            existing,
            configured,
        )
        target: "str | Path | None" = configured
    else:
        target = entities_file

    had_config = existing is not None
    try:
        created_file = config_mod.set_entities_file(Path(db_path), target)
    except ValueError as e:
        logger.info("[project_init] Refused to initialize config: %s", e)
        result.warnings.append(str(e))
        return result
    except OSError as e:
        logger.warning("[project_init] Could not write project files: %s", e)
        result.warnings.append(f"Could not write project files: {e}")
        return result

    result.entities_file = created_file
    if not had_config:
        result.created.append(str(project_root / "scistack.toml"))
        logger.info(
            "[project_init] Created %s", project_root / "scistack.toml"
        )
    result.created.append(str(created_file))
    logger.info("[project_init] Entities file ready at %s", created_file)
    return result


def _same_file(configured: Path, project_root: Path, requested: "str | Path") -> bool:
    """Whether *requested* (relative to *project_root*, or absolute) names the
    file already *configured* -- so asking for what is already there is not
    reported as a conflict."""
    requested = Path(requested)
    if not requested.is_absolute():
        requested = project_root / requested
    return os.path.normpath(str(requested)) == os.path.normpath(str(configured))


def ensure_language_stubs(config) -> InitResult:
    """Create ``scistack_entities.py``/``.m`` beside the TOML, per language.

    Call AFTER ``registry.load_from_config``: which stubs are wanted depends
    on what the loaded config actually resolved, and a fresh stub declares
    nothing, so no reload is needed to account for one.

    Created only for a language the project actually uses -- a MATLAB-only
    project has no reason to grow a Python file it will never open -- and
    only when absent. These are read-only surfaces from the GUI's point of
    view; they exist so there is an obvious place to hand-author the
    declarations the TOML cannot express.
    """
    result = InitResult()
    entities_file = getattr(config, "entities_file", None)
    if entities_file is None:
        return result

    target_dir = Path(entities_file).parent
    result.project_root = getattr(config, "project_root", None)

    wanted: list[tuple[Path, str, str]] = []
    if config.modules or config.packages:
        wanted.append((target_dir / "scistack_entities.py", _PY_STUB, "Python"))
    if getattr(config, "has_matlab", False):
        wanted.append((target_dir / "scistack_entities.m", _M_STUB, "MATLAB"))

    for path, text, language in wanted:
        if path.exists():
            logger.debug("[project_init] %s entities file present: %s", language, path)
            continue
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        except OSError as e:
            logger.warning("[project_init] Could not create %s: %s", path, e)
            result.warnings.append(f"Could not create {path}: {e}")
            continue
        logger.info(
            "[project_init] Created read-only %s entities file %s", language, path
        )
        result.created.append(str(path))

        if not _is_discoverable(path, config):
            message = (
                f"{path} was created but is not on this project's discovery "
                f"path, so declarations added to it will not be picked up. Add "
                f"its folder in 📁 Paths."
            )
            logger.warning("[project_init] %s", message)
            result.warnings.append(message)

    _report_legacy_stub_dir(config)
    return result


def _is_discoverable(path: Path, config) -> bool:
    """Whether *path* is something the loaded config would actually scan.

    Checked rather than assumed, and reported rather than fixed: silently
    adding a folder to ``modules`` would widen discovery as a side effect of
    opening a project, which is the user's call, not init's.
    """
    if path.suffix == ".py":
        known = {Path(p) for p in config.modules}
    else:
        known = {Path(p) for p in getattr(config, "matlab_sources", [])}
    if path in known:
        return True
    # Folder-scan configs list files, not directories, so a file created
    # after the scan is legitimately absent from the list; being inside a
    # scanned directory is what matters.
    return path.parent in {p.parent for p in known}


def _report_legacy_stub_dir(config) -> None:
    """Note a pre-rename ``scistack_variables/`` folder, once.

    The stub directory was renamed to ``scistack_matlab_variables``. The old
    folder is deliberately not deleted (the project's ethos is to hide, not
    delete) and is no longer on the MATLAB path, so its classdefs are inert
    and cannot shadow the new ones. Saying so here is cheaper than someone
    finding two folders later and having to work out which one is live.

    Logged only, never returned as a warning: nothing is wrong and nothing
    needs doing, so surfacing it in the UI on every single open would be
    nagging about a leftover the user may reasonably want to keep.
    """
    try:
        from scimatlab.stubs import legacy_stub_dir
    except ImportError:
        return
    try:
        legacy = legacy_stub_dir(getattr(config, "project_root", None))
    except Exception:
        return
    if legacy is None:
        return
    logger.info(
        "[project_init] %s is left over from before the stub directory was "
        "renamed to scistack_matlab_variables. It is no longer on the MATLAB "
        "path and its classdefs are inert; delete it when convenient.",
        legacy,
    )
