"""
Parse [tool.scistack] configuration from pyproject.toml or scistack.toml.

Supports multi-source pipeline discovery: explicit .py modules,
pip-installed packages, auto-discovered entry-point plugins, and
MATLAB .m files.
"""

import glob as _glob
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

from scifor.discovery import (
    extract_scistack_section,
    is_test_path,
    read_project_name,
)
# NOTE: scifor.discovery.find_project_config (the upward walk) is deliberately
# NOT used here any more. Anchoring that walk at the database directory is what
# let the reader and the writer disagree about the project root. It remains the
# right tool for scidb.entities, which walks from cwd.

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]

logger = logging.getLogger(__name__)

# Default location of the glue-node directory, relative to the project root.
# Sits beside ``src/scistack_entities.toml`` (scidb's DEFAULT_ENTITIES_RELPATH)
# because both are GUI-owned write targets, not user modules.
DEFAULT_GLUE_RELPATH = "src/scistack_glue"


def _config_path(project_root: Path, raw) -> Path:
    """A path *read from* scistack.toml/pyproject.toml, made absolute.

    Every raw config value goes through here rather than
    ``_normalize(project_root / raw)`` so that separator handling lives in
    exactly one place -- ``scifor.discovery.resolve_config_path``, which the
    non-GUI readers (``scidb.entities``) use too. A config written on
    Windows records ``src\\scistack_entities.toml``; joined naively on macOS
    that names a backslashed FILE in the project root, which is how classdef
    stubs ended up in the root instead of beside the entities file. A
    Windows-absolute value (``Y:\\LabMembers\\...``) has no meaning here at
    all and is left alone to fail as a missing path, with a warning, rather
    than being glued onto the project root.
    """
    from scifor.discovery import resolve_config_path

    return _normalize(resolve_config_path(project_root, str(raw)))


def _config_pattern(entry: str) -> str:
    """A glob pattern read from the config, with separators this platform
    understands. The Path form goes through :func:`_config_path`; a glob is
    expanded as text, so it needs the string half of the same rule."""
    from scifor.discovery import normalize_config_separators

    return normalize_config_separators(entry)


def _normalize(p) -> Path:
    """Return *p* as an absolute, normalized :class:`Path` without following
    symlinks or canonicalizing Windows mapped drives.

    ``Path.resolve()`` on Windows rewrites mapped-drive paths like
    ``y:\\foo`` to their UNC target ``\\\\server\\share\\foo``. VS Code 1.75+
    refuses to open UNC paths unless the host is in
    ``security.allowedUNCHosts`` — which means every file opened via the
    GUI's ``reveal_in_editor`` would fail with "UNC host … access is not
    allowed".

    ``os.path.abspath`` + ``os.path.normpath`` make the path absolute and
    collapse ``.``/``..`` segments while preserving the drive-letter form
    the user supplied, so stored paths continue to work anywhere VS Code
    can open them (and MATLAB accepts either form).

    Callers that genuinely need canonical-form comparison (e.g. the
    variables-vs-functions dedupe set) should still use ``.resolve()``
    directly — that's comparison-only and doesn't leak into stored paths.
    """
    return Path(os.path.normpath(os.path.abspath(str(p))))


def _identity_key(p) -> "tuple[int, int] | None":
    """A value equal for two paths naming the same file on disk, or ``None``
    when that can't be determined (path missing, stat refused, filesystem
    reports no usable inode).

    ``(st_dev, st_ino)`` is what ``os.path.samefile`` compares internally;
    using it directly keeps dedupe linear (one stat per path) instead of
    quadratic (one samefile per pair).
    """
    try:
        st = os.stat(p)
    except OSError:
        return None
    # Some filesystems report st_ino == 0, meaning "no file id available".
    if not st.st_ino:
        return None
    return (st.st_dev, st.st_ino)


def _same_path(a, b) -> bool:
    """Whether *a* and *b* name the same file/directory, despite spelling.

    :func:`_normalize` deliberately preserves the spelling the caller
    supplied, so a Windows mapped drive and its UNC target (``y:\\proj`` vs
    ``\\\\server\\share\\proj``) never compare equal as paths. That is
    correct for *storing* a path and wrong for *comparing* two, and the gap
    is what let a fresh scistack.toml get seeded with both spellings of one
    project: every file in it was then discovered, parsed and registered
    twice (35 "shadows previous definition" warnings, discovery errors
    doubling 17 -> 36 in one session).

    Falls back to normalized equality when file identity is unavailable, so
    a path that doesn't exist yet behaves exactly as it did before.
    """
    if _normalize(a) == _normalize(b):
        return True
    key_a = _identity_key(a)
    return key_a is not None and key_a == _identity_key(b)


def _dedupe_same_paths(paths: list[Path], what: str) -> list[Path]:
    """Drop paths already present under a different spelling, first wins.

    Applied to every resolved discovery list so that a config which *already*
    lists one directory twice (as a mapped drive and as its UNC target) is
    repaired on load — no migration step, and it covers hand-written configs
    that list a directory and a file inside it.
    """
    unique: list[Path] = []
    seen_norm: set[Path] = set()
    seen_ids: set[tuple[int, int]] = set()
    for p in paths:
        norm = _normalize(p)
        if norm in seen_norm:
            continue
        ident = _identity_key(p)
        if ident is not None and ident in seen_ids:
            continue
        seen_norm.add(norm)
        if ident is not None:
            seen_ids.add(ident)
        unique.append(p)
    dropped = len(paths) - len(unique)
    if dropped:
        logger.info(
            "[config] Dropped %d duplicate %s (same file via different path "
            "spellings)",
            dropped,
            what,
        )
    return unique


@dataclass
class SciStackConfig:
    """Parsed [tool.scistack] configuration."""

    project_root: Path
    """Directory containing the pyproject.toml or scistack.toml."""

    modules: list[Path] = field(default_factory=list)
    """Resolved absolute paths to user .py files."""

    entities_file: Path | None = None
    """The TOML file GUI-created Variables/Parameters/PathInputs are written
    to (default ``src/scistack_entities.toml``, see
    :func:`set_entities_file`).

    **The only file the GUI writes entity declarations into.** Declarations
    in ``.py`` modules and MATLAB entities scripts are still discovered --
    see ``variable_file`` and ``matlab_entities_file`` below -- but are
    read-only, the same contract that has always applied to a declaration
    outside the designated entities file
    (docs/claude/entity-editability-model.md). Its format is owned by
    ``scidb.entities``."""

    glue_dir: Path | None = None
    """Directory GUI-created **glue nodes** are written into (default
    ``src/scistack_glue/``, one ``.py``/``.m`` file per node).

    This **widens the GUI's writable surface** from ``entities_file`` alone to
    ``entities_file + glue_dir`` — a deliberate amendment to
    ``docs/claude/entity-editability-model.md``'s confinement rule, not an
    oversight. The spirit is preserved: still one designated, GUI-owned
    location; still never a user's hand-written module. A ``glue_`` function
    found anywhere else is discovered and usable, but read-only.

    The file is persistence, not a place the user navigates to — glue is
    written and edited through the GUI's code panel (D1a)."""

    variable_file: Path | None = None
    """A ``.py`` file of entity declarations, **read-only**.

    This was the write target before the entities file became TOML. It is
    kept as a discovery input so an existing project's declarations keep
    appearing in the GUI: :func:`load_config` folds it into ``modules``, so
    it is scanned like any other module even when the config lists it
    nowhere else. Nothing writes to it any more."""

    packages: list[str] = field(default_factory=list)
    """Explicit pip-installed package names to scan."""

    auto_discover: bool = True
    """Whether to scan ``scistack.plugins`` entry points."""

    # MATLAB support
    matlab_functions: list[Path] = field(default_factory=list)
    """Resolved absolute paths to MATLAB .m function files."""

    matlab_variables: list[Path] = field(default_factory=list)
    """Resolved absolute paths to MATLAB .m classdef files (BaseVariable subclasses)."""

    matlab_addpath: list[Path] = field(default_factory=list)
    """MATLAB path entries (auto-derived from parent dirs of functions, variables, and variable_dir)."""

    matlab_variable_dir: Path | None = None
    """Directory where ``create_variable`` writes new .m classdef files.

    Distinct from ``matlab_entities_file`` and NOT merged into it: a
    ``BaseVariable`` subclass is a *type*, and MATLAB requires one public
    classdef per file named after the file, so variables cannot live in the
    shared entities script.

    ``None`` is not a refusal to write classdefs: the destination then falls
    back to :func:`scimatlab.stubs.variable_stub_dir` (a directory beside
    the entities file), which is what both ``materialize_variable_stubs``
    and ``+scidb/entities.m`` use, and which ``load_config`` adds to
    ``matlab_addpath``. This field stays configured-only because
    :attr:`has_matlab` keys off it — defaulting it would make every
    Python-only project with an entities file load the MATLAB registry."""

    matlab_entities_file: Path | None = None
    """A MATLAB script of ``NAME = scidb.Parameter(...)`` top-level
    bindings, **read-only**.

    The MATLAB counterpart of ``variable_file``, and demoted for the same
    reason: ``entities_file`` (TOML) is language-neutral, so both languages
    now write to one file and read it through ``scidb.entities``. Existing
    scripts keep being parsed and shown (``matlab_registry.load_entities_script``);
    the GUI just never writes one."""

    matlab_sources: list[Path] = field(default_factory=list)
    """Unclassified .m files, from folder-scan fallback OR an explicit
    ``[matlab] sources = [...]`` config entry. Each file is classified
    per-content (variable vs. function) by
    :func:`scistack_gui.matlab_parser.classify_matlab_file` rather than
    being pre-sorted into ``matlab_functions``/``matlab_variables`` by the
    user. This is the field the GUI's Paths popup writes to (via
    :func:`add_path`) so a single added directory works for MATLAB without
    the user declaring function-vs-variable up front."""

    @property
    def has_matlab(self) -> bool:
        """Whether anything here needs ``matlab_registry`` to be loaded.

        One predicate, because every caller that decided this inline
        (bootstrap, both of server.py's startup paths) tested only
        ``functions``/``variables``/``sources`` -- so a project declaring
        its entities *only* in ``[matlab] entities_file`` never got its
        PathInputs and Parameters scanned at all, and they silently did not
        exist in the GUI.
        """
        return bool(
            self.matlab_functions
            or self.matlab_variables
            or self.matlab_sources
            or self.matlab_entities_file is not None
            or self.matlab_variable_dir is not None
        )


def load_config(project_path: Path | None, db_path: Path) -> SciStackConfig:
    """Load a SciStackConfig from a pyproject.toml.

    Parameters
    ----------
    project_path
        Explicit path to a config file *or* a project directory. If ``None``,
        the project root comes from :func:`resolve_project_root` — the folder
        the user opened, **not** anywhere near the database.
    db_path
        Path to the .duckdb file. Used only as
        :func:`resolve_project_root`'s last-resort fallback; it does not
        otherwise influence which project this is.

    Raises
    ------
    FileNotFoundError
        If an explicit ``project_path`` was given and does not exist on disk.
        (When no pyproject.toml/scistack.toml can be *located*, this function
        no longer raises — it falls back to scanning the project root for
        ``.py``/``.m`` files directly. See :func:`_folder_scan_config`.)
    ValueError
        If the located pyproject.toml has no ``[tool.scistack]`` section or
        the section is invalid.
    """
    logger.info(
        "[config] Locating config file (project_path=%s, db_path=%s)",
        project_path,
        db_path,
    )
    root = resolve_project_root(project_path, db_path)
    toml_path = locate_config_at(root)
    if toml_path is None:
        # Folder-scan the PROJECT, not the database's directory. Scanning
        # next to the .duckdb is what produced "0 .py, 0 .m" for a project
        # whose code lives on another drive entirely.
        logger.info(
            "[config] No pyproject.toml/scistack.toml found; falling back to "
            "folder-scan discovery rooted at %s",
            root,
        )
        return _folder_scan_config(root)
    project_root = toml_path.parent
    logger.info("[config] Found config at %s", toml_path)

    logger.info("[config] Loading TOML file")
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)

    logger.info("[config] Extracting [tool.scistack] section")
    section = _extract_scistack_section(data, toml_path.name)
    if section is None:
        logger.info(
            "[config] %s has no [tool.scistack] section; using defaults.", toml_path
        )
        section = {}
    else:
        logger.debug(
            "[config] Found config section with keys: %s", list(section.keys())
        )

    # --- modules ---
    logger.info("[config] Processing modules list")
    raw_modules = section.get("modules", [])
    if not isinstance(raw_modules, list):
        raise ValueError("[tool.scistack] modules must be a list of file paths.")
    logger.debug("[config] Found %d module entries in config", len(raw_modules))
    modules: list[Path] = []
    for entry_idx, entry in enumerate(raw_modules):
        logger.debug(
            "[config] Processing module entry %d/%d: %s",
            entry_idx + 1,
            len(raw_modules),
            entry,
        )
        if any(c in entry for c in ("*", "?", "[")):
            # Glob pattern (e.g. "pipelines/*.py")
            logger.debug("[config] Entry is a glob pattern")
            matched = sorted(
                Path(m)
                for m in _glob.glob(
                    str(project_root / _config_pattern(entry)),
                    recursive=True,
                )
                if m.endswith(".py")
            )
            if not matched:
                logger.warning("[config] modules glob matched no .py files: %s", entry)
            else:
                logger.debug("[config] Glob matched %d .py files", len(matched))
            modules.extend(matched)
        else:
            p = _config_path(project_root, entry)
            if p.is_dir():
                # Recursively discover all .py files in the directory,
                # pruning noise dirs (.venv, node_modules, etc.) the same
                # way folder-scan mode does -- otherwise a bare directory
                # entry sweeps in far more than the user intended.
                logger.debug("[config] Entry is a directory, searching for .py files")
                found = _walk_source_files(p, ".py")
                if not found:
                    logger.warning(
                        "[config] modules directory contains no .py files: %s",
                        p,
                    )
                else:
                    logger.debug("[config] Found %d .py files in directory", len(found))
                modules.extend(found)
            else:
                if not p.exists():
                    logger.warning(
                        "[config] Module listed in [tool.scistack] not found: %s",
                        p,
                    )
                else:
                    logger.debug("[config] Adding module file: %s", p)
                modules.append(p)

    before_count = len(modules)
    modules = [p for p in modules if not _is_test_file(p)]
    excluded_count = before_count - len(modules)
    if excluded_count:
        logger.debug(
            "[config] Excluded %d test file(s) from modules discovery", excluded_count
        )
    modules = _dedupe_same_paths(modules, "module file(s)")
    logger.info("[config] Resolved %d module files total", len(modules))

    # --- entities_file (TOML, the write target) ---
    #
    # scidb owns "where is this project's entities file" (CLAUDE.md NOTE 3);
    # this used to read the key here instead, which meant it MISSED scidb's
    # conventional-path fallback. A project with a pre-existing
    # src/scistack_entities.toml and no entities_file key therefore had its
    # entities read by scidb and by MATLAB and never loaded into the GUI
    # registry -- see .claude/plan-preexisting-entities-on-db-create-26-09-10.md
    # and resolve_entities_path for the full rule, including the `""`
    # opt-out that clear_entities_file writes.
    logger.info("[config] Processing entities_file")
    from scidb.entities import resolve_entities_path

    entities_file: Path | None = resolve_entities_path(project_root, section)
    if entities_file is None:
        logger.info(
            "[config] No entities file for %s (no entities_file key and no "
            "conventional src/scistack_entities.toml, or an explicit opt-out)",
            project_root,
        )
    elif section.get("entities_file"):
        logger.info("[config] entities_file set to: %s (declared in %s)",
                    entities_file, toml_path)
    else:
        logger.info(
            "[config] entities_file set to: %s (conventional path; %s declares "
            "no entities_file key)",
            entities_file,
            toml_path,
        )

    # --- glue_dir (the second writable surface: one file per glue node) ---
    logger.info("[config] Processing glue_dir")
    glue_dir: Path | None = None
    raw_gd = section.get("glue_dir")
    if raw_gd is not None:
        glue_dir = _config_path(project_root, raw_gd)
        logger.info("[config] glue_dir set to: %s", glue_dir)
        # Folded into modules so glue nodes are found by ordinary discovery —
        # they are real functions in real files, and get no new discovery path.
        # A hand-written config never maintains this, so load_config does.
        if glue_dir.is_dir():
            found = [
                p for p in _walk_source_files(glue_dir, ".py") if not _is_test_file(p)
            ]
            new_files = [
                p for p in found if not any(_same_path(p, m) for m in modules)
            ]
            modules.extend(new_files)
            logger.info(
                "[config] glue_dir contributed %d module file(s) (%d already covered)",
                len(new_files),
                len(found) - len(new_files),
            )
    else:
        logger.debug("[config] No glue_dir configured")

    # --- variable_file (legacy .py declarations, read-only) ---
    logger.info("[config] Processing variable_file")
    variable_file: Path | None = None
    raw_vf = section.get("variable_file")
    if raw_vf is not None:
        variable_file = _config_path(project_root, raw_vf)
        # Folded into modules so its declarations are still discovered even
        # if nothing else in the config covers it. Before the entities file
        # became TOML this was guaranteed by set_variable_file, which always
        # added it -- a hand-written config never had that guarantee, and
        # now that nothing writes the key, nothing maintains it either.
        if not _covered_by_modules(variable_file, raw_modules, project_root):
            modules.append(variable_file)
            logger.info(
                "[config] Added read-only variable_file %s to modules for "
                "discovery (no other config entry covers it)",
                variable_file,
            )
        else:
            logger.debug(
                "[config] variable_file %s is already covered by modules",
                variable_file,
            )
    else:
        logger.debug("[config] No variable_file configured")

    # --- packages ---
    logger.info("[config] Processing packages list")
    packages = section.get("packages", [])
    if not isinstance(packages, list):
        raise ValueError("[tool.scistack] packages must be a list of package names.")
    logger.debug("[config] Found %d packages: %s", len(packages), packages)

    # A packaged project (pyproject.toml with a [project].name + src/{name}/
    # layout) gets its OWN code auto-folded into packages, so it flows
    # through the same registry.load_from_config -> _load_packages pipeline
    # used for execution -- otherwise a packaged project relying purely on
    # this automatic layout (no explicit packages=[...] entry) would show
    # functions in the "Discovered Code" panel that raise KeyError at
    # actual run time (registry.get_function never having been populated).
    # Deliberately NOT uv.lock-based -- this only reads ordinary packaging
    # metadata ([project].name), so it works regardless of package manager.
    own_name = read_project_name(project_root)
    if (
        own_name
        and own_name not in packages
        and (project_root / "src" / own_name).is_dir()
    ):
        packages = [*packages, own_name]
        logger.info(
            "[config] Auto-folded packaged project's own code (%s) into packages",
            own_name,
        )

    # --- auto_discover ---
    logger.info("[config] Processing auto_discover setting")
    auto_discover = section.get("auto_discover", True)
    if not isinstance(auto_discover, bool):
        raise ValueError("[tool.scistack] auto_discover must be true or false.")
    logger.debug("[config] auto_discover = %s", auto_discover)

    # --- MATLAB section ([tool.scistack.matlab] or [matlab] in scistack.toml) ---
    logger.info("[config] Processing MATLAB configuration")
    matlab_section = section.get("matlab", {})
    if matlab_section:
        logger.debug(
            "[config] Found MATLAB section with keys: %s", list(matlab_section.keys())
        )
    else:
        logger.debug("[config] No MATLAB section found")

    matlab_functions = _resolve_glob_paths(
        project_root, matlab_section.get("functions", []), "matlab.functions"
    )
    matlab_variables = _resolve_glob_paths(
        project_root, matlab_section.get("variables", []), "matlab.variables"
    )
    # Unified, unclassified .m paths -- same auto-classify-per-file behavior
    # as folder-scan mode's matlab_sources (see classify_matlab_file), just
    # reachable from an explicit config too. This is what lets a single
    # GUI-added directory work for MATLAB without the user having to
    # declare "functions" vs. "variables" up front.
    matlab_sources = _resolve_glob_paths(
        project_root, matlab_section.get("sources", []), "matlab.sources"
    )
    matlab_variable_dir: Path | None = None
    raw_mvd = matlab_section.get("variable_dir")
    if raw_mvd is not None:
        matlab_variable_dir = _config_path(project_root, raw_mvd)
        logger.debug("[config] matlab_variable_dir set to: %s", matlab_variable_dir)
    else:
        logger.debug("[config] No matlab_variable_dir configured")

    matlab_entities_file: Path | None = None
    raw_mef = matlab_section.get("entities_file")
    if raw_mef is not None:
        matlab_entities_file = _config_path(project_root, raw_mef)
        logger.info(
            "[config] matlab_entities_file set to: %s", matlab_entities_file
        )
    else:
        logger.debug("[config] No matlab_entities_file configured")

    # Dedupe: any file in matlab.variables must not ALSO be parsed as a
    # plain function. This handles the common case where matlab.functions
    # points at a parent directory (e.g. "src/") containing it as a subtree.
    logger.info("[config] Deduplicating MATLAB functions vs variables")
    non_function_path_set = {p.resolve() for p in matlab_variables}
    original_fn_count = len(matlab_functions)
    matlab_functions = [
        p for p in matlab_functions if p.resolve() not in non_function_path_set
    ]
    excluded = original_fn_count - len(matlab_functions)
    if excluded:
        logger.info(
            "[config] Excluded %d file(s) from matlab.functions because they are "
            "also declared in matlab.variables.",
            excluded,
        )

    # Derive addpath from parent directories of all MATLAB file paths.
    logger.info("[config] Deriving MATLAB addpath from file locations")
    addpath_set: set[Path] = set()
    for p in matlab_functions:
        addpath_set.add(p.parent)
    for p in matlab_variables:
        addpath_set.add(p.parent)
    for p in matlab_sources:
        addpath_set.add(p.parent)
    if matlab_variable_dir is not None:
        addpath_set.add(matlab_variable_dir)
    if matlab_entities_file is not None:
        addpath_set.add(matlab_entities_file.parent)
    # Where classdef stubs for TOML-declared variables land when no
    # [matlab] variable_dir is configured. Included only for projects that
    # have MATLAB code at all -- a Python-only project never needs the
    # directory, and this must not be folded into the matlab_variable_dir
    # FIELD, whose non-None-ness is what makes has_matlab true.
    if entities_file is not None and matlab_variable_dir is None and (
        matlab_functions or matlab_variables or matlab_sources or matlab_entities_file
    ):
        try:
            from scimatlab.stubs import variable_stub_dir

            stub_dir = variable_stub_dir(project_root)
        except ImportError:  # scimatlab is optional; MATLAB is unusable without it
            stub_dir = None
        if stub_dir is not None:
            logger.info(
                "[config] No [matlab] variable_dir; classdef stubs for declared "
                "variables resolve to %s (added to the MATLAB path)",
                stub_dir,
            )
            addpath_set.add(stub_dir)
    matlab_addpath = sorted(addpath_set)
    logger.debug("[config] MATLAB addpath contains %d directories", len(matlab_addpath))

    logger.info("[config] Building final configuration")
    config = SciStackConfig(
        project_root=project_root,
        modules=modules,
        entities_file=entities_file,
        glue_dir=glue_dir,
        variable_file=variable_file,
        packages=packages,
        auto_discover=auto_discover,
        matlab_functions=matlab_functions,
        matlab_variables=matlab_variables,
        matlab_addpath=matlab_addpath,
        matlab_variable_dir=matlab_variable_dir,
        matlab_entities_file=matlab_entities_file,
        matlab_sources=matlab_sources,
    )
    logger.info(
        "[config] Configuration loaded from %s: %d modules, %d packages, auto_discover=%s, "
        "%d MATLAB functions, %d MATLAB variables, %d MATLAB sources; "
        "entities_file=%s (writable), variable_file=%s (read-only), "
        "matlab entities_file=%s (read-only)",
        toml_path,
        len(modules),
        len(packages),
        auto_discover,
        len(matlab_functions),
        len(matlab_variables),
        len(matlab_sources),
        entities_file,
        variable_file,
        matlab_entities_file,
    )
    _log_declaration_surfaces(config)
    return config


def _log_declaration_surfaces(config) -> None:
    """Name every file that can declare an entity, and say which may be written.

    The summary line above reports the three *configured* surfaces, and for a
    project with none of them set it prints three ``None``s -- which reads as
    "nothing declares entities here" even when a conventionally-named
    ``src/scistack_entities.py`` is being discovered as an ordinary module and
    is the only thing declaring the project's variables. That gap is what made
    ``Raw_EMG`` look like it came from nowhere (2026-09-01). So the surfaces
    are enumerated explicitly, including the ones nothing configured.
    """
    lines: list[str] = []

    if config.glue_dir is not None:
        lines.append(f"  writable  {config.glue_dir}  (glue nodes, one file each)")

    if config.entities_file is not None:
        lines.append(f"  writable  {config.entities_file}  (TOML, GUI writes here)")
    else:
        lines.append(
            "  writable  <none configured> — the GUI has nowhere to declare "
            "entities; opening the project should have created one"
        )

    for path, why in (
        (config.variable_file, "legacy Python entities file"),
        (config.matlab_entities_file, "legacy MATLAB entities script"),
    ):
        if path is not None:
            lines.append(f"  read-only {path}  ({why})")

    # Conventionally-named entity modules picked up by ordinary discovery.
    # These declare real entities and are read-only, but nothing else names
    # them anywhere, so a variable from one appears to have no source at all.
    for module in config.modules:
        if Path(module).stem == "scistack_entities" and module != config.variable_file:
            lines.append(
                f"  read-only {module}  (discovered as a module; declares "
                f"entities but is not the configured entities_file)"
            )

    try:
        from scimatlab.stubs import variable_stub_dir

        stub_dir = variable_stub_dir(config.project_root)
    except Exception:  # pragma: no cover - reporting only
        stub_dir = None
    if stub_dir is not None:
        lines.append(f"  generated {stub_dir}  (MATLAB classdef stubs, not a source)")

    logger.info(
        "[config] Entity declaration surfaces for %s:\n%s",
        config.project_root,
        "\n".join(lines),
    )


def _resolve_glob_paths(
    project_root: Path,
    raw_entries: list,
    label: str,
) -> list[Path]:
    """Resolve a list of file paths / glob patterns relative to project_root.

    Each entry can be a single ``.m`` file, a directory (recursively walked
    for ``.m`` files), or a glob pattern (only ``.m`` matches are kept).
    """
    if not isinstance(raw_entries, list):
        raise ValueError(f"[tool.scistack] {label} must be a list of file paths.")
    logger.debug("[config] Resolving %d entries for %s", len(raw_entries), label)
    result: list[Path] = []
    for entry_idx, entry in enumerate(raw_entries):
        logger.debug(
            "[config] Processing %s entry %d/%d: %s",
            label,
            entry_idx + 1,
            len(raw_entries),
            entry,
        )
        if any(c in entry for c in ("*", "?", "[")):
            # Glob pattern — expand and keep only .m files.
            logger.debug("[config] Entry is a glob pattern")
            matched = sorted(
                Path(p)
                for p in _glob.glob(
                    str(project_root / _config_pattern(entry)),
                    recursive=True,
                )
                if p.endswith(".m")
            )
            if not matched:
                logger.warning("[config] %s glob matched no .m files: %s", label, entry)
            else:
                logger.debug("[config] Glob matched %d .m files", len(matched))
            result.extend(matched)
        else:
            p = _config_path(project_root, entry)
            if p.is_dir():
                # Recursively discover all .m files in the directory,
                # pruning noise dirs and MATLAB private/@class/+package
                # dirs the same way folder-scan mode does.
                logger.debug("[config] Entry is a directory, searching for .m files")
                found = _walk_source_files(p, ".m", matlab=True)
                if not found:
                    logger.warning(
                        "[config] %s directory contains no .m files: %s",
                        label,
                        p,
                    )
                else:
                    logger.debug("[config] Found %d .m files in directory", len(found))
                result.extend(found)
            else:
                if not p.exists():
                    logger.warning("[config] %s file not found: %s", label, p)
                else:
                    logger.debug("[config] Adding .m file: %s", p)
                result.append(p)

    before_count = len(result)
    result = [p for p in result if not _is_test_file(p)]
    excluded_count = before_count - len(result)
    if excluded_count:
        logger.debug(
            "[config] Excluded %d test file(s) from %s discovery",
            excluded_count,
            label,
        )
    result = _dedupe_same_paths(result, f"{label} path(s)")
    logger.debug("[config] Resolved %d total paths for %s", len(result), label)
    return result



def _extract_scistack_section(data: dict, filename: str) -> dict | None:
    """Extract the scistack config section from parsed TOML data.

    For pyproject.toml the section is at ``[tool.scistack]``.
    For scistack.toml the section is at the top level (the whole file).
    """
    logger.debug("[config] Extracting scistack section from %s", filename)
    return extract_scistack_section(data, filename)


# ---------------------------------------------------------------------------
# Which directory is "the project"?
# ---------------------------------------------------------------------------

_project_root_hint: Path | None = None
"""Set once at startup from ``--project-root`` (the VS Code workspace
folder) -- the launching context's statement of which folder the user
opened. See :func:`resolve_project_root`.

The extension also spawns the server with ``cwd`` set to that same folder,
so rules 2 and 3 there agree; the flag remains because a child process's
cwd is not something the server can rely on being meaningful."""


def set_project_root_hint(path: "Path | str | None") -> None:
    """Record the launching context's idea of the project root."""
    global _project_root_hint
    _project_root_hint = _normalize(path) if path is not None else None
    logger.info("[config] Project root hint set to %s", _project_root_hint)


def locate_config_at(root: Path) -> Path | None:
    """The config file in *root*, or ``None``.

    **Looks in that directory and nowhere else.** No upward walk: the
    project root is decided once, by :func:`resolve_project_root`, and the
    config file lives there by definition. Walking is what let the reader
    and the writer disagree — see :func:`resolve_project_root`.

    ``pyproject.toml`` wins over ``scistack.toml`` when both exist: a
    packaged project's own metadata file is the more authoritative of the
    two, and ``add_path`` refuses to write to it anyway
    (``_reject_packaged_project``).
    """
    for name in ("pyproject.toml", "scistack.toml"):
        candidate = _normalize(root) / name
        if candidate.exists():
            logger.debug("[config] Found %s at project root %s", name, root)
            return candidate
    logger.info(
        "[config] No pyproject.toml/scistack.toml at project root %s", root
    )
    return None


def resolve_project_root(project_path: "Path | None", db_path: Path) -> Path:
    """**The** answer to "which directory is this project?".

    Every caller — reading config, writing config, placing the entities
    file — goes through here, so the reader and the writer cannot disagree.
    They used to: writing asked ``infer_project_root`` (which consulted
    ``--project-root``) while reading walked upward from the *database*
    directory. With a database on ``C:\\`` and a project on ``Y:\\`` that
    walk could never reach the project, so the GUI wrote a ``scistack.toml``
    it was then structurally incapable of reading back, and discovery
    silently stayed empty. See .claude/plan-unify-project-root.md.

    **The project root is the folder the user opened.** The database's
    location does not influence it (rule 4 is a last resort for a database
    opened with no session context at all). Order, most to least
    authoritative:

    1. An explicit ``--project``/``--module`` argument — the user naming a
       project outright.
    2. ``--project-root``, i.e. the VS Code workspace folder.
    3. The current working directory (browser/CLI, where the user launched
       from). ``scidb.entities`` already resolves this way.
    4. The database's own directory, with a warning.

    Deliberately does NOT search upward for a config file. A stray
    ``pyproject.toml`` in a parent directory — entirely plausible on a
    shared network drive — would otherwise capture every project beneath
    it. The cost is that opening a *subfolder* of a packaged repo no longer
    finds the config above it; open the repo root instead. The resolved
    root is logged on every load, and surfaced by
    :func:`describe_managed_paths`, so a wrong answer is visible rather
    than mysterious.

    An explicit ``project_path`` that doesn't exist is a hard error — that's
    a typo, not a missing-config situation.
    """
    if project_path is not None:
        p = _normalize(project_path)
        if p.is_file():
            # --module /path/to/pipeline.py, or --project /path/pyproject.toml
            logger.info("[config] Project root %s (from explicit path %s)", p.parent, p)
            return p.parent
        if p.is_dir():
            logger.info("[config] Project root %s (from explicit path)", p)
            return p
        raise FileNotFoundError(f"Path does not exist: {p}")

    if _project_root_hint is not None:
        logger.info(
            "[config] Project root %s (from --project-root)", _project_root_hint
        )
        return _project_root_hint

    cwd = _normalize(Path.cwd())
    if cwd.is_dir():
        logger.info("[config] Project root %s (working directory)", cwd)
        return cwd

    fallback = _normalize(db_path).parent
    logger.warning(
        "[config] Falling back to the database's own directory as the project "
        "root: %s -- pass --project-root to put project files elsewhere",
        fallback,
    )
    return fallback


# ---------------------------------------------------------------------------
# Folder-scan fallback (no pyproject.toml/scistack.toml present)
# ---------------------------------------------------------------------------

# Directories never worth walking into, for either language.
_NOISE_DIR_NAMES = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "node_modules",
    "build",
    "dist",
}


def _is_noise_dir(name: str) -> bool:
    return name in _NOISE_DIR_NAMES or name.startswith(".")


def _is_matlab_skip_dir(name: str) -> bool:
    """MATLAB directory conventions that must never be swept in as if their
    contents were standalone pipeline functions: ``private/`` helpers,
    ``@ClassName/`` method files, and ``+package/`` namespace folders.
    Proper support for the latter two is tracked as follow-on work — for
    folder-scan discovery the safe default is to skip them entirely rather
    than mis-register a class method or namespaced function."""
    return name == "private" or name.startswith("@") or name.startswith("+")


def _is_test_dir(name: str) -> bool:
    return name.lower() in {"test", "tests"}


# MATLAB test-suite naming convention: a PascalCase ``Test`` prefix (e.g.
# ``TestForEach.m``) or suffix (e.g. ``SomeFeatureTest.m``). Deliberately
# case-sensitive on the ``T`` so a lowercase word like ``latest.m`` never
# matches.
_MATLAB_TEST_FILE_RE = re.compile(r"^(Test[A-Z]\w*|\w*Test)\.m$")


def _is_test_file(p: Path) -> bool:
    """True if *p* should be excluded from discovery as test-only: any
    directory component is ``test``/``tests``, a Python filename following
    ``test_*.py``/``*_test.py``, or a MATLAB filename following the
    ``Test*.m``/``*Test.m`` PascalCase convention. Functions, variables,
    constants, PathInputs, Sweeps, and submodules found exclusively in a
    file matching this rule are never scanned/imported in the first place,
    so they never reach the final discovery results."""
    if is_test_path(p):
        return True
    return bool(_MATLAB_TEST_FILE_RE.match(p.name))


def _walk_source_files(root: Path, suffix: str, *, matlab: bool = False) -> list[Path]:
    """Recursively find files ending in *suffix* under *root*, pruning noise
    directories (and, for MATLAB, private/class/package folders) as we go.
    Test directories and test-named files are excluded (see _is_test_file)."""
    results: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [
            d
            for d in dirnames
            if not _is_noise_dir(d)
            and not _is_test_dir(d)
            and not (matlab and _is_matlab_skip_dir(d))
        ]
        for fname in filenames:
            if fname.endswith(suffix):
                candidate = _normalize(Path(dirpath) / fname)
                if _is_test_file(candidate):
                    logger.debug(
                        "[config] Excluding test file from discovery: %s", candidate
                    )
                    continue
                results.append(candidate)
    return sorted(results)


def _folder_scan_config(root: Path) -> SciStackConfig:
    """Build a :class:`SciStackConfig` by scanning *root* directly for
    ``.py``/``.m`` files, with no config file at all.

    Python files become ``modules`` — identical to how an explicit
    ``modules = ["some_dir"]`` entry is already resolved, so
    :func:`scistack_gui.registry._load_file_modules` needs no changes.

    MATLAB files become ``matlab_sources`` — a single unified list, since a
    folder scan has no way to know in advance which files are functions vs.
    ``BaseVariable`` classdefs. Each file is classified individually by
    :func:`scistack_gui.matlab_parser.classify_matlab_file` when
    :func:`scistack_gui.matlab_registry.load_from_sources` runs.
    """
    logger.info("[config] Folder-scan: walking %s for .py/.m files", root)
    py_files = _walk_source_files(root, ".py")
    m_files = _walk_source_files(root, ".m", matlab=True)
    logger.info(
        "[config] Folder-scan found %d .py file(s), %d .m file(s)",
        len(py_files),
        len(m_files),
    )

    addpath = sorted({p.parent for p in m_files})

    config = SciStackConfig(
        project_root=root,
        modules=py_files,
        matlab_sources=m_files,
        matlab_addpath=addpath,
    )
    logger.info(
        "[config] Folder-scan configuration built for %s: %d modules, "
        "%d MATLAB sources",
        root,
        len(py_files),
        len(m_files),
    )
    return config


# ---------------------------------------------------------------------------
# Writing scistack.toml (GUI Paths popup, loose-script projects only)
# ---------------------------------------------------------------------------
#
# pyproject.toml-based ("packaged") projects are out of scope for this
# write path -- see add_path/remove_path's explicit rejection below. This
# is deliberately not a general-purpose TOML editor: scistack.toml has no
# [project]/[build-system] noise to preserve, its entire content is keys
# this module already knows about, so every write regenerates the whole
# file from those known keys rather than attempting comment/formatting
# preservation (which would need a TOML round-trip library this codebase
# doesn't otherwise depend on).


def _portable_relpath(rel: Path) -> str:
    """A project-relative path as it should be RECORDED in scistack.toml.

    Forward slashes, always. These keys are the portable ones -- they point
    inside the project, so the same repo opened on another machine must
    resolve them -- and ``str(WindowsPath("src/x.toml"))`` is
    ``src\\x.toml``, which on macOS names a backslashed file in the project
    root rather than ``src/x.toml``. Windows accepts ``/`` natively, so
    POSIX form is correct on both. Readers additionally tolerate the
    backslashed form already committed to existing configs -- see
    ``scifor.discovery.resolve_config_path``.
    """
    return rel.as_posix()


def _toml_str(s: str) -> str:
    """Render *s* as a quoted TOML basic string, escaping backslashes
    (important for Windows paths) and double quotes."""
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _toml_array(items: list) -> str:
    if not items:
        return "[]"
    inner = ",\n    ".join(_toml_str(str(item)) for item in items)
    return f"[\n    {inner},\n]"


def _render_scistack_toml(
    *,
    modules: list,
    entities_file=None,
    glue_dir=None,
    variable_file=None,
    packages: list,
    auto_discover: bool,
    matlab_functions: list,
    matlab_variables: list,
    matlab_sources: list,
    matlab_variable_dir,
    matlab_entities_file=None,
    schema_keys: dict | None = None,
) -> str:
    """Render a complete scistack.toml from known [tool.scistack] fields.

    Round-trips every field this module understands (not just modules/
    matlab.sources) so that add_path/remove_path never silently drop
    hand-authored packages/auto_discover/entities_file/etc. That includes
    the read-only ``variable_file``/``[matlab] entities_file`` keys: this
    function no longer writes them from scratch, but must not delete one a
    user still has.
    """
    lines = [
        "# Written by the SciStack GUI's Paths popup.",
        "# Hand-editing is fine -- this file is re-read on every scan.",
        "",
        f"modules = {_toml_array(modules)}",
    ]
    if entities_file is not None:
        lines.append(f"entities_file = {_toml_str(str(entities_file))}")
    if glue_dir is not None:
        lines.append(f"glue_dir = {_toml_str(str(glue_dir))}")
    if variable_file is not None:
        lines.append(f"variable_file = {_toml_str(str(variable_file))}")
    if packages:
        lines.append(f"packages = {_toml_array(packages)}")
    if not auto_discover:
        lines.append("auto_discover = false")
    if (
        matlab_functions
        or matlab_variables
        or matlab_sources
        or matlab_variable_dir
        or matlab_entities_file
    ):
        lines.append("")
        lines.append("[matlab]")
        if matlab_functions:
            lines.append(f"functions = {_toml_array(matlab_functions)}")
        if matlab_variables:
            lines.append(f"variables = {_toml_array(matlab_variables)}")
        if matlab_sources:
            lines.append(f"sources = {_toml_array(matlab_sources)}")
        if matlab_variable_dir is not None:
            lines.append(f"variable_dir = {_toml_str(str(matlab_variable_dir))}")
        if matlab_entities_file is not None:
            lines.append(f"entities_file = {_toml_str(str(matlab_entities_file))}")

    # `[schema_keys]` is HAND-AUTHORED (scidb.schema_order reads it; nothing
    # here writes it), and this function rewrites the whole file — so it has to
    # be carried across or the Paths popup silently deletes a level order the
    # user typed. Emitted last: a TOML table swallows every key after it, so a
    # top-level key rendered below this line would land inside it.
    if schema_keys:
        lines.append("")
        lines.append("[schema_keys]")
        for key, levels in schema_keys.items():
            if isinstance(levels, (list, tuple)):
                lines.append(f"{key} = {_toml_array(list(levels))}")
    lines.append("")
    return "\n".join(lines)


def _load_raw_scistack_section(toml_path: Path) -> dict:
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)
    return _extract_scistack_section(data, toml_path.name) or {}


def _resolve_raw_entry(entry: str, project_root: Path) -> Path:
    """A raw ``modules``/``matlab.sources`` entry as an absolute path.

    Same separator handling as everything else read from the config -- the
    Paths popup compares against these, so an entry written on Windows must
    resolve to the same path here that ``load_config`` resolved it to, or
    "remove this path" silently matches nothing."""
    return _config_path(project_root, entry)


def _reject_packaged_project(toml_path: Path | None) -> None:
    if toml_path is not None and toml_path.name == "pyproject.toml":
        raise ValueError(
            "Packaged project (pyproject.toml found at "
            f"{toml_path}) -- the Paths popup's add/remove UI only manages "
            "loose-script projects (scistack.toml). Edit [tool.scistack] "
            "in pyproject.toml by hand instead."
        )


def describe_managed_paths(db_path: Path) -> dict:
    """Info the Paths popup needs to pick its rendering mode: whether the
    resolved config is a packaged (pyproject.toml) project -- read-only in
    the popup -- and, for loose-script projects, the raw (pre-discovery)
    ``modules`` entries currently written to scistack.toml. Empty until the
    first path is added via :func:`add_path`, even if folder-scan discovery
    is already finding files implicitly.

    Also reports ``project_root`` and ``config_path``. Everything here now
    hangs off which folder was resolved as the project, and the answer is
    deliberately NOT derived from the database's location -- so when it is
    wrong (VS Code opened somewhere unexpected), the popup can say which
    folder it settled on instead of just showing an empty list.
    """
    project_root = resolve_project_root(None, db_path)
    toml_path = locate_config_at(project_root)
    packaged = toml_path is not None and toml_path.name == "pyproject.toml"
    managed_paths: list[str] = []
    if toml_path is not None and toml_path.name == "scistack.toml":
        section = _load_raw_scistack_section(toml_path)
        managed_paths = list(section.get("modules", []))
    return {
        "packaged": packaged,
        "managed_paths": managed_paths,
        "project_root": str(project_root),
        "config_path": str(toml_path) if toml_path is not None else None,
    }


def add_path(db_path: Path, new_path: Path) -> Path:
    """Add *new_path* (a directory) to scistack.toml's ``modules`` and
    ``[matlab] sources`` lists, creating the file if none exists yet, and
    return the file that was written.

    Only valid for loose-script projects (no pyproject.toml at the
    resolved project root) -- see :func:`_reject_packaged_project`.

    On the very first write (no scistack.toml/pyproject.toml found at all,
    i.e. the project was previously running on pure folder-scan discovery),
    seeds both lists with the project root itself first, so the code that
    was implicitly discovered under folder-scan mode doesn't silently
    disappear the moment an explicit config file exists -- from that point
    on, discovery is exclusively config-driven (see load_config).
    """
    raw_new_path = Path(new_path)
    logger.info("[config] add_path: db_path=%s, new_path=%s", db_path, raw_new_path)
    if not raw_new_path.is_absolute():
        raise ValueError(f"Path must be absolute: {new_path}")
    if not raw_new_path.exists():
        raise FileNotFoundError(f"Path does not exist: {new_path}")
    if not raw_new_path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {new_path}")

    # Same resolver the READER uses, so `is_first_write` reflects reality.
    # It didn't before: this looked for a config next to the database while
    # the write below went to the project root, so every add believed it was
    # the first, started from an empty section, and silently discarded every
    # path added earlier.
    project_root = resolve_project_root(None, db_path)
    toml_path = locate_config_at(project_root)
    _reject_packaged_project(toml_path)

    is_first_write = toml_path is None
    if is_first_write:
        target_path = project_root / "scistack.toml"
        section: dict = {}
        logger.info(
            "[config] add_path: no config found, will create %s", target_path
        )
    else:
        target_path = toml_path
        section = _load_raw_scistack_section(toml_path)

    raw_modules = list(section.get("modules", []))
    matlab_section = dict(section.get("matlab", {}))
    raw_sources = list(matlab_section.get("sources", []))

    if is_first_write:
        for seed in _first_write_seed_roots(db_path, project_root):
            raw_modules.append(seed)
            raw_sources.append(seed)
            logger.info(
                "[config] add_path: seeding new scistack.toml with %s", seed
            )

    normalized_new = _normalize(new_path)
    new_str = str(normalized_new)

    # Identity, not spelling: adding \\server\share\x when y:\x is already
    # listed must not append a second entry for the same directory (see
    # _same_path).
    existing_modules_resolved = [
        _resolve_raw_entry(e, project_root) for e in raw_modules
    ]
    if any(_same_path(normalized_new, e) for e in existing_modules_resolved):
        logger.info(
            "[config] add_path: %s is already in modules (possibly under a "
            "different spelling) — not adding again",
            new_str,
        )
    else:
        raw_modules.append(new_str)

    existing_sources_resolved = [
        _resolve_raw_entry(e, project_root) for e in raw_sources
    ]
    if any(_same_path(normalized_new, e) for e in existing_sources_resolved):
        logger.info(
            "[config] add_path: %s is already in matlab.sources (possibly "
            "under a different spelling) — not adding again",
            new_str,
        )
    else:
        raw_sources.append(new_str)

    content = _render_scistack_toml(
        modules=raw_modules,
        entities_file=section.get("entities_file"),
        glue_dir=section.get("glue_dir"),
        variable_file=section.get("variable_file"),
        packages=list(section.get("packages", [])),
        auto_discover=section.get("auto_discover", True),
        matlab_functions=list(matlab_section.get("functions", [])),
        matlab_variables=list(matlab_section.get("variables", [])),
        matlab_sources=raw_sources,
        matlab_variable_dir=matlab_section.get("variable_dir"),
        matlab_entities_file=matlab_section.get("entities_file"),
        # Round-tripped, never written by the GUI: hand-authored level order
        # would otherwise be DELETED by the Paths popup, which rewrites the
        # whole file from the fields it knows.
        schema_keys=section.get("schema_keys"),
    )
    target_path.write_text(content)
    logger.info("[config] add_path: wrote %s (added %s)", target_path, new_str)
    return target_path


def remove_path(db_path: Path, path_to_remove: Path) -> Path:
    """Remove *path_to_remove* from scistack.toml's ``modules`` and
    ``[matlab] sources`` lists, and return the file that was written.

    Raises FileNotFoundError if no scistack.toml exists yet (never creates
    a file on remove). Only valid for loose-script projects.
    """
    logger.info(
        "[config] remove_path: db_path=%s, path_to_remove=%s", db_path, path_to_remove
    )
    project_root = resolve_project_root(None, db_path)
    toml_path = locate_config_at(project_root)
    if toml_path is None:
        raise FileNotFoundError(
            f"No scistack.toml/pyproject.toml at {project_root}; nothing to remove."
        )
    _reject_packaged_project(toml_path)

    section = _load_raw_scistack_section(toml_path)
    project_root = toml_path.parent
    target = _normalize(path_to_remove)

    # Identity, not spelling, so removing a path spelled as a mapped drive
    # also removes the UNC-spelled entry for the same directory (and vice
    # versa) instead of leaving a duplicate behind (see _same_path).
    raw_modules = [
        e for e in section.get("modules", [])
        if not _same_path(_resolve_raw_entry(e, project_root), target)
    ]
    matlab_section = dict(section.get("matlab", {}))
    raw_sources = [
        e for e in matlab_section.get("sources", [])
        if not _same_path(_resolve_raw_entry(e, project_root), target)
    ]
    removed = (len(section.get("modules", [])) - len(raw_modules)) + (
        len(matlab_section.get("sources", [])) - len(raw_sources)
    )
    logger.info("[config] remove_path: dropping %d entry/entries for %s", removed, target)

    content = _render_scistack_toml(
        modules=raw_modules,
        entities_file=section.get("entities_file"),
        glue_dir=section.get("glue_dir"),
        variable_file=section.get("variable_file"),
        packages=list(section.get("packages", [])),
        auto_discover=section.get("auto_discover", True),
        matlab_functions=list(matlab_section.get("functions", [])),
        matlab_variables=list(matlab_section.get("variables", [])),
        matlab_sources=raw_sources,
        matlab_variable_dir=matlab_section.get("variable_dir"),
        matlab_entities_file=matlab_section.get("entities_file"),
        # Round-tripped, never written by the GUI: hand-authored level order
        # would otherwise be DELETED by the Paths popup, which rewrites the
        # whole file from the fields it knows.
        schema_keys=section.get("schema_keys"),
    )
    toml_path.write_text(content)
    logger.info("[config] remove_path: wrote %s (removed %s)", toml_path, target)
    return toml_path


def _first_write_seed_roots(db_path: Path, project_root: Path) -> list[str]:
    """Directories to seed a brand-new scistack.toml's ``modules`` with.

    Creating the first config file switches discovery from folder-scan mode
    (which walks the database's own directory) to config-driven, so the
    database's directory is seeded to keep whatever was implicitly
    discovered. The project root is seeded too, and is usually a *different*
    directory now that it is inferred rather than assumed to be the
    database's -- that is the point of the fix, but it must add a root, not
    silently swap one.

    Identity, not spelling, decides whether those are two roots or one: the
    database is routinely opened by one spelling (a UNC path) while the
    project root arrives as another (a mapped drive) for the SAME directory,
    and seeding both made discovery register every file in the project
    twice. When they are the same directory the PROJECT ROOT spelling wins —
    it is the non-UNC one, and the form the rest of the session already uses
    (entities file, generated MATLAB ``project_root=``, reveal_in_editor).
    """
    db_dir = _normalize(db_path).parent
    root = _normalize(project_root)
    if _same_path(db_dir, root):
        logger.info(
            "[config] add_path: database directory %s is the same directory as "
            "the project root %s — seeding once, keeping the project-root "
            "spelling",
            db_dir,
            root,
        )
        return [str(root)]
    return [str(db_dir), str(root)]


def _covered_by_modules(target: Path, raw_modules: list, project_root: Path) -> bool:
    """Whether *target* would already be discovered by an existing entry in
    *raw_modules* (an exact file match, or a directory entry it lives
    under). Glob entries are ignored -- worst case we add a redundant
    explicit entry alongside a glob that happens to already match."""
    for entry in raw_modules:
        if any(c in entry for c in ("*", "?", "[")):
            continue
        resolved = _resolve_raw_entry(entry, project_root)
        if resolved == target:
            return True
        if resolved.is_dir() and target.is_relative_to(resolved):
            return True
    return False


def set_entities_file(
    db_path: Path, file_path: "Path | str | None" = None
) -> Path:
    """Set (or auto-create) the TOML entities file, and write the key into
    scistack.toml.

    Only valid for loose-script projects (no pyproject.toml at the resolved
    project root) -- see :func:`_reject_packaged_project`. Packaged
    projects must add ``entities_file`` under ``[tool.scistack]`` in
    pyproject.toml by hand, same as every other path in that mode.

    If *file_path* is ``None``, defaults to
    ``<project_root>/src/scistack_entities.toml``, where *project root* is
    :func:`resolve_project_root`'s answer -- **not** the database's own
    directory, which is usually a datasets folder that project files have
    no business being written into. A relative *file_path* resolves against
    that same root, which is what lets the creation wizard pass a bare
    ``"src/scistack_entities.toml"`` without knowing where the root will
    land; an absolute one is used as-is.

    The file is created with :func:`scidb.entities.initial_text` if it does
    not exist -- including its empty ``variables = []`` key, which has to be
    above the first section header and is fiddly to add correctly later. An
    existing file's contents are never touched. Unlike the ``.py`` entities
    file this replaces, it is **not** added to ``modules``: it is not
    Python, and executing it as a module would fail.
    """
    logger.info("[config] set_entities_file: db_path=%s, file_path=%s", db_path, file_path)
    project_root = resolve_project_root(None, db_path)
    toml_path = locate_config_at(project_root)
    _reject_packaged_project(toml_path)

    is_first_write = toml_path is None
    if is_first_write:
        target_path = project_root / "scistack.toml"
        section: dict = {}
        logger.info(
            "[config] set_entities_file: no config found, will create %s", target_path
        )
    else:
        target_path = toml_path
        section = _load_raw_scistack_section(toml_path)

    raw_modules = list(section.get("modules", []))
    matlab_section = dict(section.get("matlab", {}))
    raw_sources = list(matlab_section.get("sources", []))

    if is_first_write:
        for seed in _first_write_seed_roots(db_path, project_root):
            raw_modules.append(seed)
            raw_sources.append(seed)
            logger.info(
                "[config] set_entities_file: seeding new scistack.toml with %s", seed
            )

    # scidb owns both the default location and the file's initial contents
    # -- it owns the format (CLAUDE.md NOTE 3), so the GUI never hard-codes
    # either.
    from scidb.entities import DEFAULT_ENTITIES_RELPATH, initial_text

    if file_path is not None:
        raw_target = Path(file_path)
        entities_file = (
            _normalize(raw_target)
            if raw_target.is_absolute()
            else _normalize(project_root / raw_target)
        )
    else:
        entities_file = _normalize(project_root / DEFAULT_ENTITIES_RELPATH)

    if not entities_file.exists():
        entities_file.parent.mkdir(parents=True, exist_ok=True)
        entities_file.write_text(initial_text(), encoding="utf-8")
        logger.info("[config] set_entities_file: created new file %s", entities_file)
    else:
        logger.info(
            "[config] set_entities_file: %s already exists; leaving its contents "
            "untouched",
            entities_file,
        )

    try:
        entities_file_for_toml: "Path | str" = _portable_relpath(
            entities_file.relative_to(project_root)
        )
    except ValueError:
        entities_file_for_toml = entities_file

    content = _render_scistack_toml(
        modules=raw_modules,
        entities_file=entities_file_for_toml,
        glue_dir=section.get("glue_dir"),
        variable_file=section.get("variable_file"),
        packages=list(section.get("packages", [])),
        auto_discover=section.get("auto_discover", True),
        matlab_functions=list(matlab_section.get("functions", [])),
        matlab_variables=list(matlab_section.get("variables", [])),
        matlab_sources=raw_sources,
        matlab_variable_dir=matlab_section.get("variable_dir"),
        matlab_entities_file=matlab_section.get("entities_file"),
        # Round-tripped, never written by the GUI: hand-authored level order
        # would otherwise be DELETED by the Paths popup, which rewrites the
        # whole file from the fields it knows.
        schema_keys=section.get("schema_keys"),
    )
    target_path.write_text(content)
    logger.info(
        "[config] set_entities_file: wrote %s (entities_file=%s, toml value=%s)",
        target_path,
        entities_file,
        entities_file_for_toml,
    )
    return entities_file


def set_glue_dir(db_path: Path, dir_path: "Path | str | None" = None) -> Path:
    """Set (or auto-create) the glue-node directory and write the key into
    scistack.toml. Returns the resolved directory.

    The mirror of :func:`set_entities_file` for the *second* writable surface
    (see ``SciStackConfig.glue_dir``). Same rules: loose-script projects only,
    relative paths resolve against :func:`resolve_project_root`, an existing
    directory is left alone.

    Defaults to ``<project_root>/src/scistack_glue/``. The directory is
    created empty — a glue node is one file per node, written by the GUI's
    code panel, and there is no package ``__init__`` to scaffold because
    discovery walks the directory for ``.py``/``.m`` files rather than
    importing it as a package.
    """
    logger.info("[config] set_glue_dir: db_path=%s, dir_path=%s", db_path, dir_path)
    project_root = resolve_project_root(None, db_path)
    toml_path = locate_config_at(project_root)
    _reject_packaged_project(toml_path)

    is_first_write = toml_path is None
    if is_first_write:
        target_path = project_root / "scistack.toml"
        section: dict = {}
        logger.info("[config] set_glue_dir: no config found, will create %s", target_path)
    else:
        target_path = toml_path
        section = _load_raw_scistack_section(toml_path)

    raw_modules = list(section.get("modules", []))
    matlab_section = dict(section.get("matlab", {}))
    raw_sources = list(matlab_section.get("sources", []))

    if is_first_write:
        for seed in _first_write_seed_roots(db_path, project_root):
            raw_modules.append(seed)
            raw_sources.append(seed)
            logger.info("[config] set_glue_dir: seeding new scistack.toml with %s", seed)

    if dir_path is not None:
        raw_target = Path(dir_path)
        glue_dir = (
            _normalize(raw_target)
            if raw_target.is_absolute()
            else _normalize(project_root / raw_target)
        )
    else:
        glue_dir = _normalize(project_root / DEFAULT_GLUE_RELPATH)

    if not glue_dir.exists():
        glue_dir.mkdir(parents=True, exist_ok=True)
        logger.info("[config] set_glue_dir: created directory %s", glue_dir)
    else:
        logger.info("[config] set_glue_dir: %s already exists", glue_dir)

    try:
        glue_dir_for_toml: "Path | str" = _portable_relpath(
            glue_dir.relative_to(project_root)
        )
    except ValueError:
        glue_dir_for_toml = glue_dir

    content = _render_scistack_toml(
        modules=raw_modules,
        entities_file=section.get("entities_file"),
        glue_dir=glue_dir_for_toml,
        variable_file=section.get("variable_file"),
        packages=list(section.get("packages", [])),
        auto_discover=section.get("auto_discover", True),
        matlab_functions=list(matlab_section.get("functions", [])),
        matlab_variables=list(matlab_section.get("variables", [])),
        matlab_sources=raw_sources,
        matlab_variable_dir=matlab_section.get("variable_dir"),
        matlab_entities_file=matlab_section.get("entities_file"),
        # Round-tripped, never written by the GUI: hand-authored level order
        # would otherwise be DELETED by the Paths popup, which rewrites the
        # whole file from the fields it knows.
        schema_keys=section.get("schema_keys"),
    )
    target_path.write_text(content)
    logger.info(
        "[config] set_glue_dir: wrote %s (glue_dir=%s, toml value=%s)",
        target_path,
        glue_dir,
        glue_dir_for_toml,
    )
    return glue_dir


def clear_entities_file(db_path: Path) -> Path:
    """Stop pointing at an entities file (loose-script projects only).

    Never deletes the file on disk. Consistent with the project's
    ``feedback_never_delete_mark_hidden`` ethos: "remove" means stop
    pointing at it, never destroy it.

    Written as ``entities_file = ""``, an explicit opt-out, rather than by
    deleting the key: with the key simply gone,
    ``scidb.entities.resolve_entities_path`` falls back to the conventional
    ``src/scistack_entities.toml``, so clearing a file at that (default,
    overwhelmingly common) location would be a silent no-op. The empty
    string is also honoured by scidb and MATLAB, which the key-deletion form
    never was -- clearing used to leave the file live for everything except
    the GUI.
    """
    logger.info("[config] clear_entities_file: db_path=%s", db_path)
    project_root = resolve_project_root(None, db_path)
    toml_path = locate_config_at(project_root)
    if toml_path is None:
        raise FileNotFoundError(
            f"No scistack.toml/pyproject.toml at {project_root}; nothing to clear."
        )
    _reject_packaged_project(toml_path)

    section = _load_raw_scistack_section(toml_path)
    matlab_section = dict(section.get("matlab", {}))
    content = _render_scistack_toml(
        modules=list(section.get("modules", [])),
        entities_file="",
        glue_dir=section.get("glue_dir"),
        variable_file=section.get("variable_file"),
        packages=list(section.get("packages", [])),
        auto_discover=section.get("auto_discover", True),
        matlab_functions=list(matlab_section.get("functions", [])),
        matlab_variables=list(matlab_section.get("variables", [])),
        matlab_sources=list(matlab_section.get("sources", [])),
        matlab_variable_dir=matlab_section.get("variable_dir"),
        matlab_entities_file=matlab_section.get("entities_file"),
        # Round-tripped, never written by the GUI: hand-authored level order
        # would otherwise be DELETED by the Paths popup, which rewrites the
        # whole file from the fields it knows.
        schema_keys=section.get("schema_keys"),
    )
    toml_path.write_text(content)
    logger.info(
        '[config] clear_entities_file: wrote %s (entities_file = "", explicit '
        "opt-out; the file itself is untouched)",
        toml_path,
    )
    return toml_path
