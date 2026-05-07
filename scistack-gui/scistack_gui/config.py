"""
Parse [tool.scistack] configuration from pyproject.toml or scistack.toml.

Supports multi-source pipeline discovery: explicit .py modules,
pip-installed packages, auto-discovered entry-point plugins, and
MATLAB .m files.
"""

import glob as _glob
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[no-redef]

logger = logging.getLogger(__name__)


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


@dataclass
class SciStackConfig:
    """Parsed [tool.scistack] configuration."""

    project_root: Path
    """Directory containing the pyproject.toml or scistack.toml."""

    modules: list[Path] = field(default_factory=list)
    """Resolved absolute paths to user .py files."""

    variable_file: Path | None = None
    """The .py file where ``create_variable`` writes new classes."""

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
    """Directory where ``create_variable`` writes new .m classdef files."""


def load_config(project_path: Path | None, db_path: Path) -> SciStackConfig:
    """Load a SciStackConfig from a pyproject.toml.

    Parameters
    ----------
    project_path
        Explicit path to a pyproject.toml file *or* a directory containing one.
        If ``None``, searches upward from *db_path* for a pyproject.toml that
        contains a ``[tool.scistack]`` section.
    db_path
        Path to the .duckdb file (used as fallback search root).

    Raises
    ------
    FileNotFoundError
        If no pyproject.toml can be located.
    ValueError
        If the located pyproject.toml has no ``[tool.scistack]`` section or
        the section is invalid.
    """
    logger.info("[config] Step 1: Locating config file (project_path=%s, db_path=%s)", project_path, db_path)
    toml_path = _locate_pyproject(project_path, db_path)
    project_root = toml_path.parent
    logger.info("[config] Step 2: Found config at %s", toml_path)

    logger.info("[config] Step 3: Loading TOML file")
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)

    logger.info("[config] Step 4: Extracting [tool.scistack] section")
    section = _extract_scistack_section(data, toml_path.name)
    if section is None:
        logger.info("[config] %s has no [tool.scistack] section; using defaults.", toml_path)
        section = {}
    else:
        logger.debug("[config] Found config section with keys: %s", list(section.keys()))

    # --- modules ---
    logger.info("[config] Step 5: Processing modules list")
    raw_modules = section.get("modules", [])
    if not isinstance(raw_modules, list):
        raise ValueError("[tool.scistack] modules must be a list of file paths.")
    logger.debug("[config] Found %d module entries in config", len(raw_modules))
    modules: list[Path] = []
    for entry_idx, entry in enumerate(raw_modules):
        logger.debug("[config] Processing module entry %d/%d: %s", entry_idx + 1, len(raw_modules), entry)
        if any(c in entry for c in ("*", "?", "[")):
            # Glob pattern (e.g. "pipelines/*.py")
            logger.debug("[config] Entry is a glob pattern")
            matched = sorted(
                Path(m) for m in _glob.glob(
                    str(project_root / entry), recursive=True,
                )
                if m.endswith(".py")
            )
            if not matched:
                logger.warning("[config] modules glob matched no .py files: %s", entry)
            else:
                logger.debug("[config] Glob matched %d .py files", len(matched))
            modules.extend(matched)
        else:
            p = _normalize(project_root / entry)
            if p.is_dir():
                # Recursively discover all .py files in the directory.
                logger.debug("[config] Entry is a directory, searching for .py files")
                found = sorted(p.rglob("*.py"))
                if not found:
                    logger.warning(
                        "[config] modules directory contains no .py files: %s", p,
                    )
                else:
                    logger.debug("[config] Found %d .py files in directory", len(found))
                modules.extend(found)
            else:
                if not p.exists():
                    logger.warning(
                        "[config] Module listed in [tool.scistack] not found: %s", p,
                    )
                else:
                    logger.debug("[config] Adding module file: %s", p)
                modules.append(p)
    logger.info("[config] Resolved %d module files total", len(modules))

    # --- variable_file ---
    logger.info("[config] Step 6: Processing variable_file")
    variable_file: Path | None = None
    raw_vf = section.get("variable_file")
    if raw_vf is not None:
        variable_file = _normalize(project_root / raw_vf)
        logger.debug("[config] variable_file set to: %s", variable_file)
    else:
        logger.debug("[config] No variable_file configured")

    # --- packages ---
    logger.info("[config] Step 7: Processing packages list")
    packages = section.get("packages", [])
    if not isinstance(packages, list):
        raise ValueError("[tool.scistack] packages must be a list of package names.")
    logger.debug("[config] Found %d packages: %s", len(packages), packages)

    # --- auto_discover ---
    logger.info("[config] Step 8: Processing auto_discover setting")
    auto_discover = section.get("auto_discover", True)
    if not isinstance(auto_discover, bool):
        raise ValueError("[tool.scistack] auto_discover must be true or false.")
    logger.debug("[config] auto_discover = %s", auto_discover)

    # --- MATLAB section ([tool.scistack.matlab] or [matlab] in scistack.toml) ---
    logger.info("[config] Step 9: Processing MATLAB configuration")
    matlab_section = section.get("matlab", {})
    if matlab_section:
        logger.debug("[config] Found MATLAB section with keys: %s", list(matlab_section.keys()))
    else:
        logger.debug("[config] No MATLAB section found")

    matlab_functions = _resolve_glob_paths(
        project_root, matlab_section.get("functions", []), "matlab.functions"
    )
    matlab_variables = _resolve_glob_paths(
        project_root, matlab_section.get("variables", []), "matlab.variables"
    )
    matlab_variable_dir: Path | None = None
    raw_mvd = matlab_section.get("variable_dir")
    if raw_mvd is not None:
        matlab_variable_dir = _normalize(project_root / raw_mvd)
        logger.debug("[config] matlab_variable_dir set to: %s", matlab_variable_dir)
    else:
        logger.debug("[config] No matlab_variable_dir configured")

    # Dedupe: any file in matlab.variables must not be parsed as a
    # function. This handles the common case where matlab.functions points
    # at a parent directory (e.g. "src/") that contains the variables dir
    # (e.g. "src/vars/") as a subtree.
    logger.info("[config] Step 10: Deduplicating MATLAB functions vs variables")
    var_path_set = {p.resolve() for p in matlab_variables}
    original_fn_count = len(matlab_functions)
    matlab_functions = [
        p for p in matlab_functions if p.resolve() not in var_path_set
    ]
    excluded = original_fn_count - len(matlab_functions)
    if excluded:
        logger.info(
            "[config] Excluded %d file(s) from matlab.functions because they are "
            "also declared in matlab.variables.",
            excluded,
        )

    # Derive addpath from parent directories of all MATLAB file paths.
    logger.info("[config] Step 11: Deriving MATLAB addpath from file locations")
    addpath_set: set[Path] = set()
    for p in matlab_functions:
        addpath_set.add(p.parent)
    for p in matlab_variables:
        addpath_set.add(p.parent)
    if matlab_variable_dir is not None:
        addpath_set.add(matlab_variable_dir)
    matlab_addpath = sorted(addpath_set)
    logger.debug("[config] MATLAB addpath contains %d directories", len(matlab_addpath))

    logger.info("[config] Step 12: Building final configuration")
    config = SciStackConfig(
        project_root=project_root,
        modules=modules,
        variable_file=variable_file,
        packages=packages,
        auto_discover=auto_discover,
        matlab_functions=matlab_functions,
        matlab_variables=matlab_variables,
        matlab_addpath=matlab_addpath,
        matlab_variable_dir=matlab_variable_dir,
    )
    logger.info(
        "[config] Configuration loaded from %s: %d modules, %d packages, auto_discover=%s, "
        "%d MATLAB functions, %d MATLAB variables",
        toml_path, len(modules), len(packages), auto_discover,
        len(matlab_functions), len(matlab_variables),
    )
    return config


def _resolve_glob_paths(
    project_root: Path, raw_entries: list, label: str,
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
        logger.debug("[config] Processing %s entry %d/%d: %s", label, entry_idx + 1, len(raw_entries), entry)
        if any(c in entry for c in ("*", "?", "[")):
            # Glob pattern — expand and keep only .m files.
            logger.debug("[config] Entry is a glob pattern")
            matched = sorted(
                Path(p) for p in _glob.glob(
                    str(project_root / entry), recursive=True,
                )
                if p.endswith(".m")
            )
            if not matched:
                logger.warning("[config] %s glob matched no .m files: %s", label, entry)
            else:
                logger.debug("[config] Glob matched %d .m files", len(matched))
            result.extend(matched)
        else:
            p = _normalize(project_root / entry)
            if p.is_dir():
                # Recursively discover all .m files in the directory.
                logger.debug("[config] Entry is a directory, searching for .m files")
                found = sorted(p.rglob("*.m"))
                if not found:
                    logger.warning(
                        "[config] %s directory contains no .m files: %s", label, p,
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
    logger.debug("[config] Resolved %d total paths for %s", len(result), label)
    return result


def _locate_pyproject(project_path: Path | None, db_path: Path) -> Path:
    """Find the pyproject.toml or scistack.toml to use."""
    if project_path is not None:
        logger.debug("[config] Explicit project_path provided: %s", project_path)
        p = _normalize(project_path)
        if p.is_file():
            logger.debug("[config] project_path is a file: %s", p)
            return p
        if p.is_dir():
            logger.debug("[config] project_path is a directory, searching for config file")
            # Prefer pyproject.toml, fall back to scistack.toml
            for name in ("pyproject.toml", "scistack.toml"):
                candidate = p / name
                if candidate.exists():
                    logger.debug("[config] Found %s in directory", name)
                    return candidate
            raise FileNotFoundError(
                f"No pyproject.toml or scistack.toml found in directory: {p}"
            )
        raise FileNotFoundError(f"Path does not exist: {p}")

    # Search upward from the database file's directory.
    logger.debug("[config] No explicit project_path, searching upward from db_path: %s", db_path)
    search_dir = _normalize(db_path).parent
    search_count = 0
    while True:
        search_count += 1
        logger.debug("[config] Searching directory %d: %s", search_count, search_dir)
        for name in ("pyproject.toml", "scistack.toml"):
            candidate = search_dir / name
            if candidate.exists():
                logger.debug("[config] Found %s, checking for [tool.scistack] section", name)
                try:
                    with open(candidate, "rb") as f:
                        data = tomllib.load(f)
                    section = _extract_scistack_section(data, name)
                    if section is not None:
                        logger.debug("[config] %s contains [tool.scistack] section", name)
                        return candidate
                    else:
                        logger.debug("[config] %s has no [tool.scistack] section, continuing search", name)
                except Exception:
                    logger.debug("[config] Failed to parse %s, continuing search", name)
                    pass  # skip unparseable files
        parent = search_dir.parent
        if parent == search_dir:
            logger.debug("[config] Reached filesystem root, search failed")
            break
        search_dir = parent

    raise FileNotFoundError(
        f"No pyproject.toml/scistack.toml with [tool.scistack] found "
        f"in ancestors of {db_path}."
    )


def _extract_scistack_section(data: dict, filename: str) -> dict | None:
    """Extract the scistack config section from parsed TOML data.

    For pyproject.toml the section is at ``[tool.scistack]``.
    For scistack.toml the section is at the top level (the whole file).
    """
    logger.debug("[config] Extracting scistack section from %s", filename)
    if filename == "scistack.toml":
        # The entire file IS the scistack config.
        logger.debug("[config] scistack.toml: entire file is config")
        return data  # empty file → {} → valid all-defaults config
    # pyproject.toml
    section = data.get("tool", {}).get("scistack")
    if section is None:
        logger.debug("[config] pyproject.toml: no [tool.scistack] section found")
    else:
        logger.debug("[config] pyproject.toml: found [tool.scistack] section")
    return section
