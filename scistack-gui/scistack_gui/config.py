"""
Parse [tool.scistack] configuration from pyproject.toml.

Supports multi-source pipeline discovery: explicit .py modules,
pip-installed packages, and auto-discovered entry-point plugins.
"""

import logging
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


@dataclass
class SciStackConfig:
    """Parsed [tool.scistack] configuration."""

    project_root: Path
    """Directory containing the pyproject.toml."""

    modules: list[Path] = field(default_factory=list)
    """Resolved absolute paths to user .py files."""

    variable_file: Path | None = None
    """The .py file where ``create_variable`` writes new classes."""

    packages: list[str] = field(default_factory=list)
    """Explicit pip-installed package names to scan."""

    auto_discover: bool = True
    """Whether to scan ``scistack.plugins`` entry points."""


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
    toml_path = _locate_pyproject(project_path, db_path)
    project_root = toml_path.parent

    with open(toml_path, "rb") as f:
        data = tomllib.load(f)

    section = data.get("tool", {}).get("scistack")
    if section is None:
        raise ValueError(
            f"{toml_path} does not contain a [tool.scistack] section."
        )

    # --- modules ---
    raw_modules = section.get("modules", [])
    if not isinstance(raw_modules, list):
        raise ValueError("[tool.scistack] modules must be a list of file paths.")
    modules: list[Path] = []
    for entry in raw_modules:
        p = (project_root / entry).resolve()
        if not p.exists():
            logger.warning("Module listed in [tool.scistack] not found: %s", p)
        modules.append(p)

    # --- variable_file ---
    variable_file: Path | None = None
    raw_vf = section.get("variable_file")
    if raw_vf is not None:
        variable_file = (project_root / raw_vf).resolve()

    # --- packages ---
    packages = section.get("packages", [])
    if not isinstance(packages, list):
        raise ValueError("[tool.scistack] packages must be a list of package names.")

    # --- auto_discover ---
    auto_discover = section.get("auto_discover", True)
    if not isinstance(auto_discover, bool):
        raise ValueError("[tool.scistack] auto_discover must be true or false.")

    config = SciStackConfig(
        project_root=project_root,
        modules=modules,
        variable_file=variable_file,
        packages=packages,
        auto_discover=auto_discover,
    )
    logger.info(
        "Loaded config from %s: %d modules, %d packages, auto_discover=%s",
        toml_path, len(modules), len(packages), auto_discover,
    )
    return config


def _locate_pyproject(project_path: Path | None, db_path: Path) -> Path:
    """Find the pyproject.toml to use."""
    if project_path is not None:
        p = project_path.resolve()
        if p.is_file():
            return p
        if p.is_dir():
            candidate = p / "pyproject.toml"
            if candidate.exists():
                return candidate
            raise FileNotFoundError(
                f"No pyproject.toml found in directory: {p}"
            )
        raise FileNotFoundError(f"Path does not exist: {p}")

    # Search upward from the database file's directory.
    search_dir = db_path.resolve().parent
    while True:
        candidate = search_dir / "pyproject.toml"
        if candidate.exists():
            # Only accept if it actually has [tool.scistack].
            try:
                with open(candidate, "rb") as f:
                    data = tomllib.load(f)
                if data.get("tool", {}).get("scistack") is not None:
                    return candidate
            except Exception:
                pass  # skip unparseable files
        parent = search_dir.parent
        if parent == search_dir:
            break
        search_dir = parent

    raise FileNotFoundError(
        f"No pyproject.toml with [tool.scistack] found in ancestors of {db_path}."
    )
