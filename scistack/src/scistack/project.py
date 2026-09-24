"""
Project scaffolder for SciStack.

Creates the standard project folder layout described in
``docs/claude/project-library-structure.md``::

    {name}/
    ├── .scistack/
    │   ├── project.toml
    │   └── snapshots/
    ├── pyproject.toml
    ├── src/{name}/__init__.py
    ├── .gitignore
    ├── README.md
    └── {name}.duckdb

Typical use::

    from scistack.project import scaffold_project

    project_root = scaffold_project(
        parent_dir=Path("/home/user/projects"),
        name="my_study",
        schema_keys=["subject", "session"],
    )
"""

from __future__ import annotations

import datetime
import logging
import re
from pathlib import Path

from scistack.uv_wrapper import sync

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
_NAME_RE = re.compile(r"[a-z][a-z0-9_]*")


def validate_project_name(name: str) -> None:
    """Raise :class:`ValueError` if ``name`` is not a valid project name.

    A valid project name is a valid Python package identifier: lowercase
    letters, digits, and underscores, starting with a lowercase letter.
    No auto-conversion is applied — the user must pick a valid name.
    """
    if not name:
        raise ValueError("Project name cannot be empty.")
    if not _NAME_RE.fullmatch(name):
        raise ValueError(
            f"Invalid project name: {name!r}. Must use only lowercase letters, "
            f"digits, and underscores, and must start with a lowercase letter. "
            f"Examples: 'my_study', 'eeg_analysis', 'experiment_2024'."
        )


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------
_PYPROJECT_TEMPLATE = """\
[project]
name = "{name}"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "scidb",
]

[dependency-groups]
dev = [
    "scistack-gui",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/{name}"]
"""

_GITIGNORE_TEMPLATE = """\
# SciStack
data/
*.duckdb
*.duckdb.wal
.venv/

# Python
__pycache__/
*.py[cod]
*.egg-info/
dist/
build/
"""

_README_TEMPLATE = """\
# {name}

A SciStack scientific data processing project.

## Setup

```bash
uv sync
```

## Usage

```python
from scidb import configure_database, BaseVariable

db = configure_database("{name}.duckdb", {schema_keys!r})

# Define your variables and pipeline here.
```
"""

_INIT_TEMPLATE = """\
\"\"\"
{name} — SciStack project.

Define your BaseVariable subclasses, @lineage_fcn functions,
and constants here (or in submodules of this package).
\"\"\"
"""

_PROJECT_TOML_TEMPLATE = """\
# SciStack project metadata
[scistack]
version = "0.1.0"
created = "{created}"
"""


# ---------------------------------------------------------------------------
# Scaffolder
# ---------------------------------------------------------------------------
def scaffold_project(
    parent_dir: Path,
    name: str,
    schema_keys: list[str],
    *,
    run_uv_sync: bool = True,
    configure_db: bool = True,
) -> Path:
    """Create a new SciStack project from scratch.

    Args:
        parent_dir: Directory under which the project folder is created.
        name: Project (and Python package) name. Must pass
            :func:`validate_project_name`.
        schema_keys: Metadata keys for the DuckDB dataset schema
            (e.g. ``["subject", "session"]``).
        run_uv_sync: If True (default), run ``uv sync`` after writing files
            to create ``uv.lock`` and the project venv. Set to False in
            tests that don't have ``uv`` or network access.
        configure_db: If True (default), call ``configure_database`` to
            create and initialise the ``.duckdb`` file. Set to False in
            tests that don't want the database side-effect.

    Returns:
        Absolute path to the created project directory.

    Raises:
        ValueError: If ``name`` is invalid.
        FileExistsError: If ``parent_dir / name`` already exists.
        UvNotFoundError: If ``run_uv_sync`` is True and ``uv`` is not on PATH.
    """
    validate_project_name(name)

    parent_dir = Path(parent_dir).resolve()
    project_root = parent_dir / name

    if project_root.exists():
        raise FileExistsError(
            f"Directory already exists: {project_root}. "
            f"Choose a different name or remove the existing directory."
        )

    logger.info("Scaffolding project %s in %s", name, parent_dir)

    # --- Directory structure ---
    project_root.mkdir(parents=True)
    (project_root / ".scistack" / "snapshots").mkdir(parents=True)
    src_pkg = project_root / "src" / name
    src_pkg.mkdir(parents=True)

    # --- Files ---
    (project_root / "pyproject.toml").write_text(_PYPROJECT_TEMPLATE.format(name=name))
    (project_root / ".gitignore").write_text(_GITIGNORE_TEMPLATE)
    (project_root / "README.md").write_text(
        _README_TEMPLATE.format(name=name, schema_keys=schema_keys)
    )
    (src_pkg / "__init__.py").write_text(_INIT_TEMPLATE.format(name=name))
    (project_root / ".scistack" / "project.toml").write_text(
        _PROJECT_TOML_TEMPLATE.format(
            created=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        )
    )

    # --- Database ---
    if configure_db:
        _create_database(project_root, name, schema_keys)

    # --- uv sync ---
    if run_uv_sync:
        result = sync(project_root)
        if result.ok:
            logger.info("uv sync succeeded for %s", name)
        else:
            logger.warning(
                "uv sync failed for %s (exit %d): %s",
                name,
                result.returncode,
                result.stderr,
            )

    return project_root


def _create_database(project_root: Path, name: str, schema_keys: list[str]) -> None:
    """Create and configure the project's DuckDB file."""
    from scidb.database import clear_current_database

    from scidb import configure_database

    db_path = project_root / f"{name}.duckdb"
    db = configure_database(db_path, schema_keys)
    db.close()

    # Clear the global state so the scaffolder doesn't leave a configured
    # database behind (the scaffolder is a tool, not the user's pipeline).
    clear_current_database()
