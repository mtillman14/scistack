"""
Creating a SciStack project: the one owner of ``init``.

``scistack init`` (CLI) and creating a database in the GUI (browser wizard
and VS Code) both call :func:`init_project`, so a project made either way has
the same layout (plan ``.claude/plan-portability.md`` Stage 1c)::

    my_study/
    ├── pyproject.toml                      # packaging only: name, deps, build
    ├── scistack.toml                       # THE config (scidb.config_file)
    ├── .gitignore
    └── src/my_study/
        ├── __init__.py
        └── scistack_entities.toml          # package data: ships in the wheel

**Every step creates only if absent.** An existing file is never rewritten,
reformatted or reordered, so running init in an existing folder (or twice)
only fills in what is missing; what it did and did not do is in the returned
:class:`InitReport` and logged.

The project's code is a package so it can be built into a wheel and shared
(portability Stage 5). Its entities file lives INSIDE the package so it is
package data: declarations travel with the code that uses them, while
``scistack.toml`` (project configuration) does not.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from scistacklog import Log

_NAME_RE = re.compile(r"[a-z][a-z0-9_]*")


def validate_project_name(name: str) -> None:
    """Raise :class:`ValueError` unless *name* is a valid package name:
    lowercase letters, digits and underscores, starting with a letter."""
    if not name:
        raise ValueError("Project name cannot be empty.")
    if not _NAME_RE.fullmatch(name):
        raise ValueError(
            f"Invalid project name: {name!r}. Use only lowercase letters, digits "
            f"and underscores, starting with a lowercase letter (e.g. 'my_study')."
        )


def package_name_for(root: "Path | str") -> str:
    """The package name for the project at *root*: ``pyproject.toml``'s
    ``[project].name`` when it has one (``-`` read as ``_``), else the folder
    name made into a valid identifier (``"Gait Study 2"`` -> ``gait_study_2``,
    ``"2024 data"`` -> ``project_2024_data``)."""
    from scifor.discovery import read_project_name

    declared = read_project_name(Path(root))
    if declared:
        return declared.replace("-", "_").lower()
    name = re.sub(r"[^a-z0-9_]+", "_", Path(root).resolve().name.lower()).strip("_")
    if not name or not name[0].isalpha():
        name = f"project_{name}".rstrip("_")
    return name


@dataclass
class InitReport:
    """What :func:`init_project` did. Paths are absolute."""

    root: Path
    package: str
    entities_file: "Path | None" = None
    """The entities file the project uses; ``None`` when it opts out
    (``entities_file = ""``) or its config could not be read."""
    created: list[Path] = field(default_factory=list)
    kept: list[Path] = field(default_factory=list)
    """Files that already existed and were left exactly as they were."""
    warnings: list[str] = field(default_factory=list)


_PYPROJECT = """\
[project]
name = "{name}"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "scidb",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/{name}"]
"""

_INIT = '''"""{name}: a SciStack project.

Functions and hand-written declarations live in this package. Variables,
Parameters and PathInputs created from the GUI are in scistack_entities.toml
beside this file, which ships with the package.
"""
'''

_GITIGNORE = """\
# SciStack
*.duckdb
*.duckdb.wal
*.duckdb.tmp
scidb.log

# Python
__pycache__/
*.py[cod]
*.egg-info/
dist/
build/
"""


def _create(path: Path, text: str, report: InitReport) -> None:
    if path.exists():
        report.kept.append(path)
        Log.info(f"[project] init: kept existing {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    report.created.append(path)
    Log.info(f"[project] init: created {path}")


def init_project(
    root: "Path | str",
    *,
    name: "str | None" = None,
    entities_file: "str | Path | None" = None,
) -> InitReport:
    """Make *root* a SciStack project, creating only what is missing.

    *name* is the package name (default :func:`package_name_for`).
    *entities_file* is where to put a new entities file, relative to *root*
    or absolute (default: inside the package, ``scidb.entities.
    default_entities_relpath``); it applies only when the project does not
    already name one.
    """
    from scifor.discovery import config_path_at, project_config_at, read_scistack_section

    from . import config_file
    from .entities import (
        default_entities_relpath,
        initial_text,
        is_entities_opt_out,
        resolve_entities_path,
    )

    root = Path(root).resolve()
    package = name or package_name_for(root)
    validate_project_name(package)  # before anything is written
    root.mkdir(parents=True, exist_ok=True)
    Log.info(f"[project] init_project: root={root} package={package}")

    report = InitReport(root=root, package=package)

    pyproject = root / "pyproject.toml"
    had_pyproject = pyproject.exists()
    _create(pyproject, _PYPROJECT.format(name=package), report)
    if had_pyproject:
        from scifor.discovery import read_project_name

        declared = read_project_name(root)
        if declared and declared.replace("-", "_").lower() != package:
            report.warnings.append(
                f"{pyproject} names the project {declared!r}, not {package!r}; "
                f"left unchanged. Its code is discovered from src/{declared}/."
            )
    _create(root / "src" / package / "__init__.py", _INIT.format(name=package), report)
    _create(root / ".gitignore", _GITIGNORE, report)

    # --- config + entities file -------------------------------------------
    config = project_config_at(root)
    section = read_scistack_section(config) if config is not None else None
    if config is None and config_path_at(root).exists():
        # Present but unparseable: never overwrite what the user wrote.
        report.warnings.append(
            f"{config_path_at(root)} could not be parsed; left unchanged and no "
            f"entities file was set up. Fix the file and run init again."
        )
        report.kept.append(config_path_at(root))
        Log.warn(f"[project] init: {report.warnings[-1]}")
        return report

    if section is not None and is_entities_opt_out(section):
        report.kept.append(config)
        Log.info(f'[project] init: {config} opts out of an entities file (entities_file = "")')
        return report

    # A declared entities_file wins even when the file is missing (then the
    # file the project already names is created); with no key, an existing
    # conventional file is adopted (scidb.entities.resolve_entities_path).
    existing = resolve_entities_path(root, section) if section is not None else None
    if existing is not None:
        target = existing
    else:
        rel = (
            Path(entities_file)
            if entities_file is not None
            else default_entities_relpath(root, package)
        )
        target = rel if rel.is_absolute() else root / rel
    report.entities_file = target
    _create(target, initial_text(), report)

    try:
        recorded = target.relative_to(root).as_posix()
    except ValueError:
        recorded = str(target)
    if section is None:
        # The project root is always seeded (the GUI's first-write rule), as
        # "." so the file stays portable. The package's own files under it
        # are loaded as a package, not as loose modules (GUI config loader).
        config_file.write(
            config_path_at(root),
            {"modules": ["."], "entities_file": recorded, "matlab": {"sources": ["."]}},
        )
        report.created.append(config_path_at(root))
    elif not section.get("entities_file") and existing is None:
        # Fill in the one missing key; every other key survives the write
        # (scidb.config_file), so nothing the user set changes.
        config_file.write(config, {**section, "entities_file": recorded})
        Log.info(f"[project] init: added entities_file = {recorded!r} to {config}")
    else:
        report.kept.append(config)

    for w in report.warnings:
        Log.warn(f"[project] init: {w}")
    Log.info(
        f"[project] init_project done: {len(report.created)} created, "
        f"{len(report.kept)} kept, {len(report.warnings)} warning(s)"
    )
    return report
