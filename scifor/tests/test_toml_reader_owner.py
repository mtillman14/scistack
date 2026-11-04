"""``scifor.project`` is the one place that reads TOML project files.

``scistack.toml`` and the entities file used to be opened by three packages
(scifor, the GUI's ``config.py`` twice, scidb), each deciding for itself
what a section meant (design review 09, T1 / 07 G3). This scans the whole
repo's package sources for a TOML read outside ``scifor/src/scifor/project``
and fails on any file not listed below with its reason. Fix a failure by
calling ``scifor.project.config`` (``read_project_config``,
``load_config_file``, ``read_scistack_section``) or
``scifor.project.entities_file``.
"""

import os
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
OWNER = REPO / "scifor" / "src" / "scifor" / "project"

#: Files that read TOML which is not a project config or entities file.
ALLOWED = {
    "scidb/src/scidb/project/bundle.py": "bundle manifest: the config bytes a .scistack "
    "bundle carries, and a bundled package's pyproject dependencies",
    "scidb/src/scidb/project/library.py": "an installed library's packaged files "
    "(entities defaults, pyproject metadata) read as raw data",
    "scistackplot/src/scistackplot/model/spec.py": "a PlotSpec saved as TOML",
}

_READ = re.compile(r"\btoml(?:lib|i)\.loads?\(")
_SKIP_PARTS = {
    "tests",
    "node_modules",
    "build",
    "site",
    "dist",
    "examples",
    "csv-stats",
    "__pycache__",
    ".venv",
}


def _sources() -> list[Path]:
    """Every ``.py`` under a package folder (one holding ``pyproject.toml``)
    or ``tools/``, pruning tests, vendored and generated trees as it walks."""
    roots = [d for d in REPO.iterdir() if (d / "pyproject.toml").is_file()]
    roots.append(REPO / "tools")
    found = []
    for root in roots:
        if root.name in _SKIP_PARTS:
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in _SKIP_PARTS and d != "frontend"]
            here = Path(dirpath)
            if here == OWNER or OWNER in here.parents:
                continue
            found.extend(here / f for f in filenames if f.endswith(".py"))
    return found


def test_no_toml_read_outside_scifor_project() -> None:
    readers = sorted(
        p.relative_to(REPO).as_posix()
        for p in _sources()
        if _READ.search(p.read_text(encoding="utf-8", errors="replace"))
    )
    unexpected = [r for r in readers if r not in ALLOWED]
    assert unexpected == [], "read TOML through scifor.project.config instead"


def test_allowed_readers_still_read_toml() -> None:
    """A listed file that stopped reading TOML is a stale entry."""
    stale = [
        name
        for name in ALLOWED
        if not _READ.search((REPO / name).read_text(encoding="utf-8"))
    ]
    assert stale == []
