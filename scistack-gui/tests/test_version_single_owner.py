"""The release tag is the one owner of every package's version.

hatch-vcs writes the tag into each distribution's metadata; a package's
``__version__`` must read it back from there, never restate it. Ten packages
used to hardcode ``__version__ = "0.1.0"``, so every saved plot recorded
``saved_with: {"scistackplot": "0.1.0"}`` whatever release made it.

This lives in scistack-gui's suite because scistack-gui depends on all the
packages it checks; the source scan covers the whole monorepo.
"""

from __future__ import annotations

import ast
import importlib
import importlib.metadata
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

#: import name -> distribution name. scidbnet is left out (slated for removal;
#: its imports may not be installed) but the source scan still covers it.
VERSIONED = {
    "scifor": "scifor",
    "scidb": "scistack-db",
    "scilineage": "scilineage",
    "scicanonicalhash": "scicanonicalhash",
    "scistackplot": "scistackplot",
    "scistack_gui": "scistack-gui",
    "scipathgen": "scipathgen",
    "scihist": "scihist",
    "scistack": "scistack",
    "scistackplotdb": "scistackplotdb",
}


def _tracked_pyprojects() -> list[Path]:
    """``<pkg>/pyproject.toml`` files in the monorepo, minus gitignored ones."""
    found = sorted(REPO.glob("*/pyproject.toml"))
    try:
        ignored = subprocess.run(
            ["git", "check-ignore", "--stdin"], cwd=REPO, capture_output=True, text=True,
            input="\n".join(str(p.relative_to(REPO)) for p in found),
        ).stdout.split()
    except OSError:  # no git: scan everything
        return found
    return [p for p in found if str(p.relative_to(REPO)) not in ignored]


def _package_sources():
    """Shipped source of every package: ``<pkg>/src/**`` plus scistack-gui's flat layout.

    Only packages git tracks: a gitignored clone with its own pyproject (the
    csv-stats repo, cloned at the root for editable dev) is not ours to version.
    """
    roots = [p.parent / "src" for p in _tracked_pyprojects()]
    roots.append(REPO / "scistack-gui" / "scistack_gui")
    for root in roots:
        if root.is_dir():
            yield from root.rglob("*.py")


def test_no_package_hardcodes_its_version():
    """``__version__ = "<literal>"`` is only allowed as the not-installed fallback."""
    offenders = []
    for path in _package_sources():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not any(isinstance(t, ast.Name) and t.id == "__version__" for t in node.targets):
                continue
            if isinstance(node.value, ast.Constant) and node.value.value != "0.0.0":
                offenders.append(f"{path.relative_to(REPO)}:{node.lineno} = {node.value.value!r}")
    assert not offenders, (
        "Hardcoded __version__ (read it from importlib.metadata instead; the git "
        "tag owns the version):\n  " + "\n  ".join(offenders)
    )


@pytest.mark.parametrize("module,dist", sorted(VERSIONED.items()))
def test_version_matches_installed_metadata(module, dist):
    mod = importlib.import_module(module)
    assert mod.__version__ == importlib.metadata.version(dist)
