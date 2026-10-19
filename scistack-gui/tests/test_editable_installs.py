"""Every monorepo package must import from this checkout, not a stale copy.

A plain ``pip install ./scistackstatsdb`` copies the package into
site-packages; new modules added to the source afterwards are invisible, and
the failure surfaces far away as ``No module named 'scistackstatsdb.bundle_section'``
or ``cannot import name 'SavedStatsExists'``. This test names the culprit and
the fix instead.

Lives in scistack-gui's suite because scistack-gui depends on every package
it checks.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from test_version_single_owner import REPO, _tracked_pyprojects


def _source_packages() -> list[tuple[str, Path]]:
    """(import name, package dir) for every ``<pkg>/src/<name>/__init__.py``."""
    out = []
    for pyproject in _tracked_pyprojects():
        src = pyproject.parent / "src"
        for init in sorted(src.glob("*/__init__.py")):
            out.append((init.parent.name, pyproject.parent))
    return out


@pytest.mark.parametrize("name,dist_dir", _source_packages(), ids=lambda v: str(v))
def test_package_imports_from_this_checkout(name, dist_dir):
    spec = importlib.util.find_spec(name)
    if spec is None or spec.origin is None:
        pytest.skip(f"{name} is not installed")
    origin = Path(spec.origin).resolve()
    assert origin.is_relative_to(REPO.resolve()), (
        f"{name} imports from {origin}, a copy outside this checkout, so source "
        f"edits are invisible. Reinstall it editable:\n"
        f"  pip install -e {dist_dir.relative_to(REPO)}"
    )
