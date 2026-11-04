"""Every scidb module imports first, in a fresh interpreter.

``import scidb.X`` for every X, in isolation. A cycle that only bites in one
import order (``foreach`` before ``database`` but not after) is invisible to a
suite whose ``conftest`` has already imported everything; a subprocess per
module is the only honest probe.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

PKG_DIR = Path(__import__("scidb").__file__).parent
PKG = "scidb"


def _module_names() -> list[str]:
    names = []
    for p in sorted(PKG_DIR.rglob("*.py")):
        rel = p.relative_to(PKG_DIR).with_suffix("")
        parts = list(rel.parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        if "__pycache__" in parts:
            continue
        names.append(".".join([PKG, *parts]) if parts else PKG)
    return names


MODULES = _module_names()


@pytest.mark.parametrize("module", MODULES)
def test_every_module_imports_first(module):
    """``import scidb.<module>`` succeeds in a fresh interpreter, before any
    other scidb module has been imported."""
    proc = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, (
        f"import {module} failed in a fresh interpreter:\n{proc.stderr[-3000:]}"
    )
