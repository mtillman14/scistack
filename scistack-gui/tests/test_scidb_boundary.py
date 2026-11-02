"""scistack_gui imports only the scidb modules scidb declares as supported.

The list is ``scidb.SUPPORTED_MODULES``; the checker is
``tools/import_layers.py``. Guards against new reach-ins to scidb internals
(design review 04a, B4). Fix a failure by using a supported module; adding
to ``SUPPORTED_MODULES`` needs a reason in review.
"""

import re
import sys
from pathlib import Path

import scidb

REPO = Path(__file__).resolve().parents[2]
# Appended, not prepended: the repo root holds a directory per package, and
# those must not shadow the installed packages.
sys.path.append(str(REPO))

from tools.import_layers import boundary_violations  # noqa: E402


def test_imports_only_supported_scidb_modules() -> None:
    outside = boundary_violations(
        REPO / "scistack-gui",
        "scistack_gui",
        REPO / "scidb" / "src",
        "scidb",
        scidb.SUPPORTED_MODULES,
    )
    assert outside == []


# `db._duck` is scidb's private DuckDB handle. The GUI still reaches it
# directly; lower this when a use is replaced by a scidb API, never raise it.
DUCK_BASELINE = 27


def test_private_duck_handle_uses_do_not_grow() -> None:
    pattern = re.compile(r"\._duck\b")
    count = sum(
        len(pattern.findall(p.read_text(encoding="utf-8")))
        for p in (REPO / "scistack-gui" / "scistack_gui").rglob("*.py")
        if "__pycache__" not in p.parts
    )
    assert count <= DUCK_BASELINE, f"{count} uses of ._duck > baseline {DUCK_BASELINE}"
