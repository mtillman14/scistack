"""scistackplotdb imports only the scidb modules scidb declares as supported.

The list is ``scidb.SUPPORTED_MODULES``; the checker is
``tools/import_layers.py``. Guards against new reach-ins to scidb internals
(design review 04a, B4). Fix a failure by using a supported module; adding
to ``SUPPORTED_MODULES`` needs a reason in review.
"""

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
        REPO / "scistackplotdb/src", "scistackplotdb", REPO / "scidb" / "src", "scidb",
        scidb.SUPPORTED_MODULES,
    )
    assert outside == []
