"""Every ``py.scimatlab.bridge.<name>`` the MATLAB sources call resolves.

``bridge.py`` became the ``scimatlab.bridge`` package (P4 stage 9, design
review 05 M2): ``entry``, ``wire``, ``convert``, ``pipeline``. MATLAB still
calls ``py.scimatlab.bridge.<name>``, so the package re-exports each one; a
name a ``.m`` file calls that the package does not export would fail only at
MATLAB run time. Scanned from the sources, the same grep the plan names.
"""

import re
from pathlib import Path

import scimatlab.bridge as bridge

_ROOT = Path(__file__).resolve().parents[1]
_CALL = re.compile(r"py\.scimatlab\.bridge\.([A-Za-z_]+)")


def _called_names() -> set[str]:
    names: set[str] = set()
    for path in [*(_ROOT / "src" / "scimatlab" / "matlab").rglob("*.m"), *(_ROOT / "tests").rglob("*.m")]:
        names.update(_CALL.findall(path.read_text(encoding="utf-8", errors="replace")))
    return names


def test_matlab_calls_are_found():
    assert "for_each_prepare" in _called_names()


def test_every_matlab_call_resolves():
    missing = sorted(n for n in _called_names() if not callable(getattr(bridge, n, None)))
    assert missing == [], f"py.scimatlab.bridge names with no export: {missing}"
