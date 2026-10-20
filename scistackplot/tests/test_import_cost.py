"""What ``import scistackplot`` loads — the cost of every Plot Studio open.

The GUI's first ``plot_describe`` imports this package. On Windows that import
took 4 s (2026-10-20), 2 s of it ``scipy.stats``, pulled in at module level by
``csvstats.descriptive`` for the CI95 t quantile. csv-stats 0.4.2 imports scipy
inside the functions that need it, so the cost moves to the first figure that
draws an interval. matplotlib is likewise loaded only to render.
"""

from __future__ import annotations

import subprocess
import sys


def test_importing_scistackplot_loads_neither_scipy_nor_matplotlib():
    code = (
        "import sys, scistackplot\n"
        "heavy = {'scipy', 'matplotlib'}\n"
        "print(sorted(heavy & {m.split('.')[0] for m in sys.modules}))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "[]", out.stdout
