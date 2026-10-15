"""No module defines the same top-level name twice.

Found 2026-10-15: ``render/mpl.py`` carried two ``_apply_legend`` functions
(and two ``LEGEND_PAD``\\ s). Python keeps the LATER one, so the earlier copy
was dead code that still read as live — a legend-order fix made to it changed
nothing, and its own ``_legend_width_fraction`` had lost its ``return``
without anything noticing. Two owners of one concept in one file.
"""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

import scistackplot

PACKAGE = Path(scistackplot.__file__).parent


def _top_level_names(tree: ast.Module) -> list[str]:
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            # Constants only: a lower-case module variable may legitimately be
            # rebound (a try/except import fallback, a registry being built).
            names.extend(
                t.id for t in targets if isinstance(t, ast.Name) and t.id.isupper()
            )
    return names


def test_no_module_defines_a_top_level_name_twice():
    duplicates = {}
    for path in sorted(PACKAGE.rglob("*.py")):
        counts = Counter(_top_level_names(ast.parse(path.read_text(encoding="utf-8"))))
        repeated = sorted(name for name, n in counts.items() if n > 1)
        if repeated:
            duplicates[str(path.relative_to(PACKAGE))] = repeated
    assert not duplicates, f"shadowed top-level definitions: {duplicates}"
