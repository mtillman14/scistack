"""Which shapes can be plotted" has ONE owner: ``shape.is_plottable``.

It was written out four times (the catalog, ``LongTable.describe``, the GUI's
eligibility gate, the owner) until 2026-11-01, and agreed only by accident —
adding text measures would have had to land in all four or the catalog would
list a variable the panel then refused (CLAUDE.md NOTE 4,
docs/claude/categorical-measures.md).
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from scistackplot import LongTable, Shape, is_plottable
from scistackplot.shape import PLOTTABLE_SHAPES

ROOT = Path(__file__).resolve().parents[2]
SOURCES = [
    ROOT / "scistackplot" / "src",
    ROOT / "scistackplotdb" / "src",
    ROOT / "scistack-gui" / "scistack_gui",
]

#: A shape tuple spelled out where the owner should have been asked.
INLINE_RULE = re.compile(
    r"Shape\.SCALAR,\s*Shape\.SERIES_1D,\s*Shape\.MATRIX_2D\s*\)", re.MULTILINE
)


def test_no_module_restates_the_plottable_rule():
    offenders = []
    for root in SOURCES:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if path.name == "shape.py":
                continue
            if INLINE_RULE.search(path.read_text(encoding="utf-8")):
                offenders.append(str(path.relative_to(ROOT)))
    assert offenders == [], f"ask shape.is_plottable instead: {offenders}"


def test_every_shape_but_unknown_is_plottable():
    assert {shape for shape in Shape if is_plottable(shape)} == set(PLOTTABLE_SHAPES)
    assert is_plottable(Shape.CATEGORICAL)
    assert not is_plottable(Shape.UNKNOWN)


def test_the_table_description_asks_the_owner():
    frame = pd.DataFrame({"subject": ["1", "2"], "Outcome": ["fall", "ok"], "Speed": [1.0, 2.0]})
    table = LongTable.from_frame(frame, factors=["subject"], measures=["Outcome", "Speed"])
    described = {m["name"]: m for m in table.describe()["measures"]}
    for name, entry in described.items():
        assert entry["plottable"] is is_plottable(table.shape_of(name))
    assert described["Outcome"]["levels"] == ["fall", "ok"]
