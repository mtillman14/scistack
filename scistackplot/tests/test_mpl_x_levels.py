"""The matplotlib box/violin fallback order when a plot carries no x_order.

It used ``sorted(..., key=str)`` — lexicographic, so unpadded IDs drew
``1, 10, 2``. Every other ordering fallback in the package is the natural
sort; this one now is too.
"""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from scistackplot.render.mpl import _x_levels


def _resolved(x_order):
    return SimpleNamespace(x_order=x_order, encoding=SimpleNamespace(x="subject"))


def test_a_stated_order_wins():
    frame = pd.DataFrame({"subject": ["02", "01"]})
    assert _x_levels(frame, _resolved(["02", "01"])) == ["02", "01"]


def test_without_one_numbers_sort_naturally():
    frame = pd.DataFrame({"subject": ["10", "2", "1", None]})
    assert _x_levels(frame, _resolved(None)) == ["1", "2", "10"]


def test_embedded_numbers_sort_naturally():
    frame = pd.DataFrame({"subject": ["S10", "S2", "S1"]})
    assert _x_levels(frame, _resolved(None)) == ["S1", "S2", "S10"]
