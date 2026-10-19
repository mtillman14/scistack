"""Derived difference bars: handed in at resolve time, never stored (P6)."""

from __future__ import annotations

import pandas as pd

from scistackplot import LongTable, PlotKind, PlotSpec, Role, resolve
from scistackplot.diffbars import DifferenceBar, bars_for_panel, with_derived_bars


def _table() -> LongTable:
    rows = [{"subject": f"S{s}", "brand": b, "M": float(s + i)} for s in range(4) for i, b in enumerate("xyz")]
    return LongTable.from_frame(pd.DataFrame(rows), factors=["subject", "brand"], measures=["M"], name="M",
                                schema_levels=["subject"])


def _spec(**kw) -> PlotSpec:
    return PlotSpec(measures=["M"], roles={"brand": Role.GROUP, "subject": Role.COLLAPSE}, groups=["brand"],
                    kind=PlotKind.BAR, **kw)


def test_origin_round_trips_and_manual_is_not_written():
    manual = DifferenceBar(a={"brand": "x"}, b={"brand": "y"})
    assert "origin" not in manual.to_dict()
    test = DifferenceBar(a={"brand": "x"}, b={"brand": "y"}, origin="test")
    assert DifferenceBar.from_dict(test.to_dict()) == test
    assert DifferenceBar.from_dict(manual.to_dict()).origin == "manual"


def test_derived_bars_draw_and_the_stored_spec_is_untouched():
    spec = _spec()
    derived = [DifferenceBar(a={"brand": "x"}, b={"brand": "z"}, label="**", origin="test")]
    merged = with_derived_bars(spec, derived)
    assert spec.difference_bars == []
    (resolved,) = resolve(merged, _table())
    drawn, unresolved = bars_for_panel(resolved, resolved.panels[0])
    assert unresolved == []
    assert [d.bar.origin for d in drawn] == ["test"]


def test_a_manual_bar_wins_over_a_derived_duplicate():
    manual = DifferenceBar(a={"brand": "y"}, b={"brand": "x"}, label="mine")
    spec = _spec(difference_bars=[manual])
    derived = [DifferenceBar(a={"brand": "x"}, b={"brand": "y"}, label="*", origin="test"),
               DifferenceBar(a={"brand": "x"}, b={"brand": "z"}, label="**", origin="test")]
    merged = with_derived_bars(spec, derived)
    assert [b.label for b in merged.difference_bars] == ["**", "mine"]
