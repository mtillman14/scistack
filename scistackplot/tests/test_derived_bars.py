"""Derived difference bars: handed in at resolve time, never stored (P6)."""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, render_plotly, resolve
from scistackplot.reduce.diffbars import Derived, DifferenceBar, PanelNote, StatMarks, bars_for_panel, with_derived


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
    merged = with_derived(spec, Derived(bars=derived))
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
    merged = with_derived(spec, Derived(bars=derived))
    assert [b.label for b in merged.difference_bars] == ["**", "mine"]


# --- panel notes, caption, StatMarks (plan-stats-on-figure) ---------------


def test_stat_marks_round_trip_strictly_and_derived_is_never_stored():
    marks = StatMarks(brackets=True, pairs="all", label="effect", hidden=("a1:x-y",), omnibus=True, caption=True)
    spec = _spec(stat_marks=marks)
    assert PlotSpec.from_dict(spec.to_dict()).stat_marks == marks
    merged = with_derived(spec, Derived(panel_notes=(PanelNote(text="F"),), caption="cap"))
    assert merged.derived is not None
    assert "derived" not in spec.to_dict()
    assert PlotSpec.from_dict(merged.to_dict()).derived == merged.derived
    with pytest.raises(ValueError):
        StatMarks.from_dict({"brackets": True, "bogus": 1})
    with pytest.raises(ValueError):
        StatMarks(label="nope")


def test_with_derived_keeps_notes_and_caption_off_the_stored_spec():
    spec = _spec()
    merged = with_derived(spec, Derived(panel_notes=(PanelNote(text="F(2, 9) = 4.1"),), caption="Holm-adjusted"))
    assert spec.derived is None
    assert merged.derived.caption == "Holm-adjusted"
    assert with_derived(spec, Derived()).derived is None


def test_plotly_draws_the_panel_note_and_caption_when_asked():
    note = PanelNote(match={}, text="Group: F(2, 9) = 4.1, p = .03")
    facts = Derived(panel_notes=(note,), caption="Paired t-tests, Holm-adjusted.")
    on = with_derived(_spec(stat_marks=StatMarks(omnibus=True, caption=True)), facts)
    (resolved,) = resolve(on, _table())
    layout = render_plotly(resolved)["layout"]
    names = {a.get("name"): a["text"] for a in layout["annotations"] if a.get("name")}
    assert "F(2, 9)" in names["panel_note"]
    assert "Holm-adjusted" in names["not_plotted_note"]


def test_plotly_draws_nothing_unless_the_figure_asks():
    facts = Derived(panel_notes=(PanelNote(text="F"),), caption="cap")
    (resolved,) = resolve(with_derived(_spec(), facts), _table())
    names = [a.get("name") for a in render_plotly(resolved)["layout"]["annotations"]]
    assert "panel_note" not in names and "not_plotted_note" not in names


def test_matplotlib_draws_the_panel_note_and_caption():
    pytest.importorskip("matplotlib")
    from scistackplot import render_matplotlib

    facts = Derived(panel_notes=(PanelNote(text="omnibus line"),), caption="the caption")
    on = with_derived(_spec(stat_marks=StatMarks(omnibus=True, caption=True)), facts)
    fig = render_matplotlib(resolve(on, _table())[0])
    assert any("the caption" in t.get_text() and t.get_gid() == "not_plotted_note" for t in fig.texts)
    assert any(ax.get_title(loc="left") == "omnibus line" for ax in fig.axes)
