"""
Difference bars, Stage 4 (2026-09-27): the plotly preview draws what the
export placed (``layout_decisions()["difference_bars"]``) and never places
its own, except undecided (library callers), where it places on its own
frame's estimated panel heights. ``layout.meta.difference_bars`` is what
Plot Studio picks from (``diffbars.difference_meta``).
"""

from __future__ import annotations

import logging
import math

import matplotlib

matplotlib.use("Agg")

import pandas as pd  # noqa: E402
import pytest  # noqa: E402

import scistackplot.reduce as reduce_mod  # noqa: E402
from scistackplot import (  # noqa: E402
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    YAxis,
    layout_decisions,
    render_plotly,
    resolve_one,
)
from scistackplot.diffbars import DifferenceBar, target_bounds, slot_endpoints  # noqa: E402
from scistackplot.render.plotly_ import DIFF_BAR_TAG, DIFF_LABEL_TAG  # noqa: E402

LAYER = "scistackplot"


@pytest.fixture(autouse=True)
def _clear_plan_cache():
    reduce_mod.clear_plan_cache()
    yield
    reduce_mod.clear_plan_cache()


@pytest.fixture
def table() -> LongTable:
    rows = []
    for group, subjects in (("sham", ("01", "02", "03")), ("stim", ("04", "05", "06"))):
        for subject in subjects:
            for side in ("L", "R"):
                for number, session in enumerate(("pre", "post", "follow")):
                    rows.append(
                        {
                            "group": group,
                            "subject": subject,
                            "side": side,
                            "session": session,
                            "Step": 2.0 + number + int(subject) * 0.1,
                        }
                    )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["group", "subject", "side", "session"],
        measures=["Step"],
        name="Step",
        schema_levels=["subject", "side", "session"],
        level_order={"session": ["pre", "post", "follow"], "group": ["sham", "stim"]},
    )


def _bar(side: str, a: str, b: str, label: str = "*", group: str = "sham") -> DifferenceBar:
    return DifferenceBar(
        match={"group": group, "side": side}, a={"session": a}, b={"session": b}, label=label
    )


BARS = (_bar("R", "pre", "post"), _bar("R", "post", "follow", "**"), _bar("R", "pre", "follow"))


def _spec(*bars: DifferenceBar, kind: PlotKind = PlotKind.BAR, **extra) -> PlotSpec:
    return PlotSpec(
        measures=["Step"],
        kind=kind,
        roles={
            "group": Role.ITERATE,
            "side": Role.FACET,
            "session": Role.GROUP,
            "subject": Role.COLLAPSE,
        },
        groups=["session"],
        difference_bars=list(bars),
        style=extra.pop("style", StyleOptions(width=6.0, height=4.0)),
        **extra,
    )


def _sham(spec: PlotSpec, table: LongTable):
    figure, _, _ = resolve_one(spec, table, 0)
    return figure


def _decided(figure):
    decisions = layout_decisions(figure)
    return decisions, render_plotly(figure, decisions=decisions)


def _named(items, tag: str) -> list[dict]:
    return [item for item in items if str(item.get("name", "")).startswith(tag + ":")]


def _panel_meta(payload, side: str) -> dict:
    return next(
        p for p in payload["layout"]["meta"]["difference_bars"]["panels"] if p["match"]["side"] == side
    )


def _axis_key(ref: str, letter: str) -> str:
    return f"{letter}axis{ref[1:]}"


# --- decided: the export's placement, drawn ----------------------------------


def test_the_preview_draws_the_exports_placement(table):
    figure = _sham(_spec(*BARS), table)
    decisions, payload = _decided(figure)
    placement = decisions["difference_bars"]
    index = next(i for i, p in enumerate(figure.panels) if p.key["side"] == "R")
    layout = payload["layout"]
    shapes = _named(layout.get("shapes", []), DIFF_BAR_TAG)
    labels = _named(layout["annotations"], DIFF_LABEL_TAG)
    assert len(shapes) == len(labels) == len(BARS) == len(placement.bars[index])
    for number, bar in enumerate(placement.bars[index]):
        shape = next(s for s in shapes if s["name"] == f"{DIFF_BAR_TAG}:{index}:{number}")
        assert shape["path"] == (
            f"M {bar.left},{bar.left_foot} L {bar.left},{bar.y} "
            f"L {bar.right},{bar.y} L {bar.right},{bar.right_foot}"
        )
        label = next(a for a in labels if a["name"] == f"{DIFF_LABEL_TAG}:{index}:{number}")
        assert (label["x"], label["y"], label["text"]) == (bar.middle, bar.label_y, bar.bar.label)
        assert label["yanchor"] == "bottom" and label["xanchor"] == "center"


def test_every_panel_draws_the_placed_range(table):
    figure = _sham(_spec(*BARS), table)
    decisions, payload = _decided(figure)
    layout = payload["layout"]
    for panel in payload["layout"]["meta"]["difference_bars"]["panels"]:
        axis = layout[_axis_key(panel["yaxis"], "y")]
        assert axis["range"] == pytest.approx(
            list(decisions["difference_bars"].limits[panel["index"]])
        )
        assert panel["y_limits"] == axis["range"]
    assert axis["range"][1] > figure.y_limits[1]


def test_bars_are_drawn_in_their_own_panels_axes(table):
    figure = _sham(_spec(*BARS), table)
    _, payload = _decided(figure)
    right = _panel_meta(payload, "R")
    for shape in _named(payload["layout"]["shapes"], DIFF_BAR_TAG):
        assert (shape["xref"], shape["yref"]) == (right["xaxis"], right["yaxis"])


def test_a_log_axis_places_shapes_in_log_units(table):
    figure = _sham(_spec(*BARS, style=StyleOptions(width=6.0, height=4.0, log_y=True)), table)
    decisions, payload = _decided(figure)
    index = next(i for i, p in enumerate(figure.panels) if p.key["side"] == "R")
    bar = decisions["difference_bars"].bars[index][0]
    label = next(
        a for a in payload["layout"]["annotations"] if a.get("name") == f"{DIFF_LABEL_TAG}:{index}:0"
    )
    assert label["y"] == pytest.approx(math.log10(bar.label_y))
    shape = next(s for s in payload["layout"]["shapes"] if s.get("name") == f"{DIFF_BAR_TAG}:{index}:0")
    assert f"{math.log10(bar.y)}" in shape["path"]


def test_a_label_is_text_never_markup(table):
    figure = _sham(_spec(_bar("R", "pre", "post", "p < 0.05 & more")), table)
    _, payload = _decided(figure)
    (label,) = _named(payload["layout"]["annotations"], DIFF_LABEL_TAG)
    assert label["text"] == "p &lt; 0.05 &amp; more"


def test_a_typed_max_lists_the_unfit_bars(table):
    figure = _sham(_spec(*BARS, y_axis=YAxis(maximum=4.8)), table)
    _, payload = _decided(figure)
    right = _panel_meta(payload, "R")
    assert right["unfit"]
    assert len(right["bars"]) + len(right["unfit"]) == len(BARS)


# --- undecided: placed on the preview's own frame -----------------------------


def test_undecided_the_preview_places_on_estimated_panels(table, caplog):
    figure = _sham(_spec(*BARS), table)
    with caplog.at_level(logging.DEBUG, logger=LAYER):
        payload = render_plotly(figure)
    meta = payload["layout"]["meta"]["difference_bars"]
    assert meta["estimated"] is True
    assert len(_named(payload["layout"]["shapes"], DIFF_BAR_TAG)) == len(BARS)
    assert _panel_meta(payload, "R")["y_limits"][1] > figure.y_limits[1]
    assert "difference bars (undecided)" in caplog.text


def test_decided_meta_says_measured(table):
    _, payload = _decided(_sham(_spec(*BARS), table))
    assert payload["layout"]["meta"]["difference_bars"]["estimated"] is False


# --- meta: what Plot Studio picks from ----------------------------------------


def test_meta_offers_every_tick_of_every_panel_with_its_bounds(table):
    figure = _sham(_spec(), table)
    _, payload = _decided(figure)
    meta = payload["layout"]["meta"]["difference_bars"]
    assert len(meta["panels"]) == len(figure.panels)
    for panel in meta["panels"]:
        assert panel["match"]["group"] == "sham"
        assert _axis_key(panel["xaxis"], "x") in payload["layout"]
        assert [t["values"] for t in panel["targets"]] == [
            {"session": "pre"},
            {"session": "post"},
            {"session": "follow"},
        ]
        assert [(t["x0"], t["x1"]) for t in panel["targets"]] == [
            (-0.5, 0.5),
            (0.5, 1.5),
            (1.5, 2.5),
        ]
        assert [t["label"] for t in panel["targets"]] == ["pre", "post", "follow"]
        assert panel["bars"] == []


def test_every_point_drawn_falls_inside_a_ticks_bounds(table):
    """With the overlay the axis is positional: every x drawn in the panel —
    bars and sample points at their offsets — lies inside some target."""
    figure = _sham(_spec(show_sample=["subject"]), table)
    _, payload = _decided(figure)
    right = _panel_meta(payload, "R")
    xs = [
        x
        for trace in payload["data"]
        if trace.get("xaxis", "x") == right["xaxis"]
        for x in trace.get("x", [])
        if isinstance(x, (int, float))
    ]
    assert len(xs) > len(right["targets"])  # the sample points are in there
    for x in xs:
        assert any(t["x0"] <= x <= t["x1"] for t in right["targets"]), x


def test_bounds_split_uneven_positions_half_way():
    from scistackplot.diffbars import Endpoint

    ends = [
        Endpoint(0, 0.0, {"x": "a"}, "a"),
        Endpoint(1, 1.0, {"x": "b"}, "b"),
        Endpoint(3, 3.0, {"x": "c"}, "c"),
    ]
    assert target_bounds(ends) == {0: (-0.5, 0.5), 1: (0.5, 1.5), 3: (2.5, 3.5)}


def test_meta_lists_the_bars_of_other_figures(table):
    figure = _sham(_spec(_bar("R", "pre", "post", group="stim")), table)
    _, payload = _decided(figure)
    meta = payload["layout"]["meta"]["difference_bars"]
    assert [b["match"] for b in meta["not_in_figure"]] == [{"group": "stim", "side": "R"}]


def test_meta_explains_an_unresolved_bar(table):
    figure = _sham(_spec(_bar("R", "pre", "later")), table)
    _, payload = _decided(figure)
    (item,) = _panel_meta(payload, "R")["unresolved"]
    assert "'later' is not a tick of this figure" in item["reason"]


def test_only_figures_that_can_carry_bars_get_meta(table):
    from dataclasses import replace

    from scistackplot.diffbars import carries_difference_bars

    figure = _sham(_spec(kind=PlotKind.STRIP), table)
    assert carries_difference_bars(figure)
    assert render_plotly(figure)["layout"]["meta"]["difference_bars"] is not None
    assert not carries_difference_bars(replace(figure, kind=PlotKind.LINE))
    assert not carries_difference_bars(
        replace(figure, spec=replace(figure.spec, x_measure="Step"))
    )


def test_target_positions_match_the_tick_positions(table):
    figure = _sham(_spec(), table)
    ends = slot_endpoints(figure)
    _, payload = _decided(figure)
    right = _panel_meta(payload, "R")
    assert [t["position"] for t in right["targets"]] == [e.position for e in ends]
