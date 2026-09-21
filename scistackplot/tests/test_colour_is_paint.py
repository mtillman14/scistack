"""
Colour is paint (user decision, 2026-09-21).

Tagging a grouping layer as the colour must change NOTHING a reader measures:
not which marks exist, not where they sit, not how tall they are, not what
the ticks and brackets say. It only paints, and adds a legend. Before this,
the coloured layer was pulled out of the tick axis and dodged innermost
(scidb.log 2026-09-21 16:00: colouring `session` on a `subject › session ›
ColName` nesting moved every bar), so a colour click looked like a grouping
change and the figure could not be trusted to draw what it said.

Three things are held here, each on both renderers, for every categorical
kind and every layer a colour could land on:

1. the drawn geometry (``plot_geometry``) is the same with the colour tag on
   and off — the partition, the positions, the numbers;
2. the tick labels and bracket labels are the same;
3. the numbers drawn ARE the numbers an independent pandas computation gives
   for that grouping — the plot draws what it says.

Plus the plan-level statement that ``reduce`` never partitions on colour.
"""

from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("matplotlib")
import matplotlib.pyplot as plt  # noqa: E402

from plot_geometry import (  # noqa: E402
    mpl_bracket_labels,
    mpl_marks,
    mpl_tick_labels,
    on_tick,
    plotly_bracket_labels,
    plotly_marks,
    plotly_tick_labels,
    positions_only,
)
from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.render.base import MARK_OFFSET_GROUP, MARK_SPAN  # noqa: E402
from scistackplot.resolved import COLOR, X  # noqa: E402
from scistackplot.roles import grouping_layers  # noqa: E402
from scistackplot.xaxis import LEAF_SEPARATOR  # noqa: E402

# subject x session x trial, plus `group` — a subject property with no schema
# depth. Unbalanced on purpose, and every value distinct, so a mark in the
# wrong place would carry a number that gives it away.
ROWS = [
    ("01", "A", "pre", "1", 1.0),
    ("01", "A", "pre", "2", 3.0),
    ("01", "A", "post", "1", 10.0),
    ("01", "A", "post", "2", 12.0),
    ("02", "A", "pre", "1", 5.0),
    ("02", "A", "post", "1", 20.0),
    ("03", "B", "pre", "1", 7.0),
    ("03", "B", "pre", "2", 8.0),
    ("03", "B", "post", "1", 30.0),
    ("04", "B", "pre", "1", 9.0),
    ("04", "B", "post", "1", 40.0),
    ("04", "B", "post", "2", 44.0),
]
CATEGORICAL_KINDS = [PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN, PlotKind.SCATTER, PlotKind.STRIP]


@pytest.fixture
def frame() -> pd.DataFrame:
    return pd.DataFrame(ROWS, columns=["subject", "group", "session", "trial", "M"])


def _table(frame, groups) -> LongTable:
    """The table for one grouping list: `group` is a factor only when it
    groups, so an ungrouped `group` never has to be given a role (as a
    depth-less factor it would collapse LAST, after subject, and the bar
    would be a mean of group means — a different figure, not this test's)."""
    factors = ["subject", "session", "trial"] + (["group"] if "group" in groups else [])
    return LongTable.from_frame(
        frame[[*factors, "M"]],
        factors=factors,
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
        level_order={"session": ["pre", "post"], "group": ["A", "B"]},
    )


# The grouping lists under test, innermost first; trial is always collapsed
# so every kind has a sample. `groups` with two entries nests one layer inside
# another; three nests two.
GROUPINGS = [
    ["session", "subject"],
    ["subject", "session"],
    ["session", "group"],
    ["session", "subject", "group"],
]


def _spec(kind, groups, color=None, **kwargs) -> PlotSpec:
    roles = {"trial": Role.COLLAPSE}
    for name in ["subject", "session"]:
        roles[name] = Role.GROUP if name in groups else Role.COLLAPSE
    if "group" in groups:
        roles["group"] = Role.GROUP
    base = dict(
        measures=["M"],
        roles=roles,
        groups=list(groups),
        color=color,
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _one(spec, frame):
    figures = resolve(spec, _table(frame, spec.groups))
    assert len(figures) == 1, [f.title for f in figures]
    return figures[0]


def _mpl(figure):
    drawn = render_matplotlib(figure)
    try:
        return mpl_marks(drawn, figure), mpl_tick_labels(drawn), mpl_bracket_labels(drawn)
    finally:
        plt.close(drawn)


def _plotly(figure):
    payload = render_plotly(figure)
    return plotly_marks(payload, figure), plotly_tick_labels(payload), plotly_bracket_labels(payload)


def _comparable(kind, marks):
    # A strip's jitter is seeded per colour level, so only the tick is stable.
    return on_tick(marks) if kind is PlotKind.STRIP else marks


# --- 1. the geometry does not move --------------------------------------------------


@pytest.mark.parametrize("kind", CATEGORICAL_KINDS + [PlotKind.SPAGHETTI])
@pytest.mark.parametrize("groups", GROUPINGS)
def test_colouring_any_layer_draws_the_same_marks_in_both_backends(frame, kind, groups):
    if kind is PlotKind.SPAGHETTI and len(groups) < 2:
        pytest.skip("a spaghetti needs two layers")
    plain = _one(_spec(kind, groups), frame)
    plain_mpl, plain_ticks, plain_brackets = _mpl(plain)
    plain_plotly, plain_ptick, plain_pbrackets = _plotly(plain)
    assert plain_mpl, "nothing drawn"
    for color in groups:
        coloured = _one(_spec(kind, groups, color=color), frame)
        assert coloured.encoding.color, f"{color} did not become the colour"
        marks, ticks, brackets = _mpl(coloured)
        assert _comparable(kind, marks) == _comparable(kind, plain_mpl), (
            f"matplotlib: colouring {color} moved a mark"
        )
        assert (ticks, brackets) == (plain_ticks, plain_brackets), (
            f"matplotlib: colouring {color} relabelled the axis"
        )
        pmarks, pticks, pbrackets = _plotly(coloured)
        assert _comparable(kind, pmarks) == _comparable(kind, plain_plotly), (
            f"plotly: colouring {color} moved a mark"
        )
        assert (pticks, pbrackets) == (plain_ptick, plain_pbrackets), (
            f"plotly: colouring {color} relabelled the axis"
        )


@pytest.mark.parametrize("kind", [PlotKind.BAR, PlotKind.SCATTER, PlotKind.SPAGHETTI])
@pytest.mark.parametrize("groups", GROUPINGS)
@pytest.mark.parametrize("color", [None, 0, -1])
def test_the_two_backends_draw_the_same_marks(frame, kind, groups, color):
    """Cross-backend parity, coloured or not: the preview (plotly) and the
    export (matplotlib) place every mark at the same tick with the same y."""
    if kind is PlotKind.SPAGHETTI and len(groups) < 2:
        pytest.skip("a spaghetti needs two layers")
    figure = _one(_spec(kind, groups, color=None if color is None else groups[color]), frame)
    marks, ticks, brackets = _mpl(figure)
    pmarks, pticks, pbrackets = _plotly(figure)
    assert positions_only(marks) == positions_only(pmarks)
    if kind is PlotKind.BAR:
        assert [(m.x, m.low, m.high) for m in marks] == [
            (m.x, m.low, m.high) for m in pmarks
        ], "error bars differ between the backends"
    assert ticks == pticks
    assert brackets == pbrackets


def test_bars_sit_on_their_tick_at_full_width_whatever_is_coloured(frame):
    """No dodge: a coloured bar is ON its tick and MARK_SPAN wide, in both
    backends (plotly says so with one shared offsetgroup)."""
    for color in [None, "session", "subject"]:
        figure = _one(_spec(PlotKind.BAR, ["session", "subject"], color=color), frame)
        marks, _, _ = _mpl(figure)
        assert {m.offset for m in marks} == {0.0}
        assert {m.width for m in marks} == {MARK_SPAN}
        payload = render_plotly(figure)
        bars = [t for t in payload["data"] if t["type"] == "bar"]
        assert bars and {t["offsetgroup"] for t in bars} == {MARK_OFFSET_GROUP}
        assert payload["layout"]["barmode"] == "group"


def test_boxes_and_violins_share_the_offsetgroup_too(frame):
    for kind in (PlotKind.BOX, PlotKind.VIOLIN):
        figure = _one(_spec(kind, ["session", "subject"], color="session"), frame)
        payload = render_plotly(figure)
        traces = [t for t in payload["data"] if t["type"] in ("box", "violin")]
        assert traces and {t["offsetgroup"] for t in traces} == {MARK_OFFSET_GROUP}


# --- 2. the colour is where the list put it --------------------------------------


def test_the_coloured_layer_keeps_its_nesting_position(frame):
    """`[session, subject, group]` with session coloured: session is STILL the
    innermost tick (inside subject, inside group) — not pulled out and dodged.
    The composed leaf keys say so."""
    figure = _one(_spec(PlotKind.BAR, ["session", "subject", "group"], color="session"), frame)
    layers = grouping_layers(figure.spec, _table(frame, figure.spec.groups))
    assert layers.ticks == ["group", "subject", "session"]
    leaves = [str(k) for k in figure.x_order if LEAF_SEPARATOR in str(k)]
    assert leaves and all(k.split(LEAF_SEPARATOR)[-1] in ("pre", "post") for k in leaves)
    frame = figure.panels[0].frame
    assert set(frame[COLOR]) == {"pre", "post"}
    # And the legend lists the colour levels.
    payload = render_plotly(figure)
    assert {t["name"] for t in payload["data"] if t["type"] == "bar"} == {"pre", "post"}


def test_reduce_never_partitions_on_colour(frame):
    """The plan-level statement: the panel frame with the colour on is the
    plain frame plus a `__color` column that is a FUNCTION of `__x`."""
    for groups in GROUPINGS:
        plain = _one(_spec(PlotKind.BAR, groups), frame).panels[0].frame
        for color in groups:
            coloured = _one(_spec(PlotKind.BAR, groups, color=color), frame).panels[0].frame
            assert COLOR in coloured.columns and COLOR not in plain.columns
            keep = [c for c in plain.columns]
            pd.testing.assert_frame_equal(
                coloured[keep].sort_values(X).reset_index(drop=True),
                plain[keep].sort_values(X).reset_index(drop=True),
            )
            per_x = coloured.groupby(X)[COLOR].nunique()
            assert (per_x == 1).all(), "a tick with two colours means colour split it"


# --- 3. the numbers are the numbers ---------------------------------------------


def _expected_bar_heights(frame, groups) -> dict[str, float]:
    """The nested chain by hand: trial averaged within the grouping layers
    (plus every collapsed key above it), then each remaining collapsed key
    in turn, deepest first — `grouping-and-collapse.md`'s worked example
    generalised. Keyed by the composed leaf (outermost first)."""
    ticks = list(reversed(groups))
    factors = ["subject", "session", "trial"] + (["group"] if "group" in groups else [])
    # Deepest first: trial (3), session (2), subject (1). Each step averages
    # the key away within every factor still present.
    working = frame[[*factors, "M"]].copy()
    for key in [n for n in ["trial", "session", "subject"] if n not in groups]:
        factors = [c for c in factors if c != key]
        working = working.groupby(factors, as_index=False)["M"].mean()
    out = working.groupby(ticks)["M"].mean()
    return {
        LEAF_SEPARATOR.join(str(v) for v in (key if isinstance(key, tuple) else (key,))): float(v)
        for key, v in out.items()
    }


@pytest.mark.parametrize("groups", GROUPINGS)
@pytest.mark.parametrize("color", [None, 0])
def test_drawn_bar_heights_are_the_independent_means(frame, groups, color):
    figure = _one(_spec(PlotKind.BAR, groups, color=None if color is None else groups[color]), frame)
    marks, _, _ = _mpl(figure)
    expected = _expected_bar_heights(frame, groups)
    drawn = {m.x: m.y for m in marks}
    assert set(drawn) == set(expected)
    for key, value in expected.items():
        assert drawn[key] == pytest.approx(value), key
