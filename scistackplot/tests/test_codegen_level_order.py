"""The exported seaborn code states every level order it depends on.

seaborn orders an unstated ``order`` / ``hue_order`` / ``col_order`` /
``row_order`` by first appearance in ``df`` — the database's row order. The
preview orders them by the table's factor levels (the project's declared
``[schema_keys]`` first), so an export that left them unsaid drew
``post, pre`` beside a preview drawing ``pre, post``.

The frame below is deliberately built in the WRONG order (post before pre,
sham before stim), so a test can only pass if the order was stated.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import PlotKind, PlotSpec, Role, generate_plot_function, resolve
from scistackplot.table import LongTable

SESSIONS = ["pre", "post"]
GROUPS = ["stim", "sham"]


@pytest.fixture
def table() -> LongTable:
    rows = [
        {"group": group, "session": session, "subject": subject, "StepLength": 1.0 + i}
        for i, (group, session, subject) in enumerate(
            (g, s, p)
            for g in reversed(GROUPS)
            for s in reversed(SESSIONS)
            for p in ["01", "02"]
        )
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["group", "session", "subject"],
        measures=["StepLength"],
        level_order={"group": GROUPS, "session": SESSIONS},
        schema_levels=["subject"],
    )


# The composed nested keys of a `group` inside `session` axis (the spec list
# is innermost first), outermost first, in declared order.
NESTED = [f"{s} · {g}" for s in SESSIONS for g in GROUPS]


def _coloured_bar() -> PlotSpec:
    # group ticks inside session brackets, group coloured. Colour is paint
    # (2026-09-21): group stays a tick layer, so the x is NESTED and its order
    # is the composed one (`_nested_x_args`), while `hue_order` still names
    # the group levels for the palette and legend.
    return PlotSpec(
        measures=["StepLength"],
        roles={"group": Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["group", "session"],
        color="group",
        kind=PlotKind.BAR,
    )


def _faceted_box() -> PlotSpec:
    return PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.FACET, "group": Role.FACET, "subject": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )


def _run(source: str, table: LongTable):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    [function] = [v for k, v in namespace.items() if k.startswith("plot_")]
    return function(table.frame.copy(), "figure.png")


def test_x_and_hue_orders_are_stated(table):
    source = generate_plot_function(_coloured_bar(), table)
    assert f"order={NESTED!r}" in source, "the nested x order, composed, declared first"
    assert "hue_order=_hue_order" in source
    assert "['stim', 'sham']" in source
    assert "dodge=False" in source


def test_a_flat_x_order_is_stated(table):
    """One tick layer: the declared session order goes on the call as
    `order=_x_order`, computed from the frame so a level the figure never
    draws is not listed."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"group": Role.FACET, "session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
    )
    source = generate_plot_function(spec, table)
    assert "order=_x_order" in source
    assert "['pre', 'post']" in source


def test_the_preview_agrees(table):
    """The reference the export is checked against: the same composed leaves
    in the same order (the preview also holds spacers between groups, which
    seaborn cannot draw — see `_nested_x_args`)."""
    from scistackplot.xaxis import LEAF_SEPARATOR, is_spacer

    figure = resolve(_coloured_bar(), table)[0]
    leaves = [str(v).replace(LEAF_SEPARATOR, " · ") for v in figure.x_order if not is_spacer(str(v))]
    assert leaves == NESTED
    assert [str(v) for v in figure.color_order] == GROUPS


def test_the_exported_figure_draws_in_the_declared_order(table):
    pytest.importorskip("seaborn")
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")

    figure = _run(generate_plot_function(_coloured_bar(), table), table)
    ax = figure.axes[0]
    assert [t.get_text() for t in ax.get_xticklabels()] == NESTED
    legend = ax.get_legend() or figure.legends[0]
    assert [t.get_text() for t in legend.get_texts()] == GROUPS
    matplotlib.pyplot.close(figure)


def test_facet_orders_are_stated(table):
    source = generate_plot_function(_faceted_box(), table)
    assert "col_order=_col_order" in source
    assert "row_order=_row_order" in source


def test_the_exported_facets_follow_the_declared_order(table):
    pytest.importorskip("seaborn")
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")

    spec = _faceted_box()
    source = generate_plot_function(spec, table)
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    [function] = [v for k, v in namespace.items() if k.startswith("plot_")]
    figure = function(table.frame.copy(), "figure.png")
    # Panel titles are the facet values on each y axis (row, col reversed to
    # col · row by the export), read in the grid's reading order.
    labels = [ax.get_ylabel() for ax in figure.axes if ax.get_ylabel()]
    assert labels, "every panel is named on its y axis"
    # Whichever factor ended up on columns and which on rows, each must read
    # in its declared order — the frame lists both backwards.
    cols = list(dict.fromkeys(label.split(" · ")[0] for label in labels))
    rows = list(dict.fromkeys(label.split(" · ")[1] for label in labels))
    assert cols in (SESSIONS, GROUPS)
    assert rows in (SESSIONS, GROUPS)
    assert cols != rows
    matplotlib.pyplot.close(figure)


def test_only_present_levels_are_ordered(table):
    """Under ITERATE each figure sees a subset; the helper keeps what the
    frame holds instead of reserving a slot for an absent level."""
    import textwrap

    source = generate_plot_function(_coloured_bar(), table)
    # The helper is emitted INSIDE the function; lift its five lines out.
    lines = source.splitlines()
    start = next(i for i, line in enumerate(lines) if "def _in_order(" in line)
    body = textwrap.dedent("\n".join(lines[start : start + 5]))
    scope: dict = {}
    exec(body, scope)  # noqa: S102
    values = pd.Series(["post", "post", None, "extra"])
    assert scope["_in_order"](values, SESSIONS) == ["post", "extra"]
