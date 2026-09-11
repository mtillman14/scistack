"""
Nested x grouping: several factors sharing one categorical axis.

"Stim and sham side by side, each split by session" — up to three layers. The
composition is a pure, label-only function (:func:`scistackplot.xaxis.plan_x_axis`),
which is what lets both renderers draw the same brackets and codegen emit the
resolved order instead of replaying the rules.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, RoleError, resolve
from scistackplot.roles import validate
from scistackplot.xaxis import is_spacer, leaf_key, plan_x_axis


# --- the composition, with no frames at all --------------------------------


def test_two_layers_nest_leaves_under_groups():
    plan = plan_x_axis(
        [("stim", "pre"), ("stim", "post"), ("sham", "pre"), ("sham", "post")],
        [["stim", "sham"], ["pre", "post"]],
    )

    leaves = [key for key in plan.order if not is_spacer(key)]
    assert leaves == [
        leaf_key(("stim", "pre")),
        leaf_key(("stim", "post")),
        leaf_key(("sham", "pre")),
        leaf_key(("sham", "post")),
    ]
    assert [(g.label, g.start, g.end) for g in plan.groups] == [
        ("stim", 0, 1),
        ("sham", 3, 4),
    ]


def test_ticks_show_the_innermost_layer_only():
    """The layers above become brackets; repeating them per tick is noise."""
    plan = plan_x_axis(
        [("stim", "pre"), ("stim", "post")], [["stim", "sham"], ["pre", "post"]]
    )

    assert [t for t in plan.tick_labels if t] == ["pre", "post"]


def test_a_gap_separates_groups():
    plan = plan_x_axis(
        [("stim", "pre"), ("sham", "pre")], [["stim", "sham"], ["pre"]]
    )

    assert sum(is_spacer(key) for key in plan.order) == 1
    # The spacer carries no tick.
    spacer_at = next(i for i, key in enumerate(plan.order) if is_spacer(key))
    assert plan.tick_labels[spacer_at] == ""


def test_declared_order_wins_over_observed():
    """Zero-padded IDs and a legend that does not reshuffle: the standing rule."""
    plan = plan_x_axis(
        [("10", "a"), ("02", "a"), ("01", "a")],
        [["01", "02", "10"], ["a"]],
    )

    assert [g.label for g in plan.groups] == ["01", "02", "10"]


def test_three_layers_give_wider_gaps_at_outer_boundaries():
    """A change of outer group must read as a bigger break than an inner one."""
    combos = [
        (outer, middle, "x")
        for outer in ["A", "B"]
        for middle in ["p", "q"]
    ]
    plan = plan_x_axis(combos, [["A", "B"], ["p", "q"], ["x"]])

    # Between p and q inside A: one spacer. Between A and B: two.
    runs, run = [], 0
    for key in plan.order:
        if is_spacer(key):
            run += 1
        elif run:
            runs.append(run)
            run = 0
    assert runs == [1, 2, 1]
    assert plan.depth == 2


def test_only_observed_combinations_get_a_position():
    """Real designs are ragged; a reserved-but-empty position reads as missing
    data rather than as a combination nobody ran."""
    plan = plan_x_axis(
        [("stim", "pre"), ("stim", "post"), ("sham", "pre")],
        [["stim", "sham"], ["pre", "post"]],
    )

    assert sum(not is_spacer(key) for key in plan.order) == 3
    assert [(g.label, g.start, g.end) for g in plan.groups] == [
        ("stim", 0, 1),
        ("sham", 3, 3),
    ]


def test_an_unknown_level_is_never_dropped():
    plan = plan_x_axis([("stim", "pre"), ("mystery", "pre")], [["stim"], ["pre"]])

    assert [g.label for g in plan.groups] == ["stim", "mystery"]


def test_no_combinations_is_an_empty_plan():
    assert plan_x_axis([], [["a"], ["b"]]).order == []


# --- through resolve -------------------------------------------------------


@pytest.fixture
def grouped_table() -> LongTable:
    rows = [
        {
            "group": group,
            "session": session,
            "subject": subject,
            "StepLength": 1.0 + index * 0.1,
        }
        for index, (group, session, subject) in enumerate(
            (g, s, p)
            for g in ["stim", "sham"]
            for s in ["pre", "post"]
            for p in ["01", "02"]
        )
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["group", "session", "subject"],
        measures=["StepLength"],
        level_order={"group": ["stim", "sham"], "session": ["pre", "post"]},
        schema_levels=["subject"],
    )


def _nested_spec(**kwargs) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"group": Role.X, "session": Role.X, "subject": Role.FREE},
        x_layers=["group", "session"],
        kind=PlotKind.BOX,
    )
    base.update(kwargs)
    return PlotSpec(**base)


def test_resolve_composes_the_axis(grouped_table):
    figure = resolve(_nested_spec(), grouped_table)[0]

    assert figure.x_plan is not None
    assert [g.label for g in figure.x_plan.groups] == ["stim", "sham"]
    # x_order mirrors the plan so every existing consumer keeps working.
    assert figure.x_order == figure.x_plan.order


def test_the_outer_layer_is_the_declared_order_not_the_role_order(grouped_table):
    """Membership is roles; ORDER is x_layers. Swapping the list swaps the axis
    without touching a single role."""
    figure = resolve(_nested_spec(x_layers=["session", "group"]), grouped_table)[0]

    assert [g.label for g in figure.x_plan.groups] == ["pre", "post"]


def test_an_x_holder_missing_from_the_order_is_appended(grouped_table):
    """Neither control can put the spec in a state the other rejects."""
    figure = resolve(_nested_spec(x_layers=["group"]), grouped_table)[0]

    assert [g.label for g in figure.x_plan.groups] == ["stim", "sham"]
    assert figure.x_plan.n_layers == 2


def test_a_stale_name_in_the_order_is_ignored(grouped_table):
    spec = _nested_spec(x_layers=["gone", "group", "session"])

    figure = resolve(spec, grouped_table)[0]

    assert [g.label for g in figure.x_plan.groups] == ["stim", "sham"]


def test_panel_rows_carry_the_composed_key(grouped_table):
    figure = resolve(_nested_spec(), grouped_table)[0]
    drawn = set(figure.panels[0].frame["__x"])

    assert drawn == {
        leaf_key(("stim", "pre")),
        leaf_key(("stim", "post")),
        leaf_key(("sham", "pre")),
        leaf_key(("sham", "post")),
    }


def test_one_x_factor_still_takes_the_simple_path(grouped_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"group": Role.X, "session": Role.FREE, "subject": Role.FREE},
        kind=PlotKind.BOX,
    )

    figure = resolve(spec, grouped_table)[0]

    assert figure.x_plan is None
    assert figure.x_order == ["stim", "sham"]


def test_facets_share_one_composed_axis(grouped_table):
    """A panel missing a combination gets a gap where its neighbours do, not a
    differently-shaped axis."""
    spec = _nested_spec(
        roles={"group": Role.X, "session": Role.X, "subject": Role.FACET}
    )

    figure = resolve(spec, grouped_table)[0]

    assert len(figure.panels) == 2
    assert figure.x_plan is not None


def test_group_brackets_stay_out_of_the_row_below():
    """
    The brackets hang INSIDE the vertical gap, so the gap has to be sized for
    them — that is why ``_gaps`` takes the x depth rather than being a constant.

    The case is a wrapped grid: the panel at (0, 1) has an empty cell below it,
    so it is the bottom of its own column and draws tick labels and group
    brackets into the gap that separates the two rows.
    """
    from scistackplot import FacetOptions, render_plotly
    from scistackplot.render.plotly_ import X_GROUP_ROW

    rows = [
        {"group": g, "session": s, "site": site, "subject": p, "StepLength": 1.0}
        for g in ["stim", "sham"]
        for s in ["pre", "post"]
        for site in ["A", "B", "C"]
        for p in ["01", "02"]
    ]
    table = LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["group", "session", "site", "subject"],
        measures=["StepLength"],
        schema_levels=["subject"],
    )
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "group": Role.X,
            "session": Role.X,
            "site": Role.FACET,
            "subject": Role.FREE,
        },
        x_layers=["group", "session"],
        kind=PlotKind.BOX,
        facet=FacetOptions(n_cols=2),
    )

    layout = render_plotly(resolve(spec, table)[0])["layout"]
    # slots: 1=(0,0) 2=(0,1) 3=(1,0). Only (0,1) and (1,0) draw brackets — and
    # the top row's are the ones in the right-hand column.
    top_row_brackets = [a["y"] for a in layout["annotations"] if a["x"] > 0.5]
    assert top_row_brackets
    lowest_in_top_row = min(top_row_brackets)
    row_below_top = layout["yaxis3"]["domain"][1]

    # Clearance for the label's own height, not merely a non-overlap.
    assert lowest_in_top_row - row_below_top >= X_GROUP_ROW


# --- refusals --------------------------------------------------------------


def test_more_than_three_layers_is_refused(grouped_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "group": Role.X,
            "session": Role.X,
            "subject": Role.X,
            "extra": Role.X,
        },
    )
    frame = grouped_table.frame.assign(extra="e")
    table = LongTable.from_frame(
        frame,
        factors=["group", "session", "subject", "extra"],
        measures=["StepLength"],
    )

    with pytest.raises(RoleError, match="At most 3 factors"):
        validate(spec, table)


def test_nesting_is_refused_for_a_1d_measure(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.X, "session": Role.X},
    )

    with pytest.raises(RoleError, match="needs a categorical axis"):
        validate(spec, series_table)


# --- export ----------------------------------------------------------------


def test_generated_code_builds_the_same_axis(grouped_table):
    """The order is EMITTED, not re-derived: a second implementation of the
    nesting rules is a second thing that can drift."""
    pytest.importorskip("seaborn")
    matplotlib = pytest.importorskip("matplotlib")

    from scistackplot import generate_plot_function

    source = generate_plot_function(_nested_spec(), grouped_table)

    assert "order=[" in source
    assert "stim · pre" in source
    # Groups are separated by ordering; seaborn has no empty category.
    assert "seaborn has no" in source

    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    figure = namespace["plot_steplength"](grouped_table.frame.copy(), "figure.png")
    ticks = [t.get_text() for t in figure.axes[0].get_xticklabels()]
    assert ticks == ["stim · pre", "stim · post", "sham · pre", "sham · post"]
    matplotlib.pyplot.close(figure)
