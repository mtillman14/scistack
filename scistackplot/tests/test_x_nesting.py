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
        roles={"group": Role.GROUP, "session": Role.GROUP, "subject": Role.COLLAPSE},
        # Innermost first: session ticks inside group brackets.
        groups=["session", "group"],
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
    """Membership is roles; ORDER is `groups`. Swapping the list swaps the axis
    without touching a single role."""
    figure = resolve(_nested_spec(groups=["group", "session"]), grouped_table)[0]

    assert [g.label for g in figure.x_plan.groups] == ["pre", "post"]


def test_a_group_holder_missing_from_the_order_is_appended(grouped_table):
    """Neither control can put the spec in a state the other rejects. The
    unordered holder lands OUTERMOST (appended to an innermost-first list)."""
    figure = resolve(_nested_spec(groups=["session"]), grouped_table)[0]

    assert [g.label for g in figure.x_plan.groups] == ["stim", "sham"]
    assert figure.x_plan.n_layers == 2


def test_a_stale_name_in_the_order_is_ignored(grouped_table):
    spec = _nested_spec(groups=["gone", "session", "group"])

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
        roles={"group": Role.GROUP, "session": Role.COLLAPSE, "subject": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )

    figure = resolve(spec, grouped_table)[0]

    assert figure.x_plan is None
    assert figure.x_order == ["stim", "sham"]


def test_facets_share_one_composed_axis(grouped_table):
    """A panel missing a combination gets a gap where its neighbours do, not a
    differently-shaped axis."""
    spec = _nested_spec(
        roles={"group": Role.GROUP, "session": Role.GROUP, "subject": Role.FACET},
        kind=PlotKind.STRIP,  # nothing collapsed, so no distribution to box
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
            "group": Role.GROUP,
            "session": Role.GROUP,
            "site": Role.FACET,
            "subject": Role.COLLAPSE,
        },
        groups=["session", "group"],
        kind=PlotKind.BOX,
        facet=FacetOptions(n_cols=2),
    )

    layout = render_plotly(resolve(spec, table)[0])["layout"]
    # slots: 1=(0,0) 2=(0,1) 3=(1,0). Only (0,1) and (1,0) draw brackets — and
    # the top row's are the ones on the right-hand column's axis.
    top_row_brackets = [a["y"] for a in layout["annotations"] if a["xref"] == "x2"]
    assert top_row_brackets
    lowest_in_top_row = min(top_row_brackets)
    row_below_top = layout["yaxis3"]["domain"][1]

    # Clearance for the label's own height, not merely a non-overlap.
    assert lowest_in_top_row - row_below_top >= X_GROUP_ROW


# --- refusals --------------------------------------------------------------


def test_more_than_three_labelled_layers_is_refused(grouped_table):
    roles = {name: Role.GROUP for name in ["group", "session", "subject", "extra"]}
    spec = PlotSpec(measures=["StepLength"], roles=roles)
    frame = grouped_table.frame.assign(extra="e")
    table = LongTable.from_frame(
        frame,
        factors=["group", "session", "subject", "extra"],
        measures=["StepLength"],
    )

    with pytest.raises(RoleError, match="At most 3 labelled"):
        validate(spec, table)
    # Colour is paint: the coloured layer is still a labelled tick, so
    # colouring one of four does not bring the grouping under the cap.
    with pytest.raises(RoleError, match="At most 3 labelled"):
        validate(PlotSpec(measures=["StepLength"], roles=roles, color="extra"), table)


def test_grouping_a_1d_measure_makes_series_not_ticks(series_table):
    from scistackplot.roles import grouping_layers

    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.GROUP, "session": Role.GROUP},
    )
    validate(spec, series_table)
    assert grouping_layers(spec, series_table).ticks == []


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


def test_generated_code_keeps_the_nested_axis_through_an_aggregate(grouped_table):
    """Nested x + a collapse: the composed `_x` column is built BEFORE the
    averaging groupby, and pandas drops every column the groupby does not
    name — so the plot call asked for an `_x` that no longer existed
    (KeyError). Pre-existing for every nested + aggregate export; found by the
    spaghetti tests, which aggregate trials under a nested axis."""
    pytest.importorskip("seaborn")
    matplotlib = pytest.importorskip("matplotlib")

    from scistackplot import generate_plot_function

    # Two trials per subject, so the chain has a PRE-collapse (trial within
    # subject) — the averaging groupby this guards. With subject alone
    # collapsed it is the sample and nothing is averaged (schema-level
    # parity, 2026-09-19).
    frame = pd.concat(
        [grouped_table.frame.assign(trial=t) for t in ("1", "2")], ignore_index=True
    )
    table = LongTable.from_frame(
        frame,
        factors=["group", "session", "subject", "trial"],
        measures=["StepLength"],
        level_order={"group": ["stim", "sham"], "session": ["pre", "post"]},
        schema_levels=["subject", "trial"],
    )
    spec = _nested_spec(
        roles={
            "group": Role.GROUP,
            "session": Role.GROUP,
            "subject": Role.COLLAPSE,
            "trial": Role.COLLAPSE,
        },
        kind=PlotKind.SCATTER,
    )
    source = generate_plot_function(spec, table)
    assert "'_x'" in source.split("groupby(")[1].split(")")[0]

    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    figure = namespace["plot_steplength"](frame.copy(), "figure.png")
    ticks = [t.get_text() for t in figure.axes[0].get_xticklabels()]
    assert ticks == ["stim · pre", "stim · post", "sham · pre", "sham · post"]
    matplotlib.pyplot.close(figure)


# --- the plotly axis: stated order, spacers, brackets -----------------------
#
# The composition above was always right. What was missing was saying it to
# plotly, which orders a categorical axis by FIRST APPEARANCE IN THE TRACES
# unless told otherwise — and the trace order is `_summarize(sort=False)`, i.e.
# database row order. Spacers made it worse: they hold no data by construction,
# so they reached no trace and plotly never learned they existed.


def test_the_plotly_axis_states_the_whole_plan_order(grouped_table):
    """Including the spacers — they are the gap between groups."""
    from scistackplot import render_plotly
    from scistackplot.xaxis import is_spacer

    figure = resolve(_nested_spec(), grouped_table)[0]
    layout = render_plotly(figure)["layout"]

    assert layout["xaxis"]["categoryorder"] == "array"
    assert layout["xaxis"]["categoryarray"] == list(figure.x_plan.order)
    assert any(is_spacer(key) for key in layout["xaxis"]["categoryarray"])


def test_the_plotly_axis_is_stated_categorical(grouped_table):
    """Never left to auto-detection: plotly reads an array of numeric-LOOKING
    strings as a linear axis, and on a linear axis `categoryarray` is ignored.
    Zero-padded schema keys are exactly that kind of string."""
    from scistackplot import render_plotly

    layout = render_plotly(resolve(_nested_spec(), grouped_table)[0])["layout"]

    assert layout["xaxis"]["type"] == "category"


def test_zero_padded_levels_keep_their_declared_order_in_plotly():
    """The case the stated type exists for: every level stringifies as a
    number, so auto-detection would make this a number line and drop the
    order — putting "10" between "01" and "02"."""
    from scistackplot import render_plotly

    table = LongTable.from_frame(
        pd.DataFrame(
            [
                {"session": session, "subject": "01", "StepLength": 1.0}
                # Deliberately NOT in declared order: this is what the database
                # hands back, and what plotly would otherwise draw.
                for session in ["10", "02", "01"]
            ]
        ),
        factors=["session", "subject"],
        measures=["StepLength"],
        level_order={"session": ["01", "02", "10"]},
        schema_levels=["subject"],
    )
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE},
        kind=PlotKind.BAR,
    )

    layout = render_plotly(resolve(spec, table)[0])["layout"]

    assert layout["xaxis"]["type"] == "category"
    assert layout["xaxis"]["categoryarray"] == ["01", "02", "10"]


def test_a_flat_categorical_axis_is_ordered_too(grouped_table):
    """Not a nested-axis fix: a single factor on x had the same hole."""
    from scistackplot import render_plotly

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"group": Role.GROUP, "session": Role.COLLAPSE, "subject": Role.COLLAPSE},
        kind=PlotKind.BAR,
    )

    layout = render_plotly(resolve(spec, grouped_table)[0])["layout"]

    assert layout["xaxis"]["categoryarray"] == ["stim", "sham"]


def test_a_numeric_axis_is_left_alone(series_table):
    """A 1-D measure's x is its sample index. Forcing that into categories
    would turn a number line into evenly spaced ticks."""
    from scistackplot import render_plotly

    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.GROUP},
        color="session",
        kind=PlotKind.LINE,
    )

    layout = render_plotly(resolve(spec, series_table)[0])["layout"]

    assert "categoryarray" not in layout["xaxis"]
    assert layout["xaxis"]["type"] != "category"


def test_brackets_span_the_leaves_they_name(grouped_table):
    """Brackets are placed at leaf INDICES in `plan.order`, which is only right
    because the spacers occupy slots. Before they did, every bracket sat left
    of its bars."""
    from scistackplot import render_plotly

    figure = resolve(_nested_spec(), grouped_table)[0]
    payload = render_plotly(figure)

    by_label = {a["text"]: a["x"] for a in payload["layout"]["annotations"]}
    for group in figure.x_plan.groups:
        assert group.start - 0.5 <= by_label[group.label] <= group.end + 0.5


@pytest.mark.parametrize(
    "kwargs",
    [
        {"kind": PlotKind.BAR},
        {"kind": PlotKind.BOX},
        {"kind": PlotKind.VIOLIN},
        {
            "kind": PlotKind.STRIP,
            "roles": {"group": Role.GROUP, "session": Role.GROUP, "subject": Role.FACET},
        },
    ],
    ids=lambda kw: str(kw["kind"]),
)
def test_brackets_follow_the_axis_when_zoomed(grouped_table, kwargs):
    """Zooming onto one pair of bars left every group's label under the plot:
    the brackets were in PAPER coordinates, pinned to the panel, while the
    ticks followed the axis range. They are in the panel's own x-axis
    coordinates (category serial numbers = indices in `plan.order`), so plotly
    clips and hides them with the ticks — for every kind, from one owner."""
    from scistackplot import render_plotly
    from scistackplot.render.plotly_ import X_GROUP_TAG

    figure = resolve(_nested_spec(**kwargs), grouped_table)[0]
    layout = render_plotly(figure)["layout"]
    axis_ids = {
        ("x" if key == "xaxis" else "x" + key[len("xaxis"):])
        for key in layout
        if key.startswith("xaxis")
    }

    marks = [
        m
        for m in layout["annotations"] + layout.get("shapes", [])
        if str(m.get("name", "")).startswith(X_GROUP_TAG)
    ]
    assert marks
    for mark in marks:
        assert mark["xref"] in axis_ids, mark
        assert mark["yref"] == "paper", mark
    for shape in (m for m in marks if m.get("type") == "line"):
        # Inside the slots of the leaves it names, never past the axis ends.
        assert -0.5 <= shape["x0"] < shape["x1"] <= len(figure.x_plan.order) - 0.5


def test_the_two_backends_order_the_axis_the_same_way(grouped_table):
    """The panel and the saved PNG disagreeing is its own bug, and is how this
    shipped: matplotlib indexes into `x_order` and was always right."""
    pytest.importorskip("matplotlib")
    from scistackplot import render_matplotlib, render_plotly
    from scistackplot.xaxis import is_spacer

    figure = resolve(_nested_spec(), grouped_table)[0]
    layout = render_plotly(figure)["layout"]
    drawn = [key for key in layout["xaxis"]["categoryarray"] if not is_spacer(key)]

    mpl_figure = render_matplotlib(figure)
    try:
        # matplotlib positions leaves at their index in x_plan.order, so the
        # ticks it labels (spacers are blank) are the same sequence.
        labels = [t.get_text() for t in mpl_figure.axes[0].get_xticklabels()]
    finally:
        import matplotlib.pyplot as plt

        plt.close(mpl_figure)

    assert [key.split("␟")[-1] for key in drawn] == [
        label for label in labels if label
    ]


# --- bar layout, stated ----------------------------------------------------


def test_bar_layout_is_stated_not_inherited(grouped_table):
    from scistackplot import render_plotly

    layout = render_plotly(
        resolve(_nested_spec(kind=PlotKind.BAR), grouped_table)[0]
    )["layout"]

    assert layout["barmode"] == "group"
    assert layout["bargap"] == 0.2
    assert layout["bargroupgap"] == 0.0


def test_a_non_bar_figure_states_no_bar_layout(grouped_table):
    from scistackplot import render_plotly

    layout = render_plotly(resolve(_nested_spec(), grouped_table)[0])["layout"]

    assert "barmode" not in layout


def test_box_layout_is_stated_like_the_bars(grouped_table):
    """The box layout is stated the way the bar layout is: "group" mode with
    every trace in one `offsetgroup` (colour is paint — one box per position,
    full width), never left to plotly's default."""
    from scistackplot import render_plotly

    layout = render_plotly(
        resolve(_nested_spec(kind=PlotKind.BOX), grouped_table)[0]
    )["layout"]

    assert layout["boxmode"] == "group"
    assert layout["boxgap"] == 0.2
    assert layout["boxgroupgap"] == 0.15
    assert "violinmode" not in layout
    assert "barmode" not in layout


def test_violin_layout_is_stated_like_the_bars(grouped_table):
    from scistackplot import render_plotly

    layout = render_plotly(
        resolve(_nested_spec(kind=PlotKind.VIOLIN), grouped_table)[0]
    )["layout"]

    assert layout["violinmode"] == "group"
    assert layout["violingap"] == 0.2
    assert layout["violingroupgap"] == 0.1
    assert "boxmode" not in layout
    assert "barmode" not in layout


def test_only_the_drawn_kind_states_a_mark_layout(grouped_table):
    """A bar figure must not carry box/violin layout keys, and vice versa."""
    from scistackplot import render_plotly

    layout = render_plotly(
        resolve(_nested_spec(kind=PlotKind.BAR), grouped_table)[0]
    )["layout"]

    assert "boxmode" not in layout
    assert "violinmode" not in layout


# --- where a new layer lands (depth) ---------------------------------------
#
# `FactorInfo.depth` is "how many schema keys pin one value of this factor", so
# a subject-level grouping and `subject` itself are both 1 and `session` is 2.
# The panel's `xLayers.ts` places a ticked factor by the same numbers; its own
# cases are in `scistack-gui/frontend/.../xLayers.test.ts`.


@pytest.fixture
def depth_table() -> LongTable:
    """The study that prompted this: a subject-level grouping beside sessions."""
    rows = [
        {
            "subject": subject,
            "session": session,
            "InterventionGroup": "onward" if subject == "01" else "usual",
            "bandpass.low_hz": "20",
            "StepLength": 1.0,
        }
        for subject in ["01", "02"]
        for session in ["pre", "post"]
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "session", "InterventionGroup", "bandpass.low_hz"],
        measures=["StepLength"],
        variant_factors=["bandpass.low_hz"],
        # What the scidb source supplies: the grouping variable's own depth.
        factor_depths={"InterventionGroup": 1},
        schema_levels=["subject", "session"],
    )


def test_schema_keys_are_numbered_by_their_place_in_the_hierarchy(depth_table):
    assert depth_table.factor("subject").depth == 1
    assert depth_table.factor("session").depth == 2


def test_a_joined_grouping_takes_the_depth_the_source_gave_it(depth_table):
    """A subject-level sheet pins one value per subject, so it sits WITH
    `subject`, not with whatever position the column list left it in."""
    assert depth_table.factor("InterventionGroup").depth == 1


def test_a_variant_axis_has_no_depth(depth_table):
    """It is not a place in the hierarchy, so it cannot claim to sit outside a
    subject. `None` is not depth zero."""
    assert depth_table.factor("bandpass.low_hz").depth is None


def test_a_new_layer_is_placed_by_depth_not_appended(depth_table):
    """The defect this fixes: appending put `InterventionGroup` INSIDE
    `session`, giving one bar per group within each session — the transpose of
    "one cluster per group, one bar per session". Innermost first, so the
    deeper `session` comes first and the subject-level grouping wraps it."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "InterventionGroup": Role.GROUP, "subject": Role.COLLAPSE},
        # Nothing declared: this is a spec that never went through the arrows.
        groups=[],
        kind=PlotKind.BAR,
    )

    assert spec.ordered_groups(depths=depth_table.factor_depths) == [
        "session",
        "InterventionGroup",
    ]
    figure = resolve(spec, depth_table)[0]
    assert [g.label for g in figure.x_plan.groups] == ["onward", "usual"]


def test_an_explicit_order_still_wins(depth_table):
    """Depth is where to start, not an order the user cannot override."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "InterventionGroup": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["InterventionGroup", "session"],
        kind=PlotKind.BAR,
    )

    assert spec.ordered_groups(depths=depth_table.factor_depths) == [
        "InterventionGroup",
        "session",
    ]


def test_a_depthless_factor_sorts_innermost(depth_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "bandpass.low_hz": Role.GROUP,
            "session": Role.GROUP,
            "subject": Role.COLLAPSE,
            "InterventionGroup": Role.COLLAPSE,
        },
    )

    assert spec.ordered_groups(depths=depth_table.factor_depths) == [
        "bandpass.low_hz",
        "session",
    ]


def test_equal_depths_keep_declaration_order(depth_table):
    """`subject` and a subject-level grouping tie. A tie must not swap on a
    dict rebuild — a legend that reshuffles for no reason is the standing
    complaint this package keeps answering."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "InterventionGroup": Role.GROUP},
    )
    assert spec.ordered_groups(depths=depth_table.factor_depths) == [
        "subject",
        "InterventionGroup",
    ]

    reversed_spec = PlotSpec(
        measures=["StepLength"],
        roles={"InterventionGroup": Role.GROUP, "subject": Role.GROUP},
    )
    assert reversed_spec.ordered_groups(depths=depth_table.factor_depths) == [
        "InterventionGroup",
        "subject",
    ]


def test_the_control_and_the_figure_read_the_same_order(depth_table):
    """`capability.grouping["layers"]` is what the panel draws; `resolve` is
    what the figure draws. The panel showing an order the renderer disagrees
    with is the whole reason this is one function."""
    from scistackplot import capabilities

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "InterventionGroup": Role.GROUP, "subject": Role.COLLAPSE},
        kind=PlotKind.BAR,
    )

    grouping = capabilities(spec, depth_table)["grouping"]
    figure = resolve(spec, depth_table)[0]

    assert grouping["layers"] == ["session", "InterventionGroup"], "innermost first"
    assert grouping["ticks"] == ["InterventionGroup", "session"], "drawing order"
    # The leaves are composed outer-to-inner, so the first tick layer's
    # levels are the bracket labels.
    assert [g.label for g in figure.x_plan.groups] == ["onward", "usual"]


# --- the role a new grouping takes -----------------------------------------


def test_a_new_grouping_groups(depth_table):
    """A grouping ticked in the Grouping section is a request for one mark
    per level; anything else would quietly do nothing."""
    from scistackplot import role_for_new_grouping

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE},
        kind=PlotKind.BAR,
    )

    assert role_for_new_grouping(spec, depth_table) is Role.GROUP


def test_a_new_grouping_facets_when_the_labelled_ticks_are_full(depth_table):
    from scistackplot import role_for_new_grouping
    from scistackplot.spec import MAX_X_LAYERS

    roles = {
        name: Role.GROUP
        for name in ["subject", "session", "InterventionGroup"][:MAX_X_LAYERS]
    }
    spec = PlotSpec(measures=["StepLength"], roles=roles, kind=PlotKind.BAR)

    assert role_for_new_grouping(spec, depth_table) is Role.FACET


def test_colouring_a_layer_does_not_make_room_for_another(depth_table):
    """Colour is paint (2026-09-21): the coloured layer keeps its tick labels
    and still counts against the cap, so a full grouping stays full."""
    from scistackplot import role_for_new_grouping

    roles = {name: Role.GROUP for name in ["subject", "session", "InterventionGroup"]}
    spec = PlotSpec(
        measures=["StepLength"], roles=roles, color="subject", kind=PlotKind.BAR
    )

    assert role_for_new_grouping(spec, depth_table) is Role.FACET


def test_a_new_grouping_groups_a_1d_measure_as_a_series(series_table):
    """A 1-D measure's x is its sample index; a group is a line per level."""
    from scistackplot import role_for_new_grouping

    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.ITERATE, "session": Role.ITERATE, "trial": Role.ITERATE},
        kind=PlotKind.LINE,
    )

    assert role_for_new_grouping(spec, series_table) is Role.GROUP


def test_a_role_already_chosen_is_never_overwritten(depth_table):
    """Re-applying a picker must not undo a decision already made."""
    from scistackplot import role_for_new_grouping

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"InterventionGroup": Role.FACET, "session": Role.GROUP},
        kind=PlotKind.BAR,
    )

    assert role_for_new_grouping(spec, depth_table, "InterventionGroup") is Role.FACET


def test_the_capability_report_publishes_the_same_answer(depth_table):
    """The panel applies this rather than inventing a second rule, and it is
    published per SPEC because the factor is not in the table yet."""
    from scistackplot import capabilities, role_for_new_grouping

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE},
        kind=PlotKind.BAR,
    )

    assert capabilities(spec, depth_table)["grouping"]["new_grouping_role"] == str(
        role_for_new_grouping(spec, depth_table)
    )


# --- the summary must not eat the axis -------------------------------------
#
# `_summarize` (BAR and BAND) groups the panel frame down to centre + error and
# `reset_index`es the keys back. Anything not in the keys is gone — which for a
# while included the nested axis's LAYER columns, so `_plan_nested_x` found no
# panel carrying every layer, composed an empty plan, and bar figures lost
# their order, their ticks and their brackets. Box never summarizes, and every
# nested test used box.


@pytest.mark.parametrize(
    "kind", [PlotKind.BOX, PlotKind.VIOLIN, PlotKind.STRIP, PlotKind.BAR]
)
def test_the_nested_axis_survives_every_scalar_kind(grouped_table, kind):
    figure = resolve(_nested_spec(kind=kind), grouped_table)[0]

    assert [g.label for g in figure.x_plan.groups] == ["stim", "sham"]
    assert figure.x_order == figure.x_plan.order
    assert figure.x_order != []


def test_a_summarized_panel_still_carries_its_layer_columns(grouped_table):
    figure = resolve(_nested_spec(kind=PlotKind.BAR), grouped_table)[0]
    frame = figure.panels[0].frame

    assert {"group", "session"} <= set(frame.columns)
    assert set(frame["group"]) == {"stim", "sham"}


def test_carrying_the_layers_does_not_split_the_summary(grouped_table):
    """The layer columns are a FUNCTION of the composed key, so grouping by
    them as well must change no group — one row per leaf, not one per
    replicate. If this ever fails, the bars became replicate bars."""
    figure = resolve(_nested_spec(kind=PlotKind.BAR), grouped_table)[0]
    frame = figure.panels[0].frame

    # 2 groups x 2 sessions, each summarizing the two subjects' rows.
    assert len(frame) == 4
    assert frame["__x"].nunique() == 4


def test_a_nested_bar_axis_reaches_plotly_in_order(grouped_table):
    """End to end, on the kind that was broken: the composed order (spacers
    included) is what the figure states, and the brackets are drawn."""
    from scistackplot import render_plotly

    figure = resolve(_nested_spec(kind=PlotKind.BAR), grouped_table)[0]
    payload = render_plotly(figure)

    assert payload["layout"]["xaxis"]["categoryarray"] == list(figure.x_plan.order)
    assert [a["text"] for a in payload["layout"]["annotations"]] == ["stim", "sham"]


@pytest.mark.parametrize(
    "shown, expected",
    [
        # Everything shown: every bracket row is ruled.
        ({0, 1, 2}, {0, 1}),
        # Tick row hidden: the innermost row has nothing above to bracket.
        ({0, 1}, {0}),
        # Middle row hidden: the outer rule still brackets the ticks.
        ({0, 2}, {0}),
        # A hidden row draws no rule of its own.
        ({1, 2}, {1}),
        # Only the outermost row shows: nothing to bracket.
        ({0}, set()),
    ],
)
def test_a_bracket_row_is_ruled_only_under_shown_labels(shown, expected):
    """A rule sits ABOVE its label and spans the labels above it — so it needs
    its own label AND some shown row nearer the axis (tick row = plan depth)."""
    from scistackplot.render.base import ruled_bracket_depths

    assert ruled_bracket_depths(2, shown) == expected
