"""
Spaghetti: markers plus one polyline per subject across the x positions.

The figure this kind exists for — intervention groups side by side on x, one
tick per session inside each, one line per subject joining its sessions — so
within a group you read how every subject changed. Two things had no home in
the existing kinds: SCATTER/STRIP draw no lines, and LINE (which has the
series machinery) is refused for scalar measures and for a nested x.

The tests pin the parts that could silently drift apart: the availability
rule, the series identity under a NESTED x (the BAR nested-x bug went
unnoticed because no nested test used the summarising kind — this file makes
sure the spaghetti kind has one), the per-series offset as ONE rule shared by
both renderers and the generated code, and lines that never leave their group.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    Shape,
    available_plots,
    generate_plot_function,
    render_plotly,
    resolve,
)
from scistackplot.capability import why_unavailable
from scistackplot.resolved import SERIES, X
from scistackplot.spaghetti import SPAGHETTI_SPREAD, series_offsets

GROUPS = ["Digitimer", "Sham", "Onward"]
SESSIONS = ["S1", "S2", "S3"]
#: Subjects are NESTED in their group: each one belongs to exactly one.
SUBJECTS_BY_GROUP = {
    "Digitimer": ["01", "02", "10"],
    "Sham": ["03", "04"],
    "Onward": ["05", "06", "07"],
}


@pytest.fixture
def study_frame() -> pd.DataFrame:
    rows = []
    rng = np.random.default_rng(7)
    for group in GROUPS:
        for subject in SUBJECTS_BY_GROUP[group]:
            for session in SESSIONS:
                for trial in ["1", "2"]:
                    rows.append(
                        {
                            "Intervention": group,
                            "subject": subject,
                            "session": session,
                            "trial": trial,
                            "StepLength": float(rng.normal(1.2, 0.1)),
                        }
                    )
    # Database order is not axis order: shuffle so a test that passes only
    # because the rows happened to arrive sorted cannot pass here.
    return pd.DataFrame(rows).sample(frac=1.0, random_state=3).reset_index(drop=True)


@pytest.fixture
def study_table(study_frame) -> LongTable:
    return LongTable.from_frame(
        study_frame,
        factors=["Intervention", "subject", "session", "trial"],
        measures=["StepLength"],
        name="StepLength",
        level_order={"Intervention": GROUPS, "session": SESSIONS},
        schema_levels=["subject", "session", "trial"],
        # The intervention is a subject-level grouping (a joined factor), so the
        # depth rule nests session INSIDE it on the axis.
        factor_depths={"Intervention": 1},
    )


def _study_spec(**kwargs) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={
            "Intervention": Role.GROUP,
            "session": Role.GROUP,
            "subject": Role.GROUP,
            "trial": Role.COLLAPSE,
        },
        # Innermost first: the subjects are the lines, joined across sessions,
        # inside Intervention brackets.
        groups=["subject", "session", "Intervention"],
        kind=PlotKind.SPAGHETTI,
    )
    base.update(kwargs)
    return PlotSpec(**base)


# --- availability -----------------------------------------------------------


def test_offered_exactly_when_two_layers_group():
    """The first grouping layer IS the line identity and the second the
    positions it joins; with one layer there is nothing to join and the kind
    would be a scatter wearing a new name."""
    two = {"session": Role.GROUP, "subject": Role.GROUP}
    one = {"session": Role.GROUP, "subject": Role.COLLAPSE}
    assert PlotKind.SPAGHETTI in available_plots(Shape.SCALAR, two, n_groups=2)
    assert PlotKind.SPAGHETTI not in available_plots(Shape.SCALAR, one, n_groups=1)
    reason = why_unavailable(PlotKind.SPAGHETTI, Shape.SCALAR, one, n_groups=1)
    assert reason and "two grouping layers" in reason


def test_a_1d_measure_offers_it_only_through_a_collapse():
    """Kind implies the collapse (SCALAR_KINDS): a spaghetti of per-trial
    medians is a legitimate ask; on raw vectors it means nothing."""
    grouped = {"session": Role.GROUP, "subject": Role.GROUP}
    assert PlotKind.SPAGHETTI not in available_plots(Shape.SERIES_1D, grouped, n_groups=2)
    assert PlotKind.SPAGHETTI in available_plots(
        Shape.SERIES_1D, grouped, collapsible=True, n_groups=2
    )


def test_not_offered_for_a_relational_scatter():
    assert PlotKind.SPAGHETTI not in available_plots(
        Shape.SCALAR, {"subject": Role.GROUP}, has_x_measure=True, n_groups=2
    )


# --- the offset rule: one owner ---------------------------------------------


def test_offsets_are_symmetric_and_naturally_ordered():
    offsets = series_offsets(["10", "2", "1"])
    assert list(offsets) == ["1", "2", "10"]
    assert offsets["1"] == -SPAGHETTI_SPREAD
    assert offsets["2"] == 0.0
    assert offsets["10"] == SPAGHETTI_SPREAD


def test_a_single_series_sits_on_its_tick():
    assert series_offsets(["only"]) == {"only": 0.0}
    assert series_offsets([]) == {}


def test_offsets_do_not_depend_on_arrival_order():
    assert series_offsets(["b", "a", "c"]) == series_offsets(["c", "b", "a"])


# --- through resolve --------------------------------------------------------


def test_series_identity_excludes_every_x_layer(study_table):
    """The failure this guards: excluding only the OUTER layer would make
    "subject 01 at S1" and "subject 01 at S2" two different series — three
    one-point lines per subject, i.e. no lines at all, and nothing raising."""
    figure = resolve(_study_spec(), study_table)[0]
    frame = figure.panels[0].frame

    assert figure.encoding.series == SERIES
    assert set(frame[SERIES]) == {
        s for subjects in SUBJECTS_BY_GROUP.values() for s in subjects
    }
    # One row per (subject, session): trials were averaged away first.
    assert len(frame) == sum(len(s) for s in SUBJECTS_BY_GROUP.values()) * len(SESSIONS)


def test_the_axis_nests_session_inside_intervention(study_table):
    figure = resolve(_study_spec(), study_table)[0]

    assert figure.x_plan is not None
    assert [g.label for g in figure.x_plan.groups] == GROUPS
    assert [t for t in figure.x_plan.tick_labels if t] == SESSIONS * len(GROUPS)


def test_every_series_gets_one_figure_wide_offset(study_table):
    figure = resolve(_study_spec(), study_table)[0]
    ids = set(figure.panels[0].frame[SERIES].astype(str))

    assert figure.series_offsets == series_offsets(ids)
    assert set(figure.series_offsets) == ids
    assert "series_offsets" in figure.to_dict()


def test_a_non_spaghetti_figure_carries_no_offsets(study_table):
    figure = resolve(_study_spec(kind=PlotKind.BOX), study_table)[0]
    assert figure.series_offsets == {}


def test_the_same_subject_keeps_its_offset_in_every_facet(scalar_table):
    """Offsets are decided per FIGURE, not per panel, and the series key
    leaves the panel-constant factors out: subject 01 in the "pre" facet and
    in the "post" facet is ONE series, at one offset. Keyed as "pre | 01" and
    "post | 01" it would have been two, at two offsets."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.FACET, "trial": Role.GROUP, "subject": Role.GROUP},
        groups=["subject", "trial"],
        kind=PlotKind.SPAGHETTI,
    )
    figure = resolve(spec, scalar_table)[0]

    assert len(figure.panels) == 2
    for panel in figure.panels:
        assert set(panel.frame[SERIES]) == {"01", "02", "03"}
    assert figure.series_offsets == {
        "01": -SPAGHETTI_SPREAD,
        "02": 0.0,
        "03": SPAGHETTI_SPREAD,
    }


def test_faceted_by_group_the_lines_are_still_per_subject(study_table):
    spec = _study_spec(
        roles={
            "Intervention": Role.FACET,
            "session": Role.GROUP,
            "subject": Role.GROUP,
            "trial": Role.COLLAPSE,
        },
        groups=["subject", "session"],
    )
    figure = resolve(spec, study_table)[0]

    assert len(figure.panels) == len(GROUPS)
    every_subject = {s for subjects in SUBJECTS_BY_GROUP.values() for s in subjects}
    assert set(figure.series_offsets) == every_subject


# --- plotly -----------------------------------------------------------------


def _leaf_positions(figure) -> dict[str, int]:
    return {key: index for index, key in enumerate(figure.x_plan.order)}


def test_plotly_draws_one_lines_and_markers_trace_per_subject(study_table):
    figure = resolve(_study_spec(), study_table)[0]
    payload = render_plotly(figure)
    traces = [t for t in payload["data"] if t.get("mode") == "lines+markers"]

    assert len(traces) == sum(len(s) for s in SUBJECTS_BY_GROUP.values())
    for trace in traces:
        # Three sessions, joined left to right.
        assert len(trace["x"]) == len(SESSIONS)
        assert trace["x"] == sorted(trace["x"])


def test_plotly_lines_never_cross_a_group_boundary(study_table):
    """A subject belongs to one intervention, so every point of its line lies
    inside that group's span on the axis — the whole reason for the figure."""
    figure = resolve(_study_spec(), study_table)[0]
    payload = render_plotly(figure)
    spans = [(g.start - 0.5, g.end + 0.5) for g in figure.x_plan.groups]

    for trace in payload["data"]:
        if trace.get("mode") != "lines+markers":
            continue
        inside = [
            any(low <= x <= high for (low, high) in spans) for x in trace["x"]
        ]
        assert all(inside), trace["x"]
        # …and all inside the SAME span.
        which = {
            next(i for i, (low, high) in enumerate(spans) if low <= x <= high)
            for x in trace["x"]
        }
        assert len(which) == 1, trace["x"]


def test_plotly_positions_are_index_plus_the_shared_offset(study_table):
    figure = resolve(_study_spec(), study_table)[0]
    payload = render_plotly(figure)
    leaf_at = _leaf_positions(figure)
    frame = figure.panels[0].frame

    for trace in payload["data"]:
        if trace.get("mode") != "lines+markers":
            continue
        subject = trace["text"][0].split("<br>")[-1]
        expected = sorted(
            leaf_at[key] + figure.series_offsets[subject]
            for key in frame.loc[frame[SERIES] == subject, X]
        )
        assert trace["x"] == pytest.approx(expected)


def test_plotly_axis_is_positional_with_named_ticks(study_table):
    """A category axis stringifies a numeric x into a NEW category, so the
    offsets need a linear axis whose ticks are the levels' indices."""
    figure = resolve(_study_spec(), study_table)[0]
    axis = render_plotly(figure)["layout"]["xaxis"]
    n = len(figure.x_plan.order)

    assert axis["type"] != "category"
    assert "categoryarray" not in axis
    assert axis["tickvals"] == list(range(n))
    assert axis["ticktext"] == list(figure.x_plan.tick_labels)
    assert axis["range"] == [-0.5, n - 0.5]


def test_plotly_group_brackets_are_still_drawn(study_table):
    figure = resolve(_study_spec(), study_table)[0]
    layout = render_plotly(figure)["layout"]

    assert {a["text"] for a in layout["annotations"]} >= set(GROUPS)
    assert len(layout.get("shapes", [])) == len(figure.x_plan.groups)


def test_plotly_flat_axis_is_positional_too(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        kind=PlotKind.SPAGHETTI,
    )
    figure = resolve(spec, scalar_table)[0]
    axis = render_plotly(figure)["layout"]["xaxis"]

    assert axis["tickvals"] == [0, 1]
    # The fixture declares no session order, so the axis is natural-sorted.
    assert axis["ticktext"] == [str(v) for v in scalar_table.factor("session").levels]
    assert axis["type"] != "category"


def test_other_kinds_keep_their_category_axis(study_table):
    axis = render_plotly(resolve(_study_spec(kind=PlotKind.BOX), study_table)[0])[
        "layout"
    ]["xaxis"]
    assert axis["type"] == "category"
    assert "range" not in axis


def test_plotly_legend_entry_once_per_colour_not_per_subject(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.GROUP},
        groups=["subject", "trial", "session"],
        color="trial",
        kind=PlotKind.SPAGHETTI,
    )
    payload = render_plotly(resolve(spec, scalar_table)[0])
    shown = [t["name"] for t in payload["data"] if t.get("showlegend")]
    assert sorted(shown) == ["1", "2", "3", "4"]


# --- matplotlib -------------------------------------------------------------


matplotlib = pytest.importorskip("matplotlib")


def test_matplotlib_draws_one_line_per_subject_in_x_order(study_table):
    from scistackplot import render_matplotlib

    figure = resolve(_study_spec(), study_table)[0]
    drawn = render_matplotlib(figure)
    lines = [line for line in drawn.axes[0].get_lines() if line.get_marker() == "o"]

    assert len(lines) == sum(len(s) for s in SUBJECTS_BY_GROUP.values())
    for line in lines:
        xs = list(line.get_xdata())
        assert xs == sorted(xs)
        assert len(xs) == len(SESSIONS)
    matplotlib.pyplot.close(drawn)


def test_both_backends_place_every_marker_at_the_same_x(study_table):
    """The offset is one rule (spaghetti.series_offsets); this is the proof
    the two renderers both read it rather than each inventing a jitter."""
    from scistackplot import render_matplotlib

    figure = resolve(_study_spec(), study_table)[0]
    payload = render_plotly(figure)
    plotly_xs = sorted(
        round(x, 6)
        for t in payload["data"]
        if t.get("mode") == "lines+markers"
        for x in t["x"]
    )
    drawn = render_matplotlib(figure)
    mpl_xs = sorted(
        round(float(x), 6)
        for line in drawn.axes[0].get_lines()
        if line.get_marker() == "o"
        for x in line.get_xdata()
    )
    matplotlib.pyplot.close(drawn)

    assert mpl_xs == plotly_xs


def test_matplotlib_ticks_name_the_inner_layer(study_table):
    from scistackplot import render_matplotlib

    drawn = render_matplotlib(resolve(_study_spec(), study_table)[0])
    labels = [t.get_text() for t in drawn.axes[0].get_xticklabels() if t.get_text()]
    assert labels == SESSIONS * len(GROUPS)
    matplotlib.pyplot.close(drawn)


# --- generated code ---------------------------------------------------------


pytest.importorskip("seaborn")


def _run(source: str, frame, function_name: str):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


def test_generated_code_draws_the_same_lines(study_table, study_frame):
    spec = _study_spec()
    source = generate_plot_function(spec, study_table)

    assert 'kind="line"' in source
    assert "units='_series'" in source
    assert "estimator=None" in source
    figure = _run(source, study_frame, "plot_steplength")
    lines = [line for line in figure.axes[0].get_lines() if line.get_marker() == "o"]
    assert len(lines) == sum(len(s) for s in SUBJECTS_BY_GROUP.values())
    for line in lines:
        assert len(line.get_xdata()) == len(SESSIONS)
    matplotlib.pyplot.close(figure)


def test_generated_offsets_match_the_library_rule(study_table, study_frame):
    """The generated preamble restates ``series_offsets`` in plain pandas (an
    exported endpoint cannot import this package). Run that preamble and
    compare — a drift in either spelling of the rule fails here."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        kind=PlotKind.SPAGHETTI,
    )
    source = generate_plot_function(spec, study_table)
    # Everything up to the plot call, dedented back to module level.
    body = source.split('    with plt.rc_context')[1].split("\n", 1)[1]
    preamble = body.split("        g = sns.relplot(")[0]
    preamble = "\n".join(line[8:] for line in preamble.splitlines())
    namespace: dict = {"pd": pd, "re": __import__("re"), "df": study_frame.copy()}
    exec(preamble, namespace)  # noqa: S102

    expected = series_offsets(namespace["df"]["_series"].astype(str).unique())
    assert namespace["_offset"] == expected
    assert namespace["_order"] == SESSIONS


def test_generated_nested_axis_positions_follow_the_composed_order(
    study_table, study_frame
):
    source = generate_plot_function(_study_spec(), study_table)
    figure = _run(source, study_frame, "plot_steplength")
    ax = figure.axes[0]

    labels = [t.get_text() for t in ax.get_xticklabels()]
    assert labels == [f"{g} · {s}" for g in GROUPS for s in SESSIONS]
    # Every subject's line stays within its group's three positions.
    for line in ax.get_lines():
        if line.get_marker() != "o":
            continue
        xs = np.asarray(line.get_xdata(), dtype=float)
        assert xs.max() - xs.min() < len(SESSIONS)
    matplotlib.pyplot.close(figure)


def test_generated_code_imports_re_only_for_spaghetti(study_table):
    assert "import re" in generate_plot_function(_study_spec(), study_table)
    assert "import re" not in generate_plot_function(
        _study_spec(kind=PlotKind.BOX), study_table
    )
