"""What the panel is told about a 1-D measure that can be collapsed.

The kind list, the two shapes, the role re-roll, and the end-to-end figure —
the user's own example: a per-trial vector of step lengths, averaged within
each trial, drawn as a violin across trials.
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
    Statistic,
    available_plots,
    capabilities,
    default_roles,
    resolve,
    roles_for_kind,
    why_unavailable,
)
from scistackplot.resolved import X, Y

REPLICATES = {"trial": Role.FREE}
NO_REPLICATES = {"trial": Role.ITERATE}


# --- the kind list ----------------------------------------------------------


def test_a_1d_measure_offers_the_scalar_kinds_when_collapsible():
    kinds = available_plots(Shape.SERIES_1D, REPLICATES, collapsible=True)
    assert PlotKind.VIOLIN in kinds
    assert PlotKind.BOX in kinds
    assert PlotKind.BAR in kinds
    assert PlotKind.SCATTER in kinds
    assert PlotKind.STRIP in kinds


def test_the_1d_kinds_stay_offered_alongside_them():
    """The union, not a switch — collapsing is a view of the same variable, and
    a user who picks a violin must be able to get back to the line."""
    kinds = available_plots(Shape.SERIES_1D, REPLICATES, collapsible=True)
    assert PlotKind.LINE in kinds
    assert PlotKind.BAND in kinds


def test_without_collapsible_the_1d_list_is_unchanged():
    assert available_plots(Shape.SERIES_1D, REPLICATES) == [
        PlotKind.LINE,
        PlotKind.BAND,
    ]


def test_the_distribution_kinds_still_need_replicates():
    kinds = available_plots(Shape.SERIES_1D, NO_REPLICATES, collapsible=True)
    assert PlotKind.SCATTER in kinds  # one point per record is a figure
    assert PlotKind.VIOLIN not in kinds  # one point per figure is not
    reason = why_unavailable(
        PlotKind.VIOLIN, Shape.SERIES_1D, NO_REPLICATES, collapsible=True
    )
    assert reason and "replicates" in reason


def test_an_offered_kind_carries_no_refusal():
    """A kind the panel enables must not also come with a reason it is
    disabled — the tooltip and the radio would contradict each other."""
    for kind in available_plots(Shape.SERIES_1D, REPLICATES, collapsible=True):
        assert (
            why_unavailable(kind, Shape.SERIES_1D, REPLICATES, collapsible=True) is None
        )


def test_a_relational_plot_is_unaffected():
    kinds = available_plots(
        Shape.SERIES_1D, REPLICATES, has_x_measure=True, collapsible=True
    )
    assert kinds == [PlotKind.SCATTER, PlotKind.LINE]


# --- the capability report --------------------------------------------------


def _violin(measure="Signal", **kwargs) -> PlotSpec:
    roles = kwargs.pop("roles", {"subject": Role.X, "trial": Role.FREE})
    return PlotSpec(measures=[measure], kind=PlotKind.VIOLIN, roles=roles, **kwargs)


def test_the_report_carries_both_shapes(series_table):
    report = capabilities(_violin(), series_table)
    assert report["shape"] == "scalar", "the figure is scalar"
    assert report["raw_shape"] == "1d", "the data is not"
    assert report["collapse"] == {
        "applies": True,
        "active": True,
        "statistic": "mean",
    }


def test_a_line_reports_the_collapse_as_available_but_inactive(series_table):
    spec = PlotSpec(measures=["Signal"], kind=PlotKind.LINE)
    report = capabilities(spec, series_table)
    assert report["shape"] == "1d"
    assert report["collapse"]["applies"] is True
    assert report["collapse"]["active"] is False


def test_a_scalar_measure_reports_no_collapse(scalar_table):
    report = capabilities(PlotSpec(measures=["StepLength"]), scalar_table)
    assert report["collapse"]["applies"] is False
    assert report["raw_shape"] == report["shape"] == "scalar"


def test_the_kind_list_does_not_strand_the_user(series_table):
    """The regression this arrangement exists to prevent: with the kinds
    computed from the COLLAPSED table, selecting a violin would drop LINE and
    BAND from the list and leave no way back."""
    available = capabilities(_violin(), series_table)["available"]
    assert "line" in available
    assert "violin" in available


def test_each_kind_says_whether_it_collapses(series_table):
    kinds = {info["kind"]: info for info in capabilities(_violin(), series_table)["kinds"]}
    assert kinds["violin"]["collapses"] is True
    assert kinds["line"]["collapses"] is False


def test_grouping_the_x_axis_is_offered_once_collapsed(series_table):
    """A 1-D measure's x axis is its own index, so grouping is refused for it.
    Collapsed, the measure is scalar and the refusal must lift — the rule in
    ``grouping_summary`` is untouched; it simply sees a scalar measure."""
    assert capabilities(_violin(), series_table)["grouping"]["available"] is True

    line = PlotSpec(measures=["Signal"], kind=PlotKind.LINE)
    assert capabilities(line, series_table)["grouping"]["available"] is False


def test_a_factor_may_take_the_x_role_once_collapsed(series_table):
    factors = {f["name"]: f for f in capabilities(_violin(), series_table)["factors"]}
    assert factors["subject"]["x_available"] is True

    line = PlotSpec(measures=["Signal"], kind=PlotKind.LINE)
    factors = {f["name"]: f for f in capabilities(line, series_table)["factors"]}
    assert factors["subject"]["x_available"] is False
    assert "1-D" in (factors["subject"]["x_reason"] or "")


# --- the role re-roll -------------------------------------------------------


def test_choosing_a_scalar_kind_rerolls_untouched_1d_roles(series_table):
    """A 1-D measure opens with every key iterated, which leaves no replicates
    — box and violin would be permanently greyed out and the first click would
    appear to do nothing."""
    opened = PlotSpec(
        measures=["Signal"],
        kind=PlotKind.LINE,
        roles=default_roles(series_table, "Signal"),
    )
    assert set(opened.roles.values()) == {Role.ITERATE}

    suggested = roles_for_kind(opened, series_table, PlotKind.VIOLIN)

    assert suggested == default_roles(series_table, "Signal", shape=Shape.SCALAR)
    assert Role.FREE in suggested.values(), "the distribution now has replicates"


def test_the_reroll_never_overwrites_a_role_the_user_set(series_table):
    chosen = PlotSpec(
        measures=["Signal"],
        kind=PlotKind.LINE,
        roles={"subject": Role.COLOR, "session": Role.FACET, "trial": Role.FREE},
    )
    assert roles_for_kind(chosen, series_table, PlotKind.VIOLIN) is None


def test_the_reroll_is_symmetric(series_table):
    """Going back to a line re-defaults the other way, by the same rule."""
    collapsed = PlotSpec(
        measures=["Signal"],
        kind=PlotKind.VIOLIN,
        roles=default_roles(series_table, "Signal", shape=Shape.SCALAR),
    )
    suggested = roles_for_kind(collapsed, series_table, PlotKind.LINE)
    assert suggested == default_roles(series_table, "Signal", shape=Shape.SERIES_1D)


def test_no_reroll_when_the_shape_does_not_change(series_table, scalar_table):
    box = PlotSpec(
        measures=["Signal"],
        kind=PlotKind.VIOLIN,
        roles=default_roles(series_table, "Signal", shape=Shape.SCALAR),
    )
    assert roles_for_kind(box, series_table, PlotKind.BOX) is None
    scalar = PlotSpec(
        measures=["StepLength"], roles=default_roles(scalar_table, "StepLength")
    )
    assert roles_for_kind(scalar, scalar_table, PlotKind.VIOLIN) is None


def test_the_report_carries_the_suggestion_per_kind(series_table):
    opened = PlotSpec(
        measures=["Signal"],
        kind=PlotKind.LINE,
        roles=default_roles(series_table, "Signal"),
    )
    kinds = {info["kind"]: info for info in capabilities(opened, series_table)["kinds"]}
    assert kinds["violin"]["roles"], "the panel applies this synchronously"
    assert kinds["band"]["roles"] is None, "no shape change, no suggestion"


# --- end to end: the user's example -----------------------------------------


@pytest.fixture
def step_length_table() -> LongTable:
    """Per-trial vectors of step lengths — one vector per (subject, trial)."""
    rows = []
    for subject in ["01", "02", "03"]:
        for trial in ["1", "2", "3"]:
            base = 1.0 + 0.1 * int(subject) + 0.01 * int(trial)
            rows.append(
                {
                    "subject": subject,
                    "trial": trial,
                    # Ragged on purpose: trials do not contain the same number
                    # of steps, which is exactly why this is a vector.
                    "StepLength": [base - 0.05, base, base + 0.05][: 2 + int(trial) % 2],
                }
            )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "trial"],
        measures=["StepLength"],
        name="StepLength",
        schema_levels=["subject", "trial"],
    )


def test_a_violin_across_trials_of_per_trial_vectors(step_length_table):
    """The whole feature, end to end: average within each trial, then show the
    distribution across trials, per subject."""
    spec = PlotSpec(
        measures=["StepLength"],
        kind=PlotKind.VIOLIN,
        roles={"subject": Role.X, "trial": Role.FREE},
    )

    (figure,) = resolve(spec, step_length_table)

    (panel,) = figure.panels
    drawn = panel.frame
    expected = [
        float(np.mean(cell)) for cell in step_length_table.frame["StepLength"]
    ]
    assert sorted(drawn[Y].tolist()) == pytest.approx(sorted(expected))
    assert set(drawn[X].astype(str)) == {"01", "02", "03"}
    assert len(drawn) == 9, "one value per trial, not one per step"


def test_the_figure_says_its_points_are_summaries(step_length_table):
    spec = PlotSpec(
        measures=["StepLength"],
        kind=PlotKind.VIOLIN,
        roles={"subject": Role.X, "trial": Role.FREE},
    )
    (figure,) = resolve(spec, step_length_table)
    assert any("collapsed to its mean" in note for note in figure.fanout_notes)


def test_the_median_reaches_the_figure(step_length_table):
    spec = PlotSpec(
        measures=["StepLength"],
        kind=PlotKind.VIOLIN,
        collapse_statistic=Statistic.MEDIAN,
        roles={"subject": Role.X, "trial": Role.FREE},
    )
    (figure,) = resolve(spec, step_length_table)
    expected = [
        float(np.median(cell)) for cell in step_length_table.frame["StepLength"]
    ]
    assert sorted(figure.panels[0].frame[Y].tolist()) == pytest.approx(sorted(expected))


def test_the_y_limits_are_the_collapsed_range(step_length_table):
    """Not the range of every sample — off by the whole within-trial spread."""
    spec = PlotSpec(
        measures=["StepLength"],
        kind=PlotKind.SCATTER,
        roles={"subject": Role.X, "trial": Role.FREE},
    )
    (figure,) = resolve(spec, step_length_table)
    means = [float(np.mean(c)) for c in step_length_table.frame["StepLength"]]
    every_sample = [
        float(v) for cell in step_length_table.frame["StepLength"] for v in cell
    ]
    low, high = figure.y_limits

    assert low <= min(means) and high >= max(means), "every drawn point is inside"
    # Padding is a fraction of the range either way, so the collapsed axis is
    # strictly tighter than one scaled to every sample.
    assert (high - low) < (max(every_sample) - min(every_sample))


def test_switching_kinds_does_not_share_a_plan(step_length_table):
    """A line and a violin of one 1-D measure are different frames, different
    roles and different limits — the plan cache must not serve one for the
    other even though ``kind`` is otherwise presentation."""
    from scistackplot import reduce as reduce_mod

    reduce_mod.clear_plan_cache()
    roles = {"subject": Role.COLOR, "trial": Role.FREE}
    line = PlotSpec(measures=["StepLength"], kind=PlotKind.LINE, roles=roles)
    violin = PlotSpec(measures=["StepLength"], kind=PlotKind.VIOLIN, roles=roles)

    (line_figure,) = resolve(line, step_length_table)
    (violin_figure,) = resolve(violin, step_length_table)

    assert len(reduce_mod._plan_cache) == 2
    assert line_figure.row_count > violin_figure.row_count, "samples vs summaries"


# --- the click has to be reachable, and it has to draw something ------------
#
# Both of these came out of one failing assertion (2026-09-14): with only two
# factors the scalar default is `subject=X, session=COLOR` — no replicates —
# so the re-roll handed back roles that STILL could not draw a violin. Looking
# at that showed the other half: from the opening state box/violin are refused
# for want of replicates, so the kind whose selection would have supplied them
# could not be clicked either.


def _two_factor_table() -> LongTable:
    rows = [
        {"subject": subject, "session": session, "Signal": [1.0 * index, 2.0 + index]}
        for index, (subject, session) in enumerate(
            [(s, p) for s in ["01", "02"] for p in ["pre", "post"]]
        )
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "session"],
        measures=["Signal"],
        name="Signal",
        schema_levels=["subject", "session"],
    )


def test_a_two_factor_table_frees_the_innermost_key_for_a_distribution():
    """`subject=X, session=COLOR` is one point per combination — nothing to
    distribute. The INNERMOST key is freed: its levels are replicates of each
    other, where freeing `subject` would pool across people."""
    table = _two_factor_table()
    opened = PlotSpec(
        measures=["Signal"], kind=PlotKind.LINE, roles=default_roles(table, "Signal")
    )

    suggested = roles_for_kind(opened, table, PlotKind.VIOLIN)

    assert suggested == {"subject": Role.X, "session": Role.FREE}


def test_a_one_factor_table_frees_its_only_factor():
    """One point per violin is not a violin. With nothing else to free, the
    single factor leaves the axis and the figure is one distribution."""
    frame = pd.DataFrame(
        {"subject": ["01", "02", "03"], "Signal": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]}
    )
    table = LongTable.from_frame(
        frame,
        factors=["subject"],
        measures=["Signal"],
        name="Signal",
        schema_levels=["subject"],
    )
    opened = PlotSpec(
        measures=["Signal"], kind=PlotKind.LINE, roles=default_roles(table, "Signal")
    )

    assert roles_for_kind(opened, table, PlotKind.VIOLIN) == {"subject": Role.FREE}


def test_a_non_distribution_kind_is_not_given_replicates():
    """Only the kinds that summarize need a FREE factor; a scatter is a figure
    with one point per record, and freeing a factor for it would silently
    change what the axis means."""
    table = _two_factor_table()
    opened = PlotSpec(
        measures=["Signal"], kind=PlotKind.LINE, roles=default_roles(table, "Signal")
    )

    assert roles_for_kind(opened, table, PlotKind.SCATTER) == {
        "subject": Role.X,
        "session": Role.COLOR,
    }


def test_a_distribution_kind_is_selectable_from_the_opening_state(series_table):
    """The loop that has to close: a 1-D measure opens with every key iterated,
    so there are no replicates — and the kind whose selection would supply them
    must still be clickable. Each kind is judged against the roles it brings."""
    opened = PlotSpec(
        measures=["Signal"],
        kind=PlotKind.LINE,
        roles=default_roles(series_table, "Signal"),
    )

    report = capabilities(opened, series_table)

    assert report["has_replicates"] is False, "as the spec stands"
    assert "violin" in report["available"], "but the click is still reachable"
    kinds = {info["kind"]: info for info in report["kinds"]}
    assert kinds["violin"]["reason"] is None
    assert Role.FREE.value in (kinds["violin"]["roles"] or {}).values()


def test_a_kind_with_no_suggestion_is_still_judged_on_the_current_roles(series_table):
    """BAND needs replicates and does not change the shape, so it brings no
    roles — and must stay refused while everything is iterated."""
    opened = PlotSpec(
        measures=["Signal"],
        kind=PlotKind.LINE,
        roles=default_roles(series_table, "Signal"),
    )

    kinds = {info["kind"]: info for info in capabilities(opened, series_table)["kinds"]}

    assert kinds["band"]["available"] is False
    assert "replicates" in (kinds["band"]["reason"] or "")
