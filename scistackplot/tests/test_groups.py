"""
Combines: combining a factor's levels into a new factor that REPLACES it.

``session ∈ {pre, post1, post2}`` → ``Phase ∈ {baseline, post}``, without
editing data. A derived table like ``apply_variant_sets``, so everything
downstream sees one ordinary factor (docs/claude/synthetic-factors.md). The
combine holds its source's slot and the source is collapsed away
(``.claude/plot-studio-combine-section.md``).
"""

from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, capabilities, resolve
from scistackplot.groups import apply_level_groups
from scistackplot.spec import LevelGroup

PHASES = {"pre": "baseline", "post": "post"}


@pytest.fixture
def phased_table(scalar_frame) -> LongTable:
    """Four session levels that bucket into two phases."""
    frames = []
    for session in ["pre", "post1", "post2"]:
        frames.append(scalar_frame.assign(session=session))
    return LongTable.from_frame(
        pd.concat(frames, ignore_index=True),
        factors=["subject", "session", "trial"],
        measures=["StepLength"],
        schema_levels=["subject", "session", "trial"],
    )


def _group(**kwargs) -> LevelGroup:
    base = dict(
        name="Phase",
        source="session",
        mapping={"pre": "baseline", "post1": "post", "post2": "post"},
        unmatched=None,
    )
    base.update(kwargs)
    return LevelGroup(**base)


def test_levels_bucket_into_a_new_factor(phased_table):
    spec = PlotSpec(measures=["StepLength"], level_groups=[_group()])

    derived = apply_level_groups(spec, phased_table)

    assert derived.has_factor("Phase")
    assert [str(level) for level in derived.factor("Phase").levels] == [
        "baseline",
        "post",
    ]
    mapped = derived.frame.groupby("Phase")["session"].nunique().to_dict()
    assert mapped == {"baseline": 1, "post": 2}


def test_the_source_stays_in_the_table_marked_replaced(phased_table):
    """The source's rows are still needed — they are averaged within each
    combined level — but it is marked, so the panel lists it only through the
    combine's dropdown."""
    spec = PlotSpec(measures=["StepLength"], level_groups=[_group()])

    derived = apply_level_groups(spec, phased_table)

    assert derived.factor("session").combined_into == "Phase"
    assert derived.factor("Phase").combined_from == "session"


def test_unmatched_levels_are_dropped_by_default(phased_table):
    """"Just these two groups, ignore the rest" is the common intent."""
    spec = PlotSpec(
        measures=["StepLength"],
        level_groups=[_group(mapping={"pre": "baseline"})],
    )

    derived = apply_level_groups(spec, phased_table)

    assert set(derived.frame["session"]) == {"pre"}
    assert set(derived.frame["Phase"]) == {"baseline"}


def test_unmatched_levels_can_go_to_a_bucket(phased_table):
    spec = PlotSpec(
        measures=["StepLength"],
        level_groups=[_group(mapping={"pre": "baseline"}, unmatched="other")],
    )

    derived = apply_level_groups(spec, phased_table)

    assert set(derived.frame["Phase"]) == {"baseline", "other"}
    # The catch-all sorts last, after the buckets the user named.
    assert [str(v) for v in derived.factor("Phase").levels] == ["baseline", "other"]
    assert len(derived.frame) == len(phased_table.frame)


def test_a_group_never_leaves_rows_unlabelled(phased_table):
    """A NaN group would become its own silent series in every legend."""
    spec = PlotSpec(
        measures=["StepLength"],
        level_groups=[_group(mapping={"pre": "baseline"}, unmatched="other")],
    )

    derived = apply_level_groups(spec, phased_table)

    assert derived.frame["Phase"].notna().all()


def test_level_order_is_declared_not_observed(phased_table):
    """A filter removing a group's last row must not reorder the legend."""
    spec = PlotSpec(
        measures=["StepLength"],
        level_groups=[
            _group(mapping={"post1": "post", "post2": "post", "pre": "baseline"})
        ],
    )

    derived = apply_level_groups(spec, phased_table)

    assert [str(v) for v in derived.factor("Phase").levels] == ["post", "baseline"]


def test_a_group_reading_a_missing_column_is_skipped(phased_table):
    """A spec outlives the table it was written against."""
    spec = PlotSpec(
        measures=["StepLength"], level_groups=[_group(source="gone")]
    )

    derived = apply_level_groups(spec, phased_table)

    assert not derived.has_factor("Phase")
    assert len(derived.frame) == len(phased_table.frame)


def test_no_groups_returns_the_same_table(phased_table):
    """A project that buckets nothing pays nothing."""
    spec = PlotSpec(measures=["StepLength"])

    assert apply_level_groups(spec, phased_table) is phased_table


# --- it is an ordinary factor from here on ---------------------------------


def test_the_derived_factor_takes_a_role(phased_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"Phase": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["Phase"],
        color="Phase",
        kind=PlotKind.BOX,
        level_groups=[_group()],
    )

    figures = resolve(spec, phased_table)

    assert len(figures) == 1
    assert figures[0].encoding.color is not None
    assert {str(v) for v in figures[0].color_order} == {"baseline", "post"}


def test_the_derived_factor_can_iterate(phased_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"Phase": Role.ITERATE, "subject": Role.COLLAPSE},
        kind=PlotKind.BOX,
        level_groups=[_group()],
    )

    figures = resolve(spec, phased_table)

    assert {f.figure_key["Phase"] for f in figures} == {"baseline", "post"}


def test_capabilities_offer_the_derived_factor(phased_table):
    """The panel builds its role dropdowns from this; a factor missing here
    cannot be assigned at all."""
    spec = PlotSpec(
        measures=["StepLength"], roles={"Phase": Role.GROUP}, level_groups=[_group()]
    )

    names = [f["name"] for f in capabilities(spec, phased_table)["factors"]]

    assert "Phase" in names


def test_generated_code_reproduces_the_bucketing(phased_table):
    """The endpoint receives the RAW table, so codegen must emit the same map.

    Reshaping done only on the interactive path is how an exported figure stops
    being the previewed one.
    """
    pytest.importorskip("seaborn")
    matplotlib = pytest.importorskip("matplotlib")

    from scistackplot import generate_plot_function

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"Phase": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
        level_groups=[_group()],
    )
    source = generate_plot_function(spec, phased_table)

    assert "'Phase'" in source and "map(_groups)" in source

    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    figure = namespace["plot_steplength"](phased_table.frame.copy(), "figure.png")
    assert figure.axes
    matplotlib.pyplot.close(figure)


# --- a combine REPLACES its source (2026-09-26) ------------------------------
#
# The combined factor holds the source's slot; the source is collapsed away, so
# each combined level is the average of the source levels in it. See
# .claude/plot-studio-combine-section.md.


@pytest.fixture
def stim_table() -> LongTable:
    """Two subjects, two stim conditions and a sham each.

    subject 01: stim1=1, stim2=3, sham=10  -> STIM 2,  SHAM 10
    subject 02: stim1=5, stim2=7, sham=20  -> STIM 6,  SHAM 20
    STIM (nested) = mean{2, 6} = 4;  SHAM = 15.
    """
    rows = [
        ("01", "stim1", 1.0),
        ("01", "stim2", 3.0),
        ("01", "sham", 10.0),
        ("02", "stim1", 5.0),
        ("02", "stim2", 7.0),
        ("02", "sham", 20.0),
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows, columns=["subject", "condition", "M"]),
        factors=["subject", "condition"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "condition"],
    )


def _stim(**kwargs) -> LevelGroup:
    base = dict(
        name="Stim",
        source="condition",
        mapping={"stim1": "STIM", "stim2": "STIM", "sham": "SHAM"},
    )
    base.update(kwargs)
    return LevelGroup(**base)


def _stim_spec(**kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={"Stim": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["Stim"],
        kind=PlotKind.BAR,
        level_groups=[_stim()],
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _bars(spec: PlotSpec, table: LongTable) -> dict[str, float]:
    from scistackplot.resolved import X, Y

    (figure,) = resolve(spec, table)
    (panel,) = figure.panels
    return {str(x): float(y) for x, y in zip(panel.frame[X], panel.frame[Y])}


def test_combined_depth_sits_just_above_the_source(stim_table):
    from scistackplot.groups import COMBINED_DEPTH_OFFSET

    derived = apply_level_groups(PlotSpec(measures=["M"], level_groups=[_stim()]), stim_table)

    source = derived.factor("condition").depth
    assert derived.factor("Stim").depth == source - COMBINED_DEPTH_OFFSET
    assert derived.factor("subject").depth < derived.factor("Stim").depth < source


def test_the_replaced_source_is_collapsed_whatever_the_spec_says(stim_table):
    from scistackplot.roles import complete_assignment

    spec = _stim_spec(
        roles={"Stim": Role.GROUP, "condition": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["Stim", "condition"],
    )
    derived = apply_level_groups(spec, stim_table)

    assignment = complete_assignment(spec, derived)

    assert assignment.roles["condition"] is Role.COLLAPSE
    assert assignment.groups == ["Stim"]


def test_a_combine_without_a_role_takes_its_sources(stim_table):
    """A spec written outside the panel (endpoint, hand-edited save) still
    reads as 'the combine holds the slot'."""
    from scistackplot.roles import complete_assignment

    spec = _stim_spec(roles={"condition": Role.FACET, "subject": Role.COLLAPSE}, groups=[])
    derived = apply_level_groups(spec, stim_table)

    roles = complete_assignment(spec, derived).roles

    assert roles["Stim"] is Role.FACET
    assert roles["condition"] is Role.COLLAPSE


def test_each_combined_level_is_the_nested_average_of_its_source_levels(stim_table):
    bars = _bars(_stim_spec(), stim_table)

    assert bars == pytest.approx({"STIM": 4.0, "SHAM": 15.0})


def test_the_collapse_chain_averages_the_source_before_the_sample(stim_table):
    from scistackplot.roles import collapse_order, complete_assignment

    spec = _stim_spec()
    derived = apply_level_groups(spec, stim_table)

    order = collapse_order(complete_assignment(spec, derived).roles, derived)

    assert order == ["condition", "subject"], "subject is the sample"


def test_an_inactive_combine_changes_nothing(stim_table):
    spec = _stim_spec(
        roles={"condition": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["condition"],
        level_groups=[_stim(active=False)],
    )

    derived = apply_level_groups(spec, stim_table)

    assert derived is stim_table
    assert set(_bars(spec, stim_table)) == {"stim1", "stim2", "sham"}


def test_switching_back_restores_rows_the_combine_dropped(stim_table):
    dropping = _stim(mapping={"stim1": "STIM", "stim2": "STIM"})  # sham in no bucket
    active = apply_level_groups(
        PlotSpec(measures=["M"], level_groups=[dropping]), stim_table
    )
    inactive = apply_level_groups(
        PlotSpec(measures=["M"], level_groups=[replace(dropping, active=False)]),
        stim_table,
    )

    assert "sham" not in set(active.frame["condition"])
    assert "sham" in set(inactive.frame["condition"])


def test_an_empty_label_means_no_bucket(stim_table):
    """The editor shows an empty label as (drop); it must never become a
    bucket literally named ''."""
    spec = PlotSpec(
        measures=["M"],
        level_groups=[_stim(mapping={"stim1": "STIM", "stim2": "STIM", "sham": ""})],
    )

    derived = apply_level_groups(spec, stim_table)

    assert [str(level) for level in derived.factor("Stim").levels] == ["STIM"]
    assert "sham" not in set(derived.frame["condition"])


def test_only_the_first_active_combine_of_a_source_applies(stim_table):
    spec = PlotSpec(
        measures=["M"],
        level_groups=[_stim(), _stim(name="Other", mapping={"sham": "S"})],
    )

    derived = apply_level_groups(spec, stim_table)

    assert derived.has_factor("Stim") and not derived.has_factor("Other")


def test_validate_refuses_two_active_combines_of_one_source(stim_table):
    from scistackplot.roles import RoleError, validate

    spec = _stim_spec(level_groups=[_stim(), _stim(name="Other")])

    with pytest.raises(RoleError, match="Two combines replace 'condition'"):
        validate(spec, apply_level_groups(spec, stim_table))


def test_validate_refuses_a_combine_named_after_an_existing_factor(stim_table):
    from scistackplot.roles import RoleError, validate

    spec = _stim_spec(
        roles={"subject": Role.COLLAPSE}, groups=[], level_groups=[_stim(name="subject")]
    )

    with pytest.raises(RoleError, match="name of an existing factor"):
        validate(spec, apply_level_groups(spec, stim_table))


def test_validate_names_the_combine_when_colour_is_on_the_replaced_source(stim_table):
    from scistackplot.roles import RoleError, validate

    spec = _stim_spec(color="condition")

    with pytest.raises(RoleError, match="combine 'Stim' replaces"):
        validate(spec, apply_level_groups(spec, stim_table))


# --- nesting: the combine lands where its source would -----------------------


def test_the_axis_never_shows_the_replaced_source(scalar_table):
    """Subjects combined into cohorts, grouped under session brackets: each
    bracket holds cohort ticks, never subject ones (the odd-ticks bug)."""
    cohort = LevelGroup(
        "Cohort", "subject", {"01": "A", "02": "A", "03": "B"}
    )
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"Cohort": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["Cohort", "session"],
        kind=PlotKind.BAR,
        level_groups=[cohort],
    )

    (figure,) = resolve(spec, scalar_table)

    assert figure.x_layers == ["session", "Cohort"]
    assert {label for label in figure.x_plan.tick_labels if label} == {"A", "B"}


def test_a_subject_does_not_recur_across_its_cohort(scalar_table):
    """A subject belongs to one cohort, so a line through 'subject 01' across
    cohort ticks would be invented. Before combines had a depth this was
    'cannot be told'."""
    from scistackplot.roles import line_recurrence

    cohort = LevelGroup("Cohort", "subject", {"01": "A", "02": "A", "03": "B"})
    derived = apply_level_groups(
        PlotSpec(measures=["StepLength"], level_groups=[cohort]), scalar_table
    )

    recurs, _reason = line_recurrence(derived, ["subject"], "Cohort")

    assert recurs is False


# --- the panel's dropdown ------------------------------------------------------


def test_capabilities_report_each_slots_choices(stim_table):
    spec = _stim_spec(level_groups=[_stim(), _stim(name="Other", active=False)])

    factors = {f["name"]: f for f in capabilities(spec, stim_table)["factors"]}

    assert factors["Stim"]["slot"] == "condition"
    assert factors["Stim"]["alternatives"] == ["condition", "Stim", "Other"]
    assert factors["Stim"]["combined_from"] == "condition"
    assert factors["condition"]["combined_into"] == "Stim"
    assert factors["subject"]["alternatives"] == ["subject"]


def test_generated_code_applies_only_active_combines_with_empty_labels_dropped(
    stim_table,
):
    """Export draws what the preview draws: same combines, same mapping."""
    from scistackplot import generate_plot_function

    spec = _stim_spec(
        level_groups=[
            _stim(mapping={"stim1": "STIM", "stim2": "STIM", "sham": ""}),
            _stim(name="Other", active=False),
        ]
    )

    source = generate_plot_function(spec, stim_table)

    # The emitted combine lines (the embedded spec JSON names every combine).
    assert "# condition -> Stim" in source
    assert "# condition -> Other" not in source
    assert "_groups = {'stim1': 'STIM', 'stim2': 'STIM'}" in source
