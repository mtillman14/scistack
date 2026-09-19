"""
The grouping model: an ordered list (innermost first), one coloured layer,
and how each kind READS that list.

* ``PlotSpec.ordered_groups`` reconciles membership (who holds GROUP) with the
  user's order, placing unordered holders by the data's nesting.
* ``roles.grouping_layers`` is the one place the innermost-first spec meets
  the outermost-first axis: ticks for categorical-x kinds, series for 1-D and
  x-y plots, and the spaghetti split (first entry = the lines).
* ``validate`` caps LABELLED tick layers — grouping minus the coloured one.
* Defaults open granular: the measure's own level grouped, the rest iterated;
  a 1-D measure on one record.
* A saved spec in the old vocabulary is refused by name, not translated.

See docs/claude/grouping-and-collapse.md.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import Aggregation, LongTable, PlotKind, PlotSpec, Role, RoleError
from scistackplot.roles import (
    Assignment,
    complete_assignment,
    default_assignment,
    grouping_layers,
    kind_requirement,
    role_for_new_grouping,
    roles_for_kind,
    validate,
    with_requirements_for,
)
from scistackplot.shape import Shape
from scistackplot.spec import MAX_X_LAYERS, LegacySpecError


# --- ordered_groups ----------------------------------------------------------


def test_the_list_is_the_users_order(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.GROUP},
        groups=["session", "subject", "trial"],
    )
    assert spec.ordered_groups(depths=scalar_table.factor_depths) == [
        "session", "subject", "trial",
    ]


def test_names_that_no_longer_group_are_dropped(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.COLLAPSE},
        groups=["session", "subject"],
    )
    assert spec.ordered_groups() == ["subject"]


def test_unordered_holders_are_placed_deeper_first(scalar_table):
    """Innermost first means the deeper key comes first: trial inside subject,
    which is the figure people ask for without pressing an arrow."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "trial": Role.GROUP},
    )
    assert spec.ordered_groups(depths=scalar_table.factor_depths) == ["trial", "subject"]


def test_an_explicit_order_beats_depth(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "trial": Role.GROUP},
        groups=["subject", "trial"],
    )
    assert spec.ordered_groups(depths=scalar_table.factor_depths) == ["subject", "trial"]


def test_a_factor_with_no_depth_goes_innermost(variant_table):
    """A variant axis is not in the hierarchy; it sits inside every tick —
    'v1 against v2 inside each subject'."""
    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.GROUP, "bandpass.low_hz": Role.GROUP},
    )
    assert spec.ordered_groups(depths=variant_table.factor_depths) == [
        "bandpass.low_hz", "subject",
    ]


# --- grouping_layers: how a kind reads the list --------------------------------


def _scalar_spec(kind=PlotKind.BAR, **kwargs) -> PlotSpec:
    return PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        kind=kind,
        **kwargs,
    )


def test_categorical_x_reads_the_list_as_ticks_outermost_first(scalar_table):
    layers = grouping_layers(_scalar_spec(), scalar_table)
    assert layers.ticks == ["session", "subject"], "drawing order: outermost first"
    assert layers.series == []


def test_spaghetti_first_entry_is_the_lines(scalar_table):
    layers = grouping_layers(_scalar_spec(kind=PlotKind.SPAGHETTI), scalar_table)
    assert layers.series == ["subject"], "one line per subject"
    assert layers.ticks == ["session"], "joined across sessions"


def test_a_1d_measure_reads_the_list_as_series(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        kind=PlotKind.BAND,
    )
    layers = grouping_layers(spec, series_table)
    assert layers.ticks == []
    assert layers.series == ["subject", "session"], "spec order kept"


def test_an_x_measure_reads_the_list_as_series(scalar_frame):
    frame = scalar_frame.assign(Speed=1.0)
    table = LongTable.from_frame(
        frame, factors=["subject", "session", "trial"], measures=["StepLength", "Speed"],
        schema_levels=["subject", "session", "trial"],
    )
    spec = PlotSpec(
        measures=["StepLength"], x_measure="Speed",
        roles={"subject": Role.GROUP, "session": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.SCATTER,
    )
    layers = grouping_layers(spec, table)
    assert layers.ticks == [] and layers.series == ["subject"]


def test_the_coloured_layer_is_not_a_tick(scalar_table):
    """It dodges inside its tick and the legend labels it."""
    layers = grouping_layers(_scalar_spec(color="subject"), scalar_table)
    assert layers.color == "subject"
    assert layers.ticks == ["session"]
    assert layers.labelled_ticks == ["session"]


# --- validate ----------------------------------------------------------------


def test_color_must_name_a_grouping_layer(scalar_table):
    with pytest.raises(RoleError, match="not a grouping layer"):
        validate(_scalar_spec(color="trial"), scalar_table)


def test_a_fourth_labelled_layer_is_refused(scalar_frame):
    frame = scalar_frame.assign(speed="fast")
    table = LongTable.from_frame(
        frame, factors=["subject", "session", "speed", "trial"], measures=["StepLength"],
        schema_levels=["subject", "session", "speed", "trial"],
    )
    roles = {n: Role.GROUP for n in ["subject", "session", "speed", "trial"]}
    spec = PlotSpec(measures=["StepLength"], roles=roles, kind=PlotKind.BAR)
    assert MAX_X_LAYERS == 3
    with pytest.raises(RoleError, match="labelled tick layers"):
        validate(spec, table)
    # Colouring one of them takes it off the tick labels: now legal.
    validate(PlotSpec(measures=["StepLength"], roles=roles, color="trial", kind=PlotKind.BAR), table)


def test_a_2d_measure_cannot_be_grouped(matrix_table):
    spec = PlotSpec(
        measures=[matrix_table.measure_names[0]],
        roles={"subject": Role.GROUP},
        kind=PlotKind.HEATMAP,
    )
    with pytest.raises(RoleError, match="2-D"):
        validate(spec, matrix_table)


def test_a_variant_factor_cannot_be_collapsed(variant_table):
    spec = PlotSpec(
        measures=["Peak"],
        roles={"subject": Role.GROUP, "bandpass.low_hz": Role.COLLAPSE},
        kind=PlotKind.BAR,
    )
    with pytest.raises(RoleError, match="cannot be collapsed"):
        validate(spec, variant_table)


# --- what each kind needs ----------------------------------------------------


@pytest.mark.parametrize("kind", [PlotKind.BOX, PlotKind.VIOLIN, PlotKind.BAND])
def test_distribution_kinds_need_a_sample(kind):
    assert kind_requirement(kind, Shape.SCALAR, {"a": Role.GROUP}, 1)
    assert kind_requirement(kind, Shape.SCALAR, {"a": Role.GROUP, "b": Role.COLLAPSE}, 1) is None


def test_bar_does_not_need_a_sample():
    """A bar of single values — no error bar — is a legitimate figure, and the
    opening state of a scalar measure."""
    assert kind_requirement(PlotKind.BAR, Shape.SCALAR, {"a": Role.GROUP}, 1) is None


def test_spaghetti_needs_two_layers():
    assert kind_requirement(PlotKind.SPAGHETTI, Shape.SCALAR, {"a": Role.GROUP}, 1)
    assert kind_requirement(PlotKind.SPAGHETTI, Shape.SCALAR, {"a": Role.GROUP}, 2) is None


def test_validate_refuses_a_kind_its_roles_cannot_draw(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.ITERATE, "session": Role.ITERATE, "trial": Role.GROUP},
        kind=PlotKind.BOX,
    )
    with pytest.raises(RoleError, match="Needs a sample"):
        validate(spec, scalar_table)


# --- defaults ----------------------------------------------------------------


def test_a_scalar_opens_on_its_own_level_and_iterates_the_rest(scalar_table):
    opened = default_assignment(scalar_table, "StepLength")
    assert opened.roles == {
        "subject": Role.ITERATE, "session": Role.ITERATE, "trial": Role.GROUP,
    }
    assert opened.groups == ["trial"]
    assert opened.color is None


def test_a_1d_measure_opens_on_one_record(series_table):
    opened = default_assignment(series_table, "Signal")
    assert set(opened.roles.values()) == {Role.ITERATE}
    assert opened.groups == []


def test_a_variant_factor_opens_as_the_innermost_coloured_layer(variant_table):
    opened = default_assignment(variant_table, "Peak")
    assert opened.roles == {"subject": Role.GROUP, "bandpass.low_hz": Role.GROUP}
    assert opened.groups == ["bandpass.low_hz", "subject"]
    assert opened.color == "bandpass.low_hz"


def test_fields_open_as_panels(struct_table):
    opened = default_assignment(struct_table, "RawEMG")
    assert opened.roles["ColName"] is Role.FACET


# --- completion --------------------------------------------------------------


def test_unmentioned_factors_iterate(scalar_table):
    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.GROUP})
    done = complete_assignment(spec, scalar_table)
    assert done.roles["subject"] is Role.ITERATE
    assert done.roles["session"] is Role.ITERATE
    assert done.groups == ["trial"]


def test_nothing_is_ever_promoted(scalar_table):
    """The old model promoted FREE ancestors of an iterated key to ITERATE.
    With every role explicit there is nothing silent to defend against: a
    grouped subject beside an iterated trial stays grouped."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.COLLAPSE, "trial": Role.ITERATE},
    )
    done = complete_assignment(spec, scalar_table)
    assert done.roles["subject"] is Role.GROUP
    assert done.roles["session"] is Role.COLLAPSE


def test_a_stale_color_is_dropped_by_completion(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"], roles={"trial": Role.GROUP}, color="subject"
    )
    assert complete_assignment(spec, scalar_table).color is None


# --- kind changes ------------------------------------------------------------


def test_choosing_a_distribution_kind_collapses_the_next_key_out(scalar_table):
    opened = default_assignment(scalar_table, "StepLength")  # trial grouped
    fixed = with_requirements_for(opened, scalar_table, PlotKind.BOX)
    assert fixed.roles["session"] is Role.COLLAPSE, "the key just outside the grouped one"
    assert fixed.roles["trial"] is Role.GROUP, "the mark the user is looking at is untouched"
    assert fixed.roles["subject"] is Role.ITERATE, "the figure count shrinks by one factor"


def test_choosing_spaghetti_pulls_a_key_in_as_the_lines(scalar_table):
    opened = default_assignment(scalar_table, "StepLength")
    fixed = with_requirements_for(opened, scalar_table, PlotKind.SPAGHETTI)
    assert fixed.groups == ["session", "trial"], "the deepest iterated key, innermost"
    assert fixed.roles["session"] is Role.GROUP


def test_a_hand_assigned_spec_is_never_re_defaulted(series_table):
    chosen = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.GROUP, "session": Role.FACET, "trial": Role.COLLAPSE},
        groups=["subject"],
    )
    assert roles_for_kind(chosen, series_table, PlotKind.VIOLIN) is None


def test_a_1d_default_re_defaults_for_a_scalar_kind(series_table):
    opened = default_assignment(series_table, "Signal").apply(
        PlotSpec(measures=["Signal"], kind=PlotKind.LINE)
    )
    suggested = roles_for_kind(opened, series_table, PlotKind.VIOLIN)
    assert isinstance(suggested, Assignment)
    assert suggested.groups == ["trial"]
    assert suggested.roles["session"] is Role.COLLAPSE, "the violin has a sample"


def test_a_ticked_grouping_groups(scalar_table):
    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.GROUP})
    assert role_for_new_grouping(spec, scalar_table) is Role.GROUP


def test_a_ticked_grouping_facets_when_the_ticks_are_full(scalar_frame):
    frame = scalar_frame.assign(speed="fast")
    table = LongTable.from_frame(
        frame, factors=["subject", "session", "speed", "trial"], measures=["StepLength"],
        schema_levels=["subject", "session", "speed", "trial"],
    )
    spec = PlotSpec(
        measures=["StepLength"],
        roles={n: Role.GROUP for n in ["subject", "session", "trial"]},
        kind=PlotKind.BAR,
    )
    assert role_for_new_grouping(spec, table) is Role.FACET


# --- serialization -----------------------------------------------------------


def test_groups_color_and_pooled_round_trip():
    spec = PlotSpec(
        measures=["M"],
        roles={"a": Role.GROUP, "b": Role.GROUP, "c": Role.COLLAPSE},
        groups=["b", "a"],
        color="a",
        aggregate=Aggregation(pooled=True),
    )
    back = PlotSpec.from_dict(spec.to_dict())
    assert back.groups == ["b", "a"]
    assert back.color == "a"
    assert back.aggregate.pooled is True
    assert back == spec


def test_pooled_defaults_off():
    assert PlotSpec.from_dict({"measures": ["M"]}).aggregate.pooled is False


@pytest.mark.parametrize(
    "raw",
    [
        {"measures": ["M"], "roles": {"a": "free"}},
        {"measures": ["M"], "roles": {"a": "aggregate"}},
        {"measures": ["M"], "roles": {"a": "x", "b": "color"}},
        {"measures": ["M"], "x_layers": ["a"]},
        {"measures": ["M"], "collapse_statistic": "mean"},
    ],
)
def test_the_old_vocabulary_is_refused_by_name(raw):
    """A clean break: 'free' on a bar meant the sample, which is now
    'collapse', and 'x_layers' ran the other way round — a mapping would be
    wrong in exactly the common cases. The message names the doc."""
    with pytest.raises(LegacySpecError, match="grouping-and-collapse"):
        PlotSpec.from_dict(raw)


def test_legacy_error_names_the_offending_role():
    with pytest.raises(LegacySpecError, match="subject='free'"):
        PlotSpec.from_dict({"measures": ["M"], "roles": {"subject": "free"}})
