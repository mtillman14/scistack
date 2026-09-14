"""
Stage 5: the role dropdown is reported by the backend, not guessed by the GUI.

The panel used to offer all six roles for every factor while ``validate``
refused several of them, so picking one could produce an error instead of a
plot. ``capability.role_options`` answers both halves — what is legal, and what
to call it — and derives the first half by ASKING ``validate``.

The test that matters is the round trip: picking a role the menu offers
introduces no new complaint, and every role it refuses does. That is what makes
"the menu and the validator cannot disagree" a property rather than a hope.

Note "introduces no NEW complaint" rather than "is valid". A spec can already
be invalid for a reason no dropdown caused — an unassigned variant factor is
refused outright — and the panel has to stay usable in that state, because
assigning a role is how the user gets out of it.
"""

from __future__ import annotations

from scistackplot import PlotSpec, Role, RoleError, role_options
from scistackplot.capability import role_hint, role_label
from scistackplot.roles import validate
from scistackplot.shape import Shape
from scistackplot.spec import PlotKind


def _error(spec: PlotSpec, table) -> str | None:
    try:
        validate(spec, table)
    except RoleError as exc:
        return str(exc)
    return None


def _by_role(spec, table, factor) -> dict:
    """One factor's reported options, keyed by role name."""
    return {o["role"]: o for o in role_options(spec, table, factor)}


def _assert_round_trip(spec: PlotSpec, table) -> None:
    """Every reported option agrees with what `validate` actually does.

    The property is "picking this role introduces no NEW problem", not "the
    resulting spec is valid" — those differ when the spec is ALREADY invalid
    for an unrelated reason, which is a state the panel has to stay usable in.
    A table with an unassigned variant factor starts out that way: every spec
    is refused until the variant is given a channel, and if that made every
    role in every dropdown unavailable the user could not click their way out.
    """
    from dataclasses import replace

    base = _error(spec, table)
    for factor in table.factor_names:
        for option in role_options(spec, table, factor):
            candidate = replace(
                spec, roles={**spec.roles, factor: Role(option["role"])}
            )
            error = _error(candidate, table)
            if option["available"]:
                assert error is None or error == base, (
                    f"{factor}={option['role']} was offered but introduces: {error}"
                )
            else:
                assert error is not None and error != base, (
                    f"{factor}={option['role']} was refused but is legal"
                )
                assert option["reason"], "a refusal must say why"


# --- the round trip, over every shape --------------------------------------


def test_round_trip_on_a_scalar_measure(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.X, "session": Role.COLOR},
        kind=PlotKind.SCATTER,
    )
    _assert_round_trip(spec, scalar_table)


def test_round_trip_on_a_1d_measure(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.COLOR},
        kind=PlotKind.LINE,
    )
    _assert_round_trip(spec, series_table)


def test_round_trip_on_a_struct_measure(struct_table):
    spec = PlotSpec(
        measures=["RawEMG"],
        roles={"ColName": Role.FACET},
        kind=PlotKind.LINE,
    )
    _assert_round_trip(spec, struct_table)


def test_round_trip_with_a_variant_factor(variant_table):
    """The variant factor is where availability and validation are most likely
    to drift: `validate` refuses to pool variants silently, and that refusal is
    conditional on things no hardcoded menu could see."""
    spec = PlotSpec(measures=["Peak"], roles={}, kind=PlotKind.SCATTER)
    _assert_round_trip(spec, variant_table)


def test_round_trip_once_the_variant_factor_is_assigned(variant_table):
    """The same table in the state the user reaches after fixing it — where
    the base spec IS valid, so every refusal is genuinely this role's fault."""
    spec = PlotSpec(
        measures=["Peak"],
        roles={"bandpass.low_hz": Role.COLOR},
        kind=PlotKind.SCATTER,
    )
    assert _error(spec, variant_table) is None
    _assert_round_trip(spec, variant_table)


def test_an_unassigned_variant_factor_does_not_freeze_every_dropdown(variant_table):
    """A table whose variant factor has no role yet is refused outright, and
    the panel shows that as its own error. The dropdowns must stay usable —
    assigning the variant a channel is how the user fixes it, and they cannot
    do that from a menu where everything is greyed out."""
    spec = PlotSpec(measures=["Peak"], roles={}, kind=PlotKind.SCATTER)
    assert _error(spec, variant_table) is not None

    options = _by_role(spec, variant_table, "bandpass.low_hz")

    assert options["color"]["available"] is True
    assert options["facet"]["available"] is True
    assert options["iterate"]["available"] is True


# --- the specific refusals the GUI used to let users walk into -------------


def test_x_is_refused_for_a_1d_measure(series_table):
    """Its x axis is the within-observation index; no factor can take it."""
    spec = PlotSpec(measures=["Signal"], roles={}, kind=PlotKind.LINE)

    option = _by_role(spec, series_table, "subject")["x"]

    assert option["available"] is False
    assert "1-D" in option["reason"]


def test_x_is_offered_for_a_scalar_measure(scalar_table):
    spec = PlotSpec(measures=["StepLength"], roles={}, kind=PlotKind.SCATTER)

    assert _by_role(spec, scalar_table, "subject")["x"]["available"] is True


def test_color_is_refused_when_another_factor_holds_it(scalar_table):
    """COLOR takes one factor. The panel does not move it off the previous
    holder, so offering it twice produces an error rather than a plot."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.COLOR},
        kind=PlotKind.SCATTER,
    )

    option = _by_role(spec, scalar_table, "subject")["color"]

    assert option["available"] is False
    assert "one factor" in option["reason"]


def test_color_is_offered_to_the_factor_that_already_holds_it(scalar_table):
    """Re-selecting the current value must never be refused."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.COLOR},
        kind=PlotKind.SCATTER,
    )

    assert _by_role(spec, scalar_table, "session")["color"]["available"] is True


def test_aggregate_and_free_stay_available_on_1d(series_table):
    """The user's decision, recorded 2026-09-11: both do real work on 1-D data
    — FREE is what `available_plots` keys BAND off, AGGREGATE averages traces
    together — so they are relabelled, never removed."""
    spec = PlotSpec(measures=["Signal"], roles={}, kind=PlotKind.LINE)

    options = _by_role(spec, series_table, "subject")

    assert options["aggregate"]["available"] is True
    assert options["free"]["available"] is True


# --- labels ----------------------------------------------------------------


def test_1d_aggregate_is_named_for_traces_not_for_rows():
    assert role_label(Role.AGGREGATE, Shape.SERIES_1D) == "Average into one line"


def test_scalar_aggregate_keeps_the_statistical_wording():
    assert role_label(Role.AGGREGATE, Shape.SCALAR) == "Average over"


def test_free_is_called_free_for_every_shape():
    """The dropdown said "Replicates" (scalars) and "One line each" (1-D), so
    the role a user has read about as FREE — in the docs, in a saved spec, in
    an exported `roles=` — appeared nowhere by that name (user, 2026-09-13).
    What a FREE factor DOES varies by shape, and that is the hint's job."""
    for shape in Shape:
        assert role_label(Role.FREE, shape) == "Free"
    assert role_hint(Role.FREE, Shape.SCALAR) != role_hint(Role.FREE, Shape.SERIES_1D)


def test_aggregate_and_free_hints_are_stated_as_a_contrast():
    """The two roles a user cannot tell apart from the labels alone.

    "Average over" and "Free" both read as "not on an axis", and hints that
    described each one on its own — both using the word "average" — did not
    separate them (user, 2026-09-13). What separates them is the error bars:
    AGGREGATE collapses first and does NOT widen them, FREE keeps each level
    and DOES. Both sides must say so, in the same terms, for every shape.
    """
    for shape in (Shape.SCALAR, Shape.SERIES_1D):
        collapse = role_hint(Role.AGGREGATE, shape)
        keep = role_hint(Role.FREE, shape)
        assert "does NOT widen the error" in collapse, (shape, collapse)
        assert "DOES widen the error" in keep, (shape, keep)


def test_the_contrast_is_the_statistic_the_plot_kind_cannot_express():
    """Not a wording preference — the two roles produce different numbers.

    With [subject, trial] and a band: trial=AGGREGATE averages each subject's
    trials first, so the band is the spread across SUBJECTS; trial=FREE pools
    every subject-trial trace. Same kind, same data, different error bars —
    which is why both roles exist. Pinned as behaviour so the hints cannot
    become a claim the reduction stops making.
    """
    import numpy as np
    import pandas as pd
    from scistackplot import LongTable, PlotKind, PlotSpec, resolve
    from scistackplot.resolved import Y_HIGH, Y_LOW
    from scistackplot.spec import Aggregation, ErrorBand, Statistic

    # s1 has 4 nearly identical trials, s2 has 1 — so pooling weights s1 four
    # times and shrinks the spread relative to averaging within subject first.
    rows = []
    for trial, offset in enumerate([0.0, 0.01, -0.01, 0.02]):
        rows.append({"subject": "s1", "trial": str(trial), "v": np.array([1.0 + offset])})
    rows.append({"subject": "s2", "trial": "0", "v": np.array([5.0])})
    frame = pd.DataFrame(rows)
    table = LongTable.from_frame(frame, factors=["subject", "trial"], measures=["v"])
    band = Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD)

    def spread(trial_role: Role) -> float:
        spec = PlotSpec(
            measures=["v"],
            roles={"subject": Role.FREE, "trial": trial_role},
            kind=PlotKind.BAND,
            aggregate=band,
        )
        panel = resolve(spec, table)[0].panels[0].frame
        return float(panel[Y_HIGH].iloc[0] - panel[Y_LOW].iloc[0])

    pooled = spread(Role.FREE)
    within_first = spread(Role.AGGREGATE)
    assert within_first > pooled, (within_first, pooled)


def test_every_role_has_a_label_and_a_hint_for_every_shape():
    """A missing entry would render an empty option in the dropdown."""
    for shape in Shape:
        for role in Role:
            assert role_label(role, shape)
            assert role_hint(role, shape)


def test_role_options_are_reported_for_every_factor(series_table):
    """Every factor gets a complete dropdown — an empty one is a dead control.

    "Complete" is FACTOR_ROLE_ORDER, not every ``Role``: Stage 6 moved X out
    of this menu and into the Grouping section, reported per factor as
    ``x_available``. A factor already ON x also lists it, so the select can
    display its own value (see `factors_menu`).
    """
    from scistackplot import capabilities
    from scistackplot.capability import FACTOR_ROLE_ORDER

    report = capabilities(
        PlotSpec(measures=["Signal"], roles={}, kind=PlotKind.LINE), series_table
    )

    for factor in report["factors"]:
        assert {o["role"] for o in factor["roles"]} == {
            str(r) for r in FACTOR_ROLE_ORDER
        }
        assert "x_available" in factor


def test_roles_are_reported_in_a_deliberate_order(series_table):
    """Not `Role`'s declaration order, which is grouped by what each role does
    to the data and leads with "Separate figures"."""
    from scistackplot.capability import FACTOR_ROLE_ORDER, ROLE_ORDER

    spec = PlotSpec(measures=["Signal"], roles={}, kind=PlotKind.LINE)

    listed = [o["role"] for o in role_options(spec, series_table, "subject")]

    assert listed == [str(r) for r in ROLE_ORDER]
    assert ROLE_ORDER[0] is Role.X
    # A role missing from ROLE_ORDER can never be reported at all.
    assert set(ROLE_ORDER) == set(Role)
    # The Factors menu is that order minus X, relative order intact — so
    # removing X did not quietly reshuffle the rest.
    assert list(FACTOR_ROLE_ORDER) == [r for r in ROLE_ORDER if r is not Role.X]


def test_an_already_invalid_spec_does_not_forbid_every_role(scalar_table):
    """A spec broken for an UNRELATED reason (two factors on colour) is shown
    by the panel as its own error. It must not also make every role in every
    dropdown look forbidden — the user could no longer click their way out."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.COLOR, "session": Role.COLOR},
        kind=PlotKind.SCATTER,
    )

    options = _by_role(spec, scalar_table, "trial")

    # Assigning `trial` a facet role leaves the colour clash untouched, so it
    # is not this option's fault and must not be reported against it.
    assert options["facet"]["available"] is True
    assert options["iterate"]["available"] is True


# --- Stage 6: the x axis is the Grouping section's question ----------------


def test_x_is_not_in_the_factors_dropdown(scalar_table):
    """Membership and order are one question, asked in one place. Leaving X in
    the per-factor dropdown as well is how the two controls start disagreeing."""
    from scistackplot.capability import factor_summary

    spec = PlotSpec(measures=["StepLength"], roles={}, kind=PlotKind.SCATTER)

    for entry in factor_summary(spec, scalar_table):
        assert "x" not in {o["role"] for o in entry["roles"]}


def test_a_factor_already_on_x_still_shows_it(scalar_table):
    """A <select> whose value is absent from its options renders blank, and a
    scalar table opens with one factor on X by default.

    So X is listed for that factor — and reported unavailable even though it
    is LEGAL, which is the one place the menu deliberately says something
    `validate` does not. Displaying the current value and offering to set it
    are different things, and setting it belongs to Grouping.
    """
    from scistackplot.capability import factors_menu

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.X},
        kind=PlotKind.SCATTER,
    )

    listed = {o["role"]: o for o in factors_menu(spec, scalar_table, "subject")}

    assert "x" in listed, "the select would render blank without it"
    assert listed["x"]["available"] is False
    assert "Grouping" in listed["x"]["reason"]
    # The capability layer still reports the truth: it IS legal.
    assert _by_role(spec, scalar_table, "subject")["x"]["available"] is True


def test_a_factor_not_on_x_does_not_list_it(scalar_table):
    from scistackplot.capability import factors_menu

    spec = PlotSpec(measures=["StepLength"], roles={}, kind=PlotKind.SCATTER)

    listed = {o["role"] for o in factors_menu(spec, scalar_table, "subject")}

    assert "x" not in listed


def test_factors_report_whether_they_can_group_x(scalar_table):
    from scistackplot.capability import factor_summary

    spec = PlotSpec(measures=["StepLength"], roles={}, kind=PlotKind.SCATTER)

    for entry in factor_summary(spec, scalar_table):
        assert entry["x_available"] is True
        assert entry["x_reason"] is None


def test_grouping_is_offered_for_a_scalar_measure(scalar_table):
    from scistackplot import grouping_summary

    report = grouping_summary(
        PlotSpec(measures=["StepLength"], roles={}, kind=PlotKind.SCATTER),
        scalar_table,
    )

    assert report["available"] is True
    assert report["reason"] is None
    assert report["max_layers"] == 3


def test_grouping_is_refused_for_a_1d_measure(series_table):
    """Its x axis is the within-observation index, not a grouping."""
    from scistackplot import grouping_summary

    report = grouping_summary(
        PlotSpec(measures=["Signal"], roles={}, kind=PlotKind.LINE), series_table
    )

    assert report["available"] is False
    assert "1-D" in report["reason"]


def test_grouping_is_refused_when_a_measure_supplies_x(scalar_table):
    """A joined x measure is a measured value; grouping it would mean nothing."""
    from scistackplot import grouping_summary

    frame = scalar_table.frame.copy()
    frame["Speed"] = 1.0
    from scistackplot import LongTable

    table = LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial"],
        measures=["StepLength", "Speed"],
        name="StepLength",
    )
    spec = PlotSpec(
        measures=["StepLength"], x_measure="Speed", roles={}, kind=PlotKind.SCATTER
    )

    report = grouping_summary(spec, table)

    assert report["available"] is False
    assert "Speed" in report["reason"]


def test_grouping_reports_the_layers_in_nesting_order(scalar_table):
    """Membership and order reconciled the same way the figure does it."""
    from scistackplot import grouping_summary

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.X, "session": Role.X},
        # Deliberately out of declaration order, and naming a factor that no
        # longer holds X — both of which `ordered_x_layers` has to absorb.
        x_layers=["session", "trial", "subject"],
        kind=PlotKind.SCATTER,
    )

    assert grouping_summary(spec, scalar_table)["layers"] == ["session", "subject"]


def test_grouping_appears_in_the_capability_report(scalar_table):
    from scistackplot import capabilities

    report = capabilities(
        PlotSpec(measures=["StepLength"], roles={}, kind=PlotKind.SCATTER),
        scalar_table,
    )

    assert report["grouping"]["available"] is True
