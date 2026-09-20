"""
The role dropdown is reported by the backend, not guessed by the GUI.

The panel used to offer every role for every factor while ``validate``
refused several of them, so picking one could produce an error instead of a
plot. ``capability.role_options`` answers both halves — what is legal, and what
to call it — and derives the first half by ASKING ``validate``.

The test that matters is the round trip: picking a role the menu offers
introduces no new complaint, and every role it refuses does. That is what makes
"the menu and the validator cannot disagree" a property rather than a hope.

Note "introduces no NEW complaint" rather than "is valid". A spec can already
be invalid for a reason no dropdown caused, and the panel has to stay usable
in that state, because assigning a role is how the user gets out of it.

Since 2026-09-17 the Factors dropdown holds three roles — Separate figures,
Separate panels, Collapse — and GROUP is the Grouping section's business
(docs/claude/grouping-and-collapse.md).
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from scistackplot import PlotSpec, Role, RoleError, role_options
from scistackplot.capability import (
    ROLE_ORDER,
    factor_summary,
    factors_menu,
    grouping_hint,
    grouping_summary,
    role_hint,
    role_label,
)
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
    """
    base = _error(spec, table)
    for factor in table.factor_names:
        for option in role_options(spec, table, factor):
            role = Role(option["role"])
            color = None if spec.color == factor and role is not Role.GROUP else spec.color
            candidate = replace(spec, roles={**spec.roles, factor: role}, color=color)
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
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
    )
    assert _error(spec, scalar_table) is None
    _assert_round_trip(spec, scalar_table)


def test_round_trip_on_a_1d_measure(series_table):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.GROUP, "trial": Role.COLLAPSE},
        kind=PlotKind.BAND,
    )
    assert _error(spec, series_table) is None
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
    to drift: `validate` refuses to collapse a variant, and that refusal is
    conditional on things no hardcoded menu could see."""
    spec = PlotSpec(measures=["Peak"], roles={}, kind=PlotKind.SCATTER)
    _assert_round_trip(spec, variant_table)


def test_a_variant_factor_cannot_be_collapsed_from_the_menu(variant_table):
    spec = PlotSpec(measures=["Peak"], roles={}, kind=PlotKind.SCATTER)
    options = _by_role(spec, variant_table, "bandpass.low_hz")
    assert options["collapse"]["available"] is False
    assert "variant" in options["collapse"]["reason"].lower()
    assert options["facet"]["available"] is True
    assert options["iterate"]["available"] is True


def test_the_last_collapsed_factor_cannot_leave_a_box_plot(scalar_table):
    """A box needs a sample. Moving its only collapsed factor to Separate
    figures is refused — with the reason, so the user knows what to do."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )
    assert _error(spec, scalar_table) is None
    options = _by_role(spec, scalar_table, "trial")
    assert options["iterate"]["available"] is False
    assert "sample" in options["iterate"]["reason"]
    assert options["collapse"]["available"] is True


def test_the_coloured_layer_can_leave_the_grouping(scalar_table):
    """Collapsing the coloured factor drops the colour with it; the option is
    judged on the role, not on a colour that no longer names a layer."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="subject",
        kind=PlotKind.BAR,
    )
    options = _by_role(spec, scalar_table, "subject")
    assert options["collapse"]["available"] is True
    assert options["iterate"]["available"] is True


# --- labels ------------------------------------------------------------------


def test_every_role_has_a_label_and_a_hint_for_every_shape():
    """A missing entry would render an empty option in the dropdown."""
    for shape in Shape:
        for role in Role:
            assert role_label(role, shape)
            assert role_hint(role, shape)


def test_the_labels_read_as_a_family():
    assert role_label(Role.ITERATE, Shape.SCALAR) == "Separate figures"
    assert role_label(Role.FACET, Shape.SCALAR) == "Separate panels"
    assert role_label(Role.COLLAPSE, Shape.SCALAR).startswith("Collapse")


def test_the_collapse_hint_names_the_sample():
    """What distinguishes Collapse from the others is what the LAST collapsed
    key does: it is the sample the error bars are drawn over."""
    for shape in (Shape.SCALAR, Shape.SERIES_1D):
        assert "sample" in role_hint(Role.COLLAPSE, shape)


def test_1d_collapse_is_described_as_traces():
    assert "trace" in role_hint(Role.COLLAPSE, Shape.SERIES_1D).lower()
    assert "trace" not in role_hint(Role.COLLAPSE, Shape.SCALAR).lower()


def test_grouping_hint_follows_the_kind():
    assert "bar" in grouping_hint(PlotKind.BAR, Shape.SCALAR)
    assert "line" in grouping_hint(PlotKind.LINE, Shape.SERIES_1D)
    assert "lines" in grouping_hint(PlotKind.SPAGHETTI, Shape.SCALAR)
    assert "heatmap" in grouping_hint(PlotKind.HEATMAP, Shape.MATRIX_2D).lower()


# --- the menu ----------------------------------------------------------------


def test_roles_are_reported_in_a_deliberate_order(series_table):
    """Figures, panels, then the one option that removes levels."""
    spec = PlotSpec(measures=["Signal"], roles={}, kind=PlotKind.LINE)

    listed = [o["role"] for o in role_options(spec, series_table, "subject")]

    assert listed == [str(r) for r in ROLE_ORDER]
    assert list(ROLE_ORDER) == [Role.ITERATE, Role.FACET, Role.COLLAPSE]
    # GROUP is the Grouping section's; it must never be a dropdown entry.
    assert Role.GROUP not in ROLE_ORDER


def test_an_already_invalid_spec_does_not_forbid_every_role(scalar_table):
    """A spec broken for an UNRELATED reason (a colour naming no layer) is shown
    by the panel as its own error. It must not also make every role in every
    dropdown look forbidden — the user could no longer click their way out."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP},
        color="trial",
        kind=PlotKind.SCATTER,
    )
    assert _error(spec, scalar_table) is not None

    options = _by_role(spec, scalar_table, "session")

    assert options["facet"]["available"] is True
    assert options["iterate"]["available"] is True
    assert options["collapse"]["available"] is True


def test_group_is_not_in_the_factors_dropdown(scalar_table):
    spec = PlotSpec(measures=["StepLength"], roles={}, kind=PlotKind.SCATTER)
    assert {o["role"] for o in factors_menu(spec, scalar_table, "session")} == {
        "iterate", "facet", "collapse",
    }


def test_a_grouped_factor_still_shows_group_greyed(scalar_table):
    """A <select> whose value is not among its options renders blank."""
    spec = PlotSpec(measures=["StepLength"], roles={"trial": Role.GROUP}, kind=PlotKind.BAR)
    options = {o["role"]: o for o in factors_menu(spec, scalar_table, "trial")}
    assert "group" in options
    assert options["group"]["available"] is False
    assert "Grouping section" in options["group"]["reason"]


def test_factors_report_whether_they_can_join_the_grouping(scalar_table):
    spec = PlotSpec(measures=["StepLength"], roles={}, kind=PlotKind.SCATTER)
    for entry in factor_summary(spec, scalar_table):
        assert entry["group_available"] is True
        assert entry["group_reason"] is None


# --- the Grouping section ----------------------------------------------------


def test_grouping_is_offered_for_a_scalar_measure(scalar_table):
    report = grouping_summary(
        PlotSpec(measures=["StepLength"], roles={}, kind=PlotKind.SCATTER),
        scalar_table,
    )
    assert report["available"] is True
    assert report["reason"] is None
    assert report["max_labelled_layers"] == 3


def test_grouping_is_offered_for_a_1d_measure_as_series(series_table):
    """Its x axis is the within-observation index, so the layers are series,
    not ticks — and the hint says so."""
    spec = PlotSpec(
        measures=["Signal"], roles={"subject": Role.GROUP}, kind=PlotKind.LINE
    )
    report = grouping_summary(spec, series_table)
    assert report["available"] is True
    assert report["ticks"] == []
    assert report["series"] == ["subject"]
    assert "line" in report["hint"]


def test_grouping_is_refused_for_a_2d_measure(matrix_table):
    report = grouping_summary(
        PlotSpec(measures=["Coherence"], roles={}, kind=PlotKind.HEATMAP), matrix_table
    )
    assert report["available"] is False
    assert "2-D" in report["reason"]


def test_grouping_reports_the_layers_innermost_first(scalar_table):
    """Membership and order reconciled the same way the figure does it."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP},
        # Deliberately naming a factor that no longer holds GROUP, which
        # `ordered_groups` has to absorb.
        groups=["session", "trial", "subject"],
        color="session",
        kind=PlotKind.BAR,
    )
    report = grouping_summary(spec, scalar_table)
    assert report["layers"] == ["session", "subject"]
    assert report["ticks"] == ["subject"], "the coloured layer dodges, it is not a tick"
    assert report["color"] == "session"
    assert report["labelled_layers"] == 1


def test_grouping_appears_in_the_capability_report(scalar_table):
    from scistackplot import capabilities

    report = capabilities(
        PlotSpec(measures=["StepLength"], roles={}, kind=PlotKind.SCATTER),
        scalar_table,
    )
    assert report["grouping"]["available"] is True
    assert report["collapse"] == {"order": [], "sample": None, "pooled": False}
    assert report["has_sample"] is False


def test_the_capability_report_names_the_sample(scalar_table):
    from scistackplot import capabilities

    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        kind=PlotKind.BAR,
    )
    report = capabilities(spec, scalar_table)
    assert report["collapse"]["order"] == ["trial", "subject"]
    assert report["collapse"]["sample"] == "subject"
    assert report["has_sample"] is True


def test_a_kind_suggestion_carries_the_whole_assignment(series_table):
    """The GUI applies roles, groups AND colour synchronously with the click."""
    from scistackplot import capabilities, default_spec

    report = capabilities(default_spec(series_table), series_table)
    kinds = {k["kind"]: k for k in report["kinds"]}
    assert kinds["violin"]["assignment"] is not None
    assert set(kinds["violin"]["assignment"]) == {"roles", "groups", "color"}
    assert kinds["line"]["assignment"] is None, "no shape change, no suggestion"


def test_the_report_never_offers_a_kind_the_validator_refuses_for_layers(scalar_table):
    """Four grouping layers exceed MAX_X_LAYERS. `validate` refused the bar,
    but the capability report still offered it — so the panel showed a kind
    that then failed to draw (role-assignment sweep, 2026-09-19). One rule,
    `roles.layer_cap_reason`, now feeds both."""
    from scistackplot import PlotKind, PlotSpec, Role, RoleError, capabilities
    from scistackplot.roles import validate

    frame = scalar_table.frame.assign(limb=["L", "R"] * (len(scalar_table.frame) // 2))
    from scistackplot import LongTable

    table = LongTable.from_frame(
        frame, factors=["subject", "session", "trial", "limb"], measures=["StepLength"],
        name="StepLength", schema_levels=["subject", "session", "trial"],
    )
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.GROUP, "limb": Role.GROUP},
        groups=["limb", "trial", "session", "subject"],
        kind=PlotKind.BAR,
    )
    with pytest.raises(RoleError, match="labelled tick layers"):
        validate(spec, table)
    report = capabilities(spec, table)
    bar = next(e for e in report["kinds"] if e["kind"] == "bar")
    assert bar["available"] is False
    assert "labelled tick layers" in bar["reason"]
    assert "bar" not in report["available"]
