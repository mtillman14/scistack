"""
"Show sample" — Stage 1: the spec fields and the rules in ``roles``.

The collapse chain is cut before the deepest checked key; a deeper key
implies every shallower collapsed key (a trial is a trial OF a subject);
the join is automatic from hierarchy depths. See
.claude/plan-show-sample.md and (after Stage 6) docs/claude/show-sample-overlay.md.
"""

from __future__ import annotations

import itertools
from dataclasses import replace

import pandas as pd
import pytest

from scistackplot import (
    OVERLAY_KINDS,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    RoleError,
    overlay_granularity,
    overlay_join,
    overlay_steps,
    overlay_unavailable,
    validate,
)
from scistackplot.roles import (
    complete_roles,
    grouping_layers,
    line_recurrence,
    spaghetti_sample_repeats,
)
from scistackplot.shape import Shape

SCHEMA = ["subject", "session", "speed", "trial", "cycle"]


@pytest.fixture
def gait() -> LongTable:
    """Two subjects x two sessions x two speeds x two trials x two cycles."""
    rows = []
    for values in itertools.product(["01", "02"], ["pre", "post"], ["slow", "fast"], ["1", "2"], ["1", "2"]):
        rows.append((*values, float(len(rows))))
    frame = pd.DataFrame(rows, columns=[*SCHEMA, "M"])
    return LongTable.from_frame(
        frame, factors=SCHEMA, measures=["M"], name="M", schema_levels=SCHEMA
    )


def _spec(show, kind=PlotKind.BAR, join=None, **kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={
            "subject": Role.COLLAPSE,
            "session": Role.GROUP,
            "speed": Role.ITERATE,
            "trial": Role.COLLAPSE,
            "cycle": Role.COLLAPSE,
        },
        groups=["session"],
        kind=kind,
        show_sample=list(show),
        join_sample=join,
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _steps(table, spec):
    return overlay_steps(spec, complete_roles(spec, table), table)


# --- the spec ---------------------------------------------------------------


def test_show_sample_round_trips_through_dict_and_json():
    spec = _spec(["trial"], join=False)
    for back in (PlotSpec.from_dict(spec.to_dict()), PlotSpec.from_json(spec.to_json())):
        assert back.show_sample == ["trial"]
        assert back.join_sample is False


def test_automatic_join_survives_a_round_trip_as_none():
    """The automatic setting is ABSENT in a saved spec (TOML has no null) and
    must come back as None, not False — False would silently override the rule."""
    spec = _spec(["subject"])
    assert "join_sample" not in spec.to_dict()
    assert PlotSpec.from_dict(spec.to_dict()).join_sample is None
    assert PlotSpec.from_dict({"measures": ["M"], "join_sample": True}).join_sample is True


def test_show_sample_round_trips_through_toml():
    pytest.importorskip("tomli_w")
    spec = _spec(["trial"], join=False)
    back = PlotSpec.from_toml(spec.to_toml())
    assert back.show_sample == ["trial"] and back.join_sample is False
    assert PlotSpec.from_toml(_spec(["subject"]).to_toml()).join_sample is None


def test_a_saved_spec_without_the_fields_opens_with_no_overlay():
    spec = PlotSpec.from_dict({"measures": ["M"]})
    assert spec.show_sample == [] and spec.join_sample is None


# --- the cut ----------------------------------------------------------------


def test_no_selection_means_no_overlay(gait):
    assert _steps(gait, _spec([])) is None


def test_checking_the_sample_key_shows_the_sample(gait):
    """``subject`` is the last key of the chain: everything inside it is
    averaged and one point per subject remains — the strip over the bars."""
    steps = _steps(gait, _spec(["subject"]))
    assert steps.averaged == ["cycle", "trial"]
    assert steps.shown == ["subject"]


def test_checking_a_deeper_key_cuts_the_chain_before_it(gait):
    steps = _steps(gait, _spec(["trial"]))
    assert steps.averaged == ["cycle"]
    assert steps.shown == ["subject", "trial"]  # outermost first
    assert steps.deepest_shown == "trial"


def test_checking_the_deepest_key_shows_the_raw_data(gait):
    steps = _steps(gait, _spec(["cycle"]))
    assert steps.averaged == []
    assert steps.shown == ["subject", "trial", "cycle"]


def test_a_deeper_key_implies_the_shallower_ones(gait):
    """``["subject", "trial"]`` and ``["trial"]`` are the same overlay: a
    trial's identity includes its subject, whatever boxes were ticked."""
    assert _steps(gait, _spec(["subject", "trial"])) == _steps(gait, _spec(["trial"]))
    assert _steps(gait, _spec(["cycle", "subject"])) == _steps(gait, _spec(["cycle"]))


def test_a_checked_key_that_is_not_collapsed_is_inert(gait):
    """The user moved ``subject`` to the grouping after ticking it: the tick
    is ignored, not refused — the spec never has to adjudicate a checkbox."""
    spec = _spec(["subject", "trial"], roles={
        "subject": Role.GROUP, "session": Role.GROUP, "speed": Role.ITERATE,
        "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE,
    }, groups=["subject", "session"])
    steps = _steps(gait, spec)
    assert steps.shown == ["trial"]
    assert steps.averaged == ["cycle"]
    only_inert = _spec(["session"])
    assert _steps(gait, only_inert) is None


def test_a_field_factor_is_deepest_of_all(struct_table):
    """ColName sits inside a record, so checking ``trial`` averages the
    muscles within each trial, and checking ``ColName`` shows the raw rows."""
    spec = PlotSpec(
        measures=["RawEMG"],
        roles={"subject": Role.GROUP, "trial": Role.COLLAPSE, "ColName": Role.COLLAPSE},
        groups=["subject"],
        kind=PlotKind.BOX,
        show_sample=["trial"],
    )
    steps = overlay_steps(spec, complete_roles(spec, struct_table), struct_table)
    assert steps.averaged == ["ColName"] and steps.shown == ["trial"]
    raw = overlay_steps(
        replace(spec, show_sample=["ColName"]), complete_roles(spec, struct_table), struct_table
    )
    assert raw.averaged == [] and raw.shown == ["trial", "ColName"]


# --- the join ---------------------------------------------------------------


def _join(table, spec):
    return overlay_join(spec, complete_roles(spec, table), table)


def test_a_key_above_the_x_layer_is_repeated_measures(gait):
    join = _join(gait, _spec(["subject"]))
    assert join.join is True and join.automatic is True
    assert "subject" in join.reason and "session" in join.reason


def test_a_key_below_the_x_layer_is_not(gait):
    for show in (["trial"], ["cycle"]):
        join = _join(gait, _spec(show))
        assert join.join is False and join.automatic is True
        assert "belongs to one session" in join.reason


def test_the_rule_reads_the_deepest_shown_key(gait):
    """x = speed (depth 3); collapsing session too and showing it gives
    points per (subject, session), and session (2) is above speed: joined."""
    spec = _spec(["session"], roles={
        "subject": Role.COLLAPSE, "session": Role.COLLAPSE, "speed": Role.GROUP,
        "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE,
    }, groups=["speed"])
    steps = _steps(gait, spec)
    assert steps.shown == ["subject", "session"]
    assert _join(gait, spec).join is True
    # ...and showing trial under the same x is below it: not joined.
    assert _join(gait, _spec(["trial"], roles=spec.roles, groups=["speed"])).join is False


def test_the_colour_layer_counts_as_a_grouping_layer(gait):
    """Colour = session (depth 2) as the bracket, trial (4) the innermost tick
    and so the span: subject (1) is above it, joined; cycle (5) is below it,
    points. Colour is paint — it neither adds nor removes a layer."""
    spec = _spec(["subject"], roles={
        "subject": Role.COLLAPSE, "session": Role.GROUP, "speed": Role.ITERATE,
        "trial": Role.GROUP, "cycle": Role.COLLAPSE,
    }, groups=["trial", "session"], color="session")
    assert _join(gait, spec).join is True
    spec = _spec(["cycle"], roles=spec.roles, groups=spec.groups, color="session")
    assert _join(gait, spec).join is False


def test_the_user_overrides_the_rule_either_way(gait):
    forced_on = _join(gait, _spec(["trial"], join=True))
    assert forced_on.join is True and forced_on.automatic is False
    forced_off = _join(gait, _spec(["subject"], join=False))
    assert forced_off.join is False and forced_off.automatic is False


def test_a_shown_key_with_no_depth_declines_to_join():
    """A derived bucket has no place in the hierarchy: the rule cannot tell
    whether its points repeat across the axis, so it says so and draws points."""
    frame = pd.DataFrame({
        "subject": ["01", "01", "02", "02"],
        "bucket": ["lo", "hi", "lo", "hi"],
        "M": [1.0, 2.0, 3.0, 4.0],
    })
    table = LongTable.from_frame(
        frame, factors=["subject", "bucket"], measures=["M"], name="M",
        schema_levels=["subject"],
    )
    spec = PlotSpec(
        measures=["M"], roles={"subject": Role.GROUP, "bucket": Role.COLLAPSE},
        groups=["subject"], kind=PlotKind.BAR, show_sample=["bucket"],
    )
    join = overlay_join(spec, complete_roles(spec, table), table)
    assert join.join is False and join.automatic is True
    assert "no place in the schema hierarchy" in join.reason


def test_no_overlay_means_no_join(gait):
    assert _join(gait, _spec([])).join is False




# --- the span: a line runs along the innermost tick, inside one bracket ----


def _gait_struct() -> LongTable:
    """subject x session x trial, each record a dict of two ColName fields —
    the user's case (2026-09-21): bars grouped [ColName, session]."""
    rows = []
    for subject, session, trial, field in itertools.product(
        ["01", "02"], ["pre", "post"], ["1", "2"], ["A", "B"]
    ):
        rows.append((subject, session, trial, field, float(len(rows))))
    frame = pd.DataFrame(rows, columns=["subject", "session", "trial", "ColName", "M"])
    return LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial", "ColName"],
        measures=["M"],
        field_factors=["ColName"],
        name="M",
        schema_levels=["subject", "session", "trial"],
    )


def _struct_spec(groups, show, **kwargs) -> PlotSpec:
    roles = {
        "subject": Role.COLLAPSE, "session": Role.COLLAPSE,
        "trial": Role.COLLAPSE, "ColName": Role.COLLAPSE,
    }
    for name in groups:
        roles[name] = Role.GROUP
    base = dict(
        measures=["M"], roles=roles, groups=list(groups), kind=PlotKind.BAR,
        show_sample=list(show),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def test_the_span_is_the_innermost_tick_and_the_rest_are_brackets():
    table = _gait_struct()
    spec = _struct_spec(["ColName", "session"], ["subject"])
    layers = grouping_layers(spec, table, complete_roles(spec, table))
    assert layers.ticks == ["session", "ColName"]
    assert layers.span == "ColName" and layers.brackets == ["session"]
    single = _struct_spec(["session"], ["subject"])
    layers = grouping_layers(single, table, complete_roles(single, table))
    assert layers.span == "session" and layers.brackets == []


def test_subjects_join_across_the_columns_inside_one_session():
    """The user's case: ColName innermost, session the bracket. A subject's
    line runs A -> B within pre and again within post — never pre·B -> post·A."""
    table = _gait_struct()
    spec = _struct_spec(["ColName", "session"], ["subject"])
    join = _join(table, spec)
    assert join.join is True and join.automatic is True, join.reason
    assert "every ColName" in join.reason and "within each session" in join.reason
    hint = overlay_granularity(
        _steps(table, spec), join, grouping_layers(spec, table, complete_roles(spec, table))
    )
    assert "across ColName within each session" in hint


def test_the_mirror_grouping_joins_across_sessions_inside_one_column():
    table = _gait_struct()
    join = _join(table, _struct_spec(["session", "ColName"], ["subject"]))
    assert join.join is True
    assert "every session" in join.reason and "within each ColName" in join.reason


def test_a_trial_recurs_across_the_columns_of_its_own_record():
    """subject is the bracket, ColName the span: trial 1 of subject 01 has a
    value in every column of that one record, so its points ARE a line —
    the case the old whole-axis rule refused ("a trial belongs to one subject")."""
    table = _gait_struct()
    join = _join(table, _struct_spec(["ColName", "subject"], ["trial"]))
    assert join.join is True, join.reason
    assert "Every record has every ColName" in join.reason


def test_a_column_only_grouping_joins_too():
    table = _gait_struct()
    join = _join(table, _struct_spec(["ColName"], ["subject"]))
    assert join.join is True and "within each" not in join.reason


def test_a_trial_under_a_session_span_stays_points_whatever_the_bracket():
    table = _gait_struct()
    join = _join(table, _struct_spec(["session", "ColName"], ["trial"]))
    assert join.join is False and "belongs to one session" in join.reason


def test_a_depth_less_plain_span_cannot_be_told(gait):
    """A derived bucket as the innermost tick: nothing says whether a subject
    has a value at every bucket, so the rule declines (points) and says why."""
    frame = pd.DataFrame({
        "subject": ["01", "01", "02", "02"],
        "session": ["pre", "post", "pre", "post"],
        "bucket": ["lo", "hi", "lo", "hi"],
        "M": [1.0, 2.0, 3.0, 4.0],
    })
    table = LongTable.from_frame(
        frame, factors=["subject", "session", "bucket"], measures=["M"], name="M",
        schema_levels=["subject", "session"],
    )
    spec = PlotSpec(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "bucket": Role.GROUP},
        groups=["bucket", "session"], kind=PlotKind.BAR, show_sample=["subject"],
    )
    join = overlay_join(spec, complete_roles(spec, table), table)
    assert join.join is False and join.automatic is True
    assert "bucket has no place in the schema hierarchy" in join.reason
    # The same bucket as the BRACKET changes nothing: session is the span.
    swapped = replace(spec, groups=["session", "bucket"])
    assert overlay_join(swapped, complete_roles(swapped, table), table).join is True


def test_no_tick_at_all_has_nothing_to_join_across(gait):
    spec = _spec(["subject"], roles={
        "subject": Role.COLLAPSE, "session": Role.ITERATE, "speed": Role.ITERATE,
        "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE,
    }, groups=[])
    join = _join(gait, spec)
    assert join.join is False and "No x layer" in join.reason


def test_line_recurrence_is_the_one_rule(gait):
    """`overlay_join` and `spaghetti_sample_repeats` read the same function."""
    assert line_recurrence(gait, ["subject"], "session", ["speed"]) == (
        True, "Each subject has a value at every session within each speed."
    )
    recurs, reason = line_recurrence(gait, ["trial"], "session", [])
    assert recurs is False and "belongs to one session" in reason
    assert line_recurrence(gait, ["subject"], None, [])[0] is None


def test_a_spaghetti_line_spans_the_innermost_tick_only(gait):
    """Lines = trial, ticks = speed (bracket) / session (span), sample =
    subject: a subject has a value at every session inside one speed, so
    each is a line — the same answer whichever layer is the bracket."""
    roles = {
        "subject": Role.COLLAPSE, "session": Role.GROUP, "speed": Role.GROUP,
        "trial": Role.GROUP, "cycle": Role.COLLAPSE,
    }
    spec = _spec([], kind=PlotKind.SPAGHETTI, roles=roles, groups=["trial", "session", "speed"])
    recurs, reason = spaghetti_sample_repeats(spec, complete_roles(spec, gait), gait, ["subject"])
    assert recurs is True and "within each speed" in reason
    # trial as the sample under a session span: different trials, no line.
    roles = {**roles, "trial": Role.COLLAPSE, "subject": Role.GROUP}
    spec = _spec([], kind=PlotKind.SPAGHETTI, roles=roles, groups=["subject", "session", "speed"])
    recurs, reason = spaghetti_sample_repeats(spec, complete_roles(spec, gait), gait, ["trial"])
    assert recurs is False and "belongs to one session" in reason


# --- availability and validation -------------------------------------------


def test_overlay_kinds_are_the_categorical_kinds():
    """Every kind with a categorical mark to place points beside — the
    spaghetti included since 2026-09-21 (`test_show_sample_spaghetti.py`)."""
    assert set(OVERLAY_KINDS) == {
        PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN, PlotKind.SCATTER, PlotKind.STRIP,
        PlotKind.SPAGHETTI,
    }


def test_why_an_overlay_is_unavailable(gait):
    roles = complete_roles(_spec(["subject"]), gait)
    assert overlay_unavailable(_spec([]), roles, Shape.SCALAR) is None
    assert "categorical mark" in overlay_unavailable(_spec([], kind=PlotKind.LINE), roles, Shape.SCALAR)
    assert overlay_unavailable(_spec([], kind=PlotKind.SPAGHETTI), roles, Shape.SCALAR) is None
    assert "x-y" in overlay_unavailable(_spec([], kind=PlotKind.SCATTER, x_measure="M"), roles, Shape.SCALAR)
    assert "one value per row" in overlay_unavailable(_spec([]), roles, Shape.SERIES_1D)
    no_sample = {name: Role.ITERATE for name in SCHEMA}
    assert "Nothing is collapsed" in overlay_unavailable(_spec([]), no_sample, Shape.SCALAR)


def test_validate_refuses_a_name_that_is_no_factor(gait):
    with pytest.raises(RoleError, match="show_sample names unknown factor"):
        validate(_spec(["trail"]), gait)


def test_validate_accepts_an_inert_selection_on_another_kind(gait):
    """Ticks kept across a kind switch are inert, not an error — the same
    contract as ``cell_statistic`` on a line."""
    validate(_spec(["trial"], kind=PlotKind.SPAGHETTI, groups=["subject", "session"], roles={
        "subject": Role.GROUP, "session": Role.GROUP, "speed": Role.ITERATE,
        "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE,
    }), gait)
    validate(_spec(["subject"]), gait)


# --- the sentence -----------------------------------------------------------


def test_the_granularity_sentence_says_what_a_point_is(gait):
    spec = _spec(["trial"])
    roles = complete_roles(spec, gait)
    steps = overlay_steps(spec, roles, gait)
    text = overlay_granularity(steps, overlay_join(spec, roles, gait, steps))
    assert text.startswith("One point per subject · trial; cycle averaged within it.")
    assert "Points only." in text

    spec = _spec(["cycle"], join=True)
    steps = overlay_steps(spec, roles, gait)
    text = overlay_granularity(steps, overlay_join(spec, roles, gait, steps))
    assert "the raw data" in text and "Lines join the points." in text


# --- the capability report -------------------------------------------------


def test_capability_report_lists_the_collapsed_keys_as_checkboxes(gait):
    from scistackplot import capabilities

    report = capabilities(_spec(["trial"]), gait)["sample_overlay"]
    assert report["available"] is True and report["reason"] is None
    # Collapse order, deepest first — raw data at the top of the list.
    assert [f["name"] for f in report["factors"]] == ["cycle", "trial", "subject"]
    by_name = {f["name"]: f for f in report["factors"]}
    assert by_name["trial"] == {"name": "trial", "checked": True, "shown": True}
    assert by_name["subject"] == {"name": "subject", "checked": False, "shown": True}
    assert by_name["cycle"] == {"name": "cycle", "checked": False, "shown": False}
    assert report["shown"] == ["subject", "trial"] and report["averaged"] == ["cycle"]
    assert report["join"] == {
        "join": False, "automatic": True, "reason": report["join"]["reason"], "setting": None,
        # The layer a line would run along, and the brackets it never crosses.
        "span": "session", "brackets": [],
    }
    assert report["granularity"].startswith("One point per subject · trial")


def test_capability_report_says_why_there_is_no_overlay(gait):
    from scistackplot import capabilities

    # A line has no categorical mark to place points beside (the spaghetti
    # used to be the example here; it carries the overlay since 2026-09-21).
    report = capabilities(_spec([], kind=PlotKind.LINE), gait)["sample_overlay"]
    assert report["available"] is False and "categorical mark" in report["reason"]
    assert report["factors"] and report["shown"] == [] and report["granularity"] == ""


def test_capability_report_names_ignored_ticks(gait):
    from scistackplot import capabilities

    spec = _spec(["session", "trial"], roles={
        "subject": Role.COLLAPSE, "session": Role.GROUP, "speed": Role.ITERATE,
        "trial": Role.COLLAPSE, "cycle": Role.COLLAPSE,
    })
    report = capabilities(spec, gait)["sample_overlay"]
    assert report["ignored"] == ["session"]
    assert report["shown"] == ["subject", "trial"]
