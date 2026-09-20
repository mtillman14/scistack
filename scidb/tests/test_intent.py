"""Tests for ``scidb.intent`` — the intent/fact vocabulary and resolver.

Pure: no database, no GUI. See ``docs/claude/intent-and-fact.md`` for the
five rules these pin, and ``.claude/plan-intent-and-fact.md`` Stage 1.

The two halves:

* the ``columns`` aspect normalizer, which is now the ONE owner of the
  selection shape (the GUI and ``provenance_save`` both route through it);
* :func:`scidb.intent.resolve`, which implements "nearest scope wins within a
  surface, then surfaces in origin order, then history as the floor".
"""

import json

import pytest

from scidb.intent import (
    ASPECT_COLUMNS,
    ASPECT_RUN_OPTIONS,
    ASPECT_WIRING,
    FROM_HISTORY,
    GLOBAL_SCOPE,
    ORIGIN_GUI,
    ORIGIN_REPLAY,
    ORIGIN_SCRIPT,
    SUBJECT_CALL_SITE,
    SURFACE_SOURCE,
    SURFACE_STORE,
    IntentError,
    Statement,
    describe_columns,
    describe_plan,
    normalize_columns,
    parse_selector,
    resolve,
    resolve_many,
    same_columns,
    selector_json,
)

CALL = (SUBJECT_CALL_SITE, "fn__scale_joint__abc123")


def stmt(**kw):
    """A statement about our one call site, with sensible defaults."""
    base = dict(
        subject_kind=SUBJECT_CALL_SITE,
        subject_ref=CALL[1],
        aspect=ASPECT_COLUMNS,
        key="value",
        value={"columns": ["ankle"], "iterate": False},
        scope=GLOBAL_SCOPE,
        surface=SURFACE_STORE,
    )
    base.update(kw)
    return Statement(**base)


# ---------------------------------------------------------------------------
# The columns aspect: one shape, one spelling, one serialisation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, None),
        ("", None),
        ([], None),
        ({}, None),
        ({"columns": [], "iterate": False}, None),
        ("filename", {"columns": ["filename"], "iterate": False}),
        (["a", "b"], {"columns": ["a", "b"], "iterate": False}),
        ({"columns": "a"}, {"columns": ["a"], "iterate": False}),
        ({"columns": ["a"], "iterate": True}, {"columns": ["a"], "iterate": True}),
        # for_columns() over every column: empty, but NOT nothing.
        ({"iterate": True}, {"columns": [], "iterate": True}),
        # a binding dict carries kind/ref alongside the selection
        (
            {"kind": "variable", "ref": ["Trials"], "columns": ["a"], "iterate": False},
            {"columns": ["a"], "iterate": False},
        ),
    ],
)
def test_normalize_columns_tolerates_every_shape(raw, expected):
    assert normalize_columns(raw) == expected


def test_an_empty_non_iterate_selection_is_no_selection_at_all():
    """No columns and no iteration means the whole variable, which is what
    binding the bare class already does — wrapping it anyway would fork the
    version key for no change in what the function receives."""
    assert normalize_columns({"columns": [], "iterate": False}) is None
    assert selector_json({"columns": [], "iterate": False}) is None


def test_a_live_column_selection_normalizes_like_a_dict():
    class FakeSelection:
        columns = ["knee"]
        iterate = True

    assert normalize_columns(FakeSelection()) == {"columns": ["knee"], "iterate": True}


def test_an_unexpected_type_is_ignored_not_raised(caplog):
    assert normalize_columns(object()) is None
    assert "unexpected type" in caplog.text


def test_selector_json_bytes_are_identity():
    """``compute_invocation_id`` folds this string in, so the spelling is
    identity: sorted keys, and ``iterate`` emitted ONLY when set."""
    assert selector_json({"columns": ["b", "a"]}) == '{"columns": ["b", "a"]}'
    assert (
        selector_json({"columns": ["a"], "iterate": True})
        == '{"columns": ["a"], "iterate": true}'
    )
    # not re-ordered: column ORDER is part of what the function receives
    assert json.loads(selector_json({"columns": ["b", "a"]}))["columns"] == ["b", "a"]


def test_parse_selector_accepts_both_stored_spellings():
    """``pipeline_variants`` parses the JSON; ``function_variant_configs``
    keeps it as a string for its identity key. Both reach the guards."""
    as_str = '{"columns": ["a"], "iterate": true}'
    assert parse_selector(as_str) == {"columns": ["a"], "iterate": True}
    assert parse_selector(json.loads(as_str)) == {"columns": ["a"], "iterate": True}
    assert parse_selector(None) is None
    assert parse_selector("not json") is None


def test_same_columns_folds_absent_and_empty():
    assert same_columns(None, {"columns": [], "iterate": False})
    assert same_columns(["a"], {"columns": ["a"]})
    assert not same_columns(["a"], {"columns": ["a"], "iterate": True})


def test_describe_columns_has_one_spelling():
    assert describe_columns(None) == "whole variable"
    assert describe_columns(["a"]) == '"a"'
    assert describe_columns(["a", "b"]) == "2 columns"
    assert describe_columns({"iterate": True}) == "per column"
    assert describe_columns({"columns": ["a", "b"], "iterate": True}) == "per column (2)"


# ---------------------------------------------------------------------------
# Statements
# ---------------------------------------------------------------------------


def test_history_is_not_a_surface():
    """Rule 5: provenance is compared against, never written as intent."""
    with pytest.raises(IntentError):
        stmt(surface=FROM_HISTORY)


def test_an_unknown_aspect_is_refused():
    with pytest.raises(IntentError):
        stmt(aspect="colour")


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def test_a_statement_beats_history_and_says_what_it_beat():
    fact = {ASPECT_COLUMNS: {"value": {"columns": ["ankle", "knee"]}}}
    plan = resolve([stmt()], fact, origin=ORIGIN_GUI)

    assert plan.columns["value"] == {"columns": ["ankle"], "iterate": False}
    field = plan.decision.get(ASPECT_COLUMNS, "value")
    assert field.surface == SURFACE_STORE
    assert field.recorded == {"columns": ["ankle", "knee"], "iterate": False}


def test_history_is_the_floor_when_nothing_is_stated():
    fact = {ASPECT_COLUMNS: {"value": {"columns": ["ankle"]}}}
    plan = resolve([], fact, origin=ORIGIN_GUI)

    field = plan.decision.get(ASPECT_COLUMNS, "value")
    assert field.surface == FROM_HISTORY
    assert plan.columns["value"] == {"columns": ["ankle"], "iterate": False}


def test_the_store_beats_source_for_a_gui_run():
    plan = resolve(
        [
            stmt(surface=SURFACE_SOURCE, value={"columns": ["knee"]}),
            stmt(surface=SURFACE_STORE, value={"columns": ["ankle"]}),
        ],
        None,
        origin=ORIGIN_GUI,
    )
    assert plan.columns["value"]["columns"] == ["ankle"]


def test_a_script_run_does_not_read_the_store():
    """Decision A (2026-09-19): a checkbox ticked in the GUI must not change
    what ``python pipeline.py`` does. The statement is REPORTED, not applied."""
    store = stmt(surface=SURFACE_STORE, value={"columns": ["ankle"]})
    source = stmt(surface=SURFACE_SOURCE, value={"columns": ["knee"]})
    plan = resolve([store, source], None, origin=ORIGIN_SCRIPT)

    assert plan.columns["value"]["columns"] == ["knee"]
    assert plan.decision.unread == [store]


def test_a_replay_reads_no_surface_at_all():
    fact = {ASPECT_COLUMNS: {"value": {"columns": ["ankle"]}}}
    plan = resolve([stmt(value={"columns": ["knee"]})], fact, origin=ORIGIN_REPLAY)

    assert plan.columns["value"]["columns"] == ["ankle"]
    assert len(plan.decision.unread) == 1


def test_the_nearest_scope_shadows_global():
    plan = resolve(
        [
            stmt(scope=GLOBAL_SCOPE, value={"columns": ["everywhere"]}),
            stmt(scope="pipe_ab12", value={"columns": ["here"]}),
        ],
        None,
        origin=ORIGIN_GUI,
        scope="pipe_ab12",
    )
    assert plan.columns["value"]["columns"] == ["here"]


def test_global_applies_when_the_scope_says_nothing():
    plan = resolve(
        [stmt(scope=GLOBAL_SCOPE, value={"columns": ["everywhere"]})],
        None,
        origin=ORIGIN_GUI,
        scope="pipe_ab12",
    )
    assert plan.columns["value"]["columns"] == ["everywhere"]


def test_another_scopes_statement_never_leaks():
    """Decision C: a duplicated hypothesis owns its rows. Nothing inherits."""
    other = stmt(scope="pipe_other", value={"columns": ["theirs"]})
    plan = resolve([other], None, origin=ORIGIN_GUI, scope="pipe_mine")

    assert plan.columns == {}
    assert plan.decision.out_of_scope == [other]


def test_the_most_recent_statement_wins_a_tie():
    plan = resolve(
        [
            stmt(value={"columns": ["old"]}, stated_at="2026-09-01T00:00:00"),
            stmt(value={"columns": ["new"]}, stated_at="2026-09-18T00:00:00"),
        ],
        None,
        origin=ORIGIN_GUI,
    )
    assert plan.columns["value"]["columns"] == ["new"]


def test_a_dropped_selection_is_visible_on_the_decision():
    """The read-side round-trip guard, in one call: history recorded a
    selection and the plan carries none."""
    fact = {ASPECT_COLUMNS: {"value": {"columns": ["ankle"], "iterate": True}}}
    plan = resolve([stmt(value=None)], fact, origin=ORIGIN_GUI)

    lost = plan.decision.lost(ASPECT_COLUMNS)
    assert [f.key for f in lost] == ["value"]
    assert lost[0].recorded == {"columns": ["ankle"], "iterate": True}


def test_an_unchanged_selection_is_not_a_loss():
    fact = {ASPECT_COLUMNS: {"value": {"columns": ["ankle"]}}}
    plan = resolve([stmt(value={"columns": ["ankle"]})], fact, origin=ORIGIN_GUI)
    assert plan.decision.lost(ASPECT_COLUMNS) == []


def test_a_whole_aspect_value_needs_no_key():
    plan = resolve(
        [stmt(aspect=ASPECT_RUN_OPTIONS, key=None, value={"distribute": True})],
        None,
        origin=ORIGIN_GUI,
    )
    assert plan.run_options == {"distribute": True}


def test_resolve_many_indexes_statements_once():
    other_call = "fn__trial_mean__def456"
    statements = [
        stmt(),
        stmt(subject_ref=other_call, value={"columns": ["knee"]}),
    ]
    plans = resolve_many(
        {CALL: None, (SUBJECT_CALL_SITE, other_call): None},
        statements,
        origin=ORIGIN_GUI,
    )
    assert plans[CALL].columns["value"]["columns"] == ["ankle"]
    assert plans[(SUBJECT_CALL_SITE, other_call)].columns["value"]["columns"] == ["knee"]


def test_describe_plan_names_every_input():
    plan = resolve(
        [
            stmt(aspect=ASPECT_WIRING, key="value", value={"kind": "variable", "ref": ["TrialMeanSymmetry"]}),
            stmt(aspect=ASPECT_WIRING, key="cycles", value={"kind": "variable", "ref": ["CycleSymmetry"]}),
            stmt(aspect=ASPECT_COLUMNS, key="value", value={"columns": ["ankle", "knee"]}),
        ],
        None,
        origin=ORIGIN_GUI,
    )
    line = describe_plan(plan)
    assert "cycles: CycleSymmetry (whole variable)" in line
    assert "value: TrialMeanSymmetry (2 columns)" in line
