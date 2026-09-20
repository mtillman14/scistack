"""Stated here, not used by the last run.

Decision A (docs/claude/intent-and-fact.md §7): a script run ignores GUI
intent. The canvas can therefore show a selection the data does not reflect,
and `graph_builder.mark_unused_intent` is what keeps that from being silent.
Pure — no database.
"""

from __future__ import annotations

from scistack_gui.domain.graph_builder import (
    UNUSED_CHANGED_SINCE,
    UNUSED_NEVER_RUN,
    UNUSED_SCRIPT_RUN,
    mark_unused_intent,
)


def _node(fn: str, selections: dict | None):
    data = {"label": fn}
    if selections is not None:
        data["columnSelections"] = selections
    return {"id": f"fn__{fn}__0123456789abcdef", "type": "functionNode", "data": data}


SEL = {"columns": ["ankle"], "iterate": False}


def test_a_script_run_that_ignored_the_selection_is_marked():
    nodes = [_node("trial_mean", {"cycles": SEL})]
    latest = {"trial_mean": {"origin": "script", "selectors": {}}}
    assert mark_unused_intent(nodes, latest) == 1
    note = nodes[0]["data"]["unusedIntent"]["cycles"]
    assert note["reason"] == UNUSED_SCRIPT_RUN
    assert note["stated"] == SEL
    assert note["recorded"] is None


def test_a_gui_run_that_used_it_is_not_marked():
    nodes = [_node("trial_mean", {"cycles": SEL})]
    latest = {"trial_mean": {"origin": "gui", "selectors": {"cycles": SEL}}}
    assert mark_unused_intent(nodes, latest) == 0
    assert "unusedIntent" not in nodes[0]["data"]


def test_a_selection_changed_after_the_last_run_is_marked_as_such():
    nodes = [_node("trial_mean", {"cycles": SEL})]
    latest = {
        "trial_mean": {
            "origin": "gui",
            "selectors": {"cycles": {"columns": ["knee"], "iterate": False}},
        }
    }
    mark_unused_intent(nodes, latest)
    assert nodes[0]["data"]["unusedIntent"]["cycles"]["reason"] == UNUSED_CHANGED_SINCE


def test_a_function_that_never_ran_is_marked_never_run():
    nodes = [_node("trial_mean", {"cycles": SEL})]
    mark_unused_intent(nodes, {})
    assert nodes[0]["data"]["unusedIntent"]["cycles"]["reason"] == UNUSED_NEVER_RUN


def test_a_run_older_than_the_origin_column_is_unknown_not_ignored():
    nodes = [_node("trial_mean", {"cycles": SEL})]
    latest = {"trial_mean": {"origin": None, "selectors": {}}}
    assert mark_unused_intent(nodes, latest) == 0


def test_a_node_without_a_selection_is_left_alone():
    nodes = [_node("trial_mean", None), _node("other", {})]
    assert mark_unused_intent(nodes, {"trial_mean": {"origin": "script", "selectors": {}}}) == 0


def test_the_same_selection_spelled_differently_is_not_a_difference():
    nodes = [_node("trial_mean", {"cycles": ["ankle"]})]
    latest = {"trial_mean": {"origin": "script", "selectors": {"cycles": SEL}}}
    assert mark_unused_intent(nodes, latest) == 0
