"""Which keys a run iterates when the schema BRANCHES (stage level_default).

``scidb.run.schema_level`` stays the one owner of the decision; rule 3 (what the
inputs imply) and the recorded-level narrowing now ask
``scifor.schema.SchemaTree.default_level``. The rules themselves are the shared
cases in docs/claude/schema-branch-cases.json (run in
scifor/tests/test_schema_tree.py); these are the integration points.
"""

from __future__ import annotations

from scifor import SchemaTree

from scidb.provenance.query import finest_schema_keys
from scidb.run.schema_level import (
    RULE_INPUTS,
    RULE_INPUTS_COMMON,
    RULE_STATED,
    SchemaLevel,
    level_choices,
    resolve_schema_level,
)

SHOP = [["brand", "category", "item"], ["brand", "location", "person"]]
TREE = SchemaTree.from_paths(SHOP)
KEYS = list(TREE.keys)
ITEM = ["brand", "category", "item"]
PERSON = ["brand", "location", "person"]


def test_item_and_person_inputs_default_to_the_shared_key_and_say_why():
    level, rule = resolve_schema_level(KEYS, levels=[ITEM, PERSON], tree=TREE)
    assert level.keys == ("brand",)
    assert rule == RULE_INPUTS_COMMON


def test_item_person_and_a_cross_branch_input_default_to_all_five():
    level, rule = resolve_schema_level(KEYS, levels=[ITEM, PERSON, KEYS], tree=TREE)
    assert level.keys == tuple(KEYS)
    assert rule == RULE_INPUTS


def test_a_single_branch_still_takes_the_union():
    level, rule = resolve_schema_level(KEYS, levels=[["brand"], ITEM], tree=TREE)
    assert level.keys == tuple(ITEM)
    assert rule == RULE_INPUTS


def test_a_dataset_level_input_does_not_narrow_the_shared_keys():
    level, _ = resolve_schema_level(KEYS, levels=[[], ITEM, PERSON], tree=TREE)
    assert level.keys == ("brand",)


def test_a_stated_level_wins_even_when_it_crosses_branches():
    """The tree never refuses a key set."""
    level, rule = resolve_schema_level(KEYS, stated=["category", "person"], levels=[ITEM, PERSON], tree=TREE)
    assert level.keys == ("category", "person")
    assert rule == RULE_STATED


def test_the_recorded_level_is_narrowed_to_what_the_inputs_share():
    level, _ = resolve_schema_level(
        KEYS, recorded=["brand", "category", "item"], levels=[ITEM, PERSON], tree=TREE
    )
    assert level.keys == ("brand",)


def test_without_a_tree_the_schema_is_one_branch_as_before():
    keys = ["subject", "session", "trial"]
    level, rule = resolve_schema_level(keys, levels=[["subject", "trial"], ["subject", "session"]])
    assert level.keys == tuple(keys)
    assert rule == RULE_INPUTS


def test_finest_schema_keys_ignores_keys_the_dataset_does_not_have():
    assert finest_schema_keys([["subject", "bogus"]], ["subject", "session"]) == ["subject"]


def test_the_panel_payload_carries_offered_and_disabled():
    choices = level_choices(TREE, [["brand"]], ["category"])
    payload = SchemaLevel.of(["category"], KEYS).to_json("rule", choices)
    assert payload["offered"] == KEYS
    assert sorted(payload["disabled"]) == ["location", "person"]
    assert "category" in payload["disabled"]["location"]


def test_the_panel_payload_without_choices_is_unchanged():
    payload = SchemaLevel.of(["brand"], KEYS).to_json("rule")
    assert "offered" not in payload and "disabled" not in payload


def test_choices_ignore_keys_the_tree_does_not_know():
    choices = level_choices(TREE, [ITEM + ["__vsig_x"]], ["item", "__vsig_x"])
    assert choices.keys == tuple(ITEM)
