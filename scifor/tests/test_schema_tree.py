"""scifor.schema.SchemaTree: the owner of parents, coherence and level defaults.

The rules are LOADED from docs/claude/schema-branch-cases.json, not copied
here, because the frontend's level checkboxes (stage level_default) read the
same file. A missing file is an error, not a skip.
"""

import json
from pathlib import Path

import pytest

from scifor import SchemaTree, get_schema, get_schema_tree, set_schema

CASES_FILE = Path(__file__).resolve().parents[2] / "docs" / "claude" / "schema-branch-cases.json"


def _data() -> dict:
    if not CASES_FILE.exists():
        raise AssertionError(f"Shared schema-branch cases not found at {CASES_FILE}.")
    return json.loads(CASES_FILE.read_text(encoding="utf-8"))


DATA = _data()


def _id(case: dict) -> str:
    return f"{case['id']:02d}-{case['name']}"


def setup_function():
    set_schema([])


@pytest.mark.parametrize("case", DATA["cases"], ids=_id)
def test_shared_case(case):
    tree = SchemaTree.from_paths(DATA["trees"][case["tree"]])
    args = case["args"]
    rule = case["rule"]
    if rule == "chain":
        got = tree.chain(args["key"])
    elif rule == "children":
        got = tree.children(args["key"])
    elif rule == "leaves":
        got = tree.leaves(args["keys"])
    elif rule == "is_chain":
        got = tree.is_chain(args["keys"])
    elif rule == "coherent":
        got = tree.coherent(args["keys"], args["inputs"])
    elif rule == "default_level":
        got = list(tree.default_level(args["inputs"]))
    elif rule == "offered":
        result = tree.offered(args["inputs"], args["ticked"])
        got = {"offered": list(result.keys), "disabled": list(result.disabled)}
    else:
        raise AssertionError(f"unknown rule {rule!r} in case {_id(case)}")
    assert got == case["expect"], case["why"]


@pytest.mark.parametrize("case", DATA["refused"], ids=_id)
def test_refused_declaration_names_the_key(case):
    with pytest.raises(ValueError, match=case["match"]):
        SchemaTree.from_paths(case["paths"])


# --- construction ---------------------------------------------------------


def test_plain_list_is_one_branch_with_previous_key_as_parent():
    tree = SchemaTree.from_paths(["subject", "session", "trial"])
    assert tree.keys == ("subject", "session", "trial")
    assert tree.parent_of == {"subject": None, "session": "subject", "trial": "session"}
    assert tree.is_linear
    assert tree == SchemaTree.linear(["subject", "session", "trial"])


def test_branches_keep_first_appearance_order_parents_first():
    tree = SchemaTree.from_paths(DATA["trees"]["shop"])
    assert tree.keys == ("brand", "category", "item", "location", "person")
    assert tree.parent_of["location"] == "brand"
    assert not tree.is_linear


def test_paths_round_trip():
    declared = DATA["trees"]["shop"]
    assert SchemaTree.from_paths(declared).paths() == declared


def test_empty_declaration_is_an_empty_tree():
    assert SchemaTree.from_paths([]).keys == ()


def test_mixing_a_key_and_a_branch_is_refused():
    with pytest.raises(ValueError, match="mix"):
        SchemaTree.from_paths(["a", ["b"]])


def test_unknown_key_is_refused_by_name_not_dropped():
    tree = SchemaTree.linear(["a", "b"])
    with pytest.raises(ValueError, match="'zzz'"):
        tree.coherent(["zzz"])


def test_disabled_reason_names_the_clash():
    tree = SchemaTree.from_paths(DATA["trees"]["shop"])
    result = tree.offered([["brand"]], ["category"])
    assert "location" in result.disabled
    assert "category" in result.disabled["location"]


# --- the global registry --------------------------------------------------


def test_set_schema_accepts_paths_and_get_schema_stays_flat():
    set_schema(DATA["trees"]["shop"])
    assert get_schema() == ["brand", "category", "item", "location", "person"]
    assert get_schema_tree().parent_of["person"] == "location"


def test_set_schema_plain_list_is_a_linear_tree():
    set_schema(["subject", "session"])
    assert get_schema_tree().is_linear
    assert get_schema() == ["subject", "session"]


def test_set_schema_replaces_the_tree():
    set_schema(DATA["trees"]["shop"])
    set_schema(["x"])
    assert get_schema_tree().keys == ("x",)


# --- the one-text-box form ------------------------------------------------


from scifor.schema import format_schema_text, parse_schema_text


def test_text_without_a_semicolon_is_one_branch_as_today():
    assert parse_schema_text("subject, session, trial") == ["subject", "session", "trial"]
    assert parse_schema_text(" subject ,session,") == ["subject", "session"]


def test_semicolons_separate_branches():
    assert parse_schema_text("brand, category, item; brand, location, person") == DATA["trees"]["shop"]


def test_empty_text_is_no_keys():
    assert parse_schema_text("") == []
    assert parse_schema_text("   ") == []


def test_an_empty_branch_is_kept_so_the_rules_can_refuse_it_by_name():
    assert parse_schema_text("a; ; b") == [["a"], [], ["b"]]
    with pytest.raises(ValueError, match="empty"):
        SchemaTree.from_paths(parse_schema_text("a; ; b"))


def test_a_text_mistake_is_refused_by_the_one_rule_owner():
    with pytest.raises(ValueError, match="'item'"):
        SchemaTree.from_paths(parse_schema_text("brand, category, item; brand, location, item"))


def test_format_round_trips_a_branched_declaration():
    text = format_schema_text(DATA["trees"]["shop"])
    assert text == "brand, category, item; brand, location, person"
    assert SchemaTree.from_paths(parse_schema_text(text)) == SchemaTree.from_paths(DATA["trees"]["shop"])


def test_format_of_a_plain_list_has_no_semicolon():
    assert format_schema_text(["subject", "session"]) == "subject, session"
