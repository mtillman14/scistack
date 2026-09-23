"""``_node_wiring`` — Stage 1 of ``.claude/plan-node-identity.md``.

The table that lets a canvas function node's id be ALLOCATED once instead of
re-derived from its recorded bindings on every build. These tests cover the
storage contract only: round trip, append-only semantics, ``last_seen``
advancing on a repeat, and the mint rule. The DECISION that uses them is
``domain/node_identity.py`` (``test_node_identity.py``).
"""

import pytest

from scistack_gui import node_wiring


@pytest.fixture
def db(populated_db):
    node_wiring.ensure_tables(populated_db)
    return populated_db


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


def test_a_recorded_association_reads_back(db):
    assert node_wiring.record(db, "fn__f__w1", "w1", run_id="run1") is True

    assert node_wiring.wirings_for_node(db, "fn__f__w1") == ["w1"]
    assert node_wiring.nodes_for_wiring(db, "w1") == ["fn__f__w1"]
    assert node_wiring.node_for_wiring(db, "w1") == "fn__f__w1"

    row = node_wiring.associations(db)[0]
    assert row["node_id"] == "fn__f__w1"
    assert row["wiring_id"] == "w1"
    assert row["run_id"] == "run1"
    assert row["first_seen"] and row["last_seen"]


def test_nothing_recorded_is_an_empty_answer_not_a_failure(db):
    assert node_wiring.associations(db) == []
    assert node_wiring.wirings_for_node(db, "fn__f__w1") == []
    assert node_wiring.node_for_wiring(db, "w1") is None
    assert node_wiring.known_node_ids(db) == set()


def test_one_node_can_hold_several_wirings_oldest_first(db):
    """A node that was rewired and run. Both shapes are its own history, and
    the ORDER is the answer to "what did it run as before?"."""
    node_wiring.record(db, "fn__grSides__w1", "w1", seen="2026-09-01T00:00:00")
    node_wiring.record(db, "fn__grSides__w1", "w2", seen="2026-09-22T00:00:00")

    assert node_wiring.wirings_for_node(db, "fn__grSides__w1") == ["w1", "w2"]


# ---------------------------------------------------------------------------
# Append-only
# ---------------------------------------------------------------------------


def test_a_repeat_advances_last_seen_and_keeps_first_seen(db):
    node_wiring.record(db, "fn__f__w1", "w1", seen="2026-09-01T00:00:00")
    assert node_wiring.record(db, "fn__f__w1", "w1", seen="2026-09-22T00:00:00") is False

    rows = node_wiring.associations(db)
    assert len(rows) == 1, "a repeat added a row instead of advancing one"
    assert rows[0]["first_seen"] == "2026-09-01T00:00:00", (
        "first_seen was rewritten — the chronology that makes 'what did it "
        "run as BEFORE' answerable is gone"
    )
    assert rows[0]["last_seen"] == "2026-09-22T00:00:00"


def test_a_later_run_id_fills_one_that_was_missing(db):
    node_wiring.record(db, "fn__f__w1", "w1")
    node_wiring.record(db, "fn__f__w1", "w1", run_id="run2")

    assert node_wiring.associations(db)[0]["run_id"] == "run2"


def test_a_run_id_already_recorded_is_not_overwritten(db):
    node_wiring.record(db, "fn__f__w1", "w1", run_id="run1")
    node_wiring.record(db, "fn__f__w1", "w1", run_id="run2")

    assert node_wiring.associations(db)[0]["run_id"] == "run1", (
        "the row records the run that FIRST made this association"
    )


def test_the_placement_suffix_is_stripped(db):
    """Placement lives in `scope`, its own column — the rule that retires the
    placement-id lookup trap as a class (`_intent` does the same)."""
    node_wiring.record(db, "fn__f__w1::pipe_abc", "w1", scope="pipe_abc")

    assert node_wiring.associations(db)[0]["node_id"] == "fn__f__w1"
    assert node_wiring.associations(db)[0]["scope"] == "pipe_abc"
    assert node_wiring.wirings_for_node(db, "fn__f__w1::pipe_abc") == ["w1"]
    assert node_wiring.wirings_for_node(db, "fn__f__w1") == ["w1"]


# ---------------------------------------------------------------------------
# Minting
# ---------------------------------------------------------------------------


def test_a_minted_id_says_only_which_function_it_runs():
    """Nothing in the suffix is readable and nothing is meant to be
    (D-2026-09-22-3). An id that LOOKS like a wiring hash and is not one is
    the confusion this whole change exists to end."""
    from scistack_gui.ids import parse_fn_node_id

    minted = node_wiring.mint_node_id("grSides")

    assert minted.startswith("fn__grSides__")
    assert minted != "fn__grSides__abc123"
    parsed = parse_fn_node_id(minted)
    assert parsed is not None, f"{minted} does not parse as a function node id"
    assert parsed[0] == "grSides"
    # SIXTEEN hex: `ids.parse_fn_node_id` recognises a DB-derived function
    # node by exactly that, in ~40 places. The length is a contract with the
    # id grammar, not part of the decision.
    assert len(parsed[1]) == 16
    assert all(c in "0123456789abcdef" for c in parsed[1])


def test_two_mints_for_one_function_are_different_nodes():
    assert node_wiring.mint_node_id("f") != node_wiring.mint_node_id("f")


def test_minting_never_returns_an_id_already_taken():
    taken: set[str] = set()
    for _ in range(5):
        minted = node_wiring.mint_node_id("f", taken)
        assert minted not in taken
        taken.add(str(minted))


# ---------------------------------------------------------------------------
# The token lookup other layers use
# ---------------------------------------------------------------------------


def test_token_resolver_maps_a_wiring_to_its_nodes_token(db):
    node_wiring.record(db, "fn__grSides__w1", "w1")
    node_wiring.record(db, "fn__grSides__w1", "w2")
    token_for = node_wiring.token_resolver(db)

    assert token_for("grSides", "w1") == "w1"
    assert token_for("grSides", "w2") == "w1", (
        "the second wiring belongs to the same node, so it must resolve to "
        "that node's token — not to itself"
    )


def test_token_resolver_falls_back_to_the_wiring_for_anything_unknown(db):
    token_for = node_wiring.token_resolver(db)

    assert token_for("grSides", "never_seen") == "never_seen", (
        "the fallback is the pre-allocation behaviour and must stay exact"
    )


def test_a_token_recorded_for_another_function_does_not_capture_this_one(db):
    node_wiring.record(db, "fn__other__w1", "w1")
    token_for = node_wiring.token_resolver(db)

    assert token_for("grSides", "w1") == "w1"


# ---------------------------------------------------------------------------
# Re-keying and forgetting
# ---------------------------------------------------------------------------


def test_rekey_moves_every_association(db):
    node_wiring.record(db, "fn__f__manual", "w1")
    node_wiring.record(db, "fn__f__manual", "w2")

    assert node_wiring.rekey_node(db, "fn__f__manual", "fn__f__w1") == 2
    assert node_wiring.wirings_for_node(db, "fn__f__manual") == []
    assert node_wiring.wirings_for_node(db, "fn__f__w1") == ["w1", "w2"]


def test_rekey_keeps_what_the_target_already_has(db):
    node_wiring.record(db, "fn__f__manual", "w1", run_id="from_manual")
    node_wiring.record(db, "fn__f__w1", "w1", run_id="already_there")

    node_wiring.rekey_node(db, "fn__f__manual", "fn__f__w1")

    assert node_wiring.associations(db)[0]["run_id"] == "already_there"


def test_rekey_to_the_same_id_does_nothing(db):
    node_wiring.record(db, "fn__f__w1", "w1")
    assert node_wiring.rekey_node(db, "fn__f__w1", "fn__f__w1") == 0
    assert node_wiring.wirings_for_node(db, "fn__f__w1") == ["w1"]


def test_forget_node_drops_only_that_node(db):
    node_wiring.record(db, "fn__f__w1", "w1")
    node_wiring.record(db, "fn__g__w2", "w2")

    assert node_wiring.forget_node(db, "fn__f__w1") == 1
    assert node_wiring.wirings_for_node(db, "fn__f__w1") == []
    assert node_wiring.wirings_for_node(db, "fn__g__w2") == ["w2"]


def test_forget_all_clears_the_table(db):
    """The manual escape hatch. Destructive: new ids mean every setting keyed
    by an old one stops resolving, which is why nothing calls it."""
    node_wiring.record(db, "fn__f__w1", "w1")

    assert node_wiring.forget_all(db) == 1
    assert node_wiring.associations(db) == []


# ---------------------------------------------------------------------------
# It is created wherever the GUI touches the database
# ---------------------------------------------------------------------------


def test_ensure_tables_runs_from_pipeline_store(populated_db):
    """Beside `_intent`, through the same choke point — so no path can reach
    a GUI database without it."""
    from scistack_gui import pipeline_store

    pipeline_store._ensure_tables(populated_db)
    rows = populated_db._duck._fetchall(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name = '_node_wiring'"
    )
    assert rows, "_node_wiring was not created by pipeline_store._ensure_tables"


# ---------------------------------------------------------------------------
# Current shape versus history — two questions with different answers
# ---------------------------------------------------------------------------


def test_current_wiring_is_the_latest_one_not_the_first(db):
    """What the node IS, as against what it HAS RUN AS. The canvas draws this
    one; drawing the union would put handles on a node for a shape the user
    rewired away from."""
    node_wiring.record(db, "fn__grSides__abc", "w1", seen="2026-09-01T00:00:00")
    node_wiring.record(db, "fn__grSides__abc", "w2", seen="2026-09-22T00:00:00")

    assert node_wiring.current_wiring(db, "fn__grSides__abc") == "w2"
    assert node_wiring.wirings_for_node(db, "fn__grSides__abc") == ["w1", "w2"]


def test_re_running_an_older_shape_makes_it_current_again(db):
    """`last_seen` decides, and a re-run advances it — the node has just run
    that way, so that is what it is."""
    node_wiring.record(db, "fn__f__abc", "w1", seen="2026-09-01T00:00:00")
    node_wiring.record(db, "fn__f__abc", "w2", seen="2026-09-22T00:00:00")
    node_wiring.record(db, "fn__f__abc", "w1", seen="2026-09-23T00:00:00")

    assert node_wiring.current_wiring(db, "fn__f__abc") == "w1"


def test_a_node_that_has_never_run_has_no_current_wiring(db):
    assert node_wiring.current_wiring(db, "fn__f__never") is None


def test_current_wiring_by_node_answers_for_every_node_at_once(db):
    """One query: the graph build needs this for every node, and asking per
    node would be an N+1 on the canvas path."""
    node_wiring.record(db, "fn__f__abc", "w1", seen="2026-09-01T00:00:00")
    node_wiring.record(db, "fn__f__abc", "w2", seen="2026-09-22T00:00:00")
    node_wiring.record(db, "fn__g__def", "w3", seen="2026-09-02T00:00:00")

    assert node_wiring.current_wiring_by_node(db) == {
        "fn__f__abc": "w2",
        "fn__g__def": "w3",
    }


def test_current_wiring_strips_a_placement_suffix(db):
    node_wiring.record(db, "fn__f__abc", "w1")
    assert node_wiring.current_wiring(db, "fn__f__abc::pipe_xyz") == "w1"
