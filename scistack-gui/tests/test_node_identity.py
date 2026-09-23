"""Allocated node identity — Stages 2-5 of ``.claude/plan-node-identity.md``.

The argument is ``docs/claude/node-identity.md``. In one sentence: a canvas
function node stops being identified by ``fn__{fn}__{wiring_id}`` — a hash of
its *recorded* input bindings — and gets an id minted once, with ``wiring_id``
demoted from identity to attribute.

Three symptoms, one cause, and each has a test here:

* a node duplicates after a run through a drawn edge — the run records the
  edge, the hash moves, the old hash is still a node;
* every statement keyed by that id is stranded when it moves;
* the overlay keyed by the node id oscillates, because moving the id makes the
  lookup miss.

The pure resolution rule is exercised against hand-built inputs (no database);
the end-to-end shape is exercised through ``/api/pipeline``.
"""

import numpy as np
import pytest
from conftest import FilteredSignal, RawSignal

import scistack_gui.db as _gui_db
from scidb import BaseVariable, for_each
from scistack_gui import node_wiring, registry as _registry
from scistack_gui.domain import node_identity
from scistack_gui.domain.node_identity import Ambiguity, resolve_identities


# ---------------------------------------------------------------------------
# The rule, pure
# ---------------------------------------------------------------------------


class TestResolveIdentities:
    def test_a_wiring_nobody_claims_gets_a_fresh_id(self):
        """Rule 4. And the id says only which function it runs — the suffix
        is not the wiring, and nothing should be able to read it as one."""
        plan = resolve_identities([("f", "w1")], associations=[])
        node_id = plan.node_by_wiring[("f", "w1")]

        assert node_id.startswith("fn__f__")
        assert node_id != "fn__f__w1", (
            "the minted id is the derived spelling — an id that looks like a "
            "wiring hash and is not one is exactly the confusion this ends"
        )
        assert plan.to_record == [(str(node_id), "w1", "main")]
        assert plan.minted == [(str(node_id), "w1")]
        assert plan.ambiguities == []

    def test_a_recorded_association_wins_and_records_nothing_new(self):
        """Rule 1. This is the normal case completely: a GUI-started run has
        already recorded the association at dispatch."""
        plan = resolve_identities(
            [("f", "w2")],
            associations=[{"node_id": "fn__f__abc", "wiring_id": "w2"}],
        )

        assert plan.node_by_wiring == {("f", "w2"): "fn__f__abc"}
        assert plan.to_record == [], "a known association was re-recorded"
        assert plan.minted == []

    def test_the_node_that_states_a_wiring_claims_it(self):
        """Rule 2 — the one that stops the duplicate forming, from the STATED
        wiring rather than from a repair applied afterwards.

        A script or terminal run records no association, so the next build has
        to recognise the new shape as a node that already exists.
        """
        plan = resolve_identities(
            [("f", "w1"), ("f", "w2")],
            associations=[{"node_id": "fn__f__abc", "wiring_id": "w1"}],
            stated_by={("f", "w2"): ["fn__f__abc"]},
            current_by_node={"fn__f__abc": "w1"},
        )

        assert plan.node_by_wiring == {
            ("f", "w1"): "fn__f__abc",
            ("f", "w2"): "fn__f__abc",
        }, "the rewired node forked instead of absorbing its new shape"
        assert plan.minted == [], "w2 minted a node of its own"
        assert plan.to_record == [("fn__f__abc", "w2", "main")]

    def test_an_unclaimed_new_wiring_is_a_genuinely_new_node(self):
        """Rule 4 again. Nothing states it, so it is not a rewired node — it
        is a second call site, and it gets its own id."""
        plan = resolve_identities(
            [("f", "w1"), ("f", "w2")],
            associations=[{"node_id": "fn__f__abc", "wiring_id": "w1"}],
        )

        assert plan.node_by_wiring[("f", "w1")] == "fn__f__abc"
        assert plan.node_by_wiring[("f", "w2")] != "fn__f__abc"
        assert len(plan.minted) == 1

    def test_an_association_for_another_function_does_not_capture_a_wiring(self):
        """A node id names its function. A wiring reused after a rename must
        not drag the old function's node along with it — and when the filter
        empties the candidate list, the NEXT rule gets its turn rather than
        the unfiltered list coming back."""
        plan = resolve_identities(
            [("f", "w1")],
            associations=[{"node_id": "fn__other__abc", "wiring_id": "w1"}],
        )

        assert plan.node_by_wiring[("f", "w1")].startswith("fn__f__")
        assert plan.minted, "it adopted another function's node"

    def test_nothing_minted_in_a_pass_can_claim_in_the_same_pass(self):
        """Why this is one pass and not a fixed-point loop: a node whose id
        did not exist a moment ago has no edges drawn onto it, so it cannot
        state anything. ``stated_by`` is read from the table, never from the
        assignment being built."""
        plan = resolve_identities(
            [("f", "w1"), ("f", "w2")],
            associations=[],
            stated_by={},
        )

        ids = {str(n) for n in plan.node_by_wiring.values()}
        assert len(ids) == 2, "two unclaimed wirings collapsed into one node"

    def test_no_history_is_an_empty_plan(self):
        plan = resolve_identities([], associations=[])
        assert plan.node_by_wiring == {}
        assert plan.to_record == []

    def test_an_existing_database_opens_to_fresh_ids_not_its_old_ones(self):
        """The clean break, stated as a test. There is NO migration: a
        database built before this change has no associations, so every wiring
        mints a new id and every setting keyed by ``fn__{fn}__{wiring}`` stops
        resolving. Deliberate (D-2026-09-22-3/-4) — the alternative is a
        compatibility shim that leaves ids looking like wiring hashes forever.
        """
        plan = resolve_identities(
            [("grSides", "aaaabbbbccccdddd")], associations=[]
        )

        assert (
            plan.node_by_wiring[("grSides", "aaaabbbbccccdddd")]
            != "fn__grSides__aaaabbbbccccdddd"
        )


class TestCurrentShapeVersusHistory:
    """What a node IS, as against what it HAS RUN AS.

    The canvas draws the first. Taking the union of everything a node has run
    as would put handles on it for a shape the user rewired away from — a
    different way of showing them a node that is not the node they see.
    """

    def test_a_wiring_the_node_just_ran_as_becomes_its_current_shape(self):
        plan = resolve_identities(
            [("f", "w1"), ("f", "w2")],
            associations=[{"node_id": "fn__f__abc", "wiring_id": "w1"}],
            stated_by={("f", "w2"): ["fn__f__abc"]},
            current_by_node={"fn__f__abc": "w1"},
        )

        assert plan.is_current("f", "w2") is True
        assert plan.is_current("f", "w1") is False, (
            "the shape the node was rewired away from is still being drawn"
        )

    def test_a_node_that_has_not_been_rewired_has_one_current_shape(self):
        plan = resolve_identities(
            [("f", "w1")],
            associations=[{"node_id": "fn__f__abc", "wiring_id": "w1"}],
            current_by_node={"fn__f__abc": "w1"},
        )

        assert plan.is_current("f", "w1") is True

    def test_a_freshly_minted_node_is_current_in_the_shape_it_was_minted_for(self):
        plan = resolve_identities([("f", "w1")], associations=[])
        assert plan.is_current("f", "w1") is True

    def test_a_wiring_with_no_node_reads_as_current(self):
        """The default everywhere the identity plan is absent — which is what
        keeps the pre-allocation behaviour exact for every caller and test
        that never heard of allocation."""
        plan = resolve_identities([], associations=[])
        assert plan.is_current("f", "never_seen") is True


class TestAmbiguity:
    """§7b. Two nodes can state the same wiring; resolution is deterministic
    and the user is TOLD — never auto-merged, because silently collapsing two
    nodes a person created is a worse failure than a message they can act on.
    """

    def test_several_claimants_pick_one_and_report_the_rest(self):
        plan = resolve_identities(
            [("f", "w2")],
            associations=[],
            stated_by={("f", "w2"): ["fn__f__bbb", "fn__f__aaa"]},
        )

        assert len(plan.ambiguities) == 1
        amb = plan.ambiguities[0]
        assert amb.chosen == "fn__f__aaa", "the tie-break is not the stable one"
        assert amb.others == ("fn__f__bbb",)
        assert plan.node_by_wiring[("f", "w2")] == "fn__f__aaa"

    def test_the_oldest_node_wins_a_tie(self):
        """Age is ``first_seen`` in ``_node_wiring`` — when the node first ran
        as anything. Not the id, which under allocation says nothing."""
        plan = resolve_identities(
            [("f", "w2")],
            associations=[
                {"node_id": "fn__f__zzz", "wiring_id": "w0", "first_seen": "2026-01-01"},
                {"node_id": "fn__f__aaa", "wiring_id": "w9", "first_seen": "2026-09-01"},
            ],
            stated_by={("f", "w2"): ["fn__f__aaa", "fn__f__zzz"]},
        )

        assert plan.node_by_wiring[("f", "w2")] == "fn__f__zzz"

    def test_a_node_that_has_already_run_as_it_wins_over_age(self):
        """Resolution order: history first, then age. Rule 1 settles this one
        before rule 2 is reached, which IS "has already run as W"."""
        plan = resolve_identities(
            [("f", "w2")],
            associations=[
                {"node_id": "fn__f__zzz", "wiring_id": "w2", "first_seen": "2026-09-01"}
            ],
            stated_by={("f", "w2"): ["fn__f__aaa"]},
        )

        assert plan.node_by_wiring[("f", "w2")] == "fn__f__zzz"

    def test_attribution_is_stable_across_rebuilds(self):
        answers = {
            resolve_identities(
                [("f", "w2")],
                associations=[],
                stated_by={("f", "w2"): ["fn__f__bbb", "fn__f__aaa"]},
            ).node_by_wiring[("f", "w2")]
            for _ in range(5)
        }
        assert len(answers) == 1, f"attribution moved between builds: {answers}"

    def test_the_signature_is_the_same_for_the_same_pair(self):
        """So "two nodes are wired identically" can be said ONCE rather than
        on every graph build."""
        a = Ambiguity("f", "w2", "fn__f__a", ("fn__f__b",))
        b = Ambiguity("f", "w2", "fn__f__a", ("fn__f__b",))
        assert a.signature == b.signature

        c = Ambiguity("f", "w2", "fn__f__a", ("fn__f__c",))
        assert a.signature != c.signature

    def test_the_warning_is_logged_once_rather_than_per_build(self):
        """A repeated warning buries the next one. The POPUP still arrives with
        every response — the panel that shows it may have mounted since — but
        the log records each pair the first time only."""
        from scistack_gui.api import pipeline as api_pipeline

        amb = Ambiguity("f", "w2", "fn__f__a", ("fn__f__b",))
        other = Ambiguity("f", "w3", "fn__f__a", ("fn__f__c",))
        for signature in (amb.signature, other.signature):
            api_pipeline._AMBIGUITIES_LOGGED.discard(signature)
        try:
            assert api_pipeline._should_log_ambiguity(amb.signature) is True
            assert api_pipeline._should_log_ambiguity(amb.signature) is False, (
                "the same ambiguity would be logged again on the next build"
            )
            assert api_pipeline._should_log_ambiguity(other.signature) is True, (
                "a DIFFERENT ambiguity was suppressed along with the first"
            )
        finally:
            for signature in (amb.signature, other.signature):
                api_pipeline._AMBIGUITIES_LOGGED.discard(signature)

    def test_the_message_names_both_nodes_and_says_nothing_was_merged(self):
        message = Ambiguity("grSides", "w2", "fn__grSides__a", ("fn__grSides__b",)).message()

        assert "grSides" in message
        assert "fn__grSides__a" in message and "fn__grSides__b" in message
        assert "merged" in message.lower(), (
            "the user must be told that nothing was collapsed"
        )
        assert "rewire" in message.lower(), "the message must say how to resolve it"


class TestIdentityPlanLookups:
    def test_token_is_the_id_suffix(self):
        plan = resolve_identities(
            [("f", "w1")],
            associations=[{"node_id": "fn__f__abcdef", "wiring_id": "w1"}],
        )

        assert plan.token("f", "w1") == "abcdef"

    def test_a_wiring_no_node_claims_is_its_own_token(self):
        """Not a fallback to the old behaviour — under allocated ids it can
        never match a real node id, so it reads as "no node", which is what
        every caller already does with it."""
        plan = resolve_identities([], associations=[])
        assert plan.token("f", "never_seen") == "never_seen"

    def test_two_wirings_of_one_node_share_a_token(self):
        plan = resolve_identities(
            [("f", "w1"), ("f", "w2")],
            associations=[
                {"node_id": "fn__f__abc", "wiring_id": "w1"},
                {"node_id": "fn__f__abc", "wiring_id": "w2"},
            ],
        )

        assert plan.token("f", "w1") == plan.token("f", "w2") == "abc"
        assert plan.wirings_of("f", "abc") == {"w1", "w2"}

    def test_the_default_token_function_is_the_identity(self):
        assert node_identity.identity_token("f", "w1") == "w1"


class TestTheGrSidesShapeEndToEnd:
    """Draw an edge onto an unbound parameter, run, and there is ONE node.

    The 2026-09-22 session: a user drew `Demographics → grSides.side`, ran the
    node, and got two `grSides` nodes. Nothing was duplicated in the database;
    the second node was the same node under a different id.
    """

    @pytest.fixture
    def wide(self, client):
        def bandpass_filter(signal, low_hz, side=None):  # noqa: ARG001
            return np.asarray(signal, dtype=float) * float(low_hz)

        _registry._functions["bandpass_filter"] = bandpass_filter
        return client

    @pytest.fixture
    def side_var(self, wide):
        class SideTable(BaseVariable):
            pass

        SideTable.save(np.zeros(3), subject=1, session="pre")
        return SideTable

    def _fn_nodes(self, client, label="bandpass_filter"):
        data = client.get("/api/pipeline").json()
        return [
            n
            for n in data["nodes"]
            if n.get("type") == "functionNode" and n["data"]["label"] == label
        ]

    def _wire(self, client, target):
        r = client.put(
            f"/api/edges/manual__side",
            json={
                "source": "var__SideTable",
                "target": target,
                "source_handle": None,
                "target_handle": "in__side",
            },
        )
        assert r.status_code == 200

    def test_the_build_uses_the_allocated_id(self, client, bp_node_id):
        """The id is a lookup, not a derivation — the build finds the one
        ``_node_wiring`` already holds rather than computing a new one."""
        assert [n["id"] for n in self._fn_nodes(client)] == [bp_node_id]

    def test_a_first_build_with_nothing_recorded_mints_an_opaque_id(self, client):
        """No migration. A database the GUI has never opened mints fresh ids,
        and they say nothing but which function the node runs — the clean
        break (D-2026-09-22-3)."""
        from scistack_gui.domain.graph_builder import wiring_id

        nodes = self._fn_nodes(client)
        wid = wiring_id(
            "bandpass_filter", {"signal": "RawSignal"}, {"FilteredSignal"}, {}
        )

        assert len(nodes) == 1
        assert nodes[0]["id"] != f"fn__bandpass_filter__{wid}", (
            "the build derived the id from the wiring instead of allocating one"
        )

    def test_re_opening_does_not_churn_ids(self, client):
        first = [n["id"] for n in self._fn_nodes(client)]
        second = [n["id"] for n in self._fn_nodes(client)]
        third = [n["id"] for n in self._fn_nodes(client)]

        assert first == second == third
        assert len(first) == 1
        db = _gui_db.get_db()
        assert node_wiring.wirings_for_node(db, first[0]), (
            "the assignment was never persisted, so it is being recomputed — "
            "and a recomputed id is one that can move"
        )

    def test_a_script_run_through_a_drawn_edge_keeps_one_node(
        self, wide, side_var, bp_node_id
    ):
        """Rule 2 in full: no node id in the run, so attribution is inferred
        from what the node STATES."""
        self._wire(wide, bp_node_id)
        for_each(
            _registry._functions["bandpass_filter"],
            inputs={"signal": RawSignal, "low_hz": 20, "side": side_var},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre"],
        )

        nodes = self._fn_nodes(wide)
        assert len(nodes) == 1, f"the run forked a node: {[n['id'] for n in nodes]}"
        assert nodes[0]["id"] == bp_node_id

    def test_the_node_carries_both_wirings_afterwards(
        self, wide, side_var, bp_node_id
    ):
        """"One node with two entries in that history — the honest description
        of what happened to it"."""
        from scistack_gui.domain.graph_builder import wiring_id

        self._wire(wide, bp_node_id)
        for_each(
            _registry._functions["bandpass_filter"],
            inputs={"signal": RawSignal, "low_hz": 20, "side": side_var},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre"],
        )
        wide.get("/api/pipeline")

        db = _gui_db.get_db()
        recorded = set(node_wiring.wirings_for_node(db, bp_node_id))
        before = wiring_id(
            "bandpass_filter", {"signal": "RawSignal"}, {"FilteredSignal"}, {}
        )
        after = wiring_id(
            "bandpass_filter",
            {"signal": "RawSignal", "side": "SideTable"},
            {"FilteredSignal"},
            {},
        )
        assert {before, after} <= recorded, (
            f"the node does not hold both shapes it has run as: {recorded}"
        )

    def test_a_run_uses_the_nodes_CURRENT_shape_not_every_shape_it_has_had(
        self, wide, side_var, bp_node_id
    ):
        """What a node IS, as against what it HAS RUN AS.

        After the rewire the node's current shape includes `side`. Running it
        must run that — not also re-run the signal-only records it produced
        before, which is a shape the user deliberately moved away from. Those
        stay its history: provenance and the Variants panel show them.
        """
        from scistack_gui.domain.graph_builder import wiring_id
        from scistack_gui.services.execution_service import derive_target_for_node

        self._wire(wide, bp_node_id)
        for_each(
            _registry._functions["bandpass_filter"],
            inputs={"signal": RawSignal, "low_hz": 20, "side": side_var},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre"],
        )
        wide.get("/api/pipeline")

        db = _gui_db.get_db()
        after = wiring_id(
            "bandpass_filter",
            {"signal": "RawSignal", "side": "SideTable"},
            {"FilteredSignal"},
            {},
        )
        assert node_wiring.current_wiring(db, bp_node_id) == after

        targets = derive_target_for_node(db, bp_node_id)
        assert targets, "the node can no longer run at all"
        assert all("side" in (t.get("input_types") or {}) for t in targets), (
            "the Run button would re-run the shape the node was rewired away "
            f"from: {[t.get('input_types') for t in targets]}"
        )

    def test_the_node_shows_its_current_inputs_not_the_union(
        self, wide, side_var, bp_node_id
    ):
        """The canvas draws the current shape. A union would put a handle on
        the node for a binding it no longer has."""
        self._wire(wide, bp_node_id)
        for_each(
            _registry._functions["bandpass_filter"],
            inputs={"signal": RawSignal, "low_hz": 20, "side": side_var},
            outputs=[FilteredSignal],
            subject=[1],
            session=["pre"],
        )

        node = next(n for n in self._fn_nodes(wide) if n["id"] == bp_node_id)
        assert node["data"]["input_params"]["side"] == "SideTable"
        assert node["data"]["input_params"]["signal"] == "RawSignal"

    def test_a_genuinely_new_call_site_is_still_its_own_node(self, client):
        """The change must not merge things that were never one node. A second
        function producing the same variable is a second topology, and a second
        wiring nothing states is a second node."""

        def scale_signal(signal, low_hz):
            return np.asarray(signal, dtype=float) / float(low_hz)

        _registry._functions["scale_signal"] = scale_signal
        for_each(
            scale_signal,
            inputs={"signal": RawSignal, "low_hz": 5},
            outputs=[FilteredSignal],
            subject=[1, 2],
            session=["pre", "post"],
        )

        labels = {
            n["data"]["label"]
            for n in client.get("/api/pipeline").json()["nodes"]
            if n.get("type") == "functionNode"
        }
        assert labels == {"bandpass_filter", "scale_signal"}


class TestDispatchRecording:
    """§7a: a GUI-started run records the association at dispatch. There is
    nothing to infer — the GUI already holds the node id."""

    def test_a_run_through_the_api_claims_its_wiring(self, client, bp_node_id):
        """The row the next build reads. It says WHICH node produced these
        records and under which run — and `written` is 0 here, not 1, because
        the node already exists: `bp_node_id` allocated it. What dispatch adds
        to an existing association is the run id."""
        from scistack_gui.services.execution_service import (
            derive_target_for_node,
            record_dispatch_wirings,
        )

        db = _gui_db.get_db()
        targets = derive_target_for_node(db, bp_node_id)
        assert targets

        record_dispatch_wirings(db, bp_node_id, "bandpass_filter", targets, "run-1")

        rows = [r for r in node_wiring.associations(db) if r["node_id"] == bp_node_id]
        assert rows, "the run is not attributed to any node"
        assert any(r["run_id"] == "run-1" for r in rows), (
            "the association does not say which run made it"
        )

    def test_a_dispatch_for_a_wiring_the_node_has_not_run_is_new(
        self, client, bp_node_id
    ):
        """The case that matters: a shape this node has never run as. THAT is
        a new row, and writing it is what stops the next build forking a
        second node for it."""
        from scistack_gui.services.execution_service import record_dispatch_wirings

        db = _gui_db.get_db()
        before = set(node_wiring.wirings_for_node(db, bp_node_id))
        targets = [
            {
                "bindings": {
                    "signal": {"kind": "variable", "ref": ["RawSignal"]},
                    "side": {"kind": "variable", "ref": ["SideTable"]},
                },
                "output_type": "FilteredSignal",
            }
        ]

        written = record_dispatch_wirings(
            db, bp_node_id, "bandpass_filter", targets, "run-2"
        )

        assert written == 1
        after = set(node_wiring.wirings_for_node(db, bp_node_id))
        assert len(after - before) == 1
        assert node_wiring.current_wiring(db, bp_node_id) in after - before, (
            "the shape the node just ran as is not its current one"
        )

    def test_a_run_with_no_node_id_records_nothing(self, client, bp_node_id):
        """A script or terminal run the GUI never saw. Nothing to record;
        `domain.node_identity` rule 2 infers it on the next build instead."""
        from scistack_gui.services.execution_service import (
            derive_target_for_node,
            record_dispatch_wirings,
        )

        db = _gui_db.get_db()
        targets = derive_target_for_node(db, bp_node_id)
        assert record_dispatch_wirings(db, None, "bandpass_filter", targets, "r") == 0

    def test_a_recording_failure_is_NOT_swallowed(self, client, bp_node_id):
        """D-2026-09-22-5. An unrecorded dispatch means the next build cannot
        tell which node produced these records, and the answer it reaches
        instead is a guess — about which node is green, which one a click runs,
        and which one your settings apply to. Letting the run proceed anyway
        would write records nothing can attribute."""
        from scistack_gui.services.execution_service import record_dispatch_wirings

        with pytest.raises(Exception):
            record_dispatch_wirings(
                None, bp_node_id, "bandpass_filter", [{"bindings": {}}], "r"
            )


class TestTheRepairPathsAreGone:
    """Stage 6: once identity no longer moves, these have nothing to do and
    should not linger as dead paths."""

    def test_superseded_manual_input_overrides_is_removed(self):
        from scistack_gui.domain import graph_builder

        assert not hasattr(graph_builder, "superseded_manual_input_overrides")

    def test_migrate_node_statements_is_removed(self):
        from scistack_gui.api import pipeline

        assert not hasattr(pipeline, "_migrate_node_statements")
