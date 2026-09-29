"""A call site with more than one output type hashes to ONE agreed wiring.

scidb.log 2026-09-29: ``pandas.read_csv(DemographicsPath)`` ran into
``Demographics``, its node's output was rewired to ``DemographicsTable`` and
run. A call site excludes outputs, so both histories were ONE call site. The
canvas hashed the union of its outputs (``1540882c…``), a wiring no Run had
claimed, and minted a phantom node; the run path hashed each output alone, so
the phantom's Run matched none of its own history ("no targets").

The one owner is now ``scidb.provenance.split_call_site_outputs``: a call
site's recorded outputs are split by the wirings Runs claimed together.
``.claude/plan-multi-output-call-site-wiring.md``.
"""

from __future__ import annotations

import ast
from pathlib import Path

import numpy as np
import pytest

import scistack_gui.db as _gui_db
from scidb import BaseVariable, PathInput, for_each
from scistack_gui import node_wiring, registry as _registry


class Table(BaseVariable):
    pass


class Table2(BaseVariable):
    pass


class Left(BaseVariable):
    pass


class Right(BaseVariable):
    pass


def load_one(filepath):
    return np.asarray([len(str(filepath))], dtype=float)


def load_two(filepath):
    n = float(len(str(filepath)))
    return np.asarray([n]), np.asarray([n + 1])


def _pi(tmp_path):
    return PathInput("{subject}/{session}.csv", root_folder=str(tmp_path / "a"), name="RAW")


def _seed_files(tmp_path):
    for subj in (1, 2):
        (tmp_path / "a" / str(subj)).mkdir(parents=True, exist_ok=True)
        for sess in ("pre", "post"):
            (tmp_path / "a" / str(subj) / f"{sess}.csv").write_text("x\n")


def _graph(client):
    return client.get("/api/pipeline").json()


def _fn_nodes(graph, label):
    return [
        n
        for n in graph["nodes"]
        if n.get("type") == "functionNode" and n["data"]["label"] == label
    ]


def _known(db, label):
    return {n for n in node_wiring.known_node_ids(db) if n.startswith(f"fn__{label}__")}


# ---------------------------------------------------------------------------
# The user's sequence: run into A, rewire the node's output to B, run, build
# ---------------------------------------------------------------------------


@pytest.fixture
def rewired_and_run(client, tmp_path):
    from scistack_gui.services.execution_service import (
        derive_target_for_node,
        record_dispatch_wirings,
    )

    _seed_files(tmp_path)
    raw = _pi(tmp_path)
    _registry._path_inputs["RAW"] = raw
    _registry._functions["load_one"] = load_one
    for_each(load_one, inputs={"filepath": raw}, outputs=[Table], subject=[1, 2], session=["pre", "post"])

    (node,) = _fn_nodes(_graph(client), "load_one")
    node_id = node["id"]
    db = _gui_db.get_db()

    # Rewire the output: draw node -> Table2.
    r = client.put(
        "/api/edges/manual__out2",
        json={"source": node_id, "target": "var__Table2", "source_handle": None, "target_handle": None},
    )
    assert r.status_code == 200, r.text
    _graph(client)

    # Run it from the node: dispatch claims what it will write, then the run.
    targets = derive_target_for_node(db, node_id)
    assert {t["output_type"] for t in targets} == {"Table2"}
    record_dispatch_wirings(db, node_id, "load_one", targets, run_id="rewire-run")
    for_each(load_one, inputs={"filepath": raw}, outputs=[Table2], subject=[1, 2], session=["pre", "post"])

    graph = _graph(client)
    return client, db, node_id, graph


class TestARewiredOutputKeepsItsNode:
    def test_no_phantom_node_is_minted(self, rewired_and_run):
        _client, db, node_id, _graph_after = rewired_and_run
        assert _known(db, "load_one") == {node_id}, (
            f"the build minted a node for the old+new output union: {sorted(_known(db, 'load_one'))}"
        )

    def test_the_node_draws_its_new_output_only(self, rewired_and_run):
        _client, _db, node_id, graph = rewired_and_run
        outs = {
            e["target"]
            for e in graph["edges"]
            if e["source"].split("::")[0] == node_id.split("::")[0]
        }
        assert "var__Table2" in outs
        assert "var__Table" not in outs, "the node still draws the output it was rewired away from"

    def test_its_run_finds_its_history(self, rewired_and_run):
        from scistack_gui.services.execution_service import derive_target_for_node

        client, db, node_id, _graph_after = rewired_and_run
        # Remove the drawn edge: the RECORDED history alone must now match.
        assert client.delete("/api/edges/manual__out2").status_code in (200, 404)
        _graph(client)
        targets = derive_target_for_node(db, node_id)
        assert targets, "the node's Run matched none of its own history"
        assert {t["output_type"] for t in targets} == {"Table2"}


# ---------------------------------------------------------------------------
# A genuinely multi-output function
# ---------------------------------------------------------------------------


@pytest.fixture
def two_output_client(client, tmp_path):
    _seed_files(tmp_path)
    raw = _pi(tmp_path)
    _registry._path_inputs["RAW"] = raw
    _registry._functions["load_two"] = load_two
    for_each(load_two, inputs={"filepath": raw}, outputs=[Left, Right], subject=[1, 2], session=["pre", "post"])
    return client


class TestAMultiOutputFunctionIsOneNode:
    def test_one_node_draws_both_outputs(self, two_output_client):
        graph = _graph(two_output_client)
        (node,) = _fn_nodes(graph, "load_two")
        outs = {e["target"] for e in graph["edges"] if e["source"].split("::")[0] == node["id"]}
        assert {"var__Left", "var__Right"} <= outs

    def test_its_run_finds_both_outputs(self, two_output_client):
        """Suspected broken before 2026-09-29: the run path hashed each output
        alone and matched nothing of the node's (union) wiring."""
        from scistack_gui.services.execution_service import derive_target_for_node

        (node,) = _fn_nodes(_graph(two_output_client), "load_two")
        targets = derive_target_for_node(_gui_db.get_db(), node["id"])
        assert {t["output_type"] for t in targets} == {"Left", "Right"}

    def test_dispatch_claims_one_wiring_and_the_node_survives_a_rerun(self, two_output_client):
        from scistack_gui.services.execution_service import (
            derive_target_for_node,
            record_dispatch_wirings,
        )

        db = _gui_db.get_db()
        (node,) = _fn_nodes(_graph(two_output_client), "load_two")
        before = node_wiring.current_wiring(db, node["id"])
        targets = derive_target_for_node(db, node["id"])
        record_dispatch_wirings(db, node["id"], "load_two", targets, run_id="rerun")

        assert node_wiring.wirings_for_node(db, node["id"]) == [before], (
            "a Run of both outputs claimed per-output wirings, which would split the node"
        )
        _graph(two_output_client)
        assert _known(db, "load_two") == {node["id"]}


# ---------------------------------------------------------------------------
# The pure split on the aggregate
# ---------------------------------------------------------------------------


class TestSplitCallSitesByClaims:
    def _agg(self):
        from scistack_gui.domain.graph_builder import AggregatedData

        agg = AggregatedData()
        fkey = ("f", "cid1")
        agg.fn_input_params[fkey] = {"x": "In"}
        agg.fn_outputs[fkey] = {"A", "B"}
        agg.fn_constants[fkey] = {"k"}
        agg.fn_variants_map[fkey] = [
            {"output_type": "A", "constants": {"k": 1}},
            {"output_type": "B", "constants": {"k": 1}},
        ]
        agg.const_fns["k"] = {fkey}
        return agg, fkey

    def test_no_claims_leaves_the_call_site_alone(self):
        from scistack_gui.domain.graph_builder import split_call_sites_by_claims

        agg, fkey = self._agg()
        split_call_sites_by_claims(agg, set())
        assert set(agg.fn_outputs) == {fkey}

    def test_per_output_claims_split_it(self):
        from scidb.provenance import compute_wiring_id

        from scistack_gui.domain.graph_builder import call_id_of, split_call_sites_by_claims

        agg, fkey = self._agg()
        claims = {compute_wiring_id("f", {"x": "In"}, [o], {}) for o in ("A", "B")}
        split_call_sites_by_claims(agg, claims)

        assert fkey not in agg.fn_outputs
        assert sorted(sorted(o) for o in agg.fn_outputs.values()) == [["A"], ["B"]]
        for sub, outs in agg.fn_outputs.items():
            assert call_id_of(sub[1]) == "cid1"
            assert [r["output_type"] for r in agg.fn_variants_map[sub]] == sorted(outs)
            assert agg.fn_input_params[sub] == {"x": "In"}
        assert agg.const_fns["k"] == set(agg.fn_outputs)


# ---------------------------------------------------------------------------
# Guard: the run path never hashes one output alone again
# ---------------------------------------------------------------------------


def test_the_run_path_has_no_per_output_wiring_hash():
    """Every run-path wiring comes from history_variant_wirings (the owner) or
    from a whole set of outputs. A `wiring_id(..., {x["output_type"]}, ...)`
    or `{x.get("output_type")}` third argument is the two-owner split coming
    back."""
    import scistack_gui.services.execution_service as mod

    tree = ast.parse(Path(mod.__file__).read_text())
    offenders = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", None) == "wiring_id"):
            continue
        if len(node.args) < 3 or not isinstance(node.args[2], ast.Set):
            continue
        if "output_type" in ast.unparse(node.args[2]):
            offenders.append(f"line {node.lineno}: {ast.unparse(node)}")
    assert not offenders, offenders
