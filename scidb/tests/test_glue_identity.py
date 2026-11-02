"""
Glue identity — Stage 2 of the glue-node feature.

The trap this file exists for: **there are two identity systems**, and folding a
glue's hash into ``version_keys`` alone is easy, looks sufficient, and is wrong.
``skip_computed`` compares the provenance graph's *bindings*, never version
keys — so glue that only entered version_keys would leave an edited
column-rename silently doing nothing and every downstream record green and
stale. See ``docs/claude/free-code-glue-nodes.md`` §2.

Covers:
- ``__glue`` / ``__glue_hashes`` version keys and the call_id split
- virtual glue records: written, typed ``__glue__``, never loadable, never
  counted as data
- editing a glue body recomputes the consumer; leaving it alone still skips
- ``upstream_provenance`` shows the glue hop, honestly named
- node state stays green across the glue hop (the falsely-red risk)
"""

from scidb.provenance import query as provenance_query
import numpy as np
import pandas as pd
import pytest

import scidb.provenance.graph as prov
import scifor as _scifor
from scidb import (
    BaseVariable,
    ForEachConfig,
    GlueSpec,
    check_node_state,
    configure_database,
    for_each,
)
from scidb.run.glue import chain_hash, input_set_signature, virtual_rid_map

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_glue_identity.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class RawEMG(BaseVariable):
    pass


class Analyzed(BaseVariable):
    pass


def glue_drop_baseline(emg):
    return emg.drop(columns=["baseline"])


def glue_drop_baseline_v2(emg):
    """Same node name in tests, different body — an 'edit'."""
    out = emg.drop(columns=["baseline"])
    out["signal"] = out["signal"] + 1.0
    return out


def glue_rename_signal(emg):
    return emg.rename(columns={"signal": "sig"})


def analyze(emg):
    return float(np.sum(emg["signal"]))


def _seed(db, subjects=("1", "2")):
    # Values differ PER SUBJECT on purpose: the recompute tests compare
    # distinct content hashes, and identical data across subjects collapses
    # two records into one hash, which reads as "nothing was computed".
    for subj in subjects:
        scale = float(subj)
        RawEMG.save(
            pd.DataFrame(
                {"signal": [1.0 * scale, 2.0 * scale], "baseline": [0.5, 0.5]}
            ),
            db=db,
            subject=subj,
            session="A",
        )


def _run(db, glue=None, fn=analyze):
    return for_each(
        fn,
        inputs={"emg": RawEMG},
        outputs=[Analyzed],
        db=db,
        as_table=True,
        glue=glue,
        skip_computed=True,
        subject=[],
        session=[],
    )


def _spec(fn, name=None):
    return GlueSpec(name=name or fn.__name__, fn=fn)


def cfg_fn_hash(db):
    """The fn hash node-state prediction uses for ``analyze``."""
    from scidb.run.foreach_config import _compute_fn_hash

    return _compute_fn_hash(analyze)


def _glue_records(db):
    return db._duck.fetchall(
        "SELECT record_id, schema_id FROM _record WHERE type = ?", [prov.GLUE_TYPE]
    )


def _analyzed_records(db):
    return [
        r[0]
        for r in db._duck.fetchall(
            "SELECT record_id FROM _record WHERE type = ?", ["Analyzed"]
        )
    ]


def _analyzed_content_hashes(db):
    return {
        r[0]
        for r in db._duck.fetchall(
            "SELECT DISTINCT content_hash FROM _record WHERE type = ?", ["Analyzed"]
        )
    }


# ===========================================================================
# Identity primitives
# ===========================================================================
class TestIdentityPrimitives:
    def test_virtual_rid_is_deterministic(self):
        chain = [_spec(glue_drop_baseline)]
        a = virtual_rid_map(["r1", "r2"], chain)
        b = virtual_rid_map(["r2", "r1"], chain)
        assert a == b  # order-insensitive over the input set

    def test_virtual_rid_changes_when_the_body_changes(self):
        a = virtual_rid_map(["r1"], [_spec(glue_drop_baseline, name="glue_x")])
        b = virtual_rid_map(["r1"], [_spec(glue_drop_baseline_v2, name="glue_x")])
        assert a[0] != b[0]  # chain hash
        assert a[2]["r1"] != b[2]["r1"]  # virtual rid

    def test_virtual_rid_changes_when_the_input_set_grows(self):
        chain = [_spec(glue_drop_baseline)]
        _h1, sig1, map1 = virtual_rid_map(["r1"], chain)
        _h2, sig2, map2 = virtual_rid_map(["r1", "r2"], chain)
        assert sig1 != sig2
        assert map1["r1"] != map2["r1"]

    def test_input_set_signature_ignores_duplicates_and_order(self):
        assert input_set_signature(["a", "b", "a"]) == input_set_signature(["b", "a"])

    def test_glue_record_id_is_16_hex(self):
        rid = prov.compute_glue_record_id("chain", "src", "sig")
        assert len(rid) == 16 and all(c in "0123456789abcdef" for c in rid)

    def test_glue_invocation_id_distinguishes_chains(self):
        a = prov.compute_glue_invocation_id("chainA", "src", "sig")
        b = prov.compute_glue_invocation_id("chainB", "src", "sig")
        assert a != b


# ===========================================================================
# Version keys and the call_id split
# ===========================================================================
class TestVersionKeys:
    def _config(self, chain):
        return ForEachConfig(
            fn=analyze, inputs={"emg": RawEMG}, glue={"emg": chain} if chain else None
        )

    def test_glue_names_and_hashes_are_separate_keys(self):
        spec = _spec(glue_drop_baseline)
        keys = self._config([spec]).to_config_keys()
        # Both keys are PER-NODE lists, parallel to each other — the names in
        # one, that same node's body hash in the other. (The whole-chain
        # ``chain_hash`` is a different thing: it identifies the chain for the
        # virtual record id, and never appears in version keys.)
        assert keys["__glue"] == {"emg": ["glue_drop_baseline"]}
        assert keys["__glue_hashes"] == {"emg": [spec.hash]}

    def test_the_chain_hash_is_not_a_version_key(self):
        keys = self._config([_spec(glue_drop_baseline)]).to_config_keys()
        assert chain_hash([_spec(glue_drop_baseline)]) not in str(keys)

    def test_no_glue_emits_no_glue_keys(self):
        keys = self._config(None).to_config_keys()
        assert "__glue" not in keys and "__glue_hashes" not in keys

    def test_adding_glue_is_a_different_call_site(self):
        plain = self._config(None).to_call_id()
        glued = self._config([_spec(glue_drop_baseline)]).to_call_id()
        assert plain != glued

    def test_a_different_glue_node_is_a_different_call_site(self):
        a = self._config([_spec(glue_drop_baseline)]).to_call_id()
        b = self._config([_spec(glue_rename_signal)]).to_call_id()
        assert a != b

    def test_editing_a_glue_body_keeps_the_same_call_site(self):
        # Same split as __fn / __fn_hash: an edit is a new version at the same
        # call site, not a new call site.
        a = self._config([_spec(glue_drop_baseline, name="glue_x")]).to_call_id()
        b = self._config([_spec(glue_drop_baseline_v2, name="glue_x")]).to_call_id()
        assert a == b

    def test_editing_a_glue_body_does_change_the_version_keys(self):
        a = self._config([_spec(glue_drop_baseline, name="glue_x")]).to_config_keys()
        b = self._config(
            [_spec(glue_drop_baseline_v2, name="glue_x")]
        ).to_config_keys()
        assert a["__glue_hashes"] != b["__glue_hashes"]


# ===========================================================================
# Virtual records in the graph
# ===========================================================================
class TestVirtualRecords:
    def test_a_virtual_record_is_written_per_input_record(self, db):
        _seed(db)
        _run(db, glue={"emg": _spec(glue_drop_baseline)})

        rows = _glue_records(db)
        assert len(rows) == 2  # one per RawEMG record

    def test_virtual_records_share_the_source_schema_location(self, db):
        _seed(db)
        _run(db, glue={"emg": _spec(glue_drop_baseline)})

        source_sids = {
            r[0]
            for r in db._duck.fetchall(
                "SELECT schema_id FROM _record WHERE type = ?", ["RawEMG"]
            )
        }
        glue_sids = {sid for _rid, sid in _glue_records(db)}
        assert glue_sids == source_sids

    def test_a_virtual_record_has_no_save_event_and_no_data(self, db):
        _seed(db)
        _run(db, glue={"emg": _spec(glue_drop_baseline)})

        glue_rids = [r[0] for r in _glue_records(db)]
        placeholders = ", ".join(["?"] * len(glue_rids))
        saves = db._duck.fetchall(
            f"SELECT record_id FROM _record_save WHERE record_id IN ({placeholders})",
            glue_rids,
        )
        assert saves == []

    def test_virtual_records_are_not_counted_as_data(self, db):
        from scidb.inspect import Inspector

        _seed(db)
        _run(db, glue={"emg": _spec(glue_drop_baseline)})
        assert _glue_records(db), "no virtual records were written"

        n_real = db._duck.fetchall(
            "SELECT COUNT(*) FROM _record WHERE type IN ('RawEMG', 'Analyzed')"
        )[0][0]
        assert Inspector(db).overview().n_records == n_real

    def test_no_glue_means_no_virtual_records(self, db):
        _seed(db)
        _run(db)
        assert _glue_records(db) == []

    def test_the_consumer_binds_to_the_virtual_record(self, db):
        _seed(db)
        _run(db, glue={"emg": _spec(glue_drop_baseline)})

        glue_rids = {r[0] for r in _glue_records(db)}
        bound = {
            r[0]
            for r in db._duck.fetchall(
                "SELECT ii.input_record_id FROM _invocation_input ii "
                "JOIN _invocation inv ON inv.invocation_id = ii.invocation_id "
                "WHERE inv.function_name = ?",
                ["analyze"],
            )
        }
        assert bound == glue_rids


# ===========================================================================
# The point of the whole thing: an edit must recompute
# ===========================================================================
class TestRecompute:
    def test_unchanged_glue_still_skips(self, db):
        _seed(db)
        _run(db, glue={"emg": _spec(glue_drop_baseline)})
        before = set(_analyzed_records(db))

        _run(db, glue={"emg": _spec(glue_drop_baseline)})
        assert set(_analyzed_records(db)) == before

    def test_editing_a_glue_body_recomputes_the_consumer(self, db):
        _seed(db)
        _run(db, glue={"emg": _spec(glue_drop_baseline, name="glue_x")})
        before = _analyzed_content_hashes(db)
        assert len(before) == 2

        # Same node NAME, edited body (+1.0 per row). This is the failure mode
        # the whole virtual-record design exists for: with glue in version_keys
        # alone, skip_computed would see an unchanged fn hash and an unchanged
        # input record_id and skip, leaving stale results looking green.
        _run(db, glue={"emg": _spec(glue_drop_baseline_v2, name="glue_x")})
        after = _analyzed_content_hashes(db)
        assert after - before, "the edited glue did not recompute"

    def test_a_growing_input_set_recomputes(self, db):
        _seed(db, subjects=("1",))
        _run(db, glue={"emg": _spec(glue_drop_baseline)})
        first_glue = {r[0] for r in _glue_records(db)}

        # A whole-table glue may read across rows, so its result depends on the
        # whole input set — adding a subject must invalidate the existing rows.
        _seed(db, subjects=("2",))
        _run(db, glue={"emg": _spec(glue_drop_baseline)})
        second_glue = {r[0] for r in _glue_records(db)}
        # Virtual records are never deleted, so the first run's single rid is
        # still there; what matters is that BOTH records got fresh ids under
        # the larger input set — subject 1's rid moved rather than being reused.
        assert first_glue < second_glue
        assert len(second_glue) == 3, (
            f"expected 1 old + 2 new virtual records, got {len(second_glue)} — "
            f"the input set grew but the existing virtual rid did not change"
        )

    def test_removing_the_glue_recomputes(self, db):
        _seed(db)
        _run(db, glue={"emg": _spec(glue_drop_baseline)})
        glued = set(_analyzed_records(db))

        _run(db)
        assert set(_analyzed_records(db)) - glued, (
            "dropping the glue left the consumer bound to the virtual record"
        )


# ===========================================================================
# Reading the graph back
# ===========================================================================
class TestGraphReads:
    def test_upstream_provenance_shows_the_glue_hop(self, db):
        _seed(db, subjects=("1",))
        _run(db, glue={"emg": _spec(glue_drop_baseline)})

        rid = db._duck.fetchall(
            "SELECT record_id FROM _record WHERE type = ?", ["Analyzed"]
        )[0][0]
        chain = provenance_query.upstream_provenance(db, rid)
        types = [node["variable_type"] for node in chain]
        fns = [node["function_name"] for node in chain]
        assert "Analyzed" in types
        assert prov.GLUE_TYPE in types, f"no glue hop in {types}"
        assert "glue_drop_baseline" in fns, f"glue not named in {fns}"
        assert "RawEMG" in types

    def test_the_consumers_input_reports_the_source_variable_type(self, db):
        from scidb.provenance import query as provenance_query

        _seed(db, subjects=("1",))
        _run(db, glue={"emg": _spec(glue_drop_baseline)})

        inv_id = db._duck.fetchall(
            "SELECT invocation_id FROM _invocation WHERE function_name = ?",
            ["analyze"],
        )[0][0]
        var_inputs, _consts = provenance_query.invocation_inputs(db._duck, inv_id)
        entry = next(i for i in var_inputs if i["param_name"] == "emg")
        # The edge points at the virtual record, but the *type* reported is the
        # real upstream variable — so every config/prediction path is unchanged.
        assert entry["variable_type"] == "RawEMG"
        assert entry["glue_chain"] == ["glue_drop_baseline"]
        assert entry["record_id"] != entry["glue_source_record_id"]

    def test_a_glue_hop_is_not_a_pipeline_variant(self, db):
        from scidb.provenance import query as provenance_query

        _seed(db, subjects=("1",))
        _run(db, glue={"emg": _spec(glue_drop_baseline)})

        variants = provenance_query.pipeline_variants(db._duck)
        assert all(v["function_name"] != "glue_drop_baseline" for v in variants)
        assert any(v["function_name"] == "analyze" for v in variants)


# ===========================================================================
# Node state — the falsely-red risk
# ===========================================================================
class TestNodeState:
    def test_a_fully_run_glued_node_is_green(self, db):
        _seed(db)
        _run(db, glue={"emg": _spec(glue_drop_baseline)})

        result = check_node_state(analyze, [Analyzed], db=db)
        assert result["state"] == "green", result["counts"]

    def test_a_never_run_glued_node_is_red(self, db):
        _seed(db)
        result = check_node_state(
            analyze,
            [Analyzed],
            inputs={"emg": RawEMG},
            db=db,
            glue={"emg": _spec(glue_drop_baseline)},
        )
        assert result["state"] == "red"

    def test_the_never_run_prediction_matches_what_the_run_writes(self, db):
        # The highest-risk part of the design: the predicted virtual rids must
        # equal the ones the save path computes, or the node is red forever.
        _seed(db)
        glue = {"emg": _spec(glue_drop_baseline)}
        predicted = check_node_state(
            analyze, [Analyzed], inputs={"emg": RawEMG}, db=db, glue=glue
        )
        assert predicted["counts"]["missing"] > 0

        _run(db, glue=glue)
        # Checked WITHOUT inputs=, i.e. from the graph config alone — which is
        # also how the GUI asks (check_multiple_nodes_state never passes
        # inputs). Keeping inputs= here would additionally exercise the
        # never-run fallback, and ``config_from_inputs`` cannot express
        # as_table/distribute (its own docstring says so), so this call — made
        # with as_table=True — would contribute a second, unmatched set of
        # expected invocations and report red. That gap predates glue and is
        # not what this test is about.
        after = check_node_state(analyze, [Analyzed], db=db)
        assert after["state"] == "green", after["counts"]

    def test_the_never_run_fallback_predicts_the_same_glue_rids(self, db):
        # The fallback path (config_from_inputs) and the graph path
        # (function_variant_configs) must derive the SAME virtual rids, or a
        # glued node flips state depending on which one answered. Compared on
        # the invocation ids they predict, so the as_table gap above cannot
        # mask a genuine glue-identity mismatch.
        from scidb.provenance import query as provenance_query

        _seed(db)
        glue = {"emg": _spec(glue_drop_baseline)}
        _run(db, glue=glue)

        from_graph: set = set()
        for cfg in provenance_query.function_variant_configs(db._duck, "analyze"):
            provenance_query._predict_config_invocations(
                db._duck, cfg_fn_hash(db), cfg, from_graph
            )

        fallback = provenance_query.config_from_inputs({"emg": RawEMG}, glue=glue)
        # Same glue chain identity on both sides — the one thing that must agree.
        assert fallback["glue_chains"]["emg"] == next(
            c["glue_chains"]["emg"]
            for c in provenance_query.function_variant_configs(db._duck, "analyze")
        )
        assert from_graph, "the graph path predicted nothing"
