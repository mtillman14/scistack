"""
Parameter-fed glue: the Parameter keeps its own value; the glue is a history hop.

The bug (scidb.log 2026-10-21, ``load_xsens_one_file``): a Parameter ``config``
holding a PATH fed ``glue_xsens_config``, which read the JSON file and handed
the function a dict. ``apply_constant_glue`` put the glued dict into
``__constants`` and ``record_run`` stamped ``declared_name='config'`` on that
one constant edge, so history said "Parameter config = {dict}": the canvas
showed a value the user never declared, and drew ``config -> fn`` straight
past the glue node.

The fix (``.claude/plan-constant-glue-provenance-26-10-21.md``, option B) gives
a constant head the same virtual ``__glue__`` hop a variable head has:

    constant(raw path) --[declared 'config']--> glue --> __constant_glue__ --> fn

and keeps the RAW value in ``__constants`` (Q1). The glued value's content hash
is part of the virtual rid (Q2), so editing the JSON file behind an unchanged
path still recomputes.

``test_an_unchanged_rerun_skips`` is the regression guard for a bug found
writing this file: the skip gate hashed the RAW constant while history stored
the GLUED one, so a Parameter-fed glue step recomputed on every run.
"""

import json

import numpy as np
import pandas as pd
import pytest

import scidb.provenance as prov
import scifor as _scifor
from scidb import BaseVariable, GlueSpec, Parameter, configure_database, for_each
from scidb.provenance_query import (
    glue_invocation_ids,
    invocation_edges_batch,
    pipeline_variants,
)

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_constant_glue_prov.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class RawEMG(BaseVariable):
    pass


class Weighted(BaseVariable):
    pass


CALLS: list = []


def weighted(emg, cfg):
    CALLS.append(dict(cfg))
    return float(np.sum(emg["signal"]) * cfg["factor"])


def glue_load_cfg(path):
    with open(path) as f:
        return json.load(f)


def glue_load_cfg_doubled(path):
    """Same node name in tests, different body — an 'edit'."""
    with open(path) as f:
        cfg = json.load(f)
    return {**cfg, "factor": cfg["factor"] * 2}


def _seed(db):
    for subj in ("1", "2"):
        RawEMG.save(
            pd.DataFrame({"signal": [1.0, float(subj)]}),
            db=db,
            subject=subj,
            session="A",
        )


def _write_cfg(path, factor):
    path.write_text(json.dumps({"factor": factor}))
    return str(path)


def _config(*paths):
    p = Parameter(*paths)
    p.name = "config"  # what the entities loader stamps
    return p


def _run(db, cfg_param, glue_fn=glue_load_cfg):
    CALLS.clear()
    return for_each(
        weighted,
        inputs={"emg": RawEMG, "cfg": cfg_param},
        outputs=[Weighted],
        db=db,
        as_table=True,
        skip_computed=True,
        glue={"cfg": [GlueSpec(name="glue_load_cfg", fn=glue_fn)]},
        subject=[],
        session=[],
    )


def _weighted_variants(db):
    return [v for v in pipeline_variants(db._duck) if v["function_name"] == "weighted"]


def _consumer_cfg_edges(db):
    """(input_record_id, record type) of every ``weighted`` call's cfg edge."""
    return db._duck._fetchall(
        "SELECT ii.input_record_id, r.type FROM _invocation_input ii "
        "JOIN _invocation inv ON inv.invocation_id = ii.invocation_id "
        "JOIN _record r ON r.record_id = ii.input_record_id "
        "WHERE inv.function_name = ? AND ii.param_name = ?",
        ["weighted", "cfg"],
    )


# ===========================================================================
# The reported bug: the Parameter shows the glue's output
# ===========================================================================
class TestParameterValue:
    def test_the_parameter_records_its_raw_value(self, db, tmp_path):
        _seed(db)
        path = _write_cfg(tmp_path / "cfg.json", 3)
        _run(db, _config(path))

        [variant] = _weighted_variants(db)
        assert variant["constants"].get("cfg") == path, (
            f"Parameter 'config' recorded {variant['constants'].get('cfg')!r}, "
            f"not the declared path — the glued value leaked into history"
        )
        assert variant["parameter_names"] == {"cfg": "config"}
        assert variant["glue_chains"] == {"cfg": ["glue_load_cfg"]}

    def test_every_value_of_a_multi_valued_parameter_records_its_path(
        self, db, tmp_path
    ):
        _seed(db)
        a = _write_cfg(tmp_path / "a.json", 2)
        b = _write_cfg(tmp_path / "b.json", 5)
        _run(db, _config(a, b))

        recorded = {v["constants"].get("cfg") for v in _weighted_variants(db)}
        assert recorded == {a, b}


# ===========================================================================
# The history hop
# ===========================================================================
class TestHistoryHop:
    def test_the_consumer_binds_a_virtual_glue_record(self, db, tmp_path):
        _seed(db)
        _run(db, _config(_write_cfg(tmp_path / "cfg.json", 3)))

        edges = _consumer_cfg_edges(db)
        assert edges, "weighted has no cfg edge"
        assert {t for _rid, t in edges} == {prov.CONSTANT_GLUE_TYPE}
        assert glue_invocation_ids(db._duck)

    def test_the_hop_reads_the_raw_constant_named_by_the_parameter(
        self, db, tmp_path
    ):
        _seed(db)
        path = _write_cfg(tmp_path / "cfg.json", 3)
        _run(db, _config(path))

        glue_invs = glue_invocation_ids(db._duck)
        assert len(glue_invs) == 1
        [edges] = invocation_edges_batch(db._duck, list(glue_invs)).values()
        assert edges["constants"] == {prov.GLUE_INPUT_PARAM: path}
        assert edges["declared_names"] == {prov.GLUE_INPUT_PARAM: "config"}


# ===========================================================================
# Q1: the raw value is the call-site key
# ===========================================================================
class TestCallSite:
    def test_editing_the_file_content_keeps_one_call_site(self, db, tmp_path):
        """The path is what the user declared; the file's content is a new
        VERSION at the same call site, exactly as an edited function body is."""
        _seed(db)
        cfg = tmp_path / "cfg.json"
        _run(db, _config(_write_cfg(cfg, 3)))
        _run(db, _config(_write_cfg(cfg, 4)))

        call_ids = {v["call_id"] for v in _weighted_variants(db)}
        assert len(call_ids) == 1, call_ids


# ===========================================================================
# Identity guarantees that must survive the change (pass today)
# ===========================================================================
class TestRecompute:
    def test_an_unchanged_rerun_skips(self, db, tmp_path):
        _seed(db)
        param = _config(_write_cfg(tmp_path / "cfg.json", 3))
        _run(db, param)
        assert CALLS, "the first run computed nothing"

        _run(db, param)
        assert CALLS == [], f"an unchanged re-run recomputed {len(CALLS)} call(s)"

    def test_editing_the_glue_body_recomputes(self, db, tmp_path):
        _seed(db)
        param = _config(_write_cfg(tmp_path / "cfg.json", 3))
        _run(db, param)

        _run(db, param, glue_fn=glue_load_cfg_doubled)
        assert CALLS, "the edited glue body did not recompute"
        assert {c["factor"] for c in CALLS} == {6}

    def test_editing_the_file_behind_an_unchanged_path_recomputes(
        self, db, tmp_path
    ):
        """Q2: the glued value's hash is part of identity, so the content of
        config.json is — not only its path and the glue body."""
        _seed(db)
        cfg = tmp_path / "cfg.json"
        _run(db, _config(_write_cfg(cfg, 3)))

        _run(db, _config(_write_cfg(cfg, 4)))
        assert CALLS, "a changed config file behind the same path did not recompute"
        assert {c["factor"] for c in CALLS} == {4}


# ===========================================================================
# One owner per concept (the audit of 2026-10-21)
# ===========================================================================
class TestOneOwner:
    def test_history_branch_params_match_what_the_save_recorded(self, db, tmp_path):
        """derived_branch_params walks history; the save stamped branch_params
        from __constants. Both must name the Parameter-fed constant under the
        CONSUMER with its raw value — not walk into the glue hop."""
        from scidb import provenance_query as pq

        _seed(db)
        path = _write_cfg(tmp_path / "cfg.json", 3)
        _run(db, _config(path))

        [rid, *_] = [
            r[0]
            for r in db._duck._fetchall(
                "SELECT record_id FROM _record WHERE type = ?", ["Weighted"]
            )
        ]
        assert pq.derived_branch_params(db._duck, rid) == {"weighted.cfg": path}

    def test_variant_keys_batch_matches_the_per_record_key(self, db, tmp_path):
        from scidb import provenance_query as pq

        _seed(db)
        _run(db, _config(_write_cfg(tmp_path / "cfg.json", 3)))
        rids = [
            r[0]
            for r in db._duck._fetchall(
                "SELECT record_id FROM _record WHERE type = ?", ["Weighted"]
            )
        ]
        batch = pq.variant_keys_batch(db._duck, rids)
        for rid in rids:
            assert batch[rid] == pq._producing_variant_key(db._duck, rid)
            assert batch[rid] is not None and "cfg" in repr(batch[rid])

    def test_a_glued_constant_is_not_a_variable_input(self, db, tmp_path):
        """A function whose only input is a glued Parameter is inputless —
        decided by the record type, the same answer in every reader."""
        from scidb import provenance_query as pq

        def make(cfg):
            return float(cfg["factor"])

        path = _write_cfg(tmp_path / "cfg.json", 3)
        for_each(
            make,
            inputs={"cfg": _config(path)},
            outputs=[Weighted],
            db=db,
            glue={"cfg": [GlueSpec(name="glue_load_cfg", fn=glue_load_cfg)]},
            subject=["1", "2"],
            session=["A"],
        )
        assert pq.is_inputless_function(db._duck, "make")
        assert pq.realized_inputless_invocations(db._duck, "make")

        # And it is realized under its RAW constant (the PathInput-node check).
        from scidb.provenance import compute_constant_record_id

        sids = pq.realized_inputless_schema_ids(
            db._duck, "make", {"cfg": compute_constant_record_id(path)}
        )
        assert sids, "realized locations not matched by the raw constant"

    def test_record_run_logs_the_hop_once(self, db, tmp_path, caplog):
        import logging

        _seed(db)
        with caplog.at_level(logging.INFO):
            _run(db, _config(_write_cfg(tmp_path / "cfg.json", 3)))
        hits = [
            r for r in caplog.records
            if "Parameter-fed glue recorded as a history hop" in r.getMessage()
        ]
        assert len(hits) == 1, [r.getMessage() for r in hits]

    def test_record_types_are_spelled_only_in_provenance(self):
        """AST guard: the record-type strings live in scidb.provenance and its
        type sets (GLUE_RECORD_TYPES, NON_VARIABLE_INPUT_TYPES,
        INTERNAL_RECORD_TYPES). A reader that spells one inline drifts the day
        a type is added — the bug class this whole change removed."""
        import ast
        from pathlib import Path

        import scidb
        from scidb.provenance import INTERNAL_RECORD_TYPES

        root = Path(scidb.__file__).parent
        offenders = []
        for path in sorted(root.rglob("*.py")):
            if path.name == "provenance.py":
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            docstrings = {
                id(node.body[0].value)
                for node in ast.walk(tree)
                if isinstance(
                    node,
                    (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
                )
                and node.body
                and isinstance(node.body[0], ast.Expr)
                and isinstance(node.body[0].value, ast.Constant)
            }
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Constant)
                    and isinstance(node.value, str)
                    and id(node) not in docstrings
                    and any(
                        node.value == t or f"'{t}'" in node.value
                        for t in INTERNAL_RECORD_TYPES
                    )
                ):
                    offenders.append(f"{path.relative_to(root)}:{node.lineno}")
        assert not offenders, (
            "record-type strings spelled outside scidb.provenance — use its "
            f"constants / type sets: {offenders}"
        )


class TestAggregate:
    def test_the_aggregate_carries_each_arguments_glue_chain(self, db, tmp_path):
        """The canvas draws a glued history input through its glue node only
        if it knows the chain (scistack_gui build_edges)."""
        _seed(db)
        _run(db, _config(_write_cfg(tmp_path / "cfg.json", 3)))

        agg = db.get_aggregated_variants()
        [fn_data] = [
            d for (fn, _cid), d in agg["functions"].items() if fn == "weighted"
        ]
        assert fn_data["glue_chains"] == {"cfg": ["glue_load_cfg"]}
        assert fn_data["parameter_names"] == {"cfg": "config"}
        assert "cfg" not in fn_data["input_params"]
