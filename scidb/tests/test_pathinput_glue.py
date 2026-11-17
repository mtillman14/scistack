"""
PathInput -> glue -> function (issue #21).

Until 2026-11-17 ``refuse_pathinput_glue`` refused this wiring outright: "a
PathInput is resolved per combo, so no loaded table exists for glue to
reshape". No table is needed. The glue gets the RESOLVED PATH, the value the
function would otherwise have received, and returns what the function should
get instead (typically the parsed file).

Design: docs/claude/free-code-glue-nodes.md §1, "A PathInput-fed parameter".
These tests pin:

* what the glue and the function receive;
* the level: once per distinct resolved path, NOT once per function combo;
* the copy: a function mutating its input cannot leak into the next combo;
* identity: ``__pathinput__ -> glue -> __pathinput_glue__ -> fn``, a rerun
  skips, an edited body recomputes, node state goes green;
* a glue error fails only its own combo;
* ``Fixed(PathInput)`` runs but warns that its identity is untracked;
* MATLAB glue on a PathInput is refused in a Python run.
"""

import json
import logging
from pathlib import Path

import pytest

import scidb.provenance.graph as prov
import scifor as _scifor
from scidb import (
    BaseVariable,
    GlueLanguageMismatchError,
    GlueSpec,
    configure_database,
    for_each,
)
from scidb.provenance.query import (
    glue_invocation_ids,
    invocation_edges_batch,
    pipeline_variants,
)
from scifor import EachOf, Fixed, PathInput

SCHEMA = ["subject", "session"]
SUBJECTS = ["1", "2"]
SESSIONS = ["A", "B"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_pathinput_glue.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class Weighted(BaseVariable):
    pass


class RawEMG(BaseVariable):
    pass


# --- Glue bodies and the consumer; each records what it was handed ---------

GLUE_SEEN: list = []
FN_SEEN: list = []


def glue_load_cfg(path):
    GLUE_SEEN.append(path)
    with open(path) as f:
        return json.load(f)


def glue_load_cfg_doubled(path):
    """Same node NAME in tests, different body — an 'edit'."""
    GLUE_SEEN.append(path)
    with open(path) as f:
        cfg = json.load(f)
    return {**cfg, "factor": cfg["factor"] * 2}


def glue_fails_for_subject_2(path):
    if "sub2" in str(path):
        raise ValueError("cannot parse sub2's config")
    with open(path) as f:
        return json.load(f)


def use_cfg(cfg):
    FN_SEEN.append(dict(cfg))
    return float(cfg["factor"])


def mutate_cfg(cfg):
    """A function that MUTATES its input — must not leak to the next combo."""
    cfg["factor"] = cfg["factor"] + 100
    FN_SEEN.append(dict(cfg))
    return float(cfg["factor"])


def weigh(emg, cfg):
    """A scalar variable input times the glued factor — the glue is what is
    under test, so the variable side stays as plain as possible."""
    FN_SEEN.append(dict(cfg))
    return float(emg) * cfg["factor"]


# --- Helpers ----------------------------------------------------------------


def _write_subject_cfgs(root, factors=None):
    """``sub{subject}.json`` per subject: a {subject}-level PathInput."""
    factors = factors or {"1": 2, "2": 3}
    for subj, factor in factors.items():
        (Path(root) / f"sub{subj}.json").write_text(json.dumps({"factor": factor}))


def _subject_cfg(root):
    return PathInput("sub{subject}.json", root_folder=str(root), name="subject_cfg")


def _spec(fn=glue_load_cfg, **kw):
    return GlueSpec(name="glue_load_cfg", fn=fn, **kw)


def _run(db, root, fn=use_cfg, glue_fn=glue_load_cfg, cfg=None, **kw):
    GLUE_SEEN.clear()
    FN_SEEN.clear()
    return for_each(
        fn,
        inputs={"cfg": cfg if cfg is not None else _subject_cfg(root)},
        outputs=[Weighted],
        db=db,
        skip_computed=True,
        glue={"cfg": [_spec(glue_fn)]},
        subject=SUBJECTS,
        session=SESSIONS,
        **kw,
    )


def _consumer_cfg_edges(db, fn_name="use_cfg"):
    """(input_record_id, record type) of every consumer call's cfg edge."""
    return db._duck.fetchall(
        "SELECT ii.input_record_id, r.type FROM _invocation_input ii "
        "JOIN _invocation inv ON inv.invocation_id = ii.invocation_id "
        "JOIN _record r ON r.record_id = ii.input_record_id "
        "WHERE inv.function_name = ? AND ii.param_name = ?",
        [fn_name, "cfg"],
    )


# ===========================================================================
# What runs, and with what
# ===========================================================================
class TestWhatRuns:
    def test_the_glue_gets_the_resolved_path_and_the_function_its_output(
        self, db, tmp_path
    ):
        _write_subject_cfgs(tmp_path)
        _run(db, tmp_path)

        assert GLUE_SEEN, "the glue never ran"
        assert all(isinstance(p, Path) for p in GLUE_SEEN), (
            f"the glue must receive the resolved path, got {GLUE_SEEN!r}"
        )
        assert {p.name for p in GLUE_SEEN} == {"sub1.json", "sub2.json"}
        assert sorted(c["factor"] for c in FN_SEEN) == [2, 2, 3, 3]

    def test_a_subject_level_pathinput_is_glued_once_per_subject(self, db, tmp_path):
        """The function runs at [subject, session] (4 combos); the PathInput
        is {subject}-level (2 files), so the glue runs twice, not four times."""
        _write_subject_cfgs(tmp_path)
        _run(db, tmp_path)

        assert len(FN_SEEN) == 4
        assert len(GLUE_SEEN) == 2, (
            f"glue ran {len(GLUE_SEEN)} time(s) for 2 distinct files — it must "
            f"run at the PathInput's level, not the function's"
        )

    def test_a_dataset_level_pathinput_is_glued_once(self, db, tmp_path):
        (tmp_path / "config.json").write_text(json.dumps({"factor": 5}))
        cfg = PathInput("config.json", root_folder=str(tmp_path), name="dataset_cfg")
        _run(db, tmp_path, cfg=cfg)

        assert len(FN_SEEN) == 4
        assert len(GLUE_SEEN) == 1

    def test_a_function_mutating_its_input_does_not_leak(self, db, tmp_path):
        """The cached glue output is handed out as a copy: subject 1's two
        sessions both see factor 2, never 102 from the previous combo."""
        _write_subject_cfgs(tmp_path)
        _run(db, tmp_path, fn=mutate_cfg)

        assert sorted(c["factor"] for c in FN_SEEN) == [102, 102, 103, 103]

    def test_the_run_logs_distinct_paths_against_combos(self, db, tmp_path, caplog):
        _write_subject_cfgs(tmp_path)
        with caplog.at_level(logging.INFO):
            _run(db, tmp_path)
        assert any(
            "2 distinct path(s) glued for 4 combo(s)" in r.getMessage()
            for r in caplog.records
        ), "the per-path glue summary was not logged"

    def test_a_glue_error_fails_only_its_own_combos(self, db, tmp_path):
        _write_subject_cfgs(tmp_path)
        _run(db, tmp_path, glue_fn=glue_fails_for_subject_2)

        assert sorted(c["factor"] for c in FN_SEEN) == [2, 2], (
            "subject 1 must still run when subject 2's glue fails"
        )

    def test_the_glue_composes_with_a_variable_input(self, db, tmp_path):
        _write_subject_cfgs(tmp_path)
        for subj in SUBJECTS:
            for sess in SESSIONS:
                RawEMG.save(2.0, db=db, subject=subj, session=sess)
        FN_SEEN.clear()
        GLUE_SEEN.clear()
        results = for_each(
            weigh,
            inputs={"emg": RawEMG, "cfg": _subject_cfg(tmp_path)},
            outputs=[Weighted],
            db=db,
            glue={"cfg": [_spec()]},
            subject=[],
            session=[],
        )
        assert sorted(float(r) for r in results["Weighted"]) == [4.0, 4.0, 6.0, 6.0]
        assert len(GLUE_SEEN) == 2


# ===========================================================================
# Identity: the __pathinput_glue__ hop
# ===========================================================================
class TestIdentity:
    def test_the_consumer_binds_a_virtual_pathinput_glue_record(self, db, tmp_path):
        _write_subject_cfgs(tmp_path)
        _run(db, tmp_path)

        edges = _consumer_cfg_edges(db)
        assert edges, "use_cfg has no cfg edge"
        assert {t for _rid, t in edges} == {prov.PATHINPUT_GLUE_TYPE}

    def test_the_hop_reads_the_pathinput_record_under_its_name(self, db, tmp_path):
        _write_subject_cfgs(tmp_path)
        _run(db, tmp_path)

        glue_invs = glue_invocation_ids(db._duck)
        assert len(glue_invs) == 1, glue_invs
        [edges] = invocation_edges_batch(db._duck, list(glue_invs)).values()
        assert set(edges["path_inputs"]) == {prov.GLUE_INPUT_PARAM}
        assert edges["declared_names"] == {prov.GLUE_INPUT_PARAM: "subject_cfg"}

    def test_history_reads_the_param_as_a_pathinput_with_its_glue(self, db, tmp_path):
        _write_subject_cfgs(tmp_path)
        _run(db, tmp_path)

        inv_ids = {
            r[0]
            for r in db._duck.fetchall(
                "SELECT invocation_id FROM _invocation WHERE function_name = ?",
                ["use_cfg"],
            )
        }
        for edges in invocation_edges_batch(db._duck, list(inv_ids)).values():
            assert "cfg" in edges["path_inputs"], "the glued PathInput vanished"
            assert edges["declared_names"].get("cfg") == "subject_cfg"
            assert edges["pathinput_glue"]["cfg"]["glue_chain"] == ["glue_load_cfg"]
            assert edges["var_inputs"] == [] and edges["constants"] == {}

        [variant] = [
            v for v in pipeline_variants(db._duck) if v["function_name"] == "use_cfg"
        ]
        assert variant["glue_chains"] == {"cfg": ["glue_load_cfg"]}

    def test_a_glued_pathinput_is_not_a_variable_input(self, db, tmp_path):
        from scidb.provenance import query as pq

        _write_subject_cfgs(tmp_path)
        _run(db, tmp_path)
        assert pq.is_inputless_function(db._duck, "use_cfg")
        assert pq.realized_inputless_invocations(db._duck, "use_cfg")

    def test_an_unchanged_rerun_skips(self, db, tmp_path):
        _write_subject_cfgs(tmp_path)
        _run(db, tmp_path)
        assert FN_SEEN, "the first run computed nothing"

        _run(db, tmp_path)
        assert FN_SEEN == [], f"an unchanged re-run recomputed {len(FN_SEEN)} call(s)"

    def test_editing_the_glue_body_recomputes(self, db, tmp_path):
        _write_subject_cfgs(tmp_path)
        _run(db, tmp_path)

        _run(db, tmp_path, glue_fn=glue_load_cfg_doubled)
        assert FN_SEEN, "the edited glue body did not recompute"
        assert sorted(c["factor"] for c in FN_SEEN) == [4, 4, 6, 6]

    def test_record_run_logs_the_hop(self, db, tmp_path, caplog):
        _write_subject_cfgs(tmp_path)
        with caplog.at_level(logging.INFO):
            _run(db, tmp_path)
        assert any(
            "PathInput-fed glue recorded as a history hop" in r.getMessage()
            for r in caplog.records
        )

    def test_node_state_is_green_after_a_run_with_a_variable_input(self, db, tmp_path):
        """The prediction path (_predict_config_invocations) must bind the
        glued PathInput exactly as the save did, or the node goes falsely red."""
        from scidb.provenance.state import check_node_state

        _write_subject_cfgs(tmp_path)
        for subj in SUBJECTS:
            for sess in SESSIONS:
                RawEMG.save(2.0, db=db, subject=subj, session=sess)
        for_each(
            weigh,
            inputs={"emg": RawEMG, "cfg": _subject_cfg(tmp_path)},
            outputs=[Weighted],
            db=db,
            glue={"cfg": [_spec()]},
            subject=[],
            session=[],
        )
        result = check_node_state(weigh, [Weighted], db=db)
        assert result["state"] == "green", result


# ===========================================================================
# Heads, languages and the untracked case
# ===========================================================================
class TestHeads:
    def test_split_chains_puts_every_pathinput_form_on_the_pathinput_head(self):
        from scidb.run.glue import normalize_glue, split_chains

        pi = PathInput("sub{subject}.json", name="cfg")
        inputs = {
            "bare": pi,
            "pinned": Fixed(pi, subject="1"),
            "either": EachOf(pi, PathInput("other{subject}.json", name="other")),
            "const": 3,
            "table": RawEMG,
        }
        chains = normalize_glue({k: _spec() for k in inputs})
        split = split_chains(inputs, chains)
        assert set(split.pathinput) == {"bare", "pinned", "either"}
        assert set(split.constant) == {"const"}
        assert set(split.table) == {"table"}

    def test_matlab_glue_on_a_pathinput_is_refused_in_a_python_run(self, db, tmp_path):
        _write_subject_cfgs(tmp_path)
        with pytest.raises(GlueLanguageMismatchError, match="cannot execute a .m file"):
            for_each(
                use_cfg,
                inputs={"cfg": _subject_cfg(tmp_path)},
                outputs=[Weighted],
                db=db,
                glue={
                    "cfg": GlueSpec(
                        name="glue_m",
                        language="matlab",
                        source_text="function y=f(x)\ny=x;\nend",
                    )
                },
                subject=SUBJECTS,
                session=SESSIONS,
            )

    def test_a_fixed_pathinput_glue_runs_but_warns_identity_is_untracked(
        self, db, tmp_path, caplog
    ):
        """A Fixed(PathInput) has no PathInput record to hop from. The glue
        runs; editing it will not invalidate — said, as for a Merge."""
        _write_subject_cfgs(tmp_path)
        with caplog.at_level(logging.WARNING, logger="scidb"):
            _run(db, tmp_path, cfg=Fixed(_subject_cfg(tmp_path), subject="1"))

        assert sorted(c["factor"] for c in FN_SEEN) == [2, 2, 2, 2]
        assert len(GLUE_SEEN) == 1
        assert any(
            "cannot be recorded in the provenance graph" in r.getMessage()
            for r in caplog.records
        ), "the Fixed(PathInput) identity gap was not reported"
