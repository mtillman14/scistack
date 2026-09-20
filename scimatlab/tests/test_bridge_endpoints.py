"""Tests for endpoint (plot_/stat_) support on the MATLAB-driven bridge.

Runs entirely in Python without MATLAB: the "MATLAB loop" is simulated by
iterating prepare's full_combos and building the result DataFrames MATLAB's
+scifor/for_each.m would produce, then calling for_each_save.

Covers the D7 contracts that pytest can reach:
- _endpoint_policy parity between the Python path and the bridge
- normalize_stat_result byte-equality with the Python stat_ wrapper
- path_output / across_variants spec reconstruction
- resolved_path_outputs alignment with full_combos (variant placeholders)
- collision-guard propagation through prepare
- draft (finalized=False) save suppression through for_each_save
- __vsig_* combo-key sanitization at the MATLAB boundary
"""

import json as _json
import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import numpy as np
import pandas as pd
import pytest
from scidb.database import configure_database
from scidb.foreach import (
    _endpoint_policy,
    normalize_stat_payload,
)
from scidb.foreach import (
    for_each as scidb_for_each,
)
from scimatlab.bridge import (
    _reconstruct_input_for_keys,
    for_each_prepare,
    for_each_save,
    normalize_stat_result,
    register_matlab_variable,
)


@pytest.fixture
def db(tmp_path):
    import scifor as _scifor

    _scifor.set_schema([])
    db = configure_database(tmp_path / "endpoints.duckdb", ["subject", "session"])
    yield db
    _scifor.set_schema([])
    db.close()


def bandpass(signal, low_hz):
    return signal * low_hz


def _var_spec(type_name):
    return {"kind": "var_type", "type_name": type_name}


def _po_spec(template):
    return {"kind": "path_output", "template": template}


# ---------------------------------------------------------------------------
# 1. _endpoint_policy parity (the single source of truth both paths call)
# ---------------------------------------------------------------------------


class TestEndpointPolicy:
    def test_kinds_and_path_param(self):
        from scifor import PathOutput

        inputs = {"df": object(), "filename": PathOutput("r_{subject}.pdf")}

        kind, pp, at, sup = _endpoint_policy("stat_x", inputs, False, None)
        assert (kind, pp, at, sup) == ("stat", "filename", True, True)

        kind, pp, at, sup = _endpoint_policy("stat_x", inputs, True, False)
        assert (kind, pp, at, sup) == ("stat", "filename", False, False)

        kind, pp, at, sup = _endpoint_policy("plot_x", inputs, True, None)
        assert (kind, pp, at, sup) == ("plot", "filename", None, False)

        kind, pp, at, sup = _endpoint_policy("process", inputs, False, None)
        assert (kind, pp, at, sup) == (None, None, None, False)

    def test_plot_requires_pathoutput(self):
        with pytest.raises(ValueError, match="requires a"):
            _endpoint_policy("plot_x", {"signal": object()}, True, None)

    def test_finalized_on_non_endpoint_warns(self):
        import warnings as _w

        with _w.catch_warnings(record=True) as caught:
            _w.simplefilter("always")
            _endpoint_policy("process", {}, True, None)
        assert any("only applies to endpoint" in str(w.message) for w in caught)


# ---------------------------------------------------------------------------
# 2. normalize_stat_result: byte-identity across languages
# ---------------------------------------------------------------------------


class TestNormalizeStatResult:
    def test_matches_python_wrapper_output(self):
        result = {
            "p_value": 0.04,
            "n": np.int64(10),
            "date": "2026-07-07 09:00:00",
            "nested": {"alpha": np.float64(0.05)},
        }
        py_payload = normalize_stat_payload(dict(result), "/tmp/r.pdf", True)
        # MATLAB path: jsonencode-style string with DIFFERENT key order and
        # already-native types — must canonicalize to the same bytes.
        matlab_json = _json.dumps(
            {
                "nested": {"alpha": 0.05},
                "n": 10,
                "date": "2026-07-07 09:00:00",
                "p_value": 0.04,
            }
        )
        bridge_payload = normalize_stat_result(matlab_json, "/tmp/r.pdf", True)
        assert bridge_payload == py_payload
        parsed = _json.loads(bridge_payload)
        assert "date" not in parsed
        assert parsed["report_path"] == "/tmp/r.pdf"

    def test_draft_has_no_report_path(self):
        payload = normalize_stat_result('{"p": 0.5}', "", False)
        assert _json.loads(payload) == {"p": 0.5}

    def test_invalid_json_raises(self):
        with pytest.raises(TypeError, match="not valid JSON"):
            normalize_stat_result("not json", "", True)


# ---------------------------------------------------------------------------
# 3. Spec reconstruction
# ---------------------------------------------------------------------------


class TestSpecReconstruction:
    def test_path_output_kind(self):
        from scifor import PathOutput

        po = _reconstruct_input_for_keys(_po_spec("plots/{subject}_{low_hz}.png"))
        assert isinstance(po, PathOutput)
        assert str(po.template) == "plots/{subject}_{low_hz}.png"

    def test_across_variants_kind(self, db):
        from scidb.across_variants import AcrossVariants

        register_matlab_variable("Filtered_EP")
        av = _reconstruct_input_for_keys(
            {"kind": "across_variants", "inner": _var_spec("Filtered_EP")}
        )
        assert isinstance(av, AcrossVariants)


# ---------------------------------------------------------------------------
# 4. Prepare: resolved paths per variant group + guard + sanitization
# ---------------------------------------------------------------------------


class TestPrepareEndpoints:
    def _seed_two_groups(self, db):
        RawSignal = register_matlab_variable("RawSignal_EP")
        Filtered = register_matlab_variable("Filtered_EP")
        register_matlab_variable("StatOut_EP")  # output types must be
        register_matlab_variable("PlotOut_EP")  # registered for prepare
        for subj in ["S01"]:
            for sess in ["1", "2"]:
                db.save_variable(RawSignal, 1.0, subject=subj, session=sess)
        for low_hz in [20, 30]:
            scidb_for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01"],
                session=["1", "2"],
                db=db,
            )
        return RawSignal, Filtered

    def test_resolved_paths_aligned_and_distinct_per_group(self, db, tmp_path):
        self._seed_two_groups(db)
        prep = for_each_prepare(
            "stat_summary",
            "hash0",
            {
                "df": _var_spec("Filtered_EP"),
                "filename": _po_spec(str(tmp_path / "r_{low_hz}.pdf")),
            },
            ["StatOut_EP"],
            {"subject": ["S01"]},
            db=db,
            finalized=True,
        )
        combos = list(prep["full_combos"])
        paths = list(prep["resolved_path_outputs"]["filename"])
        assert len(paths) == len(combos) == 2
        assert sorted(p.rsplit("/", 1)[-1] for p in paths) == ["r_20.pdf", "r_30.pdf"]
        # Injected placeholder keys must NOT cross as combo keys. What does
        # cross is the combo's handle into `row_selection`, SANITIZED (MATLAB
        # fields can't start with _), and the selection itself names the
        # records each group reads — the per-input `x__vsig_*` discriminator
        # columns are gone (2026-09-20).
        selection = list(prep["row_selection"])
        assert len(selection) == len(combos)
        for i, combo in enumerate(combos):
            assert "low_hz" not in combo
            assert not any(str(k).startswith("x__vsig_") for k in combo)
            assert combo[prep["combo_key"]] == i
            assert selection[i]["df"], "each group's selection names its records"
        assert {tuple(s["df"]) for s in selection} and len(
            {tuple(s["df"]) for s in selection}
        ) == 2, "two variant groups, two different record sets"
        assert prep["record_id_column"] == "x__record_id"
        assert prep["endpoint_kind"] == "stat"
        assert prep["path_param"] == "filename"
        assert prep["as_table_effective"] is True
        # Free the cache.
        for_each_save(prep["handle"], [], save=False)

    def test_collision_guard_propagates_through_prepare(self, db, tmp_path):
        self._seed_two_groups(db)
        with pytest.raises(ValueError, match="low_hz"):
            for_each_prepare(
                "stat_summary",
                "hash0",
                {
                    "df": _var_spec("Filtered_EP"),
                    "filename": _po_spec(str(tmp_path / "r_{subject}.pdf")),
                },
                ["StatOut_EP"],
                {"subject": ["S01"]},
                db=db,
                finalized=True,
            )

    def test_plot_requires_pathoutput_through_prepare(self, db):
        register_matlab_variable("RawSignal_EP")
        register_matlab_variable("PlotOut_EP")
        with pytest.raises(ValueError, match="requires a"):
            for_each_prepare(
                "plot_sig",
                "hash0",
                {"signal": _var_spec("RawSignal_EP")},
                ["PlotOut_EP"],
                {"subject": ["S01"], "session": ["1"]},
                db=db,
                finalized=True,
            )


# ---------------------------------------------------------------------------
# 5. Draft suppression through the save phase (simulated MATLAB loop)
# ---------------------------------------------------------------------------


class TestDraftThroughBridge:
    def _run_stat_cycle(self, db, finalized):
        RawSignal = register_matlab_variable("RawSignal_EP2")
        StatOut = register_matlab_variable("StatOut_EP2")
        db.save_variable(RawSignal, 1.0, subject="S01", session="1")
        db.save_variable(RawSignal, 2.0, subject="S01", session="2")

        prep = for_each_prepare(
            "stat_summary",
            "hash0",
            {"df": _var_spec("RawSignal_EP2")},
            ["StatOut_EP2"],
            {"subject": ["S01"]},
            db=db,
            finalized=finalized,
        )
        combos = list(prep["full_combos"])
        assert len(combos) == 1

        # Simulate the MATLAB loop + stat wrapper: normalize like
        # stat_endpoint_call does, then build the result table MATLAB
        # would hand back (metadata cols incl. sanitized vsig + output col).
        payload = normalize_stat_result('{"n": 2}', "", finalized)
        row = dict(combos[0].items())
        row["StatOut_EP2"] = payload
        result_df = pd.DataFrame([row])
        result = for_each_save(prep["handle"], [result_df], save=True)
        return db, StatOut, result

    def test_draft_suppresses_save(self, db):
        db, StatOut, result = self._run_stat_cycle(db, finalized=False)
        assert result is not None and len(result) == 1
        assert db.list_versions(StatOut) == []

    def test_finalized_saves(self, db):
        db, StatOut, result = self._run_stat_cycle(db, finalized=True)
        assert len(db.list_versions(StatOut)) == 1
        rec = next(db.load(StatOut, {"subject": "S01"}, version_id="latest"))
        assert _json.loads(rec.data) == {"n": 2}
