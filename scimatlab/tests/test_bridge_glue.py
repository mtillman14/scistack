"""``for_each_prepare`` must carry glue chains across the bridge.

A glue node executes in the language of the run, so on the MATLAB path the
bodies run in ``+scidb/for_each.m`` — a ``.m`` function cannot execute inside
Python's prepare step. What prepare owns is the **identity** half: it hashes
each glue's source text, writes the virtual glue records the consumer binds
to, and reports back which nodes MATLAB must apply to which params.

This file covers the Python half only. The runtime half (MATLAB actually
applying the bodies and enforcing the row contract) needs a real MATLAB run by
the user — the standing gap, same as ``+scidb/Parameter.m``.

Runs entirely in Python without MATLAB, same pattern as
test_bridge_mapping_inputs.py.
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import pytest
from scidb.database import configure_database
from scidb.provenance import GLUE_TYPE
from scimatlab.bridge import (
    _reconstruct_glue_chains,
    for_each_prepare,
    register_matlab_variable,
)

SCHEMA = ["subject", "trial"]

GLUE_SRC = "function out = glue_drop_baseline(emg)\n    out = removevars(emg, 'baseline');\nend\n"
GLUE_SRC_EDITED = (
    "function out = glue_drop_baseline(emg)\n"
    "    out = removevars(emg, 'baseline');\n"
    "    out.signal = out.signal + 1;\n"
    "end\n"
)


def _var_spec(type_name):
    return {"kind": "var_type", "type_name": type_name}


def _chain(source_text=GLUE_SRC, name="glue_drop_baseline", per_schema_key=False):
    return {
        "emg": [
            {
                "name": name,
                "source_text": source_text,
                "per_schema_key": per_schema_key,
            }
        ]
    }


@pytest.fixture
def db(tmp_path):
    db = configure_database(tmp_path / "bridge_glue.duckdb", SCHEMA)
    yield db
    db.close()


def _seed(db, type_name):
    Raw = register_matlab_variable(type_name)
    for subject in (1, 2):
        db.save_variable(Raw, float(subject), subject=subject, trial=1)
    return Raw


class TestReconstruction:
    def test_structs_become_matlab_glue_specs(self):
        chains = _reconstruct_glue_chains(_chain())
        (spec,) = chains["emg"]
        assert spec.name == "glue_drop_baseline"
        assert spec.language == "matlab"
        assert spec.fn is None  # MATLAB runs the body, not Python

    def test_hash_is_derived_from_the_source_text(self):
        a = _reconstruct_glue_chains(_chain())["emg"][0]
        b = _reconstruct_glue_chains(_chain(GLUE_SRC_EDITED))["emg"][0]
        assert a.hash != b.hash

    def test_none_and_empty_are_no_glue(self):
        assert _reconstruct_glue_chains(None) == {}
        assert _reconstruct_glue_chains({}) == {}


class TestPreparePassesChainsBack:
    def test_glue_chains_are_reported_to_matlab(self, db):
        _seed(db, "RawEmg_G1")
        register_matlab_variable("Filtered_G1")

        prep = for_each_prepare(
            "analyze",
            "hash1",
            {"emg": _var_spec("RawEmg_G1")},
            ["Filtered_G1"],
            {},
            db=db,
            schema_keys=["subject", "trial"],
            glue=_chain(),
        )

        assert prep["glue_chains"] == {
            "emg": [{"name": "glue_drop_baseline", "per_schema_key": False}]
        }

    def test_no_glue_reports_an_empty_mapping(self, db):
        _seed(db, "RawEmg_G2")
        register_matlab_variable("Filtered_G2")

        prep = for_each_prepare(
            "analyze",
            "hash1",
            {"emg": _var_spec("RawEmg_G2")},
            ["Filtered_G2"],
            {},
            db=db,
            schema_keys=["subject", "trial"],
        )

        assert prep["glue_chains"] == {}

    def test_the_bodies_are_not_applied_python_side(self, db):
        # The MATLAB source text is not executable here; if prepare tried to
        # run it, this call would raise rather than return a prepared state.
        _seed(db, "RawEmg_G3")
        register_matlab_variable("Filtered_G3")

        prep = for_each_prepare(
            "analyze",
            "hash1",
            {"emg": _var_spec("RawEmg_G3")},
            ["Filtered_G3"],
            {},
            db=db,
            schema_keys=["subject", "trial"],
            glue=_chain(),
        )

        assert prep["full_combos"], "prepare produced no combos"


class TestIdentityIsStillPythons:
    def test_virtual_glue_records_route_the_consumers_bindings(self, db):
        _seed(db, "RawEmg_G4")
        register_matlab_variable("Filtered_G4")

        prep = for_each_prepare(
            "analyze",
            "hash1",
            {"emg": _var_spec("RawEmg_G4")},
            ["Filtered_G4"],
            {},
            db=db,
            schema_keys=["subject", "trial"],
            glue=_chain(),
        )

        # The selections MATLAB will apply already name the VIRTUAL rids —
        # that is what makes an edited MATLAB glue invalidate the consumer,
        # exactly as on the Python path.
        rid_values = {
            rid
            for sel in prep["row_selection"]
            for rids in sel.values()
            for rid in rids
        }
        real_rids = {
            r[0]
            for r in db._duck._fetchall(
                "SELECT record_id FROM _record WHERE type = ?", ["RawEmg_G4"]
            )
        }
        assert rid_values, "no rid discriminators in the combos"
        assert not (rid_values & real_rids), (
            "MATLAB combos still bind the raw records — an edited glue would "
            "not invalidate anything"
        )

    def test_editing_a_matlab_glue_moves_the_bindings(self, db):
        _seed(db, "RawEmg_G5")
        register_matlab_variable("Filtered_G5")

        def _rids(glue):
            prep = for_each_prepare(
                "analyze",
                "hash1",
                {"emg": _var_spec("RawEmg_G5")},
                ["Filtered_G5"],
                {},
                db=db,
                schema_keys=["subject", "trial"],
                glue=glue,
            )
            return {
                rid
                for sel in prep["row_selection"]
                for rids in sel.values()
                for rid in rids
            }

        assert _rids(_chain()) != _rids(_chain(GLUE_SRC_EDITED))

    def test_glue_type_is_the_shared_sentinel(self):
        # The MATLAB path must not invent its own record type.
        assert GLUE_TYPE == "__glue__"
