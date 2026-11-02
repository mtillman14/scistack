"""``for_each_prepare`` must tell MATLAB which inputs are struct records.

A MATLAB struct is saved as a dict, stored one DuckDB column per field
(sciduckdb ``multi_column``), and loaded in the spread layout. By the time the
columns cross the bridge as a MATLAB table there is nothing left to distinguish
one struct record from a one-row table — so ``+scifor/for_each.m`` handed the
user function a 1xN table and every field access returned a 1x1 cell.

``mapping_inputs`` is the fact MATLAB cannot recompute. Runs entirely in
Python without MATLAB — same pattern as test_bridge_schema_keys.py.
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import numpy as np
import pandas as pd
import pytest
from scidb.store.database import configure_database
from scimatlab.bridge import for_each_prepare, register_matlab_variable

SCHEMA = ["subject", "trial"]


def _var_spec(type_name):
    return {"kind": "var_type", "type_name": type_name}


@pytest.fixture
def db(tmp_path):
    db = configure_database(tmp_path / "bridge_mapping_inputs.duckdb", SCHEMA)
    yield db
    db.close()


class TestBridgeMappingInputs:
    def test_dict_valued_input_is_reported_with_its_field_names(self, db):
        RawEmg = register_matlab_variable("RawEmg_MI1")
        register_matlab_variable("Filtered_MI1")
        for subject in (1, 2):
            db.save_variable(
                RawEmg,
                {"RHAM": np.array([1.0, 2.0]), "RVL": np.array([3.0])},
                subject=subject,
                trial=1,
            )

        prep = for_each_prepare(
            "fn", "hash1", {"emg": _var_spec("RawEmg_MI1")}, ["Filtered_MI1"], {},
            db=db, schema_keys=["subject", "trial"],
        )

        assert prep["mapping_inputs"] == {"emg": ["RHAM", "RVL"]}

    def test_dataframe_valued_input_is_not_reported(self, db):
        """Only dict records are rebuilt; a table-valued variable's columns
        are the table's own and must stay a table."""
        Tbl = register_matlab_variable("Tbl_MI2")
        register_matlab_variable("Filtered_MI2")
        db.save_variable(
            Tbl, pd.DataFrame({"a": [1.0], "b": [2.0]}), subject=1, trial=1
        )

        prep = for_each_prepare(
            "fn", "hash2", {"tbl": _var_spec("Tbl_MI2")}, ["Filtered_MI2"], {},
            db=db, schema_keys=["subject", "trial"],
        )

        assert prep["mapping_inputs"] == {}

    def test_array_valued_input_is_not_reported(self, db):
        Sig = register_matlab_variable("Sig_MI3")
        register_matlab_variable("Filtered_MI3")
        db.save_variable(Sig, np.array([1.0, 2.0]), subject=1, trial=1)

        prep = for_each_prepare(
            "fn", "hash3", {"sig": _var_spec("Sig_MI3")}, ["Filtered_MI3"], {},
            db=db, schema_keys=["subject", "trial"],
        )

        assert prep["mapping_inputs"] == {}

    def test_key_is_always_present_so_matlab_never_branches_on_absence(self, db):
        Sig = register_matlab_variable("Sig_MI4")
        register_matlab_variable("Filtered_MI4")
        db.save_variable(Sig, np.array([1.0]), subject=1, trial=1)

        prep = for_each_prepare(
            "fn", "hash4", {"sig": _var_spec("Sig_MI4")}, ["Filtered_MI4"], {},
            db=db, schema_keys=["subject", "trial"],
        )

        assert "mapping_inputs" in prep
        assert isinstance(prep["mapping_inputs"], dict)
