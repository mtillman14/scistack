"""MATLAB's ``parameter_names=`` reaches the prepared state the save reads.

MATLAB expands its Parameters before calling the bridge, so the declared
Parameter names only arrive stated (``+scidb/for_each.m`` 'parameter_names').
They must land on ``_ForEachState.parameter_names``, which ``_save_results``
hands to ``record_run`` (cleanup-audit B1).
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
from scimatlab import bridge
from scimatlab.bridge import (
    _parameter_names_from_matlab,
    for_each_prepare,
    register_matlab_variable,
)

SCHEMA = ["subject", "trial"]


@pytest.fixture
def db(tmp_path):
    db = configure_database(tmp_path / "bridge_param_names.duckdb", SCHEMA)
    yield db
    db.close()


def test_empty_or_missing_is_none():
    assert _parameter_names_from_matlab(None) is None
    assert _parameter_names_from_matlab({}) is None


def test_a_matlab_dict_becomes_the_argument_to_declared_map():
    assert _parameter_names_from_matlab({"gaitRiteConfig": "gaitrite_config"}) == {
        "gaitRiteConfig": "gaitrite_config"
    }


def test_prepare_carries_the_names_to_the_save(db):
    Raw = register_matlab_variable("RawEmg_PN1")
    register_matlab_variable("Filtered_PN1")
    for subject in (1, 2):
        db.save_variable(Raw, float(subject), subject=subject, trial=1)

    prep = for_each_prepare(
        "analyze",
        "hash1",
        {
            "emg": {"kind": "var_type", "type_name": "RawEmg_PN1"},
            "gain": {"kind": "constant", "value": 2},
        },
        ["Filtered_PN1"],
        {},
        db=db,
        schema_keys=["subject", "trial"],
        parameter_names={"gain": "emg_gain"},
    )
    try:
        state = bridge._for_each_state_cache[int(prep["handle"])]["state"]
        assert state.parameter_names == {"gain": "emg_gain"}
    finally:
        bridge._for_each_state_cache.pop(int(prep["handle"]), None)
