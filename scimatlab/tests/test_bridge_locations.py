"""MATLAB's ``locations`` (JSON text) reaches the bridge's combo filter.

cleanup-audit F6: the bridge could filter combos by a location selection, but
no .m file ever passed one in.
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import json

import pytest
from scidb.store.database import configure_database
from scimatlab import bridge
from scimatlab.bridge import _locations_from_matlab, for_each_prepare, register_matlab_variable


@pytest.fixture
def db(tmp_path):
    db = configure_database(tmp_path / "bridge_locations.duckdb", ["subject", "trial"])
    yield db
    db.close()


def test_json_text_mapping_and_empty():
    sel = {"include": [[["subject", "1"]]], "exclude_levels": {}}
    assert _locations_from_matlab(json.dumps(sel)) == sel
    assert _locations_from_matlab(sel) == sel
    assert _locations_from_matlab("") is None
    assert _locations_from_matlab(None) is None


def test_a_selection_filters_the_combos_handed_to_matlab(db):
    Raw = register_matlab_variable("RawEmg_LOC1")
    register_matlab_variable("Filtered_LOC1")
    for subject in (1, 2):
        db.save_variable(Raw, float(subject), subject=subject, trial=1)

    prep = for_each_prepare(
        "analyze",
        "hash1",
        {"emg": {"kind": "var_type", "type_name": "RawEmg_LOC1"}},
        ["Filtered_LOC1"],
        {},
        db=db,
        schema_keys=["subject", "trial"],
        locations=json.dumps({"include": [[["subject", "1"]]]}),
    )
    try:
        state = bridge._for_each_state_cache[int(prep["handle"])]["state"]
        assert {str(c["subject"]) for c in state.full_combos} == {"1"}
    finally:
        bridge._for_each_state_cache.pop(int(prep["handle"]), None)


def test_a_selection_narrows_the_rows_below_the_iterated_level(db):
    """A MATLAB run at subject level against a selection of trial 2: MATLAB gets
    only those rows, and each call's row selection names only them. MATLAB's
    loop is fed from both, so both must agree."""
    Raw = register_matlab_variable("RawEmg_LOC2")
    register_matlab_variable("Filtered_LOC2")
    for subject in (1, 2):
        for trial in (1, 2, 3):
            db.save_variable(Raw, float(trial), subject=subject, trial=trial)

    prep = for_each_prepare(
        "analyze",
        "hash1",
        {"emg": {"kind": "var_type", "type_name": "RawEmg_LOC2"}},
        ["Filtered_LOC2"],
        {},
        db=db,
        schema_keys=["subject"],
        locations=json.dumps(
            {"include": [[["subject", "1"], ["trial", "2"]], [["subject", "2"], ["trial", "2"]]]}
        ),
    )
    try:
        state = bridge._for_each_state_cache[int(prep["handle"])]["state"]
        assert {str(t) for t in state.loaded_inputs["emg"]["trial"]} == {"2"}
        assert [len(sel["emg"]) for sel in prep["row_selection"]] == [1, 1]
    finally:
        bridge._for_each_state_cache.pop(int(prep["handle"]), None)
