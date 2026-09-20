"""
sciduckdb + scidb: what the pipeline stored comes back as it went in.

Zero-padded keys and NULL-filled coarse records are the two things a storage
change can silently damage, and neither shows in a unit test that saves and
loads a scalar.
"""

from __future__ import annotations

import pytest

from conftest import CYCLES, JOINTS, TRIALS, cell, columns_of


def test_zero_padded_keys_stay_strings(pipeline, example_db):
    """``"01"`` must not come back as 1 — a numeric key would sort 1, 10, 2
    on a plot axis and never match its own file name again."""
    frame = pipeline.CycleSymmetry.load(as_df=True)
    assert set(frame["cycle"].astype(str)) == set(CYCLES)
    assert all(isinstance(value, str) for value in frame["cycle"].head(20))
    assert set(frame["trial"].astype(str)) == set(TRIALS)


def test_distinct_values_come_back_in_a_usable_order(pipeline, example_db):
    cycles = [str(v) for v in example_db.distinct_schema_values("cycle")]
    assert set(cycles) == set(CYCLES)
    # Every value is present exactly once — no int/str doubles like 1 and "01".
    assert len(cycles) == len(CYCLES)


def test_a_table_record_keeps_its_columns(pipeline, example_db):
    one = pipeline.CycleSymmetry.load(as_df=True).iloc[0]["data"]
    assert set(columns_of(one)) == set(JOINTS)
    for joint in JOINTS:
        assert 0.0 <= float(cell(one, joint)) <= 200.0


def test_a_1d_record_round_trips_as_a_sequence(pipeline, example_db):
    """Each joint of a waveform record is ONE array-valued cell, 51 long —
    not 51 rows, and not a string."""
    one = pipeline.CycleWaveform.load(as_df=True).iloc[0]["data"]
    assert set(columns_of(one)) == set(JOINTS)
    curve = cell(one, "knee")
    assert hasattr(curve, "__len__") and not isinstance(curve, str)
    assert len(curve) == 51


def test_a_categorical_subject_column_survives(pipeline, example_db):
    frame = pipeline.Demographics.load(as_df=True)
    groups = {str(cell(data, "group")) for data in frame["data"]}
    assert groups <= {"control", "treatment"}
    assert len(groups) >= 1


def test_coarse_records_are_findable_by_their_own_keys_only(pipeline, example_db):
    """Loading a session-level variable by subject + session finds exactly
    one record; the finer keys are not needed and not present."""
    subject = pipeline.Demographics.load(as_df=True)["subject"].iloc[0]
    frame = pipeline.SessionInfo.load(as_df=True, subject=subject, session="baseline")
    assert len(frame) == 1
    with pytest.raises(Exception):
        # Nothing was ever saved at subject level under a cycle.
        pipeline.Demographics.load(subject=subject, cycle="01")
