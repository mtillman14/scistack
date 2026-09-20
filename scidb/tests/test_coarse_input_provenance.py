"""Regression: a variable stored at a COARSER level than the dataset schema must
still record its consumed-input provenance edges when used as a for_each input.

Bug: ``_for_each_prepare`` built ``rid_per_combo`` (and the ColumnSelection
coverage set) by ``df.groupby(schema_cols_in_df)`` where ``schema_cols_in_df``
included EVERY schema key present as a column. A variable stored at a coarser
level (e.g. subject/session in a subject/session/cycle schema) carries the finer
keys as all-NaN columns. pandas ``groupby`` drops NaN-key groups by default, so
EVERY row was dropped → empty ``rid_per_combo`` → empty ``combo_to_rids`` → no
``__upstream`` → NO ``_invocation_input`` edges. That severed input provenance
for essentially every aggregation/distribute step whose input is coarser than the
full schema, and was the precondition for the re-run orphan/duplicate cascade
(outputs couldn't be tied to the input version they consumed).

Fix: group only by the schema keys the input actually populates (drop all-NaN
columns); the mapping key is still built over the full lookup-key set.
"""

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject", "session", "cycle"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_coarse_prov.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class CoarseInput(BaseVariable):
    """Stored at subject/session — the finest key ('cycle') is left unpopulated."""

    pass


class AggOut(BaseVariable):
    pass


def _sum(signal):
    if isinstance(signal, pd.DataFrame):
        return float(signal.select_dtypes(include="number").values.sum())
    if isinstance(signal, np.ndarray):
        return float(signal.sum())
    return float(signal)


def _schema_id_of(db, record_id):
    return db._duck._fetchall(
        "SELECT schema_id FROM _record WHERE record_id = ?", [record_id]
    )[0][0]


def test_coarse_input_records_input_edges_in_aggregation(db):
    from scidb import provenance_query

    # CoarseInput at subject/session (no 'cycle') → its loaded spread carries an
    # all-NaN 'cycle' column.
    CoarseInput.save(2.0, subject="S01", session="1")
    CoarseInput.save(3.0, subject="S02", session="1")
    c1 = CoarseInput.load(subject="S01", session="1")
    c1_sid = _schema_id_of(db, c1.record_id)

    # Aggregation mode: iterate subject+session; 'cycle' is the aggregated axis.
    for_each(
        _sum, {"signal": CoarseInput}, [AggOut], subject=["S01", "S02"], session=["1"]
    )

    out = AggOut.load(subject="S01", session="1")
    consumed = provenance_query.consumed_input_schema_ids(db._duck, [out.record_id])

    assert consumed.get(out.record_id) == frozenset({c1_sid}), (
        f"AggOut must record CoarseInput@{c1_sid} as a consumed input; got "
        f"{consumed.get(out.record_id)} — empty/missing means the input edge was "
        f"not recorded (all-NaN finer-key groupby-drop regression)."
    )


def test_coarse_input_has_lineage(db):
    """The aggregation output must be recognized as computed (has lineage), not
    as a raw/orphaned record — the user-visible symptom of the dropped edges."""
    CoarseInput.save(5.0, subject="S01", session="1")
    for_each(_sum, {"signal": CoarseInput}, [AggOut], subject=["S01"], session=["1"])
    out = AggOut.load(subject="S01", session="1")
    assert db.has_lineage(out.record_id) is True


class Demo(BaseVariable):
    """Stored at subject level only — COARSER than the iterated level below."""


class Fine(BaseVariable):
    """Stored at subject/session/cycle — pooled per session below."""


def _both(signal, demo):
    return _sum(signal) + _sum(demo)


def test_input_coarser_than_the_iterated_level_records_its_edge(db):
    """Iterate subject+session, pooling cycles; a subject-level input serves
    every session beneath it. The aggregation pool is keyed by the iterated
    keys the input POPULATES (`RecordPool.location_keys`) — keyed by all of
    them, the subject-level record matched no session and the output was
    saved with no edge to it."""
    from scidb import provenance_query

    Demo.save(10.0, subject="S01")
    for cycle in ("1", "2"):
        Fine.save(1.0, subject="S01", session="1", cycle=cycle)
    demo = Demo.load(subject="S01")
    demo_sid = _schema_id_of(db, demo.record_id)

    for_each(_both, {"signal": Fine, "demo": Demo}, [AggOut], subject=["S01"], session=["1"])

    out = AggOut.load(subject="S01", session="1")
    consumed = provenance_query.consumed_input_schema_ids(db._duck, [out.record_id])
    assert demo_sid in consumed.get(out.record_id, frozenset()), (
        f"AggOut must record Demo@{demo_sid} as a consumed input; got "
        f"{consumed.get(out.record_id)}"
    )
    params = {
        p
        for (p,) in db._duck._fetchall(
            "SELECT DISTINCT ii.param_name FROM _invocation_input ii "
            "JOIN _invocation inv ON inv.invocation_id = ii.invocation_id "
            "WHERE inv.function_name = ?",
            ["_both"],
        )
    }
    assert params == {"signal", "demo"}, params
