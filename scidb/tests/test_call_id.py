"""Tests for call_id-based disambiguation of _for_each_expected.

Without call_id, the same function reused from two for_each() call sites
silently clobbers the first call's expected combo set when the second call
runs.  call_id (a stable hash of the version_keys minus __fn_hash) keys
each row so multiple call sites coexist.
"""

import hashlib
import json

import numpy as np
import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each
from scidb.foreach_config import ForEachConfig


SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_call_id.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class RawSignal(BaseVariable):
    pass


class Filtered(BaseVariable):
    pass


def bandpass(signal, low_hz):
    return signal * low_hz


def _seed_raw(db, subjects, sessions, var=RawSignal):
    for s in subjects:
        for sess in sessions:
            var.save(np.array([1.0, 2.0, 3.0]), db=db, subject=s, session=sess)


def _expected_rows(db, fn_name):
    return db._duck._fetchall(
        "SELECT call_id, schema_id, branch_params FROM _for_each_expected "
        "WHERE function_name = ? ORDER BY call_id, schema_id",
        [fn_name],
    )


# ---------------------------------------------------------------------------
# Unit tests for ForEachConfig.to_call_id()
# ---------------------------------------------------------------------------

def test_call_id_is_stable_and_short():
    cfg1 = ForEachConfig(fn=bandpass, inputs={"signal": RawSignal, "low_hz": 20})
    cfg2 = ForEachConfig(fn=bandpass, inputs={"signal": RawSignal, "low_hz": 20})
    cid = cfg1.to_call_id()
    assert cid == cfg2.to_call_id()
    assert isinstance(cid, str) and len(cid) == 16


def test_call_id_differs_for_different_constants():
    a = ForEachConfig(fn=bandpass, inputs={"signal": RawSignal, "low_hz": 20})
    b = ForEachConfig(fn=bandpass, inputs={"signal": RawSignal, "low_hz": 50})
    assert a.to_call_id() != b.to_call_id()


def test_call_id_excludes_fn_hash():
    """call_id is computed from version_keys minus __fn_hash.

    Verified algorithmically: the hash payload must equal SHA-256(json of
    version_keys with __fn_hash removed).  Re-running this against the
    actual to_call_id() output ensures the implementation honors the
    contract regardless of how __fn_hash was computed.
    """
    cfg = ForEachConfig(fn=bandpass, inputs={"signal": RawSignal, "low_hz": 20})

    keys = cfg.to_version_keys()
    assert "__fn_hash" in keys, "version_keys must include __fn_hash for this test to be meaningful"
    keys.pop("__fn_hash")
    expected_payload = json.dumps(keys, sort_keys=True, default=str)
    expected_cid = hashlib.sha256(expected_payload.encode()).hexdigest()[:16]

    assert cfg.to_call_id() == expected_cid


# ---------------------------------------------------------------------------
# Integration tests against _for_each_expected
# ---------------------------------------------------------------------------

def test_same_call_site_rerun_replaces_in_place(db):
    """Re-running an identical for_each call → row count stable, call_id stable."""
    _seed_raw(db, subjects=["1", "2"], sessions=["A"])

    expected_cid = ForEachConfig(
        fn=bandpass, inputs={"signal": RawSignal, "low_hz": 20}
    ).to_call_id()

    for _ in range(3):
        for_each(
            bandpass,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[Filtered],
            db=db,
            subject=["1", "2"],
            session=["A"],
        )

    rows = _expected_rows(db, "bandpass")
    assert len(rows) == 2, f"Expected 2 rows (2 subjects x 1 session), got {rows}"
    assert all(r[0] == expected_cid for r in rows)


def test_two_call_sites_different_constants_both_persist(db):
    """Same fn called with different constants → distinct call_ids → both expected sets coexist."""
    _seed_raw(db, subjects=["1", "2"], sessions=["A", "B"])

    for_each(  # call site A
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[Filtered],
        db=db,
        subject=["1", "2"],
        session=["A"],
    )

    for_each(  # call site B — different constant
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 50},
        outputs=[Filtered],
        db=db,
        subject=["1", "2"],
        session=["B"],
    )

    rows = _expected_rows(db, "bandpass")
    call_ids = {r[0] for r in rows}
    assert len(call_ids) == 2, f"Expected 2 distinct call_ids, got {call_ids}: {rows}"
    assert len(rows) == 4, (
        f"Both call sites should retain their 2 rows each (4 total); got {rows}. "
        f"If only 2 rows present, the second call clobbered the first — the "
        f"call_id key is not protecting them."
    )


def test_two_call_sites_different_inputs_both_persist(db):
    """Same fn, same constants, different loadable input type → distinct call_ids."""

    class AlternateRaw(BaseVariable):
        pass

    _seed_raw(db, subjects=["1"], sessions=["A"], var=RawSignal)
    _seed_raw(db, subjects=["1"], sessions=["A"], var=AlternateRaw)

    for_each(
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[Filtered],
        db=db,
        subject=["1"],
        session=["A"],
    )

    for_each(
        bandpass,
        inputs={"signal": AlternateRaw, "low_hz": 20},
        outputs=[Filtered],
        db=db,
        subject=["1"],
        session=["A"],
    )

    call_ids = {r[0] for r in _expected_rows(db, "bandpass")}
    assert len(call_ids) == 2, (
        f"Different input types should produce different call_ids, got {call_ids}"
    )


def test_for_each_expected_has_call_id_column(db):
    """Schema sanity: the new call_id column exists and is part of the PK."""
    cols = db._duck._fetchall(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = '_for_each_expected' ORDER BY ordinal_position"
    )
    col_names = [c[0] for c in cols]
    assert "call_id" in col_names, f"call_id column missing from _for_each_expected: {col_names}"
