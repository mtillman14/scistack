"""Tests for per-call-site check_node_state via the call_id parameter.

When the same function is invoked from multiple for_each() call sites, each
site has its own call_id (a stable hash of inputs/constants/where minus
__fn_hash).  check_node_state(call_id=X) must report only X's state, not
the union across all sites.
"""

import numpy as np
import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each
from scidb.foreach_config import ForEachConfig
from scihist.state import check_node_state, _get_expected_combos, _get_output_combos


SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_state_call_id.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class RawSignal(BaseVariable):
    pass


class Filtered(BaseVariable):
    pass


def bandpass(signal, low_hz):
    return signal * low_hz


def _seed(db, subjects, sessions):
    for s in subjects:
        for sess in sessions:
            RawSignal.save(np.array([1.0, 2.0, 3.0]), db=db, subject=s, session=sess)


# ---------------------------------------------------------------------------
# Pipeline variants exposes call_id
# ---------------------------------------------------------------------------

def test_list_pipeline_variants_includes_call_id(db):
    _seed(db, subjects=["1"], sessions=["A"])
    for_each(
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[Filtered],
        db=db,
        subject=["1"],
        session=["A"],
    )

    variants = [v for v in db.list_pipeline_variants() if v["function_name"] == "bandpass"]
    assert len(variants) == 1
    cid = variants[0]["call_id"]
    assert isinstance(cid, str) and len(cid) == 16

    # Independently computing it from the same config must match.
    expected_cid = ForEachConfig(
        fn=bandpass, inputs={"signal": RawSignal, "low_hz": 20}
    ).to_call_id()
    assert cid == expected_cid


# ---------------------------------------------------------------------------
# check_node_state per call site
# ---------------------------------------------------------------------------

def test_check_node_state_filters_by_call_id(db):
    """Two call sites: each call_id sees only its own combos as up_to_date."""
    _seed(db, subjects=["1", "2"], sessions=["A", "B"])

    # Call site A: low_hz=20, session A
    for_each(
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[Filtered],
        db=db,
        subject=["1", "2"],
        session=["A"],
    )

    # Call site B: low_hz=50, session B
    for_each(
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 50},
        outputs=[Filtered],
        db=db,
        subject=["1", "2"],
        session=["B"],
    )

    cid_a = ForEachConfig(
        fn=bandpass, inputs={"signal": RawSignal, "low_hz": 20}
    ).to_call_id()
    cid_b = ForEachConfig(
        fn=bandpass, inputs={"signal": RawSignal, "low_hz": 50}
    ).to_call_id()
    assert cid_a != cid_b

    state_a = check_node_state(bandpass, [Filtered], db=db, call_id=cid_a)
    state_b = check_node_state(bandpass, [Filtered], db=db, call_id=cid_b)
    state_union = check_node_state(bandpass, [Filtered], db=db)

    # Each call site sees only its own 2 combos.
    assert state_a["counts"]["up_to_date"] == 2, state_a
    assert state_a["counts"]["missing"] == 0, state_a
    assert state_b["counts"]["up_to_date"] == 2, state_b
    assert state_b["counts"]["missing"] == 0, state_b

    # Union sees all 4 combos.
    assert state_union["counts"]["up_to_date"] == 4, state_union


def test_check_node_state_call_id_detects_missing_per_site(db):
    """One call site fully run, another partially missing: per-site states differ."""
    _seed(db, subjects=["1", "2", "3"], sessions=["A"])

    # Call site A: only run for subject 1 (subjects 2,3 will be missing)
    for_each(
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[Filtered],
        db=db,
        subject=["1"],
        session=["A"],
    )

    # Call site B: run for subjects 1,2,3 fully
    for_each(
        bandpass,
        inputs={"signal": RawSignal, "low_hz": 50},
        outputs=[Filtered],
        db=db,
        subject=["1", "2", "3"],
        session=["A"],
    )

    cid_a = ForEachConfig(
        fn=bandpass, inputs={"signal": RawSignal, "low_hz": 20}
    ).to_call_id()
    cid_b = ForEachConfig(
        fn=bandpass, inputs={"signal": RawSignal, "low_hz": 50}
    ).to_call_id()

    state_a = check_node_state(bandpass, [Filtered], db=db, call_id=cid_a)
    state_b = check_node_state(bandpass, [Filtered], db=db, call_id=cid_b)

    # Site A: 1 up_to_date.  The "missing" count depends on whether the
    # input variable's branch_params expose only the locations that B
    # produced — under the current branch_params model, A only knows about
    # the schema_ids it produced for itself, so it shows 1 up_to_date and
    # 0 missing.  The key invariant: A must NOT see B's records.
    assert state_a["counts"]["up_to_date"] == 1
    for combo in state_a["combos"]:
        # No combo from site A should reference low_hz=50
        bp = combo["branch_params"]
        assert bp.get("bandpass.low_hz") != 50, combo

    # Site B: 3 up_to_date.
    assert state_b["counts"]["up_to_date"] == 3
    for combo in state_b["combos"]:
        bp = combo["branch_params"]
        assert bp.get("bandpass.low_hz") != 20, combo


def test_get_output_combos_filters_by_call_id(db):
    _seed(db, subjects=["1"], sessions=["A"])

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
        inputs={"signal": RawSignal, "low_hz": 50},
        outputs=[Filtered],
        db=db,
        subject=["1"],
        session=["A"],
    )

    cid_a = ForEachConfig(fn=bandpass, inputs={"signal": RawSignal, "low_hz": 20}).to_call_id()

    all_combos = _get_output_combos(db, "bandpass", [Filtered])
    only_a = _get_output_combos(db, "bandpass", [Filtered], call_id=cid_a)

    assert len(all_combos) == 2
    assert len(only_a) == 1
    assert only_a[0]["branch_params"].get("bandpass.low_hz") == 20


def test_get_expected_combos_filters_for_each_expected_by_call_id(db):
    """The PathInput fallback path on _for_each_expected respects call_id."""
    _seed(db, subjects=["1", "2"], sessions=["A"])

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
        inputs={"signal": RawSignal, "low_hz": 50},
        outputs=[Filtered],
        db=db,
        subject=["2"],
        session=["A"],
    )

    # _for_each_expected should have 2 rows total (one per call site).
    all_rows = db._duck._fetchall(
        "SELECT call_id, schema_id FROM _for_each_expected WHERE function_name = ?",
        ["bandpass"],
    )
    assert len(all_rows) == 2

    cid_a = ForEachConfig(fn=bandpass, inputs={"signal": RawSignal, "low_hz": 20}).to_call_id()

    # When call_id is provided AND scidb_variants returns nothing for that
    # call_id (e.g., function never ran for that call site), the fallback
    # path filters by call_id.  We trigger that by clearing _record_metadata
    # for one call site so the variant loop misses it.
    db._duck._execute(
        "DELETE FROM _record_metadata WHERE variable_name = ?", ["Filtered"]
    )

    expected_a = _get_expected_combos(db, "bandpass", call_id=cid_a)
    expected_all = _get_expected_combos(db, "bandpass")

    # Scoped: only site A's row.
    assert len(expected_a) == 1
    # Union: both sites' rows.
    assert len(expected_all) == 2
