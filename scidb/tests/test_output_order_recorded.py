"""Each run records its ``outputs=`` order (Stage 1 of
.claude/plan-output-order-owner.md).

Output order is the order of ``outputs=``: a function's return order, or a
for_columns split's group pairing. ``_invocation_output.output_num`` is a
slot, not that position, so the run records the order itself
(``_run_outputs``) and ``provenance_query.recorded_output_orders`` reads it.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.provenance_query import recorded_output_orders

SCHEMA = ["subject"]


class OrdIn(BaseVariable):
    pass


class OrdA(BaseVariable):
    pass


class OrdB(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "output_order.duckdb", SCHEMA)
    for s in (1, 2):
        OrdIn.save(np.arange(3.0) + s, db=db, subject=s)
    yield db
    _scifor.set_schema([])
    db.close()


def mean_and_max(x):
    return float(np.mean(x)), float(np.max(x))


def just_mean(x):
    return float(np.mean(x))


def _invocation_ids(db):
    return {r[0] for r in db._duck._fetchall("SELECT invocation_id FROM _invocation")}


def test_a_run_records_its_outputs_order(db):
    for_each(mean_and_max, inputs={"x": OrdIn}, outputs=[OrdB, OrdA], subject=[1, 2], db=db)
    assert recorded_output_orders(db._duck, ["mean_and_max"]) == {
        "mean_and_max": [["OrdB", "OrdA"]]
    }


def test_a_rerun_in_another_order_is_newest_first_and_not_identity(db):
    for_each(mean_and_max, inputs={"x": OrdIn}, outputs=[OrdB, OrdA], subject=[1, 2], db=db)
    before = _invocation_ids(db)
    for_each(mean_and_max, inputs={"x": OrdIn}, outputs=[OrdA, OrdB], subject=[1, 2], db=db)
    assert recorded_output_orders(db._duck, ["mean_and_max"]) == {
        "mean_and_max": [["OrdA", "OrdB"], ["OrdB", "OrdA"]]
    }
    # Output order is descriptive: the calls keep their identities.
    assert _invocation_ids(db) == before


def test_single_output_and_unknown_functions(db):
    for_each(just_mean, inputs={"x": OrdIn}, outputs=[OrdA], subject=[1, 2], db=db)
    orders = recorded_output_orders(db._duck, ["just_mean", "never_ran"])
    assert orders == {"just_mean": [["OrdA"]]}
    assert recorded_output_orders(db._duck, []) == {}


def test_the_table_is_part_of_the_run_log():
    from scidb.bundle import HISTORY_TABLES
    from scidb.clear_data import KEPT_BY_CLEAR

    assert "_run_outputs" in HISTORY_TABLES
    assert "_run_outputs" in KEPT_BY_CLEAR


# --- MATLAB output NAMES (_run_outputs.output_params) --------------------------


def matlab_like(x):
    return float(np.mean(x)), float(np.max(x))


def split_like(x):
    return float(np.mean(x)), float(np.max(x))


def test_output_names_map_by_name_across_a_reordered_signature(db):
    """A MATLAB run records its signature's output names beside outputs=.
    After the signature is reordered, the names still map to the variables
    they saved to (test 6 of plan-output-order-owner)."""
    from scidb.provenance_query import recorded_output_params

    matlab_like.output_params = ["time", "force"]
    try:
        for_each(matlab_like, inputs={"x": OrdIn}, outputs=[OrdA, OrdB], subject=[1, 2], db=db)
        # The .m is edited to [force, time]; the next run lists outputs to match.
        matlab_like.output_params = ["force", "time"]
        for_each(matlab_like, inputs={"x": OrdIn}, outputs=[OrdB, OrdA], subject=[1, 2], db=db)
    finally:
        del matlab_like.output_params
    assert recorded_output_params(db._duck, ["matlab_like"]) == {
        "matlab_like": {"force": "OrdB", "time": "OrdA"}
    }


def test_python_runs_record_no_names_and_a_split_maps_none(db):
    from scidb.provenance_query import recorded_output_params

    for_each(mean_and_max, inputs={"x": OrdIn}, outputs=[OrdB, OrdA], subject=[1, 2], db=db)
    assert recorded_output_params(db._duck, ["mean_and_max"]) == {}
    # One signature output, two variables (a for_columns split): no mapping.
    split_like.output_params = ["result"]
    try:
        for_each(split_like, inputs={"x": OrdIn}, outputs=[OrdA, OrdB], subject=[1], db=db)
    finally:
        del split_like.output_params
    assert "split_like" not in recorded_output_params(db._duck, ["split_like"])


def test_a_gui_run_records_its_declared_python_port_names(db):
    """A canvas run of a Python node with declared output ports names its
    outputs through scidb.intent.run_output_names (plan-python-output-ports)."""
    from scidb.intent import run_output_names
    from scidb.provenance_query import recorded_output_params

    with run_output_names(["peak", "mean"]):
        for_each(mean_and_max, inputs={"x": OrdIn}, outputs=[OrdB, OrdA], subject=[1, 2], db=db)
    assert recorded_output_params(db._duck, ["mean_and_max"]) == {
        "mean_and_max": {"peak": "OrdB", "mean": "OrdA"}
    }
