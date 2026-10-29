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
