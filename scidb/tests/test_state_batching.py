"""Node-state checks read locations in ONE query (cleanup-audit F21).

``check_node_state`` used to call ``_schema_id_to_combo`` once per expected
combo — hundreds of queries per node on every canvas refresh, for a detail
the canvas discards.
"""

import numpy as np
import pytest

import scifor as _scifor

from scidb import BaseVariable, configure_database
from scidb.state import _schema_id_to_combo, _schema_ids_to_combos


class Located(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "state_batching.duckdb", ["subject", "session"])
    yield db
    _scifor.set_schema([])
    db.close()


def _schema_ids(db):
    for s in (1, 2):
        for sess in ("A", "B"):
            Located.save(np.arange(2.0), db=db, subject=s, session=sess)
    return [r[0] for r in db._duck._fetchall("SELECT schema_id FROM _schema")]


def test_batched_lookup_matches_the_single_lookup(db):
    sids = _schema_ids(db)
    batched = _schema_ids_to_combos(db, sids)
    assert batched == {sid: _schema_id_to_combo(db, sid) for sid in sids}
    assert {tuple(sorted(c.items())) for c in batched.values() if len(c) == 2}


def test_many_locations_cost_one_query(db, monkeypatch):
    sids = _schema_ids(db)
    calls = []
    real = db._duck._fetchall

    def counting(sql, params=None):
        calls.append(sql)
        return real(sql, params)

    monkeypatch.setattr(db._duck, "_fetchall", counting)
    _schema_ids_to_combos(db, sids * 50)
    assert len(calls) == 1


def test_predicted_locations_pass_through_and_unknown_ids_are_empty(db):
    predicted = (("subject", "9"), ("session", None))
    out = _schema_ids_to_combos(db, [predicted, 987654])
    assert out[predicted] == {"subject": "9"}
    assert out[987654] == {}
