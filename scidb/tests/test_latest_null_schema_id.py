"""THE supersession ("latest") rule, including a NULL ``schema_id``.

``provenance_query.latest_at_location_batch`` is the one owner (P4 stage 4,
design review 04c Q1). Before it, three copies answered "is this record still
the newest at its location": ``DatabaseManager.get_latest_record_id_for_variant``
(the variant half), ``state._get_latest_record_at_location`` (any variant) and
the batch function, which "mirrored" the two and differed from them on NULL
``schema_id``. What each returned for a NULL-``schema_id`` record, recorded
before the copies were deleted:

* variant half: NULL-safe (``IS NOT DISTINCT FROM``) — the record itself when
  it has save events, None when it has none (a constant record);
* any-variant half: ``TypeError`` (``int(None)``) — it never ran on one in
  practice, because only constant, PathInput and constant-glue records have a
  NULL ``schema_id`` and none of them is a variable input;
* batch: NULL-safe variant half, ``latest_any = None``.

The batch behaviour became the rule; the any-variant half was later dropped
altogether (2026-11-01: a newer record of ANOTHER variant no longer
supersedes; a newer hand save replaces through the rule itself).
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.provenance import query as provenance_query
from scidb.provenance.state import _has_superseded_ancestor


class LatestIn(BaseVariable):
    pass


class LatestOut(BaseVariable):
    pass


def scale(x, k):
    return x * k


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "latest.duckdb", ["subject"])
    yield database
    _scifor.set_schema([])
    database.close()


def _rid(db, type_name, subject):
    rows = db._duck.fetchall(
        "SELECT r.record_id FROM _record r JOIN _schema s ON s.schema_id = r.schema_id "
        "WHERE r.type = ? AND s.subject = ?",
        [type_name, str(subject)],
    )
    assert len(rows) == 1
    return rows[0][0]


def _latest(db, rid):
    return provenance_query.latest_at_location_batch(db._duck, [rid])[rid]


def test_a_record_is_latest_until_resaved(db):
    LatestIn.save(np.arange(3.0), db=db, subject=1)
    first = _rid(db, "LatestIn", 1)
    assert _latest(db, first)["latest_variant"] == first

    LatestIn.save(np.arange(3.0) + 10, db=db, subject=1)
    info = _latest(db, first)
    assert info["latest_variant"] not in (first, None)


def test_staleness_follows_the_owner(db):
    """A downstream record goes stale when its input is re-saved."""
    LatestIn.save(np.arange(3.0), db=db, subject=1)
    for_each(scale, {"x": LatestIn, "k": 2}, [LatestOut], subject=[1], db=db)
    out = _rid(db, "LatestOut", 1)
    assert not _has_superseded_ancestor(db, out, "subject=1")
    assert provenance_query.superseded_batch(db._duck, [out]) == {out: False}

    LatestIn.save(np.arange(3.0) + 1, db=db, subject=1)
    assert _has_superseded_ancestor(db, out, "subject=1")
    assert provenance_query.superseded_batch(db._duck, [out]) == {out: True}


def test_null_schema_id_with_save_events(db):
    """A record moved to a NULL schema_id is still found (NULL-safe)."""
    LatestIn.save(np.arange(3.0), db=db, subject=1)
    rid = _rid(db, "LatestIn", 1)
    db._duck.execute("UPDATE _record SET schema_id = NULL WHERE record_id = ?", [rid])
    info = _latest(db, rid)
    assert info["schema_id"] is None
    assert info["latest_variant"] == rid


def test_null_schema_id_without_save_events(db):
    """A constant record: NULL schema_id and no save event — nothing is
    current there."""
    LatestIn.save(np.arange(3.0), db=db, subject=1)
    for_each(scale, {"x": LatestIn, "k": 3}, [LatestOut], subject=[1], db=db)
    (const_rid,) = [
        r[0]
        for r in db._duck.fetchall("SELECT record_id FROM _record WHERE type = '__constant__'")
    ]
    info = _latest(db, const_rid)
    assert info["schema_id"] is None
    assert info["latest_variant"] is None


def test_a_vanished_record_reads_as_superseded(db):
    info = _latest(db, "no-such-record")
    assert info["latest_variant"] is None and info["type"] is None
