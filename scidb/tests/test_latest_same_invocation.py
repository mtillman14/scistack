"""`is_latest`: of the records ONE invocation wrote to a location, the newest
save wins (`provenance_query._supersede_same_invocation`, 2026-09-29).

One invocation = one call (same code, options, inputs, constants). Two
different records from it at one location mean something the identity cannot
see changed between runs — the iteration level (scidb.log run n1irqety: a
placeholder-free CSV read per subject saved the whole table at every subject),
or the file behind a PathInput (identity is its NAME only). Before this, both
records shared one chain signature, both were latest, and Plot Studio "took
the first".
"""

from __future__ import annotations

import pytest
import scifor as _scifor
from scifor import PathInput

from scidb import BaseVariable, configure_database, for_each
from scidb import provenance_query
from scidb.provenance_query import _supersede_same_invocation, variant_identity_batch


class Demo(BaseVariable):
    pass


class Scaled(BaseVariable):
    pass


class Seed(BaseVariable):
    pass


def read_table(path):
    import pandas as pd

    return pd.read_csv(path)


def scale(x, k):
    return float(k)


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "latest.duckdb", ["subject", "session"])
    yield db
    _scifor.set_schema([])
    db.close()


def _latest_by_subject(db, type_name):
    """{subject: {record_id: is_latest}} over every non-excluded record."""
    rows = db._duck._fetchall(
        "SELECT r.record_id, s.subject FROM _record r "
        "LEFT JOIN _schema s ON s.schema_id = r.schema_id "
        "WHERE r.type = ? AND COALESCE(r.excluded, FALSE) = FALSE",
        [type_name],
    )
    ident = variant_identity_batch(db._duck, [rid for rid, _ in rows])
    out: dict = {}
    for rid, subject in rows:
        out.setdefault(subject, {})[rid] = ident[rid]["is_latest"]
    return out


def _csv(tmp_path, body):
    path = tmp_path / "demographics.csv"
    path.write_text(body)
    return PathInput(str(path), name="DemographicsPath")


def test_a_one_call_rerun_supersedes_whole_tables_saved_per_subject(db, tmp_path):
    """The n1irqety shape: per-subject run of a placeholder-free CSV (whole
    table at every subject), then the one-call run (one row per subject)."""
    pi = _csv(tmp_path, "subject,group\nS1,a\nS2,b\n")
    for_each(read_table, {"path": pi}, [Demo], subject=["S1", "S2"])
    first = _latest_by_subject(db, "Demo")
    assert all(len(v) == 1 for v in first.values())

    for_each(read_table, {"path": pi}, [Demo])

    latest = _latest_by_subject(db, "Demo")
    for subject in ("S1", "S2"):
        assert len(latest[subject]) == 2, "old whole table + new row coexist"
        assert sorted(latest[subject].values()) == [False, True]
        (old_rid,) = first[subject]
        assert latest[subject][old_rid] is False, "the older save is superseded"


def test_an_edited_file_rerun_wins(db, tmp_path):
    pi = _csv(tmp_path, "subject,group\nS1,a\nS2,b\nS3,c\n")
    for_each(read_table, {"path": pi}, [Demo])
    before = _latest_by_subject(db, "Demo")

    _csv(tmp_path, "subject,group\nS1,EDITED\nS2,b\n")  # S1 edited, S3 removed
    for_each(read_table, {"path": pi}, [Demo])
    after = _latest_by_subject(db, "Demo")

    (s1_old,) = before["S1"]
    assert after["S1"][s1_old] is False
    assert sum(after["S1"].values()) == 1
    # S2 unchanged: exactly one latest record whether the re-save reused the
    # record id or wrote a new one.
    assert sum(after["S2"].values()) == 1
    # Known gap, per location by design: nothing newer was written at S3, so
    # its record stays latest. Hide it by hand.
    assert list(after["S3"].values()) == [True]


def test_constant_variants_are_untouched(db):
    """Different constants are different invocations: side by side, chosen
    by branch params, never superseding each other."""
    Seed.save(1.0, db=db, subject="S1")
    for_each(scale, {"x": Seed, "k": 2}, [Scaled], subject=["S1"])
    for_each(scale, {"x": Seed, "k": 3}, [Scaled], subject=["S1"])
    latest = _latest_by_subject(db, "Scaled")
    assert len(latest["S1"]) == 2
    assert all(latest["S1"].values())


def test_same_instant_saves_stay_latest_together(monkeypatch):
    """One save batch is not a later answer: equal timestamps keep both."""
    monkeypatch.setattr(
        provenance_query,
        "producing_invocation_batch",
        lambda duck, rids: {rid: ("inv1", "f", "h") for rid in rids},
    )
    peers = {("Demo", "sid"): ["a", "b", "c"]}
    is_latest = {"a": True, "b": True, "c": True}
    saved = {"a": "2026-09-29T10:00:00", "b": "2026-09-29T10:00:00", "c": "2026-09-28T09:00:00"}
    assert _supersede_same_invocation(None, peers, is_latest, saved) == 1
    assert is_latest == {"a": True, "b": True, "c": False}


def test_an_already_stale_record_is_not_counted(monkeypatch):
    monkeypatch.setattr(
        provenance_query,
        "producing_invocation_batch",
        lambda duck, rids: {rid: ("inv1", "f", "h") for rid in rids},
    )
    peers = {("Demo", "sid"): ["a", "b"]}
    is_latest = {"a": True, "b": False}
    saved = {"a": "2026-09-28", "b": "2026-09-29"}
    assert _supersede_same_invocation(None, peers, is_latest, saved) == 0
    assert is_latest == {"a": True, "b": False}


@pytest.mark.xfail(
    strict=False,
    reason="input-file fingerprints not built: a PathInput is identified by its "
    "name only, so the skip gate should not see an edited file "
    "(.claude/plan-input-file-fingerprints.md). XPASS here means the gate "
    "re-ran for another reason — read the [skip-gate] DEBUG lines.",
)
def test_skip_computed_reruns_after_the_file_is_edited(db, tmp_path):
    pi = _csv(tmp_path, "subject,group\nS1,a\n")
    for_each(read_table, {"path": pi}, [Demo])
    _csv(tmp_path, "subject,group\nS1,EDITED\n")
    for_each(read_table, {"path": pi}, [Demo], skip_computed=True)
    latest = _latest_by_subject(db, "Demo")
    assert len(latest["S1"]) == 2
