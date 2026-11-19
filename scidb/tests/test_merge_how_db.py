"""Merge how= against a real database (docs/claude/merge-node.md).

Demographics exists for S01 only, so S02 is a location on one side only:
* exact (default): S02 is skipped, as before.
* left: S02 is called with Age NaN-padded, and its output names only the
  TrialSignal record it read (no Demographics record exists there).
* how is identity: an exact and a left run are two variants at S01.
"""

import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, Merge, configure_database, for_each
from scidb.provenance import query as pq

SCHEMA = ["subject", "trial"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_merge_how.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class Demographics(BaseVariable):
    pass


class TrialSignal(BaseVariable):
    pass


class HowOut(BaseVariable):
    pass


SEEN: list = []


def count_rows(merged):
    SEEN.append(merged.reset_index(drop=True))
    return float(len(merged))


def _seed():
    Demographics.save(pd.DataFrame({"Age": [60]}), subject="S01")
    for subj in ("S01", "S02"):
        for trial in ("1", "2"):
            TrialSignal.save(float(trial), subject=subj, trial=trial)


def _run(how):
    SEEN.clear()
    for_each(
        count_rows,
        {"merged": Merge(TrialSignal, Demographics, name="sig_demo", how=how)},
        [HowOut],
        subject=["S01", "S02"],
        trial=["1", "2"],
    )
    return list(SEEN)


def test_exact_skips_the_one_sided_location(db):
    _seed()
    assert len(_run("exact")) == 2  # S01's two trials only


def test_left_pads_the_one_sided_location(db):
    _seed()
    calls = _run("left")
    assert len(calls) == 4
    padded = [c for c in calls if c["Age"].isna().all()]
    assert len(padded) == 2, "S02's two trials: Age NaN-padded"


def test_left_lineage_names_only_records_that_exist(db):
    _seed()
    _run("left")
    out = HowOut.load(subject="S02", trial="1")
    inv = pq.producing_invocation(db._duck, out.record_id)[0]
    edges = pq.invocation_edges_batch(db._duck, [inv])[inv]
    merged = {e["record_id"] for e in edges["var_inputs"] if e["param_name"] == "merged"}
    assert merged == {TrialSignal.load(subject="S02", trial="1").record_id}


def test_how_is_identity(db):
    _seed()
    _run("exact")
    _run("left")
    versions = HowOut.list_versions(db=db, subject="S01", trial="1")
    assert len(versions) == 2, "exact and left are two computations"


def test_to_csv_honours_how(db, tmp_path):
    _seed()
    path = tmp_path / "merged.csv"
    Merge(TrialSignal, Demographics, name="sig_demo", how="left").to_csv(str(path))
    df = pd.read_csv(path)
    assert len(df) == 4
    assert df["Age"].isna().sum() == 2
