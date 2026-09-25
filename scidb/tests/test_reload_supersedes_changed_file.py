"""
Re-loading a batch after ONE file changed yields one new version, not a
second "latest" record.

Found by the example integration suite (2026-09-19, plan D3). A PathInput
loader shares one invocation across runs (``provenance_save`` Fix B), and a
re-run's record takes the next free ``output_num`` — so with ``output_num``
in the latest-collapse key the changed location had two "latest" records
(#34 from the first run, #60 from the second). ``output_num`` is no longer
part of that key: two records of one family at one location can only be a
re-save, and the newest wins.
"""

from __future__ import annotations

import pytest
import scifor as _scifor

from scidb import BaseVariable, PathInput, configure_database, for_each

SUBJECTS = ["01", "02", "03"]
TRIALS = ["1", "2"]


class Reading(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "reload.duckdb", ["subject", "trial"])
    yield db
    _scifor.set_schema([])
    db.close()


@pytest.fixture
def files(tmp_path):
    root = tmp_path / "data"
    for subject in SUBJECTS:
        for trial in TRIALS:
            path = root / subject / f"{subject}_{trial}.txt"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"{subject}-{trial}\n")
    return root


def read_file(filepath):
    with open(filepath) as handle:
        return handle.read().strip()


def _load(root):
    for_each(
        read_file,
        {"filepath": PathInput("{subject}/{subject}_{trial}.txt", root_folder=str(root), name="{subject}/{subject}_{trial}.txt")},
        [Reading],
        subject=[],
        trial=[],
    )


def test_one_changed_file_is_one_new_version(db, files):
    _load(files)
    n = len(SUBJECTS) * len(TRIALS)
    assert len(Reading.load(as_df=True)) == n

    (files / "02" / "02_1.txt").write_text("02-1 EDITED\n")
    _load(files)

    every = Reading.load(as_df=True, version="all")
    assert len(every) == n + 1, "exactly one record gained a version"

    latest = Reading.load(as_df=True)
    assert len(latest) == n, "still one latest record per location"
    changed = Reading.load(as_df=True, subject="02", trial="1")
    assert len(changed) == 1
    assert changed["data"].iloc[0] == "02-1 EDITED"


def test_an_unchanged_reload_adds_nothing(db, files):
    _load(files)
    _load(files)
    n = len(SUBJECTS) * len(TRIALS)
    assert len(Reading.load(as_df=True, version="all")) == n
    assert len(Reading.load(as_df=True)) == n


def test_the_edited_record_is_read_by_a_downstream_step(db, files):
    """The point of supersession: a step reading the variable sees ONE input
    per location — the newest — never the old and new together."""

    class Length(BaseVariable):
        pass

    def length(text):
        return float(len(text))

    _load(files)
    (files / "03" / "03_2.txt").write_text("03-2 with more text\n")
    _load(files)

    for_each(length, {"text": Reading}, [Length], subject=[], trial=[])
    lengths = Length.load(as_df=True)
    assert len(lengths) == len(SUBJECTS) * len(TRIALS)
    # `trial` is "1"/"2" here — every value round-trips, so the key loads as
    # ints (schema_key_is_numeric); compare as text rather than assume either.
    row = lengths[(lengths["subject"].astype(str) == "03") & (lengths["trial"].astype(str) == "2")]
    assert len(row) == 1, lengths[["subject", "trial"]].to_string()
    got = row["data"].iloc[0]
    assert float(got) == float(len("03-2 with more text"))


def test_a_for_columns_call_records_its_selector(db):
    """`for_columns()` ran with no column list, so it had no selector at all
    and looked, in history, like a whole-table input. The selector now says
    `iterate`, so a run derived from history is per-column too."""
    import pandas as pd

    from scidb import for_each

    class Wide(BaseVariable):
        pass

    class Doubled(BaseVariable):
        pass

    Wide.save(pd.DataFrame({"a": [1.0], "b": [2.0]}), subject="01", trial="1")

    def double(value):
        return float(pd.Series(value).iloc[0]) * 2

    for_each(double, {"value": Wide.for_columns()}, [Doubled], subject=[], trial=[])
    variants = [v for v in db.list_pipeline_variants() if v["function_name"] == "double"]
    assert variants and variants[0]["selectors"].get("value", {}).get("iterate") is True
