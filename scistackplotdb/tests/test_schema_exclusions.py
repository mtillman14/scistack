"""
A schema exclusion (``scidb.exclude_schema``) keeps its records out of a
plot — and re-including them brings them back, cache and all.

Found by the example integration suite (2026-09-19): ``for_each`` already
skipped an excluded trial, but the plot loader read the table straight and
drew it beside the others. "Excluded from every analysis" includes plots.
"""

from __future__ import annotations

import scidb
from scistackplotdb import ScidbSource

from conftest import SESSIONS, SUBJECTS, TRIALS


def test_an_excluded_trial_is_left_out_of_the_table(seeded):
    source = ScidbSource(seeded)
    full = source.get_table(["StepLength"])
    assert len(full.frame) == len(SUBJECTS) * len(SESSIONS) * len(TRIALS)

    scidb.exclude_schema("equipment fault", subject="02", session="pre", trial="1")
    table = source.get_table(["StepLength"])
    assert len(table.frame) == len(full.frame) - 1
    gone = table.frame[
        (table.frame["subject"].astype(str) == "02")
        & (table.frame["session"].astype(str) == "pre")
        & (table.frame["trial"].astype(str) == "1")
    ]
    assert gone.empty


def test_a_wildcard_exclusion_drops_the_whole_subtree(seeded):
    scidb.exclude_schema("withdrew", subject="03")
    table = ScidbSource(seeded).get_table(["StepLength"])
    assert "03" not in set(table.frame["subject"].astype(str))
    assert len(table.frame) == (len(SUBJECTS) - 1) * len(SESSIONS) * len(TRIALS)


def test_a_coarse_record_is_dropped_only_by_an_exclusion_at_its_own_level(seeded):
    """A subject-level Mass is not touched by excluding one of that
    subject's trials — the record has no trial to match on."""
    scidb.exclude_schema("bad trial", subject="01", session="pre", trial="1")
    table = ScidbSource(seeded).get_table(["Mass"])
    assert set(table.frame["subject"].astype(str)) == set(SUBJECTS)


def test_re_including_invalidates_the_cached_frame(seeded):
    source = ScidbSource(seeded)
    n = len(source.get_table(["StepLength"]).frame)
    scidb.exclude_schema("fault", subject="01", session="post", trial="2")
    assert len(source.get_table(["StepLength"]).frame) == n - 1
    scidb.include_schema("re-reviewed", subject="01", session="post", trial="2")
    assert len(source.get_table(["StepLength"]).frame) == n, (
        "the cached frame must drop when the exclusion registry changes"
    )


def test_a_record_written_behind_the_sources_back_reaches_the_next_get_table(seeded):
    """The self-validating cache's promise, through the PUBLIC entry point:
    a terminal run writes a record this process was never told about, and
    the same source's next ``get_table`` must include it — not only after a
    by-hand ``_variable_frame`` call, which is what the cache tests do."""
    from conftest import StepLength

    source = ScidbSource(seeded)
    before = len(source.get_table(["StepLength"]).frame)
    StepLength.save(9.99, subject="01", session="pre", trial="9")
    after = len(source.get_table(["StepLength"]).frame)
    assert after == before + 1, "a memo hit served a frame the database has moved past"
