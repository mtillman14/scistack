"""``scistackstats.exclusions``: the one reading and wording of what a test
left out (Stage A; user rule 2026-10-19: never drop data silently)."""

from __future__ import annotations

from scistackstats.exclusions import from_record


def test_nothing_excluded_is_none():
    assert from_record(None, 6, "subjects") is None
    assert from_record({"total": 12, "excluded_detail": []}, 6, "subjects") is None


def test_every_subject_is_named_not_truncated():
    detail = [{"subject": f"S{i:02d}", "reason": "missing a within-subject level"} for i in range(12)]
    found = from_record({"subjects": 3, "excluded_detail": detail}, 15, "subjects")
    assert found.n_text() == "3 of 15"
    sentence = found.sentence()
    assert sentence.startswith("12 of 15 subjects excluded:")
    assert all(f"S{i:02d}" in sentence for i in range(12))  # csv-stats' note stops at 10


def test_row_drops_are_counted():
    found = from_record({"total": 18, "excluded_detail": [{"rows": 2, "reason": "missing value"}]}, 20, "rows")
    assert found.n_text() == "18 of 20"
    assert found.sentence() == "2 row(s) excluded (missing value)"


def test_used_is_derived_when_the_record_lacks_it():
    found = from_record({"excluded_detail": [{"subject": "S1", "reason": "r"}]}, 5, "subjects")
    assert found.used == 4
