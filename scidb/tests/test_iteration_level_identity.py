"""The iteration level is part of invocation identity.

``.claude/plan-iteration-level-identity.md``; rules in
``docs/claude/iteration-level-identity.md``.

The bug (2026-10-01, ``DemographicsTable``): a PathInput-only loader run once per
subject and then once over the whole dataset wrote the SAME edges, so both runs were
ONE invocation, one variant, one card. The per-subject run had saved the whole table
at every subject; the one-call run saved one row per subject. Nothing could tell
them apart, and locations only the old run wrote stayed "current".

A function with no inputs has the same shape (no variable edges), so it stands in
for the loader here. What is pinned:

1. Two levels are two invocations, two cards, and the older is superseded at every
   location, including one the newer run never wrote.
2. The same level re-run is the same invocation, and skip_computed skips it; a new
   level is NOT skipped.
3. The level is recorded, in the label, NOT in call_id.
4. Currency is per (function, OUTPUT variable): one function writing two variables
   at two levels does not supersede either.
5. A run pin that names no level matches any level; otherwise matching is exact.
6. The node-state predictor rebuilds the recorded ids (a fresh run is complete).
"""

from __future__ import annotations

import numpy as np
import pytest
import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.exceptions import NotFoundError
from scidb.foreach_config import ForEachConfig, _compute_fn_hash
from scidb.provenance import compute_invocation_id
from scidb.provenance_query import (
    expected_invocations_for_function,
    present_invocation_schema_pairs,
    run_options_label,
    run_options_label_matches,
)

SCHEMA = ["subject"]
SUBJECTS = ["01", "02", "03"]


class IlValue(BaseVariable):
    pass


class IlOther(BaseVariable):
    pass


class IlRaw(BaseVariable):
    pass


class IlScaled(BaseVariable):
    pass


CALLS: list = []


def il_value():
    CALLS.append(1)
    return 1.0


def il_scale(raw):
    return float(np.sum(raw)) * 2.0


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    CALLS.clear()
    database = configure_database(tmp_path / "il.duckdb", SCHEMA)
    yield database
    database.close()
    _scifor.set_schema([])


def _invocations(db, fn_name):
    return db._duck._fetchall(
        "SELECT invocation_id, iteration_level FROM _invocation WHERE function_name = ? "
        "ORDER BY invocation_id",
        [fn_name],
    )


def _two_levels(db, subjects=SUBJECTS):
    """Per subject first (the stale run), then one call (the newer run)."""
    for_each(il_value, {}, [IlValue], subject=list(subjects))
    for_each(il_value, {}, [IlValue])


# ---------------------------------------------------------------------------
# 1. Two levels, two variants; the older one is superseded everywhere
# ---------------------------------------------------------------------------


class TestTwoLevels:
    def test_two_invocations_with_their_levels_recorded(self, db):
        _two_levels(db)
        levels = sorted(
            (tuple(level) if level is not None else None)
            for _inv, level in _invocations(db, "il_value")
        )
        assert levels == [(), ("subject",)]

    def test_two_cards_told_apart_by_the_level(self, db):
        _two_levels(db)
        cards = db.inspect.variant_cards("IlValue")
        key = "__run__.il_value"
        assert cards.varying_axes == [key]
        assert sorted(c.distinguishing[key] for c in cards.cards) == [
            "distribute=false, level=(one call)",
            "distribute=false, level=subject",
        ]

    def test_the_older_level_is_superseded_everywhere(self, db):
        _two_levels(db)
        cards = {c.distinguishing["__run__.il_value"]: c for c in db.inspect.variant_cards("IlValue").cards}
        old = cards["distribute=false, level=subject"]
        new = cards["distribute=false, level=(one call)"]
        assert old.verdict == "superseded", old.verdict_label
        assert new.verdict == "current", new.verdict_label

    def test_a_location_only_the_old_run_wrote_is_not_loaded(self, db):
        """The SS02/SS04 shape: the newer run never wrote subject 03, so a
        per-location rule alone would keep the stale record there."""
        _two_levels(db)
        with pytest.raises(NotFoundError):
            IlValue.load(subject="03")

    def test_per_location_record_ids_line_up_with_locations(self, db):
        _two_levels(db)
        old = next(
            c
            for c in db.inspect.variant_cards("IlValue").cards
            if c.distinguishing["__run__.il_value"].endswith("level=subject")
        )
        assert [loc["subject"] for loc in old.locations] == SUBJECTS
        assert all(len(rids) == 1 for rids in old.location_record_ids)
        assert sorted(r for rids in old.location_record_ids for r in rids) == old.record_ids


# ---------------------------------------------------------------------------
# 2. Same level = same invocation (skipped); new level = new work
# ---------------------------------------------------------------------------


class TestSkip:
    def test_same_level_rerun_is_one_invocation_and_is_skipped(self, db):
        for_each(il_value, {}, [IlValue], subject=list(SUBJECTS))
        n_inv = len(_invocations(db, "il_value"))
        CALLS.clear()
        for_each(il_value, {}, [IlValue], subject=list(SUBJECTS), skip_computed=True)
        assert CALLS == [], "an unchanged call at the same level must be skipped"
        assert len(_invocations(db, "il_value")) == n_inv

    def test_the_gate_refuses_a_record_made_at_another_level(self, db):
        """At ONE location, with only the level differing: the stored record
        (made iterating subject) is current for a call at that level and not for
        a call at another level. Calling the hook directly isolates exactly the
        label comparison; an end-to-end run at a different level would usually
        land on other locations and recompute for an unrelated reason."""
        from scidb.foreach import _build_skip_hook

        for_each(il_value, {}, [IlValue], subject=list(SUBJECTS))

        same = _build_skip_hook(il_value, [IlValue], db, {})
        same._iteration_level_ref.update(level=["subject"], set=True)
        assert same({"subject": "01"}) is True

        other = _build_skip_hook(il_value, [IlValue], db, {})
        other._iteration_level_ref.update(level=[], set=True)
        assert other({"subject": "01"}) is False

    def test_an_unset_level_never_skips(self, db):
        """Fail safe: a hook nobody handed the level to recomputes."""
        from scidb.foreach import _build_skip_hook

        for_each(il_value, {}, [IlValue], subject=list(SUBJECTS))
        hook = _build_skip_hook(il_value, [IlValue], db, {})
        assert hook({"subject": "01"}) is False


# ---------------------------------------------------------------------------
# 3. Recorded, labelled, not a call site
# ---------------------------------------------------------------------------


class TestIdentityTerms:
    def test_the_level_changes_the_invocation_id(self):
        base = ("h", None, False, [("x", "r1", None)])
        one = compute_invocation_id(*base, iteration_level=[])
        sub = compute_invocation_id(*base, iteration_level=["subject"])
        unset = compute_invocation_id(*base)
        assert len({one, sub, unset}) == 3

    def test_the_label_spells_the_level(self):
        assert run_options_label(False, None, iteration_level=[]) == (
            "distribute=false, level=(one call)"
        )
        assert run_options_label(True, None, iteration_level=["subject", "trial"]) == (
            "distribute=true, level=subject/trial"
        )
        assert run_options_label(False, None) == "distribute=false"

    def test_the_level_is_a_version_key_but_not_a_call_site_key(self):
        one = ForEachConfig(il_value, {}, level=[])
        sub = ForEachConfig(il_value, {}, level=["subject"])
        assert one.to_call_id() == sub.to_call_id()
        assert one.to_version_keys()["__level"] == []
        assert sub.to_version_keys()["__level"] == ["subject"]


# ---------------------------------------------------------------------------
# 4. Currency is per (function, output variable)
# ---------------------------------------------------------------------------


class TestCurrencyScope:
    def test_one_function_two_variables_two_levels_supersede_nothing(self, db):
        for_each(il_value, {}, [IlValue], subject=list(SUBJECTS))
        for_each(il_value, {}, [IlOther])
        loaded = IlValue.load(subject="01")
        assert float(loaded.data) == 1.0
        cards = db.inspect.variant_cards("IlValue").cards
        assert [c.verdict for c in cards] == ["current"]


# ---------------------------------------------------------------------------
# 5. Run-pin matching
# ---------------------------------------------------------------------------


class TestPinMatching:
    def test_a_pin_without_a_level_matches_any_level(self):
        assert run_options_label_matches("distribute=true", "distribute=true, level=subject")
        assert run_options_label_matches("distribute=true", "distribute=true, level=(one call)")

    def test_otherwise_matching_is_exact(self):
        assert not run_options_label_matches(
            "distribute=false", "distribute=false, as_table=[df], level=subject"
        )
        assert not run_options_label_matches("distribute=true", "distribute=false, level=subject")
        assert run_options_label_matches(
            "distribute=false, as_table=[df], level=subject",
            "distribute=false, as_table=[df], level=subject",
        )

    def test_a_pin_naming_a_level_matches_only_that_level(self):
        assert run_options_label_matches("distribute=false, level=subject", "distribute=false, level=subject")
        assert not run_options_label_matches(
            "distribute=false, level=subject", "distribute=false, level=(one call)"
        )

    def test_nothing_matches_none(self):
        assert not run_options_label_matches("distribute=false", None)


# ---------------------------------------------------------------------------
# 6. The predictor rebuilds what the run recorded
# ---------------------------------------------------------------------------


def test_a_completed_run_is_complete_to_the_predictor(db):
    for subject in SUBJECTS:
        IlRaw.save(np.array([1.0, 2.0]), subject=subject)
    for_each(il_scale, {"raw": IlRaw}, [IlScaled], subject=[])
    expected = expected_invocations_for_function(db, "il_scale", _compute_fn_hash(il_scale))
    inv_ids = [r[0] for r in _invocations(db, "il_scale")]
    present = present_invocation_schema_pairs(db._duck, inv_ids)
    assert expected, "the prediction must not be empty"
    assert not (expected - present), "the predictor must use the recorded level"
