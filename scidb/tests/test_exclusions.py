"""Tests for schema-based exclusions (exclude_schema / include_schema / list_exclusions)."""

import sys
from pathlib import Path

import pytest

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "scifor" / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))

from scidb.database import _local
from scidb.exclusions import (
    _TABLE,
    _current_status,
    exclusion_status,
    filter_excluded_combos,
    get_schema_overrides_hash,
    governing_overrides,
)

from scidb import (
    BaseVariable,
    configure_database,
    exclude_schema,
    for_each,
    include_schema,
    list_exclusions,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_db_state():
    if hasattr(_local, "database"):
        delattr(_local, "database")
    yield
    if hasattr(_local, "database"):
        delattr(_local, "database")


@pytest.fixture
def db(tmp_path):
    d = configure_database(tmp_path / "test.duckdb", ["subject", "trial"])
    yield d
    d.close()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _count_rows(db, **filters) -> int:
    """Count rows in __scidb_schema_overrides matching optional filters."""
    where_parts = []
    params = []
    for k, v in filters.items():
        where_parts.append(f'"{k}" = ?')
        params.append(str(v))
    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    rows = db._duck._fetchall(
        f"SELECT COUNT(*) FROM {_TABLE} {where_clause}", params or None
    )
    return rows[0][0]


# ===========================================================================
# Basic exclude / include
# ===========================================================================


class TestExcludeSchema:
    def test_basic_exclude(self, db):
        exclude_schema(subject=1, trial=2, reason="bad trial", db=db)
        assert _count_rows(db) == 1

    def test_exclude_writes_status_false(self, db):
        exclude_schema(subject=1, trial=2, reason="bad", db=db)
        status = _current_status(db, {"subject": 1, "trial": 2})
        assert status is False

    def test_exclude_no_keys_raises(self, db):
        with pytest.raises(ValueError, match="At least one schema key"):
            exclude_schema(reason="bad", db=db)

    def test_exclude_unknown_key_raises(self, db):
        with pytest.raises(ValueError, match="Unknown schema key"):
            exclude_schema(subject=1, session=99, reason="bad", db=db)

    def test_exclude_already_excluded_raises(self, db):
        exclude_schema(subject=1, trial=2, reason="first", db=db)
        with pytest.raises(ValueError, match="already excluded"):
            exclude_schema(subject=1, trial=2, reason="second", db=db)

    def test_exclude_partial_keys_wildcard(self, db):
        # Exclude entire subject (trial is NULL = wildcard)
        exclude_schema(subject=3, reason="whole subject excluded", db=db)
        status = _current_status(db, {"subject": 3})
        assert status is False

    def test_exclude_uses_global_db(self, db):
        exclude_schema(subject=1, trial=2, reason="bad")
        assert _count_rows(db) == 1


class TestIncludeSchema:
    def test_include_after_exclude(self, db):
        exclude_schema(subject=1, trial=2, reason="bad", db=db)
        include_schema(subject=1, trial=2, reason="re-reviewed", db=db)
        status = _current_status(db, {"subject": 1, "trial": 2})
        assert status is True

    def test_include_without_prior_exclude_raises(self, db):
        with pytest.raises(ValueError, match="no exclusion record"):
            include_schema(subject=1, trial=2, reason="oops", db=db)

    def test_include_already_included_raises(self, db):
        exclude_schema(subject=1, trial=2, reason="bad", db=db)
        include_schema(subject=1, trial=2, reason="ok now", db=db)
        with pytest.raises(ValueError, match="already included"):
            include_schema(subject=1, trial=2, reason="again", db=db)

    def test_include_preserves_history(self, db):
        """The original exclusion row must remain (audit trail)."""
        exclude_schema(subject=1, trial=2, reason="bad", db=db)
        include_schema(subject=1, trial=2, reason="ok", db=db)
        assert _count_rows(db) == 2

    def test_include_no_keys_raises(self, db):
        with pytest.raises(ValueError, match="At least one schema key"):
            include_schema(reason="oops", db=db)


# ===========================================================================
# list_exclusions
# ===========================================================================


class TestListExclusions:
    def test_returns_empty_when_none(self, db):
        df = list_exclusions(db=db)
        assert len(df) == 0

    def test_returns_excluded_rows(self, db):
        exclude_schema(subject=1, trial=2, reason="bad", db=db)
        exclude_schema(subject=3, reason="whole subject", db=db)
        df = list_exclusions(db=db)
        assert len(df) == 2

    def test_does_not_return_re_included(self, db):
        exclude_schema(subject=1, trial=2, reason="bad", db=db)
        include_schema(subject=1, trial=2, reason="ok", db=db)
        df = list_exclusions(db=db)
        assert len(df) == 0

    def test_shows_only_current_excluded(self, db):
        exclude_schema(subject=1, trial=1, reason="bad1", db=db)
        exclude_schema(subject=1, trial=2, reason="bad2", db=db)
        include_schema(subject=1, trial=1, reason="ok", db=db)
        df = list_exclusions(db=db)
        assert len(df) == 1
        assert str(df.iloc[0]["trial"]) == "2"


# ===========================================================================
# No-op guard — exact-keyset semantics
# ===========================================================================


class TestNoOpGuard:
    def test_specific_can_be_added_even_if_wildcard_already_excluded(self, db):
        # Wildcard row: subject=3, trial=NULL
        exclude_schema(subject=3, reason="whole subject excluded", db=db)
        # Specific row: subject=3, trial=2 (different keyset — allowed)
        exclude_schema(subject=3, trial=2, reason="trial 2 also explicitly bad", db=db)
        assert _count_rows(db) == 2

    def test_most_specific_wins_over_wildcard(self, db):
        exclude_schema(subject=3, reason="whole subject excluded", db=db)
        # Re-include just trial 2 at the more specific level
        # First need to add the specific keyset before including it
        exclude_schema(subject=3, trial=2, reason="also excluded", db=db)
        include_schema(subject=3, trial=2, reason="trial 2 is actually fine", db=db)
        # Subject=3 wildcard is still excluded
        assert _current_status(db, {"subject": 3}) is False
        # But trial 2 specifically is now included (most specific wins)
        assert _current_status(db, {"subject": 3, "trial": 2}) is True


# ===========================================================================
# filter_excluded_combos
# ===========================================================================


class TestFilterExcludedCombos:
    def test_no_overrides_returns_all(self, db):
        combos = [{"subject": "1", "trial": "1"}, {"subject": "1", "trial": "2"}]
        result = filter_excluded_combos(combos, ["subject", "trial"], db)
        assert result == combos

    def test_exact_match_excluded(self, db):
        exclude_schema(subject=1, trial=2, reason="bad", db=db)
        combos = [{"subject": "1", "trial": "1"}, {"subject": "1", "trial": "2"}]
        result = filter_excluded_combos(combos, ["subject", "trial"], db)
        assert len(result) == 1
        assert result[0]["trial"] == "1"

    def test_wildcard_excludes_all_trials(self, db):
        exclude_schema(subject=3, reason="whole subject", db=db)
        combos = [
            {"subject": "3", "trial": "1"},
            {"subject": "3", "trial": "2"},
            {"subject": "1", "trial": "1"},
        ]
        result = filter_excluded_combos(combos, ["subject", "trial"], db)
        assert len(result) == 1
        assert result[0]["subject"] == "1"

    def test_specific_include_overrides_wildcard_exclude(self, db):
        exclude_schema(subject=3, reason="whole subject", db=db)
        exclude_schema(subject=3, trial=2, reason="also excluded", db=db)
        include_schema(subject=3, trial=2, reason="trial 2 ok after all", db=db)
        combos = [
            {"subject": "3", "trial": "1"},
            {"subject": "3", "trial": "2"},
        ]
        result = filter_excluded_combos(combos, ["subject", "trial"], db)
        assert len(result) == 1
        assert result[0]["trial"] == "2"

    def test_re_included_combo_survives(self, db):
        exclude_schema(subject=1, trial=2, reason="bad", db=db)
        include_schema(subject=1, trial=2, reason="ok", db=db)
        combos = [{"subject": "1", "trial": "2"}]
        result = filter_excluded_combos(combos, ["subject", "trial"], db)
        assert len(result) == 1


# ===========================================================================
# get_schema_overrides_hash
# ===========================================================================


class TestSchemaOverridesHash:
    def test_empty_table_returns_stable_hash(self, db):
        h1 = get_schema_overrides_hash(db)
        h2 = get_schema_overrides_hash(db)
        assert h1 == h2

    def test_hash_changes_on_exclude(self, db):
        h_before = get_schema_overrides_hash(db)
        exclude_schema(subject=1, trial=2, reason="bad", db=db)
        h_after = get_schema_overrides_hash(db)
        assert h_before != h_after

    def test_hash_changes_on_include(self, db):
        exclude_schema(subject=1, trial=2, reason="bad", db=db)
        h_before = get_schema_overrides_hash(db)
        include_schema(subject=1, trial=2, reason="ok", db=db)
        h_after = get_schema_overrides_hash(db)
        assert h_before != h_after


# ===========================================================================
# Backends without a DuckDB layer (no _duck) — exclusions degrade gracefully
# ===========================================================================


class _NoDuckDB:
    """Database double that doesn't expose a DuckDB backend.

    Mirrors backends (remote/net) and the lightweight test doubles used by
    scihist's for_each tests, which implement the public Database surface but
    have no ``_duck``.  Regression guard for the AttributeError that crashed
    for_each Step 9.5 when it reached straight into ``db._duck``.
    """

    dataset_schema_keys = ["subject", "trial"]


class TestExclusionsWithoutDuckBackend:
    def test_overrides_hash_returns_empty_payload_hash(self):
        import hashlib
        import json

        # Same value an empty overrides table would produce, not a crash.
        empty_payload_hash = hashlib.sha256(
            json.dumps([], sort_keys=True).encode()
        ).hexdigest()[:16]
        assert get_schema_overrides_hash(_NoDuckDB()) == empty_payload_hash

    def test_overrides_hash_is_stable_without_backend(self):
        assert get_schema_overrides_hash(_NoDuckDB()) == get_schema_overrides_hash(
            _NoDuckDB()
        )

    def test_filter_returns_combos_unchanged_without_backend(self):
        combos = [{"subject": "1", "trial": "1"}, {"subject": "1", "trial": "2"}]
        result = filter_excluded_combos(combos, ["subject", "trial"], _NoDuckDB())
        assert result == combos


# ===========================================================================
# Integration with for_each
# ===========================================================================


class ScalarOutput(BaseVariable):
    schema_version = 1


class TestForEachIntegration:
    """for_each must skip excluded combos and encode override hash in version_keys."""

    def _setup(self, db):
        ScalarOutput.save(10.0, subject=1, trial=1)
        ScalarOutput.save(20.0, subject=1, trial=2)
        ScalarOutput.save(30.0, subject=2, trial=1)

    def test_excluded_combo_skipped(self, db, tmp_path):
        self._setup(db)
        exclude_schema(subject=1, trial=2, reason="bad", db=db)

        class DoubledOutput(BaseVariable):
            schema_version = 1

        processed = []

        def double(x):
            processed.append(x)
            return x * 2

        for_each(
            double,
            inputs={"x": ScalarOutput},
            outputs=[DoubledOutput],
            subject=[],
            trial=[],
        )

        # (subject=1, trial=2) should have been skipped
        assert len(processed) == 2
        assert 20.0 not in processed


# ===========================================================================
# governing_overrides (the one owner of the matching rules) and the
# include guard built on it
# ===========================================================================


class TestGoverningOverrides:
    def test_no_rows_is_none(self, db):
        assert governing_overrides([{"subject": "1", "trial": "1"}], ["subject", "trial"], db) == [None]

    def test_wildcard_row_governs_with_reason(self, db):
        exclude_schema(subject=3, reason="withdrew", db=db)
        (o,) = governing_overrides([{"subject": "3", "trial": "2"}], ["subject", "trial"], db)
        assert o.keys == {"subject": "3"}
        assert o.included is False
        assert o.reason == "withdrew"
        assert o.changed_at is not None

    def test_specific_beats_wildcard(self, db):
        exclude_schema(subject=3, trial=2, reason="bad trial", db=db)
        exclude_schema(subject=3, reason="withdrew", db=db)  # later but coarser
        (o,) = governing_overrides([{"subject": "3", "trial": "2"}], ["subject", "trial"], db)
        assert o.reason == "bad trial"

    def test_tie_broken_by_latest(self, db):
        exclude_schema(subject=3, reason="first", db=db)
        include_schema(subject=3, reason="second", db=db)
        (o,) = governing_overrides([{"subject": "3"}], ["subject", "trial"], db)
        assert o.reason == "second" and o.included is True

    def test_deeper_row_does_not_govern_coarser_combo(self, db):
        exclude_schema(subject=3, trial=2, reason="bad trial", db=db)
        assert governing_overrides([{"subject": "3"}], ["subject", "trial"], db) == [None]

    def test_exclusion_status_agrees(self, db):
        exclude_schema(subject=3, reason="withdrew", db=db)
        include_schema(subject=3, trial=2, reason="fine", db=db)
        combos = [{"subject": "3", "trial": "1"}, {"subject": "3", "trial": "2"}, {"subject": "1", "trial": "1"}]
        assert exclusion_status(combos, ["subject", "trial"], db) == [True, False, False]

    def test_to_dict(self, db):
        exclude_schema(subject=3, reason="withdrew", db=db)
        (o,) = governing_overrides([{"subject": "3"}], ["subject", "trial"], db)
        d = o.to_dict()
        assert d["keys"] == {"subject": "3"} and d["included"] is False
        assert d["reason"] == "withdrew" and isinstance(d["changed_at"], str)


class TestIncludeUnderCoarserExclusion:
    def test_partial_reinclude_is_allowed(self, db):
        exclude_schema(subject=3, reason="withdrew", db=db)
        include_schema(subject=3, trial=2, reason="trial 2 fine", db=db)
        assert _current_status(db, {"subject": 3, "trial": 2}) is True
        assert _current_status(db, {"subject": 3}) is False
        combos = [{"subject": "3", "trial": "1"}, {"subject": "3", "trial": "2"}]
        assert filter_excluded_combos(combos, ["subject", "trial"], db) == [combos[1]]

    def test_double_partial_reinclude_raises(self, db):
        exclude_schema(subject=3, reason="withdrew", db=db)
        include_schema(subject=3, trial=2, reason="fine", db=db)
        with pytest.raises(ValueError, match="already included"):
            include_schema(subject=3, trial=2, reason="again", db=db)

    def test_reinclude_after_coarser_include_raises(self, db):
        exclude_schema(subject=3, reason="withdrew", db=db)
        include_schema(subject=3, reason="came back", db=db)
        with pytest.raises(ValueError, match="already included"):
            include_schema(subject=3, trial=2, reason="redundant", db=db)

    def test_deeper_exclusion_does_not_cover_a_coarser_include(self, db):
        exclude_schema(subject=3, trial=2, reason="bad trial", db=db)
        with pytest.raises(ValueError, match="no exclusion record"):
            include_schema(subject=3, reason="nothing to lift", db=db)
