"""Integration tests for SchemaKey filter types.

Tests verify that schema-key-based filters correctly restrict which records
are returned from load(), covering:
- String key isin (session IN [...])
- Numeric key isin (subject IN [...])
- Equality and inequality on string keys
- Ordering operators (<, <=, >, >=) on numeric keys stored as VARCHAR
- AND/OR combinations of schema key filters
- Combined schema key + variable filters
- Error cases (unknown key)
"""

import sys
from pathlib import Path

import pytest

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root / "src"))

from scidb.model.exceptions import NotFoundError

from scidb import BaseVariable, configure_database, schema_key

# ===========================================================================
# Variable classes
# ===========================================================================


class Measurement(BaseVariable):
    """Scalar measurement used across all schema key filter tests."""

    schema_version = 1


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def session_db(tmp_path):
    """DB with schema keys [subject, session].

    Data layout (all for Measurement):
      subject=1, session="BL"    → 1.0
      subject=1, session="POST"  → 2.0
      subject=1, session="FOL"   → 3.0
      subject=2, session="BL"    → 4.0
      subject=2, session="POST"  → 5.0
    """
    db = configure_database(tmp_path / "test.duckdb", ["subject", "session"])
    Measurement.save(1.0, subject=1, session="BL")
    Measurement.save(2.0, subject=1, session="POST")
    Measurement.save(3.0, subject=1, session="FOL")
    Measurement.save(4.0, subject=2, session="BL")
    Measurement.save(5.0, subject=2, session="POST")
    yield db
    db.close()


@pytest.fixture
def numeric_db(tmp_path):
    """DB with schema key [subject] and subjects 1–5."""
    db = configure_database(tmp_path / "test.duckdb", ["subject"])
    for i in range(1, 6):
        Measurement.save(float(i * 10), subject=i)
    yield db
    db.close()


# ===========================================================================
# isin — string schema key
# ===========================================================================


class TestSchemaKeyIsin:
    def test_isin_two_of_three_sessions(self, session_db):
        results = Measurement.load(where=schema_key("session").isin(["BL", "POST"]))
        assert len(results) == 4  # subjects 1+2, sessions BL+POST

    def test_isin_single_session(self, session_db):
        results = Measurement.load(where=schema_key("session").isin(["BL"]))
        assert len(results) == 2

    def test_isin_empty_returns_nothing(self, session_db):
        with pytest.raises(NotFoundError):
            Measurement.load(where=schema_key("session").isin([]))

    def test_isin_no_match_raises(self, session_db):
        with pytest.raises(NotFoundError):
            Measurement.load(where=schema_key("session").isin(["NONEXISTENT"]))

    def test_isin_with_metadata_narrows_further(self, session_db):
        result = Measurement.load(
            subject=1,
            where=schema_key("session").isin(["BL", "POST"]),
        )
        # subject=1 has BL and POST → 2 records, but single subject+session → array
        assert len(result) == 2

    def test_isin_all_sessions_returns_all(self, session_db):
        results = Measurement.load(
            where=schema_key("session").isin(["BL", "POST", "FOL"])
        )
        assert len(results) == 5


# ===========================================================================
# isin — numeric schema key
# ===========================================================================


class TestSchemaKeyIsinNumeric:
    def test_isin_numeric_subjects(self, numeric_db):
        results = Measurement.load(where=schema_key("subject").isin([1, 2]))
        assert len(results) == 2

    def test_isin_numeric_single(self, numeric_db):
        result = Measurement.load(where=schema_key("subject").isin([3]))
        # Single result may come back as a single BaseVariable, not a list
        if hasattr(result, "__len__"):
            assert len(result) == 1
        else:
            assert result.metadata["subject"] in ("3", 3)

    def test_isin_float_whole_numbers(self, numeric_db):
        # 1.0 and 2.0 should match subjects 1 and 2 (stored as "1", "2")
        results = Measurement.load(where=schema_key("subject").isin([1.0, 2.0]))
        assert len(results) == 2


# ===========================================================================
# Equality and inequality — string key
# ===========================================================================


class TestSchemaKeyEquality:
    def test_eq_string_key(self, session_db):
        results = Measurement.load(where=schema_key("session") == "BL")
        assert len(results) == 2

    def test_ne_string_key(self, session_db):
        # All except BL → POST + FOL = 3 records
        results = Measurement.load(where=schema_key("session") != "BL")
        assert len(results) == 3

    def test_eq_numeric_key(self, numeric_db):
        result = Measurement.load(where=schema_key("subject") == 3)
        # Single record
        if hasattr(result, "__len__"):
            assert len(result) == 1
        else:
            assert result.metadata["subject"] in ("3", 3)


# ===========================================================================
# Ordering operators — numeric key stored as VARCHAR
# ===========================================================================


class TestSchemaKeyOrdering:
    def test_gt(self, numeric_db):
        results = Measurement.load(where=schema_key("subject") > 3)
        assert len(results) == 2  # subjects 4, 5

    def test_ge(self, numeric_db):
        results = Measurement.load(where=schema_key("subject") >= 3)
        assert len(results) == 3  # subjects 3, 4, 5

    def test_lt(self, numeric_db):
        results = Measurement.load(where=schema_key("subject") < 3)
        assert len(results) == 2  # subjects 1, 2

    def test_le(self, numeric_db):
        results = Measurement.load(where=schema_key("subject") <= 3)
        assert len(results) == 3  # subjects 1, 2, 3

    def test_gt_excludes_boundary(self, numeric_db):
        """Verify > 5 returns nothing (subjects only go up to 5)."""
        with pytest.raises(NotFoundError):
            Measurement.load(where=schema_key("subject") > 5)

    def test_ordering_correct_for_multi_digit(self, tmp_path):
        """Lexicographic ordering would give wrong results (10 < 2 as string).
        Numeric cast must be used so 10 > 9."""
        db = configure_database(tmp_path / "test.duckdb", ["subject"])
        try:
            for i in [1, 2, 9, 10, 11]:
                Measurement.save(float(i), subject=i)
            results = Measurement.load(where=schema_key("subject") > 9)
            assert (
                len(results) == 2
            )  # subjects 10, 11 — would be 0 with lexicographic sort
        finally:
            db.close()


# ===========================================================================
# Compound combinations
# ===========================================================================


class TestSchemaKeyCompound:
    def test_and_two_schema_key_filters(self, numeric_db):
        f = (schema_key("subject") >= 2) & (schema_key("subject") <= 4)
        results = Measurement.load(where=f)
        assert len(results) == 3  # subjects 2, 3, 4

    def test_or_two_schema_key_filters(self, session_db):
        f = (schema_key("session") == "BL") | (schema_key("session") == "FOL")
        results = Measurement.load(where=f)
        assert len(results) == 3  # subject 1+2 BL, subject 1 FOL

    def test_not_schema_key_filter(self, session_db):
        f = ~schema_key("session").isin(["BL"])
        results = Measurement.load(where=f)
        assert len(results) == 3  # POST×2 + FOL×1


# ===========================================================================
# SchemaKey filter propagation into Merge inputs in for_each
# ===========================================================================


class AuxMeasurement(BaseVariable):
    """Second variable type for Merge tests (distinct from Measurement)."""

    schema_version = 1


class SubjectLabel(BaseVariable):
    """Subject-level variable (saved without session) for cross-level Merge tests."""

    schema_version = 1


@pytest.fixture
def merge_session_db(tmp_path):
    """DB with [subject, session] and data in both Measurement and AuxMeasurement.

    Measurement:    subject={1,2}, session={BL, POST, FOL}
    AuxMeasurement: subject={1,2}, session={BL, POST, FOL}
    """
    db = configure_database(tmp_path / "merge_test.duckdb", ["subject", "session"])
    for subj in (1, 2):
        for sess, val in [("BL", 10.0), ("POST", 20.0), ("FOL", 30.0)]:
            Measurement.save(float(val + subj), subject=subj, session=sess)
            AuxMeasurement.save(float(val + subj + 100), subject=subj, session=sess)
    yield db
    db.close()


class TestSchemaKeyFilterWithMerge:
    """Regression: where=schema_key(...).isin(...) must filter Merge input data.

    Before the fix, SchemaKeyInFilter/SchemaKeyCompareFilter were silently
    dropped when loading Merge constituents (where=None was hard-coded), so
    the user's function received rows for all sessions regardless of the filter.
    """

    def test_isin_filters_merge_input_rows(self, merge_session_db):
        """Merge loaded with schema_key("session").isin([...]) must exclude non-listed sessions."""
        from scidb.run.foreach import Merge

        from scidb import for_each

        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={"inputVal": Merge(Measurement, AuxMeasurement)},
            outputs=[Measurement],
            as_table=True,
            where=schema_key("session").isin(["BL", "POST"]),
            save=False,
        )

        assert captured, "function was never called"
        # Every row in every call must have session in {"BL", "POST"}
        for tbl in captured:
            if "session" in tbl.columns:
                bad = set(tbl["session"].unique()) - {"BL", "POST"}
                assert not bad, f"unexpected sessions in Merge output: {bad}"

    def test_compare_filter_filters_merge_input_rows(self, merge_session_db):
        """Merge loaded with schema_key("subject") == 1 must contain only subject=1 rows."""
        from scidb.run.foreach import Merge

        from scidb import for_each

        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={"inputVal": Merge(Measurement, AuxMeasurement)},
            outputs=[Measurement],
            as_table=True,
            where=schema_key("subject") == 1,
            save=False,
        )

        assert captured, "function was never called"
        for tbl in captured:
            if "subject" in tbl.columns:
                bad = {str(v) for v in tbl["subject"].unique()} - {"1"}
                assert not bad, f"unexpected subjects in Merge output: {bad}"


# ===========================================================================
# Cross-level Merge: subject-level variable + session-level filter
# ===========================================================================


@pytest.fixture
def cross_level_db(tmp_path):
    """DB with [subject, session].

    SubjectLabel: saved at subject level only (no session key).
      subject=1 → "A",  subject=2 → "B"

    Measurement: saved at session level.
      subject=1, session=BL → 1.0
      subject=1, session=POST → 2.0
      subject=2, session=BL → 3.0
      subject=2, session=POST → 4.0
    """
    db = configure_database(tmp_path / "cross.duckdb", ["subject", "session"])
    SubjectLabel.save("A", subject=1)
    SubjectLabel.save("B", subject=2)
    Measurement.save(1.0, subject=1, session="BL")
    Measurement.save(2.0, subject=1, session="POST")
    Measurement.save(3.0, subject=2, session="BL")
    Measurement.save(4.0, subject=2, session="POST")
    yield db
    db.close()


class SessionValue(BaseVariable):
    """Session-level variable for finer-filter-on-coarser-constituent tests."""

    schema_version = 1


class SessionSide(BaseVariable):
    """Session-level filter variable — same level as SessionValue, finer than SubjectLabel."""

    schema_version = 1


@pytest.fixture
def cross_level_db_with_session_filter(tmp_path):
    """DB with [subject, session].

    SubjectLabel: subject-level only.
      subject=1 → "A",  subject=2 → "B"

    SessionValue: session-level.
      subject=1,session=BL → 10.0;  subject=1,session=POST → 20.0
      subject=2,session=BL → 30.0;  subject=2,session=POST → 40.0

    SessionSide: session-level filter variable (same level as SessionValue).
      subject=1,session=BL → "U";  subject=1,session=POST → "A"
      subject=2,session=BL → "U";  subject=2,session=POST → "A"
    """
    db = configure_database(tmp_path / "cross_session.duckdb", ["subject", "session"])
    SubjectLabel.save("A", subject=1)
    SubjectLabel.save("B", subject=2)
    SessionValue.save(10.0, subject=1, session="BL")
    SessionValue.save(20.0, subject=1, session="POST")
    SessionValue.save(30.0, subject=2, session="BL")
    SessionValue.save(40.0, subject=2, session="POST")
    SessionSide.save("U", subject=1, session="BL")
    SessionSide.save("A", subject=1, session="POST")
    SessionSide.save("U", subject=2, session="BL")
    SessionSide.save("A", subject=2, session="POST")
    yield db
    db.close()


class TestCrossLevelMergeWithSchemaKeyFilter:
    """Regression: session-level where= must not wipe out subject-level Merge constituents.

    When one Merge constituent is stored at subject level (no session column) and
    the where= filter references session, that constituent must be loaded in full
    rather than returning 0 rows (which previously caused 'Cannot merge: one or
    more constituents have no data').
    """

    def test_isin_does_not_empty_subject_level_constituent(self, cross_level_db):
        from scidb.run.foreach import Merge

        from scidb import for_each

        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={"inputVal": Merge(SubjectLabel, Measurement)},
            outputs=[Measurement],
            as_table=True,
            where=schema_key("session").isin(["BL"]),
            save=False,
        )

        assert captured, (
            "function was never called — subject-level constituent was incorrectly emptied"
        )
        for tbl in captured:
            if "session" in tbl.columns:
                bad = set(tbl["session"].unique()) - {"BL"}
                assert not bad, f"unexpected sessions after filter: {bad}"

    def test_compare_does_not_empty_subject_level_constituent(self, cross_level_db):
        from scidb.run.foreach import Merge

        from scidb import for_each

        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={"inputVal": Merge(SubjectLabel, Measurement)},
            outputs=[Measurement],
            as_table=True,
            where=schema_key("session") == "POST",
            save=False,
        )

        assert captured, "function was never called"
        for tbl in captured:
            if "session" in tbl.columns:
                bad = set(tbl["session"].unique()) - {"POST"}
                assert not bad, f"unexpected sessions after filter: {bad}"

    def test_finer_variable_filter_skipped_for_coarser_constituent(
        self, cross_level_db_with_session_filter
    ):
        """Regression: a session-level VariableFilter must not error when applied to
        a subject-level Merge constituent (SubjectLabel).

        SessionSide (session-level) == 'U' is finer than SubjectLabel (subject-level):
        the filter must be skipped for SubjectLabel (returning all subjects) while
        still filtering SessionValue (session-level, same as filter) to BL only.
        The function must be called and receive merged rows for BL sessions.
        """
        from scidb.run.foreach import Merge

        from scidb import for_each

        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        # SessionSide == "U" is session-level; SubjectLabel is subject-level → skip for SubjectLabel
        for_each(
            collect,
            inputs={"inputVal": Merge(SubjectLabel, SessionValue)},
            outputs=[SessionValue],
            as_table=True,
            where=SessionSide == "U",
            save=False,
        )

        assert captured, (
            "function was never called — session-level filter incorrectly rejected subject-level constituent"
        )
        for tbl in captured:
            if "session" in tbl.columns:
                bad = set(tbl["session"].unique()) - {"BL"}
                assert not bad, f"unexpected sessions after filter: {bad}"

    def test_finer_variable_filter_skipped_in_direct_load(
        self, cross_level_db_with_session_filter
    ):
        """Regression: a session-level VariableFilter must not raise when used in a
        direct load() call against a subject-level variable.

        Before the fix, the finer-filter skip only applied when validate_coverage=False
        (Merge constituent loads).  A direct load raised ValueError instead of silently
        skipping the inapplicable filter.
        """
        # SubjectLabel is subject-level; SessionSide is session-level (finer).
        # The filter is not applicable — all subjects should be returned.
        results = SubjectLabel.load(where=SessionSide == "U")
        if not hasattr(results, "__len__"):
            results = [results]
        assert len(results) == 2, f"expected 2 subjects, got {len(results)}"
        labels = {r.data for r in results}
        assert labels == {"A", "B"}


# ===========================================================================
# Error handling
# ===========================================================================


class TestSchemaKeyFilterErrors:
    def test_unknown_key_raises(self, session_db):
        with pytest.raises(ValueError, match="Unknown schema key"):
            Measurement.load(where=schema_key("nonexistent") == "BL")

    def test_unknown_key_isin_raises(self, session_db):
        with pytest.raises(ValueError, match="Unknown schema key"):
            Measurement.load(where=schema_key("nonexistent").isin(["BL"]))


# ===========================================================================
# SchemaKey combined with variable-level filter on for_each-computed output
# ===========================================================================


class ComputedOutput(BaseVariable):
    """Output variable computed via for_each with a variable-level where= filter."""

    schema_version = 1


class FilterVar(BaseVariable):
    """Fine-grained filter variable (session-level) used as where= in for_each."""

    schema_version = 1


@pytest.fixture
def foreach_where_db(tmp_path):
    """DB with [subject, session] schema.

    FilterVar (session-level):
      subject=1, session=BL  → "U"
      subject=1, session=MID → "U"
      subject=1, session=TX  → "A"
      subject=2, session=BL  → "U"
      subject=2, session=MID → "A"

    ComputedOutput is saved via for_each with where=FilterVar == "U":
      subject=1, session=BL  → 1.0  (__where = "FilterVar == 'U'")
      subject=1, session=MID → 1.0  (__where = "FilterVar == 'U'")
      subject=2, session=BL  → 1.0  (__where = "FilterVar == 'U'")
    """
    from scidb import for_each

    db = configure_database(tmp_path / "foreach_where.duckdb", ["subject", "session"])

    FilterVar.save("U", subject=1, session="BL")
    FilterVar.save("U", subject=1, session="MID")
    FilterVar.save("A", subject=1, session="TX")
    FilterVar.save("U", subject=2, session="BL")
    FilterVar.save("A", subject=2, session="MID")

    def compute(filterVar):
        return 1.0  # value doesn't matter; we're testing the load path

    # Provide explicit iteration axes so for_each iterates per (subject, session)
    # rather than loading everything as a single table.  The where= filter
    # then skips combinations where FilterVar != "U" ((1,TX) and (2,MID)).
    for_each(
        compute,
        inputs={"filterVar": FilterVar},
        outputs=[ComputedOutput],
        where=FilterVar == "U",
        subject=[1, 2],
        session=["BL", "MID", "TX"],
    )

    yield db
    db.close()


class TestSchemaKeyWithForEachWhereFilter:
    """Regression: schema_key() filter combined with a variable-level filter must not
    error when loading for_each-computed data.

    The SchemaKey portion is split off as a row selector and the variable-level
    portion drives the semantic variant match, so combining them returns only the
    SchemaKey-selected rows of the matched variant (no error, no leak).
    """

    def test_schema_key_isin_combined_with_variable_filter(self, foreach_where_db):
        """Load with schema_key + variable filter returns only the selected sessions."""
        results = ComputedOutput.load(
            where=schema_key("session").isin(["BL"]) & (FilterVar == "U")
        )
        if not hasattr(results, "__len__"):
            results = [results]
        assert len(results) == 2  # subject=1,BL and subject=2,BL
        sessions = {r.metadata["session"] for r in results}
        assert sessions == {"BL"}

    def test_schema_key_eq_combined_with_variable_filter(self, foreach_where_db):
        """Equality SchemaKey + variable filter returns correct subset."""
        result = ComputedOutput.load(
            where=(schema_key("session") == "MID") & (FilterVar == "U")
        )
        # Single result may come back as a single BaseVariable, not a list
        if hasattr(result, "__len__"):
            assert len(result) == 1
            assert result[0].metadata["session"] == "MID"
        else:
            assert result.metadata["session"] == "MID"

    def test_variable_filter_alone_still_works(self, foreach_where_db):
        """Baseline: variable-only filter (no SchemaKey) still returns all matching records."""
        results = ComputedOutput.load(where=FilterVar == "U")
        if not hasattr(results, "__len__"):
            results = [results]
        assert len(results) == 3  # BL×2 + MID×1

    def test_schema_key_only_still_works(self, foreach_where_db):
        """Baseline: SchemaKey-only filter still returns records regardless of __where."""
        results = ComputedOutput.load(where=schema_key("session").isin(["BL"]))
        if not hasattr(results, "__len__"):
            results = [results]
        assert len(results) == 2

    def test_not_found_when_session_has_no_matching_where(self, foreach_where_db):
        """NotFoundError when the requested session exists but not with the given where= variant."""
        with pytest.raises(NotFoundError):
            ComputedOutput.load(
                where=schema_key("session").isin(["TX"]) & (FilterVar == "U")
            )

    def test_schema_key_isin_on_right_side(self, foreach_where_db):
        """SchemaKey filter commutes: (FilterVar == 'U') & schema_key(...).isin([...])."""
        results = ComputedOutput.load(
            where=(FilterVar == "U") & schema_key("session").isin(["BL"])
        )
        if not hasattr(results, "__len__"):
            results = [results]
        assert len(results) == 2
        sessions = {r.metadata["session"] for r in results}
        assert sessions == {"BL"}


# ===========================================================================
# SchemaKey on a Merge input (the for_each Merge-constituent load path)
# ===========================================================================


class ComputedOutput2(BaseVariable):
    """Second for_each-computed constituent (has a stored __where), so that BOTH
    Merge constituents leak via Strategy 1 if the restriction is dropped — the
    inner join cannot mask the bug the way a raw-saved constituent would."""

    schema_version = 1


class CollectOut(BaseVariable):
    """Output for the end-to-end Merge + as_table regression test."""

    schema_version = 1


class TestSchemaKeyOnMergeConstituent:
    """Regression: a SchemaKey filter combined with a variable-level filter must
    still restrict rows when the input is a ``Merge`` whose constituent was itself
    computed by ``for_each``.

    The Merge path pre-resolves the where= filter per constituent into a
    ``_PreresolvedFilter`` whose ``resolve()`` already bakes in the SchemaKey
    restriction.  ``_load_with_where`` matches the constituent's variant
    semantically (by consumed inputs) and then applies that pre-resolved id set as
    a row restriction; without it the SchemaKey filter would appear to do nothing
    and every session would leak through.
    """

    def test_preresolved_filter_restricts_rows(self, foreach_where_db):
        """Directly exercise the bug: a _PreresolvedFilter whose variable_filter
        matches the computed variant must still restrict rows to its resolved
        schema_ids (the row-restriction applied on top of the variant match)."""
        from scidb.run.foreach import _PreresolvedFilter

        db = foreach_where_db
        # Resolve ComputedOutput's schema_ids restricted to session=BL.
        bl_ids = (
            schema_key("session")
            .isin(["BL"])
            .resolve(db, ComputedOutput, ComputedOutput.table_name())
        )
        pf = _PreresolvedFilter(bl_ids, variable_filter=(FilterVar == "U"))

        out = db.load_all_as_df(
            ComputedOutput,
            layout="spread",
            include_rid=True,
            stringify_schema=True,
            version_id="latest",
            where=pf,
        )
        sessions = set(out["session"].astype(str))
        assert sessions == {"BL"}, f"Strategy 1 leaked sessions: {sessions}"
        assert len(out) == 2  # subject=1,BL and subject=2,BL

    def test_merge_input_schema_key_restricts_table(self, foreach_where_db):
        """End-to-end: for_each with a Merge of two for_each-computed constituents
        and as_table=True must hand the function a table containing only the
        SchemaKey-selected sessions.

        Both constituents match the same variant (same consumed inputs), so both
        pass the variant match.  If the pre-resolved schema-id restriction is
        dropped, both leak session=MID and the inner join cannot remove it — the
        table would wrongly contain MID.
        """
        from scidb import Merge, for_each

        # Second computed constituent computed under the same where= as ComputedOutput.
        def compute2(filterVar):
            return 2.0

        for_each(
            compute2,
            inputs={"filterVar": FilterVar},
            outputs=[ComputedOutput2],
            where=FilterVar == "U",
            subject=[1, 2],
            session=["BL", "MID", "TX"],
        )

        captured = {}

        def collect(merged):
            captured["df"] = merged
            return 1.0

        for_each(
            collect,
            inputs={"merged": Merge(ComputedOutput, ComputedOutput2)},
            outputs=[CollectOut],
            where=schema_key("session").isin(["BL"]) & (FilterVar == "U"),
            as_table=True,
            save=False,
        )

        # The merged table presented to the function carries only data columns
        # (schema keys are consumed by the inner join), so assert on row count:
        # after the fix both constituents restrict to {1/BL, 2/BL} -> 2 rows.
        # Before the fix both leaked session=MID -> 3 rows.
        df = captured["df"]
        assert len(df) == 2, (
            f"Merge SchemaKey filter leaked rows: got {len(df)} rows "
            f"(expected 2 = subject 1 & 2 at session BL)\n{df}"
        )
