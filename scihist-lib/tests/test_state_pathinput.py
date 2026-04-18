"""Tests for check_node_state with PathInput-only functions.

PathInput-only functions have no DB-variable inputs, so _get_expected_combos()
cannot infer the expected set from _record_metadata.  Instead, scidb.for_each
persists the full expected combo set in _for_each_expected at runtime, and
_get_expected_combos falls back to reading from that table when the normal
logic produces an empty set.
"""

import json
import numpy as np
import pytest

from scidb import BaseVariable, for_each as scidb_for_each
from scidb.foreach import _persist_expected_combos
from scilineage import lineage_fcn
from scihist import for_each
from scihist.state import check_node_state, _get_expected_combos


# ---------------------------------------------------------------------------
# Variable types
# ---------------------------------------------------------------------------

class PathInputOutput(BaseVariable):
    schema_version = 1


class RawForFallback(BaseVariable):
    schema_version = 1


class ProcessedForFallback(BaseVariable):
    schema_version = 1


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------

@lineage_fcn
def import_from_file(data):
    return np.asarray(data, dtype=float)


@lineage_fcn
def process_raw(raw):
    return np.asarray(raw, dtype=float) * 2.0


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPersistExpectedCombos:
    """Test that _persist_expected_combos writes correct rows."""

    def test_writes_expected_rows(self, db):
        """Persisting combos creates _for_each_expected entries."""
        combos = [
            {"subject": 1, "trial": "A"},
            {"subject": 1, "trial": "B"},
            {"subject": 2, "trial": "A"},
            {"subject": 2, "trial": "B"},
        ]
        _persist_expected_combos(db, "import_from_file", combos)

        rows = db._duck._fetchall(
            "SELECT function_name, schema_id, branch_params "
            "FROM _for_each_expected ORDER BY schema_id"
        )
        assert len(rows) == 4
        for fn, sid, bp in rows:
            assert fn == "import_from_file"
            assert bp == "{}"

    def test_replaces_on_rerun(self, db):
        """Re-running with different combos replaces old entries."""
        combos_v1 = [
            {"subject": 1, "trial": "A"},
            {"subject": 1, "trial": "B"},
        ]
        _persist_expected_combos(db, "import_from_file", combos_v1)

        rows = db._duck._fetchall(
            "SELECT schema_id FROM _for_each_expected WHERE function_name = ?",
            ["import_from_file"],
        )
        assert len(rows) == 2

        # Re-run with more combos
        combos_v2 = [
            {"subject": 1, "trial": "A"},
            {"subject": 1, "trial": "B"},
            {"subject": 2, "trial": "A"},
        ]
        _persist_expected_combos(db, "import_from_file", combos_v2)

        rows = db._duck._fetchall(
            "SELECT schema_id FROM _for_each_expected WHERE function_name = ?",
            ["import_from_file"],
        )
        assert len(rows) == 3

    def test_empty_combos_is_noop(self, db):
        """Empty combo list doesn't crash or write rows."""
        _persist_expected_combos(db, "import_from_file", [])
        rows = db._duck._fetchall(
            "SELECT schema_id FROM _for_each_expected"
        )
        assert len(rows) == 0

    def test_deduplicates_same_schema_id(self, db):
        """Multiple combos mapping to the same schema_id are deduplicated."""
        # Both combos have the same schema keys (only subject matters here)
        combos = [
            {"subject": 1, "trial": "A"},
            {"subject": 1, "trial": "A"},  # duplicate
        ]
        _persist_expected_combos(db, "import_from_file", combos)

        rows = db._duck._fetchall(
            "SELECT schema_id FROM _for_each_expected WHERE function_name = ?",
            ["import_from_file"],
        )
        assert len(rows) == 1


class TestGetExpectedCombosFallback:
    """Test that _get_expected_combos falls back to _for_each_expected."""

    def test_fallback_when_no_variable_inputs(self, db):
        """When existing logic returns empty, fallback reads _for_each_expected."""
        combos = [
            {"subject": 1, "trial": "A"},
            {"subject": 2, "trial": "B"},
        ]
        _persist_expected_combos(db, "import_from_file", combos)

        expected = _get_expected_combos(db, "import_from_file")
        assert len(expected) == 2
        # Each entry should be (schema_id, "{}")
        for sid, bp in expected:
            assert bp == "{}"

    def test_no_fallback_when_variable_inputs_exist(self, db):
        """DB-variable functions use existing logic; fallback NOT triggered."""
        # Seed input data so the existing logic finds expected combos
        for subj in (1, 2):
            for trial in ("A", "B"):
                RawForFallback.save(np.random.randn(5), subject=subj, trial=trial)

        # Run the function so lineage records exist
        for_each(
            process_raw,
            inputs={"raw": RawForFallback},
            outputs=[ProcessedForFallback],
            subject=[1, 2],
            trial=["A", "B"],
        )

        # Also persist some _for_each_expected rows (they should be ignored)
        _persist_expected_combos(db, "process_raw", [{"subject": 99, "trial": "Z"}])

        expected = _get_expected_combos(db, "process_raw")
        # Should return 4 combos from the existing logic (2x2 input combos)
        assert len(expected) == 4
        # The fallback row (subject=99) should NOT be present
        schema_ids = {sid for sid, _ in expected}
        fallback_rows = db._duck._fetchall(
            "SELECT schema_id FROM _for_each_expected WHERE function_name = ?",
            ["process_raw"],
        )
        fallback_sids = {r[0] for r in fallback_rows}
        assert not (fallback_sids & schema_ids), \
            "Fallback schema_ids should not appear in existing-logic results"


class TestCheckNodeStatePathInput:
    """Integration tests: check_node_state with PathInput-only functions."""

    def _setup_pathinput_run(self, db, tmp_path, combos_to_succeed):
        """Run import_from_file for selected combos, persist all expected.

        Creates filesystem files and runs the function only for combos in
        combos_to_succeed.  All 4 combos are persisted as expected.
        """
        all_combos = [
            {"subject": "1", "trial": "A"},
            {"subject": "1", "trial": "B"},
            {"subject": "2", "trial": "A"},
            {"subject": "2", "trial": "B"},
        ]

        # Persist all combos as expected
        _persist_expected_combos(db, "import_from_file", all_combos)

        # Simulate successful runs by saving outputs + lineage for selected combos
        for combo in combos_to_succeed:
            data = np.random.randn(5)
            # Save output via scihist-style lineage save
            from scihist.foreach import save as _scihist_save
            from scilineage import LineageFcnResult

            result = import_from_file(data)
            assert isinstance(result, LineageFcnResult)
            _scihist_save(
                PathInputOutput,
                result,
                db=db,
                **combo,
            )

    def test_grey_when_partial_success(self, db, tmp_path):
        """3/4 combos succeed, 1 fails → grey."""
        succeed = [
            {"subject": "1", "trial": "A"},
            {"subject": "1", "trial": "B"},
            {"subject": "2", "trial": "B"},
        ]
        self._setup_pathinput_run(db, tmp_path, succeed)

        result = check_node_state(
            import_from_file, [PathInputOutput], db=db,
        )
        assert result["state"] == "grey"
        assert result["counts"]["up_to_date"] == 3
        assert result["counts"]["missing"] == 1

    def test_green_when_all_succeed(self, db, tmp_path):
        """All 4 combos succeed → green."""
        all_combos = [
            {"subject": "1", "trial": "A"},
            {"subject": "1", "trial": "B"},
            {"subject": "2", "trial": "A"},
            {"subject": "2", "trial": "B"},
        ]
        self._setup_pathinput_run(db, tmp_path, all_combos)

        result = check_node_state(
            import_from_file, [PathInputOutput], db=db,
        )
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 4
        assert result["counts"]["missing"] == 0

    def test_red_when_none_succeed(self, db, tmp_path):
        """No combos succeed → red."""
        self._setup_pathinput_run(db, tmp_path, combos_to_succeed=[])

        result = check_node_state(
            import_from_file, [PathInputOutput], db=db,
        )
        assert result["state"] == "red"
        assert result["counts"]["missing"] == 4
        assert result["counts"]["up_to_date"] == 0

    def test_expected_replaced_on_rerun(self, db, tmp_path):
        """Re-run with fewer combos replaces expected set."""
        # First run: 4 expected, 4 succeed
        all_combos = [
            {"subject": "1", "trial": "A"},
            {"subject": "1", "trial": "B"},
            {"subject": "2", "trial": "A"},
            {"subject": "2", "trial": "B"},
        ]
        self._setup_pathinput_run(db, tmp_path, all_combos)

        # Simulate a re-run where only 2 combos are expected (e.g. files removed)
        new_expected = [
            {"subject": "1", "trial": "A"},
            {"subject": "1", "trial": "B"},
        ]
        _persist_expected_combos(db, "import_from_file", new_expected)

        result = check_node_state(
            import_from_file, [PathInputOutput], db=db,
        )
        # 2 expected, but 4 actual records still exist in the DB.
        # All 4 actuals are checked (up_to_date), and no expected combos
        # are missing — so state is green.
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 4
        assert result["counts"]["missing"] == 0
