"""Test that exclude_variant excludes all matching variants."""

import tempfile
import shutil
import numpy as np
from scidb import configure_database, BaseVariable, for_each


class RawSignal(BaseVariable):
    pass


class Filtered(BaseVariable):
    pass


def bandpass(signal, low_hz):
    """Dummy filter function."""
    return signal * low_hz


def test_exclude_multiple_variants():
    """Test that exclude_variant excludes all branch_params for a schema combo."""

    # Setup temp database
    tmpdir = tempfile.mkdtemp()
    try:
        db = configure_database(f"{tmpdir}/test.duckdb", ["subject", "session"])

        # Save input data
        RawSignal.save(np.array([1, 2, 3]), subject="1", session="A")
        RawSignal.save(np.array([4, 5, 6]), subject="2", session="A")
        RawSignal.save(np.array([7, 8, 9]), subject="1", session="B")
        RawSignal.save(np.array([10, 11, 12]), subject="2", session="B")

        # Create multiple variants with different branch_params
        for_each(
            bandpass,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[Filtered],
            db=db,
            subject=["1", "2"],
            session=["A", "B"],
        )

        for_each(
            bandpass,
            inputs={"signal": RawSignal, "low_hz": 50},
            outputs=[Filtered],
            db=db,
            subject=["1", "2"],
            session=["A", "B"],
        )

        for_each(
            bandpass,
            inputs={"signal": RawSignal, "low_hz": 100},
            outputs=[Filtered],
            db=db,
            subject=["1", "2"],
            session=["A", "B"],
        )

        # Verify we have 12 Filtered records (4 schema combos × 3 branch variants)
        all_filtered = list(Filtered.load_all(db=db, version_id="all"))
        assert len(all_filtered) == 12, f"Expected 12 records, got {len(all_filtered)}"

        # Test 1: Exclude all variants for session=B (should exclude 6 records: 2 subjects × 3 low_hz)
        count = db.exclude_variant(Filtered, session="B")
        print(f"✓ Excluded {count} variants for session=B")
        assert count == 6, f"Expected to exclude 6 variants, excluded {count}"

        # Verify only 6 records remain (session A only)
        non_excluded = list(Filtered.load_all(db=db, version_id="all"))
        assert len(non_excluded) == 6, f"Expected 6 non-excluded, got {len(non_excluded)}"
        for rec in non_excluded:
            assert rec.metadata["session"] == "A", "Only session A should remain"

        # Test 2: Exclude specific variant (subject=1, session=A, low_hz=20)
        count = db.exclude_variant(Filtered, subject="1", session="A", low_hz=20)
        print(f"✓ Excluded {count} variant for (subject=1, session=A, low_hz=20)")
        assert count == 1, f"Expected to exclude 1 variant, excluded {count}"

        # Verify 5 records remain
        non_excluded = list(Filtered.load_all(db=db, version_id="all"))
        assert len(non_excluded) == 5, f"Expected 5 non-excluded, got {len(non_excluded)}"

        # Test 3: Re-include all session B variants
        count = db.include_variant(Filtered, session="B")
        print(f"✓ Re-included {count} variants for session=B")
        assert count == 6, f"Expected to re-include 6 variants, re-included {count}"

        # Verify 11 records now (excluded only subject=1, session=A, low_hz=20)
        non_excluded = list(Filtered.load_all(db=db, version_id="all"))
        assert len(non_excluded) == 11, f"Expected 11 non-excluded, got {len(non_excluded)}"

        # Test 4: Exclude by record_id still works
        # Get a record_id from the database
        rid_query = db._duck._fetchall(
            "SELECT record_id FROM _record_metadata WHERE variable_name = 'Filtered' "
            "AND excluded = FALSE LIMIT 1"
        )
        rid = rid_query[0][0]
        count = db.exclude_variant(rid)
        assert count == 1, "Excluding by record_id should return 1"

        non_excluded = list(Filtered.load_all(db=db, version_id="all"))
        assert len(non_excluded) == 10, f"Expected 10 non-excluded, got {len(non_excluded)}"

        print("\n✅ All tests passed!")

        db.close()
    finally:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    test_exclude_multiple_variants()
