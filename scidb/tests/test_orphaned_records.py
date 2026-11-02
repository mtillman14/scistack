"""
Regression tests for orphaned records.

An orphaned record exists in the save log / _record entity but has no corresponding
row in the variable's data table.  This can happen when a previous save used an unexpected
schema key (e.g. a prior for_each run with distribute=True saved an output with
schema keys the user didn't intend), or when a save partially failed.

Two symptoms:
  1. load_all_as_df (spread layout) returns rows with NaN data for orphaned records.
  2. Orphaned records that have a non-null schema key value (e.g. distribute=1) prevent
     that column from being identified as all-null, so it is not dropped before being
     sent to MATLAB — causing a spurious column in the Merge constituent table.

Fix: use INNER JOIN instead of LEFT JOIN when assembling the spread DataFrame from
(meta_df, data_df), so orphaned records are excluded rather than NaN-filled.
"""

import pytest

from scidb import BaseVariable, configure_database

SCHEMA = ["subject", "session", "distribute"]


@pytest.fixture
def db(tmp_path):
    db = configure_database(tmp_path / "test.duckdb", SCHEMA)
    yield db
    db.close()


class Grouping(BaseVariable):
    pass


def _inject_orphan(db, type_name, subject, distribute=None):
    """Directly insert a _record (+ _record_save) row with no matching data row.

    This simulates a record that was logged (e.g. by a buggy prior for_each run)
    but whose data row was never written to {type_name}_data.

    Uses a dummy subject ("9999") so that schema_level is always determinable —
    the _schema table requires schema_level NOT NULL.  The exact schema values
    don't matter; what matters is that the injected schema_id has no data row.
    """
    duck = db._duck

    # Build schema_level and key_values for the orphan.
    # Use a dummy subject that won't collide with real test data.
    if distribute is not None:
        schema_level = "distribute"
        key_values = {"subject": "9999", "distribute": str(distribute)}
    else:
        schema_level = "subject"
        key_values = {"subject": "9999"}

    schema_id = duck.get_or_create_schema_id(schema_level, key_values)

    # Insert a phantom record (entity + save-event, no corresponding data row)
    import hashlib
    import time

    phantom_rid = (
        "phantom"
        + hashlib.md5(
            f"{type_name}{subject}{distribute}{time.time()}".encode()
        ).hexdigest()[:10]
    )
    duck.con.execute(
        "INSERT INTO _record "
        "(record_id, created_at, type, schema_id, content_hash, schema_version, excluded) "
        "VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, 1, FALSE)",
        [phantom_rid, type_name, schema_id, "deadbeef" * 2],
    )
    duck.con.execute(
        "INSERT INTO _record_save (record_id, timestamp) VALUES (?, CURRENT_TIMESTAMP)",
        [phantom_rid],
    )
    return phantom_rid


class TestOrphanedRecordsExcluded:
    def test_no_nan_rows_from_orphaned_record(self, db):
        """Orphaned record must not appear as NaN row in spread load."""
        Grouping.save("SHAM", subject=1, session="A")
        Grouping.save("STIM", subject=2, session="A")

        _inject_orphan(db, "Grouping", subject=None, distribute=None)

        result = db.load_all_as_df(Grouping, layout="spread", stringify_schema=True)

        # Should have exactly the 2 real records — not 3
        assert len(result) == 2, f"Expected 2 rows, got {len(result)}: {result}"

        # Data column must not contain NaN
        assert result["Grouping"].notna().all(), f"NaN data values found:\n{result}"

    def test_distribute_column_dropped_when_only_orphan_has_it(self, db):
        """A schema key that is non-null only in an orphaned record must be treated as
        all-null after the orphan is excluded, so the Python Merge fix can drop it."""
        Grouping.save("SHAM", subject=1, session="A")
        Grouping.save("STIM", subject=2, session="A")

        # Inject orphan with distribute=1 (simulates prior buggy for_each output)
        _inject_orphan(db, "Grouping", subject=None, distribute=1)

        result = db.load_all_as_df(Grouping, layout="spread", stringify_schema=True)

        # distribute must be all-null in the 2 valid rows (none were saved with distribute)
        assert len(result) == 2
        assert result["distribute"].isna().all(), (
            f"distribute column has unexpected non-null values:\n{result['distribute']}"
        )

    def test_valid_records_unaffected(self, db):
        """INNER JOIN must not drop valid records that have data."""
        Grouping.save("SHAM", subject=1, session="A")
        Grouping.save("STIM", subject=2, session="A")
        Grouping.save("ONWARD", subject=3, session="A")

        result = db.load_all_as_df(Grouping, layout="spread", stringify_schema=True)

        assert len(result) == 3
        assert set(result["Grouping"].tolist()) == {"SHAM", "STIM", "ONWARD"}
