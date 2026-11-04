"""Integration tests for the scidb.inspect.Inspector facade (Phase 1).

Builds a small real pipeline (raw → bandpass with two constant variants,
plus a re-save), closes the writer, then inspects the database through a
strictly read-only Inspector.
"""

import dataclasses
import json

import numpy as np
import pytest
from scidb.inspect import Inspector

from scidb import BaseVariable, NotFoundError, configure_database, for_each

SCHEMA_KEYS = ["subject", "session"]


class InspRaw(BaseVariable):
    schema_version = 1


class InspFiltered(BaseVariable):
    schema_version = 1


def bandpass(signal, low_hz):
    return signal * low_hz


def build_populated_db(db_path):
    """2 raw records → 2×2 filtered (two low_hz variants), then one re-save
    of raw S01 so the version trail has depth. Closes the writer."""
    db = configure_database(db_path, SCHEMA_KEYS)
    InspRaw.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")
    InspRaw.save(np.array([4.0, 5.0, 6.0]), subject="S02", session="1")
    for_each(
        bandpass,
        {"signal": InspRaw, "low_hz": 20},
        [InspFiltered],
        subject=["S01", "S02"],
        session=["1"],
    )
    for_each(
        bandpass,
        {"signal": InspRaw, "low_hz": 30},
        [InspFiltered],
        subject=["S01", "S02"],
        session=["1"],
    )
    # Re-save S01 raw with different content → new record, old one superseded.
    InspRaw.save(np.array([9.0, 9.0, 9.0]), subject="S01", session="1")
    db.close()


@pytest.fixture
def populated_db_path(tmp_path):
    path = tmp_path / "insp.duckdb"
    build_populated_db(path)
    return path


@pytest.fixture
def insp(populated_db_path):
    inspector = Inspector.open(populated_db_path)
    yield inspector
    inspector.close()


class TestOverview:
    def test_counts(self, insp):
        o = insp.overview()
        # 3 raw (incl. the superseded S01) + 4 filtered; constants excluded.
        assert o.n_records == 7
        assert o.n_invocations == 4  # 2 subjects × 2 low_hz variants
        assert o.n_runs == 2  # one _run per for_each execution
        assert o.n_excluded_records == 0
        assert o.schema_keys == SCHEMA_KEYS
        assert o.n_schema_locations >= 2
        assert o.db_size_bytes > 0
        assert o.last_save is not None
        assert o.last_run is not None

    def test_json_round_trip(self, insp):
        o = insp.overview()
        parsed = json.loads(json.dumps(dataclasses.asdict(o), default=str))
        assert parsed["n_records"] == 7


class TestVariables:
    def test_summaries(self, insp):
        by_name = {v.name: v for v in insp.variables()}
        # configure_database registers every known BaseVariable subclass, so
        # other test modules' types may appear too — assert on ours only.
        assert by_name["InspRaw"].record_count == 3
        assert by_name["InspRaw"].variant_count == 0  # raw saves, no producing fn
        assert by_name["InspRaw"].schema_level == "session"
        assert by_name["InspRaw"].last_saved is not None
        assert by_name["InspFiltered"].record_count == 4
        assert by_name["InspFiltered"].variant_count == 2  # low_hz = 20 | 30

    def test_summary_last_saved_is_last_saved_by_variable(self, insp):
        """One owner: the summary's last_saved IS the dedicated query's."""
        by_name = {v.name: v for v in insp.variables()}
        latest = insp.last_saved_by_variable()
        assert by_name["InspRaw"].last_saved == latest["InspRaw"]
        assert by_name["InspFiltered"].last_saved == latest["InspFiltered"]

    def test_detail(self, insp):
        d = insp.variable("InspFiltered")
        assert d.record_count == 4
        assert d.variant_count == 2
        assert d.data_columns  # data table exists with at least one column
        assert d.records_by_level.get("session") == 4

    def test_detail_accepts_class(self, insp):
        assert insp.variable(InspFiltered).name == "InspFiltered"

    def test_unknown_variable_raises(self, insp):
        with pytest.raises(NotFoundError):
            insp.variable("NoSuchVariable")

    def test_json_round_trip(self, insp):
        payload = [dataclasses.asdict(v) for v in insp.variables()]
        assert json.loads(json.dumps(payload, default=str))


class TestSchemaTree:
    def test_hierarchy(self, insp):
        tree = insp.schema_tree()
        assert tree.schema_keys == SCHEMA_KEYS
        roots = {r.value: r for r in tree.roots}
        assert set(roots) >= {"S01", "S02"}
        s01_sessions = {c.value: c for c in roots["S01"].children}
        assert "1" in s01_sessions
        # S01/1 holds: latest raw + superseded raw + 2 filtered variants.
        assert s01_sessions["1"].record_count == 4
        assert s01_sessions["1"].schema_id is not None
        assert s01_sessions["1"].schema_level == "session"

    def test_json_round_trip(self, insp):
        tree = insp.schema_tree()
        parsed = json.loads(json.dumps(dataclasses.asdict(tree), default=str))
        assert parsed["schema_keys"] == SCHEMA_KEYS


class TestRecords:
    def test_latest_collapses_resaves(self, insp):
        latest = insp.records("InspRaw", subject="S01", session="1")
        assert len(latest) == 1

    def test_versions_expose_resave_trail(self, insp):
        all_versions = insp.records("InspRaw", latest=False, subject="S01", session="1")
        assert len(all_versions) == 2
        # Distinct content → distinct records.
        assert len({r.record_id for r in all_versions}) == 2

    def test_variants_coexist_in_latest(self, insp):
        recs = insp.records("InspFiltered", subject="S01", session="1")
        assert len(recs) == 2  # low_hz=20 and low_hz=30 are separate variants
        for r in recs:
            assert r.schema == {"subject": "S01", "session": "1"}
            assert r.excluded is False
            assert r.timestamp

    def test_json_round_trip(self, insp):
        recs = insp.records("InspFiltered")
        payload = [dataclasses.asdict(r) for r in recs]
        assert len(json.loads(json.dumps(payload, default=str))) == 4


class TestReadOnlyGuard:
    """Regression guard: an Inspector-opened connection must never write."""

    def test_ddl_rejected(self, insp):
        with pytest.raises(Exception, match="(?i)read.only"):
            insp._db._duck.execute("CREATE TABLE _sneaky_write (i INTEGER)")

    def test_dml_rejected(self, insp):
        with pytest.raises(Exception, match="(?i)read.only"):
            insp._db._duck.execute("DELETE FROM _record_save")

    def test_read_only_flag_set(self, insp):
        assert insp._db.read_only is True
        assert insp._db._duck.read_only is True


class TestLiveDbProperty:
    def test_db_inspect_property_shares_connection(self, tmp_path):
        db = configure_database(tmp_path / "live.duckdb", SCHEMA_KEYS)
        try:
            InspRaw.save(np.array([1.0]), subject="S01", session="1")
            insp = db.inspect
            assert insp is db.inspect  # lazy singleton
            assert insp.overview().n_records == 1
        finally:
            db.close()


class LsFirst(BaseVariable):
    schema_version = 1


class LsSecond(BaseVariable):
    schema_version = 1


class LsNeverSaved(BaseVariable):
    schema_version = 1


class TestLastSavedByVariable:
    """The sidebar's "Last saved" sort reads this; per-save recency, not
    first-save ``created_at``."""

    def test_order_resave_and_never_saved(self, tmp_path):
        import time

        path = tmp_path / "last_saved.duckdb"
        db = configure_database(path, SCHEMA_KEYS)
        try:
            LsFirst.save(np.array([1.0]), subject="S01", session="1")
            time.sleep(0.02)
            LsSecond.save(np.array([2.0]), subject="S01", session="1")
            before = db.inspect.last_saved_by_variable()
            assert before["LsSecond"] > before["LsFirst"]
            assert "LsNeverSaved" not in before

            # Re-saving IDENTICAL content keeps the record (created_at
            # frozen) but appends a save event, so the time must move.
            time.sleep(0.02)
            LsFirst.save(np.array([1.0]), subject="S01", session="1")
            after = db.inspect.last_saved_by_variable()
            assert after["LsFirst"] > before["LsFirst"]
            assert after["LsFirst"] > after["LsSecond"]
        finally:
            db.close()
