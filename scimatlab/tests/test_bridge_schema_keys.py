"""Tests for schema_keys=/schema_filter= on the MATLAB-driven for_each bridge.

These verify that ``scimatlab.bridge.for_each_prepare(schema_keys=...,
schema_filter=...)`` reuses the same ``scifor.expand_schema_keys()`` +
``SchemaKeyInFilter`` machinery as the pure-Python ``scidb.for_each`` path
(scidb/src/scidb/foreach.py), rather than duplicating DB-querying logic in
the bridge. Runs entirely in Python without MATLAB — same pattern as
test_bridge_skip_computed.py / test_bridge_where.py.
"""

import sys
from pathlib import Path

# Add source paths for the monorepo packages (mirrors test_bridge_where.py)
_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import numpy as np
import pandas as pd
import pytest
from scidb.database import configure_database
from scimatlab.bridge import for_each_prepare, register_matlab_variable

SCHEMA = ["subject", "session", "trial"]


def _var_spec(type_name):
    return {"kind": "var_type", "type_name": type_name}


@pytest.fixture
def db(tmp_path):
    db = configure_database(tmp_path / "bridge_schema_keys.duckdb", SCHEMA)
    yield db
    db.close()


def _seed(db, RawSignal, subjects=(1, 2), sessions=("A", "B"), trials=(1, 2)):
    for subj in subjects:
        for sess in sessions:
            for trial in trials:
                db.save_variable(
                    RawSignal, np.array([1.0, 2.0, 3.0]),
                    subject=subj, session=sess, trial=trial,
                )


class TestBridgeSchemaKeys:
    def test_schema_keys_resolves_all_values(self, db):
        """schema_keys=[...] auto-resolves each key's DB values, same as
        bare key=[] kwargs today."""
        RawSignal = register_matlab_variable("RawSignal_SK1")
        Filtered = register_matlab_variable("Filtered_SK1")
        _seed(db, RawSignal, subjects=(1, 2), sessions=("A",), trials=(1, 2))

        prep = for_each_prepare(
            "fn", "hash1", {"x": _var_spec("RawSignal_SK1")}, ["Filtered_SK1"], {},
            db=db, schema_keys=["subject", "session", "trial"],
        )
        del Filtered  # registered only so the output type resolves cleanly

        assert len(prep["full_combos"]) == 4  # 2 subjects x 1 session x 2 trials
        resolved = dict(prep["extended_metadata_iterables"])
        assert sorted(resolved["subject"]) == ["1", "2"]
        assert sorted(resolved["trial"]) == ["1", "2"]

    def test_schema_keys_subset_is_aggregation(self, db):
        """schema_keys naming fewer than all schema keys aggregates over the
        rest — one combo per iterated-key value, not per record."""
        RawSignal = register_matlab_variable("RawSignal_SK2")
        register_matlab_variable("Filtered_SK2")
        _seed(db, RawSignal, subjects=(1, 2), sessions=("A", "B"), trials=(1, 2))

        prep = for_each_prepare(
            "fn", "hash2", {"x": _var_spec("RawSignal_SK2")}, ["Filtered_SK2"], {},
            db=db, schema_keys=["subject"],
        )

        assert len(prep["full_combos"]) == 2  # 2 subjects, aggregated over session+trial

    def test_schema_filter_on_non_iterated_key_constrains_loaded_data(self, db):
        """Regression test for the latent bug (docs/claude/archive/
        scidb-for-each-internals.md): schema_filter on a key NOT in
        schema_keys used to be silently ignored. Now it's ANDed into
        where= via SchemaKeyInFilter, so it actually restricts which
        records get loaded — checked here via loaded_inputs' row count,
        not just the combo count (which is unaffected either way, since
        aggregation combos are keyed by subject only)."""
        RawSignal = register_matlab_variable("RawSignal_SK3")
        register_matlab_variable("Filtered_SK3")
        _seed(db, RawSignal, subjects=(1, 2), sessions=("A", "B"), trials=(1, 2))

        prep = for_each_prepare(
            "fn", "hash3", {"x": _var_spec("RawSignal_SK3")}, ["Filtered_SK3"], {},
            db=db, schema_keys=["subject"], schema_filter={"session": ["A"]},
        )

        assert len(prep["full_combos"]) == 2  # still 2 subjects
        loaded = prep["loaded_inputs"]["x"]
        # Unfiltered would be 8 rows (2 subjects x 2 sessions x 2 trials);
        # constrained to session="A" it must be 4 (2 subjects x 2 trials).
        assert isinstance(loaded, pd.DataFrame)
        assert len(loaded) == 4
        assert set(loaded["session"].unique()) == {"A"}

    def test_schema_filter_on_iterated_key_overrides_values(self, db):
        """schema_filter on an iterated key replaces DB auto-resolution
        with the given explicit values."""
        RawSignal = register_matlab_variable("RawSignal_SK4")
        register_matlab_variable("Filtered_SK4")
        _seed(db, RawSignal, subjects=(1, 2, 3), sessions=("A",), trials=(1,))

        prep = for_each_prepare(
            "fn", "hash4", {"x": _var_spec("RawSignal_SK4")}, ["Filtered_SK4"], {},
            db=db, schema_keys=["subject"], schema_filter={"subject": [1, 2]},
        )

        assert len(prep["full_combos"]) == 2
        resolved = dict(prep["extended_metadata_iterables"])
        assert sorted(str(v) for v in resolved["subject"]) == ["1", "2"]

    def test_schema_keys_conflicts_with_metadata_iterables(self, db):
        RawSignal = register_matlab_variable("RawSignal_SK5")
        register_matlab_variable("Filtered_SK5")
        _seed(db, RawSignal, subjects=(1,), sessions=("A",), trials=(1,))

        with pytest.raises(ValueError, match="Cannot use both"):
            for_each_prepare(
                "fn", "hash5", {"x": _var_spec("RawSignal_SK5")}, ["Filtered_SK5"],
                {"subject": [1]},
                db=db, schema_keys=["subject"],
            )
