"""Pytest configuration for scihist tests."""

import sys
from pathlib import Path

import pytest

# Add all relevant packages to path
_here = Path(__file__).parent.parent
_root = _here.parent
sys.path.insert(0, str(Path(__file__).parent))  # make conftest importable
sys.path.insert(0, str(_here / "src"))
sys.path.insert(0, str(_root / "scidb" / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "scifor" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))


DEFAULT_TEST_SCHEMA_KEYS = ["subject", "trial"]


@pytest.fixture
def db(tmp_path):
    """Provide a fresh configured database."""
    from scihist import configure_database

    db_path = tmp_path / "test_db.duckdb"
    db = configure_database(db_path, DEFAULT_TEST_SCHEMA_KEYS)
    yield db
    db.close()
    from scidb.database import _local

    if hasattr(_local, "database"):
        delattr(_local, "database")


@pytest.fixture(autouse=True)
def clear_global_state():
    """Clear global state before and after each test."""
    from scidb.database import _local

    if hasattr(_local, "database"):
        delattr(_local, "database")
    yield
    if hasattr(_local, "database"):
        delattr(_local, "database")


def make_simple_mock_db():
    """Minimal mock DB for tests that only need save_batch to work.

    Uses empty dataset_schema_keys so schema values are not coerced to strings,
    keeping assertions like ``meta["subject"] == 42`` valid.
    """

    class _SimpleMockDB:
        dataset_schema_keys = []

        def distinct_schema_values(self, key):
            return []

        def distinct_schema_combinations(self, keys):
            return []

        def save_batch(self, variable_class, data_items, profile=False):
            ids = []
            for data, meta in data_items:
                variable_class.save(data, **meta)
                ids.append(f"mock-id-{len(ids)}")
            return ids

        def _save_lineage_rows_batch(self, items, output_type):
            pass  # mock: lineage rows not tracked

    return _SimpleMockDB()
