"""Pytest configuration and shared fixtures for scidb tests."""

# Add all local packages to path for imports
import sys
from contextlib import nullcontext
from pathlib import Path

import pandas as pd
import pytest

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "scifor" / "src"))

from scidb.store.database import _local

from scidb import BaseVariable, configure_database

# Default schema keys for testing - defines the hierarchical dataset structure.
# Keys not in this list are treated as version parameters (e.g. stage, type, dtype).
DEFAULT_TEST_SCHEMA_KEYS = ["subject", "trial"]


@pytest.fixture
def temp_db_path(tmp_path):
    """Provide a temporary database path."""
    return tmp_path / "test_db.sqlite"


@pytest.fixture
def db(tmp_path):
    """Provide a fresh configured database instance."""
    db_path = tmp_path / "test_db.duckdb"
    db = configure_database(db_path, DEFAULT_TEST_SCHEMA_KEYS)
    yield db
    db.close()


@pytest.fixture
def configured_db(tmp_path):
    """Provide a configured global database."""
    db_path = tmp_path / "test_db.duckdb"
    db = configure_database(db_path, DEFAULT_TEST_SCHEMA_KEYS)
    yield db
    db.close()
    # Clear the global state
    if hasattr(_local, "database"):
        delattr(_local, "database")


@pytest.fixture(autouse=True)
def clear_global_db():
    """Clear global database state before each test."""
    if hasattr(_local, "database"):
        delattr(_local, "database")
    yield
    if hasattr(_local, "database"):
        delattr(_local, "database")


# --- Sample Variable Classes for Testing ---
# ScalarValue, ArrayValue, MatrixValue use native SciDuck storage (no to_db/from_db).
# DataFrameValue uses custom serialization to test that path too.


class ScalarValue(BaseVariable):
    """Simple scalar value — uses native SciDuck storage."""

    schema_version = 1


class ArrayValue(BaseVariable):
    """1D numpy array — uses native SciDuck storage."""

    schema_version = 1


class MatrixValue(BaseVariable):
    """2D numpy array — uses native SciDuck storage."""

    schema_version = 2  # Different schema version for testing


class DataFrameValue(BaseVariable):
    """Pandas DataFrame — uses native storage (no to_db/from_db needed)."""

    schema_version = 1


class CustomDataFrameValue(BaseVariable):
    """Pandas DataFrame — uses custom serialization (to_db/from_db)."""

    schema_version = 1

    def to_db(self) -> pd.DataFrame:
        return self.data.copy()

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df.copy()


@pytest.fixture
def scalar_class():
    return ScalarValue


@pytest.fixture
def array_class():
    return ArrayValue


@pytest.fixture
def matrix_class():
    return MatrixValue


@pytest.fixture
def dataframe_class():
    return DataFrameValue


@pytest.fixture
def custom_dataframe_class():
    return CustomDataFrameValue


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

        def transaction(self, label=""):
            return nullcontext()  # mock: for_each saves inside one transaction

    return _SimpleMockDB()
