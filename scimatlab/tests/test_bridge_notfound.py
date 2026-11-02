"""Regression tests for the no-match error contract across the MATLAB bridge.

scidb's ``load()`` raises ``NotFoundError`` when nothing matches. MATLAB's
``+scidb/BaseVariable.m`` does NOT translate that Python exception — it detects
the no-match case via the ``n == 0`` sentinel in the dict returned by
``load_and_extract`` and raises a clean ``scidb:NotFoundError`` itself.

If ``load_and_extract`` lets the Python ``NotFoundError`` cross the bridge, MATLAB
surfaces it as the opaque, generic ``MATLAB:Python:PyException`` instead, breaking
the documented error contract and every ``verifyError(..., 'scidb:NotFoundError')``
MATLAB test.  The MATLAB tests cover this end-to-end, but they are skipped in CI
without a MATLAB licence, so this pytest-level guard runs the same contract check
on every Python test run.
"""

import sys
from pathlib import Path

# Add source paths for the monorepo packages (mirrors test_bridge.py)
_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduckdb" / "src"))
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import pytest
from scidb.store.database import _local
from scidb.model.exceptions import NotFoundError
from scimatlab.bridge import load_and_extract, register_matlab_variable

from scidb import configure_database


@pytest.fixture(autouse=True)
def clear_db_state():
    if hasattr(_local, "database"):
        delattr(_local, "database")
    yield
    if hasattr(_local, "database"):
        delattr(_local, "database")


@pytest.fixture
def db(tmp_path):
    d = configure_database(tmp_path / "test.duckdb", ["subject", "session"])
    yield d
    d.close()


def test_underlying_scidb_load_raises_not_found(db):
    """Document the behaviour the bridge shields MATLAB from: the raw scidb
    load() raises NotFoundError for a registered-but-empty variable."""
    cls = register_matlab_variable("BridgeNotFoundProbe")
    db.register(cls)
    with pytest.raises(NotFoundError):
        list(db.load(cls, {"subject": 1, "session": "A"}, version_id="latest"))


def test_load_and_extract_returns_empty_sentinel_when_no_records(db):
    """The bridge entry point MUST NOT raise — it returns the n == 0 sentinel
    so MATLAB raises a clean scidb:NotFoundError (not MATLAB:Python:PyException)."""
    cls = register_matlab_variable("BridgeNotFoundProbe")
    db.register(cls)
    result = load_and_extract(cls, {"subject": 1, "session": "A"}, db=db)
    assert result["n"] == 0
