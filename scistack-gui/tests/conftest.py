"""
Shared fixtures for scistack-gui tests.

Sets up a real DuckDB database populated with variable data and a for_each
run so that the full API surface can be exercised end-to-end.
"""

import json
import sys
from pathlib import Path

import numpy as np
import pytest
import scifor as _scifor

# Make sure the local package is importable from an editable install.
sys.path.insert(0, str(Path(__file__).parent))        # make conftest importable
sys.path.insert(0, str(Path(__file__).parent.parent))
_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scistack" / "src"))   # scistack package

from fastapi.testclient import TestClient
from scidb import BaseVariable, configure_database, for_each
from scidb.database import _local

import scistack_gui.db as _gui_db
from scistack_gui import registry as _registry
from scistack_gui.app import create_app


# ---------------------------------------------------------------------------
# Test variable classes — defined at module level so they are always present
# in BaseVariable._all_subclasses when the test client is created.
# ---------------------------------------------------------------------------

class RawSignal(BaseVariable):
    pass


class FilteredSignal(BaseVariable):
    pass


# ---------------------------------------------------------------------------
# Pipeline function used to populate test DB
# ---------------------------------------------------------------------------

def bandpass_filter(signal, low_hz):
    """Simple filter stub: scales signal by low_hz constant."""
    return np.asarray(signal, dtype=float) * float(low_hz)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_db_state():
    """
    Reset all module-level singletons between tests so no state leaks.
    Runs before (via yield) and after each test.
    """
    # Pre-test: clear everything
    if hasattr(_local, "database"):
        delattr(_local, "database")
    _gui_db._db = None
    _gui_db._db_path = None
    _scifor.set_schema([])
    # Keep only the test functions registered across tests
    _registry._functions.clear()

    yield

    # Post-test: clean up again
    if hasattr(_local, "database"):
        delattr(_local, "database")
    _gui_db._db = None
    _gui_db._db_path = None
    _scifor.set_schema([])
    _registry._functions.clear()


@pytest.fixture
def tmp_db_path(tmp_path):
    """Provide a temporary .duckdb path (file is NOT created yet)."""
    return tmp_path / "test.duckdb"


@pytest.fixture
def populated_db(tmp_path):
    """
    Real DuckDB with subject/session schema, 4 RawSignal records,
    and a for_each run producing FilteredSignal (with constant low_hz=20).

    Also sets scistack_gui.db._db / _db_path so API endpoints work.
    """
    db_path = tmp_path / "test.duckdb"
    db = configure_database(db_path, ["subject", "session"])

    # Seed raw data
    for subj in [1, 2]:
        for sess in ["pre", "post"]:
            RawSignal.save(np.random.randn(10), subject=subj, session=sess)

    # Run pipeline so list_pipeline_variants() returns results
    for_each(
        bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[FilteredSignal],
        subject=[1, 2],
        session=["pre", "post"],
    )

    # Point the GUI db module at this database
    _gui_db._db = db
    _gui_db._db_path = db_path

    # Ensure pipeline structure tables exist (normally done by init_db).
    from scistack_gui import pipeline_store
    pipeline_store._ensure_tables(db)

    yield db

    db.close()


@pytest.fixture
def layout_path(tmp_path, populated_db):
    """
    Return the layout file path that layout.py will read/write.
    (It derives the path from get_db_path(), which is already set by populated_db.)
    """
    return _gui_db.get_db_path().with_suffix(".layout.json")


@pytest.fixture
def client(populated_db):
    """FastAPI TestClient backed by the populated database."""
    # Register the test function so /api/registry and /api/run can see it
    _registry._functions["bandpass_filter"] = bandpass_filter
    app = create_app()
    with TestClient(app) as c:
        yield c


@pytest.fixture
def bp_node_id(populated_db):
    """The composite ``fn__bandpass_filter__{call_id}`` ID for the seeded
    for_each call site in ``populated_db``.  Lets tests target the call-site
    node without hard-coding the call_id."""
    from scidb.foreach_config import ForEachConfig
    from scistack_gui.domain.graph_builder import fn_node_id
    cid = ForEachConfig(
        fn=bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 20},
    ).to_call_id()
    return fn_node_id("bandpass_filter", cid)


def find_fn_node_id_by_label(nodes, label: str) -> str:
    """Find a function node ID by its display label.

    Useful for tests that want to assert against the seeded call site
    without depending on the exact call_id.  Asserts a unique match —
    multiple matches would mean the test exercises multiple call sites
    and should target them explicitly.
    """
    matches = [n["id"] for n in nodes
               if n.get("type") == "functionNode" and n.get("data", {}).get("label") == label]
    assert len(matches) == 1, (
        f"expected exactly one function node with label {label!r}, got {matches}"
    )
    return matches[0]


def fn_min_state_across_call_sites(nodes, label: str) -> str | None:
    """Return the most pessimistic run_state across all call sites of ``label``.

    Used for legacy tests that asked "is fn X green?" before per-call-site
    nodes existed.  Now that the same fn can produce multiple nodes (one per
    for_each call site), the equivalent question is "are *all* call sites
    green?"  This helper returns:

      - ``None`` if no node with that label exists (preserves the old
        ``next(..., None)`` semantics).
      - The worst state (red < grey < green) across all matching nodes.
    """
    _ORDER = {"red": 0, "grey": 1, "green": 2}
    states = [n["data"].get("run_state") for n in nodes
              if n.get("type") == "functionNode" and n.get("data", {}).get("label") == label]
    states = [s for s in states if s is not None]
    if not states:
        return None
    return min(states, key=lambda s: _ORDER.get(s, 0))
