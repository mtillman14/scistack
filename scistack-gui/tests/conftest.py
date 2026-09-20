"""
Shared fixtures for scistack-gui tests.

Sets up a real DuckDB database populated with variable data and a for_each
run so that the full API surface can be exercised end-to-end.
"""

import sys
from pathlib import Path

import numpy as np
import pytest

import scifor as _scifor

# Make sure the local package is importable from an editable install.
sys.path.insert(0, str(Path(__file__).parent))  # make conftest importable
sys.path.insert(0, str(Path(__file__).parent.parent))
_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scistack" / "src"))  # scistack package

import scistack_gui.db as _gui_db
from fastapi.testclient import TestClient
from scidb.database import _local
from scistack_gui import matlab_registry as _matlab_registry
from scistack_gui import registry as _registry
from scistack_gui.app import create_app

from scidb import BaseVariable, configure_database, for_each

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


# Variable classes that exist before any test runs: the ones this conftest
# and each test module define at import time. Captured lazily on the first
# fixture call — by then every test module has been imported, so anything
# appearing in _all_subclasses AFTER this point was created by a test.
_baseline_variable_subclasses: "dict | None" = None


def _reset_variable_subclasses() -> None:
    """Drop BaseVariable subclasses created during a test.

    ``BaseVariable.__init_subclass__`` registers every subclass in the
    process-wide ``_all_subclasses`` dict, and tests create them at runtime
    — ``create_variable``, and ``matlab_registry`` surrogates via
    ``scimatlab.bridge.register_matlab_variable``. Nothing used to remove
    them, so a name registered in one file leaked into every later one:
    ``test_matlab.py``'s ``materialize_variable_stubs(["StepLength", …])``
    made ``test_target_file_service.py``'s ``create_variable("StepLength")``
    fail with "already exists" — but only when the full suite ran, never
    when that file ran alone.

    Mutated in place rather than rebound: callers hold the dict itself.
    """
    global _baseline_variable_subclasses
    if _baseline_variable_subclasses is None:
        _baseline_variable_subclasses = dict(BaseVariable._all_subclasses)
        return
    BaseVariable._all_subclasses.clear()
    BaseVariable._all_subclasses.update(_baseline_variable_subclasses)


def _reset_matlab_registry() -> None:
    """Drop MATLAB registry state between tests.

    ``matlab_registry`` keeps its own ``_config`` and name dicts, and
    registers PathInputs/Parameters into the SHARED ``registry`` dicts this
    fixture already clears. Leaving its ``_config`` behind means any later
    code path that re-scans (``target_file_service._refresh_registries``
    now does, so an entity edit no longer deletes MATLAB entities from the
    registry) re-registers a previous test's tmp_path project.
    """
    _matlab_registry._matlab_functions.clear()
    _matlab_registry._matlab_variables.clear()
    _matlab_registry._matlab_path_inputs.clear()
    _matlab_registry._matlab_parameters.clear()
    _matlab_registry._load_errors.clear()
    _matlab_registry._config = None


@pytest.fixture(autouse=True)
def clear_db_state():
    """
    Reset all module-level singletons between tests so no state leaks.
    Runs before (via yield) and after each test.
    """
    # Pre-test: clear everything
    _reset_variable_subclasses()
    if hasattr(_local, "database"):
        delattr(_local, "database")
    _gui_db._db = None
    _gui_db._db_path = None
    # Connection-lifecycle state travels with _db and must be reset with it.
    # A leaked _external_holder in particular would make every later test's
    # acquire_db_connection raise DatabaseLockedError (see db.py).
    _gui_db._db_open = False
    _gui_db._db_refcount = 0
    _gui_db._external_holder = None
    _scifor.set_schema([])
    # Keep only the test functions registered across tests
    _registry._functions.clear()
    _registry._parameters.clear()
    _registry._parameter_sources.clear()
    _registry._path_inputs.clear()
    _registry._path_input_sources.clear()
    # Every sibling *_sources dict above is cleared; this one was not, so a
    # variable attributed in one test stayed attributed in the next and could
    # make an unrelated registration look like a second declaration of the
    # same name (registry._register_variable's shadowing warning).
    _registry._variable_sources.clear()
    _registry._config = None
    _registry._module_path = None
    _reset_matlab_registry()
    from scidb.pipeline import _reset_pipeline_state as _reset_pipelines

    _reset_pipelines()

    yield

    # Post-test: clean up again
    _reset_variable_subclasses()
    if hasattr(_local, "database"):
        delattr(_local, "database")
    _gui_db._db = None
    _gui_db._db_path = None
    # Connection-lifecycle state travels with _db and must be reset with it.
    # A leaked _external_holder in particular would make every later test's
    # acquire_db_connection raise DatabaseLockedError (see db.py).
    _gui_db._db_open = False
    _gui_db._db_refcount = 0
    _gui_db._external_holder = None
    _scifor.set_schema([])
    _registry._functions.clear()
    _registry._parameters.clear()
    _registry._parameter_sources.clear()
    _registry._path_inputs.clear()
    _registry._path_input_sources.clear()
    # Every sibling *_sources dict above is cleared; this one was not, so a
    # variable attributed in one test stayed attributed in the next and could
    # make an unrelated registration look like a second declaration of the
    # same name (registry._register_variable's shadowing warning).
    _registry._variable_sources.clear()
    _registry._config = None
    _registry._module_path = None
    _reset_matlab_registry()
    _reset_pipelines()


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


@pytest.fixture(autouse=True)
def _pin_project_root(tmp_path):
    """Pin the project root to tmp_path for every GUI test.

    ``config.resolve_project_root`` answers "the folder the user opened":
    ``--project-root`` if set, else the working directory. Under pytest the
    working directory is the repo, so without this pin any test that creates
    a project would write scistack.toml and an entities file into the source
    tree. See ``.claude/plan-entities-toml-26-08-31.md`` D5.

    Since the root now also decides where config is READ from and where
    folder-scan discovery walks (``.claude/plan-unify-project-root.md``),
    this pin is what makes ``tmp_path`` behave as the project in every test,
    not just those that write files.
    """
    from scistack_gui.config import set_project_root_hint

    set_project_root_hint(tmp_path)
    yield
    set_project_root_hint(None)


@pytest.fixture
def client(populated_db):
    """FastAPI TestClient backed by the populated database."""
    # Register the test function so /api/registry and /api/run can see it
    _registry._functions["bandpass_filter"] = bandpass_filter
    app = create_app()
    with TestClient(app) as c:
        yield c


@pytest.fixture
def client_with_variable_file(client, tmp_path):
    """``client``, plus a writable ``_registry._module_path`` so
    create_path_input/create_parameter (which append a new NAME = PathInput(...)/
    Parameter(...) declaration and then re-import the file — see
    path_input_service.py) have somewhere to write. Single-file (legacy)
    mode: ``_config`` stays None, so ``_target_file()`` falls back to
    ``_module_path`` — matches how a real loose-script project without a
    pyproject.toml/scistack.toml would be configured.
    """
    target = tmp_path / "pipeline_vars.py"
    target.write_text("from scidb import EachOf, Parameter, PathInput\n")
    _registry._module_path = target
    yield client


@pytest.fixture
def glue_project(client, tmp_path):
    """``client``, plus a config-mode project so glue nodes have somewhere
    to be written.

    Glue is the GUI's SECOND writable surface (``glue_dir``, beside
    ``entities_file``); ``glue_service`` auto-creates and registers it on the
    first write, exactly as ``target_file_service`` does for the entities
    file. That auto-create needs a loose-script project — a config-less
    single-file setup has no scistack.toml to record the directory in.
    """
    from scistack_gui import registry as _reg
    from scistack_gui.config import load_config

    (tmp_path / "scistack.toml").write_text("modules = []\n")
    _reg.load_from_config(load_config(None, tmp_path / "test.duckdb"))
    yield tmp_path


@pytest.fixture
def bp_node_id(populated_db):
    """The composite ``fn__bandpass_filter__{wiring_id}`` ID for the seeded
    bandpass node.  Canvas nodes group call sites by WIRING (fn + loadable
    inputs + outputs — constants excluded), so the node id suffix is the
    wiring_id, not any single call site's call_id."""
    from scistack_gui.domain.graph_builder import wiring_id
    from scistack_gui.ids import fn_node_id

    wid = wiring_id("bandpass_filter", {"signal": "RawSignal"}, {"FilteredSignal"}, {})
    return fn_node_id("bandpass_filter", wid)


def find_fn_node_id_by_label(nodes, label: str) -> str:
    """Find a function node ID by its display label.

    Useful for tests that want to assert against the seeded call site
    without depending on the exact call_id.  Asserts a unique match —
    multiple matches would mean the test exercises multiple call sites
    and should target them explicitly.
    """
    matches = [
        n["id"]
        for n in nodes
        if n.get("type") == "functionNode" and n.get("data", {}).get("label") == label
    ]
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
      - The worst state (red < pending < green) across all matching nodes.
    """
    _ORDER = {"red": 0, "pending": 1, "green": 2}
    states = [
        n["data"].get("run_state")
        for n in nodes
        if n.get("type") == "functionNode" and n.get("data", {}).get("label") == label
    ]
    states = [s for s in states if s is not None]
    if not states:
        return None
    return min(states, key=lambda s: _ORDER.get(s, 0))
