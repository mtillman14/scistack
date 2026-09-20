"""
Cross-layer integration tests over the ``examples/aim2`` gait symmetry dataset.

One database, built ONCE per session by running the example pipeline exactly
as a user would (``examples/aim2/src/cycles/run_pipeline.py``), then read by
every layer in turn: scifor/scidb (the records), sciduckdb (what was stored),
scistackplot(+db) (what is drawn) and scistack-gui (what the panel is told).
Plan: ``.claude/plan-example-integration-tests.md``.

Run on its own — never in the same pytest invocation as a package suite:

    pytest tests/integration -q
    SCISTACK_INTEGRATION_FULL=1 pytest tests/integration -q

The default is a SUBSET (two subjects, two sessions): the level counts below
are derived from it, so every assertion holds in either mode.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("scidb")
pytest.importorskip("pandas")

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_ROOT = REPO_ROOT / "examples" / "aim2"
DATA_ROOT = EXAMPLE_ROOT / "data"
PIPELINE_DIR = EXAMPLE_ROOT / "src" / "cycles"

FULL = os.environ.get("SCISTACK_INTEGRATION_FULL") == "1"

ALL_SUBJECTS = ["subject01", "subject02", "subject03"]
ALL_SESSIONS = ["baseline", "week04", "week12", "week24"]
SUBJECTS = ALL_SUBJECTS if FULL else ALL_SUBJECTS[:2]
SESSIONS = ALL_SESSIONS if FULL else ALL_SESSIONS[:2]
SPEEDS = ["slow", "fast"]
TRIALS = ["01", "02", "03"]
CYCLES = [f"{i:02d}" for i in range(1, 11)]
JOINTS = ["ankle", "knee", "hip"]

#: Expected record counts per level, for the selected subset.
N_SUBJECTS = len(SUBJECTS)
N_SESSIONS = N_SUBJECTS * len(SESSIONS)
N_TRIALS = N_SESSIONS * len(SPEEDS) * len(TRIALS)
N_CYCLES = N_TRIALS * len(CYCLES)


def _import(name: str, path: Path):
    """Import an example script by path. The example is a plain folder of
    scripts, not a package, so ``run_pipeline``'s ``from pipeline import``
    needs the folder on ``sys.path`` — exactly as running it does."""
    if str(PIPELINE_DIR) not in sys.path:
        sys.path.insert(0, str(PIPELINE_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def pipeline():
    """The example's ``pipeline`` module: its variables, entities, functions."""
    return _import("pipeline", PIPELINE_DIR / "pipeline.py")


@pytest.fixture(scope="session")
def example_db(tmp_path_factory, pipeline):
    """The database after the whole example pipeline has run over the subset.

    Session-scoped: the run is the expensive part and every test reads the
    same result. Tests must not write to it.
    """
    import scidb

    run = _import("run_pipeline", PIPELINE_DIR / "run_pipeline.py")
    db_path = tmp_path_factory.mktemp("integration") / "aim2.duckdb"
    run.main(db_path=db_path, subjects=SUBJECTS, sessions=SESSIONS)
    db = scidb.get_database()
    yield db
    db.close()


def cell(data, column: str):
    """One column's value out of a stored record's ``data``, whatever shape
    it came back in.

    A record that was saved as a one-row DataFrame loads as a DataFrame; one
    saved as a dict (``multi_column`` storage) loads as a dict. Both are
    legitimate ways the example stores a table, and a test about the
    NUMBERS must not also be a test about which shape scidb chose.
    """
    import pandas as pd

    if isinstance(data, pd.DataFrame):
        value = data[column].iloc[0]
    elif isinstance(data, pd.Series):
        value = data[column]
    else:
        value = data[column]
    return value


def columns_of(data) -> list[str]:
    import pandas as pd

    if isinstance(data, (pd.DataFrame, pd.Series)):
        return [str(c) for c in (data.columns if isinstance(data, pd.DataFrame) else data.index)]
    return [str(k) for k in data.keys()]


# ---------------------------------------------------------------------------
# Edge-case support (plan: .claude/plan-edge-case-integration-tests.md)
# ---------------------------------------------------------------------------

#: The errors a layer is ALLOWED to raise at the GUI's boundary — each one is
#: translated into a message by the layer above. Anything else is a bug.
def typed_errors() -> tuple:
    import scidb
    from scifor.foreach import NoDataError

    return (scidb.SciStackError, NoDataError, ValueError)


def attempt(fn, *args, **kwargs):
    """Run ``fn`` under the GUI contract: ``(result, None)`` on success,
    ``(None, error)`` for a TYPED error, and a failure — naming the exception
    and the call — for anything else (a KeyError out of pandas, an
    IndexError, an AttributeError: the shapes a GUI shows as a blank panel).
    """
    try:
        return fn(*args, **kwargs), None
    except typed_errors() as error:
        assert str(error).strip(), f"{type(error).__name__} raised with no message"
        return None, error
    except Exception as error:  # noqa: BLE001 — the point is to name it
        raise AssertionError(
            f"untyped {type(error).__name__} from {getattr(fn, '__name__', fn)}: {error}"
        ) from error


def run_summary(for_each_fn, *args, **kwargs):
    """``(result, summary)`` of a ``for_each`` call, the summary being the
    scifor progress event (``completed`` / ``failed`` / ``no_data`` /
    ``failure_reasons``) — so a test can say WHY a combination did not run
    instead of only that it did not."""
    summary: dict = {}

    def progress(event):
        if event.get("event") == "summary":
            summary.update(event)

    result = for_each_fn(*args, _progress_fn=progress, **kwargs)
    return result, summary


@pytest.fixture
def scratch_db(tmp_path, example_db):
    """A fresh, empty database with the example's schema, swapped in as the
    global one for the test and swapped back out afterwards.

    For scenarios that WRITE — a step run before its loader, a changed
    parameter, a direct save, a stray key spelling. They must not touch the
    shared session database the read-only tests count on, and DuckDB will not
    open one file twice, so this swaps the global manager rather than opening
    a second handle to the shared file.

    Depends on ``example_db`` so the global manager exists to swap back to,
    whichever test runs first.
    """
    import scidb
    import scifor as _scifor

    previous = scidb.get_database()
    db = scidb.configure_database(
        tmp_path / "scratch.duckdb", ["subject", "session", "speed", "trial", "cycle"]
    )
    try:
        yield db
    finally:
        db.close()
        previous.set_current_db()
        _scifor.set_schema(list(previous.dataset_schema_keys))


def small_subset() -> dict:
    """The keys to run a scratch pipeline over: one subject, one session —
    the smallest slice that still has every level."""
    return {"subjects": SUBJECTS[:1], "sessions": SESSIONS[:1]}


@pytest.fixture
def as_gui_db():
    """Point the GUI's module-level database at a manager for one test.

    The graph build and the run service read ``scistack_gui.db.get_db()``
    rather than taking a manager, because the real process has exactly one;
    the GUI's own conftest sets it the same way.
    """
    import scistack_gui.db as gui_db

    saved = (gui_db._db, gui_db._db_path)

    def install(db):
        gui_db._db = db
        gui_db._db_path = getattr(db, "dataset_db_path", None)
        return db

    yield install
    gui_db._db, gui_db._db_path = saved
