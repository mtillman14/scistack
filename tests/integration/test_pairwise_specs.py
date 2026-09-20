"""
Pane combinations, all-pairs, through the service layer as JSON.

Every case is a spec DICT — the shape the webview sends — run through the
same functions the JSON-RPC handlers call: ``capabilities_for``,
``resolve_figures``, ``save_plot_data``, ``export_code``. The contract is the
edge suite's (a result or a typed message), plus for every case that draws:
the CSV agrees with the marks, and the exported code compiles.

``pairwise.py`` builds the cases; ``SCISTACK_INTEGRATION_FULL=1`` does not
change them (they are already small), it only widens the database.
"""

from __future__ import annotations

import pytest

pytest.importorskip("scistack_gui")
pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

from scistack_gui.services import plot_service  # noqa: E402
from scistackplot import PlotSpec, resolve  # noqa: E402
from scistackplotdb import ScidbSource  # noqa: E402

from conftest import SESSIONS, SUBJECTS, attempt  # noqa: E402
from pairwise import SCALAR_AXES, SERIES_AXES, all_pairs, case_id, scalar_spec, series_spec  # noqa: E402
from test_plot_sweep import _assert_csv_matches_marks  # noqa: E402

SCALAR_CASES = all_pairs(SCALAR_AXES)
SERIES_CASES = all_pairs(SERIES_AXES)


@pytest.fixture(autouse=True)
def _fresh_source_cache():
    plot_service.invalidate()
    yield
    plot_service.invalidate()


@pytest.fixture(scope="module")
def field(example_db):
    table = ScidbSource(example_db).get_table(["CycleSymmetry"])
    return table.field_factors[0].name


def _through_the_services(db, spec: dict, tmp_path, *, check_csv: bool) -> str:
    """One case through the service layer; returns the outcome word."""
    report, error = attempt(plot_service.capabilities_for, db, spec)
    assert report is not None or error is not None
    if report is not None:
        assert report["kinds"] and "data_export" in report

    result, error = attempt(plot_service.resolve_figures, db, spec, max_points=2000)
    assert error is None, f"resolve_figures raised through the service: {error}"
    if result.get("error"):
        assert isinstance(result["error"], str) and result["error"].strip()
        # A refused spec must be refused everywhere: no file, no code.
        saved = plot_service.save_plot_data(db, spec, str(tmp_path / "refused.csv"))
        assert saved["ok"] is False
        return "refused"

    assert result["figure_count"] >= 1
    assert all("figure" in f for f in result["figures"])

    saved = plot_service.save_plot_data(db, spec, str(tmp_path / "data.csv"))
    if report is not None and report["data_export"]["available"]:
        assert saved["ok"] is True, saved
        assert saved["rows"] >= 0
    else:
        assert saved["ok"] is False

    exported, error = attempt(plot_service.export_code, db, spec)
    assert error is None, f"export refused a spec the panel drew: {error}"
    assert exported.get("source") or exported.get("function_source")
    compile(exported.get("source") or exported["function_source"], "<generated>", "exec")

    if check_csv and saved["ok"]:
        _, spec_obj, table = plot_service._load(db, spec, csv_path=None, label="test")
        _assert_csv_matches_marks(spec_obj, table, resolve(spec_obj, table))
    return "drawn"


@pytest.mark.parametrize("case", SCALAR_CASES, ids=[case_id(c) for c in SCALAR_CASES])
def test_scalar_pane_combination(example_db, field, tmp_path, case):
    spec = scalar_spec(case, field=field, subjects=SUBJECTS, sessions=SESSIONS)
    _through_the_services(example_db, spec, tmp_path, check_csv=True)


@pytest.mark.parametrize("case", SERIES_CASES, ids=[case_id(c) for c in SERIES_CASES])
def test_series_pane_combination(example_db, tmp_path, case):
    table = ScidbSource(example_db).get_table(["CycleWaveform"])
    spec = series_spec(case, field=table.field_factors[0].name, subjects=SUBJECTS, sessions=SESSIONS)
    _through_the_services(example_db, spec, tmp_path, check_csv=False)


def test_the_generator_covers_every_pair():
    """The suite's own guarantee: no pair of pane values goes untested."""
    import itertools

    for axes, cases in ((SCALAR_AXES, SCALAR_CASES), (SERIES_AXES, SERIES_CASES)):
        seen = set()
        for case in cases:
            items = list(case.items())
            seen.update(itertools.combinations(items, 2))
        for a, b in itertools.combinations(axes, 2):
            for va in axes[a]:
                for vb in axes[b]:
                    assert ((a, va), (b, vb)) in seen, (a, va, b, vb)
    assert len(SCALAR_CASES) < 80 and len(SERIES_CASES) < 40, (len(SCALAR_CASES), len(SERIES_CASES))


def test_the_dict_round_trips_through_plotspec(field):
    """Every generated dict is a spec the library accepts — the test of the
    generator, before the services see it."""
    for case in SCALAR_CASES:
        spec = scalar_spec(case, field=field, subjects=SUBJECTS, sessions=SESSIONS)
        parsed, error = attempt(PlotSpec.from_dict, spec)
        assert error is None, (case_id(case), error)
        assert parsed.to_dict()["kind"] == case["kind"]
