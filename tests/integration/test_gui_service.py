"""
scistack-gui: the Plot Studio's backend over the example database.

Same functions the JSON-RPC handlers call, without the transport — so this is
what the panel is told when it opens one of the example's variables, and
what "Save data (CSV)" writes for it.
"""

from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("scistack_gui")
pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

from scistack_gui.services import plot_service  # noqa: E402

from conftest import JOINTS, N_CYCLES, SESSIONS, SUBJECTS  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_source_cache():
    plot_service.invalidate()
    yield
    plot_service.invalidate()


def _with_roles(spec: dict, **roles: str) -> dict:
    merged = {**spec["roles"], **roles}
    groups = [n for n in (spec.get("groups") or []) if merged.get(n) == "group"]
    for name, role in roles.items():
        if role == "group" and name not in groups:
            groups.insert(0, name)
    color = spec.get("color")
    if color is not None and merged.get(color) != "group":
        color = None
    return {**spec, "roles": merged, "groups": groups, "color": color}


def test_describe_lists_every_variable_the_pipeline_made(pipeline, example_db):
    catalog = plot_service.describe(example_db)["catalog"]
    names = {entry["name"] for entry in catalog["measures"]}
    for variable in ("CycleSymmetry", "CycleWaveform", "TrialMeanSymmetry", "Demographics"):
        assert variable in names, sorted(names)


def test_describe_opens_the_struct_variable_as_scalar_panels(pipeline, example_db):
    described = plot_service.describe(example_db, "CycleSymmetry")
    assert described["eligible"] is True, described.get("reason")
    assert described["spec"]["measures"] == ["CycleSymmetry"]
    assert described["capabilities"]["shape"] == "scalar"


def test_describe_opens_the_1d_variable_as_1d(pipeline, example_db):
    described = plot_service.describe(example_db, "CycleWaveform")
    assert described["eligible"] is True, described.get("reason")
    assert described["capabilities"]["shape"] == "1d"
    assert described["capabilities"]["data_export"]["available"] is False


def _users_example_spec(db) -> dict:
    spec = plot_service.describe(db, "CycleSymmetry")["spec"]
    spec = _with_roles(
        spec, session="group", speed="iterate", subject="collapse", trial="collapse", cycle="collapse"
    )
    spec["kind"] = "bar"
    return spec


def test_capabilities_offer_the_csv_with_three_depths(pipeline, example_db):
    report = plot_service.capabilities_for(example_db, _users_example_spec(example_db))
    export = report["data_export"]
    assert export["available"] is True
    assert [d["key"] for d in export["depths"]] == ["subject", "trial", "cycle"]
    assert export["field_factor"], "the joints can be spread one column each"


def test_resolve_figures_fans_out_one_figure_per_speed(pipeline, example_db):
    result = plot_service.resolve_figures(example_db, _users_example_spec(example_db))
    assert result.get("error") is None, result
    assert result["figure_count"] == 2
    assert all(f["row_count"] > 0 for f in result["figures"])


def test_save_plot_data_writes_the_wide_csv(pipeline, example_db, tmp_path):
    target = tmp_path / "symmetry.csv"
    result = plot_service.save_plot_data(example_db, _users_example_spec(example_db), str(target))
    assert result["ok"] is True, result
    written = pd.read_csv(target, dtype={"subject": str, "trial": str, "cycle": str})
    assert list(written.columns) == ["subject", "session", "speed", *JOINTS]
    assert len(written) == len(SUBJECTS) * len(SESSIONS) * 2
    assert set(written["subject"]) == set(SUBJECTS), "zero-padded ids survived the file"


def test_save_plot_data_long_form_at_the_raw_depth(pipeline, example_db, tmp_path):
    target = tmp_path / "raw.csv"
    result = plot_service.save_plot_data(
        example_db, _users_example_spec(example_db), str(target),
        depth="cycle", fields_as_columns=False,
    )
    assert result["ok"] is True, result
    written = pd.read_csv(target, dtype=str)
    assert len(written) == N_CYCLES * len(JOINTS)
    assert "cycle" in written.columns and "01" in set(written["cycle"])
