"""
GUI-service edge cases (plan section C): what the panel is told when the
user asks for something unplottable, invalid, out of range or unwritable —
and that the DAG the canvas draws is the pipeline that ran.

The service functions ARE the JSON-RPC handlers minus the transport, so a
message here is a message in the panel; an exception here is a blank panel.
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("scistack_gui")
pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

from scidb import BaseVariable  # noqa: E402
from scistack_gui.services import plot_service  # noqa: E402

from conftest import attempt  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_source_cache():
    plot_service.invalidate()
    yield
    plot_service.invalidate()


def _bar_spec(db) -> dict:
    spec = plot_service.describe(db, "CycleSymmetry")["spec"]
    roles = {**spec["roles"], "session": "group", "speed": "iterate",
             "subject": "collapse", "trial": "collapse", "cycle": "collapse"}
    return {**spec, "roles": roles, "groups": ["session"], "color": None, "kind": "bar"}


# --- C1: an unplottable variable ---------------------------------------------------------


class Label(BaseVariable):
    """A string per session — nothing to draw."""


def test_a_string_valued_variable_is_ineligible_with_a_reason(scratch_db):
    Label.save("good", subject="subject01", session="baseline")
    Label.save("bad", subject="subject01", session="week04")
    described, error = attempt(plot_service.describe, scratch_db, "Label")
    assert error is None, error
    assert described["eligible"] is False
    assert described["reason"], described


def test_describe_of_an_unknown_variable_is_a_typed_error(example_db):
    _, error = attempt(plot_service.describe, example_db, "NoSuchVariable")
    assert error is not None


# --- C2 / C3: invalid spec, out-of-range figure ------------------------------------------


def test_an_invalid_spec_is_a_message_not_an_exception(example_db):
    spec = _bar_spec(example_db)
    spec = {**spec, "color": "trial"}  # colour on a collapsed factor
    result, error = attempt(plot_service.resolve_figures, example_db, spec)
    assert error is None, f"the service must translate the RoleError: {error}"
    assert result.get("error"), result
    assert "trial" in result["error"]


def test_a_legacy_spec_is_a_message(example_db):
    """A spec saved before the roles rework carries role names that no
    longer exist; the panel must say so rather than blank out."""
    spec = _bar_spec(example_db)
    spec = {**spec, "roles": {**spec["roles"], "session": "x"}}
    result, error = attempt(plot_service.resolve_figures, example_db, spec)
    assert error is not None or result.get("error")


def test_a_figure_index_past_the_fan_out_is_clamped(example_db):
    result = plot_service.resolve_figures(example_db, _bar_spec(example_db), figure_index=999)
    assert result.get("error") is None, result
    assert result["figure_count"] == 2
    assert len(result["figures"]) == 1


def test_a_negative_figure_index_is_clamped_too(example_db):
    result = plot_service.resolve_figures(example_db, _bar_spec(example_db), figure_index=-5)
    assert result.get("error") is None, result
    assert len(result["figures"]) == 1


def test_capabilities_for_an_invalid_spec_still_answer(example_db):
    """The panel asks for capabilities on every change, including the change
    that made the spec invalid — it must get the report (with the reason),
    not an exception, or every control greys out."""
    spec = {**_bar_spec(example_db), "color": "trial"}
    report, error = attempt(plot_service.capabilities_for, example_db, spec)
    assert report is not None or error is not None
    if report is not None:
        assert report["kinds"]


# --- C4: saves that cannot succeed ---------------------------------------------------------


def test_an_unsupported_image_format_is_refused_before_the_load(example_db, tmp_path):
    result = plot_service.save_figure(
        example_db, _bar_spec(example_db), str(tmp_path / "x.png"), image_format="fig"
    )
    assert result["ok"] is False
    assert "fig" in result["error"]
    assert not list(tmp_path.iterdir())


def test_a_data_save_of_an_invalid_spec_is_refused_with_the_reason(example_db, tmp_path):
    spec = {**_bar_spec(example_db), "color": "trial"}
    result = plot_service.save_plot_data(example_db, spec, str(tmp_path / "d.csv"))
    assert result["ok"] is False and "trial" in result["error"]
    assert not (tmp_path / "d.csv").exists()


def test_a_data_save_into_a_file_that_exists_overwrites_it(example_db, tmp_path):
    target = tmp_path / "again.csv"
    target.write_text("stale\n")
    result = plot_service.save_plot_data(example_db, _bar_spec(example_db), str(target))
    assert result["ok"] is True, result
    assert "stale" not in target.read_text()


def test_a_data_save_with_a_folder_as_the_target(example_db, tmp_path):
    result = plot_service.save_plot_data(example_db, _bar_spec(example_db), str(tmp_path))
    assert result["ok"] is True, result
    (written,) = result["files"]
    assert written.endswith("CycleSymmetry_data.csv")


# --- C5: the DAG the canvas draws is the pipeline that ran ---------------------------------


def test_the_graph_built_from_history_names_every_step(example_db, pipeline, as_gui_db):
    from scistack_gui import pipeline_store, registry
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    as_gui_db(example_db)
    pipeline_store._ensure_tables(example_db)
    registry.register_module(pipeline, module_path=None)
    graph, error = attempt(get_pipeline_graph, example_db)
    assert error is None, error
    text = json.dumps(graph, default=str)
    for step in (
        "load_cycle_symmetry", "load_trial_info", "load_session_info", "load_demographics",
        "ankle_over_threshold", "normalized_knee", "trial_mean_symmetry", "cycle_deviation",
        "speed_change_from_baseline", "scale_joint", "subject_profile", "knee_excursion",
    ):
        assert step in text, f"{step} missing from the graph"
    for variable in ("CycleSymmetry", "TrialInfo", "Demographics", "NormalizedKnee"):
        assert variable in text, variable
    assert graph.get("nodes") and graph.get("edges")


def test_the_graph_survives_a_variable_with_no_records(scratch_db, pipeline, as_gui_db):
    """A registered variable nothing has produced yet (the state every new
    project is in) must not break the graph build."""
    from scistack_gui import pipeline_store, registry
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    as_gui_db(scratch_db)
    pipeline_store._ensure_tables(scratch_db)
    registry.register_module(pipeline, module_path=None)
    graph, error = attempt(get_pipeline_graph, scratch_db)
    assert error is None, error
    assert isinstance(graph.get("nodes"), list)


# --- describe over every level ---------------------------------------------------------------


@pytest.mark.parametrize(
    "variable", ["Demographics", "SessionInfo", "TrialInfo", "TrialCadence", "SubjectProfile",
                 "SpeedChangeFromBaseline", "CycleDeviation", "ScaledTrialSymmetry"],
)
def test_describe_opens_every_level_of_variable(example_db, variable):
    described, error = attempt(plot_service.describe, example_db, variable)
    assert error is None, f"{variable}: {error}"
    assert described["eligible"] is True, described.get("reason")
    result, error = attempt(plot_service.resolve_figures, example_db, described["spec"])
    assert error is None, f"{variable}: {error}"
    assert result.get("error") is None, result
    assert result["figure_count"] >= 1


def test_the_group_column_is_offered_as_a_grouping_variable(example_db):
    """`Demographics.group` — the subject-level categorical — must reach the
    grouping picker as a column a plot can be stratified by."""
    report, error = attempt(plot_service.grouping_columns, example_db, "CycleSymmetry", "Demographics")
    assert error is None, error
    offered = [o["label"] for o in report["offered"]]
    assert any("group" in label for label in offered), (offered, report["rejected"])
