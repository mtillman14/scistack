"""StatStudio's backend adaptation (services/stats_service.py, api/stats.py).

The statistics are tested where they live (scistackstats, scistackstatsdb).
These check the GUI wiring: one round trip per tab, JSON-safe replies, the
plot link (one shared design, brackets) and that derived bars reach a resolve
without being stored.
"""

import json

import pytest

pytest.importorskip("scistackstats")
pytest.importorskip("scistackstatsdb")

from scistack_gui.api.plot import _with_derived
from scistack_gui.api.tables import ALL_HANDLERS
from scistack_gui.services import plot_service, stats_service


@pytest.fixture(autouse=True)
def _clear_source_cache():
    plot_service.invalidate()
    yield
    plot_service.invalidate()


def _plot() -> dict:
    return {
        "measures": ["RawSignal"],
        "roles": {"session": "group", "subject": "collapse"},
        "groups": ["session"],
        "kind": "bar",
    }


def _seeded(db) -> dict:
    opened = stats_service.open_stats(db, seed=_plot())
    assert opened["ok"] and opened["seed_notes"]
    return opened["spec"]


def test_handlers_are_registered():
    names = {h.name for h in ALL_HANDLERS}
    assert {"stats_open", "stats_compute", "stats_export", "stats_marks", "stats_saved_save",
            "stats_add_to_pipeline", "report_studio_open"} <= names
    # One tab holds one design in memory; the old sync and sharing RPCs are gone.
    assert not {"stats_sync", "stats_divergence", "design_update", "design_apply", "stats_linked"} & names


def test_open_from_a_figure_seeds(populated_db):
    spec = _seeded(populated_db)
    assert spec["descriptives"]["cell"] == "mean_sd"
    assert spec["design"]["measures"] == ["RawSignal"]


def test_compute_returns_both_tabs_as_grids(populated_db):
    result = stats_service.compute(populated_db, _seeded(populated_db))
    assert result["ok"], result
    json.dumps(result)  # crosses the webview boundary
    (desc,) = result["descriptives"]
    assert desc["caption"] and desc["header_rows"]
    (analysis,) = result["comparisons"]
    assert analysis["grid"]["rows"], "the pre/post pair"
    assert analysis["family_statement"]
    (readout,) = result["readout"]
    assert readout["design"]["factors"][0]["name"] == "session"
    assert any(t["id"] == "ttest_paired" and t["reason"] is None for t in readout["tests"])


def test_a_bad_spec_is_a_reply_not_a_crash(populated_db):
    result = stats_service.compute(populated_db, {"format": 1, "design": _plot(), "colour": 1})
    assert result["ok"] is False and "colour" in result["error"]


def test_export_text(populated_db):
    out = stats_service.export(populated_db, _seeded(populated_db), "md", "descriptives")
    assert out["ok"] and "*Note.*" in out["text"]


def test_a_figure_saves_with_its_table_settings(populated_db):
    from scistack_gui.services import saved_plot_service

    stats = {k: v for k, v in _seeded(populated_db).items() if k not in ("design", "guide")}
    saved = saved_plot_service.save(populated_db, "RawSignal", "Fig", _plot(), {}, stats=stats)
    assert saved["ok"]
    opened = saved_plot_service.open_plot(populated_db, saved["plot"]["plot_id"])["plot"]
    assert opened["stats"]["comparisons"][0]["id"] == "a1", "the table settings come back with the figure"
    assert "design" not in opened["stats"]
    assert opened["stats_notes"] == []
    plain = saved_plot_service.save(populated_db, "RawSignal", "Plain", _plot(), {})
    assert saved_plot_service.open_plot(populated_db, plain["plot"]["plot_id"])["plot"]["stats"] is None


def test_open_carries_the_table_role_words(populated_db):
    opened = stats_service.open_stats(populated_db, seed=_plot())
    assert opened["role_labels"] == {"group": "Compare", "facet": "Separate sections",
                                     "iterate": "Separate tables", "collapse": "Average over"}


def test_a_fresh_table_opens_on_the_figure_opening_design(populated_db):
    # One owner of "the default design for a variable" (plot_service.opening_spec).
    described = plot_service.describe(populated_db, "RawSignal")
    opened = stats_service.open_stats(populated_db, variable="RawSignal")
    assert opened["spec"]["design"]["roles"] == described["spec"]["roles"]


def test_saved_tables_and_the_plot_link(populated_db):
    spec = _seeded(populated_db)
    saved = stats_service.saved_save(populated_db, "Table 1", spec, overwrite=True, current_stats_id=None)
    assert saved["ok"]
    assert [i["name"] for i in stats_service.saved_list(populated_db)["items"]] == ["Table 1"]
    reopened = stats_service.open_stats(populated_db, stats_id=saved["info"]["stats_id"])
    assert reopened["spec"] == spec


def test_marks_are_off_unless_asked_for(populated_db):
    out = stats_service.stats_marks(populated_db, _plot())
    assert out["reason"] == "no test results are asked for"
    asked = {**_plot(), "stat_marks": {"brackets": True, "pairs": "all", "caption": True}}
    out = stats_service.stats_marks(populated_db, asked)
    # Works on an unsaved figure: the table is seeded from it, nothing is stored.
    assert out["reason"] is None and len(out["bars"]) == 1
    assert out["bars"][0]["origin"] == "test"
    assert out["caption"] and out["test"]
    assert stats_service.saved_list(populated_db)["items"] == []


def test_derived_facts_join_a_request_but_not_the_spec():
    plot = _plot()
    bar = {"match": {}, "a": {"session": "pre"}, "b": {"session": "post"}, "label": "*", "origin": "test"}
    facts = {"bars": [bar], "panel_notes": [{"match": {}, "text": "F", "origin": "test"}], "caption": "cap"}
    merged = _with_derived(plot, facts)
    assert merged["difference_bars"][0]["origin"] == "test"
    assert merged["derived"]["caption"] == "cap"
    assert "difference_bars" not in plot and "derived" not in plot
    assert _with_derived(plot, None) is plot


def test_compute_carries_the_vocabulary(populated_db):
    catalog = stats_service.compute(populated_db, _seeded(populated_db))["catalog"]
    assert {"mean_sd", "median_iqr"} <= {c["id"] for c in catalog["cells"]}
    assert "holm" in {c["id"] for c in catalog["corrections"]}
    assert any(e["name"] == "Cohen's d_z" for e in catalog["effect_sizes"])


# --- the Model formula popup (plan-statstudio-model-formula) -----------------


def test_formula_handlers_are_registered():
    names = {h.name for h in ALL_HANDLERS}
    assert {"stats_formula_check", "stats_formula_apply"} <= names


def test_the_formula_check_starts_from_the_default_model(populated_db):
    out = stats_service.formula_check(populated_db, _seeded(populated_db))
    assert out["ok"]
    json.dumps(out)
    (check,) = out["checks"]
    assert check["errors"] == [] and check["text"] == check["default"] == "session + (1 | subject)"
    assert check["offers"]["groups"] == ["subject"] and check["offers"]["factors"] == ["session"]


def test_a_problem_comes_back_with_its_span(populated_db):
    (check,) = stats_service.formula_check(populated_db, _seeded(populated_db),
                                           text="session * height + (1 | subject)")["checks"]
    (problem,) = check["errors"]
    assert "height" in problem["message"] and problem["position"] == 10


def test_apply_keeps_the_text_only_when_it_says_more_than_the_design(populated_db):
    spec = _seeded(populated_db)
    plain = stats_service.formula_apply(populated_db, spec, text="session + (1 | subject)")
    assert plain["ok"] and "formula" not in plain["spec"]["comparisons"][0]
    slope = stats_service.formula_apply(populated_db, spec, text="session + (1 + session | subject)")
    assert slope["ok"] and slope["spec"]["comparisons"][0]["formula"] == "session + (1 + session | subject)"
    assert slope["design_changed"] is False


def test_apply_refuses_a_formula_with_problems_and_changes_nothing(populated_db):
    out = stats_service.formula_apply(populated_db, _seeded(populated_db), text="session + (1 | nobody)")
    assert out["ok"] is False and "nobody" in out["errors"][0]["message"]


def test_a_random_slope_the_rows_cannot_support_is_refused_with_lme4s_reason(populated_db):
    """2 subjects x 2 sessions: a session slope per subject is 4 random
    effects for 4 rows; csv-stats refuses it before fitting (decision 3)."""
    spec = stats_service.formula_apply(populated_db, _seeded(populated_db),
                                       text="session + (1 + session | subject)")["spec"]
    result = stats_service.compute(populated_db, spec)
    assert result["ok"], result
    (analysis,) = result["comparisons"]
    (stratum,) = analysis["strata"]
    assert stratum["model"] == "RawSignal ~ session + (1 + session | subject)"
    assert stratum["selection"]["pairwise"] == "lmm_pairwise"
    (readout,) = result["readout"]
    assert readout["model"]["text"] == "session + (1 + session | subject)"
    assert readout["model"]["needs_mixed_model"] is True
    grid_text = json.dumps(analysis["grid"])
    assert "number of observations" in grid_text


def test_the_covariate_picker_reuses_the_grouping_rpcs(populated_db):
    report = plot_service.grouping_graph(populated_db, "RawSignal", purpose="covariate")
    assert set(report) == {"offered", "rejected"}
    assert all(o["kind"] != "categorical" for o in report["offered"])
    with pytest.raises(ValueError, match="purpose"):
        plot_service.grouping_graph(populated_db, "RawSignal", purpose="colour")


# --- the step-by-step wizard (plan-statstudio-guided) ---------------------------


def test_preview_returns_the_guide_without_running_a_test(populated_db, monkeypatch):
    from scistackstats import GUIDE_STEPS

    def no_engine():
        raise AssertionError("stats_preview must not reach csv-stats")

    # The modules themselves: the dotted string "scistackstats.run" resolves
    # to the package's exported run() FUNCTION, which shadows the module.
    import importlib

    monkeypatch.setattr(importlib.import_module("scistackstats.engine"), "get_engine", no_engine)
    monkeypatch.setattr(importlib.import_module("scistackstats.run"), "get_engine", no_engine)
    result = stats_service.preview(populated_db, _seeded(populated_db))
    assert result["ok"], result
    json.dumps(result)
    guide = result["guide"]
    assert [s["id"] for s in guide["steps"]] == list(GUIDE_STEPS)
    assert guide["has_results"] is False
    assert "unit" in guide["concepts"]


def test_compute_carries_the_guide_with_results(populated_db):
    result = stats_service.compute(populated_db, _seeded(populated_db))
    guide = result["guide"]
    assert guide["has_results"] is True
    test_step = next(s for s in guide["steps"] if s["id"] == "test")
    assert test_step["cards"], "the test choices need the run's recommendation"


def test_a_seeded_table_opens_in_the_wizard(populated_db):
    assert _seeded(populated_db)["guide"]["mode"] == "guided"


def test_preview_handler_is_registered():
    assert {"stats_preview", "stats_guide_log"} <= {h.name for h in ALL_HANDLERS}
