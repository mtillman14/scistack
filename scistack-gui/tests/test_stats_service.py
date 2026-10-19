"""StatStudio's backend adaptation (services/stats_service.py, api/stats.py).

The statistics are tested where they live (scistackstats, scistackstatsdb).
These check the GUI wiring: one round trip per tab, JSON-safe replies, the
plot link (divergence, sync, brackets) and that derived bars reach a resolve
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
    opened = stats_service.open_stats(db, seed=_plot(), plot_id="plot-1")
    assert opened["ok"] and opened["seed_notes"]
    return opened["spec"]


def test_handlers_are_registered():
    names = {h.name for h in ALL_HANDLERS}
    assert {"stats_open", "stats_compute", "stats_export", "stats_sync", "stats_divergence",
            "stats_overlay_bars", "stats_saved_save", "stats_add_to_pipeline"} <= names


def test_open_from_a_figure_links_and_seeds(populated_db):
    spec = _seeded(populated_db)
    assert spec["linked_plot"] == "plot-1"
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


def test_divergence_and_sync_both_ways(populated_db):
    stats = _seeded(populated_db)
    plot = {**_plot(), "roles": {"session": "iterate", "subject": "collapse"}, "groups": []}
    diff = stats_service.divergence(stats, plot)
    assert diff["diverged"] and {d["path"] for d in diff["differences"]} >= {"roles", "groups"}
    to_stats = stats_service.sync(stats, plot, "to_stats")
    assert stats_service.divergence(to_stats["stats_spec"], plot)["diverged"] is False
    to_plot = stats_service.sync(stats, plot, "to_plot")
    assert stats_service.divergence(stats, to_plot["plot_spec"])["diverged"] is False


def test_saved_tables_and_the_plot_link(populated_db):
    spec = _seeded(populated_db)
    saved = stats_service.saved_save(populated_db, "Table 1", spec, overwrite=True, current_stats_id=None)
    assert saved["ok"]
    assert [i["name"] for i in stats_service.saved_list(populated_db)["items"]] == ["Table 1"]
    assert [i["name"] for i in stats_service.linked(populated_db, "plot-1")["items"]] == ["Table 1"]
    reopened = stats_service.open_stats(populated_db, stats_id=saved["info"]["stats_id"])
    assert reopened["spec"] == spec


def test_brackets_are_off_unless_asked_for(populated_db):
    spec = _seeded(populated_db)
    assert stats_service.overlay_bars(populated_db, spec, _plot())["reason"] == "test brackets are off"
    spec["overlay"] = {"show": True, "pairs": "all"}
    out = stats_service.overlay_bars(populated_db, spec, _plot())
    assert out["reason"] is None and len(out["bars"]) == 1
    assert out["bars"][0]["origin"] == "test"


def test_derived_bars_join_a_request_but_not_the_spec():
    plot = _plot()
    bar = {"match": {}, "a": {"session": "pre"}, "b": {"session": "post"}, "label": "*", "origin": "test"}
    merged = _with_derived(plot, [bar])
    assert merged["difference_bars"][0]["origin"] == "test"
    assert "difference_bars" not in plot
    assert _with_derived(plot, None) is plot


def test_compute_carries_the_vocabulary(populated_db):
    catalog = stats_service.compute(populated_db, _seeded(populated_db))["catalog"]
    assert {"mean_sd", "median_iqr"} <= {c["id"] for c in catalog["cells"]}
    assert "holm" in {c["id"] for c in catalog["corrections"]}
    assert any(e["name"] == "Cohen's d_z" for e in catalog["effect_sizes"])
