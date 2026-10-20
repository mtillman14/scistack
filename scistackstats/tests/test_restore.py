"""Opening stored stats specs: salvage, never fail; stats presets (Stage 9)."""

from __future__ import annotations

from conftest import bar_spec
from scistackstats import DV, Analysis, DescriptiveSpec, StatsSpec
from scistackstats.restore import STATS_SETTINGS, restore_stats_spec, stats_settings, with_stats_settings
from scistackstats.spec import FormatSpec


def _spec() -> StatsSpec:
    return StatsSpec(
        design=bar_spec(), dvs=(DV("Ratings"),), linked_plot="p1",
        descriptives=DescriptiveSpec(cell="median_iqr", statistics=("n", "iqr")),
        comparisons=(Analysis(id="a1", family="nonparametric"),),
        formatting=FormatSpec(decimals=1),
    )


def test_a_clean_spec_restores_exactly_with_no_notes():
    restored = restore_stats_spec(_spec().to_dict())
    assert restored.spec == _spec()
    assert restored.notes == []


def test_bad_settings_become_notes_and_defaults():
    raw = _spec().to_dict()
    raw["comparisons"][0]["family"] = "bayesian"
    raw["comparisons"][0]["shiny"] = True
    raw["comparisons"].append({"id": "a2", "pairwise_test": "magic_test"})
    raw["descriptives"]["statistics"] = ["n", "vibes"]
    raw["dvs"].append({"variable": ""})
    raw["formatting"] = {"decimals": 99}
    raw["unknown_top"] = 1
    restored = restore_stats_spec(raw)
    paths = {n.path for n in restored.notes}
    assert {"comparisons[0].family", "comparisons[0].shiny", "comparisons[1].pairwise_test",
            "descriptives.statistics", "dvs[1]", "formatting.decimals", "unknown_top"} <= paths
    spec = restored.spec
    assert spec.comparisons[0].family == "auto"  # the default since 2026-10-19
    assert spec.comparisons[1].pairwise_test is None, "the design chooses instead"
    assert spec.descriptives.statistics == ("n",)
    assert spec.dvs == (DV("Ratings"),)
    assert spec.formatting.decimals is None


def test_a_broken_design_is_salvaged_by_the_plot_layer():
    raw = _spec().to_dict()
    raw["design"]["kind"] = "hologram"
    restored = restore_stats_spec(raw)
    assert any(n.path.startswith("design.") for n in restored.notes)
    assert restored.spec.design.y_measure == "Ratings"


def test_not_a_table_opens_on_defaults():
    restored = restore_stats_spec("garbage", fallback_measure="Ratings")
    assert restored.spec.design.y_measure == "Ratings"
    assert restored.notes


def test_preset_carries_only_stats_settings():
    settings = stats_settings(_spec())
    assert set(settings) == set(STATS_SETTINGS)
    target = StatsSpec(design=bar_spec(measures=["Other"]), dvs=(DV("Other"),), linked_plot="p9")
    applied, notes = with_stats_settings(target, settings)
    assert notes == []
    assert applied.design == target.design and applied.dvs == target.dvs and applied.linked_plot == "p9"
    assert applied.descriptives == _spec().descriptives
    assert applied.comparisons == _spec().comparisons
    assert applied.formatting == _spec().formatting


def test_a_sweep_reopens_as_a_sweep():
    from dataclasses import replace

    swept = replace(_spec(), variant_mode="sweep")
    assert restore_stats_spec(swept.to_dict()).spec.variant_mode == "sweep"
    raw = {**swept.to_dict(), "variant_mode": "everything"}
    restored = restore_stats_spec(raw)
    assert restored.spec.variant_mode == "pinned"
    assert any(n.path == "variant_mode" for n in restored.notes)
