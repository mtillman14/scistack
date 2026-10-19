"""Saved stats tables, stats presets and the plot link, in a real database."""

from __future__ import annotations

import json

import pytest

from scistackplot import PlotKind, PlotSpec, Role
from scistackstats import DV, Analysis, DescriptiveSpec, StatsSpec
from scistackstatsdb import (
    SavedStatsError,
    SavedStatsExists,
    apply_stats_preset,
    hide_saved_stats,
    linked_stats_for,
    list_saved_stats,
    list_stats_presets,
    load_saved_stats,
    rename_saved_stats,
    save_stats,
    save_stats_preset,
    saved_stats_history,
)
from scistackstatsdb.saved import STORE


def _spec(linked: str | None = "plot-1", cell: str = "mean_sd") -> StatsSpec:
    plot = PlotSpec(measures=["StatStepLength"], roles={"session": Role.GROUP, "subject": Role.COLLAPSE,
                                                        "trial": Role.COLLAPSE}, groups=["session"], kind=PlotKind.BAR)
    return StatsSpec(design=plot, dvs=(DV("StatStepLength"),), linked_plot=linked,
                     descriptives=DescriptiveSpec(cell=cell), comparisons=(Analysis(),))


def test_save_open_round_trip(db):
    info = save_stats(db, "Table 2", _spec())
    assert info.version == 1 and info.linked_plot == "plot-1" and info.dvs == ("StatStepLength",)
    opened = load_saved_stats(db, info.stats_id)
    assert opened.spec == _spec()
    assert opened.notes == []


def test_resave_appends_a_version_and_old_versions_open(db):
    first = save_stats(db, "Table 2", _spec(cell="mean_sd"))
    second = save_stats(db, "Table 2", _spec(cell="median_iqr"))
    assert second.stats_id == first.stats_id and second.version == 2
    assert load_saved_stats(db, first.stats_id).spec.descriptives.cell == "median_iqr"
    assert load_saved_stats(db, first.stats_id, version=1).spec.descriptives.cell == "mean_sd"
    assert [h.version for h in saved_stats_history(db, first.stats_id)] == [2, 1]


def test_overwrite_refusal_and_rename_and_hide(db):
    a = save_stats(db, "A", _spec())
    save_stats(db, "B", _spec())
    with pytest.raises(SavedStatsExists):
        save_stats(db, "B", _spec(), overwrite=False, current_stats_id=a.stats_id)
    rename_saved_stats(db, a.stats_id, "A renamed")
    assert {i.name for i in list_saved_stats(db)} == {"A renamed", "B"}
    hide_saved_stats(db, a.stats_id)
    assert {i.name for i in list_saved_stats(db)} == {"B"}
    assert len(list_saved_stats(db, include_hidden=True)) == 2, "hidden, never deleted"


def test_save_is_strict(db):
    with pytest.raises(SavedStatsError):
        save_stats(db, "Bad", {"format": 1, "design": {"measures": ["X"]}, "comparisons": [{"family": "magic"}]})


def test_opening_salvages_and_never_rewrites(db):
    info = save_stats(db, "T", _spec())
    row, text = STORE.load(db, info.stats_id)
    envelope = json.loads(text)
    envelope["spec"]["comparisons"][0]["family"] = "bayesian"
    db._duck._execute(f"UPDATE {STORE.table} SET envelope_json = ? WHERE stats_id = ?",
                      [json.dumps(envelope), info.stats_id])
    opened = load_saved_stats(db, info.stats_id)
    assert opened.spec.comparisons[0].family == "parametric"
    assert any(n["path"] == "comparisons[0].family" for n in opened.notes)
    _, after = STORE.load(db, info.stats_id)
    assert json.loads(after)["spec"]["comparisons"][0]["family"] == "bayesian"


def test_linked_stats_for_a_plot(db):
    save_stats(db, "Linked", _spec(linked="plot-1"))
    save_stats(db, "Other", _spec(linked="plot-2"))
    save_stats(db, "Loose", _spec(linked=None))
    assert [i.name for i in linked_stats_for(db, "plot-1")] == ["Linked"]


def test_stats_presets_apply_settings_only(db):
    preset = save_stats_preset(db, "Median tables", _spec(cell="median_iqr"))
    assert [p.name for p in list_stats_presets(db)] == ["Median tables"]
    target = _spec(linked="plot-9", cell="mean_sd")
    applied, notes = apply_stats_preset(db, preset.preset_id, target)
    assert notes == []
    assert applied.descriptives.cell == "median_iqr"
    assert applied.linked_plot == "plot-9" and applied.design == target.design
