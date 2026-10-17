"""StatsSpec wire format and the Plot → StatStudio seeding rules (S12)."""

from __future__ import annotations

from dataclasses import replace

import pytest

from conftest import bar_spec
from scistackplot import PlotKind
from scistackplot.spec import Aggregation, ErrorBand, Statistic
from scistackstats import DV, Analysis, DescriptiveSpec, FormatSpec, SeedError, StatsSpec, StatsSpecError, seed_from_plot


def _full() -> StatsSpec:
    return StatsSpec(
        design=bar_spec(),
        dvs=(DV("Ratings"), DV("EMG", columns=("TA", "SOL"))),
        linked_plot="plot-123",
        descriptives=DescriptiveSpec(cell="median_iqr", statistics=("n", "skewness")),
        comparisons=(Analysis(
            id="x", family="nonparametric", omnibus=False, pairwise=True, omnibus_test="kruskal",
            pairwise_test="mann_whitney", equal_var=True, alpha=0.01, ci_level=0.9, tails="greater",
            correction="fdr_bh", family_scope="dv", effect_size="rank_biserial",
            difference_spread="se", overrides={"brand": "between"},
        ),),
        formatting=FormatSpec(decimals=3, p_decimals=4, effect_decimals=3, stars=True),
    )


def test_round_trip_keeps_every_field():
    spec = _full()
    assert StatsSpec.from_dict(spec.to_dict()) == spec


def test_from_dict_is_strict():
    raw = _full().to_dict()
    raw["comparisons"][0]["colour"] = "red"
    with pytest.raises(StatsSpecError, match="colour"):
        StatsSpec.from_dict(raw)
    with pytest.raises(StatsSpecError, match="format"):
        StatsSpec.from_dict({**_full().to_dict(), "format": 99})


def test_validation_names_the_field():
    with pytest.raises(StatsSpecError, match="family_scope"):
        Analysis(family_scope="everything")
    with pytest.raises(StatsSpecError, match="override"):
        Analysis(overrides={"brand": "sideways"})
    with pytest.raises(StatsSpecError, match="unique"):
        StatsSpec(design=bar_spec(), comparisons=(Analysis(id="a"), Analysis(id="a")))


def test_dv_key_and_targets():
    assert DV("EMG", columns=("TA", "SOL")).key == "EMG[TA, SOL]"
    assert StatsSpec(design=bar_spec()).targets == (DV("Ratings"),)


@pytest.mark.parametrize(
    "plot, cell, family",
    [
        (bar_spec(), "mean_sd", "parametric"),
        (bar_spec(aggregate=Aggregation(error=ErrorBand.SEM)), "mean_sem", "parametric"),
        (bar_spec(aggregate=Aggregation(error=ErrorBand.CI95)), "mean_ci95", "parametric"),
        (bar_spec(aggregate=Aggregation(error=ErrorBand.IQR)), "median_iqr", "nonparametric"),
        (bar_spec(aggregate=Aggregation(statistic=Statistic.MEDIAN)), "median_iqr", "nonparametric"),
        (bar_spec(kind=PlotKind.BOX), "median_iqr", "nonparametric"),
        (bar_spec(kind=PlotKind.VIOLIN), "median_iqr", "nonparametric"),
        (bar_spec(kind=PlotKind.SCATTER), "mean_sd", "parametric"),
    ],
)
def test_seed_rules(plot, cell, family):
    seeded = seed_from_plot(plot, linked_plot="p1")
    assert seeded.spec.descriptives.cell == cell
    assert seeded.spec.comparisons[0].family == family
    assert seeded.spec.design == plot, "the design is the plot's own spec"
    assert seeded.spec.linked_plot == "p1"
    assert seeded.notes


def test_sem_seed_notes_journals_prefer_sd():
    notes = seed_from_plot(bar_spec(aggregate=Aggregation(error=ErrorBand.SEM))).notes
    assert any("SD" in n and "Journals" in n for n in notes)


def test_compared_figure_refuses_to_seed():
    from scistackplot.spec import Comparison

    plot = replace(bar_spec(), comparison=Comparison(layer="brand", level="Acme"))
    with pytest.raises(SeedError, match="reference"):
        seed_from_plot(plot)


def test_an_inactive_comparison_does_not_block_seeding():
    from scistackplot.spec import Comparison

    plot = replace(bar_spec(), comparison=Comparison(layer="brand", level="Acme", active=False))
    assert seed_from_plot(plot).spec.design == plot
