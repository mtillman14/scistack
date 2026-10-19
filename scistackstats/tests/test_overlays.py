"""Comparisons as brackets on the linked figure (Stage 10)."""

from __future__ import annotations

from dataclasses import replace

from conftest import bar_spec, popcorn_table
from scistackplot import Role, resolve
from scistackplot.diffbars import bars_for_panel, with_derived_bars
from scistackplot.spec import Comparison
from scistackstats import Analysis, FakeEngine, StatsSpec, run, use_engine
from scistackstats.overlays import bars_for
from scistackstats.spec import OverlaySpec

P = {("ttest_paired", "Acme", "Butter"): 0.001, ("ttest_paired", "Acme", "Corn"): 0.0001,
     ("ttest_paired", "Butter", "Corn"): 0.4}


def _report(spec):
    with use_engine(FakeEngine(p_values=P)):
        return run(spec, {"Ratings": popcorn_table()})


def _spec(**overlay) -> StatsSpec:
    return StatsSpec(design=bar_spec(), comparisons=(Analysis(),), overlay=OverlaySpec(show=True, **overlay))


def test_significant_pairs_become_starred_test_bars():
    spec = _spec()
    out = bars_for(_report(spec), spec, bar_spec())
    assert out.reason is None
    pairs = [(b.a["brand"], b.b["brand"], b.label, b.origin) for b in out.bars]
    # Holm over 3: .0003, .002, .4
    assert pairs == [("Acme", "Butter", "**", "test"), ("Acme", "Corn", "***", "test")]
    assert len(out.ids) == 2


def test_the_bars_resolve_on_the_figure():
    spec = _spec()
    plot = bar_spec()
    merged = with_derived_bars(plot, bars_for(_report(spec), spec, plot).bars)
    (resolved,) = resolve(merged, popcorn_table())
    drawn, unresolved = bars_for_panel(resolved, resolved.panels[0])
    assert len(drawn) == 2 and unresolved == []


def test_all_pairs_label_ns_and_p_labels():
    spec = _spec(pairs="all")
    assert [b.label for b in bars_for(_report(spec), spec, bar_spec()).bars] == ["**", "***", "ns"]
    spec = _spec(pairs="all", label="p")
    labels = [b.label for b in bars_for(_report(spec), spec, bar_spec()).bars]
    assert labels[1] == "p < .001" and labels[2].startswith("p = ")


def test_adjacent_and_hidden():
    spec = _spec(pairs="adjacent")
    out = bars_for(_report(spec), spec, bar_spec())
    assert [(b.a["brand"], b.b["brand"]) for b in out.bars] == [("Acme", "Butter"), ("Butter", "Corn")]
    hidden = _spec(pairs="adjacent", hidden=(out.ids[0],))
    assert len(bars_for(_report(hidden), hidden, bar_spec()).bars) == 1


def test_no_bars_when_designs_differ_or_off_or_compared():
    spec = _spec()
    report = _report(spec)
    other = bar_spec(roles={"brand": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.ITERATE})
    assert "different designs" in bars_for(report, spec, other).reason
    off = replace(spec, overlay=OverlaySpec(show=False))
    assert bars_for(report, off, bar_spec()).bars == []
    compared = replace(bar_spec(), comparison=Comparison(layer="brand", level="Acme"))
    assert "reference level" in bars_for(report, spec, compared).reason
