"""Test results as marks on the figure: brackets, panel notes, caption (plan-stats-on-figure)."""

from __future__ import annotations

from conftest import bar_spec, popcorn_table
from scistackplot import Role, StatMarks, resolve
from scistackplot.model.spec import Comparison
from scistackplot.reduce.diffbars import bars_for_panel, with_derived
from scistackstats import Analysis, FakeEngine, StatsSpec, run, use_engine
from scistackstats.marks import marks_for

P = {("ttest_paired", "Acme", "Butter"): 0.001, ("ttest_paired", "Acme", "Corn"): 0.0001,
     ("ttest_paired", "Butter", "Corn"): 0.4}


def _report(spec):
    with use_engine(FakeEngine(p_values=P)):
        return run(spec, {"Ratings": popcorn_table()})


def _spec() -> StatsSpec:
    return StatsSpec(design=bar_spec(), comparisons=(Analysis(),))


def _plot(**marks):
    marks.setdefault("brackets", True)
    return bar_spec(stat_marks=StatMarks(**marks))


def _marks(**marks):
    spec, plot = _spec(), _plot(**marks)
    return marks_for(_report(spec), spec, plot)


def test_significant_pairs_become_starred_test_bars():
    out = _marks()
    assert out.reason is None
    pairs = [(b.a["brand"], b.b["brand"], b.label, b.origin) for b in out.bars]
    # Holm over 3: .0003, .002, .4
    assert pairs == [("Acme", "Butter", "**", "test"), ("Acme", "Corn", "***", "test")]
    assert len(out.ids) == 2


def test_the_bars_resolve_on_the_figure():
    spec, plot = _spec(), _plot()
    out = marks_for(_report(spec), spec, plot)
    (resolved,) = resolve(with_derived(plot, out.derived()), popcorn_table())
    drawn, unresolved = bars_for_panel(resolved, resolved.panels[0])
    assert len(drawn) == 2 and unresolved == []
    assert plot.derived is None  # the stored spec is untouched


def test_all_pairs_label_ns_and_p_labels():
    assert [b.label for b in _marks(pairs="all").bars] == ["**", "***", "ns"]
    labels = [b.label for b in _marks(pairs="all", label="p").bars]
    assert labels[1] == "p < .001" and labels[2].startswith("p = ")


def test_effect_and_difference_labels_are_numbers():
    effect = [b.label for b in _marks(pairs="all", label="effect").bars]
    difference = [b.label for b in _marks(pairs="all", label="difference").bars]
    assert len(effect) == len(difference) == 3
    assert all(any(ch.isdigit() for ch in text) or text == "—" for text in effect + difference)
    # The estimate is the real mean difference (FakeEngine), so it differs per pair.
    assert len(set(difference)) > 1


def test_adjacent_and_hidden():
    out = _marks(pairs="adjacent")
    assert [(b.a["brand"], b.b["brand"]) for b in out.bars] == [("Acme", "Butter"), ("Butter", "Corn")]
    assert len(_marks(pairs="adjacent", hidden=(out.ids[0],)).bars) == 1


def test_nothing_asked_nothing_drawn():
    spec = _spec()
    out = marks_for(_report(spec), spec, bar_spec())
    assert out.bars == [] and out.panel_notes == [] and out.caption is None
    assert "no test results are asked for" in out.reason


def test_omnibus_line_per_stratum_and_caption_text():
    out = _marks(brackets=False, omnibus=True, caption=True)
    assert out.bars == []
    assert out.caption is not None and "Holm" in out.caption and "Missing data" in out.caption
    for note in out.panel_notes:
        assert note.origin == "test" and "p " in note.text


def test_caption_states_the_star_key_only_when_starred():
    assert "p < .05" in _marks(caption=True).caption
    assert "p < .05" not in _marks(caption=True, label="p").caption


def test_no_marks_when_designs_differ_or_compared():
    spec = _spec()
    report = _report(spec)
    other = bar_spec(roles={"brand": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.ITERATE},
                     stat_marks=StatMarks(brackets=True))
    assert "different designs" in marks_for(report, spec, other).reason
    compared = bar_spec(comparison=Comparison(layer="brand", level="Acme"), stat_marks=StatMarks(brackets=True))
    assert "reference level" in marks_for(report, spec, compared).reason
