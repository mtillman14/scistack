"""The facts a figure draws of a statistics result (plan-stats-on-figure.md).

``marks_for(report, stats_spec, plot_spec)`` reads WHAT the figure asks for
from ``plot_spec.stat_marks`` (the figure owns the request) and returns the
facts in the plot layer's own shapes: ``scistackplot.reduce.diffbars.
DifferenceBar`` with ``origin="test"``, ``PanelNote`` and a caption. The caller
joins them to the figure for one resolve with
``scistackplot.reduce.diffbars.with_derived`` and never stores them (they are
intent). The plot layer never sees a stats type, and every label and sentence
is written by :mod:`scistackstats.format`.

A mark is only drawn where it means what it says:

- the stats table's design must equal the figure's
  (``scistackplot.model.design.design_fingerprint``). With different designs a
  comparison's rows are not the figure's marks;
- not on a figure compared to a reference level (S9): a test of raw values
  is not a test of change scores;
- primary comparisons only (S13); refused and hidden ones are skipped.

Pairs policy: ``significant`` (adjusted p below the analysis' alpha), ``all``
(non-significant ones labelled "ns" under the stars label), or ``adjacent``
(levels next to each other in level order). More than ten brackets in one
panel logs a WARN, and the result carries a note for the GUI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from scistacklog import Log

from scistackplot import PlotSpec
from scistackplot.model.design import design_fingerprint
from scistackplot.reduce.diffbars import TEST, Derived, DifferenceBar, PanelNote

from .catalog import TESTS
from .format import bracket_label, figure_caption, omnibus_line
from .run import CORRECTION_LABELS, StatsReport
from .spec import StatsSpec

LAYER = "scistackstats"
ORIGIN = TEST
#: Brackets per panel above which the figure is likely unreadable.
CROWDED = 10


@dataclass
class Marks:
    bars: list[DifferenceBar] = field(default_factory=list)
    #: ``comparison id`` per bar, aligned with ``bars`` (to hide one).
    ids: list[str] = field(default_factory=list)
    panel_notes: list[PanelNote] = field(default_factory=list)
    caption: str | None = None
    #: Why nothing is drawn, when nothing is.
    reason: str | None = None
    notes: list[str] = field(default_factory=list)
    #: What ran, in a phrase ("Paired t-tests, Holm-adjusted"), for the GUI.
    test: str | None = None

    def derived(self) -> Derived:
        """The plot layer's input (``diffbars.with_derived``)."""
        return Derived(bars=tuple(self.bars), panel_notes=tuple(self.panel_notes), caption=self.caption)

    def to_dict(self) -> dict[str, Any]:
        return {"bars": [b.to_dict() for b in self.bars], "ids": list(self.ids),
                "panel_notes": [n.to_dict() for n in self.panel_notes], "caption": self.caption,
                "reason": self.reason, "notes": list(self.notes), "test": self.test}


def _p(c) -> Any:
    return c.p_adjusted if c.family else c.p


def _what_ran(analysis) -> str | None:
    tests = []
    for s in analysis.strata:
        t = s.selection.get("pairwise")
        if t and t in TESTS and TESTS[t].label not in tests:
            tests.append(TESTS[t].label)
    if not tests:
        return None
    text = ", ".join(tests)
    correction = analysis.analysis.get("correction")
    if correction and correction != "none":
        text += f", {CORRECTION_LABELS.get(correction, correction)}-adjusted"
    return text


def _pending(analysis) -> str | None:
    for s in analysis.strata:
        if s.action:
            return s.action.get("message") or "a decision is needed before the tests can run"
    return None


def marks_for(report: StatsReport, stats_spec: StatsSpec, plot_spec: PlotSpec) -> Marks:
    """The test brackets, panel notes and caption ``report`` puts on ``plot_spec``."""
    rule = plot_spec.stat_marks
    if not rule.wanted:
        return Marks(reason="no test results are asked for")
    if plot_spec.comparison is not None and plot_spec.comparison.active:
        return Marks(reason="the figure compares to a reference level; tests of raw values are not drawn on it (S9)")
    if design_fingerprint(stats_spec.design) != design_fingerprint(plot_spec):
        return Marks(reason="the figure and the stats table have different designs; sync them to draw test results")
    if not report.analyses:
        return Marks(reason="no analysis to draw")
    analysis = report.analyses[0]
    formatting = stats_spec.formatting
    out = Marks(test=_what_ran(analysis))
    if rule.brackets:
        _brackets(out, analysis, rule, formatting)
    if rule.omnibus:
        for stratum in analysis.strata:
            line = omnibus_line(stratum, formatting)
            if line:
                out.panel_notes.append(PanelNote(match=dict(stratum.stratum), text=line, origin=ORIGIN))
    if rule.caption:
        out.caption = figure_caption(analysis, stats_spec, starred=rule.brackets and rule.label == "stars")
    if not (out.bars or out.panel_notes or out.caption):
        out.reason = _pending(analysis) or (
            "no comparison meets the pairs rule" if rule.brackets and rule.pairs == "significant"
            else "no test result to draw")
    Log.info("[marks] analysis %s: %d bracket(s) (%s, %s), %d hidden, %d panel note(s), caption %s%s",
             analysis.analysis.get("id"), len(out.bars), rule.pairs, rule.label, len(rule.hidden),
             len(out.panel_notes), "yes" if out.caption else "no",
             f"; {out.reason}" if out.reason else "", layer=LAYER)
    return out


def _brackets(out: Marks, analysis, rule, formatting) -> None:
    alpha = analysis.analysis.get("alpha", 0.05)
    hidden = set(rule.hidden)
    per_panel: dict[tuple, int] = {}
    for stratum in analysis.strata:
        levels = (stratum.design.get("factors") or [{}])[0].get("levels", [])
        position = {lv: i for i, lv in enumerate(levels)}
        for c in stratum.primary:
            if c.refused or c.id in hidden:
                continue
            p = _p(c)
            if rule.pairs == "significant" and not (p is not None and p < alpha):
                continue
            if rule.pairs == "adjacent" and abs(position.get(c.a, -9) - position.get(c.b, 9)) != 1:
                continue
            out.bars.append(DifferenceBar(
                match=dict(c.stratum),
                a={**c.context, c.term: c.a},
                b={**c.context, c.term: c.b},
                label=bracket_label(c, rule.label, analysis, formatting),
                origin=ORIGIN,
            ))
            out.ids.append(c.id)
            key = tuple(sorted(c.stratum.items()))
            per_panel[key] = per_panel.get(key, 0) + 1
    for key, count in per_panel.items():
        if count > CROWDED:
            where = ", ".join(f"{k}={v}" for k, v in key) or "the figure"
            Log.warn("[marks] %d test brackets on %s; consider pairs=adjacent or hiding some", count, where, layer=LAYER)
            out.notes.append(f"{count} test brackets on {where}: consider adjacent pairs or hiding some")
