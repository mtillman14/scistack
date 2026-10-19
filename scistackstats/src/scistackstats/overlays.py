"""Comparisons → brackets on the linked figure (S20 part 1, P6).

Results become the plot layer's own shape, ``scistackplot.diffbars.
DifferenceBar`` with ``origin="test"``. The caller joins them to a figure for
one resolve with ``scistackplot.diffbars.with_derived_bars``, and never
stores them (they are intent). The plot layer never sees a stats type.

A bracket is only drawn where it means what it says:

- the stats table's design must equal the figure's
  (``scistackplot.design.design_fingerprint``). With different designs a
  comparison's rows are not the figure's marks;
- not on a figure compared to a reference level (S9): a test of raw values
  is not a test of change scores;
- primary comparisons only (S13); refused and hidden ones are skipped.

Pairs policy: ``significant`` (adjusted p below the analysis' alpha), ``all``
(non-significant ones labelled "ns"), or ``adjacent`` (levels next to each
other in level order). More than ten brackets in one panel logs a WARN, and
the result carries a note for the GUI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from scistacklog import Log

from scistackplot import PlotSpec
from scistackplot.design import design_fingerprint
from scistackplot.diffbars import DifferenceBar

from .format import p_phrase, stars
from .run import Comparison, StatsReport
from .spec import StatsSpec

LAYER = "scistackstats"
ORIGIN = "test"
#: Brackets per panel above which the figure is likely unreadable.
CROWDED = 10


@dataclass
class Brackets:
    bars: list[DifferenceBar] = field(default_factory=list)
    #: ``comparison id`` per bar, aligned with ``bars`` (to hide one).
    ids: list[str] = field(default_factory=list)
    #: Why nothing is drawn, when nothing is.
    reason: str | None = None
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"bars": [b.to_dict() for b in self.bars], "ids": list(self.ids),
                "reason": self.reason, "notes": list(self.notes)}


def _p(c: Comparison) -> Any:
    return c.p_adjusted if c.family else c.p


def bars_for(report: StatsReport, stats_spec: StatsSpec, plot_spec: PlotSpec) -> Brackets:
    """The test brackets ``report`` puts on ``plot_spec`` (module docstring)."""
    rule = stats_spec.overlay
    if not rule.show:
        return Brackets(reason="test brackets are off")
    if plot_spec.comparison is not None and plot_spec.comparison.active:
        return Brackets(reason="the figure compares to a reference level; tests of raw values are not drawn on it (S9)")
    if design_fingerprint(stats_spec.design) != design_fingerprint(plot_spec):
        return Brackets(reason="the figure and the stats table have different designs; sync them to draw test brackets")
    analyses = report.analyses
    if rule.analysis is not None:
        analyses = [a for a in analyses if a.analysis.get("id") == rule.analysis]
    if not analyses:
        return Brackets(reason=f"no analysis {rule.analysis!r}" if rule.analysis else "no analysis to draw")
    analysis = analyses[0]
    alpha = analysis.analysis.get("alpha", 0.05)
    hidden = set(rule.hidden)
    out = Brackets()
    per_panel: dict[tuple, int] = {}
    for stratum in analysis.strata:
        levels = (stratum.design.get("factors") or [{}])[0].get("levels", [])
        position = {lv: i for i, lv in enumerate(levels)}
        for c in stratum.primary:
            if c.refused or c.id in hidden:
                continue
            p = _p(c)
            significant = p is not None and p < alpha
            if rule.pairs == "significant" and not significant:
                continue
            if rule.pairs == "adjacent" and abs(position.get(c.a, -9) - position.get(c.b, 9)) != 1:
                continue
            if rule.label == "p":
                label = p_phrase(p, stats_spec.formatting.p_decimals)
            else:
                label = stars(p) or "ns"
            bar = DifferenceBar(
                match=dict(c.stratum),
                a={**c.context, c.term: c.a},
                b={**c.context, c.term: c.b},
                label=label,
                origin=ORIGIN,
            )
            out.bars.append(bar)
            out.ids.append(c.id)
            key = tuple(sorted(c.stratum.items()))
            per_panel[key] = per_panel.get(key, 0) + 1
    for key, count in per_panel.items():
        if count > CROWDED:
            where = ", ".join(f"{k}={v}" for k, v in key) or "the figure"
            Log.warn("[overlays] %d test brackets on %s; consider pairs=adjacent or hiding some", count, where, layer=LAYER)
            out.notes.append(f"{count} test brackets on {where}: consider adjacent pairs or hiding some")
    if not out.bars and out.reason is None:
        out.reason = "no comparison meets the pairs rule" if rule.pairs == "significant" else "no comparisons to draw"
    Log.info("[overlays] analysis %s: %d bracket(s) (%s, %s), %d hidden%s",
             analysis.analysis.get("id"), len(out.bars), rule.pairs, rule.label, len(hidden),
             f"; {out.reason}" if out.reason else "", layer=LAYER)
    return out
