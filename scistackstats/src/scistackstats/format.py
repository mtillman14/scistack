"""How every statistic reads — the one formatting owner (S17, S19).

Two layers:

1. **Text of one number**: :func:`number`, :func:`p_value`, :func:`statistic`,
   :func:`effect`, :func:`apa_sentence`. APA conventions: unicode minus, no
   leading zero on p, ``< .001`` below the precision, df shown to one decimal
   when fractional (Welch, Greenhouse–Geisser).
2. **A table as a grid of strings**: :class:`Grid` (header rows with spans,
   body rows, caption, notes). :func:`descriptives_grids`,
   :func:`comparisons_grid` and :func:`omnibus_grid` build them, including
   the backend-written caption (S17). Every exporter (``export``) and the GUI
   render a ``Grid``. None of them formats a number itself, so two exports of
   one result cannot disagree.

Display names come from the plot layer's alias owner
(``scistackplot.aliases.display_text``: the design's own aliases over the
project's), through :class:`Labels`.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Iterable

from scistackplot import LongTable
from scistackplot.aliases import DisplayText, display_text
from scistackplot.variants import set_name

from .catalog import TESTS
from .describe import CELL_LAYOUTS, STATISTICS, DescriptivesTable, DVBlock
from .effects import EFFECT_SIZES
from .exclusions import RESTATED_CODES, Exclusions
from .run import CORRECTION_LABELS, AnalysisReport, Comparison, StatsReport
from .spec import FormatSpec, StatsSpec

MINUS = "−"
DASH = "—"
STAR_LEVELS = ((0.001, "***"), (0.01, "**"), (0.05, "*"))


# ---------------------------------------------------------------------------
# One number
# ---------------------------------------------------------------------------


def _finite(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def number(x: Any, decimals: int) -> str:
    """``x`` to ``decimals`` places, unicode minus, never ``-0.00``."""
    if not _finite(x):
        return DASH
    text = f"{x:.{decimals}f}"
    if text.lstrip("-").strip("0.") == "":
        text = text.lstrip("-")
    return text.replace("-", MINUS)


def p_value(p: Any, decimals: int = 3) -> str:
    """APA p: ``.041``, ``< .001``, ``> .999``; no leading zero."""
    if not _finite(p):
        return DASH
    floor = 10 ** (-decimals)
    if p < floor:
        return f"< {floor:.{decimals}f}".replace("0.", ".", 1)
    if p > 1 - floor:
        return f"> {1 - floor:.{decimals}f}".replace("0.", ".", 1)
    return f"{p:.{decimals}f}".replace("0.", ".", 1)


def p_phrase(p: Any, decimals: int = 3, symbol: str = "p") -> str:
    text = p_value(p, decimals)
    if text.startswith(("<", ">")):
        return f"{symbol} {text}"
    return f"{symbol} = {text}"


def stars(p: Any) -> str:
    if not _finite(p):
        return ""
    for level, mark in STAR_LEVELS:
        if p < level:
            return mark
    return ""


def df_text(df: Any) -> str:
    if not _finite(df):
        return DASH
    return f"{df:.0f}" if abs(df - round(df)) < 1e-9 else f"{df:.1f}"


_STAT_SYMBOLS = {"chi2": "χ²"}


def statistic(name: Any, value: Any, df: Iterable[Any] | None, decimals: int = 2) -> str:
    """``t(5) = −2.14``, ``F(2, 10.3) = 4.12``, ``U = 12.00``."""
    if name is None or not _finite(value):
        return DASH
    symbol = _STAT_SYMBOLS.get(str(name), str(name))
    dfs = [d for d in (df or []) if d is not None]
    head = f"{symbol}({', '.join(df_text(d) for d in dfs)})" if dfs else symbol
    return f"{head} = {number(value, decimals)}"


def interval(pair: Any, decimals: int) -> str:
    if not isinstance(pair, (list, tuple)) or len(pair) != 2:
        return ""
    return f"[{number(pair[0], decimals)}, {number(pair[1], decimals)}]"


def effect(size: dict[str, Any] | None, decimals: int = 2, *, with_name: bool = True) -> str:
    """``Cohen's d_z = 0.84 [0.21, 1.45]`` (the CI only when csv-stats has one)."""
    if not size:
        return DASH
    value = number(size.get("value"), decimals)
    ci = interval(size.get("ci"), decimals)
    body = f"{value} {ci}".strip()
    return f"{size.get('name', '')} = {body}" if with_name else body


def auto_decimals(values: Iterable[Any]) -> int:
    """Decimals that show two significant digits of the smallest non-zero
    ``|value|``, clamped to 0–6; 2 when there is nothing to go on."""
    sizes = [abs(v) for v in values if _finite(v) and v != 0]
    if not sizes:
        return 2
    smallest = min(sizes)
    return max(0, min(6, 1 - math.floor(math.log10(smallest))))


# ---------------------------------------------------------------------------
# Labels (aliases)
# ---------------------------------------------------------------------------


@dataclass
class Labels:
    """Display names per DV, from the plot layer's alias owner."""

    texts: dict[str, DisplayText] = field(default_factory=dict)
    variables: dict[str, str] = field(default_factory=dict)

    def _text(self, dv: str) -> DisplayText:
        return self.texts.get(dv) or DisplayText()

    def dv(self, dv: str) -> str:
        variable = self.variables.get(dv, dv)
        name = self._text(dv).name(variable, variable)
        return dv.replace(variable, name, 1) if name != variable else dv

    def factor(self, dv: str, name: str) -> str:
        return self._text(dv).name(name, name)

    def level(self, dv: str, factor: str, value: Any) -> str:
        return self._text(dv).level(factor, value)

    def key(self, dv: str, mapping: dict[str, Any]) -> str:
        return ", ".join(f"{self.factor(dv, k)} = {self.level(dv, k, v)}" for k, v in mapping.items())


def labels_for(stats_spec: StatsSpec, tables: dict[str, LongTable]) -> Labels:
    """:class:`Labels` for every DV whose table is loaded."""
    labels = Labels()
    for dv in stats_spec.targets:
        labels.variables[dv.key] = dv.variable
        table = tables.get(dv.variable)
        if table is not None:
            labels.texts[dv.key] = display_text(stats_spec.design, table)
    return labels


# ---------------------------------------------------------------------------
# Grids
# ---------------------------------------------------------------------------


@dataclass
class HeaderCell:
    text: str
    span: int = 1


@dataclass
class GridRow:
    cells: list[str]
    #: "data", or "section" (a stratum heading: one cell spanning the row).
    kind: str = "data"


@dataclass
class Grid:
    title: str
    #: Header rows, outermost first. The first ``label_columns`` columns are
    #: row labels; a header cell spans ``span`` columns.
    header_rows: list[list[HeaderCell]]
    rows: list[GridRow]
    caption: str
    notes: list[str] = field(default_factory=list)
    label_columns: int = 1

    @property
    def width(self) -> int:
        return sum(c.span for c in self.header_rows[-1]) if self.header_rows else 0

    def flat_header(self, sep: str = " · ") -> list[str]:
        """One header row: the nested texts joined per column."""
        columns: list[list[str]] = [[] for _ in range(self.width)]
        for row in self.header_rows:
            i = 0
            for cell in row:
                for j in range(i, i + cell.span):
                    if cell.text and (not columns[j] or columns[j][-1] != cell.text):
                        columns[j].append(cell.text)
                i += cell.span
        return [sep.join(parts) for parts in columns]

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "header_rows": [[{"text": c.text, "span": c.span} for c in row] for row in self.header_rows],
            "rows": [{"cells": r.cells, "kind": r.kind} for r in self.rows],
            "caption": self.caption, "notes": list(self.notes), "label_columns": self.label_columns,
        }


def _variants_sentence(stats_spec: StatsSpec) -> str:
    if stats_spec.variant_mode == "sweep":
        return ("Variants: swept; every variant of every variant axis is analysed separately "
                "(see the specification curve).")
    sets = stats_spec.design.variant_sets
    if not sets:
        return "Variants: no selection (every variant in the data)."
    names = [set_name(v, i, primary=stats_spec.design.y_measure) for i, v in enumerate(sets)]
    return f"Variants: {', '.join(names)}."


def _join_words(words: list[str]) -> str:
    if len(words) <= 1:
        return "".join(words)
    return ", ".join(words[:-1]) + " and " + words[-1]


def _unit_sentence(rows: dict[str, Any], labels: Labels, dv: str) -> str:
    unit = rows.get("unit")
    if unit is None:
        return "No unit of analysis (nothing collapsed, or weighted by N): each cell summarises the rows as recorded."
    unit_name = labels.factor(dv, unit)
    averaged = [labels.factor(dv, a) for a in rows.get("averaged", [])]
    first = f" ({_join_words(averaged)} averaged within {unit_name} first)" if averaged else ""
    return f"Each cell summarises one value per {unit_name}{first}; n is the number of {unit_name} values."


def _cell_text(stats: dict[str, Any], layout: str, d: int) -> str:
    if not stats or not stats.get("n"):
        return DASH
    if layout == "mean_sd":
        return f"{number(stats.get('mean'), d)} ({number(stats.get('sd'), d)})"
    if layout == "mean_sem":
        return f"{number(stats.get('mean'), d)} ({number(stats.get('sem'), d)})"
    if layout == "mean_ci95":
        return f"{number(stats.get('mean'), d)} [{number(stats.get('ci_low'), d)}, {number(stats.get('ci_high'), d)}]"
    if layout == "median_iqr":
        return f"{number(stats.get('median'), d)} [{number(stats.get('q1'), d)}, {number(stats.get('q3'), d)}]"
    return ""


def _stat_text(stats: dict[str, Any], stat_id: str, d: int) -> str:
    if not stats:
        return DASH
    if stat_id in ("n", "n_missing"):
        value = stats.get(stat_id)
        return str(int(value)) if _finite(value) else DASH
    if not stats.get("n"):
        return DASH
    if stat_id == "ci95":
        return f"[{number(stats.get('ci_low'), d)}, {number(stats.get('ci_high'), d)}]"
    if stat_id == "quartiles":
        return f"{number(stats.get('q1'), d)}, {number(stats.get('q3'), d)}"
    if stat_id == "mode":
        count = stats.get("mode_count")
        return f"{number(stats.get('mode'), d)} (×{int(count)})" if _finite(count) else DASH
    if stat_id in ("skewness", "kurtosis", "cv"):
        return number(stats.get(stat_id), max(d, 2))
    return number(stats.get(stat_id), d)


def _block_decimals(block: DVBlock, layout: str, formatting: FormatSpec) -> int:
    if formatting.decimals is not None:
        return formatting.decimals
    keys = ("iqr",) if layout == "median_iqr" else ("sd",)
    values = [c.stats.get(k) for s in block.sections for c in s.cells for k in keys]
    if not any(_finite(v) and v for v in values):
        values = [c.stats.get("mean") for s in block.sections for c in s.cells]
    return auto_decimals(values)


def _omnibus_p(report: StatsReport | None, dv: str, stratum: dict[str, str], term: str | None) -> tuple[Any, str | None]:
    if report is None or term is None:
        return None, None
    for analysis in report.analyses:
        for s in analysis.strata:
            if s.dv == dv and s.stratum == stratum and s.omnibus and not s.omnibus.refused:
                for e in s.omnibus.effects:
                    if e["term"] == term:
                        return e["p"], s.omnibus.test
    return None, None


def descriptives_grids(
    table: DescriptivesTable,
    stats_spec: StatsSpec,
    labels: Labels | None = None,
    report: StatsReport | None = None,
) -> list[Grid]:
    """The Descriptives tab as grids: one per run of DVs sharing columns.

    With ``report``, a final **p** column shows the omnibus main effect of
    the innermost grouping layer per DV and stratum (unadjusted, and the
    notes say so).
    """
    labels = labels or Labels()
    formatting = stats_spec.formatting
    header_text, _ = CELL_LAYOUTS[table.cell]
    groups: list[list[DVBlock]] = []
    for block in table.blocks:
        if groups and not block.refused and not groups[-1][0].refused \
                and (groups[-1][0].layers, groups[-1][0].columns) == (block.layers, block.columns):
            groups[-1].append(block)
        else:
            groups.append([block])
    grids = []
    for blocks in groups:
        first = blocks[0]
        if first.refused:
            grids.append(Grid(title=labels.dv(first.dv), header_rows=[[HeaderCell("Measure")]],
                              rows=[GridRow([f"{labels.dv(first.dv)}: not available ({first.refused})"])],
                              caption=f"{labels.dv(first.dv)} could not be summarised: {first.refused}"))
            continue
        layers = first.layers
        primary = layers[-1] if layers else None
        with_p = report is not None and primary is not None
        header_rows: list[list[HeaderCell]] = []
        n_cols = len(first.columns)
        if layers:
            for depth, layer in enumerate(layers):
                row = [HeaderCell("Measure" if depth == len(layers) - 1 else ""),
                       HeaderCell("Statistic" if depth == len(layers) - 1 else "")]
                # Consecutive columns sharing this layer's prefix merge into one spanning cell.
                seen: list[list] = []
                for c in first.columns:
                    key = tuple(c[l] for l in layers[: depth + 1])
                    if not seen or seen[-1][0] != key:
                        seen.append([key, 0])
                    seen[-1][1] += 1
                for key, count in seen:
                    row.append(HeaderCell(labels.level(first.dv, layer, key[-1]), count))
                if with_p:
                    row.append(HeaderCell("p" if depth == len(layers) - 1 else ""))
                header_rows.append(row)
        else:
            header_rows.append([HeaderCell("Measure"), HeaderCell("Statistic"), HeaderCell("All")]
                               + ([HeaderCell("p")] if with_p else []))
        rows: list[GridRow] = []
        width = 2 + max(n_cols, 1) + (1 if with_p else 0)
        notes: list[str] = []
        missing = 0
        tests_used: set[str] = set()
        for block in blocks:
            d = _block_decimals(block, table.cell, formatting)
            dv_name = labels.dv(block.dv)
            for section in block.sections:
                if section.stratum:
                    rows.append(GridRow([labels.key(block.dv, section.stratum)] + [""] * (width - 1), "section"))
                first_row = True
                stat_rows: list[tuple[str, list[str]]] = []
                if table.cell != "none":
                    stat_rows.append((header_text, [_cell_text(c.stats, table.cell, d) for c in section.cells]))
                for stat_id in table.statistics:
                    stat_rows.append((STATISTICS[stat_id].label, [_stat_text(c.stats, stat_id, d) for c in section.cells]))
                for label, values in stat_rows:
                    p_cell: list[str] = []
                    if with_p:
                        p, test = _omnibus_p(report, block.dv, section.stratum, primary) if first_row else (None, None)
                        if test:
                            tests_used.add(test)
                        p_cell = [p_value(p, formatting.p_decimals) + (stars(p) if formatting.stars else "") if first_row else ""]
                    rows.append(GridRow([dv_name if first_row else "", label, *values, *p_cell]))
                    first_row = False
                missing += sum(int(c.stats.get("n_missing") or 0) for c in section.cells)
        names = [labels.dv(b.dv) for b in blocks]
        by = " × ".join(labels.factor(first.dv, l) for l in layers)
        title = f"{_join_words(names)}{f' by {by}' if by else ''}"
        sentences = [f"{title}.", f"Cells: {header_text.lower() if header_text else 'see the statistic rows'}.",
                     _unit_sentence(first.rows, labels, first.dv)]
        if table.cell == "mean_ci95" or "ci95" in table.statistics:
            sentences.append("95% CIs of the mean are t-based.")
        if any(s in ("sd", "variance") for s in table.statistics) or table.cell == "mean_sd":
            sentences.append("SD uses n − 1.")
        strata = first.rows.get("strata") or []
        if strata:
            sentences.append(f"Separate sections per {_join_words([labels.factor(first.dv, s) for s in strata])}.")
        if missing:
            sentences.append(f"{missing} missing value(s) excluded.")
        sentences.append(_variants_sentence(stats_spec))
        if with_p:
            label = _join_words(sorted(TESTS[t].label for t in tests_used if t in TESTS)) or "omnibus test"
            notes.append(f"p: {label}, main effect of {labels.factor(first.dv, primary)}; not adjusted.")
        for block in blocks:
            notes.extend(block.notes)
        grids.append(Grid(title=title, header_rows=header_rows, rows=rows, caption=" ".join(sentences),
                          notes=notes, label_columns=2))
    return grids


def _comparison_decimals(analysis: AnalysisReport, dv: str, formatting: FormatSpec) -> int:
    if formatting.decimals is not None:
        return formatting.decimals
    values = []
    for c in analysis.comparisons():
        if c.dv != dv or c.refused:
            continue
        if isinstance(c.spread, list) and len(c.spread) == 2 and all(_finite(v) for v in c.spread):
            values.append((c.spread[1] - c.spread[0]) / 2)
        elif _finite(c.spread):
            values.append(c.spread)
    return auto_decimals(values)


def clusters_text(c: Comparison, p_decimals: int = 3) -> str:
    """SPM: ``nodes 34.7–58.2 (p = .003)``, or ``no supra-threshold cluster``."""
    if not c.clusters:
        return "no supra-threshold cluster"
    parts = [f"nodes {number(cl.get('start'), 1)}–{number(cl.get('end'), 1)} ({p_phrase(cl.get('p'), p_decimals)})"
             for cl in c.clusters]
    return "; ".join(parts)


def _spread_text(c: Comparison, spread: str, d: int) -> str:
    if c.clusters is not None:
        return clusters_text(c)
    if spread == "ci":
        return f"{number(c.estimate, d)} {interval(c.spread, d)}".strip()
    return f"{number(c.estimate, d)} ({number(c.spread, d)})"


def _comparison_context(c: Comparison, labels: Labels) -> str:
    merged = {**c.stratum, **c.context}
    return labels.key(c.dv, merged) if merged else ""


def apa_sentence(c: Comparison, analysis: AnalysisReport, formatting: FormatSpec, labels: Labels | None = None) -> str:
    """One comparison as an APA-style sentence (S19)."""
    labels = labels or Labels()
    dv = labels.dv(c.dv)
    a = labels.level(c.dv, c.term, c.a)
    b = labels.level(c.dv, c.term, c.b)
    where = _comparison_context(c, labels)
    head = f"{dv}{f' ({where})' if where else ''}: {a} − {b}"
    if c.refused:
        return f"{head}: not run ({c.refused})."
    if c.clusters is not None:
        threshold = f", t* = {number(c.threshold, 2)}" if _finite(c.threshold) else ""
        return f"{head}: SPM{{t}} {clusters_text(c, formatting.p_decimals)}{threshold}."
    d = _comparison_decimals(analysis, c.dv, formatting)
    ci_level = analysis.analysis.get("ci_level", 0.95)
    spread = analysis.analysis.get("difference_spread", "ci")
    if spread == "ci" and isinstance(c.spread, list):
        est = f"Δ = {number(c.estimate, d)}, {ci_level * 100:g}% CI {interval(c.spread, d)}"
    elif _finite(c.spread):
        est = f"Δ = {number(c.estimate, d)} ({'SE' if spread == 'se' else 'SD'} = {number(c.spread, d)})"
    else:
        est = f"Δ = {number(c.estimate, d)}"
    parts = [est, statistic(c.statistic_name, c.statistic, c.df), p_phrase(c.p, formatting.p_decimals)]
    method = analysis.analysis.get("correction", "none")
    if c.family and method != "none":
        parts[-1] += f" ({CORRECTION_LABELS.get(method, method)}-adjusted {p_value(c.p_adjusted, formatting.p_decimals)})"
    if c.effect_size:
        parts.append(effect(c.effect_size, formatting.effect_decimals))
    return f"{head}, " + ", ".join(p for p in parts if p and p != DASH) + "."


def _unique(values: Iterable[Any]) -> list[Any]:
    out: list[Any] = []
    for v in values:
        if v is not None and v not in out:
            out.append(v)
    return out


def comparisons_grid(report: StatsReport, analysis: AnalysisReport, stats_spec: StatsSpec,
                     labels: Labels | None = None) -> Grid:
    """The primary comparisons of one analysis, with the S17 caption."""
    labels = labels or Labels()
    formatting = stats_spec.formatting
    spread = analysis.analysis.get("difference_spread", "ci")
    ci_level = analysis.analysis.get("ci_level", 0.95)
    spread_header = {"ci": f"Δ [{ci_level * 100:g}% CI]", "se": "Δ (SE)", "sd_diff": "Δ (SD of differences)"}[spread]
    comps = analysis.comparisons()
    sizes = _unique(c.effect_size.get("name") for c in comps if c.effect_size)
    effect_header = sizes[0] if len(sizes) == 1 else "Effect size"
    header = ["Measure", "Within", "Comparison", spread_header, "Statistic", "p", "p (adj.)", effect_header, "n"]
    rows: list[GridRow] = []
    for c in comps:
        dv = labels.dv(c.dv)
        pair = f"{labels.level(c.dv, c.term, c.a)} − {labels.level(c.dv, c.term, c.b)}"
        if c.refused:
            rows.append(GridRow([dv, _comparison_context(c, labels), pair, DASH, f"not run: {c.refused}",
                                 DASH, DASH, DASH, DASH]))
            continue
        d = _comparison_decimals(analysis, c.dv, formatting)
        n = (c.n or {}).get("subjects") or (c.n or {}).get("total")
        if c.excluded:
            n = Exclusions(**c.excluded).n_text()  # "15 of 17": never just the n used
        adjusted = p_value(c.p_adjusted, formatting.p_decimals) + (stars(c.p_adjusted) if formatting.stars else "")
        rows.append(GridRow([
            dv, _comparison_context(c, labels), pair, _spread_text(c, spread, d),
            (f"t* = {number(c.threshold, 2)}" if c.clusters is not None else statistic(c.statistic_name, c.statistic, c.df)),
            p_value(c.p, formatting.p_decimals),
            adjusted if c.family else DASH,
            effect(c.effect_size, formatting.effect_decimals, with_name=len(sizes) != 1),
            str(n) if n is not None else DASH,
        ]))
    caption = comparisons_caption(report, analysis, stats_spec, labels)
    notes = [analysis.family_statement]
    for s in analysis.strata:
        if s.excluded:
            notes.append(f"{labels.dv(s.dv)}{f' ({labels.key(s.dv, s.stratum)})' if s.stratum else ''}: "
                         f"{Exclusions(**s.excluded).sentence()}")
        if s.refused:
            notes.append(f"{labels.dv(s.dv)}{f' ({labels.key(s.dv, s.stratum)})' if s.stratum else ''}: "
                         f"comparisons not run: {s.refused}")
        if s.effect_size_note:
            notes.append(s.effect_size_note)
        if s.difference and s.difference.get("note"):
            notes.append(s.difference["note"])
    for c in comps:
        for w in c.warnings:
            if w.get("code") in RESTATED_CODES and c.excluded:
                continue  # restated in full below
            notes.append(f"{labels.dv(c.dv)} {c.a} − {c.b}: {w.get('message', w.get('code'))}")
        if c.excluded:
            notes.append(f"{labels.dv(c.dv)} {_comparison_context(c, labels)} {c.a} − {c.b}: "
                         f"{Exclusions(**c.excluded).sentence()}")
    for r in analysis.refusals:
        notes.append(f"{labels.dv(r['dv'])}: not analysed ({r['reason']})")
    if any(c.clusters is not None for c in comps):
        notes.append("SPM: p is the smallest cluster p-value of each comparison (cluster-level inference, spm1d).")
    if formatting.stars:
        notes.append("* p < .05, ** p < .01, *** p < .001 (adjusted).")
    return Grid(title=f"Comparisons ({analysis.analysis.get('id')})", header_rows=[[HeaderCell(h) for h in header]],
                rows=rows, caption=caption, notes=_unique(notes), label_columns=3)


def missing_sentence(analysis: AnalysisReport) -> str:
    """The analysis's missing-data policy, stated in every caption (user
    decision 2026-10-19: the choice is recorded and known)."""
    policy = analysis.analysis.get("missing", "use_all")
    tests = {t for s in analysis.strata for t in (s.selection.get("omnibus"), s.selection.get("pairwise")) if t}
    if policy == "complete_cases":
        dropped = sum(len((s.excluded or {}).get("units", [])) for s in analysis.strata)
        return ("Missing data: complete cases only (chosen); "
                + (f"{dropped} incomplete unit(s) excluded, named in the notes." if dropped else "no unit was incomplete."))
    text = "Missing data: every observation used where the test allows it"
    if tests & {"lmm", "lmm_pairwise"}:
        text += " (linear mixed model; Wald tests: z and χ², no t/F degrees of freedom)"
    return text + "."


def comparisons_caption(report: StatsReport, analysis: AnalysisReport, stats_spec: StatsSpec, labels: Labels) -> str:
    strata = analysis.strata
    if not strata:
        return "No comparisons."
    dv0 = strata[0].dv
    primary = _unique(f["name"] for s in strata for f in s.design.get("factors", [])[:1])
    outer = _unique(f["name"] for s in strata for f in s.design.get("factors", [])[1:])
    sentences = []
    if primary:
        what = f"Pairwise comparisons of the {_join_words([labels.factor(dv0, p) for p in primary])} levels"
        if outer:
            what += f" within each level of {_join_words([labels.factor(dv0, o) for o in outer])}"
        sentences.append(what + ".")
    sentences += [d["label"] + "." for d in _unique(s.difference for s in strata if s.difference)]
    pair_tests = _unique(s.selection.get("pairwise") for s in strata if s.selection.get("pairwise"))
    omni_tests = _unique(s.omnibus.test for s in strata if s.omnibus and s.omnibus.test)
    if pair_tests:
        sentences.append(f"Test: {_join_words([TESTS[t].label for t in pair_tests if t in TESTS])}.")
    if omni_tests:
        sentences.append(f"Omnibus: {_join_words([TESTS[t].label for t in omni_tests if t in TESTS])}.")
    size_ids = _unique(c.effect_size.get("id") for c in analysis.comparisons() if c.effect_size)
    for sid in size_ids:
        info = EFFECT_SIZES.get(sid)
        if info:
            sentences.append(f"Effect size: {info.name} ({info.formula}; {info.citation}).")
    sentences.append(analysis.family_statement)
    sentences.append(missing_sentence(analysis))
    units = _unique((labels.factor(r["dv"], r["unit"]) if r.get("unit") else None) for r in report.dvs)
    if units:
        sentences.append(f"Unit of analysis: {_join_words(units)}.")
    if any(r.get("unit") is None for r in report.dvs):
        sentences.append("Some measures have no unit of analysis: their rows are treated as independent.")
    sentences.append(_variants_sentence(stats_spec))
    return " ".join(sentences)


def omnibus_grid(report: StatsReport, analysis: AnalysisReport, stats_spec: StatsSpec,
                 labels: Labels | None = None) -> Grid | None:
    """The secondary results (main effects and interactions), or None."""
    labels = labels or Labels()
    formatting = stats_spec.formatting
    rows: list[GridRow] = []
    notes: list[str] = []
    sizes: list[str] = []
    for s in analysis.strata:
        if s.omnibus is None:
            continue
        where = labels.key(s.dv, s.stratum) if s.stratum else ""
        if s.omnibus.refused:
            rows.append(GridRow([labels.dv(s.dv), where, DASH, f"not run: {s.omnibus.refused}", DASH, DASH]))
            continue
        for e in s.omnibus.effects:
            term = " × ".join(labels.factor(s.dv, t) for t in str(e["term"]).split(":"))
            size = e.get("effect_size") or {}
            if size.get("name"):
                sizes.append(size["name"])
            rows.append(GridRow([labels.dv(s.dv), where, term, statistic(e.get("statistic_name"), e.get("statistic"), e.get("df")),
                                 p_value(e.get("p"), formatting.p_decimals), effect(size, formatting.effect_decimals, with_name=False)]))
            gg = e.get("sphericity_correction")
            if gg:
                notes.append(f"{labels.dv(s.dv)} {term}: Greenhouse–Geisser corrected (ε = {number(gg.get('epsilon'), 2)}).")
        for w in s.omnibus.warnings:
            if w.get("code") in RESTATED_CODES and s.omnibus.excluded:
                continue  # restated in full below
            notes.append(f"{labels.dv(s.dv)}: {w.get('message', w.get('code'))}")
        if s.omnibus.excluded:
            notes.append(f"{labels.dv(s.dv)}{f' ({where})' if where else ''}: "
                         f"{Exclusions(**s.omnibus.excluded).sentence()}")
    if not rows:
        return None
    names = _unique(sizes)
    header = ["Measure", "Within", "Effect", "Statistic", "p", names[0] if len(names) == 1 else "Effect size"]
    tests = _unique(s.omnibus.test for s in analysis.strata if s.omnibus and s.omnibus.test)
    caption = (f"Omnibus tests: {_join_words([TESTS[t].label for t in tests if t in TESTS])}; main effects and "
               f"interactions of every grouping layer. Not adjusted for multiple comparisons. "
               + missing_sentence(analysis) + " "
               + _variants_sentence(stats_spec))
    return Grid(title=f"Omnibus ({analysis.analysis.get('id')})", header_rows=[[HeaderCell(h) for h in header]],
                rows=rows, caption=caption, notes=_unique(notes), label_columns=3)


def sweep_grid(report: StatsReport, analysis: AnalysisReport, stats_spec: StatsSpec,
               labels: Labels | None = None) -> Grid | None:
    """The specification curve as a table (S3, S7): each comparison under
    every variant of a sweep, sorted by Δ within the comparison. None when
    nothing was swept."""
    labels = labels or Labels()
    formatting = stats_spec.formatting
    swept = [c for c in analysis.comparisons() if c.variant and not c.refused]
    if not swept:
        return None
    spread = analysis.analysis.get("difference_spread", "ci")
    ci_level = analysis.analysis.get("ci_level", 0.95)
    groups: dict[tuple, list[Comparison]] = {}
    for c in swept:
        rest = {k: v for k, v in c.stratum.items() if k not in c.variant}
        groups.setdefault((c.dv, tuple(rest.items()), tuple(c.context.items()), c.a, c.b), []).append(c)
    rows: list[GridRow] = []
    for (dv, rest, context, a, b), members in groups.items():
        members.sort(key=lambda m: m.estimate if _finite(m.estimate) else math.inf)
        d = _comparison_decimals(analysis, dv, formatting)
        where = labels.key(dv, {**dict(rest), **dict(context)})
        pair = f"{labels.level(dv, members[0].term, a)} − {labels.level(dv, members[0].term, b)}"
        for m in members:
            rows.append(GridRow([labels.dv(dv), where, pair, labels.key(dv, m.variant), _spread_text(m, spread, d),
                                 p_value(m.p, formatting.p_decimals),
                                 p_value(m.p_adjusted, formatting.p_decimals) if m.family else DASH]))
    header = ["Measure", "Within", "Comparison", "Variant",
              {"ci": f"Δ [{ci_level * 100:g}% CI]", "se": "Δ (SE)", "sd_diff": "Δ (SD of differences)"}[spread],
              "p", "p (adj.)"]
    caption = ("Specification curve: each comparison under every variant, sorted by Δ within the comparison. "
               + analysis.family_statement + " " + _variants_sentence(stats_spec))
    return Grid(title=f"Variant sweep ({analysis.analysis.get('id')})", header_rows=[[HeaderCell(h) for h in header]],
                rows=rows, caption=caption, label_columns=4)
