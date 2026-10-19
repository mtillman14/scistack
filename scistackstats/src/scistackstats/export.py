"""Render :class:`~scistackstats.format.Grid` tables as text formats (S19).

Every format carries the caption and the notes (including the family
statement, S7). No number is formatted here: the grids already hold the
strings ``format`` wrote, so a CSV, a LaTeX table and the GUI cannot
disagree.

- ``txt``: aligned plain text;
- ``md``: a pipe table (nested headers joined with ·);
- ``csv``: header + rows, then a blank row and the caption/notes as rows;
- ``latex``: booktabs, ``\\multicolumn`` + ``\\cmidrule`` for nested headers,
  with symbols mapped to math so pdflatex needs no unicode setup;
- ``html``: a ``<table>`` with ``<caption>``, spanning header cells and a
  notes footer, inline APA-style rules. Pasted into Word it stays a table;
- ``json``: the grids as data (plus the full-precision results when given).
"""

from __future__ import annotations

import csv
import html
import io
import json
from typing import Any

from scistacklog import Log

from scistackplot import LongTable

from .describe import DescriptivesTable
from .format import Grid, Labels, comparisons_grid, descriptives_grids, labels_for, omnibus_grid, sweep_grid
from .run import StatsReport
from .spec import StatsSpec

LAYER = "scistackstats"
FORMATS = ("txt", "md", "csv", "latex", "html", "json")


def _txt(grid: Grid) -> str:
    header = grid.flat_header()
    body = [r.cells for r in grid.rows]
    widths = [max([len(header[i])] + [len(r[i]) for r in body if i < len(r)]) for i in range(len(header))]
    lines = [grid.title, ""]
    lines.append("  ".join(h.ljust(w) for h, w in zip(header, widths)).rstrip())
    lines.append("  ".join("-" * w for w in widths))
    for row in grid.rows:
        if row.kind == "section":
            lines.append(row.cells[0])
        else:
            lines.append("  ".join(c.ljust(w) for c, w in zip(row.cells, widths)).rstrip())
    lines += ["", f"Note. {grid.caption}", *grid.notes]
    return "\n".join(lines)


def _md_cell(text: str) -> str:
    return text.replace("|", "\\|")


def _md(grid: Grid) -> str:
    header = grid.flat_header()
    lines = [f"**{_md_cell(grid.title)}**", "", "| " + " | ".join(_md_cell(h) for h in header) + " |",
             "|" + "|".join("---" for _ in header) + "|"]
    for row in grid.rows:
        cells = [f"*{row.cells[0]}*"] + [""] * (len(header) - 1) if row.kind == "section" else row.cells
        lines.append("| " + " | ".join(_md_cell(c) for c in cells) + " |")
    lines += ["", f"*Note.* {_md_cell(grid.caption)}"] + [f"- {_md_cell(n)}" for n in grid.notes]
    return "\n".join(lines)


def _csv(grid: Grid) -> str:
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(grid.flat_header())
    for row in grid.rows:
        writer.writerow(row.cells)
    writer.writerow([])
    writer.writerow([f"Note. {grid.caption}"])
    for note in grid.notes:
        writer.writerow([note])
    return out.getvalue()


_LATEX_SPECIAL = {
    "\\": r"\textbackslash{}", "&": r"\&", "%": r"\%", "$": r"\$", "#": r"\#",
    "_": r"\_", "{": r"\{", "}": r"\}", "~": r"\textasciitilde{}", "^": r"\textasciicircum{}",
}
_LATEX_SYMBOLS = {
    "−": "$-$", "Δ": r"$\Delta$", "η²": r"$\eta^2$", "ω²": r"$\omega^2$", "ε²": r"$\varepsilon^2$",
    "χ²": r"$\chi^2$", "ε": r"$\varepsilon$", "×": r"$\times$", "·": r"$\cdot$", "–": "--", "—": "---",
    "≥": r"$\geq$", "≤": r"$\leq$", "<": r"$<$", ">": r"$>$", "’": "'",
}


def latex_escape(text: str) -> str:
    out = "".join(_LATEX_SPECIAL.get(ch, ch) for ch in str(text))
    for symbol, replacement in sorted(_LATEX_SYMBOLS.items(), key=lambda kv: -len(kv[0])):
        out = out.replace(symbol, replacement)
    return out


def _latex(grid: Grid) -> str:
    width = grid.width
    lines = [r"\begin{table}[htbp]", r"\centering",
             rf"\caption{{{latex_escape(grid.title)}}}",
             r"\begin{tabular}{" + "l" * grid.label_columns + "c" * (width - grid.label_columns) + "}",
             r"\toprule"]
    for row in grid.header_rows:
        cells, rules, col = [], [], 1
        for cell in row:
            text = latex_escape(cell.text)
            if cell.span > 1:
                cells.append(rf"\multicolumn{{{cell.span}}}{{c}}{{{text}}}")
                if cell.text:
                    rules.append(rf"\cmidrule(lr){{{col}-{col + cell.span - 1}}}")
            else:
                cells.append(text)
            col += cell.span
        lines.append(" & ".join(cells) + r" \\")
        if rules and row is not grid.header_rows[-1]:
            lines.append(" ".join(rules))
    lines.append(r"\midrule")
    for row in grid.rows:
        if row.kind == "section":
            lines.append(rf"\multicolumn{{{width}}}{{l}}{{\textit{{{latex_escape(row.cells[0])}}}}} \\")
        else:
            lines.append(" & ".join(latex_escape(c) for c in row.cells) + r" \\")
    lines += [r"\bottomrule", r"\end{tabular}",
              rf"\par\smallskip\parbox{{\linewidth}}{{\footnotesize \textit{{Note.}} {latex_escape(grid.caption)}"
              + "".join(rf" {latex_escape(n)}" for n in grid.notes) + "}",
              r"\end{table}"]
    return "\n".join(lines)


_RULE = "border-top:1px solid #000;"
_CELL = "padding:2px 8px;font-family:inherit;"


def _html(grid: Grid) -> str:
    e = html.escape
    parts = [f'<table style="border-collapse:collapse;{_RULE}border-bottom:1px solid #000;">',
             f'<caption style="caption-side:top;text-align:left;font-style:italic;padding-bottom:4px;">{e(grid.title)}</caption>',
             "<thead>"]
    for i, row in enumerate(grid.header_rows):
        last = i == len(grid.header_rows) - 1
        style = _CELL + ("border-bottom:1px solid #000;" if last else "")
        cells = "".join(
            f'<th colspan="{c.span}" style="{style}text-align:{"left" if j < grid.label_columns else "center"};'
            f'font-weight:normal;">{e(c.text)}</th>' if c.span > 1 else
            f'<th style="{style}text-align:{"left" if j < grid.label_columns else "center"};font-weight:normal;">{e(c.text)}</th>'
            for j, c in enumerate(row)
        )
        parts.append(f"<tr>{cells}</tr>")
    parts += ["</thead>", "<tbody>"]
    for row in grid.rows:
        if row.kind == "section":
            parts.append(f'<tr><td colspan="{grid.width}" style="{_CELL}font-style:italic;">{e(row.cells[0])}</td></tr>')
        else:
            parts.append("<tr>" + "".join(
                f'<td style="{_CELL}text-align:{"left" if j < grid.label_columns else "center"};">{e(c)}</td>'
                for j, c in enumerate(row.cells)) + "</tr>")
    notes = "".join(f"<br>{e(n)}" for n in grid.notes)
    parts += ["</tbody>",
              f'<tfoot><tr><td colspan="{grid.width}" style="{_CELL}{_RULE}font-size:90%;">'
              f"<i>Note.</i> {e(grid.caption)}{notes}</td></tr></tfoot>",
              "</table>"]
    return "\n".join(parts)


_RENDER = {"txt": _txt, "md": _md, "csv": _csv, "latex": _latex, "html": _html}


def render(grids: list[Grid], fmt: str, *, data: Any = None) -> str:
    """``grids`` in ``fmt``; ``data`` (a ``to_dict()``) rides along in json."""
    if fmt not in FORMATS:
        raise ValueError(f"unknown export format {fmt!r}; choose one of {list(FORMATS)}")
    if fmt == "json":
        payload: dict[str, Any] = {"grids": [g.to_dict() for g in grids]}
        if data is not None:
            payload["data"] = data
        return json.dumps(payload, indent=2, ensure_ascii=False, default=str)
    sep = "\n\n" if fmt != "csv" else "\n"
    text = sep.join(_RENDER[fmt](g) for g in grids)
    Log.info("[export] %s: %d table(s), %d character(s)", fmt, len(grids), len(text), layer=LAYER)
    return text


def export_descriptives(
    table: DescriptivesTable,
    stats_spec: StatsSpec,
    fmt: str,
    *,
    tables: dict[str, LongTable] | None = None,
    labels: Labels | None = None,
    report: StatsReport | None = None,
) -> str:
    """The Descriptives tab in ``fmt`` (aliases from ``tables`` when given)."""
    labels = labels or (labels_for(stats_spec, tables) if tables else None)
    grids = descriptives_grids(table, stats_spec, labels, report)
    return render(grids, fmt, data=table.to_dict())


def export_comparisons(
    report: StatsReport,
    stats_spec: StatsSpec,
    fmt: str,
    *,
    tables: dict[str, LongTable] | None = None,
    labels: Labels | None = None,
) -> str:
    """Every analysis' primary comparisons, then its omnibus table, in ``fmt``."""
    labels = labels or (labels_for(stats_spec, tables) if tables else None)
    grids: list[Grid] = []
    for analysis in report.analyses:
        grids.append(comparisons_grid(report, analysis, stats_spec, labels))
        omnibus = omnibus_grid(report, analysis, stats_spec, labels)
        if omnibus is not None:
            grids.append(omnibus)
        swept = sweep_grid(report, analysis, stats_spec, labels)
        if swept is not None:
            grids.append(swept)
    return render(grids, fmt, data=report.to_dict())
