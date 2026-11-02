"""What a figure leaves out — the one owner of "Not plotted" (user, 2026-11-01).

Plan: `.claude/plan-plot-omissions.md`. User rule: data is never dropped
silently ([[feedback_never_drop_data]]). Every place a figure loses or
thins a value reports it HERE, with one of these reasons, and this module
owns their wording; the GUI's "Not plotted" button and popup, the optional
footnote and the log line only display it.

| reason | meaning | lost? |
|---|---|---|
| ``no_value`` | the record exists but its value is empty (NaN / NULL) | yes |
| ``no_record`` | the dataset has the location; this variable has no record there (``scidb.provenance.inventory.absent_locations``) | yes |
| ``outside_range`` | a histogram value outside the typed From / To range | yes |
| ``nan_elements`` | some entries inside a vector are NaN | no: thinned |
| ``partly_averaged`` | a collapsed average used fewer members than exist | no: thinned |

Removed on purpose (location filter, Filters, exclusions) is never here.
A text measure draws a missing value as its own category
(``table.category_text``), so only ``no_record`` can apply to one. A
histogram with its NaN bar shown DRAWS its NaNs (``bins``), so they are not
``no_value``. A custom ``plot_`` function gets ``no_value`` and ``no_record``
only: what the function itself drops cannot be known.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scistacklog import Log

LAYER = "scistackplot"

NO_VALUE = "no_value"
NO_RECORD = "no_record"
OUTSIDE_RANGE = "outside_range"
NAN_ELEMENTS = "nan_elements"
PARTLY_AVERAGED = "partly_averaged"

#: The popup's / footnote's name for each reason, in display order.
REASON_LABELS = {
    NO_VALUE: "No value",
    NO_RECORD: "No record",
    OUTSIDE_RANGE: "Outside the bin range",
    NAN_ELEMENTS: "NaN elements",
    PARTLY_AVERAGED: "Partly averaged",
}
#: Reasons whose value is not drawn at all (the "N of M not plotted" count);
#: the others are drawn from fewer members than exist.
LOST = (NO_VALUE, NO_RECORD, OUTSIDE_RANGE)

#: The most items carried to the GUI (the popup lists these; the counts are
#: always complete). The Data inventory popup's cap.
MAX_ITEMS = 5000


@dataclass(frozen=True)
class Omission:
    """One left-out (or thinned) value: where it is, which variable, why."""

    keys: dict[str, str]
    variable: str
    reason: str
    detail: str = ""

    def to_dict(self) -> dict:
        return {
            "keys": dict(self.keys),
            "variable": self.variable,
            "reason": self.reason,
            "label": REASON_LABELS[self.reason],
            "detail": self.detail,
        }


@dataclass
class OmissionReport:
    """One figure's omissions. ``drawn``: the values the figure draws."""

    drawn: int = 0
    items: list[Omission] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        found = {reason: 0 for reason in REASON_LABELS}
        for item in self.items:
            found[item.reason] += 1
        return {reason: n for reason, n in found.items() if n}

    @property
    def lost(self) -> int:
        return sum(n for reason, n in self.counts().items() if reason in LOST)

    @property
    def offered(self) -> int:
        return self.drawn + self.lost

    def button_text(self) -> str:
        """The toolbar button: "Not plotted: 3 of 120", or "Partly plotted:
        2" when nothing is lost but some values are thinned. Empty: no button."""
        if self.lost:
            return f"Not plotted: {self.lost} of {self.offered}"
        if self.items:
            return f"Partly plotted: {len(self.items)}"
        return ""

    def sentence(self) -> str:
        """The footnote / log sentence: "3 of 120 values not plotted (2 no
        value, 1 no record); 2 partly averaged"."""
        counts = self.counts()
        lost = [f"{counts[r]} {REASON_LABELS[r].lower()}" for r in LOST if r in counts]
        thinned = [
            f"{counts[r]} {REASON_LABELS[r].lower()}" for r in REASON_LABELS if r in counts and r not in LOST
        ]
        parts = []
        if lost:
            parts.append(f"{self.lost} of {self.offered} values not plotted ({', '.join(lost)})")
        if thinned:
            parts.append(", ".join(thinned))
        return "; ".join(parts)

    def to_dict(self) -> dict:
        return {
            "drawn": self.drawn,
            "lost": self.lost,
            "offered": self.offered,
            "counts": self.counts(),
            "labels": {r: REASON_LABELS[r] for r in self.counts()},
            "button": self.button_text(),
            "sentence": self.sentence(),
            "items": [item.to_dict() for item in self.items[:MAX_ITEMS]],
            "truncated": max(len(self.items) - MAX_ITEMS, 0),
        }


def _keys_of(row: dict, keys: list[str]) -> dict[str, str]:
    return {name: str(row[name]) for name in keys if name in row and row[name] == row[name]}


def no_value(sample: pd.DataFrame, measures: list[str], keys: list[str]) -> list[Omission]:
    """Sample rows whose value is empty (any of ``measures``: a regression
    point needs its x and its y)."""
    present = [m for m in measures if m in sample.columns]
    if not present or sample.empty:
        return []
    empty = sample[present].apply(pd.to_numeric, errors="coerce").isna()
    mask = empty.any(axis=1).to_numpy()
    items = []
    for row, flags in zip(sample[mask].to_dict(orient="records"), empty[mask].to_numpy(), strict=True):
        for measure, flag in zip(present, flags, strict=True):
            if flag:
                items.append(Omission(_keys_of(row, keys), measure, NO_VALUE))
    return items


def partly_averaged(
    raw: pd.DataFrame, sample: pd.DataFrame, measure: str, keys: list[str]
) -> list[Omission]:
    """Sample values averaged from fewer members than exist: the raw rows
    under one sample unit (its ``keys``) where some, not all, had no value.
    All of them empty is ``no_value`` on the sample row instead."""
    group = [name for name in keys if name in raw.columns and name in sample.columns]
    if measure not in raw.columns or raw.empty or len(raw) == len(sample):
        return []
    values = pd.to_numeric(raw[measure], errors="coerce")
    frame = pd.DataFrame({"empty": values.isna().to_numpy()}, index=raw.index)
    for name in group:
        frame[name] = raw[name].to_numpy()
    if group:
        stats = frame.groupby(group, dropna=False, sort=False)["empty"].agg(["sum", "count"]).reset_index()
    else:
        stats = pd.DataFrame({"sum": [int(frame["empty"].sum())], "count": [len(frame)]})
    partial = stats[(stats["sum"] > 0) & (stats["sum"] < stats["count"])]
    return [
        Omission(
            _keys_of(row, group),
            measure,
            PARTLY_AVERAGED,
            f"{int(row['count'] - row['sum'])} of {int(row['count'])} records had a value",
        )
        for row in partial.to_dict(orient="records")
    ]


def nan_elements(records: pd.DataFrame, measure: str, keys: list[str]) -> list[Omission]:
    """1-D records with NaN entries: the line / band skips those positions."""
    if measure not in records.columns or records.empty:
        return []
    items = []
    for row in records.to_dict(orient="records"):
        cell = row[measure]
        try:
            array = np.asarray(cell, dtype=float).ravel()
        except (TypeError, ValueError):
            continue
        missing = int(np.isnan(array).sum())
        if missing:
            items.append(
                Omission(
                    _keys_of(row, keys),
                    measure,
                    NAN_ELEMENTS,
                    f"{missing} of {array.size} elements NaN",
                )
            )
    return items


def outside_range(
    sample: pd.DataFrame, measure: str, keys: list[str], low: float, high: float, regular
) -> list[Omission]:
    """Histogram values the regular bins could hold (``regular``: finite, no
    single-value bin) that fall outside the bins' [low, high]."""
    if measure not in sample.columns or sample.empty:
        return []
    values = pd.to_numeric(sample[measure], errors="coerce").to_numpy(dtype=float)
    keep = regular(values)
    out = keep & ((values < low) | (values > high))
    return [
        Omission(_keys_of(row, keys), measure, OUTSIDE_RANGE, f"{value:g} outside {low:g} to {high:g}")
        for row, value in zip(
            sample[out].to_dict(orient="records"), values[out], strict=True
        )
    ]


def no_record(absent: pd.DataFrame | None, figure_key: dict[str, Any], measure: str) -> list[Omission]:
    """The table's absent locations (``LongTable.absent``, already through the
    spec's filters) inside this figure (its ITERATE values)."""
    if absent is None or absent.empty:
        return []
    rows = absent
    for name, value in figure_key.items():
        if name in rows.columns:
            rows = rows[rows[name].astype(str) == str(value)]
    keys = list(absent.columns)
    return [Omission(_keys_of(row, keys), measure, NO_RECORD) for row in rows.to_dict(orient="records")]


def log_report(report: OmissionReport, figure_label: str) -> None:
    """One line per figure: INFO with the counts, WARN when nothing is drawn."""
    if not report.items:
        Log.debug("not plotted (%s): nothing left out", figure_label or "figure", layer=LAYER)
        return
    if report.drawn == 0 and report.lost:
        Log.warn("not plotted (%s): %s — nothing is drawn", figure_label or "figure", report.sentence(), layer=LAYER)
        return
    Log.info("not plotted (%s): %s", figure_label or "figure", report.sentence(), layer=LAYER)


def combine(reports: "list[OmissionReport | None]") -> OmissionReport:
    """Every figure's report as one (the fan-out's total): the toolbar
    button's text for the whole plot. Items and drawn values add up."""
    total = OmissionReport()
    for report in reports:
        if report is None:
            continue
        total.drawn += report.drawn
        total.items.extend(report.items)
    return total
