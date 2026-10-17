"""The Descriptives tab: summary statistics per DV × stratum × cell (S18).

Every number is ``csvstats.descriptive.summarize`` (through the engine): the
one owner of each formula, the same one the plot's error bars use. This
module only decides *which rows* make a cell, from the :class:`AnalysisFrame`
the comparisons read too.

Layout:
- one **block per DV** (S16);
- one **section per stratum** (separate figures × panels);
- **columns** = every combination of the grouping layers, outermost first
  (nested headers, as the figure's nested ticks read);
- each cell holds every statistic; :class:`~scistackstats.spec.DescriptiveSpec`
  picks what is shown. A combination with no rows in a stratum is a cell with
  ``n = 0``, so every section has the same columns.

``n`` counts units (the plot's sample), because the rows are already
collapsed to one per unit per cell.
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass, field
from typing import Any

from scistacklog import Log

from scistackplot import LongTable

from .engine import get_engine
from .spec import StatsSpec
from .tables import UNIT, AnalysisFrame, AnalysisFrameError, analysis_frame

LAYER = "scistackstats"


@dataclass(frozen=True)
class StatisticInfo:
    id: str
    label: str
    #: Keys of ``csvstats.descriptive.summarize`` this statistic shows.
    keys: tuple[str, ...]


#: The built-in statistics (S18), in picker order.
STATISTICS: dict[str, StatisticInfo] = {
    s.id: s
    for s in (
        StatisticInfo("n", "n", ("n",)),
        StatisticInfo("n_missing", "n missing", ("n_missing",)),
        StatisticInfo("mean", "Mean", ("mean",)),
        StatisticInfo("sd", "SD", ("sd",)),
        StatisticInfo("sem", "SEM", ("sem",)),
        StatisticInfo("ci95", "95% CI (t-based)", ("ci_low", "ci_high")),
        StatisticInfo("median", "Median", ("median",)),
        StatisticInfo("quartiles", "Q1, Q3", ("q1", "q3")),
        StatisticInfo("iqr", "IQR", ("iqr",)),
        StatisticInfo("min", "Min", ("min",)),
        StatisticInfo("max", "Max", ("max",)),
        StatisticInfo("range", "Range", ("range",)),
        StatisticInfo("variance", "Variance", ("variance",)),
        StatisticInfo("cv", "CV (SD / mean)", ("cv",)),
        StatisticInfo("skewness", "Skewness (G1)", ("skewness",)),
        StatisticInfo("kurtosis", "Excess kurtosis (G2)", ("kurtosis",)),
        StatisticInfo("mode", "Mode", ("mode", "mode_count")),
    )
}

#: What each combined cell (``DescriptiveSpec.cell``) shows, and its header.
CELL_LAYOUTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "mean_sd": ("Mean (SD)", ("mean", "sd")),
    "mean_sem": ("Mean (SEM)", ("mean", "sem")),
    "mean_ci95": ("Mean [95% CI]", ("mean", "ci_low", "ci_high")),
    "median_iqr": ("Median [Q1, Q3]", ("median", "q1", "q3")),
    "none": ("", ()),
}


def _clean(stats: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for key, value in stats.items():
        if isinstance(value, float) and not math.isfinite(value):
            value = None
        out[key] = value
    return out


@dataclass
class Cell:
    #: ``{factor: level}``, outermost first; ``{}`` when there are no layers.
    column: dict[str, str]
    stats: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"column": dict(self.column), "stats": _clean(self.stats)}


@dataclass
class Section:
    stratum: dict[str, str]
    cells: list[Cell]
    #: Every unit in the section, across cells.
    n_units: int | None

    def to_dict(self) -> dict[str, Any]:
        return {"stratum": dict(self.stratum), "n_units": self.n_units, "cells": [c.to_dict() for c in self.cells]}


@dataclass
class DVBlock:
    dv: str
    #: Grouping layers, outermost first (the header rows).
    layers: list[str]
    #: Every column combination, outermost first, in level order.
    columns: list[dict[str, str]]
    unit: str | None
    sections: list[Section] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    refused: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "dv": self.dv, "layers": self.layers, "columns": self.columns, "unit": self.unit,
            "sections": [s.to_dict() for s in self.sections], "notes": self.notes, "refused": self.refused,
        }


@dataclass
class DescriptivesTable:
    blocks: list[DVBlock]
    cell: str
    statistics: list[str]
    engine: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        header, keys = CELL_LAYOUTS[self.cell]
        return {
            "cell": self.cell, "cell_header": header, "cell_keys": list(keys),
            "statistics": [{"id": s, "label": STATISTICS[s].label, "keys": list(STATISTICS[s].keys)}
                           for s in self.statistics],
            "blocks": [b.to_dict() for b in self.blocks],
            "engine": dict(self.engine),
        }


def _columns(frame: AnalysisFrame) -> list[dict[str, Any]]:
    layers = frame.outer_first
    if not layers:
        return [{}]
    return [dict(zip(layers, combo)) for combo in itertools.product(*(frame.levels[n] for n in layers))]


def describe_frame(frame: AnalysisFrame) -> DVBlock:
    engine = get_engine()
    layers = frame.outer_first
    columns = _columns(frame)
    block = DVBlock(
        dv=frame.dv.key, layers=layers,
        columns=[{k: str(v) for k, v in c.items()} for c in columns],
        unit=frame.unit, notes=list(frame.notes),
    )
    for stratum, rows in frame.stratum_groups():
        cells = []
        for column in columns:
            part = rows
            for name, level in column.items():
                part = part[part[name] == level]
            cells.append(Cell({k: str(v) for k, v in column.items()}, engine.summarize(part[frame.y].to_numpy())))
        n_units = int(rows[UNIT].nunique()) if frame.unit is not None else None
        block.sections.append(Section({k: str(v) for k, v in stratum.items()}, cells, n_units))
    return block


def describe_table(stats_spec: StatsSpec, tables: dict[str, LongTable]) -> DescriptivesTable:
    """The Descriptives tab for every DV of ``stats_spec``.

    ``tables`` maps each DV's variable to its LongTable (``tables.load_tables``).
    A DV whose rows cannot be built is a block with ``refused`` set. It is
    reported, not raised, so one bad DV never blanks the table.
    """
    unknown = [s for s in stats_spec.descriptives.statistics if s not in STATISTICS]
    if unknown:
        raise ValueError(f"unknown descriptive statistic(s) {unknown}; choose from {list(STATISTICS)}")
    engine = get_engine()
    blocks: list[DVBlock] = []
    with Log.timer("describe", layer=LAYER, extra=f"{len(stats_spec.targets)} DV(s)") as timing:
        for dv in stats_spec.targets:
            with timing.phase(f"dv {dv.key}"):
                try:
                    frame = analysis_frame(stats_spec, dv, tables[dv.variable])
                except (AnalysisFrameError, KeyError) as exc:
                    reason = str(exc) if isinstance(exc, AnalysisFrameError) else f"no table loaded for {dv.variable}"
                    Log.warn("[describe] %s refused: %s", dv.key, reason, layer=LAYER)
                    blocks.append(DVBlock(dv=dv.key, layers=[], columns=[], unit=None, refused=reason))
                    continue
                block = describe_frame(frame)
                ns = [c.stats.get("n", 0) for s in block.sections for c in s.cells]
                Log.info(
                    "[describe] %s: %d section(s) x %d column(s), n per cell %s..%s",
                    dv.key, len(block.sections), len(block.columns),
                    min(ns) if ns else 0, max(ns) if ns else 0, layer=LAYER,
                )
                blocks.append(block)
    return DescriptivesTable(
        blocks=blocks, cell=stats_spec.descriptives.cell,
        statistics=list(stats_spec.descriptives.statistics), engine=engine.versions(),
    )
