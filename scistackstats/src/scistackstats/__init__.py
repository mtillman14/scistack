"""
scistackstats — statistics from the same design a figure draws.

StatStudio's engine, usable from code (the GUI is a thin wrapper). The design
is a plot-layer ``PlotSpec`` (grouping layers = factors, separate figures and
panels = strata, collapse chain = unit of analysis), so a table and a figure
built from the same design summarise the same rows. Every number is computed
by csv-stats (``csvstats``), through :mod:`scistackstats.engine`.

::

    from scistackplot import PlotSpec, Role, PlotKind
    from scistackstats import seed_from_plot, load_tables, describe_table, run

    plot = PlotSpec(measures=["Ratings"], roles={"brand": Role.GROUP, "subject": Role.COLLAPSE},
                    groups=["brand"], kind=PlotKind.BAR)
    spec = seed_from_plot(plot).spec
    tables = load_tables(source, spec)          # any scistackplot DataSource
    descriptives = describe_table(spec, tables)  # mean (SD) per brand, across subjects
    report = run(spec, tables)                   # paired comparisons of the brands, Holm-adjusted
    print(export_comparisons(report, spec, "md", tables=tables))  # txt md csv latex html json

Design: ``docs/claude/statistics-design.md``; plan ``.claude/plan-statistics.md``.
"""

from __future__ import annotations

from .codegen import NotExportable, generate_stats_function, why_not_exportable
from .catalog import TESTS, available_tests, select_tests, why_unavailable
from .describe import CELL_LAYOUTS, STATISTICS, DescriptivesTable, describe_table
from .design import Design, Factor, Tiers, comparison_tiers, derive_design
from .effects import EFFECT_SIZES, DifferenceSpec, default_effect_size, difference_spec
from .engine import CsvStatsEngine, EngineRefusal, FakeEngine, get_engine, use_engine
from .model import ModelCheck, apply_to_design, default_formula, for_frame
from .export import FORMATS, export_comparisons, export_descriptives, render
from .format import Grid, Labels, apa_sentence, comparisons_grid, descriptives_grids, labels_for, omnibus_grid, sweep_grid
from .run import Comparison, StatsReport, run
from .seed import SeedError, Seeded, seed_from_plot
from .spec import GUIDE_STEPS, DV, Analysis, DescriptiveSpec, FormatSpec, GuideState, StatsSpec, StatsSpecError
from .explain import CONCEPTS, Concept, StepGuide, build_steps
from .guide import GuideFacts, guide, guide_facts
from .tables import (ANALYSIS_KIND, UNIT, AnalysisFrame, AnalysisFrameError, CovariateValues, analysis_frame,
                     load_covariates, load_tables)

__all__ = [
    "ANALYSIS_KIND", "Analysis", "AnalysisFrame", "AnalysisFrameError", "CELL_LAYOUTS", "Comparison",
    "CsvStatsEngine", "DV", "DescriptiveSpec", "DescriptivesTable", "Design", "DifferenceSpec",
    "EFFECT_SIZES", "EngineRefusal", "Factor", "FakeEngine", "STATISTICS", "SeedError", "Seeded",
    "StatsReport", "StatsSpec", "StatsSpecError", "TESTS", "Tiers", "UNIT", "analysis_frame",
    "available_tests", "comparison_tiers", "default_effect_size", "derive_design", "describe_table",
    "difference_spec", "get_engine", "load_tables", "run", "seed_from_plot", "select_tests",
    "use_engine", "why_unavailable",
    "FORMATS", "FormatSpec", "Grid", "Labels", "apa_sentence", "comparisons_grid", "descriptives_grids",
    "export_comparisons", "export_descriptives", "labels_for", "omnibus_grid", "render", "sweep_grid",
    "NotExportable", "generate_stats_function", "why_not_exportable",
    "CovariateValues", "ModelCheck", "apply_to_design", "default_formula", "for_frame", "load_covariates",
    "CONCEPTS", "Concept", "GUIDE_STEPS", "GuideFacts", "GuideState", "StepGuide", "build_steps", "guide",
    "guide_facts",
]

try:
    from importlib import metadata as _metadata

    __version__ = _metadata.version("scistackstats")
except Exception:  # pragma: no cover - running from a source tree
    __version__ = "0.0.0"
