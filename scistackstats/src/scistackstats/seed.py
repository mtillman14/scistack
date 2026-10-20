"""Plot → StatStudio settings: the one place that maps a figure to a table (S12).

``seed_from_plot`` is what the StatStudio button in Plot Studio calls, and
what a script calls to start from a saved figure. The design is copied
whole (it is the plot's own ``PlotSpec``); this module decides only the
stats-only settings, one rule at a time, and logs each rule it applied.

| Figure | Seeded table |
|---|---|
| bar/band + SD | mean (SD) |
| bar/band + SEM | mean (SEM), with a note that journals usually report SD |
| bar/band + CI95 | mean [95% CI] (t-based, the same owner as the bars) |
| bar/band + IQR, or a median centre | median [Q1, Q3], rank-based tests |
| box / violin | median [Q1, Q3], rank-based tests |
| spaghetti | mean (SD); a note that the lines' unit is paired |
| scatter / strip / line | mean (SD) |
| compare-to-reference active | refused (S9) |

The tests themselves are chosen from the data at run time
(``catalog.select_tests``): within factors get paired tests, between factors
independent ones.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from scistacklog import Log

from scistackplot import PlotKind, PlotSpec
from scistackplot.spec import ErrorBand, Statistic

from .spec import DV, Analysis, DescriptiveSpec, GuideState, StatsSpec

LAYER = "scistackstats"


class SeedError(ValueError):
    """This figure cannot seed a StatStudio table; the message says why."""


@dataclass
class Seeded:
    spec: StatsSpec
    #: One line per rule applied, for the GUI's "set up from the figure" note.
    notes: list[str] = field(default_factory=list)


def seed_from_plot(plot: PlotSpec, *, linked_plot: str | None = None) -> Seeded:
    """A StatsSpec set up from ``plot`` (module docstring)."""
    if plot.comparison is not None and plot.comparison.active:
        raise SeedError(
            "this figure compares to a reference level; statistics on compared values are not "
            "designed yet (S9). Turn off Compare to reference to open StatStudio."
        )
    notes: list[str] = []
    error = plot.aggregate.error
    median = plot.aggregate.statistic is Statistic.MEDIAN
    if plot.kind in (PlotKind.BOX, PlotKind.VIOLIN):
        cell, family = "median_iqr", "nonparametric"
        notes.append(f"{plot.kind} plot: median [Q1, Q3], rank-based tests")
    elif error is ErrorBand.IQR or median:
        cell, family = "median_iqr", "nonparametric"
        notes.append("median / IQR in the figure: median [Q1, Q3], rank-based tests")
    elif error is ErrorBand.SEM:
        cell, family = "mean_sem", "auto"
        notes.append("± SEM in the figure: mean (SEM). Journals usually ask for SD in a descriptive table")
    elif error is ErrorBand.CI95:
        cell, family = "mean_ci95", "auto"
        notes.append("95% CI in the figure: mean [95% CI], t-based like the bars")
    else:
        cell, family = "mean_sd", "auto"
        notes.append(f"{'± SD' if error is ErrorBand.SD else 'no error bars'} in the figure: mean (SD)")
    if plot.kind is PlotKind.SPAGHETTI:
        notes.append("spaghetti plot: its lines join one unit across levels, so those comparisons are paired")
    spec = StatsSpec(
        design=plot,
        dvs=(DV(plot.y_measure),),
        linked_plot=linked_plot,
        descriptives=DescriptiveSpec(cell=cell, statistics=("n",)),
        comparisons=(Analysis(id="a1", family=family),),
        # A new table opens in the step-by-step wizard (user decision
        # 2026-10-20); a saved one reopens in the view it was saved in.
        guide=GuideState(mode="guided"),
    )
    for note in notes:
        Log.info("[seed] %s: %s", plot.y_measure, note, layer=LAYER)
    return Seeded(spec=spec, notes=notes)
