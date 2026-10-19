"""SPM on 1-D curves: rows from sample_curves, clusters through the report and
the formatter (Stage 13, S4)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from conftest import popcorn_table
from scistackplot import LongTable, PlotKind, PlotSpec, Role
from scistackstats import Analysis, FakeEngine, StatsSpec, run, use_engine
from scistackstats.format import apa_sentence, comparisons_grid

NODES = 21


def _curve_table() -> LongTable:
    rng = np.random.default_rng(4)
    rows = []
    for s in range(6):
        base = np.sin(np.linspace(0, np.pi, NODES))
        for session, shift in (("pre", 0.0), ("post", 0.8)):
            for trial in ("1", "2"):
                bump = shift * np.exp(-((np.arange(NODES) - 10) ** 2) / 8.0)
                rows.append({"subject": f"S{s}", "session": session, "trial": trial,
                             "Gait": list(base + bump + rng.normal(0, 0.05, NODES))})
    return LongTable.from_frame(pd.DataFrame(rows), factors=["subject", "session", "trial"], measures=["Gait"],
                                name="Gait", schema_levels=["subject", "session", "trial"])


def _spec() -> StatsSpec:
    design = PlotSpec(measures=["Gait"], roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
                      groups=["session"], kind=PlotKind.BAND)
    return StatsSpec(design=design, comparisons=(Analysis(family="spm", omnibus=False),))


def test_spm_runs_on_curves_and_reports_clusters():
    spec = _spec()
    with use_engine(FakeEngine(default_p=0.01)) as fake:
        report = run(spec, {"Gait": _curve_table()})
    (c,) = report.analyses[0].comparisons()
    assert c.test == "spm_ttest_paired", "session is within subject"
    assert c.clusters and c.threshold == 3.0
    call = next(x for x in fake.calls if x["test"] == "spm_ttest_paired")
    assert call["rows"] == 12, "one curve per subject per session (trials averaged)"
    grid = comparisons_grid(report, report.analyses[0], spec)
    assert grid.rows[0].cells[3] == "nodes 40.0–60.0 (p = .010)"
    assert grid.rows[0].cells[4] == "t* = 3.00"
    assert "SPM{t}" in apa_sentence(c, report.analyses[0], spec.formatting)


def test_spm_on_a_scalar_measure_is_refused():
    spec = StatsSpec(design=PlotSpec(measures=["Ratings"], roles={"brand": Role.GROUP, "subject": Role.COLLAPSE,
                                                                  "trial": Role.COLLAPSE}, groups=["brand"]),
                     comparisons=(Analysis(family="spm"),))
    with use_engine(FakeEngine()):
        report = run(spec, {"Ratings": popcorn_table()})
    assert any("1-D" in r["reason"] for r in report.analyses[0].refusals)


def test_spm_with_the_real_engine():
    pytest.importorskip("spm1d", reason="SPM tests need spm1d (pip install spm1d)")
    report = run(_spec(), {"Gait": _curve_table()})
    (c,) = report.analyses[0].comparisons()
    assert c.refused is None and c.clusters, "the bump at node 10 is supra-threshold"
    assert c.clusters[0]["start"] < 10 < c.clusters[0]["end"]
