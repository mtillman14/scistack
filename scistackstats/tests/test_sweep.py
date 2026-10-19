"""Variant sweep: every variant its own stratum, a variants family, and the
specification curve table (Stage 13, S3/S7)."""

from __future__ import annotations

import pandas as pd

from conftest import BRANDS, bar_spec, popcorn_frame
from scistackplot import LongTable
from scistackstats import Analysis, FakeEngine, StatsSpec, run, use_engine
from scistackstats.format import sweep_grid


def _swept_table() -> LongTable:
    parts = []
    for low_hz, offset in (("10", 0.0), ("20", 0.5)):
        part = popcorn_frame().assign(low_hz=low_hz)
        part["Ratings"] = part["Ratings"] + offset
        parts.append(part)
    frame = pd.concat(parts, ignore_index=True)
    return LongTable.from_frame(
        frame, factors=["subject", "brand", "trial", "low_hz"], measures=["Ratings"], name="Ratings",
        schema_levels=["subject", "trial"], variant_factors=["low_hz"], level_order={"brand": BRANDS},
    )


def _run(scope: str = "stratum"):
    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(family_scope=scope),), variant_mode="sweep")
    with use_engine(FakeEngine()):
        return spec, run(spec, {"Ratings": _swept_table()})


def test_each_variant_is_its_own_stratum():
    _, report = _run()
    analysis = report.analyses[0]
    assert len(analysis.strata) == 2
    assert {c.variant["low_hz"] for c in analysis.comparisons()} == {"10", "20"}
    assert report.dvs[0]["variant_axes"] == ["low_hz"]


def test_the_variants_family_corrects_one_comparison_across_variants():
    _, report = _run("variants")
    families = report.analyses[0].families
    assert len(families) == 3, "one per brand pair"
    assert {len(f.members) for f in families} == {2}
    assert "across the variants" in report.analyses[0].family_statement


def test_specification_curve_table():
    spec, report = _run()
    grid = sweep_grid(report, report.analyses[0], spec)
    assert grid is not None and len(grid.rows) == 6
    assert [r.cells[3] for r in grid.rows[:2]] == ["low_hz = 10", "low_hz = 20"] or \
           [r.cells[3] for r in grid.rows[:2]] == ["low_hz = 20", "low_hz = 10"]
    assert "swept" in grid.caption


def test_a_pinned_spec_has_no_curve(table):
    spec = StatsSpec(design=bar_spec(), comparisons=(Analysis(),))
    with use_engine(FakeEngine()):
        report = run(spec, {"Ratings": table})
    assert sweep_grid(report, report.analyses[0], spec) is None
