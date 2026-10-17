"""Stats through a real scidb database: the same loading the figures use."""

from __future__ import annotations

import pytest

from conftest import SUBJECTS
from scistackplot import PlotKind, PlotSpec, Role
from scistackstats import Analysis, StatsSpec, describe_table, run
from scistackstatsdb import load_stats_tables


def _spec() -> StatsSpec:
    plot = PlotSpec(
        measures=["StatStepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
    )
    return StatsSpec(design=plot, comparisons=(Analysis(omnibus=False),))


def test_descriptives_and_a_paired_test_from_the_database(seeded):
    spec = _spec()
    tables = load_stats_tables(seeded, spec)
    (block,) = describe_table(spec, tables).blocks
    assert block.unit == "subject"
    assert {c.stats["n"] for c in block.sections[0].cells} == {len(SUBJECTS)}

    report = run(spec, tables).analyses[0]
    (comparison,) = report.comparisons()
    assert comparison.test == "ttest_paired"
    assert {comparison.a, comparison.b} == {"pre", "post"}
    # post - pre = 0.1 + 0.01 * subject, so the mean difference is exact.
    expected = 0.1 + 0.01 * sum(int(s) for s in SUBJECTS) / len(SUBJECTS)
    sign = 1 if comparison.a == "post" else -1
    assert comparison.estimate == pytest.approx(sign * expected)
