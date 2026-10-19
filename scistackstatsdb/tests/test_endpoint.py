"""The generated stat_ endpoint runs in a real project and stores one record."""

from __future__ import annotations

import json

import pytest
from scidb import BaseVariable, for_each

from conftest import StatStepLength
from scistackplot import PlotKind, PlotSpec, Role
from scistackstats import Analysis, StatsSpec, run
from scistackstatsdb import generate_stats_endpoint, load_stats_tables, required_declarations


class StatStepLengthStats(BaseVariable):
    schema_version = 1


def _spec() -> StatsSpec:
    plot = PlotSpec(measures=["StatStepLength"], roles={"session": Role.GROUP, "subject": Role.COLLAPSE,
                                                        "trial": Role.COLLAPSE}, groups=["session"], kind=PlotKind.BAR)
    return StatsSpec(design=plot, comparisons=(Analysis(omnibus=False),))


def test_endpoint_source_shape(seeded):
    code = generate_stats_endpoint(_spec(), load_stats_tables(seeded, _spec()))
    assert code.function_name == "stat_statsteplength"
    assert code.output_variable == "StatStepLengthStats"
    assert '"df": StatStepLength' in code.foreach_source
    assert "subject=" not in code.foreach_source, "no iteration keywords (P7)"
    assert "finalized=True" in code.foreach_source
    assert required_declarations(code) == ["StatStepLengthStats"]
    compile(code.source, "<generated>", "exec")


def test_endpoint_runs_and_stores_the_report(seeded):
    spec = _spec()
    tables = load_stats_tables(seeded, spec)
    code = generate_stats_endpoint(spec, tables)
    namespace = {"for_each": for_each, "StatStepLength": StatStepLength,
                 "StatStepLengthStats": StatStepLengthStats}
    exec(compile(code.source, "<generated>", "exec"), namespace)  # noqa: S102
    record = StatStepLengthStats.load()
    stored = json.loads(record.data)
    (comparison,) = stored["analyses"][0]["strata"][0]["primary"]
    (expected,) = run(spec, tables).analyses[0].comparisons()
    assert comparison["test"] == "ttest_paired"
    assert comparison["estimate"] == pytest.approx(expected.estimate)
    assert comparison["p"] == pytest.approx(expected.p)
