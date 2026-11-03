"""The categorical rewrite is a reduction step, applied by ``reduce`` and
printed by codegen (P5 stage 6, design review 06 P2).

``reduction.CategoricalRewrite`` holds the rule and
``categorical.categorical_step`` makes every choice (level order, record
filters, the one-hot's pairs); ``categorical.apply_categorical`` applies the
step and ``export.reduction_code.emit_categorical`` prints it. The emitted
pandas is executed on the frame the step rewrites and must give the same
frame (the standing "export equals preview" rule).

Regression guarded: codegen's own copy took the category order from the
table's DECLARED levels only, while the preview appended any text those did
not name (or, with none declared, read the order off the data). A strip of
categories a source had not listed came out all-NaN in the export.
"""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from scistackplot import LEVEL_FACTOR, CategoricalOptions, CategoricalStat, LongTable, PlotKind
from scistackplot import PlotSpec, Role, resolve
from scistackplot.export.codegen import generate_plot_function
from scistackplot.export.reduction_code import emit_categorical
from scistackplot.model.spec import Filter
from scistackplot.reduce.categorical import apply_categorical, categorical_step
from scistackplot.reduce.reduction import CategoricalRewrite, CollapseLevels

KEYS = ["subject", "session", "trial"]


def _table() -> LongTable:
    frame = pd.DataFrame(
        {
            "subject": ["1", "1", "1", "2", "2", "2"],
            "session": ["1", "1", "2", "1", "2", "2"],
            "trial": ["1", "2", "1", "1", "1", "2"],
            "Outcome": ["fall", "ok", None, "ok", "trip", "fall"],
        }
    )
    return LongTable.from_frame(frame, factors=KEYS, measures=["Outcome"], schema_levels=KEYS)


def _spec(kind: PlotKind, stat: CategoricalStat, level: str | None = None, **extra) -> PlotSpec:
    roles = {"subject": Role.COLLAPSE, "session": Role.COLLAPSE, "trial": Role.COLLAPSE}
    if kind is not PlotKind.STRIP and level is None:
        roles[LEVEL_FACTOR] = Role.GROUP
    return PlotSpec(
        measures=["Outcome"],
        kind=kind,
        roles=roles,
        categorical=CategoricalOptions(stat=stat, level=level),
        **extra,
    )


CASES = {
    "count": _spec(PlotKind.BAR, CategoricalStat.COUNT),
    "proportion": _spec(PlotKind.BAR, CategoricalStat.PROPORTION),
    "one-category": _spec(PlotKind.BAR, CategoricalStat.PROPORTION, level="fall"),
    "positions": _spec(PlotKind.STRIP, CategoricalStat.VALUE),
    "record-filters": _spec(
        PlotKind.BAR,
        CategoricalStat.COUNT,
        filters=[
            Filter("Outcome", include=["fall", "ok", "trip"]),
            Filter("Outcome", exclude=["trip"]),
        ],
    ),
}


def _run(lines: list[str], frame: pd.DataFrame) -> pd.DataFrame:
    namespace: dict = {"df": frame.copy(), "pd": pd, "np": np}
    exec("\n".join(lines), namespace)  # noqa: S102
    return namespace["df"]


@pytest.mark.parametrize("name", list(CASES))
def test_emitted_rewrite_matches_the_step(name):
    table = _table()
    step = categorical_step(CASES[name], table)
    assert step is not None
    expected = step.apply(table.frame)
    got = _run(emit_categorical(step), table.frame)
    pd.testing.assert_frame_equal(got, expected)


@pytest.mark.parametrize("name", list(CASES))
def test_apply_categorical_is_the_step(name):
    table = _table()
    step = categorical_step(CASES[name], table)
    assert step is not None
    _spec_after, rewritten = apply_categorical(CASES[name], table)
    pd.testing.assert_frame_equal(rewritten.frame, step.apply(table.frame))


def test_undeclared_categories_are_still_positions():
    """A source that listed no levels: the order comes from the data, in
    the export as in the preview."""
    table = _table()
    table = replace(
        table, measures=[replace(m, levels=[]) for m in table.measures]
    )
    step = categorical_step(CASES["positions"], table)
    assert isinstance(step, CategoricalRewrite) and step.levels
    got = _run(emit_categorical(step), table.frame)
    assert bool(got["Outcome"].notna().all())


def test_a_numeric_measure_has_no_step(scalar_table):
    assert categorical_step(PlotSpec(measures=["StepLength"]), scalar_table) is None


def test_the_generated_function_prints_the_step():
    table = _table()
    spec = CASES["proportion"]
    source = generate_plot_function(spec, table)
    step = categorical_step(spec, table)
    assert step is not None
    for line in emit_categorical(step):
        assert line in source


def test_the_figure_records_the_rewrite():
    figure = resolve(CASES["count"], _table())[0]
    kinds = [type(step) for step in figure.reduction.steps]
    # The shared step, then the figure's collapse chain (pooled for text).
    assert kinds[0] is CategoricalRewrite
    assert set(kinds[1:]) <= {CollapseLevels}
