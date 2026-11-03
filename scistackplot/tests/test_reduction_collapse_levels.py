"""The collapse chain is reduction steps, applied by ``reduce`` and printed by
codegen (P5 stage 6, design review 06 P2).

``chain.collapse_passes`` plans the passes (``reduction.CollapseLevels``);
``resolve._collapse_levels`` applies them, and
``export.reduction_code.emit_collapse_levels`` prints them for the marks'
chain and the "Show sample" overlay. The emitted pandas is executed on the
frame each pass collapses and must give the same frame (the standing "export
equals preview" rule).

Regressions guarded, all from codegen's own groupby lines:

* the export grouped with pandas' default ``dropna=True``, so a row whose
  kept factor was MISSING vanished from the exported figure while the
  preview kept it as a group of its own;
* with nothing left to group by the export called ``groupby([])``, which
  raises; the preview reduces to one row;
* a text measure's ``final`` keys were averaged one pass at a time in the
  export and pooled in one pass in the preview (a % of pooled records is not
  the mean of per-key %s).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import PlotKind, PlotSpec, Role, resolve
from scistackplot.export.codegen import generate_plot_function
from scistackplot.export.reduction_code import emit_collapse_levels
from scistackplot.reduce.chain import collapse_passes
from scistackplot.reduce.reduction import CollapseLevels
from scistackplot.reduce.resolve import _collapse_levels

FACTORS = ("subject", "session", "trial")


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "subject": ["01", "01", "01", "02", "02", "02"],
            "session": ["pre", "pre", None, "pre", "post", "post"],
            "trial": ["1", "2", "1", "1", "1", "2"],
            "M": [1.0, 3.0, 10.0, 2.0, 4.0, 8.0],
        }
    )


def _run(lines: list[str], frame: pd.DataFrame) -> pd.DataFrame:
    namespace: dict = {"df": frame.copy(), "pd": pd, "np": np}
    exec("\n".join(lines), namespace)  # noqa: S102
    return namespace["df"]


STEPS = {
    "nested-trial": CollapseLevels(("trial",), FACTORS, ("M",)),
    "pooled": CollapseLevels(("trial", "session"), FACTORS, ("M",)),
    "sum": CollapseLevels(("trial",), FACTORS, ("M",), how="sum"),
    "everything": CollapseLevels(FACTORS, FACTORS, ("M",)),
}


@pytest.mark.parametrize("name", list(STEPS))
def test_emitted_pass_matches_the_step(name):
    step = STEPS[name]
    expected = step.apply(_frame())
    got = _run(emit_collapse_levels(step, list(_frame().columns)), _frame())
    pd.testing.assert_frame_equal(
        got.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False
    )


def test_a_missing_level_is_a_group_not_a_dropped_row():
    collapsed = STEPS["nested-trial"].apply(_frame())
    assert collapsed["session"].isna().sum() == 1
    assert len(collapsed) == 4


def test_a_pass_whose_key_is_gone_does_nothing():
    step = CollapseLevels(("cycle",), FACTORS, ("M",))
    pd.testing.assert_frame_equal(step.apply(_frame()), _frame())


def test_collapse_levels_applies_the_planned_passes(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.BAR,
    )
    frame = scalar_table.frame
    expected = frame
    for step in collapse_passes(["trial", "subject"], spec, scalar_table, None):
        expected = step.apply(expected)
    got = _collapse_levels(frame, ["trial", "subject"], spec, scalar_table, None)
    pd.testing.assert_frame_equal(got, expected)


def test_nested_or_pooled_passes(scalar_table):
    spec = PlotSpec(measures=["StepLength"])
    nested = collapse_passes(["trial", "session"], spec, scalar_table, None)
    pooled = collapse_passes(["trial", "session"], spec, scalar_table, None, pooled=True)
    assert [step.dropped for step in nested] == [("trial",), ("session",)]
    assert [step.dropped for step in pooled] == [("trial", "session")]
    assert collapse_passes([], spec, scalar_table, None) == []


def _box() -> PlotSpec:
    return PlotSpec(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )


def test_the_generated_function_prints_the_passes(scalar_table):
    source = generate_plot_function(_box(), scalar_table)
    assert "dropna=False, sort=False" in source
    assert "as_index=False" not in source


def test_the_figure_records_its_chain(scalar_table):
    figure = resolve(_box(), scalar_table)[0]
    passes = figure.reduction.of_type(CollapseLevels)
    assert passes and all(isinstance(step, CollapseLevels) for step in passes)
