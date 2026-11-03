"""The 1-D cell collapse is a reduction step, applied by ``reduce`` and printed
by codegen (P5 stage 6, design review 06 P2).

``reduction.CollapseCells`` holds the rule; ``cell.apply_cell_collapse``
applies it and ``export.reduction_code.emit_collapse_cells`` prints it. The
emitted pandas is executed on the frames the step collapses and must give the
same values (the standing "export equals preview" rule).

Regressions guarded, all from codegen's own copy of the rule:

* a cell that is not an array (a lone number in a 1-D column) had no
  samples in the preview (NaN) and was its own one-sample vector in the
  export;
* a text array was a NaN gap in the preview and raised ``ValueError`` in the
  export (``np.asarray(..., dtype="float64")``);
* a pre-exploded measure (one row per sample) was grouped back to one row
  per record in the preview and left one row per sample in the export.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import PlotKind, PlotSpec, Role, resolve
from scistackplot.export.codegen import generate_plot_function
from scistackplot.export.reduction_code import emit_collapse_cells, emit_step
from scistackplot.model.spec import Filter, Statistic
from scistackplot.reduce.cell import apply_cell_collapse, collapse_step
from scistackplot.reduce.reduction import CollapseCells, CollapseLevels, ValueFilter


def _cells() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trial": ["1", "2", "3", "4", "5", "6", "7", "8"],
            "Signal": [
                np.array([1.0, 2.0, 6.0]),
                [4.0, np.nan, 8.0],
                np.array([np.nan, np.nan]),
                np.array([]),
                5.0,
                None,
                np.array(["a", "b"], dtype=object),
                np.array(3.0),
            ],
        }
    )


def _run(lines: list[str], frame: pd.DataFrame) -> pd.DataFrame:
    namespace: dict = {"df": frame.copy(), "pd": pd, "np": np}
    exec("\n".join(lines), namespace)  # noqa: S102
    return namespace["df"]


@pytest.mark.parametrize("statistic", [Statistic.MEAN, Statistic.MEDIAN])
def test_emitted_collapse_matches_the_step(statistic):
    step = CollapseCells("Signal", statistic)
    expected, _, _ = step.apply(_cells())
    got = _run(emit_collapse_cells(step), _cells())
    pd.testing.assert_series_equal(
        got["Signal"].astype("float64"), expected["Signal"].astype("float64")
    )


def test_cells_without_numbers_are_gaps_not_values():
    collapsed, rows, _ = CollapseCells("Signal", Statistic.MEAN).apply(_cells())
    values = collapsed["Signal"].tolist()
    assert values[:2] == [3.0, 6.0] and rows == 8
    assert all(np.isnan(v) for v in values[2:])


def _exploded() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "subject": ["01"] * 4 + ["02"] * 4,
            "t": [0, 1, 2, 3] * 2,
            "Signal": [1.0, 2.0, 3.0, 10.0, 4.0, np.nan, 6.0, 8.0],
        }
    )


@pytest.mark.parametrize("statistic", [Statistic.MEAN, Statistic.MEDIAN])
def test_emitted_exploded_collapse_matches_the_step(statistic):
    step = CollapseCells("Signal", statistic, exploded=True, index_column="t")
    expected, rows, samples = step.apply(_exploded())
    got = _run(emit_step(step), _exploded())
    pd.testing.assert_frame_equal(got, expected)
    assert (rows, samples) == (2, 8) and "t" not in got.columns


def test_apply_cell_collapse_is_the_step(series_table):
    spec = PlotSpec(measures=["Signal"], kind=PlotKind.VIOLIN)
    step = collapse_step(spec, series_table)
    assert step == CollapseCells("Signal", Statistic.MEAN)
    expected, _, _ = CollapseCells("Signal", Statistic.MEAN).apply(series_table.frame)
    got = apply_cell_collapse(spec, series_table).frame
    pd.testing.assert_series_equal(got["Signal"], expected["Signal"])


def test_no_step_unless_a_scalar_kind_is_picked(series_table):
    assert collapse_step(PlotSpec(measures=["Signal"], kind=PlotKind.LINE), series_table) is None


def _violin() -> PlotSpec:
    return PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.VIOLIN,
        filters=[Filter("session", include=["pre"])],
    )


def test_the_generated_function_prints_the_step(series_table):
    source = generate_plot_function(_violin(), series_table)
    for line in emit_collapse_cells(CollapseCells("Signal", Statistic.MEAN)):
        assert line in source


def test_the_figure_records_the_collapse_before_its_filters(series_table):
    figure = resolve(_violin(), series_table)[0]
    kinds = [type(step) for step in figure.reduction.steps]
    # The shared steps in the order applied, then the figure's collapse chain.
    assert kinds[:2] == [CollapseCells, ValueFilter]
    assert kinds[2:] and set(kinds[2:]) == {CollapseLevels}
