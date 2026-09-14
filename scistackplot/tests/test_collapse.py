"""Collapsing a 1-D measure to a scalar.

The rule under test: a scalar-only kind selected for a vector-valued measure
means "reduce each cell to one value first", after which the measure is an
ordinary scalar and nothing downstream knows it was ever 1-D
(``docs/claude/measure-shape-and-collapse.md``).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    Shape,
    Statistic,
    apply_collapse,
    collapse_note,
    collapses,
    effective_shape,
)
from scistackplot.series_stats import cell_arrays, collapse_cells


def violin(measure: str, **kwargs) -> PlotSpec:
    return PlotSpec(measures=[measure], kind=PlotKind.VIOLIN, **kwargs)


# --- the per-cell reduction -------------------------------------------------


def test_collapse_cells_is_the_mean_of_each_cell():
    arrays = [np.array([1.0, 2.0, 3.0]), np.array([10.0, 20.0])]
    assert collapse_cells(arrays, Statistic.MEAN).tolist() == [2.0, 15.0]


def test_collapse_cells_median():
    arrays = [np.array([1.0, 2.0, 100.0])]
    assert collapse_cells(arrays, Statistic.MEDIAN).tolist() == [2.0]


def test_collapse_cells_skips_nan_like_pandas():
    """``Series.mean()`` semantics: NaN samples are skipped, not propagated."""
    arrays = [np.array([1.0, np.nan, 3.0])]
    assert collapse_cells(arrays, Statistic.MEAN).tolist() == [2.0]
    assert pd.Series([1.0, np.nan, 3.0]).mean() == 2.0


def test_collapse_cells_handles_ragged_cells():
    arrays = [np.arange(3, dtype=float), np.arange(100, dtype=float)]
    assert collapse_cells(arrays, Statistic.MEAN).tolist() == [1.0, 49.5]


def test_an_empty_or_all_nan_cell_is_nan_not_an_error():
    arrays = [np.empty(0), np.array([np.nan, np.nan]), np.array([5.0])]
    values = collapse_cells(arrays, Statistic.MEAN)
    assert np.isnan(values[0]) and np.isnan(values[1])
    assert values[2] == 5.0


# --- when it fires ----------------------------------------------------------


def test_a_scalar_kind_on_a_1d_measure_collapses(series_table):
    assert collapses(violin("Signal"), series_table)
    assert effective_shape(violin("Signal"), series_table) is Shape.SCALAR


@pytest.mark.parametrize("kind", [PlotKind.LINE, PlotKind.BAND])
def test_the_1d_kinds_do_not_collapse(series_table, kind):
    spec = PlotSpec(measures=["Signal"], kind=kind)
    assert not collapses(spec, series_table)
    assert effective_shape(spec, series_table) is Shape.SERIES_1D
    assert apply_collapse(spec, series_table) is series_table


def test_a_scalar_measure_is_never_collapsed(scalar_table):
    spec = violin("StepLength")
    assert not collapses(spec, scalar_table)
    assert apply_collapse(spec, scalar_table) is scalar_table


def test_a_relational_plot_does_not_collapse(series_table):
    """``x_measure`` pairs samples; collapsing both measures is a different
    feature and must not happen behind the user's back."""
    spec = PlotSpec(measures=["Signal"], x_measure="Signal", kind=PlotKind.SCATTER)
    assert not collapses(spec, series_table)


def test_a_spec_naming_a_missing_measure_does_not_raise(series_table):
    """A spec outlives the table it was written against."""
    assert not collapses(violin("Gone"), series_table)


# --- the derived table ------------------------------------------------------


def test_the_collapsed_table_holds_one_value_per_record(series_table):
    expected = [float(np.mean(cell)) for cell in series_table.frame["Signal"]]

    collapsed = apply_collapse(violin("Signal"), series_table)

    assert collapsed.frame["Signal"].tolist() == pytest.approx(expected)
    assert len(collapsed.frame) == len(series_table.frame)
    assert collapsed.shape_of("Signal") is Shape.SCALAR
    assert collapsed.measure("Signal").exploded is False


def test_the_median_statistic_is_honoured(series_table):
    spec = violin("Signal", collapse_statistic=Statistic.MEDIAN)
    expected = [float(np.median(cell)) for cell in series_table.frame["Signal"]]
    assert apply_collapse(spec, series_table).frame["Signal"].tolist() == pytest.approx(
        expected
    )


def test_factors_and_their_levels_are_untouched(series_table):
    collapsed = apply_collapse(violin("Signal"), series_table)
    assert collapsed.factor_names == series_table.factor_names
    assert collapsed.factor("subject").levels == series_table.factor("subject").levels
    assert collapsed.schema_levels == series_table.schema_levels


def test_the_source_table_is_not_mutated(series_table):
    before = series_table.frame["Signal"].iloc[0]
    apply_collapse(violin("Signal"), series_table)
    assert series_table.shape_of("Signal") is Shape.SERIES_1D
    assert np.asarray(series_table.frame["Signal"].iloc[0]).size == np.asarray(
        before
    ).size


def test_the_index_column_goes_away_with_the_samples():
    """A within-observation axis has nothing to index once the samples are gone
    — left in place it would read as a factor with one level per position."""
    frame = pd.DataFrame(
        {"trial": ["1", "2"], "Signal": [[1.0, 2.0], [3.0, 4.0]]}
    )
    table = LongTable.from_frame(
        frame, factors=["trial"], measures=["Signal"], name="Signal"
    )
    table.index_column = "time"
    assert apply_collapse(violin("Signal"), table).index_column is None


def test_an_empty_record_stays_as_a_nan_row():
    """Dropping it would change the level counts the factor summary reports, so
    'this trial recorded nothing' would read as 'this trial does not exist'."""
    frame = pd.DataFrame(
        {"trial": ["1", "2", "3"], "Signal": [[1.0, 3.0], [], [np.nan, np.nan]]}
    )
    table = LongTable.from_frame(
        frame, factors=["trial"], measures=["Signal"], name="Signal"
    )

    collapsed = apply_collapse(violin("Signal"), table)

    assert len(collapsed.frame) == 3
    assert collapsed.frame["trial"].tolist() == ["1", "2", "3"]
    assert collapsed.frame["Signal"].iloc[0] == 2.0
    assert collapsed.frame["Signal"].iloc[1:].isna().all()


def test_a_stacked_frame_collapses_every_variant_row():
    """After ``apply_variant_sets`` two variables share ONE value column; the
    collapse must reduce all of it, not only the primary's rows."""
    frame = pd.DataFrame(
        {
            "subject": ["01", "01"],
            "Variant": ["Raw", "Filtered"],
            "Signal": [[1.0, 3.0], [10.0, 20.0, 30.0]],
        }
    )
    table = LongTable.from_frame(
        frame,
        factors=["subject", "Variant"],
        measures=["Signal"],
        variant_factors=["Variant"],
        name="Signal",
        schema_levels=["subject"],
    )

    collapsed = apply_collapse(violin("Signal"), table)

    assert collapsed.frame["Signal"].tolist() == pytest.approx([2.0, 20.0])


def test_a_pre_exploded_table_collapses_by_grouping():
    """A source may supply a real within-observation axis and one row per
    sample. No shipped source does, which is why this is tested rather than
    left to raise: the failure would be a violin of every sample of every trial,
    which looks like a plausible figure."""
    frame = pd.DataFrame(
        {
            "trial": ["1", "1", "1", "2", "2"],
            "time": [0.0, 0.1, 0.2, 0.0, 0.1],
            "Signal": [1.0, 2.0, 3.0, 10.0, 20.0],
        }
    )
    table = LongTable.from_frame(
        frame,
        factors=["trial"],
        measures=["Signal"],
        index_column="time",
        name="Signal",
        schema_levels=["trial"],
    )
    table.measures[0].shape = Shape.SERIES_1D
    table.measures[0].exploded = True

    collapsed = apply_collapse(violin("Signal"), table)

    assert collapsed.frame["trial"].tolist() == ["1", "2"]
    assert collapsed.frame["Signal"].tolist() == pytest.approx([2.0, 15.0])
    assert collapsed.shape_of("Signal") is Shape.SCALAR


# --- narration --------------------------------------------------------------


def test_the_figure_says_its_points_are_summaries(series_table):
    """A violin of trial means and a violin of raw samples look identical."""
    note = collapse_note(violin("Signal"), series_table)
    assert note and "mean" in note and "Signal" in note
    assert collapse_note(PlotSpec(measures=["Signal"], kind=PlotKind.LINE), series_table) is None


def test_collapse_is_logged(series_table, caplog):
    import logging

    from scistackplot.collapse import LAYER

    with caplog.at_level(logging.INFO, logger=LAYER):
        apply_collapse(violin("Signal"), series_table)
    assert any("collapsed 1-D measure" in record.getMessage() for record in caplog.records)


def test_cell_arrays_and_collapse_agree_on_the_frame(series_table):
    """The two halves of the reduction, pinned together: whatever
    ``cell_arrays`` reads is what ``collapse_cells`` reduces."""
    arrays = cell_arrays(series_table.frame["Signal"])
    values = collapse_cells(arrays, Statistic.MEAN)
    collapsed = apply_collapse(violin("Signal"), series_table)
    assert collapsed.frame["Signal"].tolist() == pytest.approx(values.tolist())


def test_roles_survive_the_collapse(series_table):
    """The derived table is only about the measure — a spec's role assignment
    passes through it untouched."""
    spec = violin("Signal", roles={"trial": Role.FREE, "subject": Role.X})
    collapsed = apply_collapse(spec, series_table)
    assert all(name in collapsed.frame.columns for name in spec.roles)
