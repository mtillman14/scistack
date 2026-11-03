"""A text measure's exported plot function draws what the preview draws.

The generated code receives the RAW table (text) and has to make the same
numbers ``categorical.apply_categorical`` made — the same step
(``reduction.CategoricalRewrite``, printed by
``reduction_code.emit_categorical``) — and pool the same way
(``reduce._collapse_levels``). Each test runs the generated function and
reads the drawn bars / ticks / matrix back off the matplotlib figure.
"""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

from scistackplot import (
    LEVEL_FACTOR,
    BarOptions,
    CategoricalOptions,
    CategoricalStat,
    PlotKind,
    Role,
    generate_plot_function,
    resolve,
)
from scistackplot.reduce.resolved import X, Y

from test_categorical import _bars, _spec, _table

matplotlib = pytest.importorskip("matplotlib")
pytest.importorskip("seaborn")


def _run(spec, table):
    source = generate_plot_function(spec, table)
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    [function] = [v for k, v in namespace.items() if k.startswith("plot_")]
    return source, function(table.frame.copy(), "figure.png")


def _heights(figure) -> list[float]:
    return sorted(
        round(float(patch.get_height()), 6)
        for ax in figure.axes
        for container in ax.containers
        for patch in container
        if np.isfinite(patch.get_height())
    )


def test_the_exported_count_bar_has_the_previewed_heights():
    spec = _spec()
    source, figure = _run(spec, _table())
    assert "pooled" in source or "summed" in source
    assert _heights(figure) == sorted(_bars(spec).values())
    matplotlib.pyplot.close(figure)


def test_the_exported_proportion_bar_has_the_previewed_heights():
    spec = _spec(CategoricalStat.PROPORTION)
    _source, figure = _run(spec, _table())
    assert _heights(figure) == pytest.approx(sorted(_bars(spec).values()))
    matplotlib.pyplot.close(figure)


def test_the_exported_one_category_bar():
    spec = replace(_spec(), categorical=CategoricalOptions(level="fall"))
    _source, figure = _run(spec, _table())
    assert _heights(figure) == [5.0]
    matplotlib.pyplot.close(figure)


def test_the_exported_stacked_bar_piles_to_100():
    spec = replace(
        _spec(CategoricalStat.PROPORTION, session=Role.GROUP),
        groups=[LEVEL_FACTOR, "session"],
        bar=BarOptions(stack=True),
    )
    source, figure = _run(spec, _table())
    assert "stacked" in source
    tops = sorted(
        round(float(patch.get_y() + patch.get_height()), 6)
        for ax in figure.axes
        for container in ax.containers
        for patch in container
    )
    # Two ticks, two levels each: the upper bar of every pile ends at 100.
    assert tops[-2:] == pytest.approx([100.0, 100.0])
    matplotlib.pyplot.close(figure)


def test_the_exported_strip_labels_the_categories():
    spec = _spec(kind=PlotKind.STRIP)
    _source, figure = _run(spec, _table())
    labels = [t.get_text() for t in figure.axes[0].get_yticklabels()]
    assert labels == ["fall", "ok"]
    [preview] = resolve(spec, _table())
    assert preview.y_categories == labels
    matplotlib.pyplot.close(figure)


def test_the_exported_heatmap_is_the_previewed_matrix():
    spec = replace(
        _spec(session=Role.GROUP), kind=PlotKind.HEATMAP, groups=[LEVEL_FACTOR, "session"], color=None
    )
    _source, figure = _run(spec, _table())
    image = figure.axes[0].images[0]
    [preview] = resolve(spec, _table())
    np.testing.assert_allclose(
        np.ma.filled(image.get_array(), np.nan), preview.panels[0].frame["__z"].iloc[0]
    )
    matplotlib.pyplot.close(figure)


def test_a_record_filter_on_the_text_is_exported():
    from scistackplot.model.spec import Filter

    spec = replace(_spec(), filters=[Filter(column="Outcome", include=["fall"])])
    source, figure = _run(spec, _table())
    assert _heights(figure) == [5.0]
    assert "_text.isin" in source
    matplotlib.pyplot.close(figure)
