"""Both renderers name a variant the same thing the panel does.

The reported symptom (2026-09-11): the Plot Studio legend read
``["FilteredEMG", "RawEMG"]`` while the saved PNG beside it read
``["FilteredEMG", "latest"]``. The cause was a stale build — ``auto_label``
had been fixed and the file on disk predated it — but the reason nobody could
tell was that **nothing tested it**. The interactive legend comes from plotly,
the saved one from matplotlib, and the only way to compare them was to open a
picture.

One rule, asserted through both renderers and against its own definition:
a variant row is called :func:`variants.set_name`, in the legend and in the name
box, and the two renderers agree. A figure whose exported legend disagrees with
the one on screen is the same figure reading two different ways depending on how
you look at it — the thing ``ResolvedPlot`` exists to prevent.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scistackplot import (
    DataFrameSource,
    PlotKind,
    PlotSpec,
    Role,
    VariantSet,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.render.base import legend_levels
from scistackplot.variants import VARIANT_FACTOR, set_name

matplotlib = pytest.importorskip("matplotlib")


@pytest.fixture
def stacked_table():
    """Two variables in one value column — what "stack RawEMG onto FilteredEMG"
    produces (scistackplotdb: ``stacked [...] into one value column``)."""
    rows = [
        {
            "subject": subject,
            "bandpass.low_hz": low_hz,
            "FilteredEMG": 1.0 + 0.1 * int(low_hz),
            "RawEMG": 5.0 + 0.1 * int(low_hz),
        }
        for subject in ("01", "02")
        for low_hz in ("20", "40")
    ]
    source = DataFrameSource(
        pd.DataFrame(rows),
        factors=["subject", "bandpass.low_hz"],
        measures=["FilteredEMG", "RawEMG"],
        variant_factors=["bandpass.low_hz"],
        name="FilteredEMG",
    )
    return source.get_table(["FilteredEMG", "RawEMG"])


@pytest.fixture
def two_variable_spec():
    """One row per variable, coloured by variant — the user's figure."""
    return PlotSpec(
        measures=["FilteredEMG"],
        variant_sets=[VariantSet(), VariantSet(variable="RawEMG")],
        roles={
            "subject": Role.X,
            VARIANT_FACTOR: Role.COLOR,
            "bandpass.low_hz": Role.FREE,
        },
        kind=PlotKind.SCATTER,
    )


def _mpl_legend_labels(figure) -> list[str]:
    """What the SAVED figure says, read off the drawn legend."""
    return [
        text.get_text() for legend in figure.legends for text in legend.get_texts()
    ]


def _plotly_legend_labels(payload: dict) -> list[str]:
    """What the INTERACTIVE figure says: the traces that claim a legend entry."""
    return [trace["name"] for trace in payload["data"] if trace.get("showlegend")]


def test_both_renderers_use_the_variant_row_names(stacked_table, two_variable_spec):
    expected = [
        set_name(
            variant,
            index,
            primary=two_variable_spec.y_measure,
            latest_column=stacked_table.latest_column,
        )
        for index, variant in enumerate(two_variable_spec.variant_sets)
    ]
    assert expected == ["FilteredEMG", "RawEMG"], "the names the panel shows"

    resolved = resolve(two_variable_spec, stacked_table)[0]

    figure = render_matplotlib(resolved)
    try:
        assert _mpl_legend_labels(figure) == expected
    finally:
        matplotlib.pyplot.close(figure)

    assert _plotly_legend_labels(render_plotly(resolved)) == expected


def test_a_renamed_variant_reaches_both_legends(stacked_table):
    """A row the user typed a name into. `set_name` returns it verbatim, and
    neither renderer may fall back to the column's own level text."""
    spec = PlotSpec(
        measures=["FilteredEMG"],
        variant_sets=[
            VariantSet(name="Filtered"),
            VariantSet(name="Raw", variable="RawEMG"),
        ],
        roles={
            "subject": Role.X,
            VARIANT_FACTOR: Role.COLOR,
            "bandpass.low_hz": Role.FREE,
        },
        kind=PlotKind.SCATTER,
    )

    resolved = resolve(spec, stacked_table)[0]

    figure = render_matplotlib(resolved)
    try:
        assert _mpl_legend_labels(figure) == ["Filtered", "Raw"]
    finally:
        matplotlib.pyplot.close(figure)

    assert _plotly_legend_labels(render_plotly(resolved)) == ["Filtered", "Raw"]


def test_the_legend_is_the_resolved_colour_levels(stacked_table, two_variable_spec):
    """Both renderers read the same list, so neither can drift on its own.

    `legend_levels` is the shared definition; this pins the renderers to it
    rather than to each other, which is what makes a third backend (MATLAB) a
    new leaf instead of a third opinion.
    """
    resolved = resolve(two_variable_spec, stacked_table)[0]

    shared = [str(level) for level in legend_levels(resolved)]

    figure = render_matplotlib(resolved)
    try:
        assert _mpl_legend_labels(figure) == shared
    finally:
        matplotlib.pyplot.close(figure)
    assert _plotly_legend_labels(render_plotly(resolved)) == shared
