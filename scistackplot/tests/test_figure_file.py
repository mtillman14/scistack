"""
Stage 3 of the label-legibility plan: a saved figure is exactly the size the
spec asks for.

The GUI used to save with ``bbox_inches="tight"``, which crops or grows the
canvas around whatever sticks out, so the file never had the requested size.
``write_figure`` is now the one owner of render-and-write: the file is
``StyleOptions.width x height`` at the dpi given, and anything that still
reaches past the canvas is reported, never fixed by resizing the file.
"""

from __future__ import annotations

import io
from dataclasses import replace

import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.image as mpimg  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

from scistackplot import canvas_overflow, render_matplotlib, resolve, write_figure  # noqa: E402
from scistackplot.figure_file import OVERFLOW_TOLERANCE_IN  # noqa: E402
from test_mpl_label_fit import _graph1, _graph2, gait_table  # noqa: E402,F401

DPI = 100


def _pixels(buffer: io.BytesIO) -> tuple[int, int]:
    buffer.seek(0)
    image = mpimg.imread(buffer, format="png")
    return image.shape[1], image.shape[0]


CASES = [
    pytest.param(_graph1(), id="graph1"),
    # The legend goes below the panels here: the case "tight" grew most.
    pytest.param(replace(_graph2(), sample_color="subject"), id="graph2-legend-below"),
    pytest.param(_graph2(width=14.0), id="graph2-wide"),
]

#: Too small for graph1 on purpose: the file must still be exactly its size,
#: even where the layout runs out of room (that is WARNed, not "fixed").
TOO_SMALL = pytest.param(_graph1(width=3.5, height=3.0), id="journal-column")


@pytest.mark.parametrize("spec", [*CASES, TOO_SMALL])
def test_the_file_is_exactly_the_requested_size(gait_table, spec):
    (resolved,) = resolve(spec, gait_table)
    buffer = io.BytesIO()
    written = write_figure(resolved, buffer, dpi=DPI, format="png")
    expected = (round(spec.style.width * DPI), round(spec.style.height * DPI))
    assert written.pixels == expected
    assert _pixels(buffer) == expected
    assert (written.width_in, written.height_in) == (spec.style.width, spec.style.height)


@pytest.mark.parametrize("spec", CASES)
def test_nothing_is_drawn_past_the_canvas(gait_table, spec):
    """Which is what makes the exact size safe: nothing needed cropping."""
    (resolved,) = resolve(spec, gait_table)
    figure = render_matplotlib(resolved)
    try:
        assert canvas_overflow(figure) <= OVERFLOW_TOLERANCE_IN
    finally:
        plt.close(figure)


def test_bbox_inches_is_refused(gait_table):
    (resolved,) = resolve(_graph1(), gait_table)
    with pytest.raises(ValueError, match="bbox_inches"):
        write_figure(resolved, io.BytesIO(), bbox_inches="tight")


def test_overflow_is_measured():
    """A text placed past the right edge is what "tight" would have grown the
    file for; here it is measured and reported instead."""
    figure = plt.figure(figsize=(4, 3))
    try:
        figure.text(1.1, 0.5, "past the edge")
        assert canvas_overflow(figure) > 0.3
    finally:
        plt.close(figure)


def test_the_figure_is_closed_afterwards(gait_table):
    (resolved,) = resolve(_graph1(), gait_table)
    before = set(plt.get_fignums())
    write_figure(resolved, io.BytesIO(), dpi=DPI, format="png")
    assert set(plt.get_fignums()) == before
