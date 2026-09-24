"""
Stage 5 of the label-legibility plan: fixed tick settings, and the generated
code reproducing the fit and saving at exactly its size.

The generated seaborn code replays the export's decision as operations on the
labels seaborn drew (drop the shared prefix, wrap, every k-th, rotation and
font), and lays the grid out inside the figure so the script can save without
``bbox_inches="tight"``.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
pytest.importorskip("seaborn")
import matplotlib.pyplot as plt  # noqa: E402

from scistackplot import (  # noqa: E402
    PlotSpec,
    generate_plot_function,
    generate_script,
    render_matplotlib,
    resolve,
)
from test_mpl_label_fit import _single_axis_spec, _single_axis_table  # noqa: E402

IDS = [f"SS{i:02d}" for i in range(1, 41)]


def _run(source: str, frame):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace["plot_it"](frame.copy(), "figure.png")


# --- fixed settings -------------------------------------------------------------


def test_tick_settings_round_trip_and_default_to_fitted():
    spec = _single_axis_spec(4.0)
    assert (spec.style.tick_rotation, spec.style.tick_font_size, spec.style.tick_every) == (
        None,
        None,
        None,
    )
    pinned = replace(spec, style=replace(spec.style, tick_rotation=45, tick_every=3))
    again = PlotSpec.from_dict(pinned.to_dict())
    assert (again.style.tick_rotation, again.style.tick_every) == (45, 3)
    assert again.style.tick_font_size is None


def test_a_fixed_rotation_reaches_the_export():
    spec = _single_axis_spec(6.0)
    spec = replace(spec, style=replace(spec.style, tick_rotation=45))
    (resolved,) = resolve(spec, _single_axis_table(["pre", "post"]))
    figure = render_matplotlib(resolved)
    try:
        (ax,) = [a for a in figure.axes if a.get_visible()]
        assert {round(t.get_rotation()) for t in ax.get_xticklabels()} == {45}
    finally:
        plt.close(figure)


# --- the generated code ---------------------------------------------------------


def test_generated_code_replays_the_fit():
    table = _single_axis_table(IDS)
    source = generate_plot_function(_single_axis_spec(4.0), table, function_name="plot_it")
    assert "fitted them at 4.0 x 3.0 in" in source
    assert "startswith('SS')" in source
    figure = _run(source, table.frame)
    try:
        (ax,) = [a for a in figure.axes if a.get_visible()]
        shown = [t.get_text() for t in ax.get_xticklabels()]
        assert shown[0] == "01" and shown[-1] == "40"
        assert "" in shown, "every k-th label"
        assert not any(label.startswith("SS") for label in shown)
    finally:
        plt.close(figure)


def test_labels_that_fit_generate_no_relabelling():
    table = _single_axis_table(["pre", "post"])
    source = generate_plot_function(_single_axis_spec(6.0), table, function_name="plot_it")
    assert "fitted them" not in source


def test_the_generated_figure_is_exactly_its_size():
    table = _single_axis_table(IDS)
    source = generate_plot_function(_single_axis_spec(4.0), table, function_name="plot_it")
    assert "tight_layout" in source
    figure = _run(source, table.frame)
    try:
        assert tuple(figure.get_size_inches()) == (4.0, 3.0)
    finally:
        plt.close(figure)


def test_the_generated_script_saves_without_trimming():
    source = generate_script(_single_axis_spec(4.0), _single_axis_table(IDS))
    assert "bbox_inches" not in source
