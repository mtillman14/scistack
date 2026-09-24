"""The preview's paper is the export's paper — both read ``scistackplot.paper``.

The reported gap (2026-09-24): a light-mode Plot Studio still did not show
the file Save would write. The export (matplotlib) drew a white page, a black
box round each panel and outward ticks, with no grid. The preview (plotly.js)
drew plotly's own defaults: a grey grid, a zero line, no frame, no ticks.
Marks, sizes and labels had long been matched; the paper never was.

These tests read the paper back out of both renderers and hold each to the
one owner, :data:`scistackplot.paper.PAPER`, so the two cannot drift apart. They
also pin PAPER to matplotlib's own defaults (turning the owner into existence
changed no exported file) and check that a user's ``matplotlibrc`` cannot
change an export.
"""

from __future__ import annotations

import io
import re

import pytest

from scistackplot import PlotKind, PlotSpec, Role, render_matplotlib, render_plotly, resolve
from scistackplot.paper import (
    PAPER,
    figure_rc_params,
    paper_axes_code,
    paper_rc_params,
    plotly_axis_style,
)
from scistackplot.textsize import resolve_sizes

matplotlib = pytest.importorskip("matplotlib")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import to_rgba  # noqa: E402

AXIS_KEY = re.compile(r"^[xy]axis\d*$")


@pytest.fixture
def faceted_bar_spec():
    """Two panels, bars with error bars: every piece of paper at once."""
    return PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.FACET, "trial": Role.COLLAPSE},
        kind=PlotKind.BAR,
    )


def _same_colour(a, b) -> bool:
    return to_rgba(a) == pytest.approx(to_rgba(b))


def _saved(figure):
    """Ticks are made lazily: save first (outside any rc_context), then look."""
    figure.savefig(io.BytesIO(), format="png")
    return figure


def _assert_mpl_paper(figure) -> None:
    assert _same_colour(figure.get_facecolor(), PAPER.background)
    visible = [ax for ax in figure.axes if ax.get_visible()]
    assert visible
    for ax in visible:
        assert _same_colour(ax.get_facecolor(), PAPER.background)
        for side, spine in ax.spines.items():
            assert spine.get_visible(), side
            assert _same_colour(spine.get_edgecolor(), PAPER.axis_line), side
            assert spine.get_linewidth() == pytest.approx(PAPER.axis_line_width), side
        for axis in (ax.xaxis, ax.yaxis):
            ticks = axis.get_major_ticks()
            assert ticks
            for tick in ticks[:3]:
                assert tick._tickdir == PAPER.tick_direction  # noqa: SLF001
                assert tick.tick1line.get_markersize() == pytest.approx(PAPER.tick_length)
                assert tick.tick1line.get_markeredgewidth() == pytest.approx(PAPER.tick_width)
                assert _same_colour(tick.tick1line.get_color(), PAPER.axis_line)
                # Bottom/left only; the far sides are frame, not ticks.
                assert not tick.tick2line.get_visible()
                assert tick.gridline.get_visible() is PAPER.grid
                if tick.label1.get_text():
                    assert _same_colour(tick.label1.get_color(), PAPER.text)
        if ax.get_ylabel():
            assert _same_colour(ax.yaxis.label.get_color(), PAPER.text)


# --- the owner ----------------------------------------------------------------


def test_paper_is_matplotlibs_own_default_look():
    """Stating the paper changed nothing in an export: every value IS the default."""
    defaults = matplotlib.rcParamsDefault
    # 'auto' / 'inherit' in the defaults mean "follow the key PAPER also sets".
    derived = {"savefig.facecolor", "axes.titlecolor", "xtick.labelcolor", "ytick.labelcolor"}
    for key, value in paper_rc_params().items():
        if key in derived:
            continue
        default = defaults[key]
        if isinstance(value, str) and key.endswith(("color", "facecolor")):
            assert _same_colour(value, default), key
        else:
            assert value == pytest.approx(default) if isinstance(value, float) else value == default, key


def test_error_bar_ink_is_matplotlibs_default():
    defaults = matplotlib.rcParamsDefault
    assert PAPER.error_bar_width == pytest.approx(defaults["lines.linewidth"])
    assert PAPER.error_cap_width == pytest.approx(defaults["lines.markeredgewidth"])


def test_rc_params_are_accepted_by_matplotlib():
    params = figure_rc_params(resolve_sizes(PlotSpec(measures=["y"]).style))
    with matplotlib.rc_context(params):
        pass
    # The text sizes are still there, unchanged by the paper.
    assert "font.size" in params and "xtick.labelsize" in params


# --- both renderers draw it -----------------------------------------------------


def test_matplotlib_draws_the_paper(scalar_table, faceted_bar_spec):
    figure = _saved(render_matplotlib(resolve(faceted_bar_spec, scalar_table)[0]))
    try:
        _assert_mpl_paper(figure)
    finally:
        plt.close(figure)


def test_plotly_draws_the_same_paper(scalar_table, faceted_bar_spec):
    layout = render_plotly(resolve(faceted_bar_spec, scalar_table)[0])["layout"]
    assert layout["paper_bgcolor"] == PAPER.background
    assert layout["plot_bgcolor"] == PAPER.background
    assert layout["font"]["color"] == PAPER.text
    axes = {key: axis for key, axis in layout.items() if AXIS_KEY.match(key)}
    # Two panels: x, y, x2, y2 — every one of them boxed, not just the first.
    assert len(axes) == 4, sorted(axes)
    expected = plotly_axis_style()
    for key, axis in axes.items():
        for name, value in expected.items():
            assert axis[name] == value, (key, name)
    # And what those keys say, spelled out against the owner:
    for axis in axes.values():
        assert axis["showline"] and axis["mirror"] is True
        assert axis["linecolor"] == PAPER.axis_line
        assert axis["linewidth"] == PAPER.axis_line_width
        assert axis["ticks"] == "outside"
        assert axis["ticklen"] == PAPER.tick_length
        assert axis["tickwidth"] == PAPER.tick_width
        assert axis["showgrid"] is PAPER.grid
        assert axis["zeroline"] is False


def test_error_bars_have_one_ink(scalar_table, faceted_bar_spec):
    resolved = resolve(faceted_bar_spec, scalar_table)[0]

    errors = [t["error_y"] for t in render_plotly(resolved)["data"] if "error_y" in t]
    assert errors, "the bar spec draws error bars"
    for error in errors:
        assert error["color"] == PAPER.error_bar_color
        assert error["thickness"] == PAPER.error_bar_width
        assert error["width"] == PAPER.error_cap

    figure = render_matplotlib(resolved)
    try:
        bars = [c for ax in figure.axes for c in ax.containers if getattr(c, "errorbar", None)]
        assert bars
        for container in bars:
            _, caps, lines = container.errorbar.lines
            for collection in lines:
                assert _same_colour(collection.get_colors()[0], PAPER.error_bar_color)
                assert collection.get_linewidths()[0] == pytest.approx(PAPER.error_bar_width)
            for cap in caps:
                # matplotlib draws a cap as a marker 2 x capsize long.
                assert cap.get_markersize() == pytest.approx(2 * PAPER.error_cap)
                assert cap.get_markeredgewidth() == pytest.approx(PAPER.error_cap_width)
                assert _same_colour(cap.get_markeredgecolor(), PAPER.error_bar_color)
    finally:
        plt.close(figure)


# --- nothing outside can change it ----------------------------------------------

#: A plausible personal matplotlibrc (ggplot-ish) — everything paper.py states.
HOSTILE_RC = {
    "figure.facecolor": "#e5e5e5",
    "axes.facecolor": "#e5e5e5",
    "savefig.facecolor": "#777777",
    "axes.grid": True,
    "axes.edgecolor": "#ff00ff",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "xtick.direction": "in",
    "ytick.direction": "in",
    "xtick.color": "#ff00ff",
    "text.color": "#ff00ff",
    "axes.labelcolor": "#ff00ff",
}


def test_a_personal_matplotlibrc_cannot_change_an_export(scalar_table, faceted_bar_spec):
    """The render AND the save happen under the hostile rc, as in a user's session."""
    resolved = resolve(faceted_bar_spec, scalar_table)[0]
    with matplotlib.rc_context(HOSTILE_RC):
        figure = _saved(render_matplotlib(resolved))
    try:
        _assert_mpl_paper(figure)
    finally:
        plt.close(figure)


def test_the_saved_file_has_the_paper_background(scalar_table, faceted_bar_spec):
    """write_figure states the facecolor: savefig reads it outside the rc_context."""
    from matplotlib.image import imread

    from scistackplot import write_figure

    buffer = io.BytesIO()
    with matplotlib.rc_context(HOSTILE_RC):
        write_figure(resolve(faceted_bar_spec, scalar_table)[0], buffer, dpi=50, format="png")
    buffer.seek(0)
    corner = imread(buffer, format="png")[0, 0][:3]
    assert tuple(corner) == pytest.approx(to_rgba(PAPER.background)[:3], abs=1 / 255)


# --- the generated code -------------------------------------------------------------


def test_generated_code_states_the_paper(scalar_table, faceted_bar_spec):
    pytest.importorskip("seaborn")
    from scistackplot import generate_plot_function

    source = generate_plot_function(faceted_bar_spec, scalar_table, function_name="plot_it")
    assert "'axes.grid': False" in source
    for line in paper_axes_code("g.axes.flat"):
        assert line in source

    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    # Drawn under the hostile rc too: seaborn despines by default, and a
    # generated export must not lose the frame the preview shows.
    with matplotlib.rc_context(HOSTILE_RC):
        figure = _saved(namespace["plot_it"](scalar_table.frame.copy(), "figure.png"))
    try:
        _assert_mpl_paper(figure)
    finally:
        plt.close(figure)


def test_generated_error_bars_are_the_exports(scalar_table, faceted_bar_spec):
    """seaborn's bar error bars are grey, 2.25 pt and capless by default; the
    generated code states the export's (paper.seaborn_err_kws)."""
    pytest.importorskip("seaborn")
    from scistackplot import generate_plot_function

    source = generate_plot_function(faceted_bar_spec, scalar_table, function_name="plot_it")
    assert "err_kws=" in source

    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    figure = namespace["plot_it"](scalar_table.frame.copy(), "figure.png")
    try:
        # seaborn draws each error bar as one two-point Line2D; the caps are
        # its end markers.
        bars = [
            line for ax in figure.axes for line in ax.lines if line.get_marker() == "_"
        ]
        assert bars, "the generated bar plot draws capped error bars"
        for line in bars:
            assert len(line.get_xdata()) == 2
            assert _same_colour(line.get_color(), PAPER.error_bar_color)
            assert line.get_linewidth() == pytest.approx(PAPER.error_bar_width)
            # Exactly Axes.errorbar's caps (checked against render_matplotlib
            # in test_error_bars_have_one_ink).
            assert line.get_markersize() == pytest.approx(2 * PAPER.error_cap)
            assert line.get_markeredgewidth() == pytest.approx(PAPER.error_cap_width)
            assert _same_colour(line.get_markeredgecolor(), PAPER.error_bar_color)
    finally:
        plt.close(figure)
