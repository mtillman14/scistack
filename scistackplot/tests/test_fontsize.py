"""Text sizes: each element sized independently, on every path, never leaking.

The resolution itself is pure and tested in ``test_textsize.py``; this file
checks that the numbers reach the drawn text — the matplotlib export, the
generated seaborn code and the plotly preview.
"""

from __future__ import annotations

import io

import pytest

from scistackplot import (
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    TextSizes,
    generate_plot_function,
    render_plotly,
    resolve,
)

matplotlib = pytest.importorskip("matplotlib")
pytest.importorskip("seaborn")

#: Every element a different size, so a swapped or ignored one shows.
DISTINCT = TextSizes(
    base=12.0,
    title=22.0,
    x_label=17.0,
    y_label=19.0,
    x_ticks=11.0,
    y_ticks=13.0,
    groups=9.0,
    legend=15.0,
    legend_title=16.0,
)


def _spec(text: TextSizes, *, width: float = 14.0) -> PlotSpec:
    # Nested x (session ticks inside subject brackets), a colour legend and a
    # title: every element this feature sizes is drawn. Wide enough that the
    # fixed sizes fit, so nothing here is about crowding.
    return PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
        style=StyleOptions(text=text, width=width, height=7.0, title="Step length"),
    )


def _saved(figure):
    """The save happens OUTSIDE render's rc_context (plot_service does it), and
    matplotlib creates tick objects lazily, so save first, then look."""
    figure.savefig(io.BytesIO(), format="png")
    return figure


def _sizes(figure) -> dict[str, list[float]]:
    ax = figure.axes[0]
    legend = figure.legends[0] if figure.legends else ax.get_legend()
    title = figure._suptitle  # noqa: SLF001 - matplotlib keeps no public getter
    return {
        "title": [title.get_fontsize()] if title is not None else [],
        "x_label": [ax.xaxis.label.get_fontsize()],
        "y_label": [ax.yaxis.label.get_fontsize()],
        "x_ticks": [t.get_fontsize() for t in ax.get_xticklabels() if t.get_text()],
        "y_ticks": [t.get_fontsize() for t in ax.get_yticklabels() if t.get_text()],
        "groups": [t.get_fontsize() for t in ax.texts if t.get_text()],
        "legend": [t.get_fontsize() for t in legend.get_texts()] if legend else [],
        "legend_title": [legend.get_title().get_fontsize()] if legend else [],
    }


def test_matplotlib_draws_each_element_at_its_own_size(scalar_table):
    from scistackplot import render_matplotlib

    before = dict(matplotlib.rcParams)
    figure = _saved(render_matplotlib(resolve(_spec(DISTINCT), scalar_table)[0]))
    try:
        for name, values in _sizes(figure).items():
            assert values, f"{name}: nothing drawn to check"
            expected = getattr(DISTINCT, name)
            assert all(v == pytest.approx(expected) for v in values), (name, values)
    finally:
        matplotlib.pyplot.close(figure)
    # rc_context, not rcParams: nothing leaks into the next figure.
    for key in ("font.size", "xtick.labelsize", "legend.fontsize", "axes.labelsize"):
        assert matplotlib.rcParams[key] == before[key], key


def test_base_alone_scales_everything_like_the_old_single_knob(scalar_table):
    """Unset elements derive from base with matplotlib's ratios."""
    from scistackplot import render_matplotlib

    figure = _saved(render_matplotlib(resolve(_spec(TextSizes(base=20.0)), scalar_table)[0]))
    try:
        sizes = _sizes(figure)
        for name in ("x_label", "y_label", "x_ticks", "y_ticks", "legend", "legend_title"):
            assert all(v == pytest.approx(20.0) for v in sizes[name]), (name, sizes[name])
        assert sizes["title"] == [pytest.approx(24.0)]
        assert all(v == pytest.approx(20.0 * 0.833, abs=1e-2) for v in sizes["groups"])
    finally:
        matplotlib.pyplot.close(figure)


def test_a_fixed_legend_size_is_not_shrunk_to_fit(scalar_table):
    """On a narrow figure the fit would shrink the legend; a fixed size may
    move below the panels but keeps its size."""
    from scistackplot import render_matplotlib

    text = TextSizes(base=12.0, legend=18.0)
    figure = _saved(render_matplotlib(resolve(_spec(text, width=4.0), scalar_table)[0]))
    try:
        legend = figure.legends[0]
        assert all(t.get_fontsize() == pytest.approx(18.0) for t in legend.get_texts())
        assert "shrink" not in figure.scistackplot_legend["steps"]
    finally:
        matplotlib.pyplot.close(figure)


def test_generated_code_draws_each_element_at_its_own_size(scalar_table, scalar_frame):
    source = generate_plot_function(_spec(DISTINCT), scalar_table)
    assert "'font.size': 12.0" in source
    assert "'ytick.labelsize': 13.0" in source
    # One rc key for both axis titles, so the y title is set per axes.
    assert "_ax.yaxis.label.set_size(19.0)" in source

    before = matplotlib.rcParams["font.size"]
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    function = next(v for k, v in namespace.items() if k.startswith("plot_"))
    figure = _saved(function(scalar_frame.copy(), "figure.png"))
    try:
        sizes = _sizes(figure)
        for name in ("title", "x_label", "y_label", "y_ticks"):
            expected = getattr(DISTINCT, name)
            assert sizes[name], name
            assert all(v == pytest.approx(expected) for v in sizes[name]), (name, sizes[name])
    finally:
        matplotlib.pyplot.close(figure)
    assert matplotlib.rcParams["font.size"] == before


def test_generated_code_says_nothing_extra_when_the_axis_titles_agree(scalar_table):
    source = generate_plot_function(_spec(TextSizes(base=12.0)), scalar_table)
    assert "yaxis.label.set_size" not in source


def test_plotly_preview_sizes_each_element(scalar_table):
    """Visible before a save: the preview uses the same numbers as px."""
    layout = render_plotly(resolve(_spec(DISTINCT), scalar_table)[0])["layout"]
    assert layout["font"]["size"] == 12.0
    assert layout["title"]["font"]["size"] == 22.0
    assert layout["xaxis"]["title"]["font"]["size"] == 17.0
    assert layout["yaxis"]["title"]["font"]["size"] == 19.0
    assert layout["xaxis"]["tickfont"]["size"] == 11.0
    assert layout["yaxis"]["tickfont"]["size"] == 13.0
    assert layout["legend"]["font"]["size"] == 15.0
    assert layout["legend"]["title"]["font"]["size"] == 16.0
    brackets = [a for a in layout["annotations"] if str(a.get("name", "")).startswith("x-group")]
    assert brackets and all(a["font"]["size"] == 9.0 for a in brackets)
    # The family is the one the label decisions were measured in (stage 4).
    assert layout["font"]["family"].startswith("DejaVu Sans")
    # The GUI reads the resolved sizes for its placeholders.
    assert layout["meta"]["text_sizes"]["pinned"] == sorted(
        ["title", "x_label", "y_label", "x_ticks", "y_ticks", "groups", "legend", "legend_title"]
    )
