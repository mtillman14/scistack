"""Font size: one number scales every label, on every path, and never leaks."""

from __future__ import annotations

import io

import pytest

from scistackplot import (
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    generate_plot_function,
    render_plotly,
    resolve,
)

matplotlib = pytest.importorskip("matplotlib")
pytest.importorskip("seaborn")


@pytest.fixture
def big_spec():
    return PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
        style=StyleOptions(font_size=20.0),
    )


def _text_sizes(figure) -> dict[str, list[float]]:
    ax = figure.axes[0]
    legend = figure.legends[0] if figure.legends else ax.get_legend()
    return {
        "xlabel": [ax.xaxis.label.get_fontsize()],
        "ylabel": [ax.yaxis.label.get_fontsize()],
        "xticks": [t.get_fontsize() for t in ax.get_xticklabels()],
        "yticks": [t.get_fontsize() for t in ax.get_yticklabels()],
        "legend": [t.get_fontsize() for t in legend.get_texts()] if legend else [],
    }


def test_the_default_is_larger_than_matplotlibs():
    """10 pt at 8 x 6 in was unreadable on a slide (user, 2026-09-16)."""
    assert StyleOptions().font_size == 14.0
    assert StyleOptions().font_size > matplotlib.rcParamsDefault["font.size"]


def test_matplotlib_draws_every_label_at_the_spec_size(scalar_table, big_spec):
    from scistackplot import render_matplotlib

    before = matplotlib.rcParams["font.size"]
    figure = render_matplotlib(resolve(big_spec, scalar_table)[0])
    try:
        # The save happens OUTSIDE render's rc_context (plot_service does it),
        # and matplotlib creates tick objects lazily — so save first, then look.
        figure.savefig(io.BytesIO(), format="png", bbox_inches="tight")
        sizes = _text_sizes(figure)
        for name, values in sizes.items():
            assert values, name
            assert all(v == pytest.approx(20.0) for v in values), (name, values)
    finally:
        matplotlib.pyplot.close(figure)
    # rc_context, not rcParams: nothing leaks into the next figure.
    assert matplotlib.rcParams["font.size"] == before


def test_generated_code_draws_at_the_spec_size_without_leaking(
    scalar_table, scalar_frame, big_spec
):
    source = generate_plot_function(big_spec, scalar_table)
    assert 'with plt.rc_context({"font.size": 20.0}):' in source

    before = matplotlib.rcParams["font.size"]
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    figure = namespace["plot_steplength"](scalar_frame.copy(), "figure.png")
    try:
        figure.savefig(io.BytesIO(), format="png", bbox_inches="tight")
        sizes = _text_sizes(figure)
        for name in ("xlabel", "ylabel", "xticks", "yticks"):
            assert all(v == pytest.approx(20.0) for v in sizes[name]), (name, sizes[name])
    finally:
        matplotlib.pyplot.close(figure)
    assert matplotlib.rcParams["font.size"] == before


def test_plotly_preview_uses_the_same_number(scalar_table, big_spec):
    """Visible before a save: the preview font follows the setting as px."""
    layout = render_plotly(resolve(big_spec, scalar_table)[0])["layout"]
    assert layout["font"] == {"size": 20.0}


def test_font_size_survives_the_spec_round_trip(big_spec):
    assert PlotSpec.from_dict(big_spec.to_dict()).style.font_size == 20.0
