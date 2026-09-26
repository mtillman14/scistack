"""The marks' palette: one owner (``render.base.mark_palette``) for the
preview and the export (2026-09-26).

Two defects, both "the export is not the previewed figure":

* the preview ignored ``StyleOptions.palette`` and always drew
  ``DEFAULT_PALETTE``, while the export honoured the name (and, with no
  name, fell back to seaborn's default colours instead of the preview's);
* seaborn desaturates bar / box / violin fills to 75% by default, so the
  exported bars were paler than the preview's.
"""

from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
sns = pytest.importorskip("seaborn")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import to_hex  # noqa: E402

from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    generate_plot_function,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.render import base  # noqa: E402
from scistackplot.render.base import DEFAULT_PALETTE, mark_palette, palette_for  # noqa: E402

ROWS = [
    ("01", "s1", 1.0),
    ("01", "s2", 10.0),
    ("02", "s1", 5.0),
    ("02", "s2", 20.0),
    ("03", "s1", 7.0),
    ("03", "s2", 30.0),
]


@pytest.fixture
def frame() -> pd.DataFrame:
    return pd.DataFrame(ROWS, columns=["subject", "session", "M"])


@pytest.fixture
def table(frame) -> LongTable:
    return LongTable.from_frame(
        frame,
        factors=["subject", "session"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session"],
    )


def _spec(palette=None, color="session", kind=PlotKind.BAR) -> PlotSpec:
    return PlotSpec(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP},
        groups=["session"],
        color=color,
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
        style=StyleOptions(palette=palette),
    )


def _figure(spec, table):
    (figure,) = resolve(spec, table)
    return figure


def _run(source: str, frame, function_name: str = "plot_m"):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


def _bar_colours(figure) -> list[str]:
    """Bar fills left to right, as hex (alpha dropped: the preview draws
    bars at ``style.alpha``, which is not what this file tests)."""
    bars = [p for p in figure.axes[0].patches if p.get_height()]
    return [to_hex(p.get_facecolor()) for p in sorted(bars, key=lambda p: p.get_x())]


# --- the owner -------------------------------------------------------------------------


def test_no_palette_is_the_default():
    assert mark_palette(None, 5) == DEFAULT_PALETTE


def test_a_named_palette_is_seaborns_over_the_level_count():
    assert mark_palette("viridis", 2) == tuple(sns.color_palette("viridis", 2).as_hex())
    assert mark_palette("viridis", 3) != mark_palette("viridis", 2), "a colormap is sampled per n"


def test_an_unknown_palette_falls_back_and_says_so(monkeypatch):
    warned: list[str] = []
    monkeypatch.setattr(base.Log, "warn", lambda msg, *a, **k: warned.append(msg % a))
    mark_palette.cache_clear()
    assert mark_palette("no-such-palette", 2) == DEFAULT_PALETTE
    assert warned and "no-such-palette" in warned[0]
    mark_palette.cache_clear()


# --- the preview honours the name ------------------------------------------------------


def test_preview_paints_with_the_named_palette(table):
    figure = _figure(_spec("viridis"), table)
    expected = list(sns.color_palette("viridis", 2).as_hex())
    assert [palette_for(figure, level, i) for i, level in enumerate(["s1", "s2"])] == expected
    drawn = render_matplotlib(figure)
    assert _bar_colours(drawn) == expected
    plt.close(drawn)
    payload = render_plotly(figure)
    bars = [t for t in payload["data"] if t["type"] == "bar"]
    assert [t["marker"]["color"] for t in bars] == expected


# --- the export is the preview -----------------------------------------------------------


@pytest.mark.parametrize("palette", [None, "viridis", "Set2"])
def test_exported_bars_are_the_previews_colour(table, frame, palette):
    """Palette AND saturation: seaborn's default 0.75 saturation made the
    exported bars paler even when the palette agreed."""
    spec = _spec(palette)
    drawn = render_matplotlib(_figure(spec, table))
    preview = _bar_colours(drawn)
    plt.close(drawn)
    source = generate_plot_function(spec, table)
    assert "saturation=1" in source
    generated = _run(source, frame)
    assert _bar_colours(generated) == preview
    plt.close(generated)


def test_an_uncoloured_export_states_the_previews_one_colour(table, frame):
    """No hue: seaborn >= 0.13 reads `palette=` without `hue` as "colour each
    x level", so the export states `color=` — the preview's one colour."""
    spec = _spec("viridis", color=None)
    figure = _figure(spec, table)
    drawn = render_matplotlib(figure)
    preview = _bar_colours(drawn)
    plt.close(drawn)
    assert set(preview) == {palette_for(figure, None, 0)}
    source = generate_plot_function(spec, table)
    assert "palette=" not in source.split("sns.catplot(", 1)[1].split("\n)", 1)[0]
    generated = _run(source, frame)
    assert _bar_colours(generated) == preview
    plt.close(generated)


def test_strip_is_not_desaturated_by_the_export(table):
    """`saturation` is a fill argument: only the kinds seaborn fades get it."""
    source = generate_plot_function(replace(_spec(), kind=PlotKind.STRIP), table)
    assert "saturation=" not in source


# --- the fill opacity: one owner (render.base.fill_alpha), 2026-09-26 -------------------

FILL_KINDS = [PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN]


def _fill_alphas(figure, kind) -> set[float]:
    """The opacity of every drawn FILL: bar rectangles, box patches, violin
    bodies (a PolyCollection in matplotlib and seaborn's `fill_between`)."""
    from matplotlib.collections import PolyCollection
    from matplotlib.patches import PathPatch, Rectangle

    ax = figure.axes[0]
    if kind is PlotKind.VIOLIN:
        faces = [c.get_facecolor()[0] for c in ax.collections if isinstance(c, PolyCollection)]
    elif kind is PlotKind.BOX:
        faces = [p.get_facecolor() for p in ax.patches if isinstance(p, PathPatch)]
    else:
        faces = [p.get_facecolor() for p in ax.patches if isinstance(p, Rectangle) and p.get_height()]
    return {round(float(face[3]), 3) for face in faces}


def test_fill_alpha_is_the_style_for_bars_and_fixed_for_distributions():
    style = StyleOptions(alpha=0.4)
    assert base.fill_alpha(PlotKind.BAR, style) == 0.4
    assert base.fill_alpha(PlotKind.BOX, style) == base.BOX_FILL_ALPHA
    assert base.fill_alpha(PlotKind.VIOLIN, style) == base.VIOLIN_FILL_ALPHA
    assert base.fill_alpha(PlotKind.STRIP, style) is None


@pytest.mark.parametrize("kind", FILL_KINDS)
def test_the_preview_draws_the_owners_opacity(table, kind):
    spec = _spec(kind=kind)
    drawn = render_matplotlib(_figure(spec, table))
    assert _fill_alphas(drawn, kind) == {base.fill_alpha(kind, spec.style)}
    plt.close(drawn)


def test_plotly_draws_the_owners_opacity(table):
    """plotly used to draw opaque bars and its own half-transparent box fill."""
    payload = render_plotly(_figure(_spec(kind=PlotKind.BAR), table))
    bars = [t for t in payload["data"] if t["type"] == "bar"]
    assert bars and {t["marker"]["opacity"] for t in bars} == {StyleOptions().alpha}
    for kind in (PlotKind.BOX, PlotKind.VIOLIN):
        payload = render_plotly(_figure(_spec(kind=kind), table))
        traces = [t for t in payload["data"] if t["type"] in ("box", "violin")]
        alpha = base.fill_alpha(kind, StyleOptions())
        assert traces and all(t["fillcolor"].endswith(f",{alpha})") for t in traces)


@pytest.mark.parametrize("kind", FILL_KINDS)
def test_exported_fills_are_the_previews_opacity(table, frame, kind):
    """The export drew every fill opaque (2026-09-26)."""
    spec = _spec(kind=kind)
    drawn = render_matplotlib(_figure(spec, table))
    preview = _fill_alphas(drawn, kind)
    plt.close(drawn)
    generated = _run(generate_plot_function(spec, table), frame)
    assert _fill_alphas(generated, kind) == preview
    plt.close(generated)
