"""Pinned mark colours: the plot's over the project's over the palette.

Stages 3-4 of ``.claude/plan-custom-mark-colors.md``;
docs/claude/plot-colors.md. The owner of "what colour is this mark" stays
``render.base.palette_for`` (and ``sample_palette_for`` for the overlay);
the pins are merged once per resolve by ``colors.mark_colors`` and read
there first. What is tested: the merge, that the DRAWN colours in both
renderers are the pins, that pinning one level moves no other, and that the
exported code paints what the preview paints.
"""

from __future__ import annotations

import json
from dataclasses import replace

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
pytest.importorskip("seaborn")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import to_hex  # noqa: E402

from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    FactorVariable,
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
from scistackplot.colors import (  # noqa: E402
    PALETTE,
    PLOT,
    PROJECT,
    MarkColors,
    mark_colors,
    merge,
    warn_duplicates,
)
from scistackplot.reduce import _PLAN_IRRELEVANT_FIELDS  # noqa: E402
from scistackplot.render.base import (  # noqa: E402
    DEFAULT_PALETTE,
    colorable,
    palette_for,
    sample_palette_for,
)

RED = "#ff0000"
GREEN = "#00ff00"
BLUE = "#0000ff"

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


def _with_project(table: LongTable, project: dict) -> LongTable:
    """``table`` carrying a project layer, the way ScidbSource attaches one."""
    return replace(table, colors_source=lambda: project)


def _spec(color="session", kind=PlotKind.BAR, colors=None, **style) -> PlotSpec:
    return PlotSpec(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP},
        groups=["session"],
        color=color,
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
        colors=colors or {},
        style=StyleOptions(**style),
    )


def _figure(spec, table):
    (figure,) = resolve(spec, table)
    return figure


def _bar_colours(figure) -> list[str]:
    bars = [p for p in figure.axes[0].patches if p.get_height()]
    return [to_hex(p.get_facecolor()) for p in sorted(bars, key=lambda p: p.get_x())]


def _mpl_colours(figure) -> set[str]:
    """Every face / edge / line colour drawn on the figure's axes, as hex."""
    found: set[str] = set()

    def add(values):
        try:
            import numpy as np

            array = np.atleast_2d(np.asarray(values, dtype=float))
        except (TypeError, ValueError):
            return
        for row in array:
            if row.size >= 3:
                found.add(to_hex(row[:3]))

    for ax in figure.axes:
        for patch in ax.patches:
            add(patch.get_facecolor())
            add(patch.get_edgecolor())
        for collection in ax.collections:
            add(collection.get_facecolor())
            add(collection.get_edgecolor())
        for line in ax.lines:
            found.add(to_hex(line.get_color()))
    return found


def _plotly_colours(payload: dict) -> str:
    """The payload's traces as text, colours normalised to hex where they are
    written as rgb()/rgba() (plotly fills carry their opacity that way)."""
    import re

    text = json.dumps(payload["data"]).lower()

    def to_hex_text(match):
        r, g, b = (int(float(v)) for v in match.groups())
        return f"#{r:02x}{g:02x}{b:02x}"

    return re.sub(r"rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)[^)]*\)", to_hex_text, text)


def _run(source: str, frame, function_name: str = "plot_m"):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


# --- the merge --------------------------------------------------------------------------


def test_the_plot_wins_level_by_level():
    colors = merge(
        project_default=None,
        project_levels={"session": {"s1": "#111111", "s2": "#222222"}},
        plot_default=None,
        plot_levels={"session": {"s1": "rgb(255, 0, 0)"}},
    )
    assert colors.levels["session"] == {"s1": (RED, PLOT), "s2": ("#222222", PROJECT)}


def test_an_empty_plot_pin_gives_the_level_back_to_the_palette():
    colors = merge(
        project_default=None,
        project_levels={"session": {"s1": "#111111"}},
        plot_default=None,
        plot_levels={"session": {"s1": ""}},
    )
    assert colors.levels == {}


def test_a_bad_colour_is_dropped_and_recorded():
    colors = merge(
        project_default="nonsense-colour",
        project_levels={"session": {"s1": "#zzzzzz", "s2": "#222222"}},
        plot_default=None,
        plot_levels={},
    )
    assert colors.levels == {"session": {"s2": ("#222222", PROJECT)}}
    assert colors.single is None
    assert len(colors.dropped) == 2


@pytest.mark.parametrize(
    "plot_default, project_default, expected",
    [
        (None, None, None),
        (None, "#111111", ("#111111", PROJECT)),
        ("#222222", "#111111", ("#222222", PLOT)),
        ("", "#111111", None),  # "" = the palette, even over the project's
    ],
)
def test_the_single_mark_colour(plot_default, project_default, expected):
    colors = merge(
        project_default=project_default,
        project_levels={},
        plot_default=plot_default,
        plot_levels={},
    )
    assert colors.single == expected


def test_a_grouping_column_is_pinned_under_its_qualified_name():
    spec = replace(_spec(color=None), factor_variables=[FactorVariable("Demographics", "Sex")])
    table = _with_project(
        LongTable.from_frame(
            pd.DataFrame({"Sex": ["F", "M"], "M": [1.0, 2.0]}),
            factors=["Sex"],
            measures=["M"],
        ),
        {"Demographics.Sex": {"F": RED}, "Sex": {"F": GREEN, "M": BLUE}},
    )
    colors = mark_colors(spec, table)
    assert colors.pinned("Sex", "F") == (RED, PROJECT)  # qualified entry first
    assert colors.pinned("Sex", "M") == (BLUE, PROJECT)  # then the bare one
    assert colors.entry_key("Sex") == "Demographics.Sex"


def test_colours_are_plan_irrelevant():
    assert "colors" in _PLAN_IRRELEVANT_FIELDS
    assert "style" in _PLAN_IRRELEVANT_FIELDS  # mark_color lives there


def test_the_spec_round_trips():
    spec = _spec(colors={"session": {"s1": RED}}, mark_color=GREEN)
    again = PlotSpec.from_dict(spec.to_dict())
    assert again.colors == {"session": {"s1": RED}}
    assert again.style.mark_color == GREEN


# --- the preview paints the pins --------------------------------------------------------


def test_pinning_one_level_moves_no_other(table):
    plain = _figure(_spec(), table)
    pinned = _figure(_spec(colors={"session": {"s1": RED}}), table)
    assert palette_for(pinned, "s1", 0) == RED
    assert palette_for(pinned, "s2", 1) == palette_for(plain, "s2", 1)


def test_the_project_layer_reaches_the_figure(table):
    figure = _figure(_spec(), _with_project(table, {"session": {"s2": "#00FF00"}}))
    assert palette_for(figure, "s2", 1) == GREEN
    assert palette_for(figure, "s1", 0) == DEFAULT_PALETTE[0]


def test_matplotlib_and_plotly_draw_the_pinned_bars(table):
    figure = _figure(_spec(colors={"session": {"s1": RED, "s2": BLUE}}), table)
    drawn = render_matplotlib(figure)
    assert _bar_colours(drawn) == [RED, BLUE]
    plt.close(drawn)
    bars = [t for t in render_plotly(figure)["data"] if t["type"] == "bar"]
    assert [t["marker"]["color"] for t in bars] == [RED, BLUE]


@pytest.mark.parametrize("kind", [PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN, PlotKind.STRIP])
def test_every_summary_kind_paints_the_pin(table, kind):
    """Bars, box fills, violin bodies, strip points: every kind draws its
    colour through `palette_for`, so every one shows the pin."""
    figure = _figure(_spec(kind=kind, colors={"session": {"s1": RED}}), table)
    drawn = render_matplotlib(figure)
    assert RED in _mpl_colours(drawn), kind
    plt.close(drawn)
    assert RED in _plotly_colours(render_plotly(figure)), kind


def test_line_plots_paint_the_pinned_line(series_table):
    """A line plot's lines are painted by their colour level: the pin."""
    kind = PlotKind.LINE
    spec = PlotSpec(
        measures=["Signal"],
        kind=kind,
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "session", "subject"],
        color="subject",
        colors={"subject": {"02": RED}},
    )
    figure = _figure(spec, series_table)
    drawn = render_matplotlib(figure)
    assert RED in _mpl_colours(drawn), kind
    plt.close(drawn)
    assert RED in _plotly_colours(render_plotly(figure)), kind


def test_the_single_mark_colour_paints_an_uncoloured_figure(table):
    figure = _figure(_spec(color=None, mark_color="rgb(0, 255, 0)"), table)
    drawn = render_matplotlib(figure)
    assert set(_bar_colours(drawn)) == {GREEN}
    plt.close(drawn)


def test_the_project_default_paints_an_uncoloured_figure(table):
    figure = _figure(_spec(color=None), _with_project(table, {"default": BLUE}))
    assert palette_for(figure, None, 0) == BLUE


def test_an_overlay_coloured_by_a_pinned_key_takes_the_pin(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
        show_sample=["subject"],
        sample_color="subject",
        colors={"subject": {"02": RED}},
    )
    figure = _figure(spec, scalar_table)
    assert figure.colors.sample_color == "subject"
    assert sample_palette_for(figure, "02", 1) == RED
    assert sample_palette_for(figure, "01", 0) != RED
    assert RED in _plotly_colours(render_plotly(figure))


# --- what the GUI is offered ------------------------------------------------------------


def test_colorable_lists_each_level_with_its_drawn_colour_and_origin(table):
    figure = _figure(
        _spec(colors={"session": {"s1": RED}}), _with_project(table, {"session": {"s2": BLUE}})
    )
    (entry,) = colorable(figure)
    assert entry["role"] == "colour" and entry["key"] == "session"
    assert entry["levels"] == [
        {"raw": "s1", "text": "s1", "hex": RED, "origin": PLOT},
        {"raw": "s2", "text": "s2", "hex": BLUE, "origin": PROJECT},
    ]
    assert render_plotly(figure)["layout"]["meta"]["colorable"] == [entry]


def test_colorable_offers_the_single_mark_colour_with_no_colour_layer(table):
    (entry,) = colorable(_figure(_spec(color=None), table))
    assert entry["role"] == "marks" and entry["key"] is None
    assert entry["levels"][0]["origin"] == PALETTE
    assert entry["levels"][0]["hex"] == DEFAULT_PALETTE[0]


def test_a_pin_repeating_another_levels_colour_is_warned():
    messages = warn_duplicates(
        [("session", "s1", RED, PLOT), ("session", "s2", RED, PALETTE)]
    )
    assert len(messages) == 1 and "'s1'" in messages[0] and "'s2'" in messages[0]
    assert warn_duplicates([("s", "a", RED, PALETTE), ("s", "b", RED, PALETTE)]) == []


def test_an_empty_markcolors_pins_nothing():
    colors = MarkColors()
    assert colors.mark("anything") is None
    assert colors.sample("anything") is None


# --- the export is the preview ----------------------------------------------------------


def test_no_pins_emit_no_pin_code(table):
    source = generate_plot_function(_spec(), table)
    assert "_hue_pins" not in source and "_hue_palette" not in source


@pytest.mark.parametrize("palette", [None, "viridis"])
def test_exported_bars_are_the_previews_pinned_colours(table, frame, palette):
    spec = _spec(colors={"session": {"s2": RED}}, palette=palette)
    drawn = render_matplotlib(_figure(spec, table))
    preview = _bar_colours(drawn)
    plt.close(drawn)
    assert preview[1] == RED
    source = generate_plot_function(spec, table)
    assert "_hue_pins = {'s2': '#ff0000'}" in source
    generated = _run(source, frame)
    assert _bar_colours(generated) == preview
    plt.close(generated)


def test_a_project_pin_is_baked_into_the_export(table, frame):
    projected = _with_project(table, {"session": {"s1": GREEN}})
    source = generate_plot_function(_spec(), projected)
    assert "'s1': '#00ff00'" in source
    generated = _run(source, frame)
    assert _bar_colours(generated)[0] == GREEN
    plt.close(generated)


def test_the_exported_single_mark_colour_is_the_previews(table, frame):
    spec = _spec(color=None, mark_color=BLUE)
    source = generate_plot_function(spec, table)
    assert f"color={BLUE!r}" in source
    generated = _run(source, frame)
    assert set(_bar_colours(generated)) == {BLUE}
    plt.close(generated)
