"""
Difference bars, Stage 5 (2026-09-27): the exported seaborn code draws the
bars the export renderer placed (plan D8, baked literals). Parity tests read
every expectation off the export renderer's own placement at the saved size
(``layout_decisions``), never typed by hand (export-matches-preview rule).
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

pytest.importorskip("seaborn")
pytest.importorskip("matplotlib")

import matplotlib  # noqa: E402

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402

import scistackplot.reduce as reduce_mod  # noqa: E402
from scistackplot import (  # noqa: E402
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    generate_plot_function,
    layout_decisions,
    resolve_one,
)
from scistackplot.diffbars import DifferenceBar  # noqa: E402

LAYER = "scistackplot"


@pytest.fixture(autouse=True)
def _clear_plan_cache():
    reduce_mod.clear_plan_cache()
    yield
    reduce_mod.clear_plan_cache()
    plt.close("all")


@pytest.fixture
def frame() -> pd.DataFrame:
    rows = []
    for group, subjects in (("sham", ("01", "02", "03")), ("stim", ("04", "05", "06"))):
        for subject in subjects:
            for side in ("L", "R"):
                for number, session in enumerate(("pre", "post", "follow")):
                    rows.append(
                        {
                            "group": group,
                            "subject": subject,
                            "side": side,
                            "session": session,
                            "Step": 2.0 + number + int(subject) * 0.1,
                        }
                    )
    return pd.DataFrame(rows)


@pytest.fixture
def table(frame) -> LongTable:
    return LongTable.from_frame(
        frame,
        factors=["group", "subject", "side", "session"],
        measures=["Step"],
        name="Step",
        schema_levels=["subject", "side", "session"],
        level_order={"session": ["pre", "post", "follow"], "group": ["sham", "stim"]},
    )


BARS = (
    DifferenceBar(match={"group": "sham", "side": "R"}, a={"session": "pre"}, b={"session": "post"}),
    DifferenceBar(
        match={"group": "sham", "side": "R"}, a={"session": "post"}, b={"session": "follow"}, label="**"
    ),
    DifferenceBar(match={"group": "sham", "side": "R"}, a={"session": "pre"}, b={"session": "follow"}),
)


def _spec(*bars: DifferenceBar, **extra) -> PlotSpec:
    return PlotSpec(
        measures=["Step"],
        kind=extra.pop("kind", PlotKind.BAR),
        roles=extra.pop(
            "roles",
            {
                "group": Role.ITERATE,
                "side": Role.FACET,
                "session": Role.GROUP,
                "subject": Role.COLLAPSE,
            },
        ),
        groups=extra.pop("groups", ["session"]),
        difference_bars=list(bars),
        style=StyleOptions(width=6.0, height=4.0),
        **extra,
    )


def _run(source: str, rows: pd.DataFrame, spec: PlotSpec):
    from scistackplot.codegen import default_function_name

    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[default_function_name(spec)](rows.copy(), "figure.png")


def _drawn_bars(fig):
    """{panel column: [(xdata, ydata), ...]} and labels, from the exported figure."""
    found = {}
    for ax in fig.axes:
        if ax.get_subplotspec() is None:
            continue
        col = ax.get_subplotspec().colspan.start
        lines = [
            (tuple(line.get_xdata()), tuple(line.get_ydata()))
            for line in ax.lines
            if line.get_gid() == "difference-bar"
        ]
        labels = [t.get_text() for t in ax.texts if t.get_gid() == "difference-label"]
        found[col] = {"lines": lines, "labels": labels, "ylim": ax.get_ylim()}
    return found


def _placement(spec, table, index):
    figure, _, _ = resolve_one(spec, table, index)
    return figure, layout_decisions(figure)["difference_bars"]


def test_the_export_bakes_the_bars_and_says_they_are_fitted(table):
    source = generate_plot_function(_spec(*BARS), table)
    assert "_diff_bars = {" in source
    assert "regenerate after the data changes" in source
    # Baked numbers are plain floats: a numpy scalar's repr (np.float64(...))
    # names a module the generated code does not import (2026-09-27).
    baked = [line for line in source.splitlines() if "_diff_" in line and " = " in line]
    assert baked and not any("np." in line for line in baked)


def test_no_bars_leaves_the_generated_code_unchanged(table):
    assert "_diff_" not in generate_plot_function(_spec(), table)


def test_a_bar_that_draws_nowhere_leaves_the_code_unchanged(table):
    gone = DifferenceBar(match={"group": "sham", "side": "R"}, a={"session": "pre"}, b={"session": "later"})
    assert "_diff_" not in generate_plot_function(_spec(gone), table)


def test_the_exported_bars_are_the_placed_bars(table, frame):
    spec = _spec(*BARS)
    figure, placement = _placement(spec, table, 0)
    index = next(i for i, p in enumerate(figure.panels) if p.key["side"] == "R")
    col = figure.panels[index].grid_col
    fig = _run(generate_plot_function(spec, table), frame[frame["group"] == "sham"], spec)
    drawn = _drawn_bars(fig)[col]
    expected = {
        (
            (b.left, b.left, b.right, b.right),
            (b.left_foot, b.y, b.y, b.right_foot),
        )
        for b in placement.bars[index]
    }
    got = {
        (tuple(round(v, 9) for v in xs), tuple(round(v, 9) for v in ys))
        for xs, ys in drawn["lines"]
    }
    assert got == {
        (tuple(round(v, 9) for v in xs), tuple(round(v, 9) for v in ys)) for xs, ys in expected
    }
    assert sorted(drawn["labels"]) == sorted(b.label for b in BARS)
    assert drawn["ylim"] == pytest.approx(placement.limits[index])


def test_every_panel_of_the_figure_draws_the_placed_range(table, frame):
    spec = _spec(*BARS)
    figure, placement = _placement(spec, table, 0)
    fig = _run(generate_plot_function(spec, table), frame[frame["group"] == "sham"], spec)
    drawn = _drawn_bars(fig)
    for index, panel in enumerate(figure.panels):
        assert drawn[panel.grid_col]["ylim"] == pytest.approx(placement.limits[index])


def test_a_sibling_figure_is_raised_to_the_shared_top(table, frame):
    """Scope [] spans the figures: stim draws no bars but shares sham's raised top."""
    spec = _spec(*BARS)
    _, placement = _placement(spec, table, 0)
    shared_top = max(high for _, high in placement.limits.values())
    fig = _run(generate_plot_function(spec, table), frame[frame["group"] == "stim"], spec)
    for panel in _drawn_bars(fig).values():
        assert panel["lines"] == []
        assert panel["ylim"][1] == pytest.approx(shared_top)


def test_the_label_is_the_differences_text_size(table, frame):
    from scistackplot.spec import TextSizes

    from dataclasses import replace

    spec = replace(
        _spec(*BARS), style=StyleOptions(width=6.0, height=4.0, text=TextSizes(differences=19.0))
    )
    fig = _run(generate_plot_function(spec, table), frame[frame["group"] == "sham"], spec)
    sizes = {
        t.get_fontsize() for ax in fig.axes for t in ax.texts if t.get_gid() == "difference-label"
    }
    assert sizes == {19.0}


def test_a_nested_axis_maps_the_ends_through_the_exported_order(table, frame):
    """The export drops the preview's spacers, so a bar's x is looked up by
    its tick's name in the order the export drew."""
    from scistackplot.codegen import _nested_x_order
    from scistackplot.roles import complete_roles
    from scistackplot.cell import effective_shape

    spec = _spec(
        DifferenceBar(
            match={"side": "R"},
            a={"group": "sham", "session": "pre"},
            b={"group": "stim", "session": "pre"},
        ),
        roles={
            "group": Role.GROUP,
            "session": Role.GROUP,
            "side": Role.ITERATE,
            "subject": Role.COLLAPSE,
        },
        groups=["session", "group"],
    )
    order = _nested_x_order(spec, table, complete_roles(spec, table), effective_shape(spec, table))
    fig = _run(generate_plot_function(spec, table), frame[frame["side"] == "R"], spec)
    (panel,) = [p for p in _drawn_bars(fig).values() if p["lines"]]
    (xs, _), = panel["lines"]
    assert (xs[0], xs[-1]) == (order.index("sham · pre"), order.index("stim · pre"))


def test_the_export_is_logged(table, caplog):
    with caplog.at_level(logging.INFO, logger=LAYER):
        generate_plot_function(_spec(*BARS), table)
    assert "export difference bars: 3 bar(s)" in caplog.text
