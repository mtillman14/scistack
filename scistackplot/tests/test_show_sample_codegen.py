"""
"Show sample" — Stage 4: the exported seaborn code draws the same overlay.

The generated function builds ``_sample`` from the frame BEFORE the marks'
chain (the same cut ``reduce`` draws) and places each point by the same
arithmetic as ``render.base.sample_positions``; running it and comparing the
marker positions with ``render_matplotlib`` is the proof the two spellings of
the rule agree.
"""

from __future__ import annotations

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.collections import PathCollection  # noqa: E402

pytest.importorskip("seaborn")

from plot_geometry import despaced  # noqa: E402

from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    generate_plot_function,
    render_matplotlib,
    resolve,
)

# Sessions named so that seaborn's order of appearance after the emitted
# groupby (sorted) is the declared order too: the export's flat-x ordering is
# seaborn's, and that is a separate question from where the points land.
ROWS = [
    ("01", "s1", "1", 1.0),
    ("01", "s1", "2", 2.0),
    ("01", "s1", "3", 3.0),
    ("02", "s1", "1", 9.0),
    ("01", "s2", "1", 10.0),
    ("01", "s2", "2", 20.0),
    ("02", "s2", "1", 30.0),
]


@pytest.fixture
def frame() -> pd.DataFrame:
    return pd.DataFrame(ROWS, columns=["subject", "session", "trial", "M"])


@pytest.fixture
def table(frame) -> LongTable:
    return LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
    )


def _spec(show, kind=PlotKind.BAR, **kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
        show_sample=list(show),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _run(source: str, frame, function_name: str = "plot_m"):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


def _marker_xs(figure) -> list[float]:
    return sorted(
        round(float(x), 6)
        for line in figure.axes[0].get_lines()
        if line.get_marker() == "o"
        for x in line.get_xdata()
    )


def _preview_xs(resolved) -> list[float]:
    """The preview's overlay x positions, spacer slots removed (a nested
    preview axis has them, the export does not — `plot_geometry.despaced`)."""
    drawn = render_matplotlib(resolved)
    if resolved.sample_join:
        xs = _marker_xs(drawn)
    else:
        xs = [
            float(x)
            for c in drawn.axes[0].collections
            if isinstance(c, PathCollection)
            for x, _ in c.get_offsets()
        ]
    plt.close(drawn)
    return sorted(despaced(xs, resolved))


# --- the source -----------------------------------------------------------------


def test_no_selection_emits_no_overlay(table):
    source = generate_plot_function(_spec([]), table)
    assert "_sample = " not in source and "# show sample" not in source
    assert "import re" not in source


def test_show_subject_averages_trials_before_the_marks_chain(table):
    source = generate_plot_function(_spec(["subject"]), table)
    assert "# show sample: one point per subject; trial averaged within it" in source
    assert "_sample = df.groupby(['subject', 'session'], as_index=False)[['M']].mean()" in source
    # Built BEFORE the marks' own chain consumes df.
    assert source.index("_sample = df.groupby") < source.index("# collapse trial")
    assert "lines join each subject" in source
    assert "linestyle='-'" in source
    assert "import re" in source


def test_show_trial_is_the_raw_frame_with_an_identity(table):
    source = generate_plot_function(_spec(["trial"]), table)
    assert "_sample = df.copy()" in source
    assert "_sample['_series'] = _sample['subject'].astype(str).str.cat(_sample[['trial']].astype(str), sep=' | ')" in source
    assert "points only" in source and "linestyle='none'" in source


def test_pooled_is_one_groupby(table):
    spec = _spec(["subject"], aggregate=Aggregation(error=ErrorBand.SD, pooled=True))
    source = generate_plot_function(spec, table)
    assert "# pooled (weight by N)" in source


def test_an_inert_selection_emits_nothing(table):
    """A ticked name that is not collapsed (session groups) is inert — no
    `_sample` in the export, as none is drawn in the preview."""
    assert "_sample = " not in generate_plot_function(_spec(["session"]), table)


# --- run it ---------------------------------------------------------------------------


@pytest.mark.parametrize("show", [["subject"], ["trial"]])
def test_generated_points_land_where_the_preview_draws_them(table, frame, show):
    spec = _spec(show)
    (resolved,) = resolve(spec, table)
    figure = _run(generate_plot_function(spec, table), frame)
    generated = _marker_xs(figure)
    plt.close(figure)
    assert generated == _preview_xs(resolved)
    assert len(generated) == (4 if show == ["subject"] else 7)


def test_generated_points_sit_on_their_hues_own_bar(table, frame):
    """Colour = subject on a [subject, session] grouping: seaborn is told
    `dodge=False`, so subject 02's trials sit on subject 02's own bar in
    the export exactly where the preview draws them."""
    spec = _spec(["trial"], roles={
        "subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE,
    }, groups=["subject", "session"], color="subject")
    (resolved,) = resolve(spec, table)
    figure = _run(generate_plot_function(spec, table), frame)
    generated = _marker_xs(figure)
    plt.close(figure)
    assert generated == _preview_xs(resolved)


def test_generated_lines_are_one_per_subject_left_to_right(table, frame):
    figure = _run(generate_plot_function(_spec(["subject"]), table), frame)
    lines = [line for line in figure.axes[0].get_lines() if line.get_marker() == "o"]
    assert len(lines) == 2
    for line in lines:
        xs = list(line.get_xdata())
        assert xs == sorted(xs) and len(xs) == 2
    # The legend is the marks' — no entry per point.
    assert not figure.legends or all(
        t.get_text() not in {"01", "02"} for legend in figure.legends for t in legend.get_texts()
    )
    plt.close(figure)


def test_generated_overlay_survives_a_facet(table, frame):
    spec = _spec(["trial"], roles={
        "subject": Role.COLLAPSE, "session": Role.FACET, "trial": Role.COLLAPSE,
    }, groups=[])
    figure = _run(generate_plot_function(spec, table), frame)
    counts = sorted(
        sum(len(line.get_xdata()) for line in ax.get_lines() if line.get_marker() == "o")
        for ax in figure.axes
    )
    assert counts == [3, 4], "s2 has three trials, s1 four"
    plt.close(figure)
