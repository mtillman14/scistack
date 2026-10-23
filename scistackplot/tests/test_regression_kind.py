"""PlotKind.REGRESSION (docs/claude/histogram-and-regression.md).

User rules (2026-10-22): an x measure against a y measure (numeric or
datetime x), a least-squares line per colour level (pooled when uncoloured),
R², p and n drawn on the plot, a t-based band off by default, no slope in
the text, log axes ignored. The fit is ``csvstats.regression.linear_fit``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from csvstats.regression import linear_fit

from scistackplot import (
    FitBy,
    LongTable,
    PlotKind,
    PlotSpec,
    RegressionOptions,
    Role,
    StyleOptions,
    capabilities,
    generate_plot_function,
    plot_data,
    render_plotly,
    resolve,
)
from scistackplot.capability import available_plots, why_unavailable
from scistackplot.codegen import function_params
from scistackplot.fits import STATS_TEXT_SOURCE, annotation_text
from scistackplot.reduce import clear_plan_cache
from scistackplot.roles import CONTINUOUS_LOG_REASON, REGRESSION_NEEDS_X, RoleError
from scistackplot.shape import Shape


@pytest.fixture(autouse=True)
def _fresh_plans():
    clear_plan_cache()
    yield
    clear_plan_cache()


@pytest.fixture
def xy_table() -> LongTable:
    rng = np.random.default_rng(7)
    rows = []
    for group, slope in (("A", 2.0), ("B", -1.0)):
        for subject in range(1, 9):
            x = float(subject) + rng.normal(0, 0.1)
            rows.append(
                {
                    "group": group,
                    "subject": f"{group}{subject:02d}",
                    "Mass": x,
                    "Speed": 1.0 + slope * x + float(rng.normal(0, 0.3)),
                }
            )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["group", "subject"],
        measures=["Speed", "Mass"],
        schema_levels=["subject"],
    )


def reg(**kwargs) -> PlotSpec:
    base = dict(
        measures=["Speed"],
        x_measure="Mass",
        roles={"group": Role.GROUP, "subject": Role.GROUP},
        groups=["subject", "group"],
        color="group",
        kind=PlotKind.REGRESSION,
    )
    base.update(kwargs)
    return PlotSpec(**base)


def test_offered_only_with_an_x_measure():
    assert PlotKind.REGRESSION in available_plots(Shape.SCALAR, {}, has_x_measure=True)
    assert PlotKind.REGRESSION not in available_plots(Shape.SCALAR, {})
    assert why_unavailable(PlotKind.REGRESSION, Shape.SCALAR, {}) == REGRESSION_NEEDS_X


def test_refused_without_an_x_measure(xy_table):
    with pytest.raises(RoleError, match="X variable"):
        resolve(reg(x_measure=None, roles={"subject": Role.COLLAPSE}, groups=[], color=None), xy_table)


def test_one_fit_per_colour_level_is_csvstats_fit(xy_table):
    (figure,) = resolve(reg(), xy_table)
    fits = figure.panels[0].fits
    assert [str(f.level) for f in fits] == ["A", "B"]
    frame = xy_table.frame
    for fit in fits:
        rows = frame[frame["group"] == fit.level]
        expected = linear_fit(rows["Mass"], rows["Speed"])
        assert fit.fit.slope == pytest.approx(expected.slope)
        assert fit.fit.r_squared == pytest.approx(expected.r_squared)
        assert fit.fit.p_value == pytest.approx(expected.p_value)
    # Labelled by level when a panel holds several fits; no slope in the text.
    assert fits[0].text.startswith("A: R²")
    assert "slope" not in fits[0].text
    # The line runs over the level's own x range.
    a = frame[frame["group"] == "A"]["Mass"]
    assert fits[0].xs[0] == pytest.approx(a.min()) and fits[0].xs[-1] == pytest.approx(a.max())


def test_pooled_is_one_fit(xy_table):
    (figure,) = resolve(reg(regression=RegressionOptions(fit_by=FitBy.POOLED)), xy_table)
    (fit,) = figure.panels[0].fits
    assert fit.level is None and fit.fit.n == 16
    assert not fit.text.startswith("A")


def test_band_only_when_asked(xy_table):
    (plain,) = resolve(reg(), xy_table)
    assert plain.panels[0].fits[0].low is None
    (banded,) = resolve(reg(regression=RegressionOptions(ci_band=True)), xy_table)
    fit = banded.panels[0].fits[0]
    assert all(lo <= y <= hi for lo, y, hi in zip(fit.low, fit.ys, fit.high))


def test_too_few_points_draw_no_line_and_say_why(xy_table):
    small = LongTable.from_frame(
        xy_table.frame.head(2), factors=["group", "subject"], measures=["Speed", "Mass"]
    )
    (figure,) = resolve(reg(color=None, roles={"subject": Role.GROUP}, groups=["subject"]), small)
    assert not figure.panels[0].fits[0].fit.ok
    assert any("No line" in note for note in figure.layout_notes)


def test_annotation_text_rule():
    fit = linear_fit([1, 2, 3, 4, 5], [2, 4, 5, 4, 5])
    text = annotation_text(fit)
    assert text.startswith("R² = 0.60") and text.endswith("n = 5")
    strong = linear_fit(np.arange(50.0), np.arange(50.0) * 2 + np.sin(np.arange(50.0)))
    assert "p < 0.001" in annotation_text(strong)


def test_generated_text_matches_the_library_rule():
    namespace: dict = {}
    exec(STATS_TEXT_SOURCE, namespace)  # noqa: S102
    fit = linear_fit([1, 2, 3, 4, 5], [2, 4, 5, 4, 5])
    assert namespace["_scistack_fit_text"](fit.r_squared, fit.p_value, fit.n, "B") == annotation_text(fit, "B")


def test_log_axes_are_ignored(xy_table):
    (figure,) = resolve(reg(style=StyleOptions(log_x=True)), xy_table)
    assert not figure.spec.style.log_x
    assert CONTINUOUS_LOG_REASON in figure.layout_notes
    source = generate_plot_function(reg(style=StyleOptions(log_x=True)), xy_table)
    assert 'xscale="log"' not in source


def test_preview_draws_lines_and_annotations(xy_table):
    (figure,) = resolve(reg(regression=RegressionOptions(ci_band=True)), xy_table)
    payload = render_plotly(figure)
    notes = [a for a in payload["layout"]["annotations"] if a.get("name") == "fit-annotation"]
    assert [n["text"] for n in notes] == [f.text for f in figure.panels[0].fits]
    fills = [t for t in payload["data"] if t.get("fill") == "toself"]
    assert len(fills) == 2
    assert len(payload["layout"]["meta"]["fits"][0]["fits"]) == 2


def test_save_data_offers_the_fits(xy_table):
    data = plot_data(reg(), xy_table, depth="__fits")
    assert list(data["group"]) == ["A", "B"]
    assert {"n", "slope", "r_squared", "p_value"} <= set(data.columns)


def test_capabilities_report_the_section(xy_table):
    caps = capabilities(reg(), xy_table)
    assert caps["regression"]["applies"] and caps["regression"]["problem"] is None
    assert caps["x_measure"]["current"] == "Mass"


# --- datetime x ------------------------------------------------------------


@pytest.fixture
def dated_table() -> LongTable:
    days = pd.date_range("2026-07-01", periods=10, freq="D")
    frame = pd.DataFrame(
        {
            "session": [str(i) for i in range(10)],
            "When": pd.Series(list(days), dtype=object),
            "Score": [3.0 + 2.0 * i + (0.1 if i % 2 else -0.1) for i in range(10)],
        }
    )
    return LongTable.from_frame(
        frame, factors=["session"], measures=["Score", "When"], schema_levels=["session"]
    )


def test_datetime_x_fits_on_a_date_axis(dated_table):
    spec = PlotSpec(
        measures=["Score"],
        x_measure="When",
        roles={"session": Role.COLLAPSE},
        kind=PlotKind.REGRESSION,
    )
    (figure,) = resolve(spec, dated_table)
    assert figure.temporal_x is not None
    (fit,) = figure.panels[0].fits
    assert fit.fit.slope * 86_400_000 == pytest.approx(2.0, rel=0.05)  # +2 a day
    assert fit.fit.r_squared > 0.99
    assert render_plotly(figure)["layout"]["xaxis"]["type"] == "date"


# --- the export --------------------------------------------------------------


def test_the_function_takes_the_x_input():
    assert function_params(reg()) == ["df", "df_x", "filename"]


def test_generated_code_fits_the_previewed_lines(xy_table):
    pytest.importorskip("seaborn")
    spec = reg()
    source = generate_plot_function(spec, xy_table, function_name="plot_r")
    assert "from csvstats.regression import linear_fit" in source
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    frame = xy_table.frame.copy()
    figure = namespace["plot_r"](frame, frame, "figure.png")
    (preview,) = resolve(spec, xy_table)
    ax = figure.axes[0]
    texts = [t.get_text() for t in ax.texts]
    assert texts == [f.text for f in preview.panels[0].fits]
    lines = [line for line in ax.get_lines() if len(line.get_xdata()) == 50]
    assert len(lines) == 2
    assert lines[0].get_ydata()[0] == pytest.approx(preview.panels[0].fits[0].ys[0])
