"""Custom plot kinds, Stages 2-3: calling the user's ``plot_`` function.

``.claude/plan-custom-plot-kinds.md``. What one call receives (per role, per
``as_table``, across schema levels), what is refused and why, and that the
function draws inside the ordinary figure: panels, size, preview, file.
"""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    CustomCall,
    CustomFunctionError,
    CustomPlot,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    RoleError,
    StyleOptions,
    YAxis,
    custom_preview,
    custom_requirement,
    inspect_function,
    resolve_custom,
    resolve_custom_one,
    write_figure,
)
from scistackplot.custom import FIGURE_LEVEL, PANEL_LEVEL, layout_kind
from scistackplot.render.mpl import layout_report
from scistackplot.shape import Shape

from conftest import SESSIONS, SUBJECTS

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


@pytest.fixture
def mass_table() -> LongTable:
    """A subject-level variable: coarser than the trial-level StepLength."""
    frame = pd.DataFrame(
        {"subject": SUBJECTS, "Mass": [60.0 + 10 * i for i in range(len(SUBJECTS))]}
    )
    return LongTable.from_frame(
        frame,
        factors=["subject"],
        measures=["Mass"],
        name="Mass",
        schema_levels=["subject"],
    )


class Recorder:
    """A plot_ function that records every call's arguments."""

    def __init__(self):
        self.calls: list[dict] = []

    def panel(self, steps, mass, filename, ax, context=None):
        self.calls.append({"steps": steps, "mass": mass, "context": context})
        for _, row in steps.iterrows():
            ax.plot([context.x(row)], [row["StepLength"]], "o", color=context.color_of(row.get(context.color)))

    def figure(self, steps, mass, filename):
        self.calls.append({"steps": steps, "mass": mass})
        fig, ax = plt.subplots()
        ax.plot(steps["StepLength"].to_numpy())
        return fig


def _named(fn, name: str):
    """A plain function wrapping ``fn`` under ``name`` (bound methods cannot
    be renamed, and the signature must stay the method's)."""
    import functools

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)

    wrapper.__name__ = name
    return wrapper


def _spec(function="plot_steps", **changes) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"session": Role.GROUP, "trial": Role.COLLAPSE, "subject": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.CUSTOM,
        custom=CustomPlot(function=function),
    )
    base.update(changes)
    return PlotSpec(**base)


def _make(recorder_method, scalar_table, mass_table, name="plot_steps") -> CustomCall:
    fn = _named(recorder_method, name)
    return CustomCall(
        fn=fn,
        inputs={"steps": scalar_table, "mass": mass_table},
        variables={"steps": "StepLength", "mass": "Mass"},
        constants={"filename": None},
    )


# ---------------------------------------------------------------------------
# The signature decides the level
# ---------------------------------------------------------------------------


def test_a_function_taking_ax_is_panel_level():
    rec = Recorder()
    signature = inspect_function(_named(rec.panel, "plot_steps"))
    assert signature.level == PANEL_LEVEL
    assert signature.accepts_context
    assert "context" in signature.optional


def test_a_function_without_ax_is_figure_level():
    rec = Recorder()
    assert inspect_function(_named(rec.figure, "plot_steps")).level == FIGURE_LEVEL


@pytest.mark.parametrize(
    "shape, kind",
    [(Shape.SCALAR, PlotKind.SCATTER), (Shape.SERIES_1D, PlotKind.LINE), (Shape.MATRIX_2D, PlotKind.HEATMAP)],
)
def test_layout_kind_never_cell_collapses(shape, kind):
    assert layout_kind(shape) is kind


def test_the_deepest_input_lays_the_figure_out(scalar_table, mass_table):
    call = _make(Recorder().panel, scalar_table, mass_table)
    assert call.primary == "steps"


# ---------------------------------------------------------------------------
# What one call receives
# ---------------------------------------------------------------------------


def test_each_input_is_its_own_table_collapsed_to_the_sample(scalar_table, mass_table):
    rec = Recorder()
    call = _make(rec.panel, scalar_table, mass_table)
    figure, _labels, _index = resolve_custom_one(_spec(), call, 0)
    kwargs = figure.custom.arguments({})
    steps = kwargs["steps"]
    # trial averaged within subject; subject is the sample, session a group.
    assert set(steps.columns) == {"subject", "session", "StepLength"}
    assert len(steps) == len(SUBJECTS) * len(SESSIONS)
    expected = (
        scalar_table.frame.groupby(["subject", "session"])["StepLength"].mean()
    )
    got = steps.set_index(["subject", "session"])["StepLength"]
    pd.testing.assert_series_equal(got.sort_index(), expected.sort_index(), check_names=False)
    # The coarse input is passed whole, at its own level.
    assert list(kwargs["mass"].columns) == ["subject", "Mass"]
    assert len(kwargs["mass"]) == len(SUBJECTS)
    assert kwargs["filename"] is None


def test_an_iterated_key_selects_each_figure_and_broadcasts_the_coarse_input(
    scalar_table, mass_table
):
    rec = Recorder()
    call = _make(rec.panel, scalar_table, mass_table)
    spec = _spec(roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.COLLAPSE})
    figures = resolve_custom(spec, call)
    assert [f.figure_key for f in figures] == [{"subject": s} for s in SUBJECTS]
    for figure, subject in zip(figures, SUBJECTS):
        kwargs = figure.custom.arguments({})
        assert set(kwargs["steps"]["subject"]) == {subject}
        assert list(kwargs["mass"]["subject"]) == [subject]


def test_a_faceted_key_selects_each_panel(scalar_table, mass_table):
    rec = Recorder()
    call = _make(rec.panel, scalar_table, mass_table)
    spec = _spec(roles={"session": Role.FACET, "trial": Role.COLLAPSE, "subject": Role.COLLAPSE}, groups=[])
    figure = resolve_custom(spec, call)[0]
    for panel in figure.panels:
        steps = figure.custom.arguments(panel.key)["steps"]
        assert set(steps["session"]) == {panel.key["session"]}


def test_a_value_input_gets_the_bare_value(scalar_table, mass_table):
    rec = Recorder()
    call = _make(rec.panel, scalar_table, mass_table)
    spec = _spec(
        roles={"subject": Role.ITERATE, "session": Role.FACET, "trial": Role.GROUP},
        groups=["trial"],
        custom=CustomPlot(function="plot_steps", as_table=["steps"]),
    )
    figure = resolve_custom(spec, call)[0]
    kwargs = figure.custom.arguments(figure.panels[0].key)
    assert kwargs["mass"] == pytest.approx(60.0)
    assert isinstance(kwargs["steps"], pd.DataFrame)


def test_one_d_cells_average_sample_by_sample(series_table):
    def plot_signal(signal, ax):
        pass

    call = CustomCall(fn=plot_signal, inputs={"signal": series_table}, variables={"signal": "Signal"})
    spec = PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.COLLAPSE, "session": Role.ITERATE, "trial": Role.COLLAPSE},
        kind=PlotKind.CUSTOM,
        custom=CustomPlot(function="plot_signal"),
    )
    figure = resolve_custom(spec, call)[0]
    signal = figure.custom.arguments({})["signal"]
    assert len(signal) == len(SUBJECTS)
    one = series_table.frame
    rows = one[(one.subject == "01") & (one.session == figure.figure_key["session"])]
    expected = np.mean(np.stack([np.asarray(c) for c in rows["Signal"]]), axis=0)
    got = signal[signal.subject == "01"]["Signal"].iloc[0]
    np.testing.assert_allclose(got, expected)


# ---------------------------------------------------------------------------
# Refusals, each naming the fix
# ---------------------------------------------------------------------------


def test_an_unwired_input_is_refused(scalar_table):
    def plot_two(steps, other, ax):
        pass

    call = CustomCall(fn=plot_two, inputs={"steps": scalar_table}, variables={"steps": "StepLength"})
    reason = custom_requirement(_spec("plot_two"), call)
    assert reason is not None and "'other'" in reason and "canvas" in reason


def test_a_figure_level_function_cannot_be_faceted(scalar_table, mass_table):
    call = _make(Recorder().figure, scalar_table, mass_table)
    spec = _spec(roles={"session": Role.FACET, "trial": Role.COLLAPSE, "subject": Role.COLLAPSE}, groups=[])
    reason = custom_requirement(spec, call)
    assert reason is not None and "'ax'" in reason and "'session'" in reason


def test_a_value_input_with_several_records_per_call_is_refused(scalar_table, mass_table):
    call = _make(Recorder().panel, scalar_table, mass_table)
    spec = _spec(custom=CustomPlot(function="plot_steps", as_table=False))
    reason = custom_requirement(spec, call)
    assert reason is not None and "as a value" in reason and "as a table" in reason


def test_collapsing_non_numeric_data_is_refused(mass_table):
    frame = pd.DataFrame(
        {
            "subject": [s for s in SUBJECTS for _ in range(2)],
            "trial": ["1", "2"] * len(SUBJECTS),
            "Label": ["a", "b"] * len(SUBJECTS),
        }
    )
    labels = LongTable.from_frame(
        frame, factors=["subject", "trial"], measures=["Label"], name="Label",
        schema_levels=["subject", "trial"],
    )

    def plot_labels(labels, ax):
        pass

    call = CustomCall(fn=plot_labels, inputs={"labels": labels}, variables={"labels": "Label"})
    spec = PlotSpec(
        measures=["Label"],
        roles={"subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.CUSTOM,
        custom=CustomPlot(function="plot_labels"),
    )
    with pytest.raises(RoleError):
        resolve_custom(spec, call)


def test_a_spec_naming_another_function_is_refused(scalar_table, mass_table):
    call = _make(Recorder().panel, scalar_table, mass_table, name="plot_other")
    assert "plot_steps" in custom_requirement(_spec(), call)


# ---------------------------------------------------------------------------
# Drawing
# ---------------------------------------------------------------------------


def test_a_panel_level_function_draws_every_panel_into_the_file(scalar_table, mass_table):
    rec = Recorder()
    call = _make(rec.panel, scalar_table, mass_table)
    spec = _spec(roles={"session": Role.FACET, "trial": Role.COLLAPSE, "subject": Role.GROUP}, groups=["subject"])
    figure = resolve_custom(spec, call)[0]
    out = io.BytesIO()
    write_figure(figure, out, dpi=50, format="png")
    assert out.getvalue()[:8] == b"\x89PNG\r\n\x1a\n"
    assert len(rec.calls) == len(SESSIONS)
    # context.x agrees with the figure's own ticks.
    context = rec.calls[0]["context"]
    assert context.x({"subject": SUBJECTS[0]}) == 0.0
    assert context.color is None or context.color == "subject"


def test_trial_layouts_do_not_call_the_function(scalar_table, mass_table):
    rec = Recorder()
    call = _make(rec.panel, scalar_table, mass_table)
    figure = resolve_custom(_spec(), call)[0]
    layout_report(figure)
    assert rec.calls == []


def test_the_layout_inputs_computed_y_range_is_not_imposed(scalar_table, mass_table):
    call = _make(Recorder().panel, scalar_table, mass_table)
    figure = resolve_custom(_spec(), call)[0]
    assert all(panel.y_limits is None for panel in figure.panels)
    pinned = resolve_custom(_spec(y_axis=YAxis(minimum=0.0, maximum=5.0)), call)[0]
    assert all(panel.y_limits == (0.0, 5.0) for panel in pinned.panels)


def test_a_figure_level_function_is_sized_to_the_spec(scalar_table, mass_table):
    rec = Recorder()
    call = _make(rec.figure, scalar_table, mass_table)
    spec = _spec(style=StyleOptions(width=4.0, height=2.5))
    figure = resolve_custom(spec, call)[0]
    payload = custom_preview(figure)
    assert payload["kind"] == "image" and payload["mime"] == "image/png"
    assert (payload["width_in"], payload["height_in"]) == pytest.approx((4.0, 2.5))
    assert len(rec.calls) == 1


def test_a_preview_can_be_drawn_at_the_pane_size(scalar_table, mass_table):
    call = _make(Recorder().panel, scalar_table, mass_table)
    figure = resolve_custom(_spec(), call)[0]
    payload = custom_preview(figure, width_in=3.0, height_in=2.0)
    assert (payload["width_in"], payload["height_in"]) == pytest.approx((3.0, 2.0))


def test_a_function_that_raises_names_itself_and_the_call(scalar_table, mass_table):
    def plot_steps(steps, mass, filename, ax):
        raise ZeroDivisionError("boom")

    call = CustomCall(
        fn=plot_steps,
        inputs={"steps": scalar_table, "mass": mass_table},
        variables={"steps": "StepLength", "mass": "Mass"},
        constants={"filename": None},
    )
    figure = resolve_custom(_spec(), call)[0]
    with pytest.raises(CustomFunctionError, match="plot_steps raised ZeroDivisionError"):
        write_figure(figure, io.BytesIO(), dpi=50, format="png")


def test_a_figure_level_function_must_return_a_figure(scalar_table, mass_table):
    def plot_steps(steps, mass, filename):
        return 3

    call = CustomCall(
        fn=plot_steps,
        inputs={"steps": scalar_table, "mass": mass_table},
        variables={"steps": "StepLength", "mass": "Mass"},
        constants={"filename": None},
    )
    figure = resolve_custom(_spec(), call)[0]
    with pytest.raises(CustomFunctionError, match="matplotlib or plotly figure"):
        custom_preview(figure)


def test_a_plotly_figure_previews_as_plotly(scalar_table, mass_table):
    go = pytest.importorskip("plotly.graph_objects")

    def plot_steps(steps, mass, filename):
        return go.Figure(go.Scatter(y=steps["StepLength"]))

    call = CustomCall(
        fn=plot_steps,
        inputs={"steps": scalar_table, "mass": mass_table},
        variables={"steps": "StepLength", "mass": "Mass"},
        constants={"filename": None},
    )
    figure = resolve_custom(_spec(), call)[0]
    payload = custom_preview(figure)
    assert payload["kind"] == "plotly"
    assert payload["figure"]["data"][0]["type"] == "scatter"


def test_a_plotly_figure_is_written_through_kaleido(scalar_table, mass_table, tmp_path):
    go = pytest.importorskip("plotly.graph_objects")
    pytest.importorskip("kaleido")

    def plot_steps(steps, mass, filename):
        return go.Figure(go.Scatter(y=steps["StepLength"]))

    call = CustomCall(
        fn=plot_steps,
        inputs={"steps": scalar_table, "mass": mass_table},
        variables={"steps": "StepLength", "mass": "Mass"},
        constants={"filename": None},
    )
    figure = resolve_custom(_spec(style=StyleOptions(width=4.0, height=3.0)), call)[0]
    out = tmp_path / "steps.svg"
    written = write_figure(figure, out, dpi=96)
    assert out.read_text().lstrip().startswith(("<?xml", "<svg"))
    assert (written.width_in, written.height_in) == (4.0, 3.0)
