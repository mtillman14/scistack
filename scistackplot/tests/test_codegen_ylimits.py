"""
The exported figure's y range is the preview's y range.

Two divergences found 2026-09-24, both showing up as a different MINIMUM:

* **Filtered rows.** A scope that spans figures bakes a literal ``ylim`` into
  the export. codegen used to recompute it from the table it was handed, which
  had never been through ``apply_filters`` — so a filtered-out subject's low
  values still set the export's floor. It now reads the preview's own plan
  (``reduce.planned_y_limits``), the one owner.
* **Bar sticky edge.** Within one figure the export autoscales. matplotlib pins
  an autoscaled bar axis to exactly 0, while the preview pads 5 % below zero
  (``ylimits.finish_extents``). The export releases the sticky edge.

Parity tests, per the export-matches-preview rule: each compares the export
against ``resolve``'s limits, never against a hand-typed number.
"""

from __future__ import annotations

import re

import numpy as np
import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, generate_plot_function, resolve
from scistackplot.reduce import planned_y_limits
from scistackplot.spec import Aggregation, ErrorBand, Filter

pytest.importorskip("seaborn")
matplotlib = pytest.importorskip("matplotlib")

_YLIM = re.compile(r"g\.set\(ylim=\(([^,]+), ([^)]+)\)\)")


def _run(source: str, frame: pd.DataFrame, function_name: str):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


@pytest.fixture
def outlier_table() -> LongTable:
    """Subject 03 sits far BELOW the others — the one a filter removes."""
    rng = np.random.default_rng(3)
    rows = [
        {
            "subject": subject,
            "session": session,
            "trial": trial,
            "StepLength": -5.0 if subject == "03" else float(rng.normal(1.2, 0.1)),
        }
        for subject in ["01", "02", "03"]
        for session in ["pre", "post"]
        for trial in ["1", "2", "3", "4"]
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "session", "trial"],
        measures=["StepLength"],
        name="StepLength",
        schema_levels=["subject", "session", "trial"],
    )


def _filtered_fanout_bar() -> PlotSpec:
    """Scope [] with a session fan-out: the limits span figures -> literal."""
    return PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.ITERATE, "trial": Role.COLLAPSE},
        groups=["subject"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.NONE),
        filters=[Filter(column="subject", exclude=["03"])],
    )


def test_baked_ylim_is_the_filtered_previews(outlier_table):
    spec = _filtered_fanout_bar()
    source = generate_plot_function(spec, outlier_table)

    match = _YLIM.search(source)
    assert match, "a scope spanning figures bakes a literal ylim"
    exported = (float(match.group(1)), float(match.group(2)))

    preview = resolve(spec, outlier_table)[0]
    assert preview.y_limits is not None
    assert exported == pytest.approx(preview.y_limits)
    # The filtered-out subject's -5 must not reach the floor.
    assert exported[0] > -1.0


def test_planned_y_limits_is_what_resolve_draws(outlier_table):
    spec = _filtered_fanout_bar()
    scope, limits = planned_y_limits(spec, outlier_table)
    assert scope == []
    for figure in resolve(spec, outlier_table):
        assert figure.y_limits == pytest.approx(limits[()])


def _autoscaled_bar() -> PlotSpec:
    """No fan-out: the export autoscales per figure, no literal."""
    return PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["subject"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.NONE),
    )


def test_autoscaled_bar_pads_below_zero_like_the_preview(scalar_table, scalar_frame):
    spec = _autoscaled_bar()
    source = generate_plot_function(spec, scalar_table)
    assert not _YLIM.search(source), "within one figure the export autoscales"
    assert "use_sticky_edges = False" in source

    preview = resolve(spec, scalar_table)[0]
    figure = _run(source, scalar_frame, "plot_steplength")
    try:
        drawn = figure.axes[0].get_ylim()
    finally:
        matplotlib.pyplot.close(figure)

    assert preview.y_limits is not None
    assert preview.y_limits[0] < 0, "the preview pads below a bar's zero"
    assert drawn == pytest.approx(preview.y_limits, rel=1e-6)


def test_only_linear_autoscaled_bars_release_the_sticky_edge(scalar_table):
    line = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.COLLAPSE, "trial": Role.COLLAPSE},
        groups=["subject"],
        kind=PlotKind.LINE,
    )
    assert "use_sticky_edges" not in generate_plot_function(line, scalar_table)


# ---------------------------------------------------------------------------
# Scope = facet factors only, limits spanning figures: a per-panel literal
# ---------------------------------------------------------------------------


@pytest.fixture
def two_scale_frame() -> pd.DataFrame:
    """Two facet fields a hundredfold apart, over two sessions (the fan-out).

    One global range on both panels would flatten ``small`` to a line on the
    floor — the defect this case fixes.
    """
    rng = np.random.default_rng(5)
    rows = [
        {
            "subject": subject,
            "session": session,
            "trial": trial,
            "field": field,
            "M": float(rng.normal(scale, scale / 10)) * (2.0 if session == "post" else 1.0),
        }
        for subject in ["01", "02", "03"]
        for session in ["pre", "post"]
        for trial in ["1", "2"]
        for field, scale in (("big", 100.0), ("small", 1.0))
    ]
    return pd.DataFrame(rows)


@pytest.fixture
def two_scale_table(two_scale_frame) -> LongTable:
    return LongTable.from_frame(
        two_scale_frame,
        factors=["subject", "session", "trial", "field"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
    )


def _per_field_bar() -> PlotSpec:
    from scistackplot.spec import YAxis

    return PlotSpec(
        measures=["M"],
        roles={
            "subject": Role.GROUP,
            "session": Role.ITERATE,
            "trial": Role.COLLAPSE,
            "field": Role.FACET,
        },
        groups=["subject"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.NONE),
        # Each field its own range, shared across the session figures.
        y_axis=YAxis(scope=["field"]),
    )


def test_facet_scope_bakes_a_per_panel_table(two_scale_table):
    source = generate_plot_function(_per_field_bar(), two_scale_table)
    assert "_ylims = {" in source
    assert not _YLIM.search(source), "no single literal flattening every panel"
    assert "sharey=False" in source, "catplot takes sharey itself, not via facet_kws"
    assert "use_sticky_edges" not in source, "the limits are fixed, not autoscaled"
    assert "NOTE: the y limits separate" not in source


def test_facet_scope_export_matches_every_preview_panel(two_scale_table, two_scale_frame):
    spec = _per_field_bar()
    figures = resolve(spec, two_scale_table)
    by_field = {}
    for figure in figures:
        for panel in figure.panels:
            by_field.setdefault(panel.key["field"], set()).add(panel.y_limits)
    # Shared across figures, distinct across fields — the scope's meaning.
    assert all(len(limits) == 1 for limits in by_field.values())
    expected = {field: limits.pop() for field, limits in by_field.items()}
    assert expected["big"] != expected["small"]

    source = generate_plot_function(spec, two_scale_table)
    for session in ["pre", "post"]:
        frame = two_scale_frame[two_scale_frame["session"] == session]
        figure = _run(source, frame, "plot_m")
        try:
            drawn = {ax.get_ylabel(): ax.get_ylim() for ax in figure.axes if ax.get_ylabel()}
        finally:
            matplotlib.pyplot.close(figure)
        assert set(drawn) == {"big", "small"}
        for field, limits in expected.items():
            assert drawn[field] == pytest.approx(limits), (session, field)


def test_a_partial_iterate_scope_still_warns_in_the_docstring(two_scale_table):
    """The one case that cannot match: the scope names one ITERATE factor
    (session) but not another (trial), and neither is a column the endpoint
    can read its group from."""
    from dataclasses import replace

    from scistackplot.spec import YAxis

    spec = replace(
        _per_field_bar(),
        roles={
            "subject": Role.GROUP,
            "session": Role.ITERATE,
            "trial": Role.ITERATE,
            "field": Role.FACET,
        },
        y_axis=YAxis(scope=["session"]),
    )
    source = generate_plot_function(spec, two_scale_table)
    assert "NOTE: the y limits separate by session" in source
    assert "_ylims = {" not in source


@pytest.mark.parametrize("kind", [PlotKind.BAR, PlotKind.BOX, PlotKind.LINE])
def test_per_panel_autoscale_export_runs(kind, two_scale_table, two_scale_frame):
    """Scope = every iterate factor + the facet: each panel autoscales, so the
    export says ``sharey=False`` — as catplot's own argument, or through
    relplot's ``facet_kws``. The catplot spelling used to be
    ``facet_kws={"sharey": False}`` too, and FacetGrid raised "got multiple
    values for keyword argument 'sharey'" (2026-09-24)."""
    from dataclasses import replace

    from scistackplot.spec import YAxis

    spec = replace(_per_field_bar(), kind=kind, y_axis=YAxis(scope=["session", "field"]))
    source = generate_plot_function(spec, two_scale_table)
    if kind is PlotKind.LINE:
        assert 'facet_kws={"sharey": False}' in source
    else:
        assert "sharey=False" in source and "facet_kws" not in source

    frame = two_scale_frame[two_scale_frame["session"] == "pre"]
    figure = _run(source, frame, "plot_m")
    try:
        drawn = {ax.get_ylabel(): ax.get_ylim() for ax in figure.axes if ax.get_ylabel()}
    finally:
        matplotlib.pyplot.close(figure)
    assert drawn["big"][1] > 10 * drawn["small"][1], "each panel on its own scale"
