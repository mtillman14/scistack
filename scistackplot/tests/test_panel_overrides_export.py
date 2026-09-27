"""
Per-panel overrides, Stage 4 (2026-09-27): the exported seaborn code draws
what the preview draws — per-panel limits, titles, hide, and the
"first column" toggle. Parity tests: every expectation is read off
``resolve``, never typed by hand (export-matches-preview rule).
"""

from __future__ import annotations

import logging
from dataclasses import replace as _replace

import pytest

from scistackplot import PlotSpec, StyleOptions, YAxis, generate_plot_function, resolve
from scistackplot.panels import Y_TITLES_FIRST_COLUMN, PanelOverride

pytest.importorskip("seaborn")
pytest.importorskip("matplotlib")

from test_codegen_ylimits import (  # noqa: E402,F401
    _per_field_bar,
    _run,
    two_scale_frame,
    two_scale_table,
)

LAYER = "scistackplot"


def _field_bar(*overrides: PanelOverride, **style) -> PlotSpec:
    """A figure per session, a panel per field (big, small); scope [] so the
    range spans figures and is baked in as one literal."""
    spec = _replace(_per_field_bar(), y_axis=YAxis(), panel_overrides=list(overrides))
    return _replace(spec, style=StyleOptions(**style)) if style else spec


def _preview_panels(spec, table) -> dict[str, list[dict]]:
    """{session: [panel meta, ...]} as the preview decided them."""
    return {
        figure.figure_key["session"]: figure.to_dict()["panels"]
        for figure in resolve(spec, table)
    }


def _export_axes(spec, table, frame, session):
    import matplotlib.pyplot as plt

    source = generate_plot_function(spec, table)
    figure = _run(source, frame[frame["session"] == session], "plot_m")
    try:
        return [
            {
                "col": ax.get_subplotspec().colspan.start,
                "y_title": ax.get_ylabel(),
                "y_limits": ax.get_ylim(),
            }
            for ax in figure.axes
            if ax.get_subplotspec() is not None
        ]
    finally:
        plt.close(figure)


def test_export_panel_limits_match_the_preview(two_scale_table, two_scale_frame):
    small = PanelOverride(match={"field": "small"}, y_minimum=0.0, y_maximum=3.0)
    spec = _field_bar(small)
    source = generate_plot_function(spec, two_scale_table)
    assert "_panel_ylims = {('small',): (0.0, 3.0)}" in source
    assert "sharey=False" in source

    for session, panels in _preview_panels(spec, two_scale_table).items():
        expected = {p["grid_col"]: tuple(p["y_limits"]) for p in panels}
        drawn = {a["col"]: a["y_limits"] for a in _export_axes(
            spec, two_scale_table, two_scale_frame, session
        )}
        assert set(drawn) == set(expected)
        for col, limits in expected.items():
            assert drawn[col] == pytest.approx(limits), (session, col)


def test_export_one_panel_end_keeps_the_other_like_the_preview(
    two_scale_table, two_scale_frame
):
    spec = _field_bar(PanelOverride(match={"field": "small"}, y_maximum=50.0))
    for session, panels in _preview_panels(spec, two_scale_table).items():
        expected = {p["grid_col"]: tuple(p["y_limits"]) for p in panels}
        drawn = {a["col"]: a["y_limits"] for a in _export_axes(
            spec, two_scale_table, two_scale_frame, session
        )}
        for col, limits in expected.items():
            assert drawn[col] == pytest.approx(limits), (session, col)


def test_export_titles_and_toggle_match_the_preview(two_scale_table, two_scale_frame):
    cases = [
        _field_bar(PanelOverride(match={"field": "small"}, y_label="Small (au)")),
        _field_bar(PanelOverride(match={"field": "big"}, y_label_hidden=True)),
        _field_bar(y_titles=Y_TITLES_FIRST_COLUMN),
        _field_bar(
            PanelOverride(match={"field": "small"}, y_label="Small", y_label_hidden=False),
            y_titles=Y_TITLES_FIRST_COLUMN,
        ),
    ]
    for spec in cases:
        for session, panels in _preview_panels(spec, two_scale_table).items():
            expected = {p["grid_col"]: p["y_title"] for p in panels}
            drawn = {a["col"]: a["y_title"] for a in _export_axes(
                spec, two_scale_table, two_scale_frame, session
            )}
            assert drawn == expected, (spec.panel_overrides, spec.style.y_titles, session)


def test_an_override_for_a_panel_not_in_the_data_changes_nothing_in_the_export(
    two_scale_table,
):
    baseline = generate_plot_function(_field_bar(), two_scale_table)
    gone = generate_plot_function(
        _field_bar(PanelOverride(match={"field": "huge"}, y_maximum=1.0)), two_scale_table
    )
    assert "_panel_ylims" not in gone
    # The axis stays shared exactly as without the override.
    assert ("sharey=False" in gone) == ("sharey=False" in baseline)


def test_no_overrides_leave_the_export_unchanged(two_scale_table):
    source = generate_plot_function(_field_bar(), two_scale_table)
    assert "_panel_ylims" not in source
    assert "_ytitles" not in source


def test_export_logs_what_it_baked_in(two_scale_table, caplog):
    small = PanelOverride(match={"field": "small"}, y_maximum=3.0, y_label="S")
    with caplog.at_level(logging.INFO, logger=LAYER):
        generate_plot_function(_field_bar(small), two_scale_table)
    assert "export panel overrides: 1 limit(s), 1 title(s) baked in" in caplog.text
