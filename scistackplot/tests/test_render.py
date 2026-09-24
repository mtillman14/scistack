"""Renderer tests — both backends draw the same ResolvedPlot without reducing it."""

from __future__ import annotations

import json

import pytest

from scistackplot import (
    PlotKind,
    PlotSpec,
    Role,
    render_matplotlib,
    render_plotly,
    resolve,
)

matplotlib = pytest.importorskip("matplotlib")


@pytest.fixture
def box_spec():
    return PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
    )


@pytest.fixture
def band_spec():
    return PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        color="session",
        kind=PlotKind.BAND,
    )


# --- matplotlib ------------------------------------------------------------


def test_matplotlib_returns_a_figure(scalar_table, box_spec):
    from matplotlib.figure import Figure

    figure = render_matplotlib(resolve(box_spec, scalar_table)[0])
    assert isinstance(figure, Figure)
    matplotlib.pyplot.close(figure)


@pytest.mark.parametrize(
    "kind",
    [
        PlotKind.SCATTER,
        PlotKind.STRIP,
        PlotKind.SPAGHETTI,
        PlotKind.BOX,
        PlotKind.VIOLIN,
        PlotKind.BAR,
    ],
)
def test_every_scalar_kind_renders(scalar_table, kind):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        # subject first: the spaghetti lines, and bars inside session ticks.
        groups=["subject", "session"],
        color="session",
        kind=kind,
    )
    figure = render_matplotlib(resolve(spec, scalar_table)[0])
    assert figure.axes
    matplotlib.pyplot.close(figure)


@pytest.mark.parametrize("kind", [PlotKind.LINE, PlotKind.BAND])
def test_every_series_kind_renders(series_table, kind):
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        color="session",
        kind=kind,
    )
    figure = render_matplotlib(resolve(spec, series_table)[0])
    assert figure.axes
    matplotlib.pyplot.close(figure)


def test_facets_become_subplots(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "subject": Role.GROUP,
            "session": Role.FACET,
            "trial": Role.COLLAPSE,
        },
        kind=PlotKind.BOX,
    )
    figure = render_matplotlib(resolve(spec, scalar_table)[0])
    visible = [ax for ax in figure.axes if ax.get_visible()]
    assert len(visible) == 2
    matplotlib.pyplot.close(figure)


def test_categorical_axis_ticks_are_in_declared_order(wide_subject_table):
    spec = PlotSpec(
        measures=["Mass"], roles={"subject": Role.GROUP}, kind=PlotKind.SCATTER
    )
    figure = render_matplotlib(resolve(spec, wide_subject_table)[0])
    labels = [t.get_text() for t in figure.axes[0].get_xticklabels()]

    assert labels[:3] == ["01", "02", "03"]
    matplotlib.pyplot.close(figure)


def test_heatmap_renders_a_2d_measure():
    import numpy as np
    import pandas as pd

    from scistackplot import LongTable

    frame = pd.DataFrame(
        {"subject": ["01", "02"], "Map": [np.zeros((4, 5)), np.ones((4, 5))]}
    )
    table = LongTable.from_frame(frame, factors=["subject"], measures=["Map"])
    spec = PlotSpec(measures=["Map"], roles={"subject": Role.COLLAPSE}, kind=PlotKind.HEATMAP)

    figure = render_matplotlib(resolve(spec, table)[0])
    assert figure.axes
    matplotlib.pyplot.close(figure)


# --- plotly ----------------------------------------------------------------


def test_plotly_output_is_json_serializable(scalar_table, box_spec):
    payload = render_plotly(resolve(box_spec, scalar_table)[0])
    json.dumps(payload)  # the webview boundary requires this

    assert payload["data"]
    assert "layout" in payload


def test_plotly_makes_one_trace_per_colour_level(scalar_table, box_spec):
    payload = render_plotly(resolve(box_spec, scalar_table)[0])
    assert len(payload["data"]) == 2
    assert {trace["name"] for trace in payload["data"]} == {"pre", "post"}


def test_plotly_band_emits_fill_and_line(series_table, band_spec):
    payload = render_plotly(resolve(band_spec, series_table)[0])
    fills = [t for t in payload["data"] if t.get("fill") == "toself"]
    lines = [t for t in payload["data"] if t.get("mode") == "lines" and "fill" not in t]

    assert len(fills) == 2  # one per session
    assert len(lines) == 2


def test_plotly_facets_get_their_own_axes(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.FACET, "trial": Role.COLLAPSE},
        kind=PlotKind.BOX,
    )
    payload = render_plotly(resolve(spec, scalar_table)[0])
    assert "xaxis" in payload["layout"]
    assert "xaxis2" in payload["layout"]


def test_plotly_legend_entry_appears_once_per_level(series_table):
    """A 24-trial line plot must not produce a 24-entry legend."""
    spec = PlotSpec(
        measures=["Signal"],
        roles={"session": Role.GROUP, "subject": Role.GROUP, "trial": Role.GROUP},
        groups=["trial", "subject", "session"],
        color="session",
        kind=PlotKind.LINE,
    )
    payload = render_plotly(resolve(spec, series_table)[0])
    shown = [t for t in payload["data"] if t.get("showlegend")]
    assert len(shown) == 2


def test_a_colour_level_keeps_its_colour_in_every_panel(scalar_frame):
    """Regression: the palette was indexed by ENUMERATION, not by level.

    `color_groups` omits a level with no rows in the panel it is splitting, so a
    facet grid where one panel lacks a level handed the *next* level that
    level's colour — one series drawn in two colours, with the legend agreeing
    with only some of the panels. Wrong in a way that looks like data.
    """
    from scistackplot import LongTable

    # Subject 01 never ran the "post" session, so its panel has one colour
    # level where the others have two.
    frame = scalar_frame[
        ~((scalar_frame["subject"] == "01") & (scalar_frame["session"] == "post"))
    ]
    table = LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial"],
        measures=["StepLength"],
        level_order={"session": ["pre", "post"]},
        schema_levels=["subject", "session", "trial"],
    )
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"trial": Role.GROUP, "session": Role.GROUP, "subject": Role.FACET},
        groups=["session", "trial"],
        color="session",
        kind=PlotKind.STRIP,  # one row per (trial, session): nothing to box
    )

    payload = render_plotly(resolve(spec, table)[0])
    colours: dict[str, set] = {}
    for trace in payload["data"]:
        colours.setdefault(trace["name"], set()).add(trace["marker"]["color"])

    assert set(colours) == {"pre", "post"}
    for level, used in colours.items():
        assert len(used) == 1, f"{level} was drawn in {len(used)} different colours"
    assert colours["pre"] != colours["post"]


def test_both_backends_draw_the_same_x_group_spans(scalar_frame):
    """Brackets come from one computation, so the two views cannot disagree."""
    from scistackplot import LongTable

    frame = scalar_frame.assign(
        group=scalar_frame["subject"].map({"01": "stim", "02": "stim", "03": "sham"})
    )
    table = LongTable.from_frame(
        frame,
        factors=["group", "session", "subject", "trial"],
        measures=["StepLength"],
        level_order={"group": ["stim", "sham"], "session": ["pre", "post"]},
    )
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "group": Role.GROUP,
            "session": Role.GROUP,
            "subject": Role.COLLAPSE,
            "trial": Role.COLLAPSE,
        },
        groups=["session", "group"],
        kind=PlotKind.BOX,
    )
    figure = resolve(spec, table)[0]

    payload = render_plotly(figure)
    labels = {a["text"] for a in payload["layout"]["annotations"]}
    assert {"stim", "sham"} <= labels
    # One bracket per group span.
    assert len(payload["layout"].get("shapes", [])) == len(figure.x_plan.groups)
    # Ticks show the inner layer, never the composed key.
    assert payload["layout"]["xaxis"]["ticktext"].count("pre") == 2

    drawn = render_matplotlib(figure)
    texts = {t.get_text() for t in drawn.axes[0].texts}
    assert {"stim", "sham"} <= texts
    matplotlib.pyplot.close(drawn)


# --- legends ---------------------------------------------------------------


@pytest.fixture
def one_colour_level_spec():
    """
    A colour factor the data has TWO levels of, filtered down to one.

    The declared level order still says "pre, post" here — the legend rule has
    to look at what the figure draws, not at what the table could have drawn.
    """
    from scistackplot import Filter

    return PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BOX,
        filters=[Filter(column="session", include=["pre"])],
    )


def test_no_legend_for_a_single_colour_level(scalar_table, one_colour_level_spec):
    figure = render_matplotlib(resolve(one_colour_level_spec, scalar_table)[0])
    assert figure.legends == []
    matplotlib.pyplot.close(figure)


def test_plotly_hides_the_legend_for_a_single_colour_level(
    scalar_table, one_colour_level_spec
):
    payload = render_plotly(resolve(one_colour_level_spec, scalar_table)[0])
    assert payload["layout"]["showlegend"] is False
    assert not [trace for trace in payload["data"] if trace.get("showlegend")]


def test_legend_is_drawn_for_two_colour_levels(scalar_table, box_spec):
    figure = render_matplotlib(resolve(box_spec, scalar_table)[0])
    assert len(figure.legends) == 1
    assert {text.get_text() for text in figure.legends[0].get_texts()} == {"pre", "post"}
    matplotlib.pyplot.close(figure)


def test_matplotlib_legend_sits_right_of_every_panel(scalar_table):
    """The export must place the legend where the interactive view does."""
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "subject": Role.GROUP,
            "session": Role.GROUP,
            "trial": Role.FACET,
        },
        groups=["session", "subject"],
        color="session",
        kind=PlotKind.BAR,
    )
    figure = render_matplotlib(resolve(spec, scalar_table)[0])
    figure.canvas.draw()
    renderer = figure.canvas.get_renderer()

    legend_left = figure.legends[0].get_window_extent(renderer).x0
    panel_right = max(
        ax.get_window_extent(renderer).x1 for ax in figure.axes if ax.get_visible()
    )
    assert panel_right <= legend_left
    matplotlib.pyplot.close(figure)


def test_matplotlib_legend_covers_levels_missing_from_the_first_panel(scalar_table):
    """A level only present in a later facet still gets a legend entry."""
    import pandas as pd

    from scistackplot import LongTable

    frame = pd.DataFrame(
        {
            "subject": ["01", "01", "02", "02"],
            "session": ["pre", "pre", "post", "post"],
            "trial": ["1", "2", "1", "2"],
            "StepLength": [1.0, 1.1, 1.4, 1.5],
        }
    )
    table = LongTable.from_frame(
        frame, factors=["subject", "session", "trial"], measures=["StepLength"]
    )
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"trial": Role.GROUP, "session": Role.GROUP, "subject": Role.FACET},
        groups=["session", "trial"],
        color="session",
        kind=PlotKind.BAR,
    )
    figure = render_matplotlib(resolve(spec, table)[0])

    assert {text.get_text() for text in figure.legends[0].get_texts()} == {"pre", "post"}
    matplotlib.pyplot.close(figure)


def test_plotly_reserves_right_margin_for_the_legend(scalar_table, box_spec):
    from scistackplot.render.plotly_ import BARE_RIGHT_MARGIN

    payload = render_plotly(resolve(box_spec, scalar_table)[0])
    legend = payload["layout"]["legend"]

    assert legend["x"] > 1.0 and legend["xanchor"] == "left"
    assert payload["layout"]["margin"]["r"] > BARE_RIGHT_MARGIN


# --- facet grid layout -----------------------------------------------------


@pytest.fixture
def wrapped_grid(struct_table):
    """A facet grid whose last row is partial — 3 fields wrapped 2 wide."""
    from scistackplot import FacetOptions

    return PlotSpec(
        measures=["RawEMG"],
        roles={"ColName": Role.FACET, "subject": Role.GROUP, "trial": Role.COLLAPSE},
        color="subject",
        kind=PlotKind.BAND,
        facet=FacetOptions(n_cols=2),
    )


def test_panel_titles_show_the_value_not_the_key(struct_table, wrapped_grid):
    resolved = resolve(wrapped_grid, struct_table)[0]
    assert {p.title for p in resolved.panels} == {"RHAM", "RTA", "LMG"}
    assert all("ColName=" not in p.title for p in resolved.panels)


def test_plotly_names_a_facet_on_its_y_axis_not_above_it(struct_table, wrapped_grid):
    """The facet name IS the y-axis title, and no caption sits above the panel.

    A caption spends a strip of every grid row on text; the axis title is room
    the panel already spends. Both halves are asserted together because half of
    this change alone leaves the panels either unnamed or named twice.
    """
    resolved = resolve(wrapped_grid, struct_table)[0]
    payload = render_plotly(resolved)
    layout = payload["layout"]

    titles = {p.title for p in resolved.panels}
    # slots: 1=(0,0) 2=(0,1) 3=(1,0) — including slot 2, which is NOT leftmost.
    assert {layout[f"yaxis{s}"]["title"]["text"] for s in ("", "2", "3")} == titles
    assert layout["yaxis2"]["showticklabels"] is False

    drawn = {a.get("text") for a in layout["annotations"]}
    assert not (drawn & titles)


def test_matplotlib_names_a_facet_on_its_y_axis_not_above_it(
    struct_table, wrapped_grid
):
    resolved = resolve(wrapped_grid, struct_table)[0]
    figure = render_matplotlib(resolved)

    visible = [ax for ax in figure.axes if ax.get_visible()]
    assert {ax.get_ylabel() for ax in visible} == {p.title for p in resolved.panels}
    assert not any(ax.get_title() for ax in visible)
    matplotlib.pyplot.close(figure)


def test_an_unfaceted_y_axis_still_names_the_measure(scalar_table, box_spec):
    """The facet rule must not cost a lone plot its measure label."""
    resolved = resolve(box_spec, scalar_table)[0]
    layout = render_plotly(resolved)["layout"]
    assert layout["yaxis"]["title"]["text"] == resolved.labels.y

    figure = render_matplotlib(resolved)
    assert figure.axes[0].get_ylabel() == resolved.labels.y
    matplotlib.pyplot.close(figure)


def test_plotly_ships_its_grid_shape(struct_table, wrapped_grid):
    payload = render_plotly(resolve(wrapped_grid, struct_table)[0])
    # 3 panels 2 wide -> 2 rows, 2 columns. The panel reads `rows`/`cols` back
    # as the EFFECTIVE grid, which is how setting one dimension fills the other.
    meta = payload["layout"]["meta"]
    assert {k: meta[k] for k in ("rows", "cols", "panels", "layout_notes")} == {
        "rows": 2,
        "cols": 2,
        "panels": 3,
        "layout_notes": [],
    }
    # The y-limit control's checkbox list travels here too, AS RESOLVED.
    assert meta["panel_factors"] == ["ColName"]
    assert meta["y_scope"] == []


def test_plotly_ticklabels_only_where_nothing_is_below(struct_table, wrapped_grid):
    """
    The partial-last-row case: with 3 panels in a 2x2 grid, the panel at
    (row 0, col 1) has an EMPTY cell below it, so it keeps its tick labels
    even though it is not on the bottom row.
    """
    payload = render_plotly(resolve(wrapped_grid, struct_table)[0])
    layout = payload["layout"]

    # slots: 1=(0,0) 2=(0,1) 3=(1,0)
    assert layout["xaxis"]["showticklabels"] is False   # (0,0) has (1,0) below
    assert layout["xaxis2"]["showticklabels"] is True   # (0,1) has nothing below
    assert layout["xaxis3"]["showticklabels"] is True   # bottom row


def test_plotly_links_shared_axes(struct_table, wrapped_grid):
    payload = render_plotly(resolve(wrapped_grid, struct_table)[0])
    assert payload["layout"]["xaxis2"]["matches"] == "x"
    assert payload["layout"]["yaxis2"]["matches"] == "y"


def test_plotly_rows_clear_each_other_without_paying_for_captions(
    struct_table, wrapped_grid
):
    """Each row's cell must clear the one below with room for tick labels.

    The upper bound is the point: the gap used to be 0.14 because a panel
    CAPTION sat in it as well. Facet names moved onto the y axis, so a gap that
    large again would mean the grid is still paying for captions it no longer
    draws — vertical room the panels should have got back.
    """
    layout = render_plotly(resolve(wrapped_grid, struct_table)[0])["layout"]
    top_row = layout["yaxis"]["domain"]
    bottom_row = layout["yaxis3"]["domain"]

    gap = top_row[0] - bottom_row[1]
    assert bottom_row[1] < top_row[0]
    assert 0.05 <= gap < 0.12


def test_matplotlib_keeps_ticklabels_above_an_empty_cell(struct_table, wrapped_grid):
    """
    3 panels in a 2x2 grid: the one at (0, 1) has an EMPTY cell below it, so it
    is the bottom of its own column and must keep its tick labels — sharex
    would otherwise strip them from the whole top row. The x title is drawn
    once under the figure (two columns would each repeat it).
    """
    resolved = resolve(wrapped_grid, struct_table)[0]
    figure = render_matplotlib(resolved)

    labelled = [
        ax
        for ax in figure.axes
        if ax.get_visible() and any(t.get_visible() for t in ax.get_xticklabels())
    ]
    # (0, 1) and (1, 0) are both bottom-of-column; (0, 0) is not.
    assert len(labelled) == 2
    assert all(ax.get_xlabel() == "" for ax in figure.axes)
    assert figure.get_supxlabel() == resolved.labels.x
    matplotlib.pyplot.close(figure)


# --- rule-defined facet layout ---------------------------------------------


def _layout_spec(rows=(), cols=(), n_rows=None, n_cols=None):
    from scistackplot import FacetOptions
    from scistackplot.spec import MatchOp, Matcher

    return PlotSpec(
        measures=["RawEMG"],
        roles={"ColName": Role.FACET, "subject": Role.GROUP, "trial": Role.COLLAPSE},
        color="subject",
        kind=PlotKind.BAND,
        facet=FacetOptions(
            n_rows=n_rows,
            n_cols=n_cols,
            rows=[Matcher(op=MatchOp(op), value=v) for op, v in rows],
            cols=[Matcher(op=MatchOp(op), value=v) for op, v in cols],
        ),
    )


def test_row_rules_place_panels_by_name(struct_table):
    """Left/right muscles as two rows — the EMG arrangement."""
    spec = _layout_spec(rows=[("starts_with", "R"), ("starts_with", "L")])
    resolved = resolve(spec, struct_table)[0]

    by_title = {p.title: p for p in resolved.panels}
    assert by_title["RHAM"].grid_row == 0
    assert by_title["RTA"].grid_row == 0
    assert by_title["LMG"].grid_row == 1
    assert resolved.grid_rows == 2
    # Two R muscles flow across that row.
    assert {by_title["RHAM"].grid_col, by_title["RTA"].grid_col} == {0, 1}


def test_column_rules_place_panels_by_name(struct_table):
    spec = _layout_spec(cols=[("ends_with", "HAM"), ("ends_with", "TA")])
    resolved = resolve(spec, struct_table)[0]

    by_title = {p.title: p for p in resolved.panels}
    assert by_title["RHAM"].grid_col == 0
    assert by_title["RTA"].grid_col == 1
    # LMG matches neither: it lands in the trailing "other" column, not nowhere.
    assert by_title["LMG"].grid_col == 2
    assert resolved.col_labels[-1] == "other"


def test_rules_on_both_axes_make_a_true_grid(struct_table):
    spec = _layout_spec(
        rows=[("starts_with", "R"), ("starts_with", "L")],
        cols=[("ends_with", "HAM"), ("ends_with", "TA"), ("ends_with", "MG")],
    )
    resolved = resolve(spec, struct_table)[0]
    by_title = {p.title: p for p in resolved.panels}

    assert (by_title["RHAM"].grid_row, by_title["RHAM"].grid_col) == (0, 0)
    assert (by_title["RTA"].grid_row, by_title["RTA"].grid_col) == (0, 1)
    assert (by_title["LMG"].grid_row, by_title["LMG"].grid_col) == (1, 2)
    assert (resolved.grid_rows, resolved.grid_cols) == (2, 3)


def test_regex_rules_are_supported(struct_table):
    spec = _layout_spec(rows=[("regex", "^R"), ("regex", "^L")])
    resolved = resolve(spec, struct_table)[0]
    by_title = {p.title: p for p in resolved.panels}
    assert by_title["LMG"].grid_row == 1


def test_an_invalid_regex_does_not_break_the_figure(struct_table):
    """The user is still typing; a half-written pattern must not raise."""
    spec = _layout_spec(rows=[("regex", "[unclosed")])
    resolved = resolve(spec, struct_table)[0]
    assert len(resolved.panels) == 3


def test_rule_layout_survives_a_spec_round_trip(struct_table):
    spec = _layout_spec(
        rows=[("starts_with", "R")], cols=[("ends_with", "MG")], n_rows=2, n_cols=3
    )
    restored = PlotSpec.from_json(spec.to_json())
    assert restored.facet.rows[0].value == "R"
    assert restored.facet.cols[0].op.value == "ends_with"
    assert (restored.facet.n_rows, restored.facet.n_cols) == (2, 3)


# --- grid size: name one dimension, the other follows ----------------------


def test_naming_columns_computes_the_rows(bilateral_table):
    """4 panels, 2 columns asked for -> 2 rows, nobody had to say so."""
    resolved = resolve(_layout_spec(n_cols=2), bilateral_table)[0]
    assert (resolved.grid_rows, resolved.grid_cols) == (2, 2)


def test_naming_rows_computes_the_columns(bilateral_table):
    resolved = resolve(_layout_spec(n_rows=1), bilateral_table)[0]
    assert (resolved.grid_rows, resolved.grid_cols) == (1, 4)


def test_an_odd_panel_count_rounds_up(struct_table):
    """3 panels in 2 columns needs 2 rows, not 1.5."""
    resolved = resolve(_layout_spec(n_cols=2), struct_table)[0]
    assert (resolved.grid_rows, resolved.grid_cols) == (2, 2)


def test_grid_shape_for_is_the_one_place_the_arithmetic_lives():
    from scistackplot import grid_shape_for

    assert grid_shape_for(4, None, 2) == (2, 2)
    assert grid_shape_for(4, 1, None) == (1, 4)
    assert grid_shape_for(13, None, 4) == (4, 4)
    assert grid_shape_for(3) == (1, 3)          # a few panels stay one row
    assert grid_shape_for(13) == (4, 4)         # a wide struct gets a grid
    assert grid_shape_for(4, 2, 2) == (2, 2)    # both pinned: honoured as given


# --- placement never overlaps ----------------------------------------------


def _cells(resolved):
    return [(p.grid_row, p.grid_col) for p in resolved.panels]


@pytest.mark.parametrize(
    "rows,cols,n_rows,n_cols",
    [
        ((), (), None, None),
        ((), (), 3, 2),
        ((("starts_with", "R"), ("starts_with", "L")), (), None, None),
        ((), (("ends_with", "HAM"), ("ends_with", "TA")), None, None),
        # Both rules match the same panel name — the collision case.
        ((("contains", "A"),), (("contains", "A"),), None, None),
        # A grid deliberately too small for the panels.
        ((), (), 1, 1),
    ],
)
def test_no_two_panels_ever_share_a_cell(struct_table, rows, cols, n_rows, n_cols):
    """
    The invariant behind the whole rewrite. Two panels in one cell means two
    plotly axis pairs with an identical domain — traces drawn on top of each
    other, which reads as bad data rather than as a layout bug.
    """
    resolved = resolve(
        _layout_spec(rows=rows, cols=cols, n_rows=n_rows, n_cols=n_cols), struct_table
    )[0]
    cells = _cells(resolved)
    assert len(set(cells)) == len(cells)
    assert len(cells) == 3  # nothing dropped either


def test_a_full_grid_grows_rather_than_overlapping(struct_table):
    resolved = resolve(_layout_spec(n_rows=1, n_cols=1), struct_table)[0]
    cells = _cells(resolved)
    assert len(set(cells)) == 3
    assert resolved.grid_rows * resolved.grid_cols >= 3
    assert any("cell of its own" in note for note in resolved.layout_notes)


def test_colliding_rules_report_the_move(struct_table):
    """A panel that could not have its ruled cell must say so, not move quietly."""
    resolved = resolve(
        _layout_spec(rows=[("contains", "A")], cols=[("contains", "A")]), struct_table
    )[0]
    assert any("both match row" in note for note in resolved.layout_notes)
    assert resolved.layout_notes == resolved.to_dict()["grid"]["layout_notes"]


# --- the EMG arrangement, end to end ---------------------------------------


def test_columns_by_side_then_a_row_by_group(bilateral_table):
    """
    The worked example: columns are L/R, row 1 is the HAM pair, and the rest
    fall into row 2 in order — with only ONE row rule written.
    """
    spec = _layout_spec(
        rows=[("contains", "HAM"), ("contains", "")],   # slot 2 left blank
        cols=[("starts_with", "L"), ("starts_with", "R")],
    )
    resolved = resolve(spec, bilateral_table)[0]
    placed = {p.title: (p.grid_row, p.grid_col) for p in resolved.panels}

    assert placed == {
        "LHAM": (0, 0),
        "RHAM": (0, 1),
        "LQUAD": (1, 0),
        "RQUAD": (1, 1),
    }
    assert (resolved.grid_rows, resolved.grid_cols) == (2, 2)
    assert resolved.layout_notes == []


def test_shrinking_the_grid_ignores_leftover_slots(bilateral_table):
    """
    Rules written into rows that no longer exist must not grow the grid back.
    They stay in the spec — widening it again restores them — but a pinned
    N rows is what the user just asked for.
    """
    spec = _layout_spec(
        rows=[("contains", "HAM"), ("contains", "QUAD")], cols=[], n_rows=1
    )
    resolved = resolve(spec, bilateral_table)[0]
    assert resolved.grid_rows == 1
    assert len(set(_cells(resolved))) == 4


def test_a_blank_slot_claims_nothing(bilateral_table):
    """
    A blank rule must not swallow the grid. 'contains ""' is true of every
    string, so an unset slot used to match the first panel it saw — with fixed
    slots, most slots are blank most of the time.
    """
    from scistackplot.spec import MatchOp, Matcher

    assert Matcher(op=MatchOp.CONTAINS, value="").matches("LHAM") is False
    assert Matcher(op=MatchOp.NOT_CONTAINS, value="").matches("LHAM") is False

    spec = _layout_spec(cols=[("contains", ""), ("starts_with", "R")])
    resolved = resolve(spec, bilateral_table)[0]
    placed = {p.title: (p.grid_row, p.grid_col) for p in resolved.panels}
    # Column 1 is unset, so the R muscles still take column 2 and the L muscles
    # fill what is left rather than everything piling into column 1.
    assert placed["RHAM"][1] == 1
    assert placed["RQUAD"][1] == 1


# --- a grid of any height stays a grid --------------------------------------


@pytest.mark.parametrize("n_rows", list(range(1, 21)))
def test_plotly_cells_never_invert_or_overlap(n_rows):
    """
    The tall-grid bug: the gaps are a fraction of the FIGURE, so at a fixed
    0.14 a 9-row grid spent more than its whole height on gaps, cell height went
    negative, and every y domain ran backwards — panels inverted and overlapped.
    """
    from scistackplot.render.plotly_ import _cell

    domains = []
    for row in range(n_rows):
        _, y0, _, height = _cell(row, 0, n_rows, 1)
        assert height > 0, f"{n_rows} rows produced a cell of height {height}"
        domains.append((y0, y0 + height))

    domains.sort()
    assert all(0.0 <= low < high <= 1.0 for low, high in domains)
    for (_, upper), (lower, _) in zip(domains, domains[1:]):
        assert lower >= upper, f"{n_rows} rows: cells overlap at {upper}/{lower}"


@pytest.mark.parametrize("n_cols", list(range(1, 21)))
def test_plotly_columns_never_invert_or_overlap(n_cols):
    from scistackplot.render.plotly_ import _cell

    domains = []
    for col in range(n_cols):
        x0, _, width, _ = _cell(0, col, 1, n_cols)
        assert width > 0
        domains.append((x0, x0 + width))

    domains.sort()
    assert all(0.0 <= low < high <= 1.0 for low, high in domains)
    for (_, right), (left, _) in zip(domains, domains[1:]):
        assert left >= right


def test_a_tall_grid_still_renders_every_panel(bilateral_table):
    """4 panels stacked in one column: four separate, upright cells."""
    resolved = resolve(_layout_spec(n_cols=1), bilateral_table)[0]
    layout = render_plotly(resolved)["layout"]

    assert (resolved.grid_rows, resolved.grid_cols) == (4, 1)
    domains = sorted(
        layout[f"yaxis{slot or ''}"]["domain"] for slot in ("", 2, 3, 4)
    )
    for (_, upper), (lower, _) in zip(domains, domains[1:]):
        assert lower >= upper


# --- content orientation is stated, never inferred --------------------------


def test_plotly_distributions_are_drawn_vertically(scalar_table):
    """
    plotly picks box/violin/bar orientation from which of x/y it recognises, so
    the same figure could draw sideways on one dataset and upright on another.
    """
    for kind in (PlotKind.BOX, PlotKind.VIOLIN, PlotKind.BAR):
        spec = PlotSpec(
            measures=["StepLength"],
            roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
            groups=["session", "subject"],
            color="session",
            kind=kind,
        )
        payload = render_plotly(resolve(spec, scalar_table)[0])
        drawn = [t for t in payload["data"] if t["type"] in ("box", "violin", "bar")]
        assert drawn, kind
        assert all(t["orientation"] == "v" for t in drawn), kind


def test_plotly_tick_labels_stay_upright(struct_table, wrapped_grid):
    """Plotly rotates category labels once a facet cell is narrow; pin them."""
    layout = render_plotly(resolve(wrapped_grid, struct_table)[0])["layout"]
    x_axes = [value for key, value in layout.items() if key.startswith("xaxis")]
    assert x_axes
    assert all(axis["tickangle"] == 0 for axis in x_axes)


def test_matplotlib_labels_that_fit_stay_upright(scalar_table, box_spec):
    """Rotation is the fitting ladder's answer to crowding, never a default."""
    figure = render_matplotlib(resolve(box_spec, scalar_table)[0])
    for ax in figure.axes:
        assert all(label.get_rotation() == 0 for label in ax.get_xticklabels())
    matplotlib.pyplot.close(figure)


# --- one rule for tick labels and axis titles ------------------------------


def test_x_ticklabels_and_title_follow_the_same_rule(struct_table, wrapped_grid):
    """Item 1: they used to drift — the title obeyed the rule, ticks did not."""
    layout = render_plotly(resolve(wrapped_grid, struct_table)[0])["layout"]
    for slot in ("", "2", "3"):
        axis = layout[f"xaxis{slot}"]
        assert axis["showticklabels"] is bool(axis["title"]["text"])


def test_matplotlib_ticklabels_and_title_follow_the_same_rule(
    struct_table, wrapped_grid
):
    """Tick labels follow `shows_x_labels` per panel; with several columns the
    title is ONE figure-level label rather than a copy per panel (which ran
    into its neighbour, spec/images/graph2.png)."""
    from scistackplot.render.base import shows_x_labels

    resolved = resolve(wrapped_grid, struct_table)[0]
    figure = render_matplotlib(resolved)
    for panel in resolved.panels:
        ax = next(
            a
            for a in figure.axes
            if a.get_visible()
            and a.get_subplotspec().rowspan.start == panel.grid_row
            and a.get_subplotspec().colspan.start == panel.grid_col
        )
        ticks_shown = any(t.get_visible() for t in ax.get_xticklabels())
        assert ticks_shown is shows_x_labels(resolved, panel.grid_row, panel.grid_col)
        assert ax.get_xlabel() == ""
    assert figure.get_supxlabel() == resolved.labels.x
    matplotlib.pyplot.close(figure)
