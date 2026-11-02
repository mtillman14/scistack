"""
Combined rows ("combine rows" / ``row_list`` cells) split into a ``Row``
factor — scistackplot's half (``rows.py``), plus the two fixes found with it
(2026-10-30, ``.claude/plan-row-list-plots.md``):

* a text array is not a series (``shape.classify_value``), and a text cell
  reaching the cell collapse is a warned gap, never a crash;
* a line series of one point draws a marker (``render.base.is_lone_point``),
  and the preview takes the export's x range with its ticks.

The parity tests run the generated endpoint on the frame scidb would hand
it (a Python list for several rows, the plain value for one,
``sciduckdb.unwrap_row_list``) and compare the lines with the preview's.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from scistackplot import (  # noqa: E402
    ROW_FACTOR,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    Shape,
    classify_column,
    classify_value,
    element_values,
    explode_row_lists,
    generate_plot_function,
    render_matplotlib,
    render_plotly,
    resolve,
    row_factor,
    row_origin,
)
from scistackplot.reduce.cell import apply_cell_collapse  # noqa: E402
from scistackplot.render.base import (  # noqa: E402
    LONE_POINT_MARKER,
    is_lone_point,
    lone_point_marker_code,
)
from scistackplot.model.roles import default_assignment  # noqa: E402
from scistackplot.reduce.series_stats import cell_array  # noqa: E402


# --- the split ------------------------------------------------------------------


def _melted() -> pd.DataFrame:
    """A struct variable after the melt: FIN is a row list (subject A had
    two source rows), AGE is an ordinary scalar field."""
    return pd.DataFrame(
        {
            "subject": ["A", "B", "A", "B"],
            "ColName": ["FIN", "FIN", "AGE", "AGE"],
            "M": pd.Series(
                [np.array([10.0, 20.0]), np.array([30.0]), 50.0, 60.0], dtype=object
            ),
        }
    )


def test_each_element_becomes_a_row_numbered_in_source_order():
    split = explode_row_lists(_melted(), "M", ["FIN"], field_factor="ColName")
    frame = split.frame
    assert split.row_factor == ROW_FACTOR
    fin = frame[frame["ColName"] == "FIN"]
    assert list(zip(fin["subject"], fin[ROW_FACTOR], fin["M"])) == [
        ("A", "1", 10.0),
        ("A", "2", 20.0),
        ("B", "1", 30.0),
    ]
    assert (split.rows_in, split.rows_out, split.max_rows, split.cells) == (4, 5, 2, 2)


def test_other_fields_keep_one_row_and_the_measure_is_numeric():
    frame = explode_row_lists(_melted(), "M", ["FIN"], field_factor="ColName").frame
    age = frame[frame["ColName"] == "AGE"]
    assert list(age["M"]) == [50.0, 60.0]
    assert list(age[ROW_FACTOR]) == ["1", "1"]
    assert frame["M"].dtype == np.float64
    assert classify_column(frame["M"]) is Shape.SCALAR


def test_no_cell_with_two_rows_means_no_row_factor_but_unwrapped_cells():
    frame = pd.DataFrame(
        {"subject": ["A", "B"], "M": pd.Series([np.array([1.0]), np.array([2.0])], dtype=object)}
    )
    split = explode_row_lists(frame, "M", ["M"])
    assert split.row_factor is None  # a one-level factor is a constant
    assert ROW_FACTOR not in split.frame.columns
    assert list(split.frame["M"]) == [1.0, 2.0]
    assert split.frame["M"].dtype == np.float64


def test_an_empty_cell_stays_one_missing_row():
    frame = pd.DataFrame(
        {"subject": ["A", "B"], "M": pd.Series([np.array([1.0, 2.0]), np.array([])], dtype=object)}
    )
    out = explode_row_lists(frame, "M", ["M"]).frame
    assert len(out) == 3  # never dropped silently
    assert out["M"].isna().sum() == 1


def test_the_row_name_never_shadows_a_column():
    frame = _melted().assign(Row="x")
    split = explode_row_lists(frame, "M", ["FIN"], field_factor="ColName")
    assert split.row_factor == "Row_"


def test_the_input_frame_is_untouched():
    frame = _melted()
    explode_row_lists(frame, "M", ["FIN"], field_factor="ColName")
    assert len(frame) == 4 and ROW_FACTOR not in frame.columns


def test_a_text_row_list_classifies_as_text_by_its_elements():
    column = pd.Series([np.array(["C5 incomplete", "T4"], dtype=object)], dtype=object)
    assert classify_column(element_values(column)) is Shape.CATEGORICAL


def test_row_factor_is_found_by_origin_not_name():
    split = explode_row_lists(_melted(), "M", ["FIN"], field_factor="ColName")
    table = _table(split)
    factor = row_factor(table)
    assert factor is not None and factor.name == ROW_FACTOR
    assert factor.origin == {"kind": "row_list", "fields": ["FIN"], "field_factor": "ColName"}
    assert factor.depth == 2  # under subject


def _table(split, field_factor="ColName") -> LongTable:
    factors = ["subject", *([field_factor] if field_factor else []), split.row_factor]
    return LongTable.from_frame(
        split.frame,
        factors=factors,
        measures=["M"],
        name="M",
        field_factors=[field_factor] if field_factor else [],
        factor_origins={split.row_factor: row_origin(split.fields, field_factor)},
        factor_depths={split.row_factor: 2},
        schema_levels=["subject"],
    )


def test_the_scalar_default_groups_row_and_iterates_subject():
    """One figure per subject, one mark per source row — what the user
    expected of FIN: one or two points, joined by Lines."""
    split = explode_row_lists(_melted(), "M", ["FIN"], field_factor="ColName")
    assignment = default_assignment(_table(split), "M")
    assert assignment.roles[ROW_FACTOR] is Role.GROUP
    assert assignment.roles["subject"] is Role.ITERATE
    assert assignment.groups == [ROW_FACTOR]


# --- a text array is not a series -------------------------------------------------


def test_a_text_array_is_not_a_series():
    assert classify_value(np.array(["a", "b"], dtype=object)) is Shape.UNKNOWN
    assert classify_value(np.array(["a", "b"])) is Shape.UNKNOWN  # '<U1'


def test_an_object_array_of_numbers_is_still_a_series():
    assert classify_value(np.array([1.0, 2.0], dtype=object)) is Shape.SERIES_1D
    assert classify_value(np.array([1.0, 2.0])) is Shape.SERIES_1D


def test_a_text_cell_collapses_to_a_gap_not_a_crash():
    assert cell_array(np.array(["Quadriplegia, C5-C7 incomplete"], dtype=object)).size == 0


def test_a_text_cell_in_the_collapse_is_warned(caplog):
    frame = pd.DataFrame(
        {
            "subject": ["A", "B"],
            "M": pd.Series(
                [np.array([1.0, 3.0]), np.array(["6/24/2026"], dtype=object)], dtype=object
            ),
        }
    )
    table = LongTable.from_frame(
        frame, factors=["subject"], measures=["M"], name="M", schema_levels=["subject"]
    )
    spec = PlotSpec(measures=["M"], kind=PlotKind.SCATTER)
    with caplog.at_level(logging.WARNING, logger="scistackplot"):
        collapsed = apply_cell_collapse(spec, table)
    assert list(collapsed.frame["M"].iloc[:1]) == [2.0]
    assert np.isnan(collapsed.frame["M"].iloc[1])
    assert any("non-numeric" in r.getMessage() for r in caplog.records)


# --- a one-point line is visible -------------------------------------------------


def test_lone_point_rule():
    assert is_lone_point([3.0])
    assert is_lone_point([np.nan, 3.0])
    assert not is_lone_point([1.0, 2.0])
    assert not is_lone_point([])


def _single_field() -> tuple[LongTable, pd.DataFrame]:
    """A one-column row-list variable: A had two source rows, B one."""
    raw = pd.DataFrame(
        {
            "subject": ["A", "B"],
            "M": pd.Series([np.array([10.0, 20.0]), np.array([30.0])], dtype=object),
        }
    )
    split = explode_row_lists(raw, "M", ["M"])
    return _table(split, field_factor=None), raw


def _line_spec(table) -> PlotSpec:
    assignment = default_assignment(table, "M")
    return PlotSpec(
        measures=["M"],
        roles=assignment.roles,
        groups=assignment.groups,
        kind=PlotKind.LINE,
    )


def _figure_for(spec, table, subject):
    for figure in resolve(spec, table):
        if figure.figure_key.get("subject") == subject:
            return figure
    raise AssertionError(f"no figure for {subject}")


def test_a_one_point_line_draws_a_marker_in_both_previews():
    table, _ = _single_field()
    figure = _figure_for(_line_spec(table), table, "B")
    drawn = render_matplotlib(figure)
    lines = [line for line in drawn.axes[0].get_lines() if len(line.get_ydata())]
    plt.close(drawn)
    assert lines and all(line.get_marker() == LONE_POINT_MARKER for line in lines)
    traces = [t for t in render_plotly(figure)["data"] if t.get("y")]
    assert traces and all(t["mode"] == "lines+markers" for t in traces)


def test_a_two_point_line_has_no_marker():
    table, _ = _single_field()
    figure = _figure_for(_line_spec(table), table, "A")
    drawn = render_matplotlib(figure)
    lines = [line for line in drawn.axes[0].get_lines() if len(line.get_ydata())]
    plt.close(drawn)
    assert [list(line.get_ydata()) for line in lines] == [[10.0, 20.0]]
    assert lines[0].get_marker() in (None, "None", "")


def test_the_export_emits_the_marker_rule():
    code = "\n".join(lone_point_marker_code("g.axes.flat"))
    assert "set_marker('o')" in code and "_ys.dropna().size) == 1" in code


# --- parity: the endpoint draws what the preview drew --------------------------


def _run(source: str, frame, function_name: str = "plot_m"):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


def _endpoint_frame(raw: pd.DataFrame, subject: str) -> pd.DataFrame:
    """What scidb hands the endpoint for one subject: several rows -> a
    Python list, one row -> the plain value (sciduckdb.unwrap_row_list)."""
    cells = [
        [float(x) for x in v] if len(v) > 1 else float(v[0])
        for v in raw["M"]
    ]
    frame = raw.assign(M=pd.Series(cells, dtype=object, index=raw.index))
    return frame[frame["subject"] == subject].reset_index(drop=True)


@pytest.mark.parametrize("subject", ["A", "B"])
def test_the_endpoint_splits_rows_like_the_preview(subject):
    pytest.importorskip("seaborn")
    table, raw = _single_field()
    spec = _line_spec(table)
    source = generate_plot_function(spec, table, function_name="plot_m")
    assert "# Row: one row per source row" in source

    preview = render_matplotlib(_figure_for(spec, table, subject))
    exported = _run(source, _endpoint_frame(raw, subject))
    try:
        def drawn(fig):
            return sorted(
                (tuple(float(y) for y in line.get_ydata()), line.get_marker())
                for ax in fig.axes
                for line in ax.get_lines()
                if len(line.get_ydata())
            )

        assert [ys for ys, _ in drawn(exported)] == [ys for ys, _ in drawn(preview)]
        # One point: a marker on both sides; two: on neither.
        lone = subject == "B"
        for fig in (preview, exported):
            assert all((marker == LONE_POINT_MARKER) == lone for _, marker in drawn(fig))
    finally:
        plt.close(preview)
        plt.close(exported)


def test_no_row_factor_emits_no_split():
    frame = pd.DataFrame({"subject": ["A", "B"], "M": [1.0, 2.0]})
    table = LongTable.from_frame(
        frame, factors=["subject"], measures=["M"], name="M", schema_levels=["subject"]
    )
    source = generate_plot_function(PlotSpec(measures=["M"], kind=PlotKind.LINE), table)
    assert "_row_lists" not in source
