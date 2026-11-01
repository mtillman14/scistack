"""Plot Studio and row-list columns (rows combined by ``on_duplicate="combine"``).

``load_variable`` reads the stored cells as they are — every cell a float
array, one element per source row (``[7]``, ``[5, 12]``). The SOURCE then
splits them into rows numbered by a ``Row`` factor (``scistackplot.rows``):
the elements are observations, not a 1-D series. This replaced the "read as
1-D" decision of ``.claude/plan-combine-duplicate-locations.md`` §6 on
2026-10-30 (``.claude/plan-row-list-plots.md``), after a combined clinical
table plotted against an "index" axis and crashed every scalar kind on a
text field.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import pytest
from scidb import BaseVariable
from scistackplot import ROW_FACTOR, Filter, PlotKind, PlotSpec, Shape, resolve, row_factor
from scistackplot.resolved import Y
from scistackplot.roles import default_assignment

from scistackplotdb import ScidbSource, generate_endpoint
from scistackplotdb.load import load_variable


class StayDays(BaseVariable):
    pass


class StayDaysFigure(BaseVariable):
    """Endpoint output: the figure's path."""

    schema_version = 1


class Admission(BaseVariable):
    pass


def _stays(db):
    df = pd.DataFrame({"subject": ["01", "01", "02"], "days": [5.0, 12.0, 7.0]})
    StayDays.save(df, db=db, on_duplicate="combine")


def _admissions(db):
    """A struct variable: subject 01 was admitted twice. ``days`` and
    ``ward`` differ between the stays, so both become row lists; ``age`` is
    the same, so it stays a scalar."""
    df = pd.DataFrame(
        {
            "subject": ["01", "01", "02"],
            "days": [5.0, 12.0, 7.0],
            "ward": ["Quadriplegia, C5-C7 incomplete", "Paraplegia", "Stroke"],
            "age": [60.0, 60.0, 45.0],
        }
    )
    Admission.save(df, db=db, on_duplicate="combine")


def test_row_list_column_cells_are_1d_arrays(db):
    _stays(db)
    frame = load_variable(db, "StayDays", with_variants=False).frame
    value_cols = [c for c in frame.columns if c not in ("record_id", "subject", "session", "trial")]
    cells = {s: frame.loc[frame["subject"] == s, value_cols[0]].iloc[0] for s in ("01", "02")}
    assert all(isinstance(c, np.ndarray) for c in cells.values())
    assert list(cells["01"]) == [5.0, 12.0]
    assert list(cells["02"]) == [7.0]


def test_load_variable_names_the_row_list_columns(db):
    _admissions(db)
    variable = load_variable(db, "Admission", with_variants=False)
    assert set(variable.row_list_columns) == {"days", "ward"}


def test_a_row_list_variable_is_scalar_rows_with_a_row_factor(db):
    _stays(db)
    table = ScidbSource(db).get_table(["StayDays"])
    assert table.shape_of("StayDays") is Shape.SCALAR
    factor = row_factor(table)
    assert factor is not None and factor.name == ROW_FACTOR
    assert factor.levels == ["1", "2"]
    rows = table.frame.sort_values(["subject", ROW_FACTOR])
    assert list(zip(rows["subject"], rows[ROW_FACTOR], rows["StayDays"])) == [
        ("01", "1", 5.0),
        ("01", "2", 12.0),
        ("02", "1", 7.0),
    ]


def test_describe_reports_a_row_list_variable_as_scalar(db):
    _stays(db)
    measures = {m["name"]: m for m in ScidbSource(db).describe()["measures"]}
    assert measures["StayDays"]["shape"] == str(Shape.SCALAR)


def test_a_text_row_list_field_is_plotted_with_the_text_fields(db, caplog):
    """A text row list joins the variable's TEXT fields (`Admission (text)`,
    scistackplotdb.text_fields) — reachable, and said in the log, never
    dropped (docs/claude/categorical-measures.md, 2026-11-01)."""
    _admissions(db)
    source = ScidbSource(db)
    with caplog.at_level(logging.INFO, logger="scistackplotdb"):
        table = source.get_table(["Admission"])
    fields = next(f for f in table.factors if f.is_field)
    assert set(fields.levels) == {"days", "age"}  # ward is text
    assert table.shape_of("Admission") is Shape.SCALAR
    assert any("Admission (text)" in r.getMessage() and "ward" in r.getMessage() for r in caplog.records)

    text = source.get_table(["Admission (text)"])
    assert text.shape_of("Admission (text)") is Shape.CATEGORICAL
    ward = text.frame.sort_values(["subject", ROW_FACTOR])["Admission (text)"].tolist()
    assert ward == ["Quadriplegia, C5-C7 incomplete", "Paraplegia", "Stroke"]


def test_a_scalar_field_beside_a_row_list_keeps_one_row(db):
    _admissions(db)
    frame = ScidbSource(db).get_table(["Admission"]).frame
    age = frame[frame["ColName"] == "age"].sort_values("subject")
    assert list(age["Admission"]) == [60.0, 45.0]
    assert list(age[ROW_FACTOR]) == ["1", "1"]


def test_scatter_of_a_struct_with_a_text_row_list_resolves(db):
    """The 2026-10-30 crash: a scalar kind collapsed every field, text
    included, before the field filter."""
    _admissions(db)
    table = ScidbSource(db).get_table(["Admission"])
    assignment = default_assignment(table, "Admission")
    spec = PlotSpec(
        measures=["Admission"],
        roles=assignment.roles,
        groups=assignment.groups,
        kind=PlotKind.SCATTER,
        filters=[Filter(column="ColName", include=["days"])],
    )
    figures = resolve(spec, table)
    by_subject = {f.figure_key["subject"]: f for f in figures}
    assert set(by_subject) == {"01", "02"}
    # Subject 01: one point per stay.
    points = by_subject["01"].panels[0].frame
    assert sorted(points[Y]) == [5.0, 12.0]


def test_the_split_is_logged(db, caplog):
    _stays(db)
    with caplog.at_level(logging.INFO, logger="scistackplotdb"):
        ScidbSource(db).get_table(["StayDays"])
    assert any(
        "split row-list field(s)" in r.getMessage() and "2 -> 3 row(s)" in r.getMessage()
        for r in caplog.records
    )


def test_the_pipeline_endpoint_draws_one_point_per_stay(db, tmp_path):
    """End to end through scidb's own load (``[v]`` -> v, several -> a list):
    the exported endpoint runs for every subject and draws the stays the
    preview drew."""
    pytest.importorskip("seaborn")
    from scidb import PathOutput, for_each

    _stays(db)
    table = ScidbSource(db).get_table(["StayDays"])
    assignment = default_assignment(table, "StayDays")
    spec = PlotSpec(
        measures=["StayDays"],
        roles=assignment.roles,
        groups=assignment.groups,
        kind=PlotKind.LINE,
    )
    interactive = resolve(spec, table)
    code = generate_endpoint(
        spec,
        table,
        input_variable="StayDays",
        path_template=str(tmp_path / "fig_{subject}.png"),
    )
    assert "_row_lists = ['StayDays']" in code.source

    failures: dict = {}

    def _capturing_for_each(*args, **kwargs):
        def _progress(event):
            if event.get("event") == "summary":
                failures.update(event.get("failure_reasons") or {})

        return for_each(*args, _progress_fn=_progress, **kwargs)

    namespace = {
        "for_each": _capturing_for_each,
        "PathOutput": PathOutput,
        "StayDays": StayDays,
        "StayDaysFigure": StayDaysFigure,
    }
    exec(compile(code.source, "<generated>", "exec"), namespace)  # noqa: S102
    files = sorted(tmp_path.glob("fig_*.png"))
    assert len(files) == len(interactive) == 2, (failures, code.source)


def test_save_data_is_available_and_writes_one_line_per_stay(db):
    """Split into rows, the measure is scalar, so "Save data (CSV)" — refused
    for 1-D — is offered, and the file has a Row column."""
    from scistackplot import capabilities, plot_data

    _stays(db)
    table = ScidbSource(db).get_table(["StayDays"])
    assignment = default_assignment(table, "StayDays")
    spec = PlotSpec(
        measures=["StayDays"],
        roles=assignment.roles,
        groups=assignment.groups,
        kind=PlotKind.SCATTER,
    )
    assert capabilities(spec, table)["data_export"]["available"] is True
    data = plot_data(spec, table)
    assert list(data.columns) == ["subject", ROW_FACTOR, "StayDays"]
    assert list(zip(data["subject"], data[ROW_FACTOR], data["StayDays"])) == [
        ("01", "1", 5.0),
        ("01", "2", 12.0),
        ("02", "1", 7.0),
    ]
