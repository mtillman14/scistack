"""scidb.inspect.preview: what a variable holds, as a function receives it."""

import datetime
import json
from dataclasses import asdict

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database
from scidb.inspect import Inspector
from scidb.inspect.render import render_data_preview


class Sheet(BaseVariable):
    pass


class Scalar(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "preview.duckdb", ["subject", "bout"])
    frames = {
        ("A", 1): {"tone": "1+", "score": 3.0, "blank": None},
        ("A", 2): {"tone": None, "score": np.nan, "blank": None},
        ("B", 1): {"tone": "2", "score": 5.0, "blank": None},
    }
    for (subject, bout), row in frames.items():
        Sheet.save(pd.DataFrame([row]), db=database, subject=subject, bout=bout)
    yield database
    _scifor.set_schema([])
    database.close()


def _column(preview, name):
    return next(c for c in preview.columns if c.name == name)


def test_types_counts_and_samples(db):
    preview = Inspector(db).preview(Sheet)
    assert (preview.n_records, preview.n_rows) == (3, 3)
    tone = _column(preview, "tone")
    assert tone.stored_type == "VARCHAR"
    assert tone.loaded_types == {"str": 2}
    assert (tone.non_null, tone.n_rows) == (2, 3)
    assert tone.samples == ["1+", "2"]
    score = _column(preview, "score")
    assert score.stored_type == "DOUBLE"
    assert score.loaded_types == {"float": 2}


def test_a_column_with_no_values_says_so(db):
    blank = _column(Inspector(db).preview(Sheet), "blank")
    assert blank.stored_type == "(all blank)"
    assert blank.non_null == 0


def test_location_narrows_rows_but_levels_stay_whole(db):
    preview = Inspector(db).preview(Sheet, location={"subject": "A"})
    assert preview.n_rows == 2
    assert {row["subject"] for row in preview.rows} == {"A"}
    assert preview.levels["subject"] == ["A", "B"]
    assert preview.levels["bout"] == ["1", "2"]


def test_a_column_selection_restricts_the_data_columns(db):
    preview = Inspector(db).preview(Sheet, columns=["tone", "nope"])
    assert [c.name for c in preview.columns] == ["tone"]
    assert preview.missing_columns == ["nope"]
    assert set(preview.rows[0]) == {"subject", "bout", "tone"}


def test_rows_are_limited_but_summaries_cover_every_row(db):
    preview = Inspector(db).preview(Sheet, limit=1)
    assert len(preview.rows) == 1 and preview.truncated
    assert _column(preview, "tone").non_null == 2


def test_the_result_is_json_safe_with_arrays_and_dates(db):
    Scalar.save(np.array([1.0, 2.0]), db=db, subject="A", bout=1)
    preview = Inspector(db).preview(Scalar)
    json.dumps(asdict(preview))

    Sheet.save(
        pd.DataFrame([{"tone": "x", "score": 1.0, "blank": None}]),
        db=db, subject="C", bout=datetime.date(2026, 7, 9),
    )
    json.dumps(asdict(Inspector(db).preview(Sheet, location={"subject": "C"})))


def test_an_unknown_key_is_refused(db):
    with pytest.raises(ValueError, match="not schema keys"):
        Inspector(db).preview(Sheet, location={"nope": 1})


def test_a_variable_with_no_imported_class_still_previews(db, monkeypatch):
    """The CLI has no user classes: the loader needs only the name."""
    monkeypatch.delitem(BaseVariable._all_subclasses, "Sheet")
    assert Inspector(db).preview("Sheet").n_rows == 3
    # The stand-in must not stay registered under the real name's slot for later tests.
    BaseVariable._all_subclasses.pop("Sheet", None)


def test_text_rendering_names_types_and_rows(db):
    text = render_data_preview(Inspector(db).preview(Sheet, location={"subject": "A"}))
    assert "Sheet at subject=A" in text
    assert "VARCHAR" in text and "str x1" in text and "1/2" in text
