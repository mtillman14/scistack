"""for_columns column groups through scidb (Stage 3 of
.claude/plan-for-columns-groups.md): the whiteboard dataset
(spec/images/example_data_and_syntax.HEIC) saved per subject, run end to end
in both modes, with the stored selector carrying the groups.
"""

import logging

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.exceptions import NotFoundError
from scidb.intent import describe_columns, normalize_columns, parse_selector, selector_json
from scifor.column_selection import ColumnGroupsError

SCHEMA = ["subject"]
NAN = float("nan")
SIX = ["6MWT", "6MWT_Date"]
TEN = ["10MWT", "10MWT_Date"]


class Walks(BaseVariable):
    pass


class SixMWT(BaseVariable):
    pass


class TenMWT(BaseVariable):
    pass


class WalkSummary(BaseVariable):
    pass


ROWS = {
    1: {"6MWT": [10.0, 12.0], "6MWT_Date": ["10/29/26", "10/30/26"], "10MWT": [5.0, 6.0], "10MWT_Date": ["a", "b"]},
    2: {"6MWT": [NAN, NAN], "6MWT_Date": ["", "  "], "10MWT": [7.0, 8.0], "10MWT_Date": ["c", "d"]},
    3: {"6MWT": [8.0, 9.0], "6MWT_Date": ["10/1/26", "10/3/26"], "10MWT": [NAN, NAN], "10MWT_Date": ["", ""]},
}


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "for_columns_groups.duckdb", SCHEMA)
    for subject, cols in ROWS.items():
        Walks.save(pd.DataFrame(cols), db=db, subject=subject)
    yield db
    _scifor.set_schema([])
    db.close()


def latest_score(g):
    return float(g.iloc[-1, 0])


def _value(var, db, subject):
    data = var.load(db=db, subject=subject).data
    return float(np.asarray(data).ravel()[0]) if not isinstance(data, pd.DataFrame) else data


def test_split_saves_each_group_to_its_variable_sparsely(db, caplog):
    with caplog.at_level(logging.INFO):
        for_each(
            latest_score,
            inputs={"g": Walks.for_columns([SIX, TEN])},
            outputs=[SixMWT, TenMWT],
            subject=[],
            db=db,
        )
    assert _value(SixMWT, db, 1) == 12.0
    assert _value(SixMWT, db, 3) == 9.0
    assert _value(TenMWT, db, 1) == 6.0
    assert _value(TenMWT, db, 2) == 8.0
    # Sparse: subject 2 has no 6MWT record, subject 3 no 10MWT record.
    with pytest.raises(NotFoundError):
        SixMWT.load(db=db, subject=2)
    with pytest.raises(NotFoundError):
        TenMWT.load(db=db, subject=3)
    assert "split into 2 output(s): SixMWT ← [6MWT, 6MWT_Date]" in caplog.text


def test_combine_saves_one_variable_with_a_column_per_group(db):
    for_each(
        latest_score,
        inputs={"g": Walks.for_columns({"6MWT": SIX, "10MWT": TEN})},
        outputs=[WalkSummary],
        subject=[],
        db=db,
    )
    s2 = WalkSummary.load(db=db, subject=2).data
    assert list(s2.columns) == ["6MWT", "10MWT"]
    assert pd.isna(s2["6MWT"].iloc[0]) and s2["10MWT"].iloc[0] == 8.0
    s1 = WalkSummary.load(db=db, subject=1).data
    assert s1["6MWT"].iloc[0] == 12.0


def test_split_output_with_no_data_anywhere_gets_an_empty_table(db, caplog):
    with caplog.at_level(logging.INFO):
        for_each(
            latest_score,
            inputs={"g": Walks.for_columns([SIX, TEN])},
            outputs=[SixMWT, TenMWT],
            subject=[2],
            db=db,
        )
    assert "[save] SixMWT: 0 records this run (1 no data); created empty table" in caplog.text
    assert len(db._duck._fetchdf(f'SELECT * FROM "{SixMWT.view_name()}"')) == 0


def test_identical_grouped_rerun_is_cached(db):
    kwargs = dict(
        inputs={"g": Walks.for_columns([SIX, TEN])},
        outputs=[SixMWT, TenMWT],
        subject=[],
        db=db,
    )
    for_each(latest_score, **kwargs)
    for_each(latest_score, **kwargs)
    assert len(SixMWT.list_versions(db=db, subject=1)) == 1


def test_every_column_cannot_be_split_across_outputs(db):
    with pytest.raises(ColumnGroupsError, match="data's column order"):
        for_each(
            latest_score,
            inputs={"g": Walks.for_columns()},
            outputs=[SixMWT, TenMWT],
            subject=[],
            db=db,
        )


def test_mismatched_groups_across_inputs_are_refused(db):
    with pytest.raises(ColumnGroupsError, match="same groups"):
        for_each(
            lambda a, b: 0.0,
            inputs={"a": Walks.for_columns([SIX, TEN]), "b": Walks.for_columns([TEN, SIX])},
            outputs=[SixMWT, TenMWT],
            subject=[],
            db=db,
        )


# --- The stored selector (scidb.intent owns its shape) -----------------------


def test_flat_selector_bytes_are_unchanged():
    """The selector is folded into invocation_id: an existing flat selection
    must serialize exactly as before groups existed."""
    assert selector_json(Walks.for_columns(["a", "b"])) == '{"columns": ["a", "b"], "iterate": true}'
    assert selector_json(Walks[["a", "b"]]) == '{"columns": ["a", "b"]}'


def test_grouped_selectors_carry_their_groups_in_order():
    split = selector_json(Walks.for_columns([TEN, SIX]))
    assert split == (
        '{"columns": ["10MWT", "10MWT_Date", "6MWT", "6MWT_Date"], '
        '"groups": [{"columns": ["10MWT", "10MWT_Date"]}, {"columns": ["6MWT", "6MWT_Date"]}], '
        '"iterate": true}'
    )
    combine = selector_json(Walks.for_columns({"6MWT": SIX, "10MWT": TEN}))
    assert '"groups": [{"columns": ["6MWT", "6MWT_Date"], "name": "6MWT"}, ' in combine


@pytest.mark.parametrize(
    "spec", [[SIX, TEN], {"6MWT": SIX, "10MWT": TEN}, ["a", "b"]]
)
def test_selector_round_trips(spec):
    cs = Walks.for_columns(spec)
    assert parse_selector(selector_json(cs)) == normalize_columns(cs)


def test_groups_that_add_nothing_store_as_the_flat_list():
    assert selector_json(Walks.for_columns({"a": ["a"], "b": ["b"]})) == selector_json(
        Walks.for_columns(["a", "b"])
    )


def test_describe_and_gui_shapes():
    assert describe_columns(Walks.for_columns([SIX, TEN])) == "per group (2)"
    # What the GUI sends: the canonical dict with a groups list.
    sent = {"columns": [], "iterate": True, "groups": [{"columns": SIX}, {"columns": TEN}]}
    assert normalize_columns(sent) == normalize_columns(Walks.for_columns([SIX, TEN]))
