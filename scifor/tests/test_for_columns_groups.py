"""for_columns column groups (Stage 2 of .claude/plan-for-columns-groups.md).

Several columns are passed to the function together. With ONE output the
groups combine into that output's columns (dict keys name them); with
SEVERAL outputs each group is saved as its own output, paired in order.
A group with no data at a combination is not called.

The data is the whiteboard example (spec/images/example_data_and_syntax.HEIC):
two rows per subject, subject 2 blank for the 6MWT, subject 3 for the 10MWT.
"""

import logging

import numpy as np
import pandas as pd
import pytest

from scifor import ColName, ColumnSelection, for_each, set_schema
from scifor.column_selection import (
    COMBINE,
    SPLIT,
    ColumnGroupsError,
    parse_column_groups,
    resolve_column_groups,
)
from scifor.missing import frame_has_no_data, is_missing_value

NAN = float("nan")

DATA = pd.DataFrame(
    {
        "subject": [1, 1, 2, 2, 3, 3],
        "6MWT": [10.0, 12.0, NAN, NAN, 8.0, 9.0],
        "6MWT_Date": ["10/29/26", "10/30/26", "", "  ", "10/1/26", "10/3/26"],
        "10MWT": [5.0, 6.0, 7.0, 8.0, NAN, NAN],
        "10MWT_Date": ["a", "b", "c", "d", None, None],
    }
)
SIX = ["6MWT", "6MWT_Date"]
TEN = ["10MWT", "10MWT_Date"]


@pytest.fixture(autouse=True)
def _schema():
    set_schema(["subject"])
    yield
    set_schema([])


def latest_score(g):
    """The score on the group's last row (a 2-column DataFrame)."""
    assert list(g.columns)[1].endswith("_Date")
    return float(g.iloc[-1, 0])


def _summary(events):
    (summary,) = [e for e in events if e.get("event") == "summary"]
    return summary


# --- What a column spec means ---------------------------------------------


def test_parse_flat_and_all_columns():
    assert parse_column_groups(None) == ([], None)
    assert parse_column_groups([]) == ([], None)
    assert parse_column_groups("a") == (["a"], None)
    assert parse_column_groups(["a", "b"]) == (["a", "b"], None)


def test_parse_unnamed_and_named_groups():
    assert parse_column_groups([SIX, "x"]) == (
        ["6MWT", "6MWT_Date", "x"],
        [(None, SIX), (None, ["x"])],
    )
    assert parse_column_groups({"6MWT": SIX, "10MWT": TEN}) == (
        SIX + TEN,
        [("6MWT", SIX), ("10MWT", TEN)],
    )


def test_groups_that_add_nothing_normalize_to_the_flat_list():
    assert parse_column_groups({"a": ["a"], "b": "b"}) == (["a", "b"], None)
    assert parse_column_groups([["a"], ["b"]]) == (["a", "b"], None)


def test_overlapping_groups_keep_the_union_once():
    cols, groups = parse_column_groups([["a", "d"], ["b", "d"]])
    assert cols == ["a", "d", "b"]
    assert groups == [(None, ["a", "d"]), (None, ["b", "d"])]


@pytest.mark.parametrize(
    "spec, match",
    [
        ([["a", "b"], []], "at least one column"),
        ({"": ["a", "b"]}, "non-empty strings"),
        ({"g": ["a", 3]}, "non-empty strings"),
        ([["a", "a"]], "more than once"),
    ],
)
def test_malformed_groups_are_refused(spec, match):
    with pytest.raises(ColumnGroupsError, match=match):
        parse_column_groups(spec)


def test_groups_need_iterate_and_no_exclusions():
    with pytest.raises(ColumnGroupsError, match="need for_columns"):
        ColumnSelection(DATA, [SIX], iterate=False)
    with pytest.raises(ColumnGroupsError, match="excl_columns"):
        ColumnSelection(DATA, [SIX, TEN], iterate=True, excl_columns=["x"])


def test_to_key_adds_groups_only_when_grouped():
    flat = ColumnSelection(DATA, ["a", "b"], iterate=True)
    assert "groups" not in flat.to_key()
    grouped = ColumnSelection(DATA, {"6MWT": SIX}, iterate=True)
    assert "groups=(('6MWT', ('6MWT', '6MWT_Date')),)" in grouped.to_key()
    unnamed = ColumnSelection(DATA, [SIX, TEN], iterate=True)
    assert grouped.to_key() != unnamed.to_key()


# --- Which mode (the one owner) -------------------------------------------


def test_one_output_combines():
    assert resolve_column_groups(["a", "b"], None, ["Out"]) == (
        COMBINE,
        [("a", ["a"]), ("b", ["b"])],
    )
    _, groups = parse_column_groups({"6MWT": SIX})
    assert resolve_column_groups(SIX, groups, ["Out"]) == (COMBINE, [("6MWT", SIX)])


def test_several_outputs_split_in_order():
    _, groups = parse_column_groups([SIX, TEN])
    assert resolve_column_groups(SIX + TEN, groups, ["SixMWT", "TenMWT"]) == (
        SPLIT,
        [("SixMWT", SIX), ("TenMWT", TEN)],
    )
    assert resolve_column_groups(["a", "b"], None, ["A", "B"]) == (
        SPLIT,
        [("A", ["a"]), ("B", ["b"])],
    )


@pytest.mark.parametrize(
    "spec, outputs, every_column, match",
    [
        ([SIX, TEN], ["Out"], False, "needs a name"),
        ({"6MWT": SIX, "10MWT": TEN}, ["A", "B"], False, "would name nothing"),
        ([SIX, TEN], ["A", "B", "C"], False, "counts must match"),
        ([], ["A", "B"], True, "data's column order"),
    ],
)
def test_mode_errors(spec, outputs, every_column, match):
    cols, groups = parse_column_groups(spec)
    with pytest.raises(ColumnGroupsError, match=match):
        resolve_column_groups(cols or ["a", "b"], groups, outputs, every_column=every_column)


# --- Missing ---------------------------------------------------------------


@pytest.mark.parametrize("value", [None, NAN, pd.NA, pd.NaT, "", "   "])
def test_missing_values(value):
    assert is_missing_value(value)


@pytest.mark.parametrize("value", ["—", "0", 0, 0.0, False, [NAN], np.array([NAN])])
def test_present_values(value):
    assert not is_missing_value(value)


def test_frame_has_no_data():
    assert frame_has_no_data(DATA.loc[DATA.subject == 2, SIX])
    assert not frame_has_no_data(DATA.loc[DATA.subject == 2, TEN])
    assert frame_has_no_data(DATA.iloc[0:0][SIX])


# --- Combine ---------------------------------------------------------------


def test_combine_names_columns_by_key_in_key_order(caplog):
    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(
            latest_score,
            inputs={"g": ColumnSelection(DATA, {"10MWT": TEN, "6MWT": SIX}, iterate=True)},
            outputs=["Out"],
            subject=[1, 2, 3],
        )
    assert list(result.columns) == ["subject", "10MWT", "6MWT"]
    rows = result.set_index("subject")
    assert rows.loc[1, "6MWT"] == 12.0 and rows.loc[1, "10MWT"] == 6.0
    # Subject 2 has no 6MWT data: not called, left missing in the row.
    assert np.isnan(rows.loc[2, "6MWT"]) and rows.loc[2, "10MWT"] == 8.0
    assert np.isnan(rows.loc[3, "10MWT"]) and rows.loc[3, "6MWT"] == 9.0
    assert (
        "[for_columns] 'g' combine → Out, 2 group(s): "
        "10MWT ← [10MWT, 10MWT_Date]; 6MWT ← [6MWT, 6MWT_Date]"
    ) in caplog.text
    assert "for_columns group 6MWT: no data × 1 — subject=2" in caplog.text


def test_combine_dict_return_is_prefixed_by_the_group_name():
    result = for_each(
        lambda g: {"first": float(g.iloc[0, 0]), "n": len(g)},
        inputs={"g": ColumnSelection(DATA, {"6MWT": SIX}, iterate=True)},
        outputs=["Out"],
        subject=[1],
    )
    assert list(result.columns) == ["subject", "6MWT__first", "6MWT__n"]


# --- Split -----------------------------------------------------------------


def test_split_saves_each_group_as_its_output_and_skips_no_data(caplog):
    events = []
    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(
            latest_score,
            inputs={"g": ColumnSelection(DATA, [SIX, TEN], iterate=True)},
            outputs=["SixMWT", "TenMWT"],
            subject=[1, 2, 3],
            _progress_fn=events.append,
        )
    rows = result.set_index("subject")
    assert rows.loc[1, "SixMWT"] == 12.0 and rows.loc[1, "TenMWT"] == 6.0
    assert pd.isna(rows.loc[2, "SixMWT"]) and rows.loc[2, "TenMWT"] == 8.0
    assert rows.loc[3, "SixMWT"] == 9.0 and pd.isna(rows.loc[3, "TenMWT"])
    summary = _summary(events)
    assert summary["completed"] == 3
    assert summary["output_skips"]["SixMWT"][0]["combos"] == ["subject=2"]
    assert summary["output_skips"]["TenMWT"][0]["combos"] == ["subject=3"]
    assert (
        "[for_columns] 'g' split into 2 output(s): "
        "SixMWT ← [6MWT, 6MWT_Date]; TenMWT ← [10MWT, 10MWT_Date]"
    ) in caplog.text


def test_split_keeps_multi_row_returns():
    result = for_each(
        lambda g: g.reset_index(drop=True),
        inputs={"g": ColumnSelection(DATA, [SIX, TEN], iterate=True)},
        outputs=["SixMWT", "TenMWT"],
        subject=[1],
    )
    six = result.set_index("subject").loc[1, "SixMWT"]
    assert list(six.columns) == SIX and len(six) == 2


def test_split_group_that_raises_skips_only_its_output(caplog):
    def picky(g):
        if g.columns[0] == "10MWT" and g.iloc[0, 0] == 7.0:
            raise ValueError("subject 2 10MWT rejected")
        return float(g.iloc[-1, 0])

    events = []
    with caplog.at_level(logging.WARNING, logger="scifor"):
        result = for_each(
            picky,
            inputs={"g": ColumnSelection(DATA, [SIX, TEN], iterate=True)},
            outputs=["SixMWT", "TenMWT"],
            subject=[1, 2],
            _progress_fn=events.append,
        )
    rows = result.set_index("subject")
    assert rows.loc[1, "TenMWT"] == 6.0
    # Subject 2: no 6MWT data AND 10MWT raised -> every slot skipped, so the
    # combination counts as failed (it has a real failure), not no-data.
    summary = _summary(events)
    assert summary["completed"] == 1 and summary["failed"] == 1
    assert "subject 2 10MWT rejected" in summary["failures"][0]["reason"]


def test_split_raise_with_another_group_saved():
    def reject_six_at_3(g):
        if g.columns[0] == "6MWT" and g.iloc[0, 0] == 8.0:
            raise ValueError("bad 6MWT")
        return float(g.iloc[-1, 0])

    data = DATA.assign(**{"10MWT": [5.0, 6.0, 7.0, 8.0, 1.0, 2.0]})
    events = []
    result = for_each(
        reject_six_at_3,
        inputs={"g": ColumnSelection(data, [SIX, TEN], iterate=True)},
        outputs=["SixMWT", "TenMWT"],
        subject=[3],
        _progress_fn=events.append,
    )
    rows = result.set_index("subject")
    assert pd.isna(rows.loc[3, "SixMWT"]) and rows.loc[3, "TenMWT"] == 2.0
    (skip,) = _summary(events)["output_skips"]["SixMWT"]
    assert skip["reason"] == "failed" and skip["detail"] == "ValueError: bad 6MWT"


def test_every_group_without_data_is_one_no_data_combination():
    events = []
    data = DATA.assign(**{"10MWT": [NAN] * 6, "10MWT_Date": [""] * 6})
    for_each(
        latest_score,
        inputs={"g": ColumnSelection(data, [SIX, TEN], iterate=True)},
        outputs=["SixMWT", "TenMWT"],
        subject=[2],
        _progress_fn=events.append,
    )
    summary = _summary(events)
    assert summary["no_data"] == 1 and summary["completed"] == 0


def test_placeholder_text_is_data_and_partial_blanks_pass_through():
    seen = []

    def record(g):
        seen.append(g.copy())
        return 0.0

    data = DATA.copy()
    data.loc[2, "6MWT_Date"] = "—"  # subject 2: score blank, date a placeholder
    data.loc[5, "10MWT"] = 4.0  # subject 3: one of two rows filled
    for_each(
        record,
        inputs={"g": ColumnSelection(data, [SIX, TEN], iterate=True)},
        outputs=["SixMWT", "TenMWT"],
        subject=[2, 3],
    )
    by_first = {(g.columns[0], len(g)): g for g in seen}
    assert ("6MWT", 2) in by_first  # subject 2's 6MWT group was called
    ten_s3 = [g for g in seen if g.columns[0] == "10MWT" and g.iloc[1, 0] == 4.0]
    assert len(ten_s3) == 1 and pd.isna(ten_s3[0].iloc[0, 0])  # both rows kept


# --- Shared rules ------------------------------------------------------------


def test_overlap_warns_once(caplog):
    with caplog.at_level(logging.WARNING, logger="scifor"):
        for_each(
            latest_score,
            inputs={
                "g": ColumnSelection(
                    DATA, [["6MWT", "6MWT_Date"], ["10MWT", "6MWT_Date"]], iterate=True
                )
            },
            outputs=["A", "B"],
            subject=[1],
        )
    lines = [r.getMessage() for r in caplog.records if "more than one group" in r.getMessage()]
    assert lines == [
        "[for_columns] column(s) in more than one group: 6MWT_Date in ['A', 'B'] "
        "— each group still receives it"
    ]


def test_two_iterate_inputs_must_declare_the_same_groups():
    for_each(
        lambda a, b: float(a.iloc[0, 0] - b.iloc[0, 0]),
        inputs={
            "a": ColumnSelection(DATA, [SIX, TEN], iterate=True),
            "b": ColumnSelection(DATA, [SIX, TEN], iterate=True),
        },
        outputs=["A", "B"],
        subject=[1],
    )
    with pytest.raises(ColumnGroupsError, match="same groups"):
        for_each(
            lambda a, b: 0.0,
            inputs={
                "a": ColumnSelection(DATA, [SIX, TEN], iterate=True),
                "b": ColumnSelection(DATA, [TEN, SIX], iterate=True),
            },
            outputs=["A", "B"],
            subject=[1],
        )


def test_colname_is_the_group_name_in_each_mode():
    combine = for_each(
        lambda g, name: name,
        inputs={"g": ColumnSelection(DATA, {"six": SIX}, iterate=True), "name": ColName()},
        outputs=["Out"],
        subject=[1],
    )
    assert combine.loc[0, "six"] == "six"
    split = for_each(
        lambda g, name: name,
        inputs={"g": ColumnSelection(DATA, [SIX, TEN], iterate=True), "name": ColName()},
        outputs=["SixMWT", "TenMWT"],
        subject=[1],
    )
    assert split.loc[0, "SixMWT"] == "SixMWT" and split.loc[0, "TenMWT"] == "TenMWT"


def test_as_table_gives_schema_keys_plus_the_group():
    seen = []
    for_each(
        lambda g: seen.append(list(g.columns)) or 0.0,
        inputs={"g": ColumnSelection(DATA, [SIX, TEN], iterate=True)},
        outputs=["A", "B"],
        as_table=["g"],
        subject=[1],
    )
    assert seen == [["subject", *SIX], ["subject", *TEN]]


def test_flat_column_with_no_data_is_left_missing_in_combine():
    """A plain for_columns list is one-column groups, so the no-data rule
    applies to it too: an all-blank column at a subject is not called."""
    calls = []

    def count(v):
        calls.append(v)
        return float(np.nansum(v))

    result = for_each(
        count,
        inputs={"v": ColumnSelection(DATA, ["6MWT", "10MWT"], iterate=True)},
        subject=[1, 2],
    )
    assert len(calls) == 3  # subject 1: both columns; subject 2: 10MWT only
    rows = result.set_index("subject")
    assert pd.isna(rows.loc[2, "6MWT"]) and rows.loc[2, "10MWT"] == 15.0


# --- Combine keeps one column set -------------------------------------------


def test_group_without_data_anywhere_still_gets_its_column(caplog):
    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(
            latest_score,
            inputs={"g": ColumnSelection(DATA, {"10MWT": TEN, "6MWT": SIX}, iterate=True)},
            outputs=["Out"],
            subject=[2],
        )
    assert list(result.columns) == ["subject", "10MWT", "6MWT"]
    assert pd.isna(result.loc[0, "6MWT"])
    assert (
        "group 6MWT had no data at any combination; added empty column(s) ['6MWT']"
        in caplog.text
    )


def test_missing_group_takes_the_dict_shape_of_the_others():
    result = for_each(
        lambda g: {"first": float(g.iloc[0, 0]), "n": len(g)},
        inputs={"g": ColumnSelection(DATA, {"10MWT": TEN, "6MWT": SIX}, iterate=True)},
        outputs=["Out"],
        subject=[2],
    )
    assert list(result.columns) == [
        "subject",
        "10MWT__first",
        "10MWT__n",
        "6MWT__first",
        "6MWT__n",
    ]
    assert result[["6MWT__first", "6MWT__n"]].isna().all().all()


def test_missing_group_with_disagreeing_shapes_warns(caplog):
    data = DATA.assign(x=[1.0] * 6, x_d=["d"] * 6)

    def shaped(g):
        if g.columns[0] == "x":
            return {"a": 1.0}
        return float(g.iloc[-1, 0])

    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(
            shaped,
            inputs={
                "g": ColumnSelection(
                    data, {"10MWT": TEN, "x": ["x", "x_d"], "6MWT": SIX}, iterate=True
                )
            },
            outputs=["Out"],
            subject=[2],
        )
    assert list(result.columns) == ["subject", "10MWT", "x__a", "6MWT"]
    assert "the other groups returned different shapes" in caplog.text


def test_rows_share_one_column_order_when_groups_skip_at_some_subjects():
    result = for_each(
        latest_score,
        inputs={"g": ColumnSelection(DATA, {"10MWT": TEN, "6MWT": SIX}, iterate=True)},
        outputs=["Out"],
        subject=[3, 2, 1],
    )
    assert list(result.columns) == ["subject", "10MWT", "6MWT"]
