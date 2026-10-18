"""``outputs=`` and scidb-only kwargs: what makes scifor.for_each a drop-in
for scidb.for_each (``scifor.outputs``).

* scidb-only kwargs (``save=``, ``db=``, ...) are accepted, logged and
  discarded — before 2026-10-18 they fell into ``**metadata_iterables`` and
  became a bogus iteration axis.
* ``outputs=`` takes classes as well as names; a class's ``column_name``
  attribute overrides its class name (one owner: ``output_column_name``).
* ``outputs`` is the third positional parameter, as in scidb.
"""

import json
import logging
from pathlib import Path

import pandas as pd
import pytest

from scifor import (
    DB_ONLY_KWARGS,
    EachOf,
    for_each,
    output_column_name,
    set_schema,
)


def setup_function():
    set_schema(["subject"])


def _df():
    return pd.DataFrame({"subject": [1, 2], "value": [10.0, 20.0]})


class StepLength:
    pass


class Renamed:
    column_name = "step_len"


class WithViewName:
    @classmethod
    def view_name(cls):
        return "ViewNamed"


# --- output_column_name ------------------------------------------------------


@pytest.mark.parametrize(
    "output, expected",
    [
        ("plain", "plain"),
        (StepLength, "StepLength"),
        (StepLength(), "StepLength"),
        (Renamed, "step_len"),
        (Renamed(), "step_len"),
        (WithViewName, "ViewNamed"),
    ],
)
def test_output_column_name_rule(output, expected):
    assert output_column_name(output) == expected


def test_empty_column_name_falls_back_to_class_name():
    class Blank:
        column_name = ""

    assert output_column_name(Blank) == "Blank"


def test_column_name_beats_view_name():
    class Both(WithViewName):
        column_name = "custom"

    assert output_column_name(Both) == "custom"


_CASES = json.loads(
    (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "claude"
        / "output-column-name-cases.json"
    ).read_text()
)["cases"]


@pytest.mark.parametrize("case", _CASES, ids=lambda c: c.get("class", c.get("string")))
def test_shared_cases(case):
    """Same table as MATLAB's TestOutputColumnName (one rule, two languages)."""
    if "string" in case:
        output = case["string"]
    else:
        attrs = {} if case["column_name"] is None else {"column_name": case["column_name"]}
        output = type(case["class"], (), attrs)
    assert output_column_name(output) == case["expected"]


# --- outputs= in for_each ----------------------------------------------------


def test_class_output_names_column_by_class_name():
    result = for_each(lambda x: x * 2, {"x": _df()}, outputs=[StepLength], subject=[1, 2])
    assert list(result["StepLength"]) == [20.0, 40.0]


def test_column_name_attribute_renames_column():
    result = for_each(lambda x: x * 2, {"x": _df()}, outputs=[Renamed], subject=[1, 2])
    assert "step_len" in result.columns
    assert "Renamed" not in result.columns


def test_mixed_class_and_string_outputs():
    result = for_each(
        lambda x: (x * 2, x * 3),
        {"x": _df()},
        outputs=[StepLength, "tripled"],
        subject=[1, 2],
    )
    assert {"StepLength", "tripled"} <= set(result.columns)


def test_outputs_is_third_positional():
    result = for_each(lambda x: x * 2, {"x": _df()}, ["doubled"], subject=[1, 2])
    assert list(result["doubled"]) == [20.0, 40.0]


def test_spread_dataframe_outputs_log_unused_names(caplog):
    # One row per call always spreads (it cannot multiply records), so the
    # DataFrame's own column "a" is used and the name goes unused.
    def fn(x):
        return pd.DataFrame({"a": [x]})

    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(fn, {"x": _df()}, outputs=[Renamed], subject=[1])
    assert "a" in result.columns
    assert "step_len" not in result.columns
    assert "not applied" in caplog.text


# --- scidb-only kwargs ---------------------------------------------------------


def test_db_only_kwargs_are_discarded(caplog):
    baseline = for_each(lambda x: x * 2, {"x": _df()}, outputs=["y"], subject=[1, 2])
    extra = {name: False for name in DB_ONLY_KWARGS}
    extra["db"] = object()
    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(
            lambda x: x * 2, {"x": _df()}, outputs=["y"], subject=[1, 2], **extra
        )
    pd.testing.assert_frame_equal(result, baseline)
    assert not (set(DB_ONLY_KWARGS) & set(result.columns))
    assert "discard_db_only_kwargs" in caplog.text
    assert "save=False" in caplog.text
    assert "db=<object>" in caplog.text


def test_db_only_kwargs_discarded_through_each_of():
    result = for_each(
        lambda x, k: x * k,
        {"x": _df(), "k": EachOf(2, 3)},
        outputs=["y"],
        save=True,
        skip_computed=True,
        subject=[1, 2],
    )
    assert "save" not in result.columns
    assert len(result) == 4


def test_no_log_without_db_only_kwargs(caplog):
    with caplog.at_level(logging.INFO, logger="scifor"):
        for_each(lambda x: x, {"x": _df()}, outputs=["y"], subject=[1])
    assert "discard_db_only_kwargs" not in caplog.text
