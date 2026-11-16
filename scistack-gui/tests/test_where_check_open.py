"""How a where= check OPENS (services/where_check_service.py), issue #13.

A chip click is a histogram of the filter's variable; "plot against" puts the
node's input on y with the filter's variable on x (numeric) or as the
grouping (categorical). Either way ``Matching`` holds the colour, every
schema key is collapsed, and the table carries the Yes/No factors.
"""

from __future__ import annotations

import pytest

pytest.importorskip("scistackplot")
pytest.importorskip("scistackplotdb")

import scistack_gui.runtime.db as _gui_db
from scidb import BaseVariable, configure_database
from scidb.store.database import _local

from scistack_gui.services import plot_service


class WcAge(BaseVariable):
    schema_version = 1


class WcSex(BaseVariable):
    schema_version = 1


class WcDistance(BaseVariable):
    schema_version = 1


@pytest.fixture
def check_db(tmp_path):
    db_path = tmp_path / "check.duckdb"
    db = configure_database(db_path, ["subject", "trial"])
    for subject, age, sex in [(1, 82, "M"), (2, 60, "F"), (3, 75, "M")]:
        WcAge.save(age, subject=subject)
        WcSex.save(sex, subject=subject)
        for trial in (1, 2):
            WcDistance.save(float(age) / 10 + trial, subject=subject, trial=trial)
    _gui_db._db = db
    _gui_db._db_path = db_path
    plot_service.invalidate()
    yield db
    plot_service.invalidate()
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


def _check(index: int, *, against: bool, filters=None) -> dict:
    return {
        "node_id": "fn__f__1",
        "function_name": "f",
        "filter_index": index,
        "against": against,
        "filters": filters
        or [
            {"variable": "WcAge", "op": ">", "value": "70"},
            {"variable": "WcSex", "op": "==", "value": "M"},
        ],
    }


def _factor_names(described) -> list[str]:
    return [f["name"] for f in described["table"]["factors"]]


def test_a_chip_click_is_a_histogram_of_the_filter_variable_painted_by_matching(check_db):
    described = plot_service.describe(check_db, "WcAge", where_check=_check(0, against=False))

    spec = described["spec"]
    assert spec["kind"] == "histogram"
    assert spec["measures"] == ["WcAge"]
    assert spec["color"] == "Matching" and spec["groups"] == ["Matching"]
    assert spec["roles"]["subject"] == "collapse"
    assert spec["colors"]["Matching"]["Yes"]
    assert {"Matching", "Filter 1", "Filter 2"} <= set(_factor_names(described))
    assert spec["where_check"]["node_id"] == "fn__f__1"


def test_plot_against_a_numeric_filter_puts_it_on_x_as_a_scatter(check_db):
    described = plot_service.describe(check_db, "WcDistance", where_check=_check(0, against=True))

    spec = described["spec"]
    assert spec["kind"] == "scatter"
    assert spec["measures"] == ["WcDistance"] and spec["x_measure"] == "WcAge"
    assert spec["roles"]["subject"] == "collapse" and spec["roles"]["trial"] == "collapse"


def test_plot_against_a_categorical_filter_groups_by_it_and_shows_the_sample(check_db):
    described = plot_service.describe(check_db, "WcDistance", where_check=_check(1, against=True))

    spec = described["spec"]
    assert spec["kind"] == "box"
    assert [g["variable"] for g in spec["factor_variables"]] == ["WcSex"]
    assert spec["groups"] == ["Matching", "WcSex"]
    assert set(spec["show_sample"]) == {"subject", "trial"}


def test_a_check_cannot_export_code(check_db):
    described = plot_service.describe(check_db, "WcAge", where_check=_check(0, against=False))
    code = described["capabilities"]["code_export"]
    assert code["available"] is False and "where= check" in code["reason"]

    ordinary = plot_service.describe(check_db, "WcAge")
    assert ordinary["capabilities"]["code_export"]["available"] is True
