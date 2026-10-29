"""for_columns column groups across the MATLAB bridge (Stage 4 of
.claude/plan-for-columns-groups.md).

Both directions carry the groups in scifor's wire form
(``groups_to_wire``), and the save merges MATLAB's per-output tables so a
combination absent from one output's table (a split group skipped there)
saves no record for that output.
"""

import sys
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scimatlab" / "src"))

import pandas as pd
import pytest
from scidb.database import configure_database
from scifor.column_selection import ColumnSelection
from scifor.outputs import is_no_output
from scimatlab.bridge import (
    _merge_output_tables,
    _reconstruct_input_for_keys,
    for_each_describe_loaded_input,
    register_matlab_variable,
)

SIX = ["6MWT", "6MWT_Date"]
TEN = ["10MWT", "10MWT_Date"]


@pytest.fixture
def db(tmp_path):
    db = configure_database(tmp_path / "bridge_column_groups.duckdb", ["subject"])
    yield db
    db.close()


@pytest.mark.parametrize(
    "groups, expected",
    [
        ([{"columns": SIX}, {"columns": TEN}], [SIX, TEN]),
        (
            [{"name": "6MWT", "columns": SIX}, {"name": "10MWT", "columns": TEN}],
            {"6MWT": SIX, "10MWT": TEN},
        ),
    ],
)
def test_matlab_to_python_keeps_the_groups(db, groups, expected):
    register_matlab_variable("BridgeWalks")
    cs = _reconstruct_input_for_keys(
        {
            "kind": "column_selection",
            "type_name": "BridgeWalks",
            "columns": SIX + TEN,
            "iterate": True,
            "groups": groups,
        }
    )
    assert cs.iterate and cs.column_spec == expected


def test_python_to_matlab_ships_the_groups():
    df = pd.DataFrame({c: [1.0] for c in SIX + TEN})
    desc = for_each_describe_loaded_input(ColumnSelection(df, [SIX, TEN], iterate=True))
    assert desc["groups"] == [{"columns": SIX}, {"columns": TEN}]
    assert desc["columns"] == SIX + TEN
    flat = for_each_describe_loaded_input(ColumnSelection(df, ["6MWT"], iterate=True))
    assert "groups" not in flat


def _out(name, subjects, values):
    return pd.DataFrame({"subject": subjects, "x__combo": subjects, name: values})


def test_absent_combination_becomes_a_skipped_slot():
    merged = _merge_output_tables(
        [_out("SixMWT", [1, 3], [12.0, 9.0]), _out("TenMWT", [1, 2], [6.0, 8.0])],
        ["SixMWT", "TenMWT"],
    )
    rows = merged.set_index("subject")
    assert rows.loc[1, "SixMWT"] == 12.0 and rows.loc[1, "TenMWT"] == 6.0
    assert is_no_output(rows.loc[2, "SixMWT"]) and rows.loc[2, "SixMWT"].reason == "skipped"
    assert is_no_output(rows.loc[3, "TenMWT"])
    assert not any(c.startswith("__present_") for c in merged.columns)


def test_output_skipped_everywhere():
    merged = _merge_output_tables(
        [pd.DataFrame(), _out("TenMWT", [1, 2], [6.0, 8.0])], ["SixMWT", "TenMWT"]
    )
    assert all(is_no_output(v) for v in merged["SixMWT"])
    assert list(merged["TenMWT"]) == [6.0, 8.0]


def test_a_real_nan_output_is_not_a_skip():
    merged = _merge_output_tables(
        [_out("A", [1], [float("nan")]), _out("B", [1], [2.0])], ["A", "B"]
    )
    assert not is_no_output(merged.loc[0, "A"]) and pd.isna(merged.loc[0, "A"])
