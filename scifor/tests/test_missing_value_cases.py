"""THE no-data rule (scifor.missing) against docs/claude/missing-value-cases.json.

MATLAB's TestMissingValueCases.m runs the same cases through
scifor.has_no_data on MATLAB tables.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scifor.missing import frame_has_no_data

CASES_FILE = (
    Path(__file__).resolve().parents[2] / "docs" / "claude" / "missing-value-cases.json"
)


def _cases() -> list[dict]:
    if not CASES_FILE.exists():
        raise AssertionError(f"Shared missing-value cases not found at {CASES_FILE}.")
    return json.loads(CASES_FILE.read_text(encoding="utf-8"))["cases"]


def _column(spec: dict) -> pd.Series:
    cls, values = spec["class"], spec["values"]
    if cls == "float":
        return pd.Series([np.nan if v is None else v for v in values], dtype="float64")
    if cls in ("text", "text_cell"):
        return pd.Series(values, dtype=object)
    if cls == "datetime":
        return pd.Series(pd.to_datetime(values))
    if cls == "categorical":
        return pd.Series(values, dtype="category")
    if cls == "bool":
        return pd.Series(values, dtype=bool)
    if cls == "array_cell":
        cells = [
            None if v is None else np.array([np.nan if x is None else x for x in v], dtype=float)
            for v in values
        ]
        return pd.Series(cells, dtype=object)
    raise AssertionError(f"unknown column class {cls!r}")


@pytest.mark.parametrize("case", _cases(), ids=lambda c: f"{c['id']:02d}-{c['name']}")
def test_missing_value_case(case):
    frame = pd.DataFrame({"c": _column(case["column"])})
    assert frame_has_no_data(frame) is case["no_data"]
