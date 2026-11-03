"""THE key-match rule (scifor.keys) against docs/claude/key-match-cases.json.

MATLAB's TestKeyMatchCases.m runs the same cases end to end through
scifor.for_each on MATLAB tables, so both languages select the same rows.
"""

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scifor import for_each, set_schema
from scifor.keys import series_mask

CASES_FILE = Path(__file__).resolve().parents[2] / "docs" / "claude" / "key-match-cases.json"


def _cases() -> list[dict]:
    if not CASES_FILE.exists():
        raise AssertionError(f"Shared key-match cases not found at {CASES_FILE}.")
    data = json.loads(CASES_FILE.read_text(encoding="utf-8"))
    return [c for c in data["cases"] if "python" in c.get("applies_to", ["python"])]


def _column(spec: dict) -> pd.Series:
    cls, values = spec["class"], spec["values"]
    if cls == "int":
        return pd.Series(values, dtype="int64")
    if cls == "float":
        return pd.Series([np.nan if v is None else v for v in values], dtype="float64")
    if cls == "bool":
        return pd.Series(values, dtype=bool)
    if cls == "categorical":
        return pd.Series(values, dtype="category")
    if cls in ("text", "object_mixed"):
        return pd.Series(values, dtype=object)
    raise AssertionError(f"unknown column class {cls!r}")


@pytest.mark.parametrize("case", _cases(), ids=lambda c: f"{c['id']:02d}-{c['name']}")
def test_key_match_case(case):
    mask = series_mask(_column(case["column"]), case["value"])
    assert list(np.flatnonzero(mask)) == case["expect"]


@pytest.mark.parametrize("case", _cases(), ids=lambda c: f"{c['id']:02d}-{c['name']}")
def test_for_each_selects_the_same_rows(case):
    """End to end: the per-combo filter is the rule (no second spelling)."""
    set_schema(["k"])
    try:
        col = _column(case["column"])
        df = pd.DataFrame({"k": col, "v": np.arange(1, len(col) + 1)})
        seen = []
        for_each(
            # several rows of one column arrive as an (n, 1) column vector
            lambda x: seen.append(np.ravel(x).tolist()) or 0,
            {"x": df},
            outputs=["out"],
            k=[case["value"]],
        )
        want = [i + 1 for i in case["expect"]]
        assert (seen[0] if seen else []) == want
    finally:
        set_schema([])


def test_zero_rows_by_type_mismatch_warns(caplog):
    """A text column asked for a number that is not there says why it matched
    nothing (once per column)."""
    from scifor import keys

    keys._warned.clear()
    df = pd.DataFrame({"k": pd.Series(["01", "02"], dtype=object), "v": [1, 2]})
    set_schema(["k"])
    try:
        with caplog.at_level(logging.WARNING, logger="scifor"):
            for_each(lambda x: x, {"x": df}, outputs=["out"], k=[3, 4])
    finally:
        set_schema([])
    hits = [r for r in caplog.records if "[key_match]" in r.getMessage()]
    assert len(hits) == 1
    assert "'k' is text" in hits[0].getMessage()
