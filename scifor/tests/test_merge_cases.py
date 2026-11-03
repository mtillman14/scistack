"""THE Merge rule against docs/claude/merge-cases.json.

The same file is read by MATLAB's TestMergeCases.m (MATLAB runs the rule
natively on MATLAB tables), so a case added there fails whichever language
has not adopted it. Owner: scifor.merge (drop_constant_keys, merge_frames).
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from scifor.merge import drop_constant_keys, merge_frames

CASES_FILE = Path(__file__).resolve().parents[2] / "docs" / "claude" / "merge-cases.json"

ERROR_FRAGMENTS = {
    "conflict": (KeyError, "Column name conflict in Merge"),
    "empty": (ValueError, "has no rows"),
    "heights": (ValueError, "different row counts"),
    "no_key": (ValueError, "no shared schema column"),
    "unmatched": (ValueError, "Merge would drop rows"),
    "many_to_many": (ValueError, "both repeat key values"),
}


def _cases() -> list[dict]:
    if not CASES_FILE.exists():
        raise AssertionError(f"Shared Merge cases not found at {CASES_FILE}.")
    data = json.loads(CASES_FILE.read_text(encoding="utf-8"))
    return [c for c in data["cases"] if "python" in c.get("applies_to", ["python"])]


def _run(case: dict) -> pd.DataFrame:
    keys = case["schema_keys"]
    parts = [pd.DataFrame(p) for p in case["parts"]]
    drop = case.get("drop") or [False] * len(parts)
    parts = [drop_constant_keys(p, keys)[0] if d else p for p, d in zip(parts, drop)]
    return merge_frames(parts, keys, positional=case.get("positional", True))


@pytest.mark.parametrize("case", _cases(), ids=lambda c: f"{c['id']:02d}-{c['name']}")
def test_merge_case(case):
    if "error" in case:
        exc, fragment = ERROR_FRAGMENTS[case["error"]]
        with pytest.raises(exc, match=fragment):
            _run(case)
        return
    got = _run(case)
    want = pd.DataFrame(case["expect"])
    assert list(got.columns) == list(want.columns)
    pd.testing.assert_frame_equal(
        got.reset_index(drop=True), want, check_dtype=False
    )


def test_every_error_kind_is_covered():
    kinds = {c["error"] for c in _cases() if "error" in c}
    assert kinds == set(ERROR_FRAGMENTS)


def test_drop_constant_keys_reports_dropped_values():
    df = pd.DataFrame({"subject": [1, 1], "trial": [1, 2], "x": [5, 6]})
    out, dropped = drop_constant_keys(df, ["subject", "trial"])
    assert list(out.columns) == ["trial", "x"]
    assert dropped == {"subject": 1}
