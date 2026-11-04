"""THE distribute rule (scifor.distribute) against docs/claude/distribute-cases.json.

MATLAB's TestDistributeCases.m runs the same cases end to end, so both languages
file a returned table's rows at the same addresses.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from scifor import for_each, set_schema

CASES_FILE = Path(__file__).resolve().parents[2] / "docs" / "claude" / "distribute-cases.json"


def _cases() -> list[dict]:
    if not CASES_FILE.exists():
        pytest.skip(f"{CASES_FILE} is not in this checkout")
    return json.loads(CASES_FILE.read_text(encoding="utf-8"))["cases"]


def teardown_function():
    set_schema([])


def _run(case: dict) -> pd.DataFrame:
    set_schema(case["schema"])
    frame = pd.DataFrame(case["frame"])
    iterate = {key: [value] for key, value in case["iterate"].items()}
    return for_each(lambda: frame, inputs={}, distribute=True, **iterate)


@pytest.mark.parametrize("case", _cases(), ids=lambda c: c["name"])
def test_distribute_case(case):
    if "error" in case:
        with pytest.raises(ValueError) as caught:
            _run(case)
        assert case["error"] in str(caught.value)
        return

    result = _run(case)
    keys = list(case["addresses"][0])
    got = [{k: str(row[k]) for k in keys} for row in result.to_dict("records")]
    assert got == case["addresses"]
    assert list(result.columns).count("val") == 1


def test_distribute_keeps_each_output_under_its_own_name():
    """Rows used to hold only the piece being split, so every output of a
    multi-output run was filed under the first output's name."""
    set_schema(["subject", "trial"])
    result = for_each(
        lambda: (pd.DataFrame({"a": [1.0, 2.0]}), pd.DataFrame({"b": [3.0, 4.0]})),
        inputs={},
        outputs=["A", "B"],
        distribute=True,
        subject=[1],
    )
    assert list(result["trial"]) == [1, 2]
    assert list(result["A"].iloc[0].columns) == ["a"]
    assert list(result["B"].iloc[1].columns) == ["b"]
    assert result["B"].iloc[1]["b"].iloc[0] == pytest.approx(4.0)


def test_distribute_outputs_of_different_lengths_leave_a_gap():
    set_schema(["subject", "trial"])
    result = for_each(
        lambda: (pd.DataFrame({"a": [1.0]}), pd.DataFrame({"b": [3.0, 4.0]})),
        inputs={},
        outputs=["A", "B"],
        distribute=True,
        subject=[1],
    )
    assert list(result["trial"]) == [1, 2]
    assert result["A"].iloc[1] is None
    assert result["B"].iloc[1]["b"].iloc[0] == pytest.approx(4.0)


def test_one_output_cannot_change_level_between_combinations():
    set_schema(["subject", "session", "speed"])

    frames = iter(
        [pd.DataFrame({"val": [1.0]}), pd.DataFrame({"session": ["x"], "val": [1.0]})]
    )
    with pytest.raises(ValueError, match="One output has one level"):
        for_each(lambda: next(frames), inputs={}, subject=["A", "B"], distribute=True)

