"""THE blame rule (scifor.failures.classify_blame) against
docs/claude/blame-cases.json; MATLAB's TestBlameCases.m reads the same file
through scifor.classify_blame."""

import json
from pathlib import Path

import pytest

from scifor.failures import ActionableError, FailureFrame, UserValueError, classify_blame

CASES_FILE = Path(__file__).resolve().parents[2] / "docs" / "claude" / "blame-cases.json"


def _cases() -> list[dict]:
    if not CASES_FILE.exists():
        raise AssertionError(f"Shared blame cases not found at {CASES_FILE}.")
    return json.loads(CASES_FILE.read_text(encoding="utf-8"))["cases"]


def _error(kind: str) -> BaseException:
    if kind == "plain":
        return RuntimeError("boom")
    if kind == "setup":
        return ActionableError("change your setup")
    if kind == "user_value":
        return UserValueError("bad value")
    if kind == "user_value_probable":
        return UserValueError("bad value?", probable=True)
    raise AssertionError(f"unknown error kind {kind!r}")


def _frames(origins: list[str]) -> list[FailureFrame]:
    return [
        FailureFrame(file=f"/x/{o}{i}.py", line=i + 1, function=f"f{i}", code="", origin=o)
        for i, o in enumerate(origins)
    ]


@pytest.mark.parametrize("case", _cases(), ids=lambda c: f"{c['id']:02d}-{c['name']}")
def test_blame_case(case):
    blame, probable, _rule = classify_blame(
        _error(case["error"]), _frames(case["frames"]), no_data=case.get("no_data", False)
    )
    assert (blame, probable) == (case["blame"], case["probable"])
