"""The ``[seam_fallback]`` line: written by as_user_value_error, read by
parse_seam_fallback (one owner of the format; ``scidb seams`` counts them)."""

import logging

import pytest

from scifor.failures import UserValueError, parse_seam_fallback, user_value_seam


def test_written_line_round_trips(caplog):
    def user_fn():
        pass

    with caplog.at_level(logging.WARNING, logger="scifor"):
        with pytest.raises(UserValueError):
            with user_value_seam("combine_outputs", fn_name="user_fn", fn=user_fn):
                raise TypeError("cannot concatenate")
    line = next(r.getMessage() for r in caplog.records if "[seam_fallback]" in r.getMessage())
    rec = parse_seam_fallback("2026-11-02 10:00:00.123 WARN  [scifor] " + line)
    assert rec == {
        "time": "2026-11-02 10:00:00.123",
        "stage": "combine_outputs",
        "fn": "user_fn",
        "output": "None",
        "exc_type": "TypeError",
        "message": "cannot concatenate",
    }


def test_other_lines_are_not_seams():
    assert parse_seam_fallback("2026-11-02 10:00:00.123 INFO  [scifor] for_each(f)") is None
