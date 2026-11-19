"""Merge name= / how= through for_each (docs/claude/merge-node.md).

The join rule itself is pinned case by case in test_merge_cases.py
(docs/claude/merge-cases.json). These tests check what only the loop can
show: a constituent with NO rows at a combo is a skipped combo for "exact"
and "inner", and a NaN-padded call for "left" / "outer".
"""

import logging

import pandas as pd
import pytest

from scifor import MERGE_HOWS, Merge, for_each, set_schema


@pytest.fixture(autouse=True)
def _schema():
    set_schema(["subject", "trial"])
    yield
    set_schema([])


# Force is recorded for subjects 1 and 2; EMG only for subject 1.
FORCE = pd.DataFrame({"subject": [1, 1, 2, 2], "trial": [1, 2, 1, 2], "force": [1.0, 2.0, 3.0, 4.0]})
EMG = pd.DataFrame({"subject": [1, 1], "trial": [1, 2], "emg": [0.1, 0.2]})


def _calls(how):
    received = []

    def fn(m):
        received.append(m.reset_index(drop=True))
        return 0

    for_each(fn, inputs={"m": Merge(FORCE, EMG, name="force_emg", how=how)}, subject=[1, 2])
    return received


def test_name_is_required():
    with pytest.raises(TypeError):
        Merge(FORCE, EMG)  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="non-empty name"):
        Merge(FORCE, EMG, name="  ")


def test_unknown_how_is_refused_with_the_choices():
    with pytest.raises(ValueError, match="not one of"):
        Merge(FORCE, EMG, name="x", how="cross")


def test_hows_are_the_documented_five():
    assert MERGE_HOWS == ("exact", "inner", "outer", "left", "right")


@pytest.mark.parametrize("how", ["exact", "inner"])
def test_a_missing_side_skips_the_combo(how):
    calls = _calls(how)
    assert len(calls) == 1, "subject 2 has no EMG: exact/inner must skip it"
    assert list(calls[0]["emg"]) == [0.1, 0.2]


@pytest.mark.parametrize("how", ["left", "outer"])
def test_a_missing_side_is_nan_padded(how, caplog):
    with caplog.at_level(logging.WARNING, logger="scifor"):
        calls = _calls(how)
    assert len(calls) == 2
    s2 = calls[1]
    assert list(s2["force"]) == [3.0, 4.0]
    assert s2["emg"].isna().all()
    assert "NaN-padded" in caplog.text, "a padded side must be logged at WARN"


def test_right_with_a_missing_left_side_skips():
    """Subject 2 has force but no EMG: a right join keeps EMG's rows -> none."""
    assert len(_calls("right")) == 1


def test_as_df_honours_how():
    df = Merge(FORCE, EMG, name="force_emg", how="left").as_df()
    assert len(df) == 4
    assert df["emg"].isna().sum() == 2


def test_exact_as_df_still_refuses_one_side_only():
    with pytest.raises(ValueError, match="Merge would drop rows"):
        Merge(FORCE, EMG, name="force_emg").as_df()
