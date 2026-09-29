"""WARN when a run iterates but nothing varies with the combo.

scidb.log run n1irqety (2026-09-29): a placeholder-free demographics CSV was
read once per subject — 18 identical reads — and the per-row ``subject``
column was dropped as pinned. The warning names the file and the fix.
"""

import logging

import pandas as pd

from scifor import PathInput
from scifor.foreach import _warn_repeated_static_path_inputs


def _static():
    return PathInput("/data/demographics.csv", name="DemographicsPath")


def test_static_pathinput_iterated_warns(caplog):
    with caplog.at_level(logging.WARNING, logger="scifor"):
        assert _warn_repeated_static_path_inputs(
            "read_csv", {"path": _static(), "sep": ","}, 18, ["subject"]
        )
    assert "demographics.csv" in caplog.text
    assert "18 iterations" in caplog.text


def test_one_call_does_not_warn():
    assert not _warn_repeated_static_path_inputs(
        "read_csv", {"path": _static()}, 1, []
    )


def test_placeholder_pathinput_does_not_warn():
    pi = PathInput("/data/{subject}.csv", name="PerSubject")
    assert not _warn_repeated_static_path_inputs("f", {"path": pi}, 18, ["subject"])


def test_a_data_input_beside_it_does_not_warn():
    """A static config file read beside per-combo data is legitimate."""
    df = pd.DataFrame({"subject": ["S1", "S2"], "x": [1.0, 2.0]})
    assert not _warn_repeated_static_path_inputs(
        "f", {"cfg": _static(), "data": df}, 2, ["subject"]
    )


def test_constants_only_does_not_warn():
    assert not _warn_repeated_static_path_inputs("f", {"k": 3}, 5, ["subject"])
